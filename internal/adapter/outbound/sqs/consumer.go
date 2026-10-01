// Package sqs provides an AWS SQS implementation of port.Consumer.
package sqs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/metrics"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
	pgdomain "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
)

const defaultDrainTimeout = 30 * time.Second

// SQSClientAPI is the subset of the AWS SQS API used by the consumer.
// Exposed for unit testing; *sqs.Client satisfies this interface.
type SQSClientAPI interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
}

// ConsumerOption is a functional option for configuring sqsConsumer.
type ConsumerOption func(*sqsConsumer)

// WithConcurrency sets the number of concurrent handler goroutines (default 1).
func WithConcurrency(n int) ConsumerOption {
	return func(c *sqsConsumer) {
		if n > 0 {
			c.concurrency = n
		} else if c.logger != nil {
			c.logger.Warn("sqs: WithConcurrency called with invalid value (must be > 0); using default of 1", map[string]any{
				"configured": n,
			})
		}
	}
}

// WithVisibilityTimeout sets the SQS visibility timeout for in-flight messages.
// The value is applied both to the initial ReceiveMessage call and to periodic
// ChangeMessageVisibility extension calls while the handler runs.
func WithVisibilityTimeout(d time.Duration) ConsumerOption {
	return func(c *sqsConsumer) { c.visibilityTimeout = d }
}

// WithDeadLetterHandler sets a handler for messages that have exhausted retries.
// The message is deleted from SQS only if the handler returns nil; on error the
// message is left visible for retry, matching the semantics of the normal handler.
func WithDeadLetterHandler(fn port.Handler) ConsumerOption {
	return func(c *sqsConsumer) {
		if fn != nil {
			c.deadLetterHandler = fn
		}
	}
}

// WithHandlerTimeout bounds the processing of each message — codec decode,
// dead-letter handler and handler — with one deadline d from when a worker
// picks it up: their contexts are cancelled at it, and the message's
// visibility is no longer extended past it, so a hung handler cannot hold a
// message invisible (and out of the queue's redrive) indefinitely — it becomes
// visible again and is redelivered, counting toward MaxReceiveCount. A handler
// that ignores its context still occupies its concurrency slot until it
// returns. 0 (default) is unbounded.
func WithHandlerTimeout(d time.Duration) ConsumerOption {
	return func(c *sqsConsumer) {
		if d > 0 {
			c.handlerTimeout = d
		}
	}
}

// WithMalformedBodyLogging includes the first 512 bytes of a malformed
// message's body in its ERROR log. Off by default: bodies may carry tenant
// data (PII), and the log otherwise records only the body's size and SHA-256
// — with WithDLQPublisher the full body is preserved in the DLQ.
func WithMalformedBodyLogging() ConsumerOption {
	return func(c *sqsConsumer) { c.logMalformedBodies = true }
}

// WithDLQPublisher forwards poison messages to the source queue's configured
// dead-letter queue via p, deleting the original only after the forward
// succeeds (on failure it stays visible and SQS's own redrive remains the
// backstop). Forwarded are, verbatim with their original attributes:
//   - bodies that are not a valid envelope (instead of deleting them);
//   - messages whose ApproximateReceiveCount exceeds the WithMaxReceiveCount
//     threshold — after the dead-letter handler, when one is set, succeeds;
//   - messages over that threshold whose codec decode fails.
//
// Start resolves the DLQ first and fails on a missing or malformed
// RedrivePolicy (see checkDLQ). The threshold must be strictly lower than the
// queue's RedrivePolicy maxReceiveCount, or SQS moves the message first.
func WithDLQPublisher(p port.DLQPublisher) ConsumerOption {
	return func(c *sqsConsumer) { c.dlq = p }
}

// WithMaxReceiveCount sets the ApproximateReceiveCount threshold at which a message
// is routed to the dead-letter handler and/or DLQ publisher instead of the
// normal handler. Requires WithDeadLetterHandler or WithDLQPublisher; 0
// disables dead-letter routing.
func WithMaxReceiveCount(n int) ConsumerOption {
	return func(c *sqsConsumer) { c.maxReceiveCount = n }
}

// WithCodec sets an optional schema-registry codec used to decode incoming
// messages whose Envelope.SchemaID is non-empty. Unset (nil, the default),
// all messages are treated as plain JSON exactly as before this option
// existed. See port.Codec for the Decode contract.
func WithCodec(codec port.Codec) ConsumerOption {
	return func(c *sqsConsumer) { c.codec = codec }
}

// WithDrainTimeout sets how long Stop() waits for in-flight handlers to finish.
// A negative duration is ignored and the default (30s) is preserved.
// A zero duration disables the drain window entirely — in-flight handler contexts
// are cancelled immediately when Stop is called, with no waiting period. This is
// useful for tests; in production prefer a positive value (≥ p99 handler latency).
func WithDrainTimeout(d time.Duration) ConsumerOption {
	return func(c *sqsConsumer) {
		if d >= 0 {
			c.drainTimeout = d
		} else if c.logger != nil {
			c.logger.Warn("sqs: WithDrainTimeout called with negative duration; using default", map[string]any{
				"configured": d.String(),
				"default":    defaultDrainTimeout.String(),
			})
		}
	}
}

// Config holds the parameters for constructing an SQS consumer.
//
// SNS subscription requirement: if this queue receives messages via an SNS
// subscription, the subscription MUST have RawMessageDelivery=true. Without it,
// SNS wraps each message in a notification envelope that the consumer cannot
// parse — messages are treated as malformed: forwarded to the DLQ with
// WithDLQPublisher, otherwise deleted, with no retry.
type Config struct {
	// QueueURL is required.
	QueueURL    string
	Region      string
	EndpointURL string // optional — set to an AWS emulator (floci) URL for local runs and tests
	MaxMessages int32  // 1-10; defaults to 10
	WaitSeconds int32  // long-poll duration; defaults to 20
	Logger      port.Logger
}

type sqsConsumer struct {
	client            SQSClientAPI
	queueURL          string
	handler           port.Handler
	logger            port.Logger
	maxMessages       int32
	waitSeconds       int32
	concurrency       int
	visibilityTimeout time.Duration
	deadLetterHandler port.Handler
	dlq               port.DLQPublisher
	drainTimeout      time.Duration
	codec             port.Codec
	// queueDepthInterval > 0 enables the platform_queue_depth / platform_dlq_depth sampler.
	queueDepthInterval time.Duration
	// handlerTimeout > 0 bounds each handler call (and the visibility
	// extension for it); 0 means unbounded.
	handlerTimeout time.Duration
	// logMalformedBodies includes a body excerpt in the malformed-message log.
	logMalformedBodies bool
	// fifo is set for a FIFO queue (URL ending in .fifo): each message group
	// of a batch is processed by one worker, in order (see processGroup).
	fifo bool

	maxReceiveCount int
	cancelFn        context.CancelFunc
	mu              sync.Mutex
	running         bool
	// doneCh is closed when the Start goroutine has fully returned (drain complete).
	// It is recreated on every Start() call so the consumer is fully restartable.
	// The initial value is a pre-closed channel so Stop() before Start() is safe.
	doneCh chan struct{}
}

// New constructs an SQS consumer. Returns an error if QueueURL is empty.
func New(cfg Config, handler port.Handler, opts ...ConsumerOption) (port.Consumer, error) {
	if cfg.QueueURL == "" {
		return nil, fmt.Errorf("sqs: QueueURL is required")
	}
	if handler == nil {
		return nil, fmt.Errorf("sqs: handler is required")
	}

	awsOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
	}
	startupCtx, startupCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer startupCancel()
	awsCfg, err := awsconfig.LoadDefaultConfig(startupCtx, awsOpts...)
	if err != nil {
		return nil, fmt.Errorf("sqs: failed to load AWS config: %w", err)
	}

	sqsOpts := []func(*sqs.Options){}
	if cfg.EndpointURL != "" {
		sqsOpts = append(sqsOpts, func(o *sqs.Options) {
			o.BaseEndpoint = aws.String(cfg.EndpointURL)
		})
	}

	client := sqs.NewFromConfig(awsCfg, sqsOpts...)
	return NewWithClient(cfg, client, handler, opts...)
}

// NewWithClient constructs an SQS consumer with an injected client.
// Useful in tests to provide a mock SQS client without real AWS credentials.
// Returns an error if QueueURL is empty or handler is nil.
func NewWithClient(cfg Config, client SQSClientAPI, handler port.Handler, opts ...ConsumerOption) (port.Consumer, error) {
	if cfg.QueueURL == "" {
		return nil, fmt.Errorf("sqs: QueueURL is required")
	}
	if handler == nil {
		return nil, fmt.Errorf("sqs: handler is required")
	}

	maxMessages := cfg.MaxMessages
	if maxMessages <= 0 || maxMessages > 10 {
		if cfg.Logger != nil && maxMessages != 0 {
			cfg.Logger.Warn("sqs: MaxMessages out of range [1,10]; using 10", map[string]any{
				"configured": maxMessages,
			})
		}
		maxMessages = 10
	}
	waitSeconds := cfg.WaitSeconds
	if waitSeconds <= 0 {
		waitSeconds = 20
	}
	// SQS rejects WaitTimeSeconds > 20 with a validation error.
	if waitSeconds > 20 {
		waitSeconds = 20
	}

	// Pre-closed initial doneCh so Stop() called before Start() returns immediately.
	initialDone := make(chan struct{})
	close(initialDone)

	c := &sqsConsumer{
		client:       client,
		queueURL:     cfg.QueueURL,
		handler:      handler,
		logger:       cfg.Logger,
		maxMessages:  maxMessages,
		waitSeconds:  waitSeconds,
		concurrency:  1,
		drainTimeout: defaultDrainTimeout,
		doneCh:       initialDone,
		fifo:         strings.HasSuffix(cfg.QueueURL, ".fifo"),
	}
	for _, opt := range opts {
		opt(c)
	}

	// If a dead-letter handler is registered but no receive-count threshold was
	// set, apply a safe default so the handler is actually invoked.
	if (c.deadLetterHandler != nil || c.dlq != nil) && c.maxReceiveCount == 0 {
		c.maxReceiveCount = 5
		if c.logger != nil {
			c.logger.Warn("sqs: WithDeadLetterHandler/WithDLQPublisher set without WithMaxReceiveCount; defaulting maxReceiveCount to 5", nil)
		}
	}

	// SQS hard limit on VisibilityTimeout is 43200 seconds (12 hours).
	// Exceeding it causes the ReceiveMessage and ChangeMessageVisibility API
	// calls to fail with an InvalidParameterValue error, which is confusing
	// to diagnose at runtime. Reject at construction time with a clear error.
	const maxSQSVisibilityTimeout = 12 * time.Hour
	if c.visibilityTimeout > maxSQSVisibilityTimeout {
		return nil, fmt.Errorf("sqs: visibilityTimeout %s exceeds SQS maximum of 12 hours", c.visibilityTimeout)
	}

	return c, nil
}

// Start begins the SQS long-poll receive loop. Blocks until ctx is cancelled.
// When Start returns, all in-flight handlers have completed (or the drain
// timeout has been exceeded). Stop() blocks until Start returns.
// The consumer is fully restartable: Start() may be called again after Stop().
func (c *sqsConsumer) Start(ctx context.Context) error {
	metrics.InitQueue(c.queueURL)

	// Create a fresh doneCh for this cycle before entering the loop.
	// All fields written here are protected by c.mu; Stop() reads doneCh under
	// the same lock, guaranteeing it sees the channel for the current cycle.
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return fmt.Errorf("sqs: consumer is already running")
	}
	loopCtx, cancel := context.WithCancel(ctx)
	defer cancel() // always release loopCtx resources when Start() exits
	c.cancelFn = cancel
	c.running = true
	thisDoneCh := make(chan struct{})
	c.doneCh = thisDoneCh
	c.mu.Unlock()

	// Reset running and signal Stop() when this Start() goroutine exits.
	// Registered first so it runs last — after the queue-depth poller below
	// has exited, so Stop() never returns while that goroutine still runs.
	defer func() {
		c.mu.Lock()
		c.running = false
		c.mu.Unlock()
		close(thisDoneCh)
	}()

	// Queue-depth sampler (opt-in): stops with loopCtx; Start does not return
	// until it has exited.
	waitDepthPoller := c.startQueueDepthPoller(loopCtx)
	defer func() {
		cancel()
		waitDepthPoller()
	}()

	// Resolve the DLQ only once this cycle is registered, so a Stop() during
	// the (up to 10s) check cancels it instead of being lost.
	if err := c.checkDLQ(loopCtx); err != nil {
		return err
	}

	// wg is local to this Start() cycle so that drain() can never mix in-flight
	// handlers from a previous cycle (which would occur if wg were a struct field
	// and a prior drain timeout left handlers still running).
	var wg sync.WaitGroup

	// drainCtx is cancelled when the drain timeout fires, propagating a
	// cancellation signal to all in-flight handler goroutines so they can
	// exit cleanly rather than leaking past the drain window.
	drainCtx, drainCancel := context.WithCancel(context.Background())
	defer drainCancel()

	// inflight tracks the number of handler goroutines currently running so the
	// drain timeout log can report how many were cancelled rather than just "some".
	var inflight atomic.Int32

	// drain waits for all in-flight handler goroutines to finish.
	// If the drain timeout fires, drainCancel() is called to signal handlers.
	drain := func() {
		drained := make(chan struct{})
		go func() {
			wg.Wait()
			close(drained)
		}()
		timer := time.NewTimer(c.drainTimeout)
		defer timer.Stop()
		select {
		case <-drained:
		case <-timer.C:
			drainCancel() // cancel in-flight handler contexts
			if c.logger != nil {
				c.logger.Warn("sqs: drain timeout exceeded; cancelling remaining handlers", map[string]any{
					"in_flight": inflight.Load(),
				})
			}
			// drain() returns immediately after cancelling contexts. The wg.Wait()
			// goroutine above will exit once handlers respond to the cancellation and
			// return — this is bounded by handler responsiveness, not permanent. Handlers
			// that do not check ctx will linger until they finish their current work.
		}
	}

	sem := make(chan struct{}, c.concurrency)

	const (
		receiveBackoffInit = time.Second
		receiveBackoffMax  = 30 * time.Second
	)
	receiveBackoff := receiveBackoffInit

	// Build the ReceiveMessage input once; visibilityTimeout is fixed per consumer.
	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(c.queueURL),
		MaxNumberOfMessages: c.maxMessages,
		WaitTimeSeconds:     c.waitSeconds,
		// Request all custom message attributes so that any OTel propagator (W3C
		// traceparent, baggage, vendor-specific headers) is forwarded intact. This
		// is the only safe choice when a composite propagator is in use.
		MessageAttributeNames: []string{"All"},
		MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{
			sqstypes.MessageSystemAttributeNameApproximateReceiveCount,
		},
	}
	// Apply the configured visibility timeout to the receive call so messages
	// are hidden for the full expected handler duration from the moment of receive,
	// not just from when the first extension fires. This prevents duplicates when
	// the handler runs close to the queue default timeout.
	if c.visibilityTimeout > 0 {
		receiveInput.VisibilityTimeout = max(int32(c.visibilityTimeout.Seconds()), 1)
	}
	if c.fifo {
		receiveInput.MessageSystemAttributeNames = append(receiveInput.MessageSystemAttributeNames,
			sqstypes.MessageSystemAttributeNameMessageGroupId)
	}

	for {
		select {
		case <-loopCtx.Done():
			drain()
			return nil
		default:
		}

		// Bound each ReceiveMessage call with a per-call timeout so a hung network
		// connection (established but no response) cannot stall the poll loop
		// indefinitely. WaitTimeSeconds covers legitimate long-poll latency; the
		// extra 5 s covers AWS control-plane overhead and TLS handshakes.
		// We pass loopCtx as the parent so cancellation still propagates immediately
		// on Stop(), even before the per-call deadline fires.
		rcvCtx, rcvCancel := context.WithTimeout(loopCtx, time.Duration(int(c.waitSeconds)+5)*time.Second)
		rcvStart := time.Now()
		in := c.receiveInput(receiveInput, sem, loopCtx)
		if c.fifo {
			// One attempt ID per logical receive: an SDK retry of a receive
			// whose response was lost gets the same messages back instead of
			// leaving their groups blocked until the visibility timeout.
			attempt := *in
			attempt.ReceiveRequestAttemptId = aws.String(uuid.NewString())
			in = &attempt
		}
		out, err := c.client.ReceiveMessage(rcvCtx, in)
		rcvDur := time.Since(rcvStart)
		rcvCancel()
		if err != nil {
			select {
			case <-loopCtx.Done():
				// Context was cancelled while ReceiveMessage was in-flight.
				// Drain handlers before returning — same as the top-of-loop path.
				drain()
				return nil
			default:
			}
			metrics.ObserveDependency("sqs", "receive_message", err, rcvDur)
			if c.logger != nil {
				c.logger.Error("sqs: receive message failed", map[string]any{"error": err.Error()})
			}
			// Interruptible backoff: use NewTimer so the timer goroutine is
			// cancelled when Stop() fires, preventing a leak of up to 30s per
			// error when the consumer is stopped during the backoff window.
			backoffTimer := time.NewTimer(receiveBackoff)
			select {
			case <-backoffTimer.C:
			case <-loopCtx.Done():
				backoffTimer.Stop()
				drain()
				return nil
			}
			// Double the delay plus a random 0–50% of the current one, so
			// concurrent consumer instances do not retry in lock-step after
			// a shared SQS error (thundering herd on recovery).
			jitter := time.Duration(rand.Int64N(int64(receiveBackoff) / 2))
			receiveBackoff = min(receiveBackoff*2+jitter, receiveBackoffMax)
			continue
		}
		receiveBackoff = receiveBackoffInit // reset on success
		metrics.ObserveDependency("sqs", "receive_message", nil, rcvDur)

		// A received message's visibility clock starts at once. Extend every
		// message from receipt — including those waiting for a busy worker —
		// so a batch queued behind slow handlers never reappears and is
		// processed twice (inflating its receive count towards the DLQ).
		// With WithHandlerTimeout, waiting is bounded too: a message still
		// queued when the timeout passes (every worker stuck) is handed back
		// to the queue instead of being extended forever.
		exts := make([]*visibilityExtender, len(out.Messages))
		for i, msg := range out.Messages {
			exts[i] = c.extendVisibility(msg)
		}
		// A unit is what one worker processes: one message, or — on a FIFO
		// queue — one message group's messages of this batch, in order.
		units := c.workUnits(out.Messages)
		if c.handlerTimeout > 0 {
			now := time.Now()
			for _, unit := range units {
				// The unit's first message may wait one handler timeout for a
				// worker. A FIFO group's later messages also wait for the
				// messages ahead of them by design, so they get the head's
				// waiting and processing budget; processGroup re-arms them as
				// each message is dispatched.
				exts[unit[0]].SetDeadline(now.Add(c.handlerTimeout))
				for _, i := range unit[1:] {
					exts[i].SetDeadline(now.Add(2 * c.handlerTimeout))
				}
			}
		}
		for u, unit := range units {
			// Interruptible semaphore acquire: if loopCtx is cancelled while all
			// concurrency slots are busy the blocking send would hold Start() forever,
			// preventing drain() from ever being called and deadlocking Stop().
			select {
			case sem <- struct{}{}:
			case <-loopCtx.Done():
				// Undispatched messages: hand them back to the queue at once
				// (visibility 0) so another consumer can take them.
				for _, rest := range units[u:] {
					for _, i := range rest {
						exts[i].Release()
					}
				}
				drain()
				return nil
			}
			if len(unit) == 1 {
				i := unit[0]
				// Reset the deadline before claiming: once claimed, the
				// extender no longer releases the message, so a waiting
				// deadline that passes in the gap before dispatch sets its own
				// would end the extension early.
				c.resetDeadline(exts[i])
				// The message waited past its deadline and was handed back.
				if !exts[i].Claim() {
					<-sem
					continue
				}
				wg.Go(func() {
					defer func() { <-sem }()
					c.runOne(drainCtx, loopCtx, out.Messages[i], exts[i], &inflight)
				})
				continue
			}
			wg.Go(func() {
				defer func() { <-sem }()
				c.processGroup(drainCtx, loopCtx, out.Messages, exts, unit, &inflight)
			})
		}
	}
}

// workUnits splits a received batch into the units one worker processes: a
// unit per message on a standard queue; on a FIFO queue one unit per message
// group, holding that group's messages in receive (= group) order.
func (c *sqsConsumer) workUnits(msgs []sqstypes.Message) [][]int {
	units := make([][]int, 0, len(msgs))
	if !c.fifo {
		for i := range msgs {
			units = append(units, []int{i})
		}
		return units
	}
	byGroup := make(map[string]int, len(msgs))
	for i, msg := range msgs {
		g := msg.Attributes[string(sqstypes.MessageSystemAttributeNameMessageGroupId)]
		if u, ok := byGroup[g]; ok {
			units[u] = append(units[u], i)
			continue
		}
		byGroup[g] = len(units)
		units = append(units, []int{i})
	}
	return units
}

// processGroup processes one FIFO message group's messages of a batch in
// order. A message that is not settled (handler error, failed decode or DLQ
// forward, panic, or one that waited past its deadline) stops the group: the
// later messages are handed back unprocessed — SQS redelivers them only after
// the earlier one, so the group's order holds. Stop hands the rest back too.
func (c *sqsConsumer) processGroup(drainCtx, loopCtx context.Context, msgs []sqstypes.Message, exts []*visibilityExtender, unit []int, inflight *atomic.Int32) {
	for k, i := range unit {
		if loopCtx.Err() != nil {
			c.releaseGroupTail(msgs, exts, unit[k:], "consumer stopping")
			return
		}
		c.resetDeadline(exts[i])
		if !exts[i].Claim() {
			c.releaseGroupTail(msgs, exts, unit[k+1:], "message waited past the handler timeout")
			return
		}
		// The rest of the group waits while this message runs: give each one
		// this message's processing budget plus its own waiting budget, so a
		// busy group is never released — only one whose message hangs.
		if c.handlerTimeout > 0 {
			rest := time.Now().Add(2 * c.handlerTimeout)
			for _, j := range unit[k+1:] {
				exts[j].SetDeadline(rest)
			}
		}
		if !c.runOne(drainCtx, loopCtx, msgs[i], exts[i], inflight) {
			c.releaseGroupTail(msgs, exts, unit[k+1:], "message not settled")
			return
		}
	}
}

// releaseGroupTail hands a FIFO group's remaining messages of this batch back
// unprocessed; SQS redelivers them after the message that stopped the group.
func (c *sqsConsumer) releaseGroupTail(msgs []sqstypes.Message, exts []*visibilityExtender, idx []int, reason string) {
	if len(idx) == 0 {
		return
	}
	for _, i := range idx {
		exts[i].Release()
	}
	if c.logger != nil {
		c.logger.Warn("sqs: FIFO group stopped — its later messages were handed back unprocessed", map[string]any{
			"queue":            c.queueURL,
			"message_group_id": msgs[idx[0]].Attributes[string(sqstypes.MessageSystemAttributeNameMessageGroupId)],
			"released":         len(idx),
			"reason":           reason,
		})
	}
}

// resetDeadline restarts a waiting message's WithHandlerTimeout deadline just
// before it is claimed (dispatch then sets the processing deadline).
func (c *sqsConsumer) resetDeadline(ext *visibilityExtender) {
	if c.handlerTimeout > 0 {
		ext.SetDeadline(time.Now().Add(c.handlerTimeout))
	}
}

// runOne dispatches one claimed message, recovering a panic, and reports
// whether the message was settled (deleted or moved to the DLQ).
func (c *sqsConsumer) runOne(drainCtx, loopCtx context.Context, msg sqstypes.Message, ext *visibilityExtender, inflight *atomic.Int32) (settled bool) {
	inflight.Add(1)
	metrics.AddInFlight(c.queueURL, 1)
	defer metrics.AddInFlight(c.queueURL, -1)
	defer inflight.Add(-1)
	defer func() {
		if r := recover(); r != nil {
			settled = false
			if c.logger != nil {
				c.logger.Error("sqs: handler panic recovered", map[string]any{
					"message_id": aws.ToString(msg.MessageId),
					"panic":      fmt.Sprintf("%v", r),
					"stack":      string(debug.Stack()),
				})
			}
		}
	}()
	defer ext.Stop()
	return c.dispatch(drainCtx, loopCtx, msg, ext)
}

// dlqStartupCheckTimeout bounds the DLQ resolution Start performs when DLQ
// forwarding is enabled.
const dlqStartupCheckTimeout = 10 * time.Second

// checkDLQ resolves the queue's DLQ when DLQ forwarding is enabled. Without a
// usable RedrivePolicy every forward fails permanently, so poison messages
// would never be deleted — and with no RedrivePolicy SQS never moves them
// either — leaving them redelivered until the retention period expires. A
// permanent configuration error is therefore returned; a transient failure is
// logged and the consumer starts anyway (each forward re-resolves).
func (c *sqsConsumer) checkDLQ(ctx context.Context) error {
	if c.dlq == nil {
		return nil
	}
	checkCtx, cancel := context.WithTimeout(ctx, dlqStartupCheckTimeout)
	defer cancel()
	dlqURL, err := c.dlq.ResolveDLQ(checkCtx, c.queueURL)
	switch {
	case err == nil:
		if c.logger != nil {
			c.logger.Info("sqs: DLQ forwarding enabled", map[string]any{"queue": c.queueURL, "dlq_url": dlqURL})
		}
		return nil
	case errors.Is(err, domain.ErrDLQNotConfigured), errors.Is(err, domain.ErrDLQInvalidRedrivePolicy):
		return fmt.Errorf("sqs: DLQ forwarding is enabled but the queue has no usable RedrivePolicy: %w", err)
	default:
		if c.logger != nil {
			c.logger.Warn("sqs: could not resolve DLQ at startup; forwards will retry resolution", map[string]any{
				"queue": c.queueURL,
				"error": err.Error(),
			})
		}
		return nil
	}
}

// Stop cancels the receive loop and waits for Start to return. Start waits up
// to DrainTimeout for in-flight handlers; past it, their contexts are
// cancelled and Start returns without waiting further, so handler goroutines
// that ignore ctx may still be running when Stop returns nil. Stop returns an
// error only if Start has not returned within DrainTimeout+5s.
// In Kubernetes, ensure your pod terminationGracePeriodSeconds > DrainTimeout
// so the process does not receive SIGKILL before handlers exit.
// Safe to call multiple times and safe to call before Start. Stop affects only
// a Start that is already running — a Stop that races ahead of a Start in
// another goroutine is a no-op — so shut down by cancelling the ctx passed to
// Start and then calling Stop to wait for the drain.
func (c *sqsConsumer) Stop() error {
	c.mu.Lock()
	if c.cancelFn != nil {
		c.cancelFn()
	}
	doneCh := c.doneCh // capture current cycle's channel under lock
	drainTimeout := c.drainTimeout
	c.mu.Unlock()

	// Hard deadline: drainTimeout (for handlers) + 5s margin for housekeeping.
	// This ensures Stop() itself cannot block indefinitely if Start() hangs.
	timer := time.NewTimer(drainTimeout + 5*time.Second)
	defer timer.Stop()
	select {
	case <-doneCh:
		return nil
	case <-timer.C:
		return fmt.Errorf("sqs: consumer did not stop within drain timeout (%s)", drainTimeout)
	}
}

// dispatch processes a single SQS message. It receives two contexts:
//   - drainCtx: cancelled by the drain timeout; propagated to the user handler
//     so in-flight handlers can be signalled when the drain deadline fires.
//   - loopCtx: the receive-loop context cancelled by Stop(); used for
//     visibility-timeout extension (which must stop when the loop stops).
//
// It reports whether the message was settled — deleted or moved to the DLQ.
func (c *sqsConsumer) dispatch(drainCtx, loopCtx context.Context, msg sqstypes.Message, ext *visibilityExtender) (settled bool) {
	tracer := otel.Tracer("platform-events")
	receivedAt := time.Now()
	metrics.ObserveReceived(c.queueURL)

	var env domain.Envelope[json.RawMessage]
	body := aws.ToString(msg.Body)
	parseErr := json.Unmarshal([]byte(body), &env)
	if parseErr == nil {
		// Valid JSON is not necessarily an envelope: an SNS notification
		// wrapper (subscription without RawMessageDelivery) decodes "Type"
		// into env.Type — Go matches keys case-insensitively — and leaves the
		// rest empty. Require id, type and source; unlike ParseEnvelope, a
		// missing time is tolerated (it only feeds the propagation metric).
		parseErr = missingEnvelopeFields(env)
	}
	if parseErr != nil {
		return c.handleMalformed(drainCtx, msg, ext, body, parseErr)
	}

	// Derive handlerCtx WITHOUT propagating loopCtx cancellation. When Stop()
	// fires it cancels loopCtx, but in-flight handlers must complete their DB
	// writes and downstream calls uninterrupted — the drain() call in Start()
	// gives them drainTimeout to finish. context.WithoutCancel preserves any
	// context values (OTel baggage, etc.) while stripping the cancel signal.
	// Inject TenantID for downstream pgcommon pool RLS enforcement.
	// Inject TraceID via the port context key so handler code can retrieve it
	// with events.TraceIDFromContext — GUCSet does not carry TraceID.
	handlerBase := HandlerContext(context.WithoutCancel(loopCtx), env.TenantID, env.TraceID, func() port.SourceMessage { return sourceMessage(c.queueURL, msg) })
	receiveCount := approxReceiveCount(msg.Attributes)
	overThreshold := c.maxReceiveCount > 0 && receiveCount > c.maxReceiveCount

	// WithHandlerTimeout bounds the whole processing of this message — codec
	// decode, dead-letter handler and handler — with ONE deadline from here:
	// every context below derives from handlerBase, and the visibility
	// extension stops at the same instant, so the message is never released
	// for redelivery while its processing is still within its budget.
	if c.handlerTimeout > 0 {
		deadline := time.Now().Add(c.handlerTimeout)
		ext.SetDeadline(deadline)
		var cancelDeadline context.CancelFunc
		handlerBase, cancelDeadline = context.WithDeadline(handlerBase, deadline)
		defer cancelDeadline()
	}
	// timedOut reports whether the WithHandlerTimeout deadline has expired
	// (as opposed to a drain-timeout cancellation).
	deadlineCtx := handlerBase
	timedOut := func() bool {
		return c.handlerTimeout > 0 && errors.Is(deadlineCtx.Err(), context.DeadlineExceeded)
	}
	// Propagation is creation → FIRST receipt; a redelivery's age would add
	// retry delay. A missing receive count (0) is treated as a first receipt.
	if receiveCount <= 1 {
		metrics.ObservePropagation(c.queueURL, env.Type, env.Timestamp, receivedAt)
	}

	// Codec decode — SchemaID is the signal: empty means Payload is already
	// plain JSON (legacy producer, NoopCodec, or WithCodec never configured
	// on the publisher). A decode failure (registry outage, corrupt payload,
	// or SchemaID set with no codec configured here) is treated like a normal
	// handler error, NOT like the malformed-JSON case above: it may be
	// transient, so the message is left visible for retry via SQS's own
	// MaxReceiveCount/redrive-policy mechanics rather than deleted immediately.
	// Only a JSON-string payload can be codec-encoded (WrapCodecPayload); a
	// dataschema on a plain JSON object or array is informational, and
	// decoding it would fail on every delivery and dead-letter the message.
	if env.SchemaID != "" && isJSONString(env.Payload) {
		decodeStart := time.Now()
		decoded, decErr := c.decode(drainCtx, handlerBase, env.SchemaID, env.Payload)
		dur := time.Since(decodeStart)
		if c.codec != nil {
			metrics.ObserveDependency("codec", "decode", decErr, dur)
		}
		if decErr != nil {
			metrics.IncFailed(c.queueURL, env.Type, "decode_error")
			if timedOut() {
				metrics.IncTimeout(c.queueURL, env.Type, "decode")
			}
			if c.logger != nil {
				c.logger.Error("sqs: codec decode failed — message left visible for retry", map[string]any{
					"message_id": aws.ToString(msg.MessageId),
					"event_type": env.Type,
					"schema_id":  env.SchemaID,
					"queue":      c.queueURL,
					"error":      decErr.Error(),
				})
			}
			// Past the dead-letter threshold, stop retrying a payload that keeps
			// failing to decode: forward the undecoded original to the DLQ.
			if c.dlq != nil && overThreshold &&
				c.forwardToDLQ(drainCtx, handlerBase, msg, env.Type, "decode_error", "codec decode failed: "+decErr.Error()) {
				return c.settle(msg, ext)
			}
			metrics.IncRetry("consume", env.Type)
			return
		}
		env.Payload = decoded
	}

	// Route to dead-letter handler when ApproximateReceiveCount reaches the threshold.
	// The message is deleted only if the dead-letter handler succeeds; on failure it
	// is left visible for retry, matching the semantics of the normal handler.
	if c.deadLetterHandler != nil || c.dlq != nil {
		// overThreshold uses > (strictly greater than) to match SQS DLQ
		// semantics: SQS moves a message after the receive count *exceeds*
		// MaxReceiveCount (i.e. on the N+1th delivery). Using >= would fire one
		// delivery too early, consuming the last retry budget before SQS would
		// have acted.
		if overThreshold {
			// Tie DLH context to drainCtx so it respects the drain deadline.
			dlhCtx, dlhCancel := context.WithCancel(handlerBase)
			// A SendToDLQ from the dead-letter handler is attributed to
			// max_receive_count and counted once, by the DLQ publisher.
			dlhCtx, dlhAttribution := port.WithDLQAttribution(dlhCtx, "max_receive_count")
			stopDrain := context.AfterFunc(drainCtx, dlhCancel)
			defer stopDrain()
			defer dlhCancel()

			start := time.Now()
			var dlhErr error
			if c.deadLetterHandler != nil {
				dlhErr = c.runDeadLetterHandler(dlhCtx, env, msg)
			}
			// The DLH runs first so a failing DLH leaves the message on the
			// source queue un-forwarded; a DLH that succeeded runs again if the
			// forward then fails, so it must be idempotent. A DLH that already
			// forwarded the message itself (SendToDLQ) is not forwarded again.
			if dlhErr == nil && c.dlq != nil && !dlhAttribution.Recorded() {
				reason := fmt.Sprintf("receive count %d exceeded consumer max receive count %d", receiveCount, c.maxReceiveCount)
				if !c.forwardToDLQ(drainCtx, handlerBase, msg, env.Type, "max_receive_count", reason) {
					dlhErr = errDLQForwardFailed
				}
			}
			dur := time.Since(start)

			if dlhErr != nil {
				// A failed DLQ forward was already logged by forwardToDLQ.
				if c.logger != nil && !errors.Is(dlhErr, errDLQForwardFailed) {
					c.logger.Error("sqs: dead-letter handler failed — message left visible for retry", map[string]any{
						"message_id": aws.ToString(msg.MessageId),
						"event_type": env.Type,
						"tenant_id":  env.TenantID,
						"event_id":   env.ID,
						"error":      dlhErr.Error(),
					})
				}
			} else if c.logger != nil {
				c.logger.Warn("sqs: dead-letter handler invoked", map[string]any{
					"message_id":                aws.ToString(msg.MessageId),
					"event_type":                env.Type,
					"tenant_id":                 env.TenantID,
					"event_id":                  env.ID,
					"approximate_receive_count": receiveCount,
					"forwarded_to_dlq":          c.dlq != nil,
				})
			}

			if c.deadLetterHandler != nil {
				metrics.ObserveProcessingDuration(c.queueURL, env.Type, dur)
			}

			if dlhErr != nil {
				metrics.IncFailed(c.queueURL, env.Type, "dead_letter_error")
				if timedOut() {
					metrics.IncTimeout(c.queueURL, env.Type, "dead_letter_handler")
				}
				metrics.IncRetry("consume", env.Type)
				return // do NOT delete — leave visible for retry
			}
			// Dead-lettered by the dead-letter handler alone (no forwarding,
			// no SendToDLQ of its own): count it here. Forwards were counted
			// in forwardToDLQ / by the DLQ publisher.
			if c.dlq == nil && !dlhAttribution.Recorded() {
				metrics.IncDLQ("consume", env.Type, "max_receive_count")
			}
			return c.settle(msg, ext)
		}
	}

	// Extract W3C traceparent from SNS message attributes for cross-service trace
	// propagation. The publisher injects traceparent when OTel is initialised.
	spanOpts := []oteltrace.SpanStartOption{oteltrace.WithSpanKind(oteltrace.SpanKindConsumer)}
	carrier := make(propagation.MapCarrier)
	for k, v := range msg.MessageAttributes {
		if v.StringValue != nil {
			carrier[k] = *v.StringValue
		}
	}
	if len(carrier) > 0 {
		// Extract into a clean context to get the remote span for linking.
		// Async message consumers start a new trace root with a Link to the
		// producer span — they do not inherit the remote span as a parent.
		extractCtx := otel.GetTextMapPropagator().Extract(context.Background(), carrier)
		remoteSpan := oteltrace.SpanFromContext(extractCtx)
		if remoteSpan.SpanContext().IsValid() {
			spanOpts = append(spanOpts, oteltrace.WithLinks(oteltrace.Link{SpanContext: remoteSpan.SpanContext()}))
		}
		// Propagate W3C baggage (feature flags, correlation IDs, etc.) into
		// handlerBase so handler code can access it via baggage.FromContext.
		if b := baggage.FromContext(extractCtx); b.Len() > 0 {
			handlerBase = baggage.ContextWithBaggage(handlerBase, b)
		}
	}

	// Build a cancellable handler context tied to the drain deadline.
	// context.WithoutCancel strips loopCtx cancellation (intentional — handlers
	// must run to completion during graceful shutdown). context.AfterFunc links
	// the drain deadline so handlers are cancelled when drain timeout fires.
	handlerCtx, handlerCancel := context.WithCancel(handlerBase)
	stopDrain := context.AfterFunc(drainCtx, handlerCancel)
	defer stopDrain()
	defer handlerCancel()
	// A handler that dead-letters its message explicitly (SendToDLQ, then
	// return nil) is counted as dead-lettered, not processed.
	handlerCtx, handlerAttribution := port.WithDLQAttribution(handlerCtx, "explicit")

	handlerCtx, span := tracer.Start(handlerCtx, "sqs.receive", spanOpts...)
	defer span.End()

	span.SetAttributes(
		attribute.String("messaging.system", "aws_sqs"),
		attribute.String("messaging.destination", c.queueURL),
		attribute.String("messaging.message_id", aws.ToString(msg.MessageId)),
		attribute.String("messaging.operation", "process"),
		attribute.String("events.event_type", env.Type),
		attribute.String("events.event_id", env.ID),
	)

	start := time.Now()

	// Wrap the handler call to capture panics inside this function where the span
	// is accessible. On panic: record error on the span, then re-panic so the
	// goroutine-level recovery can log the stack trace.
	var handlerErr error
	var handlerPanic interface{}
	func() {
		defer func() { handlerPanic = recover() }()
		handlerErr = c.handler(handlerCtx, env)
	}()
	dur := time.Since(start)

	if handlerPanic != nil {
		panicErr := fmt.Errorf("handler panic: %v", handlerPanic)
		span.RecordError(panicErr)
		span.SetStatus(codes.Error, panicErr.Error())
		metrics.ObserveProcessingDuration(c.queueURL, env.Type, dur)
		metrics.IncFailed(c.queueURL, env.Type, "handler_panic")
		metrics.IncRetry("consume", env.Type)
		panic(handlerPanic) // propagate to goroutine-level recovery for stack logging
	}

	if handlerErr != nil {
		span.RecordError(handlerErr)
		span.SetStatus(codes.Error, handlerErr.Error())
		if c.logger != nil {
			c.logger.Warn("sqs: handler returned error — message will be retried", map[string]any{
				"message_id": aws.ToString(msg.MessageId),
				"event_type": env.Type,
				"tenant_id":  env.TenantID,
				"event_id":   env.ID,
				"error":      handlerErr.Error(),
			})
		}
		// Do not delete — leave visible for retry.
		metrics.IncFailed(c.queueURL, env.Type, "handler_error")
		if timedOut() {
			metrics.IncTimeout(c.queueURL, env.Type, "handler")
		}
		metrics.IncRetry("consume", env.Type)
	} else {
		span.SetStatus(codes.Ok, "")
		settled = c.settle(msg, ext)
		if !handlerAttribution.Recorded() {
			metrics.IncProcessed(c.queueURL, env.Type)
		}
	}
	metrics.ObserveProcessingDuration(c.queueURL, env.Type, dur)
	return settled
}

// Extender states: a received message is waiting for a worker, dispatched
// to one, or released back to the queue — each transition happens once.
const (
	extWaiting int32 = iota
	extDispatched
	extReleased
)

// visibilityExtender keeps one received message hidden until stopped.
type visibilityExtender struct {
	stop     func()
	release  func()       // best-effort ChangeMessageVisibility(0)
	deadline atomic.Int64 // UnixNano; 0 = no deadline
	state    atomic.Int32
}

// Stop ends the extension and waits for its goroutine. Safe to call more
// than once and on a nil extender.
func (e *visibilityExtender) Stop() {
	if e != nil && e.stop != nil {
		e.stop()
	}
}

// SetDeadline stops extending once t has passed (WithHandlerTimeout).
func (e *visibilityExtender) SetDeadline(t time.Time) {
	if e != nil {
		e.deadline.Store(t.UnixNano())
	}
}

// Claim marks a waiting message as dispatched; false if it was already
// released back to the queue (and must not be processed).
func (e *visibilityExtender) Claim() bool {
	return e.state.CompareAndSwap(extWaiting, extDispatched)
}

// Release hands a message that was never dispatched back to the queue
// (visibility 0) and stops extending it. No-op once dispatched.
func (e *visibilityExtender) Release() {
	if !e.state.CompareAndSwap(extWaiting, extReleased) {
		return
	}
	e.Stop()
	if e.release != nil {
		e.release()
	}
}

// extendVisibility keeps msg hidden from receipt until Stop: every
// max(visibilityTimeout/2, 1s) it resets the visibility timeout — while the
// message waits for a worker and while it is processed. Once a deadline set
// with SetDeadline passes it stops; a message still waiting for a worker at
// that point is released back to the queue.
func (c *sqsConsumer) extendVisibility(msg sqstypes.Message) *visibilityExtender {
	ext := &visibilityExtender{}
	if msg.ReceiptHandle != nil {
		ext.release = func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := c.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
				QueueUrl:          aws.String(c.queueURL),
				ReceiptHandle:     msg.ReceiptHandle,
				VisibilityTimeout: 0,
			}); err != nil && c.logger != nil {
				c.logger.Warn("sqs: could not release message back to the queue; it reappears after its visibility timeout", map[string]any{
					"message_id": aws.ToString(msg.MessageId),
					"error":      err.Error(),
				})
			}
		}
	}
	if c.visibilityTimeout <= 0 || msg.ReceiptHandle == nil {
		return ext
	}
	// Clamp to minimum 1s: int32 truncation would produce 0 for sub-second
	// durations, which would make the message immediately re-visible.
	secs := max(int32(c.visibilityTimeout.Seconds()), 1)
	// extCtx is derived from context.Background() (not drainCtx) so visibility
	// extensions continue until dispatch completes, not until drain fires.
	extCtx, extCancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var once sync.Once
	ext.stop = func() {
		once.Do(func() {
			extCancel()
			<-done
		})
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(max(c.visibilityTimeout/2, time.Second))
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// With WithHandlerTimeout, stop extending once it has passed: a
				// hung handler — or a message stuck behind hung handlers — must
				// not stay invisible (and out of redrive) forever.
				if d := ext.deadline.Load(); d != 0 && time.Now().UnixNano() > d {
					if ext.state.CompareAndSwap(extWaiting, extReleased) {
						if c.logger != nil {
							c.logger.Warn("sqs: message waited past the handler timeout without a free worker — released back to the queue", map[string]any{
								"message_id": aws.ToString(msg.MessageId),
								"timeout":    c.handlerTimeout.String(),
							})
						}
						if ext.release != nil {
							ext.release()
						}
						return
					}
					if c.logger != nil {
						c.logger.Warn("sqs: handler timeout passed — no longer extending visibility; the message will be redelivered", map[string]any{
							"message_id": aws.ToString(msg.MessageId),
							"timeout":    c.handlerTimeout.String(),
						})
					}
					return
				}
				visStart := time.Now()
				// Bound each call below the tick so one hung call cannot let
				// the visibility lapse before the next attempt.
				callCtx, callCancel := context.WithTimeout(extCtx, extendCallTimeout(c.visibilityTimeout))
				_, err := c.client.ChangeMessageVisibility(callCtx, &sqs.ChangeMessageVisibilityInput{
					QueueUrl:          aws.String(c.queueURL),
					ReceiptHandle:     msg.ReceiptHandle,
					VisibilityTimeout: secs,
				})
				callCancel()
				if extCtx.Err() != nil { // cancelled because dispatch finished
					return
				}
				metrics.ObserveDependency("sqs", "change_message_visibility", err, time.Since(visStart))
				if err != nil {
					if c.logger != nil {
						// A failed extension means the message may become visible again
						// while the handler is still running, causing duplicate delivery.
						// Handlers are required to be idempotent, so this is not fatal —
						// but it is worth surfacing when debugging duplicate processing.
						c.logger.Warn("sqs: failed to extend message visibility — possible duplicate delivery", map[string]any{
							"message_id": aws.ToString(msg.MessageId),
							"error":      err.Error(),
						})
					}
				}
			case <-extCtx.Done():
				return
			}
		}
	}()
	return ext
}

// extendCallTimeout bounds one ChangeMessageVisibility extension call: below
// the extension tick (max(VT/2, 1s)), and at most 10s.
func extendCallTimeout(visibilityTimeout time.Duration) time.Duration {
	return min(max(visibilityTimeout/2, time.Second), 10*time.Second)
}

// receiveInput returns the ReceiveMessage input for the next call. Without a
// visibility timeout there is no extension, so a message received while every
// worker is busy would wait on the queue's own visibility timeout and could
// reappear mid-wait: then ask only for as many messages as there are free
// workers (waiting for one first). With a visibility timeout the whole batch
// is taken — waiting messages are extended from receipt.
func (c *sqsConsumer) receiveInput(base *sqs.ReceiveMessageInput, sem chan struct{}, loopCtx context.Context) *sqs.ReceiveMessageInput {
	if c.visibilityTimeout > 0 {
		return base
	}
	select {
	case sem <- struct{}{}:
		<-sem
	case <-loopCtx.Done():
		return base // the receive is cancelled with loopCtx anyway
	}
	input := *base
	input.MaxNumberOfMessages = min(c.maxMessages, int32(cap(sem)-len(sem)))
	if c.fifo {
		// A FIFO group's later messages wait on one worker for the earlier
		// ones; without extension they could reappear and be taken (out of
		// order) by another consumer. One message per receive never waits.
		input.MaxNumberOfMessages = 1
	}
	return &input
}

// decode runs the codec on a context that keeps parent's values, survives
// Stop() (an in-flight decode completes, like the handler) and is cancelled
// by the drain deadline. A codec panic is returned as a decode error.
func (c *sqsConsumer) decode(drainCtx, parent context.Context, schemaID string, payload json.RawMessage) (out json.RawMessage, err error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	stopDrain := context.AfterFunc(drainCtx, cancel)
	defer stopDrain()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("codec decode panic: %v", r)
			if c.logger != nil {
				c.logger.Error("sqs: codec decode panic recovered", map[string]any{
					"queue": c.queueURL, "panic": fmt.Sprintf("%v", r), "stack": string(debug.Stack()),
				})
			}
		}
	}()
	return decodeCodecPayload(ctx, c.codec, schemaID, payload)
}

// runDeadLetterHandler calls the dead-letter handler, returning a panic as an
// error so the message is counted as failed and left visible for retry.
func (c *sqsConsumer) runDeadLetterHandler(ctx context.Context, env domain.Envelope[json.RawMessage], msg sqstypes.Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("dead-letter handler panic: %v", r)
			if c.logger != nil {
				c.logger.Error("sqs: dead-letter handler panic recovered", map[string]any{
					"message_id": aws.ToString(msg.MessageId), "panic": fmt.Sprintf("%v", r), "stack": string(debug.Stack()),
				})
			}
		}
	}()
	return c.deadLetterHandler(ctx, env)
}

// HandlerContext returns parent carrying what every consumer handler gets:
// the tenant as platform-pgcommon's GUC set (so pool queries are RLS-scoped
// to the event's tenant), the envelope's trace ID (events.TraceIDFromContext)
// and the source message (events.SourceMessageFromContext). Shared with
// mock.Consumer so service tests see the same context as production.
func HandlerContext(parent context.Context, tenantID, traceID string, source func() port.SourceMessage) context.Context {
	ctx := pgcommon.WithGUCSet(parent, pgdomain.GUCSet{TenantID: tenantID})
	ctx = port.WithEnvelopeTraceID(ctx, traceID)
	return port.WithSourceMessage(ctx, source)
}

// missingEnvelopeFields reports a decoded body that lacks an envelope's
// required id, type or source.
func missingEnvelopeFields(env domain.Envelope[json.RawMessage]) error {
	var missing []string
	if env.ID == "" {
		missing = append(missing, "id")
	}
	if env.Type == "" {
		missing = append(missing, "type")
	}
	if env.Source == "" {
		missing = append(missing, "source")
	}
	if len(missing) == 0 {
		// The tenant and trace ID become the handler's pgcommon GUC set,
		// which rejects a NUL (pgcommon v1.5.1 ErrInvalidGUCValue): every
		// database call of the handler would fail on every delivery.
		if strings.ContainsRune(env.TenantID, '\x00') || strings.ContainsRune(env.TraceID, '\x00') {
			return errors.New("invalid event envelope: tenant_id or trace_id contains a NUL character")
		}
		return nil
	}
	return fmt.Errorf("not an event envelope: missing %s (an SNS subscription without RawMessageDelivery delivers a notification wrapper)", strings.Join(missing, ", "))
}

// handleMalformed disposes of a body that is not a valid envelope. It will
// never parse, so it must leave the queue: forwarded to the DLQ when one is
// configured (deleted only once the forward succeeds), otherwise deleted to
// avoid an infinite retry loop. The body is logged only as its size and
// SHA-256 unless WithMalformedBodyLogging is set — it may carry tenant data.
func (c *sqsConsumer) handleMalformed(drainCtx context.Context, msg sqstypes.Message, ext *visibilityExtender, body string, parseErr error) bool {
	if c.logger != nil {
		sum := sha256.Sum256([]byte(body))
		fields := map[string]any{
			"message_id":  aws.ToString(msg.MessageId),
			"error":       parseErr.Error(),
			"queue":       c.queueURL,
			"body_bytes":  len(body),
			"body_sha256": hex.EncodeToString(sum[:]),
		}
		if c.logMalformedBodies {
			excerpt := body
			if len(excerpt) > 512 {
				excerpt = excerpt[:512] + "...[truncated]"
			}
			fields["body"] = excerpt
		}
		c.logger.Error("sqs: message body is not a valid event envelope", fields)
	}
	metrics.IncFailed(c.queueURL, "unknown", "malformed")
	if c.dlq != nil && !c.forwardToDLQ(drainCtx, context.Background(), msg, "unknown", "malformed", "malformed message body: "+parseErr.Error()) {
		metrics.IncRetry("consume", "unknown")
		return false
	}
	return c.settle(msg, ext)
}

// settle stops msg's visibility extension — so no extension call races the
// delete — and deletes it. It reports whether the delete succeeded: a failed
// delete (logged) leaves the message to be redelivered, so on a FIFO queue
// its group must stop — running the next message now would reorder them.
func (c *sqsConsumer) settle(msg sqstypes.Message, ext *visibilityExtender) bool {
	ext.Stop()
	return c.deleteMessage(msg) == nil
}

// errDLQForwardFailed marks a dead-letter routing attempt whose DLQ forward
// failed; forwardToDLQ has already logged the cause.
var errDLQForwardFailed = errors.New("sqs: forward to DLQ failed")

// maxDLQForwardTimeout bounds a single DLQ forward (resolution + SendMessage).
const maxDLQForwardTimeout = 30 * time.Second

// dlqForwardTimeout returns the forward deadline: maxDLQForwardTimeout, capped
// at half the configured visibility timeout (minimum 1s), so a forward never
// outlives the message's visibility even if an extension call fails.
func (c *sqsConsumer) dlqForwardTimeout() time.Duration {
	if c.visibilityTimeout > 0 {
		return max(min(maxDLQForwardTimeout, c.visibilityTimeout/2), time.Second)
	}
	return maxDLQForwardTimeout
}

// forwardToDLQ forwards msg, as received, to the DLQ and reports success.
// parent supplies context values (trace, baggage); its cancellation is ignored
// so a forward in flight at Stop() completes, bounded by dlqForwardTimeout and
// the drain deadline. reasonCode (a DLQReasonValues entry) attributes the
// forward in platform_dlq_messages_total; reason is the DLQReason text.
func (c *sqsConsumer) forwardToDLQ(drainCtx, parent context.Context, msg sqstypes.Message, eventType, reasonCode, reason string) bool {
	src := sourceMessage(c.queueURL, msg)
	parent, attribution := port.WithDLQAttribution(parent, reasonCode)
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), c.dlqForwardTimeout())
	defer cancel()
	stopDrain := context.AfterFunc(drainCtx, cancel)
	defer stopDrain()

	if err := c.dlq.SendToDLQ(ctx, src.QueueURL, src.Body, src.Attributes, reason); err != nil {
		if c.logger != nil {
			c.logger.Error("sqs: DLQ forward failed — message left visible for retry", map[string]any{
				"message_id": src.MessageID,
				"queue":      c.queueURL,
				"reason":     reason,
				"error":      err.Error(),
			})
		}
		return false
	}
	// A DLQPublisher other than the SQS one does not count the message itself.
	if !attribution.Recorded() {
		metrics.IncDLQ("consume", eventType, reasonCode)
	}
	return true
}

// sourceMessage captures msg as received, for WithSourceMessage and DLQ
// forwarding. Binary message attributes are omitted.
func sourceMessage(queueURL string, msg sqstypes.Message) port.SourceMessage {
	var attrs map[string]string
	for k, v := range msg.MessageAttributes {
		if v.StringValue == nil || strings.HasPrefix(aws.ToString(v.DataType), "Binary") {
			continue
		}
		if attrs == nil {
			attrs = make(map[string]string, len(msg.MessageAttributes))
		}
		attrs[k] = *v.StringValue
	}
	return port.SourceMessage{
		QueueURL:     queueURL,
		MessageID:    aws.ToString(msg.MessageId),
		Body:         []byte(aws.ToString(msg.Body)),
		Attributes:   attrs,
		ReceiveCount: approxReceiveCount(msg.Attributes),
	}
}

// decodeCodecPayload reverses the SNS publisher's codec-encode step. Returns
// an error if the message requires a codec (non-empty schemaID) but none is
// configured on this consumer.
func decodeCodecPayload(ctx context.Context, codec port.Codec, schemaID string, payload json.RawMessage) (json.RawMessage, error) {
	if codec == nil {
		return nil, fmt.Errorf("sqs: message has schema_id %q but no Codec is configured (see WithCodec)", schemaID)
	}
	raw, err := domain.UnwrapCodecPayload(payload)
	if err != nil {
		return nil, err
	}
	decoded, err := codec.Decode(ctx, schemaID, raw)
	if err != nil {
		return nil, fmt.Errorf("sqs: codec Decode failed: %w", err)
	}
	return decoded, nil
}

// isJSONString reports whether payload is a JSON string (its first
// non-whitespace byte is a quote) — the codec wire format.
func isJSONString(payload json.RawMessage) bool {
	for _, b := range payload {
		switch b {
		case ' ', '\t', '\n', '\r':
			continue
		}
		return b == '"'
	}
	return false
}

// approxReceiveCount parses the ApproximateReceiveCount system attribute from SQS.
func approxReceiveCount(attrs map[string]string) int {
	s, ok := attrs[string(sqstypes.MessageSystemAttributeNameApproximateReceiveCount)]
	if !ok {
		return 0
	}
	n, _ := strconv.Atoi(s)
	return n
}

const deleteMessageTimeout = 10 * time.Second

// deleteMessage deletes a processed message from SQS. It uses its own bounded
// context so a network partition cannot hold a goroutine slot indefinitely.
func (c *sqsConsumer) deleteMessage(msg sqstypes.Message) error {
	if msg.ReceiptHandle == nil {
		if c.logger != nil {
			c.logger.Error("sqs: cannot delete message with nil ReceiptHandle — skipping", map[string]any{
				"message_id": aws.ToString(msg.MessageId),
			})
		}
		return errors.New("sqs: message has no receipt handle")
	}
	ctx, cancel := context.WithTimeout(context.Background(), deleteMessageTimeout)
	defer cancel()
	start := time.Now()
	_, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.queueURL),
		ReceiptHandle: msg.ReceiptHandle,
	})
	metrics.ObserveDependency("sqs", "delete_message", err, time.Since(start))
	if err != nil {
		if c.logger != nil {
			c.logger.Error("sqs: failed to delete message", map[string]any{
				"message_id": aws.ToString(msg.MessageId),
				"error":      err.Error(),
			})
		}
	}
	return err
}
