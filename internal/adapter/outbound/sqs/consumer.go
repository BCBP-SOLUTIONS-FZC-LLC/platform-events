// Package sqs provides an AWS SQS implementation of port.Consumer.
package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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
	return func(c *sqsConsumer) { c.deadLetterHandler = fn }
}

// WithMaxReceiveCount sets the ApproximateReceiveCount threshold at which a message
// is routed to the dead-letter handler instead of the normal handler.
// Requires WithDeadLetterHandler to be set; 0 disables dead-letter routing.
func WithMaxReceiveCount(n int) ConsumerOption {
	return func(c *sqsConsumer) { c.maxReceiveCount = n }
}

// WithDrainTimeout sets how long Stop() waits for in-flight handlers to finish.
func WithDrainTimeout(d time.Duration) ConsumerOption {
	return func(c *sqsConsumer) { c.drainTimeout = d }
}

// Config holds the parameters for constructing an SQS consumer.
//
// SNS subscription requirement: if this queue receives messages via an SNS
// subscription, the subscription MUST have RawMessageDelivery=true. Without it,
// SNS wraps each message in a notification envelope that the consumer cannot
// parse — messages are permanently deleted as malformed with no retry.
type Config struct {
	// QueueURL is required.
	QueueURL    string
	Region      string
	EndpointURL string // optional — set to LocalStack URL for testing
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
	drainTimeout      time.Duration

	maxReceiveCount int
	cancelFn        context.CancelFunc
	wg              sync.WaitGroup
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
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), awsOpts...)
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
	}
	for _, opt := range opts {
		opt(c)
	}

	// If a dead-letter handler is registered but no receive-count threshold was
	// set, apply a safe default so the handler is actually invoked.
	if c.deadLetterHandler != nil && c.maxReceiveCount == 0 {
		c.maxReceiveCount = 5
		if c.logger != nil {
			c.logger.Warn("sqs: WithDeadLetterHandler set without WithMaxReceiveCount; defaulting maxReceiveCount to 5", nil)
		}
	}

	return c, nil
}

// Start begins the SQS long-poll receive loop. Blocks until ctx is cancelled.
// When Start returns, all in-flight handlers have completed (or the drain
// timeout has been exceeded). Stop() blocks until Start returns.
// The consumer is fully restartable: Start() may be called again after Stop().
func (c *sqsConsumer) Start(ctx context.Context) error {
	// Create a fresh doneCh for this cycle before entering the loop.
	// All fields written here are protected by c.mu; Stop() reads doneCh under
	// the same lock, guaranteeing it sees the channel for the current cycle.
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return fmt.Errorf("sqs: consumer is already running")
	}
	loopCtx, cancel := context.WithCancel(ctx)
	c.cancelFn = cancel
	c.running = true
	thisDoneCh := make(chan struct{})
	c.doneCh = thisDoneCh
	c.mu.Unlock()

	// Reset running and signal Stop() when this Start() goroutine exits.
	defer func() {
		c.mu.Lock()
		c.running = false
		c.mu.Unlock()
		close(thisDoneCh)
	}()

	// drain waits for all in-flight handler goroutines to finish.
	drain := func() {
		done := make(chan struct{})
		go func() {
			c.wg.Wait()
			close(done)
		}()
		drainCtx, drainCancel := context.WithTimeout(context.Background(), c.drainTimeout)
		defer drainCancel()
		select {
		case <-done:
		case <-drainCtx.Done():
			if c.logger != nil {
				c.logger.Warn("sqs: drain timeout exceeded; some handlers may not have completed", nil)
			}
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

	for {
		select {
		case <-loopCtx.Done():
			drain()
			return nil
		default:
		}

		out, err := c.client.ReceiveMessage(loopCtx, receiveInput)
		if err != nil {
			select {
			case <-loopCtx.Done():
				// Context was cancelled while ReceiveMessage was in-flight.
				// Drain handlers before returning — same as the top-of-loop path.
				drain()
				return nil
			default:
			}
			if c.logger != nil {
				c.logger.Error("sqs: receive message failed", map[string]any{"error": err.Error()})
			}
			// Interruptible backoff: wake immediately if Stop() is called rather
			// than blocking for up to receiveBackoffMax (30s) and missing a shutdown.
			select {
			case <-time.After(receiveBackoff):
			case <-loopCtx.Done():
				drain()
				return nil
			}
			receiveBackoff = min(receiveBackoff*2, receiveBackoffMax)
			continue
		}
		receiveBackoff = receiveBackoffInit // reset on success

		for _, msg := range out.Messages {
			// Interruptible semaphore acquire: if loopCtx is cancelled while all
			// concurrency slots are busy the blocking send would hold Start() forever,
			// preventing drain() from ever being called and deadlocking Stop().
			select {
			case sem <- struct{}{}:
			case <-loopCtx.Done():
				drain()
				return nil
			}
			c.wg.Go(func() {
				defer func() { <-sem }()
				defer func() {
					if r := recover(); r != nil {
						if c.logger != nil {
							c.logger.Error("sqs: handler panic recovered", map[string]any{
								"message_id": aws.ToString(msg.MessageId),
								"panic":      fmt.Sprintf("%v", r),
							})
						}
					}
				}()
				c.dispatch(loopCtx, msg)
			})
		}
	}
}

// Stop cancels the receive loop and waits for all in-flight handlers to finish
// (up to DrainTimeout). Returns once Start has returned.
// Safe to call multiple times and safe to call before Start.
func (c *sqsConsumer) Stop() error {
	c.mu.Lock()
	if c.cancelFn != nil {
		c.cancelFn()
	}
	doneCh := c.doneCh // capture current cycle's channel under lock
	c.mu.Unlock()

	<-doneCh // pre-closed before first Start(), fresh channel during/after Start()
	return nil
}

func (c *sqsConsumer) dispatch(ctx context.Context, msg sqstypes.Message) {
	tracer := otel.Tracer("platform-events")

	var env domain.Envelope[json.RawMessage]
	body := aws.ToString(msg.Body)
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		if c.logger != nil {
			c.logger.Error("sqs: failed to unmarshal message body", map[string]any{
				"message_id": aws.ToString(msg.MessageId),
				"error":      err.Error(),
				"queue":      c.queueURL,
			})
		}
		// Delete malformed messages to avoid infinite retry loops.
		c.deleteMessage(msg)
		return
	}

	// Derive handlerCtx WITHOUT propagating loopCtx cancellation. When Stop()
	// fires it cancels loopCtx, but in-flight handlers must complete their DB
	// writes and downstream calls uninterrupted — the drain() call in Start()
	// gives them drainTimeout to finish. context.WithoutCancel preserves any
	// context values (OTel baggage, etc.) while stripping the cancel signal.
	handlerCtx := pgcommon.WithGUCSet(context.WithoutCancel(ctx), pgdomain.GUCSet{TenantID: env.TenantID})

	// Route to dead-letter handler when ApproximateReceiveCount reaches the threshold.
	// The message is deleted only if the dead-letter handler succeeds; on failure it
	// is left visible for retry, matching the semantics of the normal handler.
	if c.deadLetterHandler != nil && c.maxReceiveCount > 0 {
		if approxReceiveCount(msg.Attributes) >= c.maxReceiveCount {
			start := time.Now()
			dlhErr := c.deadLetterHandler(handlerCtx, env)
			dur := time.Since(start)

			dlhStatus := "success"
			if dlhErr != nil {
				dlhStatus = "error"
				if c.logger != nil {
					c.logger.Error("sqs: dead-letter handler failed — message left visible for retry", map[string]any{
						"message_id": aws.ToString(msg.MessageId),
						"event_type": env.Type,
						"tenant_id":  env.TenantID,
						"event_id":   env.ID,
						"error":      dlhErr.Error(),
					})
				}
			}
			// Emit the same consume metrics for DLH invocations so dashboards and
			// alerts can distinguish DLH activity from normal handler activity.
			if metrics.EventsConsumedTotal != nil {
				metrics.EventsConsumedTotal.WithLabelValues(c.queueURL, env.Type, "dlq_"+dlhStatus).Inc()
			}
			if metrics.EventsConsumeDuration != nil {
				metrics.EventsConsumeDuration.WithLabelValues(c.queueURL, env.Type).Observe(dur.Seconds())
			}

			if dlhErr != nil {
				return // do NOT delete — leave visible for retry
			}
			c.deleteMessage(msg)
			return
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
		propCtx := otel.GetTextMapPropagator().Extract(context.Background(), carrier)
		remoteSpan := oteltrace.SpanFromContext(propCtx)
		if remoteSpan.SpanContext().IsValid() {
			spanOpts = append(spanOpts, oteltrace.WithLinks(oteltrace.Link{SpanContext: remoteSpan.SpanContext()}))
		}
	}
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

	// Automatically extend the SQS visibility timeout so long-running handlers
	// are not re-delivered while still processing. Fires every visibilityTimeout/2
	// and stops when the handler returns.
	if c.visibilityTimeout > 0 {
		// Clamp to minimum 1s: int32 truncation would produce 0 for sub-second
		// durations, which would make the message immediately re-visible.
		secs := max(int32(c.visibilityTimeout.Seconds()), 1)
		extCtx, extCancel := context.WithCancel(context.Background())
		defer extCancel()
		go func() {
			half := max(c.visibilityTimeout/2, time.Second)
			ticker := time.NewTicker(half)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					if _, err := c.client.ChangeMessageVisibility(extCtx, &sqs.ChangeMessageVisibilityInput{
						QueueUrl:          aws.String(c.queueURL),
						ReceiptHandle:     msg.ReceiptHandle,
						VisibilityTimeout: secs,
					}); err != nil && c.logger != nil {
						// A failed extension means the message may become visible again
						// while the handler is still running, causing duplicate delivery.
						// Handlers are required to be idempotent, so this is not fatal —
						// but it is worth surfacing when debugging duplicate processing.
						c.logger.Warn("sqs: failed to extend message visibility — possible duplicate delivery", map[string]any{
							"message_id": aws.ToString(msg.MessageId),
							"error":      err.Error(),
						})
					}
				case <-extCtx.Done():
					return
				}
			}
		}()
	}

	start := time.Now()
	handlerErr := c.handler(handlerCtx, env)
	dur := time.Since(start)

	status := "success"
	if handlerErr != nil {
		status = "error"
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
	} else {
		c.deleteMessage(msg)
	}

	if metrics.EventsConsumedTotal != nil {
		metrics.EventsConsumedTotal.WithLabelValues(c.queueURL, env.Type, status).Inc()
	}
	if metrics.EventsConsumeDuration != nil {
		metrics.EventsConsumeDuration.WithLabelValues(c.queueURL, env.Type).Observe(dur.Seconds())
	}
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
func (c *sqsConsumer) deleteMessage(msg sqstypes.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), deleteMessageTimeout)
	defer cancel()
	_, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.queueURL),
		ReceiptHandle: msg.ReceiptHandle,
	})
	if err != nil && c.logger != nil {
		c.logger.Error("sqs: failed to delete message", map[string]any{
			"message_id": aws.ToString(msg.MessageId),
			"error":      err.Error(),
		})
	}
}
