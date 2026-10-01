package service

import (
	"github.com/google/uuid"

	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
)

// panicErr wraps a recovered panic value so publishClaimedSequential can
// distinguish it from a real SNS error without fragile string-prefix matching.
type panicErr struct{ msg string }

func (e *panicErr) Error() string { return e.msg }

const (
	// bookkeepTimeoutPerRecord is the per-record budget for MarkPublished/MarkFailed
	// bookkeeping writes. The total bookkeepCtx timeout = batchSize × this value so
	// that even a full default batch of 50 records has 500 ms each regardless of
	// whether the primary context is cancelled (e.g. graceful shutdown).
	bookkeepTimeoutPerRecord = 500 * time.Millisecond

	// maxLastErrorLen caps the last_error column length stored in outbox_events to
	// prevent unbounded table bloat from verbose AWS SDK or network error messages.
	maxLastErrorLen = 512

	// DefaultRetryBackoff and DefaultMaxRetryBackoff bound the delay before a
	// failed record is retried: base·2^(attempt-1), capped, with jitter.
	DefaultRetryBackoff    = 1 * time.Second
	DefaultMaxRetryBackoff = 5 * time.Minute
)

// OutboxService handles the transactional outbox publish cycle.
type OutboxService struct {
	store              port.OutboxStore
	publisher          port.Publisher
	logger             port.Logger
	clock              port.Clock
	outboxMetrics      port.OutboxMetrics
	maxAttempts        int
	publishConcurrency int
	publishTimeout     time.Duration
	retryBackoff       time.Duration
	maxRetryBackoff    time.Duration

	// Transient publish failures (transport errors, throttling, timeouts)
	// describe the publisher, not a record, so they share one backoff: an SNS
	// outage slows every retry instead of hammering SNS each poll. The streak
	// advances at most once per poll cycle (pollGen) — however many records
	// that cycle failed — and resets on the next successful publish.
	pollGen        atomic.Uint64
	transientMu    sync.Mutex
	transientGen   uint64
	transientCount int
}

// NewOutboxService creates an OutboxService with all required dependencies.
// If clock is nil, RealClock is used. publishConcurrency <= 0 defaults to 1
// (sequential). publishTimeout <= 0 disables per-record publish timeouts.
func NewOutboxService(
	store port.OutboxStore,
	publisher port.Publisher,
	logger port.Logger,
	clock port.Clock,
	maxAttempts int,
	publishConcurrency int,
	publishTimeout time.Duration,
) *OutboxService {
	if clock == nil {
		clock = port.RealClock{}
	}
	if publishConcurrency <= 0 {
		publishConcurrency = 1
	}
	return &OutboxService{
		store:              store,
		publisher:          publisher,
		logger:             logger,
		clock:              clock,
		maxAttempts:        maxAttempts,
		publishConcurrency: publishConcurrency,
		publishTimeout:     publishTimeout,
		retryBackoff:       DefaultRetryBackoff,
		maxRetryBackoff:    DefaultMaxRetryBackoff,
	}
}

// SetRetryBackoff sets the retry delay policy: a record's n-th failure delays
// its next attempt by base·2^(n-1), capped at maxBackoff, with jitter.
// Non-positive values keep the defaults (1s, 5m).
func (s *OutboxService) SetRetryBackoff(base, maxBackoff time.Duration) {
	if base > 0 {
		s.retryBackoff = base
	}
	if maxBackoff > 0 {
		s.maxRetryBackoff = maxBackoff
	}
	if s.maxRetryBackoff < s.retryBackoff {
		s.maxRetryBackoff = s.retryBackoff
	}
}

// backoff returns base·2^(n-1) capped at the maximum, with "equal jitter"
// (uniform in [d/2, d]) so records that failed together do not retry in
// lock-step.
func (s *OutboxService) backoff(n int) time.Duration {
	d := s.retryBackoff
	for i := 1; i < n && d < s.maxRetryBackoff; i++ {
		d *= 2
	}
	d = min(d, s.maxRetryBackoff)
	if half := d / 2; half > 0 {
		d = half + time.Duration(rand.Int64N(int64(half)+1))
	}
	return d
}

// SetOutboxMetrics injects the metrics observer. Call once after construction
// from the adapter layer (e.g. runner.go) so the service layer stays clean.
// Safe to call with nil — metric recording is silently skipped.
func (s *OutboxService) SetOutboxMetrics(m port.OutboxMetrics) {
	s.outboxMetrics = m
}

// Enqueue inserts a serialized envelope into the outbox within the given transaction.
// The caller controls the transaction boundary — this function only does the INSERT.
func (s *OutboxService) Enqueue(ctx context.Context, tx pgcommon.Tx, env domain.Envelope[json.RawMessage]) error {
	if tx == nil {
		return fmt.Errorf("outbox: transaction must not be nil — use pgcommon.RunInTx to obtain a transaction")
	}
	if env.ID == "" {
		return domain.ErrEnvelopeIDRequired
	}
	if env.Type == "" {
		return domain.ErrEnvelopeTypeRequired
	}
	if env.Source == "" {
		return domain.ErrEnvelopeSourceRequired
	}
	if env.Timestamp.IsZero() {
		return fmt.Errorf("outbox: Enqueue requires a non-zero Timestamp — use events.NewEnvelope to construct envelopes")
	}
	if strings.ContainsRune(env.ID, '\x00') || strings.ContainsRune(env.Type, '\x00') || strings.ContainsRune(env.Source, '\x00') {
		return fmt.Errorf("outbox: envelope fields (ID, Type, Source) must not contain null bytes")
	}
	// The ID is stored in a uuid column and read back in canonical form, while
	// publish failures are reported under the ID in the payload: anything but
	// the canonical spelling would make a failed publish look delivered.
	if id, err := uuid.Parse(env.ID); err != nil || id.String() != env.ID {
		return fmt.Errorf("outbox: envelope ID %q must be a canonical lowercase UUID — use events.NewEnvelope to construct envelopes", env.ID)
	}
	b, err := json.Marshal(env)
	if err != nil {
		return err
	}
	const maxEnvelopeBytes = 240 * 1024
	if len(b) > maxEnvelopeBytes {
		return fmt.Errorf("outbox: serialised envelope is %d bytes — exceeds safe SNS limit (%d bytes); reduce payload size", len(b), maxEnvelopeBytes)
	}
	record := domain.OutboxRecord{
		ID:          env.ID,
		EventType:   env.Type,
		Payload:     b,
		TenantID:    env.TenantID,
		TraceID:     env.TraceID,
		CreatedAt:   s.clock.Now(),
		ScheduledAt: s.clock.Now(),
	}
	return s.store.Enqueue(ctx, tx, record)
}

// PublishBatch claims up to batchSize unpublished records and publishes them
// with bounded concurrency (publishConcurrency). Per-record errors are logged
// and marked as failed; the batch continues. Returns ctx.Err() if the context
// is cancelled mid-batch so the runner can distinguish shutdown from DB errors.
func (s *OutboxService) PublishBatch(ctx context.Context, batchSize int) error {
	s.beginPoll()
	records, err := s.store.ClaimBatch(ctx, batchSize)
	if err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}

	if s.publishConcurrency == 1 {
		return s.publishClaimedSequential(ctx, records)
	}

	// bookkeepCtx is used exclusively for MarkPublished and MarkFailed so these
	// critical bookkeeping writes succeed even when ctx is cancelled by graceful
	// shutdown. The timeout must cover the full worst-case publish duration PLUS
	// the per-record bookkeeping write time. Without the publish budget,
	// MarkPublished calls for later records expire while earlier goroutines are
	// still publishing, causing spurious duplicate delivery on the next poll.
	//
	// With publishConcurrency=C and publishTimeout=T per record:
	//   sequential rounds = ceil(len(records) / C)
	//   publish budget    = rounds × T
	//   bookkeep budget   = (len(records)+1) × bookkeepTimeoutPerRecord
	var publishBudget time.Duration
	if s.publishTimeout > 0 {
		rounds := (len(records) + s.publishConcurrency - 1) / s.publishConcurrency
		publishBudget = time.Duration(rounds) * s.publishTimeout
	} else {
		// No per-record timeout; publishing can take arbitrarily long.
		// Use a 5-minute floor so bookkeeping writes succeed even under extreme
		// SNS latency — callers who disable the timeout accept slower shutdowns.
		publishBudget = 5 * time.Minute
	}
	bookkeepTimeout := publishBudget + time.Duration(len(records)+1)*bookkeepTimeoutPerRecord
	bookkeepCtx, bookkeepCancel := context.WithTimeout(context.Background(), bookkeepTimeout)
	defer bookkeepCancel()

	sem := make(chan struct{}, s.publishConcurrency)
	var wg sync.WaitGroup

	for _, rec := range records {
		if ctx.Err() != nil {
			// Context already cancelled — release the claim lease without
			// publishing or counting an attempt.
			s.releaseStranded(bookkeepCtx, rec, "batch interrupted by context cancellation")
			continue
		}

		// Interruptible semaphore acquire: a plain channel send would block until a
		// slot frees even if ctx is cancelled, delaying shutdown beyond DrainTimeout.
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			s.releaseStranded(bookkeepCtx, rec, "batch interrupted by context cancellation")
			continue
		}
		wg.Go(func() {
			defer func() { <-sem }()
			defer func() {
				if r := recover(); r != nil {
					if s.logger != nil {
						s.logger.Error("outbox: publish goroutine panic recovered", map[string]any{
							"id":         rec.ID,
							"event_type": rec.EventType,
							"panic":      fmt.Sprintf("%v", r),
							"stack":      string(debug.Stack()),
						})
					}
					// A panic is a programming error: it counts toward the retry
					// budget so a poison record cannot hold a lease forever.
					s.markFailed(bookkeepCtx, rec, fmt.Sprintf("panic: %v", r))
				}
			}()
			s.publishRecord(ctx, bookkeepCtx, rec)
		})
	}

	wg.Wait()
	return ctx.Err() // nil on success; context error if cancelled during publish
}

// publishClaimedSequential publishes a claimed batch via Publisher.PublishBatch
// (SNS batch API, up to 10 per chunk) when publishConcurrency is 1.
func (s *OutboxService) publishClaimedSequential(ctx context.Context, records []domain.OutboxRecord) error {
	bookkeepCtx, bookkeepCancel := s.newBookkeepCtx(len(records))
	defer bookkeepCancel()

	type item struct {
		rec domain.OutboxRecord
		env domain.Envelope[json.RawMessage]
	}
	items := make([]item, 0, len(records))

	for _, rec := range records {
		if ctx.Err() != nil {
			s.releaseStranded(bookkeepCtx, rec, "batch interrupted by context cancellation")
			continue
		}
		var env domain.Envelope[json.RawMessage]
		if err := json.Unmarshal(rec.Payload, &env); err != nil {
			s.handleUnmarshalError(bookkeepCtx, rec, err)
			continue
		}
		items = append(items, item{rec: rec, env: env})
	}
	if len(items) == 0 {
		return ctx.Err()
	}

	envs := make([]domain.Envelope[json.RawMessage], len(items))
	for i, it := range items {
		envs[i] = it.env
	}

	pubCtx := ctx
	var pubCancel context.CancelFunc
	if s.publishTimeout > 0 {
		pubCtx, pubCancel = context.WithTimeout(ctx, time.Duration(len(items))*s.publishTimeout)
		defer pubCancel()
	}

	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				if s.logger != nil {
					s.logger.Error("outbox: publish batch panic recovered", map[string]any{
						"panic": fmt.Sprintf("%v", r),
						"stack": string(debug.Stack()),
					})
				}
				for _, it := range items {
					s.markFailed(bookkeepCtx, it.rec, fmt.Sprintf("panic: %v", r))
				}
				err = &panicErr{msg: fmt.Sprintf("panic: %v", r)}
			}
		}()
		err = s.publisher.PublishBatch(pubCtx, envs)
	}()
	if err == nil {
		s.resetTransient()
		for _, it := range items {
			s.markPublished(bookkeepCtx, it.rec, it.env)
		}
		return ctx.Err()
	}
	var pe *panicErr
	if errors.As(err, &pe) {
		return ctx.Err()
	}

	var batchErr *domain.BatchError
	if errors.As(err, &batchErr) {
		type failInfo struct {
			msg       string
			transient bool
		}
		failed := make(map[string]failInfo, len(batchErr.Failures))
		anyTransient := false
		for _, f := range batchErr.Failures {
			// Retryable failures (throttling, service-side errors, timeouts —
			// Code "TransportError" for publishers that predate the flag) are
			// retried without consuming an attempt; anything else counts.
			transient := f.Retryable || f.Code == "TransportError"
			anyTransient = anyTransient || transient
			failed[canonicalID(f.ID)] = failInfo{msg: truncateError(f.Message), transient: transient}
		}
		if len(failed) < len(items) {
			s.resetTransient() // something got through
		}
		var transientDelay time.Duration
		if anyTransient {
			transientDelay = s.nextTransientDelay()
		}
		for _, it := range items {
			fi, ok := failed[canonicalID(it.rec.ID)]
			switch {
			case !ok:
				s.markPublished(bookkeepCtx, it.rec, it.env)
			case fi.transient:
				s.releaseLease(bookkeepCtx, it.rec, fi.msg, transientDelay)
			default:
				s.markFailed(bookkeepCtx, it.rec, fi.msg)
			}
		}
		return ctx.Err()
	}

	s.handlePublishError(ctx, bookkeepCtx, err, recordsOf(items, func(it item) domain.OutboxRecord { return it.rec }))
	return ctx.Err()
}

// canonicalID returns id in canonical UUID form when it parses, else id.
// Failures are reported under the payload's envelope ID while rec.ID comes
// back canonicalised from the uuid column; rows written before Enqueue
// required canonical IDs (or replayed from the dead-letter table) can differ
// in case or punctuation, and an unmatched failure would be marked published.
func canonicalID(id string) string {
	if u, err := uuid.Parse(id); err == nil {
		return u.String()
	}
	return id
}

func recordsOf[T any](items []T, rec func(T) domain.OutboxRecord) []domain.OutboxRecord {
	out := make([]domain.OutboxRecord, len(items))
	for i, it := range items {
		out[i] = rec(it)
	}
	return out
}

// handlePublishError settles records whose publish failed with err. Shutdown
// releases the lease immediately; transient failures (timeouts, ErrRetryable)
// release it after the shared transient backoff; anything else counts an
// attempt and backs off per record.
func (s *OutboxService) handlePublishError(ctx, bookkeepCtx context.Context, err error, recs []domain.OutboxRecord) {
	msg := truncateError(err.Error())
	switch {
	case ctx.Err() != nil:
		for _, rec := range recs {
			s.releaseStranded(bookkeepCtx, rec, "publish interrupted by shutdown: "+msg)
		}
	case isTransient(err):
		d := s.nextTransientDelay()
		for _, rec := range recs {
			s.releaseLease(bookkeepCtx, rec, msg, d)
		}
	default:
		for _, rec := range recs {
			s.markFailed(bookkeepCtx, rec, msg)
		}
	}
}

// isTransient reports whether a publish error describes the publisher (timeout,
// throttling, outage) rather than the record.
func isTransient(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, domain.ErrRetryable)
}

// nextTransientDelay returns the shared transient backoff, advancing the
// streak on the first transient failure of each poll cycle only.
func (s *OutboxService) nextTransientDelay() time.Duration {
	gen := s.pollGen.Load()
	s.transientMu.Lock()
	if s.transientGen != gen || s.transientCount == 0 {
		s.transientGen = gen
		s.transientCount++
	}
	n := s.transientCount
	s.transientMu.Unlock()
	return s.backoff(n)
}

func (s *OutboxService) resetTransient() {
	s.transientMu.Lock()
	s.transientCount = 0
	s.transientMu.Unlock()
}

// beginPoll starts a poll cycle for the transient backoff.
func (s *OutboxService) beginPoll() { s.pollGen.Add(1) }

func (s *OutboxService) newBookkeepCtx(recordCount int) (context.Context, context.CancelFunc) {
	var publishBudget time.Duration
	if s.publishTimeout > 0 {
		rounds := (recordCount + s.publishConcurrency - 1) / s.publishConcurrency
		publishBudget = time.Duration(rounds) * s.publishTimeout
	} else {
		publishBudget = 5 * time.Minute
	}
	bookkeepTimeout := publishBudget + time.Duration(recordCount+1)*bookkeepTimeoutPerRecord
	return context.WithTimeout(context.Background(), bookkeepTimeout)
}

// releaseStranded hands a claimed-but-unpublished record back on shutdown:
// claimable immediately, no attempt counted.
func (s *OutboxService) releaseStranded(bookkeepCtx context.Context, rec domain.OutboxRecord, reason string) {
	s.releaseLease(bookkeepCtx, rec, reason, 0)
}

func (s *OutboxService) releaseLease(bookkeepCtx context.Context, rec domain.OutboxRecord, reason string, retryAfter time.Duration) {
	if err := s.store.ReleaseLease(bookkeepCtx, rec.ID, reason, retryAfter); err != nil && s.logger != nil {
		s.logger.Error("outbox: failed to release record lease", map[string]any{
			"id": rec.ID, "error": err.Error(),
		})
	}
}

func (s *OutboxService) handleUnmarshalError(bookkeepCtx context.Context, rec domain.OutboxRecord, err error) {
	if s.outboxMetrics != nil {
		s.outboxMetrics.RecordUnmarshalError()
	}
	if s.logger != nil {
		s.logger.Error("outbox: failed to unmarshal payload", map[string]any{
			"id": rec.ID, "event_type": rec.EventType, "error": err.Error(),
		})
	}
	s.markFailed(bookkeepCtx, rec, truncateError(err.Error()))
}

func (s *OutboxService) markPublished(bookkeepCtx context.Context, rec domain.OutboxRecord, env domain.Envelope[json.RawMessage]) {
	if err := s.store.MarkPublished(bookkeepCtx, rec.ID); err != nil {
		if s.outboxMetrics != nil {
			s.outboxMetrics.RecordMarkPublishedError()
		}
		if s.logger != nil {
			s.logger.Error("outbox: failed to mark published — event delivered to SNS but may be re-delivered on next poll", map[string]any{
				"id": rec.ID, "event_type": env.Type, "error": err.Error(),
			})
		}
	}
}

// markFailed counts a failed attempt; the record is retried after its
// per-record backoff or dead-lettered at maxAttempts.
func (s *OutboxService) markFailed(bookkeepCtx context.Context, rec domain.OutboxRecord, msg string) {
	if markErr := s.store.MarkFailed(bookkeepCtx, rec, msg, s.maxAttempts, s.backoff(rec.Attempts+1)); markErr != nil && s.logger != nil {
		s.logger.Error("outbox: failed to mark record failed", map[string]any{
			"id": rec.ID, "error": markErr.Error(),
		})
	}
}

// publishRecord handles a single outbox record: unmarshal → publish → mark.
// Called from a bounded goroutine pool inside PublishBatch.
func (s *OutboxService) publishRecord(ctx, bookkeepCtx context.Context, rec domain.OutboxRecord) {
	// Goroutine may have been started just before context cancellation.
	if ctx.Err() != nil {
		s.releaseStranded(bookkeepCtx, rec, "batch interrupted by context cancellation")
		return
	}

	var env domain.Envelope[json.RawMessage]
	if err := json.Unmarshal(rec.Payload, &env); err != nil {
		s.handleUnmarshalError(bookkeepCtx, rec, err)
		return
	}

	// Apply per-record publish timeout so a hung SNS client cannot block the
	// entire batch. The context is still checked via the parent ctx on cancellation.
	pubCtx := ctx
	var pubCancel context.CancelFunc
	if s.publishTimeout > 0 {
		pubCtx, pubCancel = context.WithTimeout(ctx, s.publishTimeout)
		defer pubCancel()
	}

	if err := s.publisher.Publish(pubCtx, env); err != nil {
		if s.logger != nil {
			s.logger.Warn("outbox: publish failed", map[string]any{
				"id": rec.ID, "error": err.Error(),
			})
		}
		s.handlePublishError(ctx, bookkeepCtx, err, []domain.OutboxRecord{rec})
		return
	}

	s.resetTransient()
	s.markPublished(bookkeepCtx, rec, env)
}

// PendingCount returns the number of records in outbox_events that have not
// yet been published. Callers use this to update the outbox_pending_total gauge.
func (s *OutboxService) PendingCount(ctx context.Context) (int64, error) {
	return s.store.PendingCount(ctx)
}

// OldestPendingAge returns how long the oldest unpublished record has waited.
func (s *OutboxService) OldestPendingAge(ctx context.Context) (time.Duration, error) {
	return s.store.OldestPendingAge(ctx)
}

// LeasedCount returns the number of records currently claimed (leased) by a runner.
func (s *OutboxService) LeasedCount(ctx context.Context) (int64, error) {
	return s.store.LeasedCount(ctx)
}

// ReprocessDeadLetters moves up to limit records from outbox_dead_letters back
// to outbox_events for redelivery. Returns the number of records re-queued.
func (s *OutboxService) ReprocessDeadLetters(ctx context.Context, limit int) (int, error) {
	return s.store.ReprocessDeadLetters(ctx, limit)
}

// ListDeadLetters returns up to limit dead-letter records matching filter,
// ordered by failed_at ascending (oldest failures first).
func (s *OutboxService) ListDeadLetters(ctx context.Context, filter domain.DLQFilter, limit int) ([]domain.DeadLetterRecord, error) {
	return s.store.ListDeadLetters(ctx, filter, limit)
}

// ReprocessDeadLettersWith moves up to limit filtered records from
// outbox_dead_letters back to outbox_events, resetting their attempt counters.
func (s *OutboxService) ReprocessDeadLettersWith(ctx context.Context, filter domain.DLQFilter, limit int) (int, error) {
	return s.store.ReprocessDeadLettersWith(ctx, filter, limit)
}

// DiscardDeadLetters permanently deletes up to limit filtered records from
// outbox_dead_letters. Returns the number of rows deleted.
func (s *OutboxService) DiscardDeadLetters(ctx context.Context, filter domain.DLQFilter, limit int) (int64, error) {
	return s.store.DiscardDeadLetters(ctx, filter, limit)
}

// PrunePublished deletes published records older than olderThan from outbox_events.
func (s *OutboxService) PrunePublished(ctx context.Context, olderThan time.Duration, limit int) (int64, error) {
	return s.store.PrunePublished(ctx, olderThan, limit)
}

// truncateError caps an error string to maxLastErrorLen runes to prevent
// unbounded growth of the last_error column in outbox_events.
// Uses rune-safe slicing to avoid writing invalid UTF-8 to PostgreSQL.
// The truncation marker ("…[truncated]", 12 runes) is included in the cap so
// the total output never exceeds maxLastErrorLen runes.
func truncateError(msg string) string {
	const marker = "…[truncated]"
	const markerLen = 12 // rune count of marker
	if utf8.RuneCountInString(msg) <= maxLastErrorLen {
		return msg
	}
	return string([]rune(msg)[:maxLastErrorLen-markerLen]) + marker
}
