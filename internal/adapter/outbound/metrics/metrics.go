// Package metrics registers the platform-events Prometheus metrics, per the
// Enterprise Platform Observability Standard (see registry.go for the tiering
// and every metric's ratification packet).
//
// Only Tier 1 platform_* metrics are emitted. They carry the required
// {domain, service, environment} const labels, injected centrally from a
// mandatory Identity (rule 8), and are registered by InitWithIdentity; a
// platform metric that cannot be registered is disabled with a
// RegistrationWarning instead of failing (fail-soft). The pre-standard
// events_* / outbox_* / sqs_* names were removed without a compatibility
// period (no release carrying them was ever deployed).
//
// Every Record/Observe function is safe before initialisation (no-op).
package metrics

import (
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
)

// Platform holds the Tier 1 platform_* collectors. A nil field means that
// metric is not registered (no identity, or registration refused).
type Platform struct {
	MessagesReceived   *prometheus.CounterVec   // platform_messages_received_total
	MessagesProcessed  *prometheus.CounterVec   // platform_messages_processed_total
	MessagesFailed     *prometheus.CounterVec   // platform_messages_failed_total
	Retries            *prometheus.CounterVec   // platform_retry_total
	DLQMessages        *prometheus.CounterVec   // platform_dlq_messages_total
	DuplicateMessages  *prometheus.CounterVec   // platform_duplicate_messages_total
	DependencyRequests *prometheus.HistogramVec // platform_dependency_request_seconds
	EventPropagation   *prometheus.HistogramVec // platform_event_propagation_seconds
	MessagesPublished  *prometheus.CounterVec   // platform_messages_published_total
	ProcessingDuration *prometheus.HistogramVec // platform_message_processing_duration_seconds
	OutboxPending      *prometheus.GaugeVec     // platform_outbox_pending_events
	OutboxLeased       *prometheus.GaugeVec     // platform_outbox_leased_events
	OutboxOldestAge    *prometheus.GaugeVec     // platform_outbox_oldest_pending_age
	OutboxBlocked      *prometheus.GaugeVec     // platform_outbox_ordering_blocked_events
	Timeouts           *prometheus.CounterVec   // platform_message_timeouts_total
	OutboxAttempts     *prometheus.CounterVec   // platform_outbox_publish_attempts_total
	OutboxErrors       *prometheus.CounterVec   // platform_outbox_errors_total
	OutboxDLOperations *prometheus.CounterVec   // platform_outbox_dead_letter_operations_total
	LabelOverflow      *prometheus.CounterVec   // platform_telemetry_label_overflow_total
	LibraryInfo        *prometheus.GaugeVec     // platform_library_info
	QueueDepth         *prometheus.GaugeVec     // platform_queue_depth
	InFlight           *prometheus.GaugeVec     // platform_messages_in_flight
	DLQDepth           *prometheus.GaugeVec     // platform_dlq_depth

	identity Identity
}

// Identity returns the identity the platform metrics carry.
func (p *Platform) Identity() (Identity, bool) {
	if p == nil {
		return Identity{}, false
	}
	return p.identity, true
}

// platform is the active Tier 1 set; nil without InitWithIdentity.
var platform atomic.Pointer[Platform]

// CurrentPlatform returns the active Tier 1 set, or nil.
func CurrentPlatform() *Platform { return platform.Load() }

// ReplacePlatform installs p and returns the previous set. For tests that
// must restore process-wide state.
func ReplacePlatform(p *Platform) *Platform { return platform.Swap(p) }

// Histogram buckets. Identical in every service so cross-service quantiles
// are valid.
var (
	dependencyBuckets  = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}
	processingBuckets  = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120}
	propagationBuckets = []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600}
)

// maxEventTypeLabelLen is the maximum byte length of an event_type label value.
const maxEventTypeLabelLen = 128

// DefaultEventTypeLimit is the default number of distinct event_type label
// values a process records; see SetEventTypeLimit.
const DefaultEventTypeLimit = 200

// Replacement values for event_type labels outside the bounds.
const (
	EventTypeUnknown   = "unknown"
	EventTypeOversized = "__oversized__"
	EventTypeOther     = "__other__"
)

// eventTypes remembers the distinct event_type values admitted so far. The
// byte cap alone does not bound cardinality: a producer could send an
// unlimited number of short distinct types. Once limit values have been
// admitted, new ones are recorded as "__other__".
var eventTypes = struct {
	sync.RWMutex
	seen  map[string]struct{}
	limit int
}{seen: map[string]struct{}{}, limit: DefaultEventTypeLimit}

// SetEventTypeLimit sets how many distinct event_type values the process
// records (n <= 0 restores DefaultEventTypeLimit) and forgets the values
// admitted so far.
func SetEventTypeLimit(n int) {
	SetEventTypes(n, nil)
}

// SetEventTypes is SetEventTypeLimit plus known: values admitted up front, so
// they always keep their own label however many unknown values arrive later
// (e.g. from a misbehaving producer on a shared topic). The limit counts
// known values too and is raised to fit them.
func SetEventTypes(limit int, known []string) {
	if limit <= 0 {
		limit = DefaultEventTypeLimit
	}
	seen := make(map[string]struct{}, len(known))
	for _, k := range known {
		if k != "" && len(k) <= maxEventTypeLabelLen && utf8.ValidString(k) {
			seen[k] = struct{}{}
		}
	}
	eventTypes.Lock()
	eventTypes.limit = max(limit, len(seen))
	eventTypes.seen = seen
	eventTypes.Unlock()
}

// SanitizeEventType bounds an event_type label value: "unknown" for an empty
// value, invalid UTF-8 replaced with U+FFFD, "__oversized__" for one over 128
// bytes, and "__other__" once the
// process has already admitted its limit of distinct values (default 200).
// Every replacement is counted in platform_telemetry_label_overflow_total, so a misconfigured or adversarial producer cannot explode cardinality.
func SanitizeEventType(s string) string {
	if s == "" {
		return EventTypeUnknown
	}
	// Prometheus panics on a label value that is not valid UTF-8; an event
	// type built from raw bytes must not crash the caller's publish.
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "\uFFFD")
	}
	if len(s) > maxEventTypeLabelLen {
		recordLabelOverflow()
		return EventTypeOversized
	}
	eventTypes.RLock()
	_, ok := eventTypes.seen[s]
	eventTypes.RUnlock()
	if ok {
		return s
	}
	eventTypes.Lock()
	defer eventTypes.Unlock()
	if _, ok := eventTypes.seen[s]; ok {
		return s
	}
	if len(eventTypes.seen) >= eventTypes.limit {
		recordLabelOverflow()
		return EventTypeOther
	}
	eventTypes.seen[s] = struct{}{}
	return s
}

func recordLabelOverflow() {
	if p := platform.Load(); p != nil && p.LabelOverflow != nil {
		p.LabelOverflow.WithLabelValues("event_type").Inc()
	}
}

// QueueName returns the queue label value for an SQS queue URL: its last
// path segment ("unknown" for an empty URL). The full URL carries the account
// ID and is never used as a Tier 1 label value.
func QueueName(queueURL string) string {
	name := queueURL[strings.LastIndex(queueURL, "/")+1:]
	if name == "" {
		return "unknown"
	}
	return name
}

// TopicName returns the topic label value for an SNS topic ARN: its last
// segment ("unknown" for an empty ARN).
func TopicName(topicARN string) string {
	name := topicARN[strings.LastIndex(topicARN, ":")+1:]
	if name == "" {
		return "unknown"
	}
	return name
}

// ── Initialisation ───────────────────────────────────────────────────────

// InitWithIdentity registers the Tier 1 platform metrics with id's const
// labels on reg and makes them the active set. The identity is mandatory: an
// invalid one returns an error and changes nothing. A platform metric that
// cannot be registered is returned as a warning and disabled (fail-soft).
func InitWithIdentity(id Identity, reg prometheus.Registerer) ([]RegistrationWarning, error) {
	if err := id.Validate(); err != nil {
		return nil, err
	}
	p, warnings := registerPlatform(newRegistrar(reg), id)
	platform.Store(p)
	return warnings, nil
}

// precreate exports a counter without variable labels at 0 as soon as it is
// registered, so increase()/rate() alerts see its first increment after a
// restart (a series that first appears at 1 has no increase).
func precreate(c *prometheus.CounterVec, labels []string) {
	if c != nil && len(labels) == 0 {
		c.WithLabelValues()
	}
}

func registerPlatform(r *registrar, id Identity) (*Platform, []RegistrationWarning) {
	constLabels := id.platformLabels()
	var warnings []RegistrationWarning
	help := func(name string) string {
		e, _ := Lookup(name)
		return e.SemanticDefinition
	}
	warn := func(name string, err error) {
		if err != nil {
			warnings = append(warnings, RegistrationWarning{Metric: name, Err: err})
		}
	}
	counter := func(name string, labels ...string) *prometheus.CounterVec {
		c, err := register(r, constLabels, PlatformRequiredLabels, func(cl prometheus.Labels) *prometheus.CounterVec {
			return prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help(name), ConstLabels: cl}, labels)
		})
		warn(name, err)
		precreate(c, labels)
		return c
	}
	histogram := func(name string, buckets []float64, labels ...string) *prometheus.HistogramVec {
		h, err := register(r, constLabels, PlatformRequiredLabels, func(cl prometheus.Labels) *prometheus.HistogramVec {
			return prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help(name), ConstLabels: cl, Buckets: buckets}, labels)
		})
		warn(name, err)
		return h
	}
	gauge := func(name string, labels ...string) *prometheus.GaugeVec {
		g, err := register(r, constLabels, PlatformRequiredLabels, func(cl prometheus.Labels) *prometheus.GaugeVec {
			return prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help(name), ConstLabels: cl}, labels)
		})
		warn(name, err)
		return g
	}
	p := &Platform{
		identity:           id,
		MessagesReceived:   counter("platform_messages_received_total", "queue"),
		MessagesProcessed:  counter("platform_messages_processed_total", "queue", "event_type"),
		MessagesFailed:     counter("platform_messages_failed_total", "queue", "event_type", "reason"),
		Retries:            counter("platform_retry_total", "operation", "event_type"),
		DLQMessages:        counter("platform_dlq_messages_total", "operation", "event_type", "reason"),
		DuplicateMessages:  counter("platform_duplicate_messages_total", "queue", "event_type"),
		DependencyRequests: histogram("platform_dependency_request_seconds", dependencyBuckets, "dependency", "operation", "outcome"),
		EventPropagation:   histogram("platform_event_propagation_seconds", propagationBuckets, "queue", "event_type"),
		MessagesPublished:  counter("platform_messages_published_total", "topic", "event_type", "outcome"),
		ProcessingDuration: histogram("platform_message_processing_duration_seconds", processingBuckets, "queue", "event_type"),
		OutboxPending:      gauge("platform_outbox_pending_events"),
		OutboxLeased:       gauge("platform_outbox_leased_events"),
		OutboxOldestAge:    gauge("platform_outbox_oldest_pending_age"),
		OutboxBlocked:      gauge("platform_outbox_ordering_blocked_events"),
		Timeouts:           counter("platform_message_timeouts_total", "queue", "event_type", "operation"),
		OutboxAttempts:     counter("platform_outbox_publish_attempts_total", "event_type", "outcome"),
		OutboxErrors:       counter("platform_outbox_errors_total", "operation"),
		OutboxDLOperations: counter("platform_outbox_dead_letter_operations_total", "operation"),
		LabelOverflow:      counter("platform_telemetry_label_overflow_total", "label"),
		LibraryInfo:        gauge("platform_library_info", "library", "library_version"),
		QueueDepth:         gauge("platform_queue_depth", "queue"),
		InFlight:           gauge("platform_messages_in_flight", "queue"),
		DLQDepth:           gauge("platform_dlq_depth", "queue"),
	}

	// Pre-create the label-static counters at 0: a series that first appears
	// at 1 is invisible to increase()/rate(), so the first error after every
	// deploy would not alert.
	for _, op := range OutboxErrorOperationValues {
		if p.OutboxErrors != nil {
			p.OutboxErrors.WithLabelValues(op)
		}
	}
	for _, op := range DeadLetterOperationValues {
		if p.OutboxDLOperations != nil {
			p.OutboxDLOperations.WithLabelValues(op)
		}
	}
	if p.LabelOverflow != nil {
		p.LabelOverflow.WithLabelValues("event_type")
	}
	if p.LibraryInfo != nil {
		p.LibraryInfo.WithLabelValues(LibraryName, LibraryVersion()).Set(1)
	}
	return p, warnings
}

// InitQueue pre-creates the per-queue Tier 1 counters at 0 when a consumer
// starts, so the first failure on a queue is visible to increase()/rate().
func InitQueue(queueURL string) {
	p := platform.Load()
	if p == nil {
		return
	}
	q := QueueName(queueURL)
	if p.MessagesReceived != nil {
		p.MessagesReceived.WithLabelValues(q)
	}
	if p.MessagesFailed != nil {
		p.MessagesFailed.WithLabelValues(q, "unknown", "malformed")
	}
	if p.InFlight != nil {
		p.InFlight.WithLabelValues(q)
	}
}

// outcome maps an error to the approved outcome vocabulary.
func outcome(err error) string {
	if err != nil {
		return "error"
	}
	return "success"
}

// ── Tier 1 recording ─────────────────────────────────────────────────────

// ObserveReceived counts one message delivered by queueURL.
func ObserveReceived(queueURL string) {
	if p := platform.Load(); p != nil && p.MessagesReceived != nil {
		p.MessagesReceived.WithLabelValues(QueueName(queueURL)).Inc()
	}
}

// ObservePropagation records the creation-to-receipt delay of one event.
// Negative clock skew is clamped to 0; a zero createdAt is ignored.
func ObservePropagation(queueURL, eventType string, createdAt, receivedAt time.Time) {
	if createdAt.IsZero() {
		return
	}
	if p := platform.Load(); p != nil && p.EventPropagation != nil {
		d := math.Max(receivedAt.Sub(createdAt).Seconds(), 0)
		p.EventPropagation.WithLabelValues(QueueName(queueURL), SanitizeEventType(eventType)).Observe(d)
	}
}

// ObserveProcessingDuration records one handler (or dead-letter handler) run.
func ObserveProcessingDuration(queueURL, eventType string, d time.Duration) {
	if p := platform.Load(); p != nil && p.ProcessingDuration != nil {
		p.ProcessingDuration.WithLabelValues(QueueName(queueURL), SanitizeEventType(eventType)).Observe(d.Seconds())
	}
}

// IncProcessed counts one message processed successfully and acknowledged.
func IncProcessed(queueURL, eventType string) {
	if p := platform.Load(); p != nil && p.MessagesProcessed != nil {
		p.MessagesProcessed.WithLabelValues(QueueName(queueURL), SanitizeEventType(eventType)).Inc()
	}
}

// IncFailed counts one message that failed on this delivery; reason is one
// of FailureReasonValues.
func IncFailed(queueURL, eventType, reason string) {
	if p := platform.Load(); p != nil && p.MessagesFailed != nil {
		p.MessagesFailed.WithLabelValues(QueueName(queueURL), SanitizeEventType(eventType), reason).Inc()
	}
}

// IncRetry counts one failed attempt left for automatic retry; operation is
// one of FlowOperationValues.
func IncRetry(operation, eventType string) {
	if p := platform.Load(); p != nil && p.Retries != nil {
		p.Retries.WithLabelValues(operation, SanitizeEventType(eventType)).Inc()
	}
}

// IncDLQ counts one message moved to dead-letter storage.
func IncDLQ(operation, eventType, reason string) {
	if p := platform.Load(); p != nil && p.DLQMessages != nil {
		p.DLQMessages.WithLabelValues(operation, SanitizeEventType(eventType), reason).Inc()
	}
}

// IncDuplicate counts one redelivery acknowledged by the dedup ledger.
func IncDuplicate(queueURL, eventType string) {
	if p := platform.Load(); p != nil && p.DuplicateMessages != nil {
		p.DuplicateMessages.WithLabelValues(QueueName(queueURL), SanitizeEventType(eventType)).Inc()
	}
}

// AddInFlight adjusts the number of messages queueURL's consumer is processing.
func AddInFlight(queueURL string, delta float64) {
	if p := platform.Load(); p != nil && p.InFlight != nil {
		p.InFlight.WithLabelValues(QueueName(queueURL)).Add(delta)
	}
}

// SetQueueDepth records the sampled number of messages waiting on queueURL.
func SetQueueDepth(queueURL string, n float64) {
	if p := platform.Load(); p != nil && p.QueueDepth != nil {
		p.QueueDepth.WithLabelValues(QueueName(queueURL)).Set(n)
	}
}

// SetDLQDepth records the sampled number of messages in the DLQ attached to
// sourceQueueURL (labelled with the source queue's name).
func SetDLQDepth(sourceQueueURL string, n float64) {
	if p := platform.Load(); p != nil && p.DLQDepth != nil {
		p.DLQDepth.WithLabelValues(QueueName(sourceQueueURL)).Set(n)
	}
}

// ObserveDependency records one call to an external dependency.
func ObserveDependency(dependency, operation string, err error, d time.Duration) {
	if p := platform.Load(); p != nil && p.DependencyRequests != nil {
		p.DependencyRequests.WithLabelValues(dependency, operation, outcome(err)).Observe(d.Seconds())
	}
}

// ── Producer and outbox recording ────────────────────────────────────────

// IncPublished counts one event a producer attempted to publish to topicARN
// (platform_messages_published_total); outcome is "success" or "error".
func IncPublished(topicARN, eventType, outcome string) {
	if p := platform.Load(); p != nil && p.MessagesPublished != nil {
		p.MessagesPublished.WithLabelValues(TopicName(topicARN), SanitizeEventType(eventType), outcome).Inc()
	}
}

// SetOutboxPending updates platform_outbox_pending_events. n = -1 signals a
// failed count query: the gauge keeps its last value and
// platform_outbox_errors_total{operation="pending_count"} counts it.
func SetOutboxPending(n float64) {
	p := platform.Load()
	if p == nil {
		return
	}
	if n < 0 {
		incOutboxError(p, "pending_count")
		return
	}
	if p.OutboxPending != nil {
		p.OutboxPending.WithLabelValues().Set(n)
	}
}

// SetOutboxLeased updates platform_outbox_leased_events.
func SetOutboxLeased(n float64) {
	if p := platform.Load(); p != nil && p.OutboxLeased != nil {
		p.OutboxLeased.WithLabelValues().Set(n)
	}
}

// RecordOutboxLeasedCountError counts a failed leased-count query.
func RecordOutboxLeasedCountError() {
	if p := platform.Load(); p != nil {
		incOutboxError(p, "leased_count")
	}
}

// SetOutboxOldestPendingAge records the age of the oldest unpublished outbox
// event; a negative age signals a failed query (gauge left unchanged,
// platform_outbox_errors_total{operation="oldest_pending"} counted).
func SetOutboxOldestPendingAge(age time.Duration) {
	p := platform.Load()
	if p == nil {
		return
	}
	if age < 0 {
		incOutboxError(p, "oldest_pending")
		return
	}
	if p.OutboxOldestAge != nil {
		p.OutboxOldestAge.WithLabelValues().Set(age.Seconds())
	}
}

// SetOutboxBlocked records the number of strict-ordering-blocked outbox
// events; a negative n signals a failed query (gauge left unchanged,
// platform_outbox_errors_total{operation="blocked_count"} counted).
func SetOutboxBlocked(n float64) {
	p := platform.Load()
	if p == nil {
		return
	}
	if n < 0 {
		incOutboxError(p, "blocked_count")
		return
	}
	if p.OutboxBlocked != nil {
		p.OutboxBlocked.WithLabelValues().Set(n)
	}
}

// HasOutboxBlockedMetric reports whether the ordering-blocked gauge is registered.
func HasOutboxBlockedMetric() bool {
	p := platform.Load()
	return p != nil && p.OutboxBlocked != nil
}

// IncTimeout counts a message whose WithHandlerTimeout deadline expired in
// stage (one of TimeoutOperationValues).
func IncTimeout(queueURL, eventType, stage string) {
	if p := platform.Load(); p != nil && p.Timeouts != nil {
		p.Timeouts.WithLabelValues(QueueName(queueURL), SanitizeEventType(eventType), stage).Inc()
	}
}

// HasOutboxOldestAgeMetric reports whether the oldest-pending-age gauge is
// registered, so the runner can skip its query otherwise.
func HasOutboxOldestAgeMetric() bool {
	p := platform.Load()
	return p != nil && p.OutboxOldestAge != nil
}

// HasOutboxPendingMetric reports whether the pending-outbox gauge is
// registered, so the runner can skip the count query otherwise.
func HasOutboxPendingMetric() bool {
	p := platform.Load()
	return p != nil && p.OutboxPending != nil
}

// HasOutboxLeasedMetric reports whether the leased-outbox gauge is registered.
func HasOutboxLeasedMetric() bool {
	p := platform.Load()
	return p != nil && p.OutboxLeased != nil
}

// RecordOutboxDeadLettersReprocessed counts n dead-letter records moved back
// to outbox_events.
func RecordOutboxDeadLettersReprocessed(n int) {
	recordDeadLetterOperation("reprocess", float64(n))
}

// RecordOutboxDeadLettersDiscarded counts n dead-letter records deleted.
func RecordOutboxDeadLettersDiscarded(n int64) {
	recordDeadLetterOperation("discard", float64(n))
}

func recordDeadLetterOperation(op string, n float64) {
	if n <= 0 {
		return
	}
	if p := platform.Load(); p != nil && p.OutboxDLOperations != nil {
		p.OutboxDLOperations.WithLabelValues(op).Add(n)
	}
}

// RecordOutboxDeadLetter counts one outbox record moved to
// outbox_dead_letters after exhausting its attempts.
func RecordOutboxDeadLetter(eventType string) {
	IncDLQ("outbox_publish", eventType, "max_attempts")
}

// IncOutboxPublishAttempt counts one outbox publish attempt
// (platform_outbox_publish_attempts_total); outcome is "success" or "error".
func IncOutboxPublishAttempt(eventType, outcome string) {
	if p := platform.Load(); p != nil && p.OutboxAttempts != nil {
		p.OutboxAttempts.WithLabelValues(SanitizeEventType(eventType), outcome).Inc()
	}
}

// RecordOutboxPollError counts a failed outbox poll cycle.
func RecordOutboxPollError() {
	if p := platform.Load(); p != nil {
		incOutboxError(p, "poll")
	}
}

// RecordOutboxUnmarshalError counts a stored envelope that failed to unmarshal.
func RecordOutboxUnmarshalError() {
	if p := platform.Load(); p != nil {
		incOutboxError(p, "unmarshal")
	}
}

// RecordOutboxMarkPublishedError counts a MarkPublished failure after a
// successful publish (the event will be delivered again).
func RecordOutboxMarkPublishedError() {
	if p := platform.Load(); p != nil {
		incOutboxError(p, "mark_published")
	}
}

func incOutboxError(p *Platform, op string) {
	if p.OutboxErrors != nil {
		p.OutboxErrors.WithLabelValues(op).Inc()
	}
}
