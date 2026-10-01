// Package metrics registers the platform-events Prometheus metrics, per the
// Enterprise Platform Observability Standard (see registry.go for the tiering
// and every metric's ratification packet).
//
// Two families are emitted from the same recording calls:
//
//   - Tier 1 platform_*: carry the required {domain, service, environment}
//     const labels, injected centrally from an Identity (rule 8). Registered
//     only by InitWithIdentity; a platform metric that cannot be registered
//     is disabled with a RegistrationWarning instead of failing (fail-soft).
//   - Legacy events_* / outbox_* / sqs_* (Deprecated): the pre-standard
//     names, unchanged, emitted in parallel for the compatibility period.
//     Registered by Init / InitWithRegisterer, and by InitWithIdentity unless
//     legacy metrics are disabled.
//
// Every Record/Observe function is safe before initialisation (no-op).
package metrics

import (
	"errors"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
)

// Legacy metric variables (Deprecated), installed by Init, InitWithRegisterer
// or InitWithIdentity; nil when legacy metrics are not registered.
var (
	EventsPublishedTotal              *prometheus.CounterVec
	EventsPublishDuration             *prometheus.HistogramVec
	EventsConsumedTotal               *prometheus.CounterVec
	EventsConsumeDuration             *prometheus.HistogramVec
	CodecEncodeTotal                  *prometheus.CounterVec
	CodecEncodeDuration               *prometheus.HistogramVec
	CodecDecodeTotal                  *prometheus.CounterVec
	CodecDecodeDuration               *prometheus.HistogramVec
	OutboxPendingTotal                *prometheus.GaugeVec
	OutboxPublishedTotal              *prometheus.CounterVec
	OutboxAttemptsTotal               *prometheus.CounterVec
	OutboxDeadLettersTotal            *prometheus.CounterVec
	OutboxDeadLettersReprocessedTotal *prometheus.CounterVec
	OutboxDeadLettersDiscardedTotal   *prometheus.CounterVec
	OutboxLeasedTotal                 *prometheus.GaugeVec

	// SQS-level infrastructure error counters.
	SQSReceiveErrorsTotal    *prometheus.CounterVec
	SQSDeleteErrorsTotal     *prometheus.CounterVec
	SQSVisibilityErrorsTotal *prometheus.CounterVec

	// InboxDuplicatesTotal counts redeliveries the inbox ledger filtered.
	InboxDuplicatesTotal *prometheus.CounterVec

	// DLQForwardedTotal counts messages forwarded to a source queue's DLQ.
	DLQForwardedTotal *prometheus.CounterVec

	// Outbox infrastructure error counters.
	OutboxPollErrorsTotal          *prometheus.CounterVec
	OutboxUnmarshalErrorsTotal     *prometheus.CounterVec
	OutboxMarkPublishedErrorsTotal *prometheus.CounterVec

	// OversizedEventTypeLabelTotal counts event_type values replaced with
	// "__oversized__".
	OversizedEventTypeLabelTotal *prometheus.CounterVec

	initOnce  sync.Once
	metricsMu sync.RWMutex // guards the legacy vars against concurrent re-initialisation
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
	OutboxAttempts     *prometheus.CounterVec   // platform_outbox_publish_attempts_total
	OutboxErrors       *prometheus.CounterVec   // platform_outbox_errors_total
	OutboxDLOperations *prometheus.CounterVec   // platform_outbox_dead_letter_operations_total
	LabelOverflow      *prometheus.CounterVec   // platform_telemetry_label_overflow_total
	LibraryInfo        *prometheus.GaugeVec     // platform_library_info
	QueueDepth         *prometheus.GaugeVec     // platform_queue_depth
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
	legacyBuckets      = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}
	legacyShortBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5}
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
	if n <= 0 {
		n = DefaultEventTypeLimit
	}
	eventTypes.Lock()
	eventTypes.limit = n
	eventTypes.seen = map[string]struct{}{}
	eventTypes.Unlock()
}

// SanitizeEventType bounds an event_type label value: "unknown" for an empty
// value, invalid UTF-8 replaced with U+FFFD, "__oversized__" for one over 128
// bytes, and "__other__" once the
// process has already admitted its limit of distinct values (default 200).
// Every replacement is counted in platform_telemetry_label_overflow_total
// (and the legacy events_oversized_event_type_label_total for oversized ones),
// so a misconfigured or adversarial producer cannot explode cardinality.
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
		recordLabelOverflow(true)
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
		recordLabelOverflow(false)
		return EventTypeOther
	}
	eventTypes.seen[s] = struct{}{}
	return s
}

func recordLabelOverflow(oversized bool) {
	if oversized {
		metricsMu.RLock()
		c := OversizedEventTypeLabelTotal
		metricsMu.RUnlock()
		if c != nil {
			c.WithLabelValues().Inc()
		}
	}
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

// Init registers the legacy metrics using the default registerer. Panics if
// serviceName is empty. Idempotent — only the first call registers.
//
// A no-op once InitWithIdentity has installed the Tier 1 set: a leftover
// legacy Init (an old bootstrap or shared helper) must never silently disable
// the platform_* metrics, and InitWithIdentity already registered the legacy
// metrics when they are wanted.
func Init(serviceName, buildVersion string) {
	if serviceName == "" {
		panic("platform-events: metrics.Init requires a non-empty serviceName")
	}
	if platform.Load() != nil {
		return
	}
	initOnce.Do(func() {
		installLegacyOrPanic(serviceName, buildVersion, prometheus.DefaultRegisterer)
	})
}

// InitWithRegisterer registers the legacy metrics on reg, bypassing the
// sync.Once guard. Panics if serviceName is empty. It is the test-isolation
// reset: it clears any Tier 1 set, making the process legacy-only. Production
// code uses InitWithIdentity.
func InitWithRegisterer(serviceName, buildVersion string, reg prometheus.Registerer) {
	if serviceName == "" {
		panic("platform-events: metrics.InitWithRegisterer requires a non-empty serviceName")
	}
	installLegacyOrPanic(serviceName, buildVersion, reg)
	platform.Store(nil)
}

func installLegacyOrPanic(serviceName, buildVersion string, reg prometheus.Registerer) {
	l, err := registerLegacy(newRegistrar(reg), serviceName, buildVersion)
	if err != nil {
		panic(err)
	}
	l.install()
}

// InitWithIdentity registers the Tier 1 platform metrics with id's const
// labels and — unless legacy is false — the legacy metrics in parallel, and
// makes them the active set. It returns an error, changing nothing, for an
// invalid identity or a legacy registration failure. A platform metric that
// cannot be registered is returned as a warning and disabled.
func InitWithIdentity(id Identity, reg prometheus.Registerer, legacy bool) ([]RegistrationWarning, error) {
	if err := id.Validate(); err != nil {
		return nil, err
	}
	r := newRegistrar(reg)
	var l *legacySet
	if legacy {
		var err error
		if l, err = registerLegacy(r, id.Service, id.Version); err != nil {
			return nil, err
		}
	}
	p, warnings := registerPlatform(r, id)
	l.install() // nil → clears the legacy vars
	platform.Store(p)
	return warnings, nil
}

// legacySet is one registered generation of the legacy metrics.
type legacySet struct {
	publishedTotal, consumedTotal, codecEncodeTotal, codecDecodeTotal                             *prometheus.CounterVec
	publishDuration, consumeDuration, codecEncodeDuration, codecDecodeDuration                    *prometheus.HistogramVec
	outboxPending, outboxLeased                                                                   *prometheus.GaugeVec
	outboxPublished, outboxAttempts, outboxDeadLetters, outboxReprocessed, outboxDiscarded        *prometheus.CounterVec
	sqsReceiveErrors, sqsDeleteErrors, sqsVisibilityErrors, inboxDuplicates, dlqForwarded         *prometheus.CounterVec
	outboxPollErrors, outboxUnmarshalErrors, outboxMarkPublishedErrors, oversizedEventTypeCounter *prometheus.CounterVec
}

// install makes l the active legacy set; a nil l clears it.
func (l *legacySet) install() {
	if l == nil {
		l = &legacySet{}
	}
	metricsMu.Lock()
	defer metricsMu.Unlock()
	EventsPublishedTotal, EventsPublishDuration = l.publishedTotal, l.publishDuration
	EventsConsumedTotal, EventsConsumeDuration = l.consumedTotal, l.consumeDuration
	CodecEncodeTotal, CodecEncodeDuration = l.codecEncodeTotal, l.codecEncodeDuration
	CodecDecodeTotal, CodecDecodeDuration = l.codecDecodeTotal, l.codecDecodeDuration
	OutboxPendingTotal, OutboxLeasedTotal = l.outboxPending, l.outboxLeased
	OutboxPublishedTotal, OutboxAttemptsTotal, OutboxDeadLettersTotal = l.outboxPublished, l.outboxAttempts, l.outboxDeadLetters
	OutboxDeadLettersReprocessedTotal, OutboxDeadLettersDiscardedTotal = l.outboxReprocessed, l.outboxDiscarded
	SQSReceiveErrorsTotal, SQSDeleteErrorsTotal, SQSVisibilityErrorsTotal = l.sqsReceiveErrors, l.sqsDeleteErrors, l.sqsVisibilityErrors
	InboxDuplicatesTotal, DLQForwardedTotal = l.inboxDuplicates, l.dlqForwarded
	OutboxPollErrorsTotal, OutboxUnmarshalErrorsTotal, OutboxMarkPublishedErrorsTotal = l.outboxPollErrors, l.outboxUnmarshalErrors, l.outboxMarkPublishedErrors
	OversizedEventTypeLabelTotal = l.oversizedEventTypeCounter
}

// precreate exports a counter without variable labels at 0 as soon as it is
// registered, so increase()/rate() alerts see its first increment after a
// restart (a series that first appears at 1 has no increase).
func precreate(c *prometheus.CounterVec, labels []string) {
	if c != nil && len(labels) == 0 {
		c.WithLabelValues()
	}
}

const deprecatedHelp = " Deprecated: superseded by a platform_* metric (see docs/observability/metrics-registry.md)."

func registerLegacy(r *registrar, serviceName, buildVersion string) (*legacySet, error) {
	constLabels := prometheus.Labels{LabelService: serviceName}
	droppable := []string{LabelService}
	var errs []error
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		c, err := register(r, constLabels, droppable, func(cl prometheus.Labels) *prometheus.CounterVec {
			return prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help + deprecatedHelp, ConstLabels: cl}, labels)
		})
		errs = append(errs, err)
		precreate(c, labels)
		return c
	}
	histogram := func(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
		h, err := register(r, constLabels, droppable, func(cl prometheus.Labels) *prometheus.HistogramVec {
			return prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help + deprecatedHelp, ConstLabels: cl, Buckets: buckets}, labels)
		})
		errs = append(errs, err)
		return h
	}
	gauge := func(name, help string) *prometheus.GaugeVec {
		g, err := register(r, constLabels, droppable, func(cl prometheus.Labels) *prometheus.GaugeVec {
			return prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help + deprecatedHelp, ConstLabels: cl}, nil)
		})
		errs = append(errs, err)
		return g
	}

	// Build info: service is a const label like on every legacy metric (so a
	// registerer injecting service applies it once); version is variable.
	// The exposed series is unchanged: {service, version} = 1.
	buildInfo, err := register(r, constLabels, droppable, func(cl prometheus.Labels) *prometheus.GaugeVec {
		return prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "platform_events_build_info",
			Help:        "Build information for the platform-events library." + deprecatedHelp,
			ConstLabels: cl,
		}, []string{LabelVersion})
	})
	errs = append(errs, err)

	l := &legacySet{
		publishedTotal:            counter("events_published_total", "Total number of events published.", "topic", "event_type", "status"),
		publishDuration:           histogram("events_publish_duration_seconds", "Duration of event publish operations in seconds.", legacyBuckets, "topic", "event_type"),
		consumedTotal:             counter("events_consumed_total", "Total number of events consumed.", "queue", "event_type", "status"),
		consumeDuration:           histogram("events_consume_duration_seconds", "Duration of event handler invocations in seconds.", legacyBuckets, "queue", "event_type"),
		codecEncodeTotal:          counter("events_codec_encode_total", "Total Codec.Encode invocations by outcome.", "topic", "event_type", "status"),
		codecEncodeDuration:       histogram("events_codec_encode_duration_seconds", "Duration of Codec.Encode calls in seconds.", legacyShortBuckets, "topic", "event_type"),
		codecDecodeTotal:          counter("events_codec_decode_total", "Total Codec.Decode invocations by outcome.", "queue", "event_type", "status"),
		codecDecodeDuration:       histogram("events_codec_decode_duration_seconds", "Duration of Codec.Decode calls in seconds.", legacyShortBuckets, "queue", "event_type"),
		outboxPending:             gauge("outbox_pending_total", "Number of pending (unpublished) outbox records. -1 indicates a stale/error reading."),
		outboxPublished:           counter("outbox_published_total", "Total outbox records published, by event type and status.", "event_type", "status"),
		outboxAttempts:            counter("outbox_attempts_total", "Total outbox publish attempts, by event type.", "event_type"),
		outboxDeadLetters:         counter("outbox_dead_letters_total", "Total outbox records moved to outbox_dead_letters.", "event_type"),
		outboxReprocessed:         counter("outbox_dead_letters_reprocessed_total", "Total dead-letter records moved back to outbox_events for retry."),
		outboxDiscarded:           counter("outbox_dead_letters_discarded_total", "Total dead-letter records permanently deleted."),
		outboxLeased:              gauge("outbox_leased_total", "Number of outbox records currently leased by a runner."),
		sqsReceiveErrors:          counter("sqs_receive_errors_total", "Total SQS ReceiveMessage failures.", "queue"),
		sqsDeleteErrors:           counter("sqs_delete_errors_total", "Total SQS DeleteMessage failures.", "queue"),
		sqsVisibilityErrors:       counter("sqs_visibility_extension_errors_total", "Total SQS ChangeMessageVisibility failures.", "queue"),
		outboxPollErrors:          counter("outbox_poll_errors_total", "Total outbox poll cycle failures."),
		outboxUnmarshalErrors:     counter("outbox_unmarshal_errors_total", "Total outbox records whose envelope could not be unmarshalled."),
		outboxMarkPublishedErrors: counter("outbox_mark_published_errors_total", "Total MarkPublished failures after a successful publish (potential duplicate delivery)."),
		inboxDuplicates:           counter("events_inbox_duplicates_total", "Total redelivered messages acknowledged by the inbox ledger without handling.", "consumer"),
		dlqForwarded:              counter("events_dlq_forwarded_total", "Total messages forwarded to a source queue's dead-letter queue, by outcome.", "queue", "event_type", "status"),
		oversizedEventTypeCounter: counter("events_oversized_event_type_label_total", "Total event_type values replaced with __oversized__."),
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	buildInfo.WithLabelValues(buildVersion).Set(1)
	return l, nil
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
		OutboxAttempts:     counter("platform_outbox_publish_attempts_total", "event_type", "outcome"),
		OutboxErrors:       counter("platform_outbox_errors_total", "operation"),
		OutboxDLOperations: counter("platform_outbox_dead_letter_operations_total", "operation"),
		LabelOverflow:      counter("platform_telemetry_label_overflow_total", "label"),
		LibraryInfo:        gauge("platform_library_info", "library", "library_version"),
		QueueDepth:         gauge("platform_queue_depth", "queue"),
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

// ── Legacy recording (also feeds the Tier 1 metric where the mapping is 1:1) ─

// RecordPublish counts one publish attempt of one event (legacy counter and
// duration, and platform_messages_published_total). status is "success" or
// "error"; topic is the topic ARN.
func RecordPublish(topic, eventType, status string, durSeconds float64) {
	et := SanitizeEventType(eventType)
	if p := platform.Load(); p != nil && p.MessagesPublished != nil {
		p.MessagesPublished.WithLabelValues(TopicName(topic), et, status).Inc()
	}
	metricsMu.RLock()
	pt, pd := EventsPublishedTotal, EventsPublishDuration
	metricsMu.RUnlock()
	if pt != nil {
		pt.WithLabelValues(topic, et, status).Inc()
	}
	if pd != nil {
		pd.WithLabelValues(topic, et).Observe(durSeconds)
	}
}

// RecordConsume increments the legacy consume counter and duration.
func RecordConsume(queue, eventType, status string, durSeconds float64) {
	metricsMu.RLock()
	ct, cd := EventsConsumedTotal, EventsConsumeDuration
	metricsMu.RUnlock()
	if ct == nil && cd == nil {
		return
	}
	et := SanitizeEventType(eventType)
	if ct != nil {
		ct.WithLabelValues(queue, et, status).Inc()
	}
	if cd != nil {
		cd.WithLabelValues(queue, et).Observe(durSeconds)
	}
}

// RecordCodecEncode increments the legacy codec-encode counter and duration.
func RecordCodecEncode(topic, eventType, status string, durSeconds float64) {
	metricsMu.RLock()
	ct, cd := CodecEncodeTotal, CodecEncodeDuration
	metricsMu.RUnlock()
	if ct == nil && cd == nil {
		return
	}
	et := SanitizeEventType(eventType)
	if ct != nil {
		ct.WithLabelValues(topic, et, status).Inc()
	}
	if cd != nil {
		cd.WithLabelValues(topic, et).Observe(durSeconds)
	}
}

// RecordCodecDecode increments the legacy codec-decode counter and duration.
func RecordCodecDecode(queue, eventType, status string, durSeconds float64) {
	metricsMu.RLock()
	ct, cd := CodecDecodeTotal, CodecDecodeDuration
	metricsMu.RUnlock()
	if ct == nil && cd == nil {
		return
	}
	et := SanitizeEventType(eventType)
	if ct != nil {
		ct.WithLabelValues(queue, et, status).Inc()
	}
	if cd != nil {
		cd.WithLabelValues(queue, et).Observe(durSeconds)
	}
}

// SetOutboxPending updates the pending-outbox gauges. n = -1 signals a failed
// count query: the legacy gauge is set to -1, the Tier 1 gauge keeps its last
// value and platform_outbox_errors_total{operation="pending_count"} counts it.
func SetOutboxPending(n float64) {
	metricsMu.RLock()
	g := OutboxPendingTotal
	metricsMu.RUnlock()
	if g != nil {
		g.WithLabelValues().Set(n)
	}
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

// SetOutboxLeased updates the leased-outbox gauges.
func SetOutboxLeased(n float64) {
	metricsMu.RLock()
	g := OutboxLeasedTotal
	metricsMu.RUnlock()
	if g != nil {
		g.WithLabelValues().Set(n)
	}
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

// HasOutboxPendingMetric reports whether any pending-outbox gauge is
// registered, so the runner can skip the count query otherwise.
func HasOutboxPendingMetric() bool {
	metricsMu.RLock()
	g := OutboxPendingTotal
	metricsMu.RUnlock()
	p := platform.Load()
	return g != nil || (p != nil && p.OutboxPending != nil)
}

// HasOutboxLeasedMetric reports whether any leased-outbox gauge is registered.
func HasOutboxLeasedMetric() bool {
	metricsMu.RLock()
	g := OutboxLeasedTotal
	metricsMu.RUnlock()
	p := platform.Load()
	return g != nil || (p != nil && p.OutboxLeased != nil)
}

// RecordOutboxDeadLettersReprocessed counts n dead-letter records moved back
// to outbox_events.
func RecordOutboxDeadLettersReprocessed(n int) {
	recordDeadLetterOperation("reprocess", float64(n), func() *prometheus.CounterVec { return OutboxDeadLettersReprocessedTotal })
}

// RecordOutboxDeadLettersDiscarded counts n dead-letter records deleted.
func RecordOutboxDeadLettersDiscarded(n int64) {
	recordDeadLetterOperation("discard", float64(n), func() *prometheus.CounterVec { return OutboxDeadLettersDiscardedTotal })
}

func recordDeadLetterOperation(op string, n float64, legacy func() *prometheus.CounterVec) {
	if n <= 0 {
		return
	}
	metricsMu.RLock()
	c := legacy()
	metricsMu.RUnlock()
	if c != nil {
		c.WithLabelValues().Add(n)
	}
	if p := platform.Load(); p != nil && p.OutboxDLOperations != nil {
		p.OutboxDLOperations.WithLabelValues(op).Add(n)
	}
}

// RecordOutboxDeadLetter counts one outbox record moved to
// outbox_dead_letters after exhausting its attempts.
func RecordOutboxDeadLetter(eventType string) {
	metricsMu.RLock()
	c := OutboxDeadLettersTotal
	metricsMu.RUnlock()
	if c != nil {
		c.WithLabelValues(SanitizeEventType(eventType)).Inc()
	}
	IncDLQ("outbox_publish", eventType, "max_attempts")
}

// RecordOutboxAttempt increments the legacy per-record attempt counter (the
// Tier 1 attempt counter is fed by RecordOutboxPublished, which knows the outcome).
func RecordOutboxAttempt(eventType string) {
	metricsMu.RLock()
	c := OutboxAttemptsTotal
	metricsMu.RUnlock()
	if c != nil {
		c.WithLabelValues(SanitizeEventType(eventType)).Inc()
	}
}

// RecordOutboxPublished counts one outbox publish attempt by status
// ("success" or "error").
func RecordOutboxPublished(eventType, status string) {
	metricsMu.RLock()
	c := OutboxPublishedTotal
	metricsMu.RUnlock()
	et := SanitizeEventType(eventType)
	if c != nil {
		c.WithLabelValues(et, status).Inc()
	}
	if p := platform.Load(); p != nil && p.OutboxAttempts != nil {
		p.OutboxAttempts.WithLabelValues(et, status).Inc()
	}
}

// RecordInboxDuplicate increments the legacy inbox duplicate counter.
func RecordInboxDuplicate(consumer string) {
	metricsMu.RLock()
	c := InboxDuplicatesTotal
	metricsMu.RUnlock()
	if c != nil {
		c.WithLabelValues(consumer).Inc()
	}
}

// RecordDLQForward increments the legacy DLQ forward counter.
func RecordDLQForward(queue, eventType, status string) {
	metricsMu.RLock()
	c := DLQForwardedTotal
	metricsMu.RUnlock()
	if c != nil {
		c.WithLabelValues(queue, SanitizeEventType(eventType), status).Inc()
	}
}

// RecordSQSReceiveError increments the legacy ReceiveMessage error counter.
func RecordSQSReceiveError(queue string) {
	incLegacy(func() *prometheus.CounterVec { return SQSReceiveErrorsTotal }, queue)
}

// RecordSQSDeleteError increments the legacy DeleteMessage error counter.
func RecordSQSDeleteError(queue string) {
	incLegacy(func() *prometheus.CounterVec { return SQSDeleteErrorsTotal }, queue)
}

// RecordSQSVisibilityError increments the legacy ChangeMessageVisibility error counter.
func RecordSQSVisibilityError(queue string) {
	incLegacy(func() *prometheus.CounterVec { return SQSVisibilityErrorsTotal }, queue)
}

// RecordOutboxPollError counts a failed outbox poll cycle.
func RecordOutboxPollError() {
	incLegacy(func() *prometheus.CounterVec { return OutboxPollErrorsTotal })
	if p := platform.Load(); p != nil {
		incOutboxError(p, "poll")
	}
}

// RecordOutboxUnmarshalError counts a stored envelope that failed to unmarshal.
func RecordOutboxUnmarshalError() {
	incLegacy(func() *prometheus.CounterVec { return OutboxUnmarshalErrorsTotal })
	if p := platform.Load(); p != nil {
		incOutboxError(p, "unmarshal")
	}
}

// RecordOutboxMarkPublishedError counts a MarkPublished failure after a
// successful publish (the event will be delivered again).
func RecordOutboxMarkPublishedError() {
	incLegacy(func() *prometheus.CounterVec { return OutboxMarkPublishedErrorsTotal })
	if p := platform.Load(); p != nil {
		incOutboxError(p, "mark_published")
	}
}

func incOutboxError(p *Platform, op string) {
	if p.OutboxErrors != nil {
		p.OutboxErrors.WithLabelValues(op).Inc()
	}
}

func incLegacy(get func() *prometheus.CounterVec, labels ...string) {
	metricsMu.RLock()
	c := get()
	metricsMu.RUnlock()
	if c != nil {
		c.WithLabelValues(labels...).Inc()
	}
}
