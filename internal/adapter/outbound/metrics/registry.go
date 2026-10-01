package metrics

// This file is platform-events' entry in the Platform Observability Registry
// required by the Enterprise Platform Observability Standard: every metric the
// library registers, its tier, governance status and ratification packet
// (semantic definition, required labels, approved labels and their allowed
// values, cardinality justification, aggregation expectations). There is no
// live registry service, so this checked-in list is the source of truth that
// CI enforces the real instrumentation against (test/unit/metrics/
// standard_test.go), that the inventory is rendered from
// (docs/observability/metrics-registry.md) and that the rule-file lint checks
// against (monitoring/prometheus).
//
// Classification (the standard's decision tree): platform-events is a
// platform library. Every service in every domain that publishes, consumes,
// dead-letters or deduplicates events through it emits these metrics with
// identical semantics, because the library defines them. They are therefore
// Tier 1 (platform_*). A library has no domain or service of its own, so Tier
// 2 (<domain>_*) and Tier 3 (<domain>_<service>_*) do not apply — services
// add those for their own business signals.
//
// The pre-standard events_* / outbox_* / sqs_* names (and
// platform_events_build_info) fit no tier and were removed outright — no
// release emitting them was ever deployed, so the standard's compatibility
// period did not apply. The central Platform Observability Registry
// (platform-gincommon) keeps them as status "removed" with their successors;
// that is the single historical record.

// RegistryStatus is a metric's governance status.
type RegistryStatus string

const (
	// StatusCanonical: named as a canonical platform metric in the standard
	// itself (or ratified by governance). Safe for alerts, dashboards,
	// recording rules, SLOs and HPA.
	StatusCanonical RegistryStatus = "canonical"

	// StatusProposed: submitted for ratification, not yet approved (rules
	// 11/12). Shadow-emitted only — no alert, recording rule, SLO or HPA may
	// depend on it until governance marks it Canonical.
	StatusProposed RegistryStatus = "proposed"
)

// Tier is the standard's metric tier.
type Tier string

// Tiers.
const (
	TierPlatform Tier = "platform" // Tier 1: platform_*
)

// MetricType is the Prometheus metric type.
type MetricType string

// Metric types.
const (
	TypeCounter   MetricType = "counter"
	TypeHistogram MetricType = "histogram"
	TypeGauge     MetricType = "gauge"
)

// RegistryEntry is one metric's registry record and ratification packet.
type RegistryEntry struct {
	Name   string
	Type   MetricType
	Tier   Tier
	Status RegistryStatus

	// SemanticDefinition holds regardless of which service or domain emits it.
	SemanticDefinition string

	// RequiredLabels are injected centrally at registration —
	// {domain, service, environment} for Tier 1 (rule 8).
	RequiredLabels []string

	// ApprovedLabels are the variable labels this entry uses. Any other label
	// is outside the metric's contract.
	ApprovedLabels []string

	// LabelValues is the allowed value set of each approved label;
	// LabelValueRules documents the bound of a label whose values cannot be
	// enumerated here.
	LabelValues     map[string][]string
	LabelValueRules map[string]string

	// Cardinality justifies the label set against the high-cardinality ban.
	Cardinality string

	// AggregationNotes: how the metric is queried across services/domains.
	AggregationNotes string

	// GovernanceNotes records open questions for the reviewing body.
	GovernanceNotes string
}

// Identity labels (Tier 1 required labels).
const (
	LabelDomain      = "domain"
	LabelService     = "service"
	LabelEnvironment = "environment"
)

// PlatformRequiredLabels are the standard's mandated Tier 1 labels.
var PlatformRequiredLabels = []string{LabelDomain, LabelService, LabelEnvironment}

// ProhibitedLabels are forbidden on every metric (high-cardinality rule, plus
// transport identifiers that are equally unbounded).
var ProhibitedLabels = []string{
	"user_id", "email", "tenant_id", "request_id", "event_id", "session_id",
	"message_id", "trace_id", "span_id", "correlation_id", "subject", "actor",
}

// Approved label vocabularies. A label name means the same thing on every
// metric that uses it; the per-entry LabelValues lists which values that
// metric can take.
var (
	OutcomeValues = []string{"success", "error"}

	// FailureReasonValues: why a received message was not processed.
	FailureReasonValues = []string{"malformed", "decode_error", "handler_error", "handler_panic", "dead_letter_error"}

	// DLQReasonValues: why a message was dead-lettered.
	DLQReasonValues = []string{"malformed", "decode_error", "max_receive_count", "explicit", "max_attempts"}

	// FlowOperationValues: the message flow an event was in.
	FlowOperationValues = []string{"consume", "outbox_publish"}

	// DependencyValues: the external systems the library calls.
	DependencyValues = []string{"sns", "sqs", "codec"}

	// DependencyOperationValues: calls made to those dependencies.
	DependencyOperationValues = []string{
		"publish", "publish_batch", // sns
		"receive_message", "delete_message", "change_message_visibility", "send_message", "get_queue_attributes", "get_queue_url", // sqs
		"encode", "decode", // codec
	}

	// DependencyOperations lists the valid operations of each dependency.
	DependencyOperations = map[string][]string{
		"sns":   {"publish", "publish_batch"},
		"sqs":   {"receive_message", "delete_message", "change_message_visibility", "send_message", "get_queue_attributes", "get_queue_url"},
		"codec": {"encode", "decode"},
	}

	// OutboxErrorOperationValues: outbox runner steps that can fail outside
	// a publish attempt.
	OutboxErrorOperationValues = []string{"poll", "unmarshal", "mark_published", "pending_count", "leased_count", "oldest_pending", "blocked_count"}

	// TimeoutOperationValues: the processing stage a WithHandlerTimeout
	// deadline expired in.
	TimeoutOperationValues = []string{"decode", "dead_letter_handler", "handler"}

	// DeadLetterOperationValues: operator actions on outbox_dead_letters.
	DeadLetterOperationValues = []string{"reprocess", "discard"}

	// OverflowLabelValues: labels whose values the library caps.
	OverflowLabelValues = []string{"event_type"}

	// LibraryValues: the library emitting platform_library_info.
	LibraryValues = []string{LibraryName}
)

// LibraryName is this library's platform_library_info identifier.
const LibraryName = "platform-events"

// Label value bounds that cannot be enumerated here.
const (
	QueueLabelRule = "SQS queue name — the last path segment of the queue URL (e.g. `orders`, `orders.fifo`), never the full URL (it carries the account ID). Bounded by the queues a service consumes (typically 1–5). `unknown` when the queue cannot be determined (e.g. the inbox wrapper used outside the SQS consumer)."

	TopicLabelRule = "SNS topic name — the last segment of the topic ARN (e.g. `iam-events`, `orders.fifo`), never the full ARN. Bounded by the topics a service publishes to (typically 1–3)."

	EventTypeLabelRule = "Envelope `type` (`<domain>.<entity>.<past-tense-verb>[.v<N>]`), expected to come from the event-type registry in EVENT_SCHEMA_GOVERNANCE.md — and enforced in-process: at most 200 distinct values per process (`events.WithEventTypeLimit`), further ones recorded as `__other__`; values over 128 bytes as `__oversized__`; an empty or unparseable type as `unknown`. Replacements are counted in platform_telemetry_label_overflow_total."

	LibraryVersionLabelRule = "The platform-events module version the service was built with (e.g. `v1.6.0`), `devel` for an unreleased build, `unknown` when build info is unavailable. One value per deployment; changes only on a library upgrade."
)

const proposedGovernance = "New platform_* name (not among the standard's canonical or registry-proposed examples); submitted under the Registry Ratification Requirement. Shadow-emitted until ratified."

func platformEntry(status RegistryStatus, e RegistryEntry) RegistryEntry {
	e.Tier = TierPlatform
	e.Status = status
	e.RequiredLabels = PlatformRequiredLabels
	if e.GovernanceNotes == "" && status == StatusProposed {
		e.GovernanceNotes = proposedGovernance
	}
	return e
}

// Registry returns every metric platform-events registers.
func Registry() []RegistryEntry {
	eventType := map[string]string{"event_type": EventTypeLabelRule}
	queueAndType := map[string]string{"queue": QueueLabelRule, "event_type": EventTypeLabelRule}
	return []RegistryEntry{
		// ── Tier 1, Canonical (named by the standard) ───────────────────
		platformEntry(StatusCanonical, RegistryEntry{
			Name:               "platform_messages_received_total",
			Type:               TypeCounter,
			SemanticDefinition: "A message delivered to a consumer by its queue (one per SQS delivery, redeliveries included), counted on receipt before it is parsed.",
			ApprovedLabels:     []string{"queue"},
			LabelValueRules:    map[string]string{"queue": QueueLabelRule},
			Cardinality:        "queue (≤5 per service).",
			AggregationNotes:   "Inbound rate per service: sum by (domain, service) (rate(platform_messages_received_total[5m])). Platform-wide: sum by (domain) (…).",
			GovernanceNotes:    "Canonical name per the standard. queue is an approved dimension; its value is the queue name, not the URL.",
		}),
		platformEntry(StatusCanonical, RegistryEntry{
			Name:               "platform_messages_processed_total",
			Type:               TypeCounter,
			SemanticDefinition: "A received message whose handler completed successfully, after which the message was acknowledged (deleted). Messages dead-lettered instead are counted in platform_dlq_messages_total, not here.",
			ApprovedLabels:     []string{"queue", "event_type"},
			LabelValueRules:    queueAndType,
			Cardinality:        "queue (≤5) × event_type (registered types a service consumes, typically ≤30).",
			AggregationNotes:   `Success ratio: sum by (domain, service) (rate(platform_messages_processed_total[5m])) / sum by (domain, service) (rate(platform_messages_received_total[5m])).`,
			GovernanceNotes:    "Canonical name per the standard. event_type is an approved dimension.",
		}),
		platformEntry(StatusCanonical, RegistryEntry{
			Name:               "platform_messages_failed_total",
			Type:               TypeCounter,
			SemanticDefinition: "A received message that could not be processed on this delivery, by reason: malformed (not a valid envelope), decode_error (schema-registry codec failure), handler_error, handler_panic, or dead_letter_error (dead-letter handling or DLQ forwarding failed). A failed message is retried or dead-lettered; see platform_retry_total and platform_dlq_messages_total.",
			ApprovedLabels:     []string{"queue", "event_type", "reason"},
			LabelValues:        map[string][]string{"reason": FailureReasonValues},
			LabelValueRules:    queueAndType,
			Cardinality:        "queue (≤5) × event_type (≤30) × reason (5); malformed always has event_type=unknown.",
			AggregationNotes:   `Failure ratio: sum by (domain, service) (rate(platform_messages_failed_total[5m])) / sum by (domain, service) (rate(platform_messages_received_total[5m])). Poison producers: sum by (domain, service, queue) (rate(platform_messages_failed_total{reason="malformed"}[15m])).`,
			GovernanceNotes:    "Canonical name per the standard. The reason vocabulary is requested for approval.",
		}),
		platformEntry(StatusCanonical, RegistryEntry{
			Name:               "platform_retry_total",
			Type:               TypeCounter,
			SemanticDefinition: "An attempt that failed and was left for automatic retry: a consumed message whose processing failed and stays on its queue for redelivery (operation=consume), or an outbox publish that failed with attempts remaining (operation=outbox_publish).",
			ApprovedLabels:     []string{"operation", "event_type"},
			LabelValues:        map[string][]string{"operation": FlowOperationValues},
			LabelValueRules:    eventType,
			Cardinality:        "operation (2) × event_type (≤30).",
			AggregationNotes:   "Retry pressure per service: sum by (domain, service, operation) (rate(platform_retry_total[5m])).",
			GovernanceNotes:    "Canonical name per the standard, but IAM services already register platform_retry_total with conflicting label sets ({target_service,endpoint}; {event_type,reason}). In such a service registration is refused and reported as a RegistrationWarning (fail-soft), so the reference alerts do not depend on this metric until governance approves ONE label vocabulary.",
		}),
		platformEntry(StatusCanonical, RegistryEntry{
			Name:               "platform_dlq_messages_total",
			Type:               TypeCounter,
			SemanticDefinition: "A message moved to dead-letter storage: forwarded to its SQS queue's RedrivePolicy DLQ (operation=consume), or moved to outbox_dead_letters after exhausting its publish attempts (operation=outbox_publish), by reason.",
			ApprovedLabels:     []string{"operation", "event_type", "reason"},
			LabelValues:        map[string][]string{"operation": FlowOperationValues, "reason": DLQReasonValues},
			LabelValueRules:    eventType,
			Cardinality:        "operation (2) × event_type (≤30) × reason (5).",
			AggregationNotes:   "Dead-letter inflow: sum by (domain, service, operation, reason) (increase(platform_dlq_messages_total[15m])). Any sustained non-zero rate needs attention.",
			GovernanceNotes:    "Canonical name per the standard. operation and reason vocabularies are requested for approval.",
		}),

		// ── Tier 1, Proposed (registry-proposed examples in the standard) ─
		platformEntry(StatusProposed, RegistryEntry{
			Name:               "platform_duplicate_messages_total",
			Type:               TypeCounter,
			SemanticDefinition: "A redelivered message acknowledged without running its handler because the consumer's dedup ledger (inbox processed_events) had already recorded its event ID.",
			ApprovedLabels:     []string{"queue", "event_type"},
			LabelValueRules:    queueAndType,
			Cardinality:        "queue (≤5) × event_type (≤30).",
			AggregationNotes:   "Duplicate ratio: sum by (domain, service) (rate(platform_duplicate_messages_total[15m])) / sum by (domain, service) (rate(platform_messages_received_total[15m])).",
			GovernanceNotes:    "Registry-proposed example in the standard. Labelled by queue, not by the inbox ledger's consumer name, so it joins with the other consumer metrics.",
		}),
		platformEntry(StatusProposed, RegistryEntry{
			Name:               "platform_dependency_request_seconds",
			Type:               TypeHistogram,
			SemanticDefinition: "Client-observed wall time of one call to an external dependency — SNS (publish, publish_batch), SQS (receive_message, delete_message, change_message_visibility, send_message, get_queue_attributes, get_queue_url) or the configured schema-registry codec (encode, decode) — by outcome. The _count series counts calls. receive_message is a long poll: its duration includes up to WaitTimeSeconds (default 20 s) of waiting on an empty queue, so exclude it from latency views (it stays meaningful for error rates).",
			ApprovedLabels:     []string{"dependency", "operation", "outcome"},
			LabelValues:        map[string][]string{"dependency": DependencyValues, "operation": DependencyOperationValues, "outcome": OutcomeValues},
			Cardinality:        "dependency × operation (10 valid pairs) × outcome (2) × 12 buckets.",
			AggregationNotes:   `Error ratio: sum by (domain, service, dependency) (rate(platform_dependency_request_seconds_count{outcome="error"}[5m])) / sum by (domain, service, dependency) (rate(platform_dependency_request_seconds_count[5m])). p99 (long-poll receives excluded): histogram_quantile(0.99, sum by (le, domain, service, dependency, operation) (rate(platform_dependency_request_seconds_bucket{operation!="receive_message"}[5m]))).`,
			GovernanceNotes:    "Registry-proposed example in the standard. IAM services register this name with conflicting label sets ({target_service,endpoint} vs {dependency,operation,outcome}); platform-events uses {dependency,operation,outcome}. Where a service's registry already holds another shape the metric is disabled with a RegistrationWarning (fail-soft).",
		}),
		platformEntry(StatusProposed, RegistryEntry{
			Name:               "platform_event_propagation_seconds",
			Type:               TypeHistogram,
			SemanticDefinition: "Time from an event's creation (envelope `time`, set by the producer) to its FIRST receipt by a consumer (ApproximateReceiveCount ≤ 1) — end-to-end propagation delay through the outbox, SNS and SQS. Redeliveries are not observed, so retry delay does not inflate it. Negative clock skew is clamped to 0.",
			ApprovedLabels:     []string{"queue", "event_type"},
			LabelValueRules:    queueAndType,
			Cardinality:        "queue (≤5) × event_type (≤30) × 14 buckets.",
			AggregationNotes:   "p95 propagation per consumer: histogram_quantile(0.95, sum by (le, domain, service, queue) (rate(platform_event_propagation_seconds_bucket[5m]))). Includes producer clock skew.",
			GovernanceNotes:    "Registry-proposed example in the standard. New signal.",
		}),

		platformEntry(StatusProposed, RegistryEntry{
			Name:               "platform_queue_depth",
			Type:               TypeGauge,
			SemanticDefinition: "Messages waiting on a consumer's queue to be received (SQS ApproximateNumberOfMessages — visible, not in flight or delayed), sampled by the consumer every WithQueueDepthMetrics interval. Approximate by SQS design.",
			ApprovedLabels:     []string{"queue"},
			LabelValueRules:    map[string]string{"queue": QueueLabelRule},
			Cardinality:        "queue (≤5 per service).",
			AggregationNotes:   "Every replica samples the same queue, so aggregate with max, not sum: max by (domain, service, queue) (platform_queue_depth). The natural HPA/KEDA scaling signal once ratified.",
			GovernanceNotes:    "Registry-proposed example in the standard. Emitted by platform-events because consuming services may not use the SQS SDK themselves (depguard); opt-in (WithQueueDepthMetrics) since each replica polls sqs:GetQueueAttributes.",
		}),
		platformEntry(StatusProposed, RegistryEntry{
			Name:               "platform_dlq_depth",
			Type:               TypeGauge,
			SemanticDefinition: "Messages sitting in the dead-letter queue attached to a consumer's queue by its RedrivePolicy (SQS ApproximateNumberOfMessages of the DLQ), sampled with platform_queue_depth. queue is the SOURCE queue's name, so the gauge joins with the consumer's other queue metrics.",
			ApprovedLabels:     []string{"queue"},
			LabelValueRules:    map[string]string{"queue": QueueLabelRule},
			Cardinality:        "queue (≤5 per service).",
			AggregationNotes:   "max by (domain, service, queue) (platform_dlq_depth) > 0 — messages awaiting investigation or replay. Complements platform_dlq_messages_total (inflow) with the backlog.",
			GovernanceNotes:    "Registry-proposed example in the standard. Not emitted when the queue has no RedrivePolicy.",
		}),

		// ── Tier 1, Proposed (new names) ─────────────────────────────────
		platformEntry(StatusProposed, RegistryEntry{
			Name:               "platform_messages_in_flight",
			Type:               TypeGauge,
			SemanticDefinition: "Messages a consumer has received and is currently processing (decode, dead-letter routing or handler), per replica. At the consumer's concurrency limit for long periods means the replica is saturated; a value that never drops points at a hung handler (see WithHandlerTimeout).",
			ApprovedLabels:     []string{"queue"},
			LabelValueRules:    map[string]string{"queue": QueueLabelRule},
			Cardinality:        "queue (≤5 per service).",
			AggregationNotes:   "sum by (domain, service, queue) (platform_messages_in_flight) for total work in progress; compare per replica against the configured concurrency for saturation.",
			GovernanceNotes:    "Proposed by platform-events. Complements platform_queue_depth (waiting) with work in progress.",
		}),
		platformEntry(StatusProposed, RegistryEntry{
			Name:               "platform_messages_published_total",
			Type:               TypeCounter,
			SemanticDefinition: "One event a producer attempted to publish to a topic, by outcome (error includes validation, encoding and broker failures; a batch counts each message).",
			ApprovedLabels:     []string{"topic", "event_type", "outcome"},
			LabelValues:        map[string][]string{"outcome": OutcomeValues},
			LabelValueRules:    map[string]string{"topic": TopicLabelRule, "event_type": EventTypeLabelRule},
			Cardinality:        "topic (≤3) × event_type (≤30) × outcome (2).",
			AggregationNotes:   `Publish error ratio: sum by (domain, service, topic) (rate(platform_messages_published_total{outcome="error"}[5m])) / sum by (domain, service, topic) (rate(platform_messages_published_total[5m])).`,
			GovernanceNotes:    proposedGovernance + " The producer-side counterpart of platform_messages_received_total; requests approval of a topic label.",
		}),
		platformEntry(StatusProposed, RegistryEntry{
			Name:               "platform_message_processing_duration_seconds",
			Type:               TypeHistogram,
			SemanticDefinition: "Wall time a consumer's handler (or dead-letter handler) spent on one delivery, whatever the outcome.",
			ApprovedLabels:     []string{"queue", "event_type"},
			LabelValueRules:    queueAndType,
			Cardinality:        "queue (≤5) × event_type (≤30) × 14 buckets.",
			AggregationNotes:   "p99 handler latency: histogram_quantile(0.99, sum by (le, domain, service, queue) (rate(platform_message_processing_duration_seconds_bucket[5m]))).",
		}),
		platformEntry(StatusProposed, RegistryEntry{
			Name:               "platform_outbox_pending_events",
			Type:               TypeGauge,
			SemanticDefinition: "Events in a service's transactional outbox that are due to be published (unpublished, not leased and not waiting out a retry backoff), sampled every outbox GaugeInterval (default 15s) and capped at 100000 (a reading of 100000 means at least that many). Left at its last value when the count query fails (see platform_outbox_errors_total{operation=\"pending_count\"}).",
			Cardinality:        "One series per service instance.",
			AggregationNotes:   "Backlog per service: max by (domain, service) (platform_outbox_pending_events) — every runner reads the same table, so use max, not sum.",
		}),
		platformEntry(StatusProposed, RegistryEntry{
			Name:               "platform_outbox_leased_events",
			Type:               TypeGauge,
			SemanticDefinition: "Unpublished outbox events not yet due: claimed by a runner and being published, or waiting out a retry backoff. Sampled every GaugeInterval, capped at 100000.",
			Cardinality:        "One series per service instance.",
			AggregationNotes:   "max by (domain, service) (platform_outbox_leased_events).",
		}),
		platformEntry(StatusProposed, RegistryEntry{
			Name:               "platform_outbox_oldest_pending_age",
			Type:               TypeGauge,
			SemanticDefinition: "Age of the oldest unpublished outbox event (now − created_at), 0 when nothing is unpublished, sampled every GaugeInterval. Transient publish failures never dead-letter, so this is the signal that delivery has stopped even when the backlog is small (e.g. a credential outage on a low-volume service).",
			Cardinality:        "One series per service instance.",
			AggregationNotes:   "max by (domain, service) (platform_outbox_oldest_pending_age) — every runner reads the same table. Alert on it staying above the delivery-latency objective (e.g. > 600 for 10m) once ratified.",
			GovernanceNotes:    "Proposed by platform-events. Left at its last value when the query fails (platform_outbox_errors_total{operation=\"oldest_pending\"}).",
		}),
		platformEntry(StatusProposed, RegistryEntry{
			Name:               "platform_outbox_ordering_blocked_events",
			Type:               TypeGauge,
			SemanticDefinition: "Ordered outbox events (outbox.EnqueueOrdered) waiting behind an earlier unpublished record with the same ordering key. Sampled every GaugeInterval, capped at 100000. A value that keeps growing means a key's head record keeps failing and holds the rest of its key.",
			Cardinality:        "One series per service instance.",
			AggregationNotes:   "max by (domain, service) (platform_outbox_ordering_blocked_events) — every runner reads the same table.",
			GovernanceNotes:    "Proposed by platform-events with per-key ordering. Left at its last value when the query fails (platform_outbox_errors_total{operation=\"blocked_count\"}).",
		}),
		platformEntry(StatusProposed, RegistryEntry{
			Name:               "platform_message_timeouts_total",
			Type:               TypeCounter,
			SemanticDefinition: "A received message whose WithHandlerTimeout deadline expired, by the stage it expired in: decode (codec decode), dead_letter_handler, or handler. Each is also counted in platform_messages_failed_total under its usual reason (decode_error, dead_letter_error, handler_error) — this counter separates timeouts from other failures without changing that Canonical metric's vocabulary.",
			ApprovedLabels:     []string{"queue", "event_type", "operation"},
			LabelValueRules:    queueAndType,
			LabelValues:        map[string][]string{"operation": TimeoutOperationValues},
			Cardinality:        "queue (≤5) × event_type (≤30) × operation (3).",
			AggregationNotes:   "Timeout share of failures: sum by (domain, service, queue) (rate(platform_message_timeouts_total[5m])) / sum by (domain, service, queue) (rate(platform_messages_failed_total[5m])).",
			GovernanceNotes:    "Proposed by platform-events. Emitted only when the consumer uses WithHandlerTimeout.",
		}),
		platformEntry(StatusProposed, RegistryEntry{
			Name:               "platform_outbox_publish_attempts_total",
			Type:               TypeCounter,
			SemanticDefinition: "One attempt by the outbox runner to publish a claimed outbox event, by outcome.",
			ApprovedLabels:     []string{"event_type", "outcome"},
			LabelValues:        map[string][]string{"outcome": OutcomeValues},
			LabelValueRules:    eventType,
			Cardinality:        "event_type (≤30) × outcome (2).",
			AggregationNotes:   `Outbox publish error ratio: sum by (domain, service) (rate(platform_outbox_publish_attempts_total{outcome="error"}[5m])) / sum by (domain, service) (rate(platform_outbox_publish_attempts_total[5m])).`,
		}),
		platformEntry(StatusProposed, RegistryEntry{
			Name:               "platform_outbox_errors_total",
			Type:               TypeCounter,
			SemanticDefinition: "An outbox runner step that failed outside a publish attempt: poll (claiming a batch), unmarshal (a stored envelope that no longer parses), mark_published (an event published but not marked — it will be delivered again), pending_count / leased_count / oldest_pending / blocked_count (a gauge query).",
			ApprovedLabels:     []string{"operation"},
			LabelValues:        map[string][]string{"operation": OutboxErrorOperationValues},
			Cardinality:        "operation (7).",
			AggregationNotes:   "sum by (domain, service, operation) (rate(platform_outbox_errors_total[5m])) > 0.",
		}),
		platformEntry(StatusProposed, RegistryEntry{
			Name:               "platform_outbox_dead_letter_operations_total",
			Type:               TypeCounter,
			SemanticDefinition: "Outbox dead-letter records acted on by an operator call: reprocess (moved back to outbox_events for retry) or discard (permanently deleted). Counts records, not calls.",
			ApprovedLabels:     []string{"operation"},
			LabelValues:        map[string][]string{"operation": DeadLetterOperationValues},
			Cardinality:        "operation (2).",
			AggregationNotes:   "Audit trail: sum by (domain, service, operation) (increase(platform_outbox_dead_letter_operations_total[1d])).",
		}),
		platformEntry(StatusProposed, RegistryEntry{
			Name:               "platform_telemetry_label_overflow_total",
			Type:               TypeCounter,
			SemanticDefinition: "A label value the library replaced because it exceeded its cardinality bound: event_type over 128 bytes → `__oversized__`, or a new distinct event_type beyond the per-process limit (default 200) → `__other__`. Counts replacements. Non-zero means a misconfigured or adversarial producer, or a limit set below the service's real number of event types.",
			ApprovedLabels:     []string{"label"},
			LabelValues:        map[string][]string{"label": OverflowLabelValues},
			Cardinality:        "label (1).",
			AggregationNotes:   "sum by (domain, service, label) (rate(platform_telemetry_label_overflow_total[15m])) > 0.",
		}),
		platformEntry(StatusProposed, RegistryEntry{
			Name:               "platform_library_info",
			Type:               TypeGauge,
			SemanticDefinition: "Always 1. Identifies a platform library and the version a service was built with, for fleet-wide upgrade tracking.",
			ApprovedLabels:     []string{"library", "library_version"},
			LabelValues:        map[string][]string{"library": LibraryValues},
			LabelValueRules:    map[string]string{"library_version": LibraryVersionLabelRule},
			Cardinality:        "One series per service instance.",
			AggregationNotes:   "Services still on an old version: count by (library_version) (platform_library_info{library=\"platform-events\"}).",
			GovernanceNotes:    proposedGovernance + " Intended to be shared by every platform library (platform-pgcommon, platform-gincommon). It carries the library's module version, never the service's own build version (not in the Tier 1 vocabulary).",
		}),
	}
}

// Lookup returns the registry entry for name.
func Lookup(name string) (RegistryEntry, bool) {
	for _, e := range Registry() {
		if e.Name == name {
			return e, true
		}
	}
	return RegistryEntry{}, false
}
