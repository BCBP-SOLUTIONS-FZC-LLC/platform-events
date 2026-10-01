package sqs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/adapter/outbound/metrics"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/port"
)

// Standard message attributes the DLQ publisher sets on every forwarded
// message. They take precedence over caller-supplied attributes of the same
// name (EventType excepted — see SendToDLQ).
const (
	DLQAttrEventType     = "EventType"
	DLQAttrReason        = "DLQReason"
	DLQAttrOriginalQueue = "OriginalQueue"
	DLQAttrConsumerName  = "ConsumerName"
	DLQAttrFailedAt      = "FailedAt"
)

const (
	// maxSQSMessageAttributes is the SQS hard limit on attributes per message.
	maxSQSMessageAttributes = 10
	// maxDLQReasonBytes caps DLQReason so a verbose error string (e.g. one
	// embedding a stack trace) cannot eat into the 256 KiB message size limit.
	maxDLQReasonBytes = 1024
	// maxSQSMessageBytes is the SQS hard limit on body + message attributes
	// (names, data types and values). A queue may configure a lower
	// MaximumMessageSize; SQS rejects those sends with InvalidParameterValue,
	// which is reported as ErrDLQInvalidMessage.
	maxSQSMessageBytes = 1 << 20
	// maxSQSAttributeNameLen is the SQS limit on a message attribute name.
	maxSQSAttributeNameLen = 256
	// maxFIFOIDLen is the SQS limit on MessageGroupId / MessageDeduplicationId.
	maxFIFOIDLen    = 128
	fifoQueueSuffix = ".fifo"
	// defaultDLQCacheTTL bounds how long a resolved DLQ is reused, so a
	// RedrivePolicy retargeted at a different DLQ is picked up without a restart.
	defaultDLQCacheTTL = 15 * time.Minute
)

// callerAttributePriority orders caller attributes kept first when trimming to
// the SQS attribute limit: the SNS publisher's routing attributes, then W3C
// trace context. Attributes not listed follow in lexical order.
var callerAttributePriority = []string{"TenantID", "EventID", "Source", "Subject", "traceparent", "tracestate", "baggage"}

// DLQClientAPI is the subset of the AWS SQS API used by the DLQ publisher.
// *sqs.Client satisfies this interface.
type DLQClientAPI interface {
	GetQueueAttributes(ctx context.Context, params *sqs.GetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
	GetQueueUrl(ctx context.Context, params *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

// ErrNilDLQClient is returned when a DLQ publisher is constructed without a client.
var ErrNilDLQClient = errors.New("sqs: DLQ publisher client is required")

// DLQConfig holds the parameters for constructing a DLQ publisher.
type DLQConfig struct {
	Region      string
	EndpointURL string // optional — set to an AWS emulator (floci) URL for local runs and tests
	// ConsumerName, when set, is attached to every forwarded message as the
	// ConsumerName attribute so operators can tell which consumer gave up.
	ConsumerName string
	Logger       port.Logger
	// Clock stamps FailedAt and ages the resolution cache. Defaults to port.RealClock.
	Clock port.Clock
	// CacheTTL bounds how long a resolved DLQ is reused before the source
	// queue's RedrivePolicy is read again. Zero means 15 minutes; negative
	// disables expiry (resolve once per process).
	CacheTTL time.Duration
	// StrictAttributes rejects a message whose attributes exceed the SQS limit
	// of 10 with ErrDLQInvalidMessage. By default the lowest-priority caller
	// attributes are dropped (and logged) instead, so a message is never kept
	// out of the DLQ because it carried too many attributes.
	StrictAttributes bool
}

// dlqTarget is a resolved dead-letter queue.
type dlqTarget struct {
	url        string
	arn        string
	fifo       bool
	resolvedAt time.Time
}

// DLQPublisher forwards failed messages to a source queue's configured
// dead-letter queue, resolved from the source queue's RedrivePolicy.
// Safe for concurrent use.
type DLQPublisher struct {
	client       DLQClientAPI
	consumerName string
	logger       port.Logger
	clock        port.Clock
	cacheTTL     time.Duration
	strictAttrs  bool

	mu sync.RWMutex
	// cache maps source queue URL → resolved DLQ. Only successful resolutions
	// are cached, so a queue whose RedrivePolicy is added after a failed
	// lookup is picked up on the next call. Entries expire after cacheTTL and
	// are evicted when SendMessage reports the DLQ no longer exists.
	cache map[string]dlqTarget
}

// NewDLQPublisher constructs a DLQ publisher backed by a real SQS client.
func NewDLQPublisher(cfg DLQConfig) (*DLQPublisher, error) {
	startupCtx, startupCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer startupCancel()
	awsCfg, err := awsconfig.LoadDefaultConfig(startupCtx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("sqs: failed to load AWS config: %w", err)
	}

	sqsOpts := []func(*sqs.Options){}
	if cfg.EndpointURL != "" {
		sqsOpts = append(sqsOpts, func(o *sqs.Options) {
			o.BaseEndpoint = aws.String(cfg.EndpointURL)
		})
	}
	return NewDLQPublisherWithClient(cfg, sqs.NewFromConfig(awsCfg, sqsOpts...))
}

// NewDLQPublisherWithClient constructs a DLQ publisher with an injected
// client. Useful in tests to avoid real AWS credentials.
func NewDLQPublisherWithClient(cfg DLQConfig, client DLQClientAPI) (*DLQPublisher, error) {
	if client == nil {
		return nil, ErrNilDLQClient
	}
	clock := cfg.Clock
	if clock == nil {
		clock = port.RealClock{}
	}
	cacheTTL := cfg.CacheTTL
	if cacheTTL == 0 {
		cacheTTL = defaultDLQCacheTTL
	}
	return &DLQPublisher{
		client:       client,
		consumerName: cfg.ConsumerName,
		logger:       cfg.Logger,
		clock:        clock,
		cacheTTL:     cacheTTL,
		strictAttrs:  cfg.StrictAttributes,
		cache:        make(map[string]dlqTarget),
	}, nil
}

// ResolveDLQ returns the URL of sourceQueueURL's dead-letter queue. Call it at
// startup to fail fast when a queue's redrive policy is missing.
func (p *DLQPublisher) ResolveDLQ(ctx context.Context, sourceQueueURL string) (string, error) {
	if sourceQueueURL == "" {
		return "", &domain.DLQError{Kind: domain.ErrDLQInvalidMessage, Cause: errors.New("source queue URL is required")}
	}
	t, err := p.resolve(ctx, sourceQueueURL)
	if err != nil {
		return "", err
	}
	return t.url, nil
}

// SendToDLQ publishes body to sourceQueueURL's dead-letter queue.
//
// attrs are forwarded as String message attributes (entries with an empty
// value are dropped — SQS rejects them). The standard attributes DLQReason,
// OriginalQueue, FailedAt and (when configured) ConsumerName are always set
// and override caller values. EventType is taken from the body when it
// parses as an event envelope, otherwise from attrs["EventType"], otherwise
// "unknown". SQS allows at most 10 attributes per message; beyond that the
// lowest-priority caller attributes are dropped, or the message is rejected
// when StrictAttributes is set.
//
// Input SQS would reject — an empty body or reason, characters outside the
// SQS-allowed set, invalid attribute names, or more than 1 MiB of body plus
// attributes — fails with ErrDLQInvalidMessage before any AWS call.
//
// For a FIFO DLQ, MessageGroupId is the envelope ID, or a SHA-256 of the body
// when it is not an envelope (or its ID is not a valid FIFO identifier), and
// MessageDeduplicationId is unique per call, so a repeated forward of the same
// event is never deduplicated away.
func (p *DLQPublisher) SendToDLQ(ctx context.Context, sourceQueueURL string, body []byte, attrs map[string]string, reason string) error {
	eventType := p.eventTypeFor(body, attrs)
	ctx, span := otel.Tracer("platform-events").Start(ctx, "sqs.dlq_forward", oteltrace.WithSpanKind(oteltrace.SpanKindProducer))
	defer span.End()
	span.SetAttributes(
		attribute.String("messaging.system", "aws_sqs"),
		attribute.String("messaging.operation", "publish"),
		attribute.String("events.event_type", eventType),
		attribute.String("events.dlq.source_queue", sourceQueueURL),
	)

	messageID, err := p.send(ctx, span, sourceQueueURL, body, attrs, reason, eventType)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetAttributes(attribute.String("messaging.message_id", messageID))
		span.SetStatus(codes.Ok, "")
		// Count the dead-lettered message once, with the reason the consumer
		// attributed (explicit for a direct call outside a consumer dispatch).
		attribution := port.DLQAttributionFromContext(ctx)
		metrics.IncDLQ("consume", eventType, attribution.Reason())
		attribution.MarkRecorded()
	}
	return err
}

// ValidateDLQMessage reports, as a *domain.DLQError of kind
// ErrDLQInvalidMessage, input SendToDLQ rejects before calling AWS: an empty
// source queue URL, body or reason; a body or attribute value containing
// characters SQS does not allow; an invalid attribute name; or a body plus
// attributes over the 1 MiB SQS limit. The attribute-count limit is not
// checked here because it depends on the publisher's configuration.
func ValidateDLQMessage(sourceQueueURL string, body []byte, attrs map[string]string, reason string) error {
	invalid := func(format string, a ...any) error {
		return &domain.DLQError{Kind: domain.ErrDLQInvalidMessage, SourceQueue: sourceQueueURL, Cause: fmt.Errorf(format, a...)}
	}
	switch {
	case sourceQueueURL == "":
		return invalid("source queue URL is required")
	case len(body) == 0:
		return invalid("message body is empty")
	case !utf8.Valid(body):
		return invalid("message body is not valid UTF-8")
	case !validSQSText(string(body)):
		return invalid("message body contains characters SQS does not allow")
	case strings.TrimSpace(reason) == "":
		return invalid("reason is required")
	}
	for _, k := range slices.Sorted(maps.Keys(attrs)) {
		v := attrs[k]
		if v == "" {
			continue // dropped, never sent
		}
		if err := validateAttributeName(k); err != nil {
			return invalid("message attribute %q: %v", k, err)
		}
		if !utf8.ValidString(v) || !validSQSText(v) {
			return invalid("message attribute %q value contains characters SQS does not allow", k)
		}
	}
	if n := messageSize(body, attrs); n > maxSQSMessageBytes {
		return invalid("message body and attributes total %d bytes, over the SQS limit of %d", n, maxSQSMessageBytes)
	}
	return nil
}

func (p *DLQPublisher) send(ctx context.Context, span oteltrace.Span, sourceQueueURL string, body []byte, attrs map[string]string, reason, eventType string) (string, error) {
	invalid := func(format string, a ...any) error {
		return &domain.DLQError{Kind: domain.ErrDLQInvalidMessage, SourceQueue: sourceQueueURL, Cause: fmt.Errorf(format, a...)}
	}

	// Trim before validating, so an attribute that is dropped anyway cannot
	// fail the send; only what will actually be sent is validated.
	attrs, dropped := p.fitAttributes(attrs)
	if dropped != nil && p.strictAttrs {
		return "", invalid("%d message attributes exceeds the SQS limit of %d (%d are reserved by the DLQ publisher)",
			len(attrs)+len(dropped)+p.reservedAttributeCount(), maxSQSMessageAttributes, p.reservedAttributeCount())
	}
	if err := ValidateDLQMessage(sourceQueueURL, body, attrs, reason); err != nil {
		return "", err
	}
	if dropped != nil {
		if p.logger != nil {
			p.logger.Warn("sqs: dropped message attributes over the SQS limit when forwarding to DLQ", map[string]any{
				"source_queue":       sourceQueueURL,
				"event_type":         eventType,
				"dropped_attributes": dropped,
			})
		}
	}
	msgAttrs := p.buildAttributes(sourceQueueURL, attrs, reason, eventType)
	if n := messageSizeSQS(body, msgAttrs); n > maxSQSMessageBytes {
		return "", invalid("message body and attributes total %d bytes, over the SQS limit of %d", n, maxSQSMessageBytes)
	}

	target, err := p.resolve(ctx, sourceQueueURL)
	if err != nil {
		return "", err
	}
	span.SetAttributes(attribute.String("messaging.destination", target.url))

	input := &sqs.SendMessageInput{
		QueueUrl:          aws.String(target.url),
		MessageBody:       aws.String(string(body)),
		MessageAttributes: msgAttrs,
	}
	if target.fifo {
		input.MessageGroupId = aws.String(messageIdentity(body))
		// A fresh deduplication ID per SendToDLQ call. A content-derived one
		// (the envelope ID) made SQS silently drop the second forward of the
		// same event within its 5-minute window — two source queues sharing
		// one DLQ, or a redriven message that fails again — while reporting
		// success, so the caller deleted the source message: a lost
		// dead-letter. SDK retries of this call reuse the input, so they
		// still deduplicate.
		input.MessageDeduplicationId = aws.String(uuid.NewString())
	}

	sendStart := time.Now()
	out, err := p.client.SendMessage(ctx, input)
	metrics.ObserveDependency("sqs", "send_message", err, time.Since(sendStart))
	if err != nil {
		kind := domain.ErrDLQSendFailed
		code := apiErrorCode(err)
		switch {
		case isInvalidMessageCode(code):
			kind = domain.ErrDLQInvalidMessage
		case isNonExistentQueueCode(code):
			// The DLQ was deleted (or the RedrivePolicy retargeted and the old
			// queue removed): drop the cached resolution so the next call
			// re-reads the RedrivePolicy.
			kind = domain.ErrDLQUnresolved
			p.evict(sourceQueueURL)
		}
		if p.logger != nil {
			p.logger.Error("sqs: failed to forward message to DLQ", map[string]any{
				"source_queue": sourceQueueURL,
				"dlq_url":      target.url,
				"dlq_arn":      target.arn,
				"event_type":   eventType,
				"error":        err.Error(),
			})
		}
		return "", &domain.DLQError{Kind: kind, SourceQueue: sourceQueueURL, Cause: classifySQSError(err)}
	}
	messageID := aws.ToString(out.MessageId)
	if p.logger != nil {
		p.logger.Warn("sqs: message forwarded to DLQ", map[string]any{
			"source_queue":   sourceQueueURL,
			"dlq_url":        target.url,
			"dlq_arn":        target.arn,
			"event_type":     eventType,
			"dlq_message_id": messageID,
			"reason":         attrValue(msgAttrs, DLQAttrReason),
		})
	}
	return messageID, nil
}

// fitAttributes returns the non-empty caller attributes that fit alongside the
// reserved DLQ attributes, and the names of those that do not (nil when all
// fit). Caller attributes named like a reserved attribute are overridden
// anyway, so they never count against the limit.
func (p *DLQPublisher) fitAttributes(attrs map[string]string) (map[string]string, []string) {
	names := make([]string, 0, len(attrs))
	for k, v := range attrs {
		if v != "" && !p.isReservedAttribute(k) {
			names = append(names, k)
		}
	}
	room := maxSQSMessageAttributes - p.reservedAttributeCount()
	if len(names) <= room {
		return attrs, nil
	}
	slices.SortFunc(names, func(a, b string) int {
		pa, pb := attributePriority(a), attributePriority(b)
		if pa != pb {
			return pa - pb
		}
		return strings.Compare(a, b)
	})
	kept := make(map[string]string, room)
	for _, k := range names[:room] {
		kept[k] = attrs[k]
	}
	return kept, names[room:]
}

func attributePriority(name string) int {
	if i := slices.Index(callerAttributePriority, name); i >= 0 {
		return i
	}
	return len(callerAttributePriority)
}

func (p *DLQPublisher) isReservedAttribute(name string) bool {
	switch name {
	case DLQAttrEventType, DLQAttrReason, DLQAttrOriginalQueue, DLQAttrFailedAt:
		return true
	case DLQAttrConsumerName:
		return p.consumerName != ""
	}
	return false
}

// buildAttributes merges caller attributes with the standard DLQ attributes.
func (p *DLQPublisher) buildAttributes(sourceQueueURL string, attrs map[string]string, reason, eventType string) map[string]sqstypes.MessageAttributeValue {
	out := make(map[string]sqstypes.MessageAttributeValue, len(attrs)+p.reservedAttributeCount())
	set := func(k, v string) {
		if k == "" || v == "" {
			return
		}
		out[k] = sqstypes.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(v)}
	}
	for k, v := range attrs {
		set(k, v)
	}
	set(DLQAttrEventType, eventType)
	set(DLQAttrReason, sanitizeSQSText(truncateUTF8(reason, maxDLQReasonBytes)))
	set(DLQAttrOriginalQueue, sourceQueueURL)
	set(DLQAttrFailedAt, p.clock.Now().UTC().Format(time.RFC3339Nano))
	set(DLQAttrConsumerName, p.consumerName)
	return out
}

func (p *DLQPublisher) reservedAttributeCount() int {
	if p.consumerName != "" {
		return 5
	}
	return 4
}

// envelopeHeader is the subset of an event envelope the DLQ publisher reads.
type envelopeHeader struct {
	ID     string    `json:"id"`
	Type   string    `json:"type"`
	Source string    `json:"source"`
	Time   time.Time `json:"time"`
}

// parseEnvelopeHeader reports whether body is an event envelope — it carries
// every field ParseEnvelope requires (id, type, source, time) — so arbitrary
// JSON that merely has a "type" or "id" key is not mistaken for one.
func parseEnvelopeHeader(body []byte) (envelopeHeader, bool) {
	var h envelopeHeader
	if json.Unmarshal(body, &h) != nil {
		return envelopeHeader{}, false
	}
	return h, h.ID != "" && h.Type != "" && h.Source != "" && !h.Time.IsZero()
}

// eventTypeFor derives EventType from the envelope body, falling back to the
// caller's EventType attribute (as set by the SNS publisher) and then
// "unknown". Only a real envelope's type is used, so a non-envelope body
// cannot mint arbitrary event_type metric label values.
func (p *DLQPublisher) eventTypeFor(body []byte, attrs map[string]string) string {
	return DLQEventType(body, attrs)
}

// DLQEventType is the event_type a dead-lettered message is counted under:
// the envelope's type, else the EventType attribute, else "unknown".
func DLQEventType(body []byte, attrs map[string]string) string {
	if h, ok := parseEnvelopeHeader(body); ok {
		return h.Type
	}
	if v := attrs[DLQAttrEventType]; v != "" {
		return v
	}
	return "unknown"
}

// resolve returns the cached DLQ for sourceQueueURL, looking it up on a miss.
// Concurrent misses for the same queue may each perform the lookup; that is
// harmless (both resolve to the same target) and avoids holding a lock across
// network calls.
func (p *DLQPublisher) resolve(ctx context.Context, sourceQueueURL string) (dlqTarget, error) {
	p.mu.RLock()
	t, ok := p.cache[sourceQueueURL]
	p.mu.RUnlock()
	if ok && (p.cacheTTL < 0 || p.clock.Now().Sub(t.resolvedAt) < p.cacheTTL) {
		return t, nil
	}

	t, err := p.lookup(ctx, sourceQueueURL)
	if err != nil {
		return dlqTarget{}, err
	}
	p.mu.Lock()
	p.cache[sourceQueueURL] = t
	p.mu.Unlock()
	return t, nil
}

func (p *DLQPublisher) evict(sourceQueueURL string) {
	p.mu.Lock()
	delete(p.cache, sourceQueueURL)
	p.mu.Unlock()
}

func (p *DLQPublisher) lookup(ctx context.Context, sourceQueueURL string) (dlqTarget, error) {
	fail := func(kind, cause error) (dlqTarget, error) {
		return dlqTarget{}, &domain.DLQError{Kind: kind, SourceQueue: sourceQueueURL, Cause: cause}
	}

	start := time.Now()
	attrOut, err := p.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(sourceQueueURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameRedrivePolicy},
	})
	metrics.ObserveDependency("sqs", "get_queue_attributes", err, time.Since(start))
	if err != nil {
		return fail(domain.ErrDLQUnresolved, fmt.Errorf("GetQueueAttributes: %w", classifySQSError(err)))
	}
	raw := attrOut.Attributes[string(sqstypes.QueueAttributeNameRedrivePolicy)]
	if strings.TrimSpace(raw) == "" {
		return fail(domain.ErrDLQNotConfigured, nil)
	}

	targetARN, err := ParseRedrivePolicy(raw)
	if err != nil {
		return fail(domain.ErrDLQInvalidRedrivePolicy, err)
	}
	name, account, _, err := ParseSQSQueueARN(targetARN)
	if err != nil {
		return fail(domain.ErrDLQInvalidRedrivePolicy, err)
	}

	start = time.Now()
	urlOut, err := p.client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName:              aws.String(name),
		QueueOwnerAWSAccountId: aws.String(account),
	})
	metrics.ObserveDependency("sqs", "get_queue_url", err, time.Since(start))
	if err != nil {
		return fail(domain.ErrDLQUnresolved, fmt.Errorf("GetQueueUrl %s: %w", targetARN, classifySQSError(err)))
	}
	dlqURL := aws.ToString(urlOut.QueueUrl)
	if dlqURL == "" {
		return fail(domain.ErrDLQUnresolved, fmt.Errorf("GetQueueUrl %s returned an empty URL", targetARN))
	}
	return dlqTarget{url: dlqURL, arn: targetARN, fifo: strings.HasSuffix(name, fifoQueueSuffix), resolvedAt: p.clock.Now()}, nil
}

// ParseRedrivePolicy extracts deadLetterTargetArn from an SQS RedrivePolicy
// attribute value, e.g. {"deadLetterTargetArn":"arn:aws:sqs:...","maxReceiveCount":"5"}.
func ParseRedrivePolicy(raw string) (string, error) {
	var policy struct {
		DeadLetterTargetArn string `json:"deadLetterTargetArn"`
	}
	if err := json.Unmarshal([]byte(raw), &policy); err != nil {
		return "", fmt.Errorf("RedrivePolicy is not valid JSON: %w", err)
	}
	if policy.DeadLetterTargetArn == "" {
		return "", errors.New("RedrivePolicy has no deadLetterTargetArn")
	}
	return policy.DeadLetterTargetArn, nil
}

// ParseSQSQueueARN splits an SQS queue ARN (arn:<partition>:sqs:<region>:<account>:<name>)
// into its queue name, account ID and region.
func ParseSQSQueueARN(arn string) (name, account, region string, err error) {
	parts := strings.Split(arn, ":")
	if len(parts) != 6 || parts[0] != "arn" || parts[1] == "" || parts[2] != "sqs" {
		return "", "", "", fmt.Errorf("%q is not an SQS queue ARN (expected arn:<partition>:sqs:<region>:<account>:<name>)", arn)
	}
	region, account, name = parts[3], parts[4], parts[5]
	if region == "" || account == "" || name == "" {
		return "", "", "", fmt.Errorf("%q is missing region, account or queue name", arn)
	}
	return name, account, region, nil
}

// retryableSQSErrorCodes are SQS API error codes that indicate a transient
// failure. Everything else (AccessDenied, NonExistentQueue, InvalidParameterValue,
// KMS key errors) needs a configuration change and is left unwrapped.
var retryableSQSErrorCodes = map[string]struct{}{
	"Throttling":          {},
	"ThrottlingException": {},
	"RequestThrottled":    {},
	"KmsThrottled":        {},
	"RequestTimeout":      {},
	"ServiceUnavailable":  {},
	"InternalFailure":     {},
	"InternalError":       {},
}

// apiErrorCode returns err's AWS API error code, or "" when it is not an API error.
func apiErrorCode(err error) string {
	if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
		return apiErr.ErrorCode()
	}
	return ""
}

// isInvalidMessageCode reports SendMessage error codes caused by the message
// itself (size over the queue's MaximumMessageSize, disallowed characters,
// bad attributes) — permanent, reported as ErrDLQInvalidMessage.
func isInvalidMessageCode(code string) bool {
	switch code {
	case "InvalidParameterValue", "InvalidMessageContents", "InvalidAttributeName", "InvalidAttributeValue":
		return true
	}
	return false
}

// isNonExistentQueueCode reports error codes for a queue that no longer exists
// (query and JSON protocol spellings).
func isNonExistentQueueCode(code string) bool {
	return code == "AWS.SimpleQueueService.NonExistentQueue" || code == "QueueDoesNotExist"
}

// classifySQSError wraps err in a domain.RetryableError when it is transient.
func classifySQSError(err error) error {
	if _, ok := retryableSQSErrorCodes[apiErrorCode(err)]; ok {
		return &domain.RetryableError{Cause: err}
	}
	if respErr, ok := errors.AsType[*smithyhttp.ResponseError](err); ok {
		if status := respErr.HTTPStatusCode(); status >= 500 || status == 429 {
			return &domain.RetryableError{Cause: err}
		}
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return &domain.RetryableError{Cause: err}
	}
	return err
}

// messageIdentity returns the envelope ID of body, or a SHA-256 hex digest of
// body when it is not an envelope or its ID is not a valid FIFO identifier.
// Used for the FIFO group ID.
func messageIdentity(body []byte) string {
	if h, ok := parseEnvelopeHeader(body); ok && validFIFOID(h.ID) {
		return h.ID
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func truncateUTF8(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	s = s[:maxBytes]
	for !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s
}

func attrValue(attrs map[string]sqstypes.MessageAttributeValue, k string) string {
	return aws.ToString(attrs[k].StringValue)
}

// validFIFOID reports whether id is usable as a MessageGroupId /
// MessageDeduplicationId: 1–128 characters of alphanumerics and ASCII
// punctuation (0x21–0x7E).
func validFIFOID(id string) bool {
	if id == "" || len(id) > maxFIFOIDLen {
		return false
	}
	for i := 0; i < len(id); i++ {
		if id[i] < 0x21 || id[i] > 0x7E {
			return false
		}
	}
	return true
}

// validSQSRune reports whether r is in the character set SQS accepts in
// message bodies and String attribute values:
// #x9 | #xA | #xD | #x20–#xD7FF | #xE000–#xFFFD | #x10000–#x10FFFF.
func validSQSRune(r rune) bool {
	return r == 0x9 || r == 0xA || r == 0xD ||
		(r >= 0x20 && r <= 0xD7FF) ||
		(r >= 0xE000 && r <= 0xFFFD) ||
		(r >= 0x10000 && r <= 0x10FFFF)
}

// validSQSText reports whether s (valid UTF-8) contains only SQS-allowed characters.
func validSQSText(s string) bool {
	for _, r := range s {
		if !validSQSRune(r) {
			return false
		}
	}
	return true
}

// sanitizeSQSText replaces characters SQS rejects (including invalid UTF-8)
// with U+FFFD, so a reason built from an arbitrary error string is always sendable.
func sanitizeSQSText(s string) string {
	if utf8.ValidString(s) && validSQSText(s) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if !validSQSRune(r) {
			r = utf8.RuneError
		}
		b.WriteRune(r)
	}
	return b.String()
}

// validateAttributeName applies the SQS message attribute naming rules.
func validateAttributeName(name string) error {
	switch {
	case len(name) > maxSQSAttributeNameLen:
		return fmt.Errorf("name longer than %d characters", maxSQSAttributeNameLen)
	case strings.HasPrefix(strings.ToLower(name), "aws."), strings.HasPrefix(strings.ToLower(name), "amazon."):
		return errors.New(`names starting with "AWS." or "Amazon." are reserved`)
	case strings.HasPrefix(name, "."), strings.HasSuffix(name, "."), strings.Contains(name, ".."):
		return errors.New("name must not start or end with a period or contain consecutive periods")
	}
	for _, r := range name {
		if !validAttributeNameRune(r) {
			return fmt.Errorf("character %q not allowed (use A-Z, a-z, 0-9, '_', '-', '.')", r)
		}
	}
	return nil
}

func validAttributeNameRune(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.'
}

// messageSize is the SQS size of body plus non-empty String attributes: each
// attribute counts its name, data type ("String") and value.
func messageSize(body []byte, attrs map[string]string) int {
	n := len(body)
	for k, v := range attrs {
		if v != "" {
			n += len(k) + len("String") + len(v)
		}
	}
	return n
}

func messageSizeSQS(body []byte, attrs map[string]sqstypes.MessageAttributeValue) int {
	n := len(body)
	for k, v := range attrs {
		n += len(k) + len(aws.ToString(v.DataType)) + len(aws.ToString(v.StringValue))
	}
	return n
}

var _ port.DLQPublisher = (*DLQPublisher)(nil)
