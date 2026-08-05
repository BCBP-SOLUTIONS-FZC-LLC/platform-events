// Package sns provides an AWS SNS implementation of port.Publisher.
package sns

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/smithy-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/metrics"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
)

const maxSNSBatchSize = 10

// reservedSNSAttrs are the message attribute keys managed by buildMessageAttributes.
// WithAttributes callers must not use these keys — doing so would silently overwrite
// the attributes that SQS subscription filter policies depend on.
var reservedSNSAttrs = map[string]struct{}{
	"EventType": {},
	"TenantID":  {},
	"Source":    {},
	"EventID":   {},
	"Subject":   {},
}

// SNSClientAPI is the subset of the AWS SNS API used by the publisher.
// Exposed for unit testing; *sns.Client satisfies this interface.
type SNSClientAPI interface {
	Publish(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error)
	PublishBatch(ctx context.Context, params *sns.PublishBatchInput, optFns ...func(*sns.Options)) (*sns.PublishBatchOutput, error)
}

// BatchError collects per-message errors from a PublishBatch call.
type BatchError struct {
	Failures []BatchFailure
}

// BatchFailure describes a single failed message within a batch.
type BatchFailure struct {
	ID      string
	Code    string
	Message string
}

func (e *BatchError) Error() string {
	return fmt.Sprintf("sns: %d message(s) failed in batch", len(e.Failures))
}

// Publisher is the functional-option type for snsPublisher construction.
type PublisherOption func(*snsPublisher)

// WithMessageGroupID sets the function used to derive MessageGroupID (FIFO topics).
func WithMessageGroupID(fn func(domain.Envelope[json.RawMessage]) string) PublisherOption {
	return func(p *snsPublisher) { p.messageGroupIDFn = fn }
}

// WithMessageDeduplicationID sets the function used to derive MessageDeduplicationID.
func WithMessageDeduplicationID(fn func(domain.Envelope[json.RawMessage]) string) PublisherOption {
	return func(p *snsPublisher) { p.deduplicationIDFn = fn }
}

// WithAttributes sets additional SNS message attributes sent with every message.
func WithAttributes(attrs map[string]string) PublisherOption {
	return func(p *snsPublisher) { p.extraAttributes = attrs }
}

// WithCodec sets an optional schema-registry codec applied to the JSON
// payload immediately before SNS publish. Unset (nil, the default), Payload
// is published as plain JSON exactly as before this option existed — zero
// behaviour change. See port.Codec for the Encode contract.
func WithCodec(codec port.Codec) PublisherOption {
	return func(p *snsPublisher) { p.codec = codec }
}

type snsPublisher struct {
	client            SNSClientAPI
	topicARN          string
	logger            port.Logger
	messageGroupIDFn  func(domain.Envelope[json.RawMessage]) string
	deduplicationIDFn func(domain.Envelope[json.RawMessage]) string
	extraAttributes   map[string]string
	codec             port.Codec
}

// Config holds the parameters for constructing an SNS publisher.
type Config struct {
	// TopicARN is required. Panics on empty string.
	TopicARN    string
	Region      string
	EndpointURL string // optional — set to LocalStack URL for testing
	Logger      port.Logger
}

// New constructs an SNS publisher from the provided configuration.
// Returns an error if TopicARN is empty or does not look like an SNS ARN.
func New(cfg Config, opts ...PublisherOption) (port.Publisher, error) {
	if cfg.TopicARN == "" {
		return nil, fmt.Errorf("sns: TopicARN is required")
	}
	if !strings.HasPrefix(cfg.TopicARN, "arn:aws:sns:") && !strings.HasPrefix(cfg.TopicARN, "arn:aws-cn:sns:") && !strings.HasPrefix(cfg.TopicARN, "arn:aws-us-gov:sns:") {
		return nil, fmt.Errorf("sns: TopicARN %q does not look like a valid SNS ARN (expected prefix arn:aws:sns:, arn:aws-cn:sns:, or arn:aws-us-gov:sns:)", cfg.TopicARN)
	}

	awsOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
	}

	startupCtx, startupCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer startupCancel()
	awsCfg, err := awsconfig.LoadDefaultConfig(startupCtx, awsOpts...)
	if err != nil {
		return nil, fmt.Errorf("sns: failed to load AWS config: %w", err)
	}

	snsOpts := []func(*sns.Options){}
	if cfg.EndpointURL != "" {
		snsOpts = append(snsOpts, func(o *sns.Options) {
			o.BaseEndpoint = aws.String(cfg.EndpointURL)
		})
	}

	client := sns.NewFromConfig(awsCfg, snsOpts...)
	return NewWithClient(cfg.TopicARN, client, cfg.Logger, opts...)
}

// NewWithClient constructs an SNS publisher with an injected client.
// Useful in tests to provide a mock SNS client without real AWS credentials.
// Returns an error if topicARN is empty or does not look like an SNS ARN.
func NewWithClient(topicARN string, client SNSClientAPI, logger port.Logger, opts ...PublisherOption) (port.Publisher, error) {
	if topicARN == "" {
		return nil, fmt.Errorf("sns: TopicARN is required")
	}
	if !strings.HasPrefix(topicARN, "arn:aws:sns:") && !strings.HasPrefix(topicARN, "arn:aws-cn:sns:") && !strings.HasPrefix(topicARN, "arn:aws-us-gov:sns:") {
		return nil, fmt.Errorf("sns: TopicARN %q does not look like a valid SNS ARN (expected prefix arn:aws:sns:, arn:aws-cn:sns:, or arn:aws-us-gov:sns:)", topicARN)
	}
	p := &snsPublisher{
		client:   client,
		topicARN: topicARN,
		logger:   logger,
	}
	for _, opt := range opts {
		opt(p)
	}
	// Warn at construction time if extra attributes are close to the SNS limit.
	// 4 reserved + N extra + OTel headers (typically 1-3) must not exceed 10.
	if p.logger != nil && len(p.extraAttributes) >= 5 {
		p.logger.Warn("sns: WithAttributes count is high; combined with 4 reserved attributes and OTel headers the SNS limit of 10 may be exceeded at publish time", map[string]any{
			"extra_attributes_count": len(p.extraAttributes),
			"topic":                  topicARN,
		})
	}

	// FIFO topics require MessageGroupId on every publish. Validate at construction
	// so the misconfiguration is caught at wiring time, not on the first API call.
	if strings.HasSuffix(topicARN, ".fifo") && p.messageGroupIDFn == nil {
		return nil, fmt.Errorf("sns: FIFO topic %q requires WithMessageGroupID option", topicARN)
	}
	return p, nil
}

// Publish publishes a single envelope to SNS.
func (p *snsPublisher) Publish(ctx context.Context, env domain.Envelope[json.RawMessage]) error {
	// Validate required fields before the API call. Missing Type or Source will
	// cause SNS subscription filter policies (which match on message attributes)
	// to silently drop the message without any error from the API.
	if err := validateEnvelopeFields(env); err != nil {
		return err
	}

	tracer := otel.Tracer("platform-events")
	ctx, span := tracer.Start(ctx, "sns.publish", oteltrace.WithSpanKind(oteltrace.SpanKindProducer))
	defer span.End()

	span.SetAttributes(
		attribute.String("messaging.system", "aws_sns"),
		attribute.String("messaging.destination", p.topicARN),
		attribute.String("events.event_type", env.Type),
		attribute.String("events.event_id", env.ID),
	)

	env, err := p.encodeEnvelopePayload(ctx, env)
	if err != nil {
		if p.logger != nil {
			p.logger.Error("sns: codec encode failed", map[string]any{
				"topic":      p.topicARN,
				"event_type": env.Type,
				"error":      err.Error(),
			})
		}
		return err
	}

	body, err := json.Marshal(env)
	if err != nil {
		return err
	}

	input := &sns.PublishInput{
		TopicArn:          aws.String(p.topicARN),
		Message:           aws.String(string(body)),
		MessageAttributes: buildMessageAttributes(env),
	}

	// Add extra attributes, guarding reserved keys so callers cannot silently
	// overwrite EventType/TenantID/Source/EventID which SQS filter policies use.
	for k, v := range p.extraAttributes {
		if v != "" {
			if _, reserved := reservedSNSAttrs[k]; !reserved {
				input.MessageAttributes[k] = snstypes.MessageAttributeValue{
					DataType:    aws.String("String"),
					StringValue: aws.String(v),
				}
			} else if p.logger != nil {
				p.logger.Warn("sns: WithAttributes key conflicts with a reserved attribute and was ignored", map[string]any{
					"key": k,
				})
			}
		}
	}

	// Inject W3C traceparent so the SQS consumer can reconstruct the full
	// SpanContext (TraceID + SpanID) and create a valid OTel cross-service link.
	// Guard against carrier keys that collide with reserved SNS attributes so
	// propagator-injected headers cannot overwrite EventType/TenantID/Source/EventID.
	carrier := make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	for k, v := range carrier {
		if _, reserved := reservedSNSAttrs[k]; !reserved {
			input.MessageAttributes[k] = snstypes.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(v),
			}
		}
	}

	// FIFO topic handling.
	if strings.HasSuffix(p.topicARN, ".fifo") {
		if p.messageGroupIDFn != nil {
			input.MessageGroupId = aws.String(p.messageGroupIDFn(env))
		}
		if p.deduplicationIDFn != nil {
			input.MessageDeduplicationId = aws.String(p.deduplicationIDFn(env))
		} else {
			input.MessageDeduplicationId = aws.String(env.ID)
		}
	}

	// SNS hard limit: 10 message attributes per message. Validate before the
	// API call so callers get a clear error rather than an opaque InvalidParameter.
	if n := len(input.MessageAttributes); n > 10 {
		metrics.RecordPublish(p.topicARN, env.Type, "error", 0)
		if p.logger != nil {
			p.logger.Error("sns: message attribute count exceeds SNS limit of 10", map[string]any{
				"count":      n,
				"topic":      p.topicARN,
				"event_type": env.Type,
			})
		}
		return fmt.Errorf("sns: %d message attributes exceed SNS limit of 10; reduce WithAttributes entries or OTel header count", n)
	}

	start := time.Now()
	out, err := p.client.Publish(ctx, input)
	dur := time.Since(start)

	status := "success"
	if err != nil {
		status = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		if p.logger != nil {
			p.logger.Error("sns: publish failed", map[string]any{
				"topic":      p.topicARN,
				"event_type": env.Type,
				"error":      err.Error(),
			})
		}
		err = wrapIfRetryable(err)
	} else {
		span.SetStatus(codes.Ok, "")
		span.SetAttributes(attribute.String("messaging.message_id", aws.ToString(out.MessageId)))
	}

	metrics.RecordPublish(p.topicARN, env.Type, status, dur.Seconds())

	return err
}

// PublishBatch publishes multiple envelopes. Batches larger than 10 are split automatically.
// Returns a *BatchError listing per-message failures if any occur.
// All chunks are always attempted — a transport error in one chunk does not
// prevent remaining chunks from being sent.
func (p *snsPublisher) PublishBatch(ctx context.Context, envs []domain.Envelope[json.RawMessage]) error {
	var batchErr *BatchError

	for i := 0; i < len(envs); i += maxSNSBatchSize {
		end := min(i+maxSNSBatchSize, len(envs))
		chunk := envs[i:end]
		if err := p.publishChunk(ctx, chunk); err != nil {
			if batchErr == nil {
				batchErr = &BatchError{}
			}
			if be, ok := errors.AsType[*BatchError](err); ok {
				batchErr.Failures = append(batchErr.Failures, be.Failures...)
			} else {
				// Transport-level error: record every message in the chunk as failed
				// so callers know which IDs were not delivered. Remaining chunks are
				// still attempted below.
				for _, env := range chunk {
					batchErr.Failures = append(batchErr.Failures, BatchFailure{
						ID:      env.ID,
						Code:    "TransportError",
						Message: err.Error(),
					})
				}
			}
		}
	}

	if batchErr != nil && len(batchErr.Failures) > 0 {
		return batchErr
	}
	return nil
}

func (p *snsPublisher) publishChunk(ctx context.Context, envs []domain.Envelope[json.RawMessage]) error {
	tracer := otel.Tracer("platform-events")
	ctx, span := tracer.Start(ctx, "sns.publish.batch", oteltrace.WithSpanKind(oteltrace.SpanKindProducer))
	defer span.End()

	span.SetAttributes(
		attribute.String("messaging.system", "aws_sns"),
		attribute.String("messaging.destination", p.topicARN),
		attribute.Int("messaging.batch.message_count", len(envs)),
	)

	// Inject W3C traceparent once for the batch; all entries share the same
	// calling goroutine's trace context.
	traceCarrier := make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, traceCarrier)

	entries := make([]snstypes.PublishBatchRequestEntry, 0, len(envs))
	// entryEventType maps entry.Id (== env.ID, a UUID) to env.Type so that
	// the transport-error metric path uses the event type label, not a UUID.
	// Without this, every transport error creates a unique Prometheus time series
	// (cardinality explosion) and dashboards show UUIDs instead of event types.
	entryEventType := make(map[string]string, len(envs))
	var marshalErr *BatchError
	for _, env := range envs {
		// Mirror the Publish validation: missing Type or Source causes
		// buildMessageAttributes to omit EventType/Source attributes, which makes
		// SQS subscription filter policies silently drop the message — no API
		// error is returned because SNS accepts messages with arbitrary attribute sets.
		if err := validateEnvelopeFields(env); err != nil {
			metrics.RecordPublish(p.topicARN, env.Type, "error", 0)
			if marshalErr == nil {
				marshalErr = &BatchError{}
			}
			marshalErr.Failures = append(marshalErr.Failures, BatchFailure{
				ID:      env.ID,
				Code:    "InvalidEnvelope",
				Message: err.Error(),
			})
			continue
		}
		env, err := p.encodeEnvelopePayload(ctx, env)
		if err != nil {
			if marshalErr == nil {
				marshalErr = &BatchError{}
			}
			marshalErr.Failures = append(marshalErr.Failures, BatchFailure{
				ID:      env.ID,
				Code:    "CodecEncodeError",
				Message: err.Error(),
			})
			continue
		}
		body, err := json.Marshal(env)
		if err != nil {
			// Marshal failures are deterministic; record the metric and skip
			// this entry rather than aborting the chunk — other envelopes
			// in the same chunk can still be delivered successfully.
			metrics.RecordPublish(p.topicARN, env.Type, "error", 0)
			if marshalErr == nil {
				marshalErr = &BatchError{}
			}
			marshalErr.Failures = append(marshalErr.Failures, BatchFailure{
				ID:      env.ID,
				Code:    "MarshalError",
				Message: err.Error(),
			})
			continue
		}
		entryEventType[env.ID] = env.Type
		attrs := buildMessageAttributes(env)
		for k, v := range p.extraAttributes {
			if v != "" {
				if _, reserved := reservedSNSAttrs[k]; !reserved {
					attrs[k] = snstypes.MessageAttributeValue{
						DataType:    aws.String("String"),
						StringValue: aws.String(v),
					}
				}
			}
		}
		for k, v := range traceCarrier {
			if _, reserved := reservedSNSAttrs[k]; !reserved {
				attrs[k] = snstypes.MessageAttributeValue{
					DataType:    aws.String("String"),
					StringValue: aws.String(v),
				}
			}
		}
		// SNS hard limit: 10 message attributes per message. Mirror the single-
		// Publish validation so batch callers also get a clear error rather than
		// an opaque InvalidParameter from the API.
		if n := len(attrs); n > 10 {
			metrics.RecordPublish(p.topicARN, env.Type, "error", 0)
			if marshalErr == nil {
				marshalErr = &BatchError{}
			}
			marshalErr.Failures = append(marshalErr.Failures, BatchFailure{
				ID:      env.ID,
				Code:    "TooManyAttributes",
				Message: fmt.Sprintf("%d message attributes exceed SNS limit of 10; reduce WithAttributes entries or OTel header count", n),
			})
			continue
		}
		entry := snstypes.PublishBatchRequestEntry{
			Id:                aws.String(env.ID),
			Message:           aws.String(string(body)),
			MessageAttributes: attrs,
		}
		if strings.HasSuffix(p.topicARN, ".fifo") {
			if p.messageGroupIDFn != nil {
				entry.MessageGroupId = aws.String(p.messageGroupIDFn(env))
			}
			if p.deduplicationIDFn != nil {
				entry.MessageDeduplicationId = aws.String(p.deduplicationIDFn(env))
			} else {
				entry.MessageDeduplicationId = aws.String(env.ID)
			}
		}
		entries = append(entries, entry)
	}

	// If all entries failed to marshal, skip the API call entirely.
	if len(entries) == 0 {
		if marshalErr != nil {
			span.RecordError(marshalErr)
			span.SetStatus(codes.Error, marshalErr.Error())
		}
		return marshalErr
	}

	start := time.Now()
	out, err := p.client.PublishBatch(ctx, &sns.PublishBatchInput{
		TopicArn:                   aws.String(p.topicARN),
		PublishBatchRequestEntries: entries,
	})
	dur := time.Since(start)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		if p.logger != nil {
			p.logger.Error("sns: batch publish failed", map[string]any{
				"topic": p.topicARN,
				"count": len(entries),
				"error": err.Error(),
			})
		}
		perMsg := dur.Seconds() / float64(len(entries))
		for _, entry := range entries {
			eventType := entryEventType[aws.ToString(entry.Id)]
			metrics.RecordPublish(p.topicARN, eventType, "error", perMsg)
		}
		// Merge any marshal failures accumulated before the API call so callers
		// see the correct Code ("MarshalError" vs "TransportError") for each entry.
		if marshalErr != nil {
			combined := &BatchError{Failures: make([]BatchFailure, 0, len(marshalErr.Failures)+len(entries))}
			combined.Failures = append(combined.Failures, marshalErr.Failures...)
			for _, entry := range entries {
				combined.Failures = append(combined.Failures, BatchFailure{
					ID:      aws.ToString(entry.Id),
					Code:    "TransportError",
					Message: err.Error(),
				})
			}
			return combined
		}
		// Wrap transport-level errors so the outbox service's failureThreshold()
		// correctly identifies retryable failures (ThrottlingException, ServiceUnavailable)
		// and uses maxAttempts+1 instead of maxAttempts, preventing premature dead-lettering.
		return wrapIfRetryable(err)
	}

	// Build a set of failed IDs for O(1) lookup.
	failedIDs := make(map[string]struct{}, len(out.Failed))
	for _, f := range out.Failed {
		failedIDs[aws.ToString(f.Id)] = struct{}{}
	}

	perMsg := dur.Seconds() / float64(len(entries))
	for _, env := range envs {
		if _, hadMarshalErr := func() (struct{}, bool) {
			if marshalErr != nil {
				for _, f := range marshalErr.Failures {
					if f.ID == env.ID {
						return struct{}{}, true
					}
				}
			}
			return struct{}{}, false
		}(); hadMarshalErr {
			continue // already recorded as error during entry building
		}
		_, failed := failedIDs[env.ID]
		status := "success"
		if failed {
			status = "error"
		}
		metrics.RecordPublish(p.topicARN, env.Type, status, perMsg)
	}

	// Merge API failures with any marshal failures collected during entry building.
	var resultErr *BatchError
	if len(out.Failed) > 0 {
		resultErr = &BatchError{}
		for _, f := range out.Failed {
			resultErr.Failures = append(resultErr.Failures, BatchFailure{
				ID:      aws.ToString(f.Id),
				Code:    aws.ToString(f.Code),
				Message: aws.ToString(f.Message),
			})
		}
	}
	if marshalErr != nil {
		if resultErr == nil {
			resultErr = &BatchError{}
		}
		resultErr.Failures = append(resultErr.Failures, marshalErr.Failures...)
	}
	if resultErr != nil {
		span.RecordError(resultErr)
		span.SetStatus(codes.Error, resultErr.Error())
		return resultErr
	}
	span.SetStatus(codes.Ok, "")
	return nil
}

// retryableErrorCodes are AWS SNS error codes that indicate a transient failure
// which should not count toward maxAttempts in the outbox service.
var retryableErrorCodes = map[string]struct{}{
	"Throttling":                    {},
	"ThrottlingException":           {},
	"RequestThrottled":              {},
	"ProvisionedThroughputExceeded": {},
	"RequestTimeout":                {},
	"ServiceUnavailable":            {},
	"InternalFailure":               {},
}

// wrapIfRetryable wraps err in a domain.RetryableError when the underlying
// error indicates a transient failure that should not exhaust maxAttempts.
// Covers AWS API throttle/service errors and network-level timeouts.
func wrapIfRetryable(err error) error {
	if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
		if _, ok := retryableErrorCodes[apiErr.ErrorCode()]; ok {
			return &domain.RetryableError{Cause: err}
		}
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return &domain.RetryableError{Cause: err}
	}
	return err
}

// encodeEnvelopePayload runs the configured codec (if any) on env's payload.
// If no codec is configured, env is returned unchanged. If Encode returns a
// non-empty schemaID, Payload is replaced with a base64-wrapped JSON string
// and SchemaID is set to schemaID; a schemaID of "" (e.g. a NoopCodec) leaves
// the envelope untouched — no wire format change.
func (p *snsPublisher) encodeEnvelopePayload(ctx context.Context, env domain.Envelope[json.RawMessage]) (domain.Envelope[json.RawMessage], error) {
	if p.codec == nil {
		return env, nil
	}
	start := time.Now()
	encoded, schemaID, err := p.codec.Encode(ctx, env.Type, env.Payload)
	dur := time.Since(start)
	if err != nil {
		metrics.RecordCodecEncode(p.topicARN, env.Type, "error", dur.Seconds())
		return env, fmt.Errorf("sns: codec encode failed: %w", err)
	}
	if schemaID == "" {
		metrics.RecordCodecEncode(p.topicARN, env.Type, "noop", dur.Seconds())
		return env, nil
	}
	wrapped, err := domain.WrapCodecPayload(encoded)
	if err != nil {
		metrics.RecordCodecEncode(p.topicARN, env.Type, "error", dur.Seconds())
		return env, fmt.Errorf("sns: %w", err)
	}
	env.Payload = wrapped
	env.SchemaID = schemaID
	metrics.RecordCodecEncode(p.topicARN, env.Type, "success", dur.Seconds())
	return env, nil
}

func validateEnvelopeFields(env domain.Envelope[json.RawMessage]) error {
	if env.ID == "" || env.Type == "" || env.Source == "" {
		return fmt.Errorf("sns: envelope missing required fields (id=%q, type=%q, source=%q)", env.ID, env.Type, env.Source)
	}
	return nil
}

// buildMessageAttributes returns SNS message attributes for an envelope.
// Only non-empty values are included — SNS rejects empty String attribute values.
func buildMessageAttributes(env domain.Envelope[json.RawMessage]) map[string]snstypes.MessageAttributeValue {
	attrs := make(map[string]snstypes.MessageAttributeValue, 5)
	if env.Type != "" {
		attrs["EventType"] = snstypes.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(env.Type)}
	}
	if env.TenantID != "" {
		attrs["TenantID"] = snstypes.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(env.TenantID)}
	}
	if env.Source != "" {
		attrs["Source"] = snstypes.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(env.Source)}
	}
	if env.ID != "" {
		attrs["EventID"] = snstypes.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(env.ID)}
	}
	if env.Subject != "" {
		attrs["Subject"] = snstypes.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(env.Subject)}
	}
	return attrs
}
