// Package sns provides an AWS SNS implementation of port.Publisher.
package sns

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
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

type snsPublisher struct {
	client            SNSClientAPI
	topicARN          string
	logger            port.Logger
	messageGroupIDFn  func(domain.Envelope[json.RawMessage]) string
	deduplicationIDFn func(domain.Envelope[json.RawMessage]) string
	extraAttributes   map[string]string
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
// Returns an error if TopicARN is empty.
func New(cfg Config, opts ...PublisherOption) (port.Publisher, error) {
	if cfg.TopicARN == "" {
		return nil, fmt.Errorf("sns: TopicARN is required")
	}

	awsOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), awsOpts...)
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
// Returns an error if topicARN is empty.
func NewWithClient(topicARN string, client SNSClientAPI, logger port.Logger, opts ...PublisherOption) (port.Publisher, error) {
	if topicARN == "" {
		return nil, fmt.Errorf("sns: TopicARN is required")
	}
	p := &snsPublisher{
		client:   client,
		topicARN: topicARN,
		logger:   logger,
	}
	for _, opt := range opts {
		opt(p)
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
	tracer := otel.Tracer("platform-events")
	ctx, span := tracer.Start(ctx, "sns.publish", oteltrace.WithSpanKind(oteltrace.SpanKindProducer))
	defer span.End()

	span.SetAttributes(
		attribute.String("messaging.system", "aws_sns"),
		attribute.String("messaging.destination", p.topicARN),
		attribute.String("events.event_type", env.Type),
		attribute.String("events.event_id", env.ID),
	)

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
	carrier := make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	for k, v := range carrier {
		input.MessageAttributes[k] = snstypes.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(v),
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
	} else {
		span.SetAttributes(attribute.String("messaging.message_id", aws.ToString(out.MessageId)))
	}

	if metrics.EventsPublishedTotal != nil {
		metrics.EventsPublishedTotal.WithLabelValues(p.topicARN, env.Type, status).Inc()
	}
	if metrics.EventsPublishDuration != nil {
		metrics.EventsPublishDuration.WithLabelValues(p.topicARN, env.Type).Observe(dur.Seconds())
	}

	return err
}

// PublishBatch publishes multiple envelopes. Batches larger than 10 are split automatically.
// Returns a *BatchError listing per-message failures if any occur.
func (p *snsPublisher) PublishBatch(ctx context.Context, envs []domain.Envelope[json.RawMessage]) error {
	var batchErr *BatchError

	for i := 0; i < len(envs); i += maxSNSBatchSize {
		end := min(i+maxSNSBatchSize, len(envs))
		chunk := envs[i:end]
		if err := p.publishChunk(ctx, chunk); err != nil {
			var be *BatchError
			if errors.As(err, &be) {
				if batchErr == nil {
					batchErr = &BatchError{}
				}
				batchErr.Failures = append(batchErr.Failures, be.Failures...)
			} else {
				return err
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
	for _, env := range envs {
		body, err := json.Marshal(env)
		if err != nil {
			return err
		}
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
			attrs[k] = snstypes.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(v),
			}
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
				"count": len(envs),
				"error": err.Error(),
			})
		}
		for _, env := range envs {
			if metrics.EventsPublishedTotal != nil {
				metrics.EventsPublishedTotal.WithLabelValues(p.topicARN, env.Type, "error").Inc()
			}
		}
		if metrics.EventsPublishDuration != nil && len(envs) > 0 {
			perMsg := dur.Seconds() / float64(len(envs))
			for _, env := range envs {
				metrics.EventsPublishDuration.WithLabelValues(p.topicARN, env.Type).Observe(perMsg)
			}
		}
		return err
	}

	// Build a set of failed IDs for O(1) lookup.
	failedIDs := make(map[string]struct{}, len(out.Failed))
	for _, f := range out.Failed {
		failedIDs[aws.ToString(f.Id)] = struct{}{}
	}

	perMsg := dur.Seconds() / float64(len(envs))
	for _, env := range envs {
		_, failed := failedIDs[env.ID]
		status := "success"
		if failed {
			status = "error"
		}
		if metrics.EventsPublishedTotal != nil {
			metrics.EventsPublishedTotal.WithLabelValues(p.topicARN, env.Type, status).Inc()
		}
		if metrics.EventsPublishDuration != nil {
			metrics.EventsPublishDuration.WithLabelValues(p.topicARN, env.Type).Observe(perMsg)
		}
	}

	if len(out.Failed) > 0 {
		be := &BatchError{}
		for _, f := range out.Failed {
			be.Failures = append(be.Failures, BatchFailure{
				ID:      aws.ToString(f.Id),
				Code:    aws.ToString(f.Code),
				Message: aws.ToString(f.Message),
			})
		}
		return be
	}
	return nil
}

// buildMessageAttributes returns SNS message attributes for an envelope.
// Only non-empty values are included — SNS rejects empty String attribute values.
func buildMessageAttributes(env domain.Envelope[json.RawMessage]) map[string]snstypes.MessageAttributeValue {
	attrs := make(map[string]snstypes.MessageAttributeValue, 4)
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
	return attrs
}
