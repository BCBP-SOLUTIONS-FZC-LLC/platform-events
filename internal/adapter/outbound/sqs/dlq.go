package sqs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/metrics"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
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
	fifoQueueSuffix   = ".fifo"
)

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
	EndpointURL string // optional — set to LocalStack URL for testing
	// ConsumerName, when set, is attached to every forwarded message as the
	// ConsumerName attribute so operators can tell which consumer gave up.
	ConsumerName string
	Logger       port.Logger
	// Clock stamps FailedAt. Defaults to port.RealClock.
	Clock port.Clock
}

// dlqTarget is a resolved dead-letter queue.
type dlqTarget struct {
	url  string
	arn  string
	fifo bool
}

// DLQPublisher forwards failed messages to a source queue's configured
// dead-letter queue, resolved from the source queue's RedrivePolicy.
// Safe for concurrent use.
type DLQPublisher struct {
	client       DLQClientAPI
	consumerName string
	logger       port.Logger
	clock        port.Clock

	mu sync.RWMutex
	// cache maps source queue URL → resolved DLQ. Only successful resolutions
	// are cached, so a queue whose RedrivePolicy is added after a failed
	// lookup is picked up on the next call. A RedrivePolicy changed to point
	// at a different DLQ is picked up on process restart.
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
	return &DLQPublisher{
		client:       client,
		consumerName: cfg.ConsumerName,
		logger:       cfg.Logger,
		clock:        clock,
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
// "unknown". SQS allows at most 10 attributes per message; exceeding that is
// rejected before any API call.
//
// For a FIFO DLQ, MessageGroupId and MessageDeduplicationId are both set to
// the envelope ID, or to a SHA-256 of the body when it is not an envelope.
func (p *DLQPublisher) SendToDLQ(ctx context.Context, sourceQueueURL string, body []byte, attrs map[string]string, reason string) error {
	eventType := p.eventTypeFor(body, attrs)
	err := p.send(ctx, sourceQueueURL, body, attrs, reason, eventType)
	status := "success"
	if err != nil {
		status = "error"
	}
	metrics.RecordDLQForward(sourceQueueURL, eventType, status)
	return err
}

func (p *DLQPublisher) send(ctx context.Context, sourceQueueURL string, body []byte, attrs map[string]string, reason, eventType string) error {
	invalid := func(msg string) error {
		return &domain.DLQError{Kind: domain.ErrDLQInvalidMessage, SourceQueue: sourceQueueURL, Cause: errors.New(msg)}
	}
	switch {
	case sourceQueueURL == "":
		return invalid("source queue URL is required")
	case len(body) == 0:
		return invalid("message body is empty")
	case !utf8.Valid(body):
		return invalid("message body is not valid UTF-8")
	case strings.TrimSpace(reason) == "":
		return invalid("reason is required")
	}

	msgAttrs := p.buildAttributes(sourceQueueURL, attrs, reason, eventType)
	if len(msgAttrs) > maxSQSMessageAttributes {
		return invalid(fmt.Sprintf("%d message attributes exceeds the SQS limit of %d (%d are reserved by the DLQ publisher)",
			len(msgAttrs), maxSQSMessageAttributes, p.reservedAttributeCount()))
	}

	target, err := p.resolve(ctx, sourceQueueURL)
	if err != nil {
		return err
	}

	input := &sqs.SendMessageInput{
		QueueUrl:          aws.String(target.url),
		MessageBody:       aws.String(string(body)),
		MessageAttributes: msgAttrs,
	}
	if target.fifo {
		id := messageIdentity(body)
		input.MessageGroupId = aws.String(id)
		input.MessageDeduplicationId = aws.String(id)
	}

	out, err := p.client.SendMessage(ctx, input)
	if err != nil {
		dlqErr := &domain.DLQError{Kind: domain.ErrDLQSendFailed, SourceQueue: sourceQueueURL, Cause: classifySQSError(err)}
		if p.logger != nil {
			p.logger.Error("sqs: failed to forward message to DLQ", map[string]any{
				"source_queue": sourceQueueURL,
				"dlq_url":      target.url,
				"dlq_arn":      target.arn,
				"event_type":   eventType,
				"error":        err.Error(),
			})
		}
		return dlqErr
	}
	if p.logger != nil {
		p.logger.Warn("sqs: message forwarded to DLQ", map[string]any{
			"source_queue":   sourceQueueURL,
			"dlq_url":        target.url,
			"dlq_arn":        target.arn,
			"event_type":     eventType,
			"dlq_message_id": aws.ToString(out.MessageId),
			"reason":         attrValue(msgAttrs, DLQAttrReason),
		})
	}
	return nil
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
	set(DLQAttrReason, truncateUTF8(reason, maxDLQReasonBytes))
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

// eventTypeFor derives EventType from the envelope body, falling back to the
// caller's EventType attribute (as set by the SNS publisher) and then "unknown".
func (p *DLQPublisher) eventTypeFor(body []byte, attrs map[string]string) string {
	var env struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(body, &env) == nil && env.Type != "" {
		return env.Type
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
	if ok {
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

func (p *DLQPublisher) lookup(ctx context.Context, sourceQueueURL string) (dlqTarget, error) {
	fail := func(kind, cause error) (dlqTarget, error) {
		return dlqTarget{}, &domain.DLQError{Kind: kind, SourceQueue: sourceQueueURL, Cause: cause}
	}

	attrOut, err := p.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(sourceQueueURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameRedrivePolicy},
	})
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

	urlOut, err := p.client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName:              aws.String(name),
		QueueOwnerAWSAccountId: aws.String(account),
	})
	if err != nil {
		return fail(domain.ErrDLQUnresolved, fmt.Errorf("GetQueueUrl %s: %w", targetARN, classifySQSError(err)))
	}
	dlqURL := aws.ToString(urlOut.QueueUrl)
	if dlqURL == "" {
		return fail(domain.ErrDLQUnresolved, fmt.Errorf("GetQueueUrl %s returned an empty URL", targetARN))
	}
	return dlqTarget{url: dlqURL, arn: targetARN, fifo: strings.HasSuffix(name, fifoQueueSuffix)}, nil
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

// classifySQSError wraps err in a domain.RetryableError when it is transient.
func classifySQSError(err error) error {
	if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
		if _, ok := retryableSQSErrorCodes[apiErr.ErrorCode()]; ok {
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
// body when it is not an envelope. Used for FIFO group and dedup IDs.
func messageIdentity(body []byte) string {
	var env struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(body, &env) == nil && env.ID != "" && len(env.ID) <= 128 {
		return env.ID
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
