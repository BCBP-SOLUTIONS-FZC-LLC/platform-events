package events

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/sns"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
)

// BatchError collects per-message errors from a PublishBatch call.
// Exposed so pkg/outbox and callers can inspect partial batch failures.
type BatchError struct {
	Failures []BatchFailure
}

func (e *BatchError) Error() string {
	if len(e.Failures) == 0 {
		return domain.FormatBatchError("events", 0, nil)
	}
	f := e.Failures[0]
	return domain.FormatBatchError("events", len(e.Failures), &domain.BatchFailure{ID: f.ID, Code: f.Code, Message: f.Message})
}

// BatchFailure describes a single failed message within a batch.
type BatchFailure struct {
	ID      string
	Code    string
	Message string
	// Retryable reports a transient failure (throttling, service-side error,
	// timeout) that says nothing about the message itself: the outbox retries
	// it without counting an attempt. Code "TransportError" is also treated as
	// retryable, for publishers that predate this field.
	Retryable bool
}

// Publisher publishes event envelopes to a message broker.
type Publisher interface {
	Publish(ctx context.Context, env Envelope[json.RawMessage]) error
	PublishBatch(ctx context.Context, envs []Envelope[json.RawMessage]) error
}

// SNSConfig configures an SNS publisher.
type SNSConfig struct {
	// TopicARN is required. NewSNSPublisher returns an error if empty.
	TopicARN    string
	Region      string
	EndpointURL string // optional — AWS emulator (floci) endpoint for local runs and tests
	Logger      port.Logger
}

// PublisherOption is a functional option for the SNS publisher.
type PublisherOption = sns.PublisherOption

// WithMessageGroupID sets the function used to derive MessageGroupID (FIFO topics).
func WithMessageGroupID(fn func(Envelope[json.RawMessage]) string) PublisherOption {
	return sns.WithMessageGroupID(func(env domain.Envelope[json.RawMessage]) string {
		return fn(domainToPublic(env))
	})
}

// WithMessageDeduplicationID sets the function to derive MessageDeduplicationID.
func WithMessageDeduplicationID(fn func(Envelope[json.RawMessage]) string) PublisherOption {
	return sns.WithMessageDeduplicationID(func(env domain.Envelope[json.RawMessage]) string {
		return fn(domainToPublic(env))
	})
}

// WithAttributes sets additional SNS message attributes.
func WithAttributes(attrs map[string]string) PublisherOption {
	return sns.WithAttributes(attrs)
}

// WithCodec sets the Codec used to encode outgoing envelope payloads before
// publish. Unset (nil), Publish/PublishBatch behave exactly as before this
// option existed: Payload stays plain JSON and SchemaID stays empty.
// The encoded payload is base64-wrapped (about a third larger), so leave
// headroom below SNS's 256 KiB message limit — outbox.Enqueue's 240 KiB
// check runs before encoding.
func WithCodec(codec Codec) PublisherOption {
	return sns.WithCodec(codec)
}

// NewPublisherFromPort wraps any internal port.Publisher as a public events.Publisher.
// Useful in tests or when injecting a custom publisher implementation.
func NewPublisherFromPort(inner port.Publisher) Publisher {
	return &publisherAdapter{inner: inner}
}

// NewSNSPublisher constructs an SNS-backed Publisher.
// Returns an error if cfg.TopicARN is empty.
func NewSNSPublisher(cfg SNSConfig, opts ...PublisherOption) (Publisher, error) {
	inner, err := sns.New(sns.Config{
		TopicARN:    cfg.TopicARN,
		Region:      cfg.Region,
		EndpointURL: cfg.EndpointURL,
		Logger:      cfg.Logger,
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &publisherAdapter{inner: inner}, nil
}

// publisherAdapter adapts the internal port.Publisher to the public Publisher interface.
type publisherAdapter struct {
	inner port.Publisher
}

func (a *publisherAdapter) Publish(ctx context.Context, env Envelope[json.RawMessage]) error {
	return a.inner.Publish(ctx, publicToDomain(env))
}

func (a *publisherAdapter) PublishBatch(ctx context.Context, envs []Envelope[json.RawMessage]) error {
	domainEnvs := make([]domain.Envelope[json.RawMessage], len(envs))
	for i, e := range envs {
		domainEnvs[i] = publicToDomain(e)
	}
	err := a.inner.PublishBatch(ctx, domainEnvs)
	if err == nil {
		return nil
	}
	// Translate internal *sns.BatchError to the public *events.BatchError so
	// callers in pkg/outbox can inspect partial failures without importing the
	// internal adapter package.
	var snsBatchErr *sns.BatchError
	if errors.As(err, &snsBatchErr) {
		pubErr := &BatchError{Failures: make([]BatchFailure, len(snsBatchErr.Failures))}
		for i, f := range snsBatchErr.Failures {
			pubErr.Failures[i] = BatchFailure{ID: f.ID, Code: f.Code, Message: f.Message, Retryable: f.Retryable}
		}
		return pubErr
	}
	return err
}

// publicToDomain converts a public Envelope to the internal domain Envelope.
func publicToDomain(e Envelope[json.RawMessage]) domain.Envelope[json.RawMessage] {
	return domain.Envelope[json.RawMessage]{
		ID:            e.ID,
		Type:          e.Type,
		Source:        e.Source,
		SchemaVersion: e.SchemaVersion,
		TenantID:      e.TenantID,
		TraceID:       e.TraceID,
		CorrelationID: e.CorrelationID,
		Subject:       e.Subject,
		Actor:         e.Actor,
		IPAddress:     e.IPAddress,
		UserAgent:     e.UserAgent,
		SchemaID:      e.SchemaID,
		Timestamp:     e.Timestamp,
		Payload:       e.Payload,
	}
}

// domainToPublic converts an internal domain Envelope to the public Envelope.
func domainToPublic(e domain.Envelope[json.RawMessage]) Envelope[json.RawMessage] {
	return Envelope[json.RawMessage]{
		ID:            e.ID,
		Type:          e.Type,
		Source:        e.Source,
		SchemaVersion: e.SchemaVersion,
		TenantID:      e.TenantID,
		TraceID:       e.TraceID,
		CorrelationID: e.CorrelationID,
		Subject:       e.Subject,
		Actor:         e.Actor,
		IPAddress:     e.IPAddress,
		UserAgent:     e.UserAgent,
		SchemaID:      e.SchemaID,
		Timestamp:     e.Timestamp,
		Payload:       e.Payload,
	}
}
