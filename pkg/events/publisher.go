package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

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
	return fmt.Sprintf("events: %d message(s) failed in batch", len(e.Failures))
}

// BatchFailure describes a single failed message within a batch.
type BatchFailure struct {
	ID      string
	Code    string
	Message string
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
	EndpointURL string // optional — LocalStack endpoint for testing
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
			pubErr.Failures[i] = BatchFailure{ID: f.ID, Code: f.Code, Message: f.Message}
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
		Timestamp:     e.Timestamp,
		Payload:       e.Payload,
	}
}
