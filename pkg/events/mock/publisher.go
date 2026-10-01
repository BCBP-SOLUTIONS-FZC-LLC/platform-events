// Package mock provides in-memory test doubles for Publisher, Consumer and DLQPublisher.
package mock

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
)

// Publisher is a thread-safe in-memory Publisher for use in unit tests.
//
// Like the SNS publisher, it rejects an envelope without ID, Type or Source.
// SetError fails every call; SetBatchError makes PublishBatch fail part of a
// batch (with BatchFailure.Retryable to exercise the outbox's transient path).
type Publisher struct {
	mu        sync.Mutex
	published []events.Envelope[json.RawMessage]
	err       error
	batchErr  *events.BatchError
}

func validateEnvelope(env events.Envelope[json.RawMessage]) error {
	if env.ID == "" || env.Type == "" || env.Source == "" {
		return fmt.Errorf("mock: envelope requires ID, Type and Source (got id=%q type=%q source=%q)", env.ID, env.Type, env.Source)
	}
	return nil
}

// Publish records the envelope and returns any configured error.
func (m *Publisher) Publish(_ context.Context, env events.Envelope[json.RawMessage]) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	if err := validateEnvelope(env); err != nil {
		return err
	}
	m.published = append(m.published, env)
	return nil
}

// PublishBatch records every envelope, except those failing validation or
// listed in the error set with SetBatchError, which are returned as a
// *events.BatchError.
func (m *Publisher) PublishBatch(_ context.Context, envs []events.Envelope[json.RawMessage]) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	failing := map[string]events.BatchFailure{}
	if m.batchErr != nil {
		for _, f := range m.batchErr.Failures {
			failing[f.ID] = f
		}
	}
	var failures []events.BatchFailure
	for _, env := range envs {
		if err := validateEnvelope(env); err != nil {
			failures = append(failures, events.BatchFailure{ID: env.ID, Code: "InvalidEnvelope", Message: err.Error()})
			continue
		}
		if f, ok := failing[env.ID]; ok {
			failures = append(failures, f)
			continue
		}
		m.published = append(m.published, env)
	}
	if len(failures) > 0 {
		return &events.BatchError{Failures: failures}
	}
	return nil
}

// SetBatchError makes PublishBatch fail the envelopes whose IDs appear in
// be.Failures (returned as given, e.g. with Retryable set) and publish the
// rest. nil clears it.
func (m *Publisher) SetBatchError(be *events.BatchError) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.batchErr = be
}

// Published returns a copy of all published envelopes.
func (m *Publisher) Published() []events.Envelope[json.RawMessage] {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]events.Envelope[json.RawMessage], len(m.published))
	copy(out, m.published)
	return out
}

// SetError configures the error returned by the next Publish call(s).
func (m *Publisher) SetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = err
}

// Reset clears recorded envelopes and any configured error.
func (m *Publisher) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.published = nil
	m.err = nil
	m.batchErr = nil
}

// Ensure Publisher satisfies events.Publisher at compile time.
var _ events.Publisher = (*Publisher)(nil)
