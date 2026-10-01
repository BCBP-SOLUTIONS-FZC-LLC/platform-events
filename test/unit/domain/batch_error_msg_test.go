package domain_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/domain"
)

// The message names the first failure, so a log line shows why.
func TestBatchError_MessageIncludesFirstFailure(t *testing.T) {
	err := &domain.BatchError{Failures: []domain.BatchFailure{{ID: "e1", Code: "InvalidParameter", Message: "bad attr"}, {ID: "e2"}}}
	assert.Equal(t, "events: 2 message(s) failed in batch (first: e1 InvalidParameter: bad attr)", err.Error())
	assert.Equal(t, "events: 0 message(s) failed in batch", (&domain.BatchError{}).Error())
}
