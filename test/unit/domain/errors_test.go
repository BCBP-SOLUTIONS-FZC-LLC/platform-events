package domain_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
)

func TestRetryableError_Error(t *testing.T) {
	cause := errors.New("throttling")
	re := &domain.RetryableError{Cause: cause}
	assert.Equal(t, "throttling", re.Error())
}

func TestRetryableError_Unwrap(t *testing.T) {
	cause := errors.New("service unavailable")
	re := &domain.RetryableError{Cause: cause}
	assert.Equal(t, cause, re.Unwrap())
	assert.True(t, errors.Is(re, cause))
}

func TestRetryableError_Is_ErrRetryable(t *testing.T) {
	cause := errors.New("request throttled")
	re := &domain.RetryableError{Cause: cause}
	assert.True(t, errors.Is(re, domain.ErrRetryable), "should match ErrRetryable sentinel")
}

func TestRetryableError_Is_OtherError(t *testing.T) {
	cause := errors.New("something else")
	re := &domain.RetryableError{Cause: cause}
	other := errors.New("not retryable")
	assert.False(t, re.Is(other), "should not match arbitrary errors")
}

func TestRetryableError_WrappedInErrors_Is(t *testing.T) {
	cause := errors.New("inner cause")
	re := &domain.RetryableError{Cause: cause}
	wrapped := fmt.Errorf("outer: %w", re)
	assert.True(t, errors.Is(wrapped, domain.ErrRetryable))
}
