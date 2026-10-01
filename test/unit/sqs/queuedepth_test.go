package sqs_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/metrics"
	internalsqs "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/sqs"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/test/fixtures"
)

// depthClient is an SQS client double that also implements GetQueueAttributes.
type depthClient struct {
	mockSQSClient
	mu     sync.Mutex
	calls  []string
	attrFn func(queueURL string) (map[string]string, error)
	// blockUntilDone makes every call wait for the caller's context to end.
	blockUntilDone bool
}

func (d *depthClient) GetQueueAttributes(ctx context.Context, in *sqs.GetQueueAttributesInput, _ ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	d.mu.Lock()
	d.calls = append(d.calls, aws.ToString(in.QueueUrl))
	d.mu.Unlock()
	if d.blockUntilDone {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	attrs, err := d.attrFn(aws.ToString(in.QueueUrl))
	if err != nil {
		return nil, err
	}
	return &sqs.GetQueueAttributesOutput{Attributes: attrs}, nil
}

func (d *depthClient) called() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.calls...)
}

func idleReceive(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func runDepthConsumer(t *testing.T, client internalsqs.SQSClientAPI, opts ...internalsqs.ConsumerOption) *fixtures.MockLogger {
	t.Helper()
	logger := &fixtures.MockLogger{}
	c, err := internalsqs.NewWithClient(internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1, Logger: logger}, client,
		func(context.Context, domain.Envelope[json.RawMessage]) error { return nil },
		append([]internalsqs.ConsumerOption{internalsqs.WithDrainTimeout(time.Second)}, opts...)...)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = c.Start(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	return logger
}

func gaugeValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) (float64, bool) {
	t.Helper()
	m := findMetric(t, reg, name, labels)
	if m == nil {
		return 0, false
	}
	return m.GetGauge().GetValue(), true
}

func TestQueueDepth_SamplesQueueAndDLQ(t *testing.T) {
	reg := initPlatformMetrics(t)
	dlqURL := "https://sqs.us-east-1.amazonaws.com/123456789/test-queue-dlq"
	client := &depthClient{mockSQSClient: mockSQSClient{receiveMessageFn: idleReceive}}
	client.attrFn = func(q string) (map[string]string, error) {
		if q == dlqURL {
			return map[string]string{"ApproximateNumberOfMessages": "3"}, nil
		}
		return map[string]string{
			"ApproximateNumberOfMessages": "42",
			"RedrivePolicy":               `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:123456789:test-queue-dlq","maxReceiveCount":"5"}`,
		}, nil
	}
	runDepthConsumer(t, client, internalsqs.WithQueueDepthMetrics(time.Minute))

	eventually(t, func() bool {
		v, ok := gaugeValue(t, reg, "platform_dlq_depth", map[string]string{"queue": testQueue})
		return ok && v == 3
	}, "DLQ depth sampled")
	v, _ := gaugeValue(t, reg, "platform_queue_depth", map[string]string{"queue": testQueue})
	assert.InDelta(t, 42, v, 0)
	assert.Equal(t, []string{testQueueURL, dlqURL}, client.called()[:2], "DLQ URL derived from the source URL + RedrivePolicy ARN")
	assert.Equal(t, uint64(2), histogramCount(t, reg, "platform_dependency_request_seconds", map[string]string{"dependency": "sqs", "operation": "get_queue_attributes", "outcome": "success"}))
}

func TestQueueDepth_NoRedrivePolicyNoDLQGauge(t *testing.T) {
	reg := initPlatformMetrics(t)
	client := &depthClient{mockSQSClient: mockSQSClient{receiveMessageFn: idleReceive}}
	client.attrFn = func(string) (map[string]string, error) {
		return map[string]string{"ApproximateNumberOfMessages": "7"}, nil
	}
	runDepthConsumer(t, client, internalsqs.WithQueueDepthMetrics(time.Minute))
	eventually(t, func() bool {
		v, ok := gaugeValue(t, reg, "platform_queue_depth", map[string]string{"queue": testQueue})
		return ok && v == 7
	}, "queue depth sampled")
	_, ok := gaugeValue(t, reg, "platform_dlq_depth", map[string]string{"queue": testQueue})
	assert.False(t, ok)
	assert.Len(t, client.called(), 1)
}

func TestQueueDepth_ErrorsCountedAndLogged(t *testing.T) {
	reg := initPlatformMetrics(t)
	client := &depthClient{mockSQSClient: mockSQSClient{receiveMessageFn: idleReceive}}
	client.attrFn = func(string) (map[string]string, error) { return nil, errors.New("AccessDenied") }
	logger := runDepthConsumer(t, client, internalsqs.WithQueueDepthMetrics(time.Minute))
	eventually(t, func() bool {
		return histogramCount(t, reg, "platform_dependency_request_seconds", map[string]string{"dependency": "sqs", "operation": "get_queue_attributes", "outcome": "error"}) == 1
	}, "error counted")
	_, ok := gaugeValue(t, reg, "platform_queue_depth", map[string]string{"queue": testQueue})
	assert.False(t, ok)
	assert.Contains(t, warnMessages(logger), "sqs: queue depth sample failed (logged once until it recovers)")
}

func TestQueueDepth_ClientWithoutCapabilityWarns(t *testing.T) {
	initPlatformMetrics(t)
	logger := runDepthConsumer(t, &mockSQSClient{receiveMessageFn: idleReceive}, internalsqs.WithQueueDepthMetrics(time.Minute))
	eventually(t, func() bool {
		for _, m := range warnMessages(logger) {
			if m == "sqs: WithQueueDepthMetrics ignored — the SQS client does not implement GetQueueAttributes" {
				return true
			}
		}
		return false
	}, "warning logged")
}

func TestQueueDepth_DisabledByDefault(t *testing.T) {
	initPlatformMetrics(t)
	client := &depthClient{mockSQSClient: mockSQSClient{receiveMessageFn: idleReceive}}
	client.attrFn = func(string) (map[string]string, error) { return map[string]string{}, nil }
	runDepthConsumer(t, client)
	time.Sleep(100 * time.Millisecond)
	assert.Empty(t, client.called())
}

func TestQueueDepth_IntervalClampedToMinimum(t *testing.T) {
	initPlatformMetrics(t)
	client := &depthClient{mockSQSClient: mockSQSClient{receiveMessageFn: idleReceive}}
	client.attrFn = func(string) (map[string]string, error) {
		return map[string]string{"ApproximateNumberOfMessages": "1"}, nil
	}
	runDepthConsumer(t, client, internalsqs.WithQueueDepthMetrics(time.Millisecond))
	eventually(t, func() bool { return len(client.called()) >= 1 }, "first sample")
	time.Sleep(200 * time.Millisecond)
	assert.Len(t, client.called(), 1, "a 1ms interval is clamped to 10s, not a tight polling loop")
}

func TestQueueDepth_UnusableRedrivePolicyOrDLQFailure(t *testing.T) {
	for name, tc := range map[string]struct {
		policy    string
		dlqErr    error
		wantCalls int
	}{
		"not JSON":       {policy: "{", wantCalls: 1},
		"not an SQS ARN": {policy: `{"deadLetterTargetArn":"arn:aws:sns:us-east-1:1:topic"}`, wantCalls: 1},
		"DLQ read fails": {policy: `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:123456789:test-queue-dlq"}`, dlqErr: errors.New("AccessDenied"), wantCalls: 2},
	} {
		t.Run(name, func(t *testing.T) {
			reg := initPlatformMetrics(t)
			client := &depthClient{mockSQSClient: mockSQSClient{receiveMessageFn: idleReceive}}
			client.attrFn = func(q string) (map[string]string, error) {
				if q != testQueueURL {
					return nil, tc.dlqErr
				}
				return map[string]string{"ApproximateNumberOfMessages": "5", "RedrivePolicy": tc.policy}, nil
			}
			runDepthConsumer(t, client, internalsqs.WithQueueDepthMetrics(time.Minute))
			eventually(t, func() bool {
				v, ok := gaugeValue(t, reg, "platform_queue_depth", map[string]string{"queue": testQueue})
				return ok && v == 5
			}, "queue depth still sampled")
			eventually(t, func() bool { return len(client.called()) == tc.wantCalls }, "expected calls")
			_, ok := gaugeValue(t, reg, "platform_dlq_depth", map[string]string{"queue": testQueue})
			assert.False(t, ok)
		})
	}
}

func TestQueueDepth_SkippedWithoutPlatformMetrics(t *testing.T) {
	prev := metrics.ReplacePlatform(nil)
	t.Cleanup(func() { metrics.ReplacePlatform(prev) })
	client := &depthClient{mockSQSClient: mockSQSClient{receiveMessageFn: idleReceive}}
	client.attrFn = func(string) (map[string]string, error) { return map[string]string{}, nil }
	logger := runDepthConsumer(t, client, internalsqs.WithQueueDepthMetrics(time.Minute))
	eventually(t, func() bool {
		for _, m := range warnMessages(logger) {
			if m == "sqs: WithQueueDepthMetrics ignored — platform metrics are not initialised (call events.InitMetrics before Start)" {
				return true
			}
		}
		return false
	}, "warning logged")
	time.Sleep(100 * time.Millisecond)
	assert.Empty(t, client.called(), "no SQS calls when there is nowhere to record the depth")
}

// A persistent failure (e.g. missing sqs:GetQueueAttributes) is logged once,
// not on every poll, and recovery is logged once.
func TestQueueDepth_PersistentFailureLoggedOnce(t *testing.T) {
	prevMin := internalsqs.MinQueueDepthInterval
	internalsqs.MinQueueDepthInterval = 10 * time.Millisecond
	t.Cleanup(func() { internalsqs.MinQueueDepthInterval = prevMin })
	reg := initPlatformMetrics(t)

	var mu sync.Mutex
	fail := true
	client := &depthClient{mockSQSClient: mockSQSClient{receiveMessageFn: idleReceive}}
	client.attrFn = func(string) (map[string]string, error) {
		mu.Lock()
		defer mu.Unlock()
		if fail {
			return nil, errors.New("AccessDenied")
		}
		return map[string]string{"ApproximateNumberOfMessages": "4"}, nil
	}
	logger := runDepthConsumer(t, client, internalsqs.WithQueueDepthMetrics(10*time.Millisecond))

	eventually(t, func() bool { return len(client.called()) >= 5 }, "several polls")
	count := func(msg string) (n int) {
		for _, e := range logger.Entries() {
			if e.Message == msg {
				n++
			}
		}
		return n
	}
	assert.Equal(t, 1, count("sqs: queue depth sample failed (logged once until it recovers)"))
	assert.GreaterOrEqual(t, histogramCount(t, reg, "platform_dependency_request_seconds", map[string]string{"dependency": "sqs", "operation": "get_queue_attributes", "outcome": "error"}), uint64(5),
		"every failure is still counted")

	mu.Lock()
	fail = false
	mu.Unlock()
	eventually(t, func() bool {
		v, ok := gaugeValue(t, reg, "platform_queue_depth", map[string]string{"queue": testQueue})
		return ok && v == 4
	}, "recovered")
	eventually(t, func() bool { return count("sqs: queue depth sampling recovered") == 1 }, "recovery logged once")
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, count("sqs: queue depth sampling recovered"))
}

// Stopping the consumer while a depth sample is in flight is not a dependency
// failure: nothing is counted or logged.
func TestQueueDepth_StopMidCallNotCountedAsFailure(t *testing.T) {
	reg := initPlatformMetrics(t)
	client := &depthClient{mockSQSClient: mockSQSClient{receiveMessageFn: idleReceive}, blockUntilDone: true}
	logger := &fixtures.MockLogger{}
	c, err := internalsqs.NewWithClient(internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1, Logger: logger}, client,
		func(context.Context, domain.Envelope[json.RawMessage]) error { return nil },
		internalsqs.WithQueueDepthMetrics(time.Minute), internalsqs.WithDrainTimeout(time.Second))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = c.Start(ctx) }()

	eventually(t, func() bool { return len(client.called()) == 1 }, "sample in flight")
	cancel()
	<-done
	assert.Zero(t, histogramCount(t, reg, "platform_dependency_request_seconds", map[string]string{"dependency": "sqs", "operation": "get_queue_attributes", "outcome": "error"}))
	assert.NotContains(t, warnMessages(logger), "sqs: queue depth sample failed (logged once until it recovers)")
}
