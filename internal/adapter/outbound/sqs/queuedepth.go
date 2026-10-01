package sqs

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/adapter/outbound/metrics"
)

// MinQueueDepthInterval bounds how often each replica polls SQS for depth.
// A variable only so the test suite can exercise repeated polls quickly; it is
// in an internal package and not part of the public API.
var MinQueueDepthInterval = 10 * time.Second

// queueDepthCallTimeout bounds one GetQueueAttributes call.
const queueDepthCallTimeout = 10 * time.Second

// queueAttributesAPI is the optional client capability the depth poller
// needs. *sqs.Client implements it; SQSClientAPI deliberately does not
// require it, so existing test doubles keep compiling.
type queueAttributesAPI interface {
	GetQueueAttributes(ctx context.Context, params *sqs.GetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
}

// WithQueueDepthMetrics samples the queue's backlog — and that of the DLQ its
// RedrivePolicy points at — into platform_queue_depth / platform_dlq_depth
// every interval (minimum 10s) while the consumer runs. Services cannot read
// queue attributes themselves (they may not import the SQS SDK), so this is
// the only way to get queue depth into Prometheus. Each replica polls, costing
// one or two sqs:GetQueueAttributes calls per interval; 0 disables it.
//
// IAM: sqs:GetQueueAttributes on the queue and on its DLQ.
func WithQueueDepthMetrics(interval time.Duration) ConsumerOption {
	return func(c *sqsConsumer) {
		if interval > 0 && interval < MinQueueDepthInterval {
			interval = MinQueueDepthInterval
		}
		c.queueDepthInterval = interval
	}
}

// startQueueDepthPoller launches the depth sampler for one Start cycle and
// returns a function that waits for it to exit (after ctx is cancelled).
func (c *sqsConsumer) startQueueDepthPoller(ctx context.Context) (wait func()) {
	if c.queueDepthInterval <= 0 {
		return func() {}
	}
	// Without the Tier 1 metrics there is nowhere to record the depth: polling
	// would only cost SQS calls (and log IAM errors). InitMetrics must run
	// before Start.
	if p := metrics.CurrentPlatform(); p == nil || (p.QueueDepth == nil && p.DLQDepth == nil) {
		if c.logger != nil {
			c.logger.Warn("sqs: WithQueueDepthMetrics ignored — platform metrics are not initialised (call events.InitMetrics before Start)", map[string]any{"queue": c.queueURL})
		}
		return func() {}
	}
	api, ok := c.client.(queueAttributesAPI)
	if !ok {
		if c.logger != nil {
			c.logger.Warn("sqs: WithQueueDepthMetrics ignored — the SQS client does not implement GetQueueAttributes", map[string]any{"queue": c.queueURL})
		}
		return func() {}
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		ticker := time.NewTicker(c.queueDepthInterval)
		defer ticker.Stop()
		failing := map[string]string{} // queue URL → last logged error
		for {
			c.sampleQueueDepth(ctx, api, failing)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	})
	return wg.Wait
}

// sampleQueueDepth reads the source queue's backlog and RedrivePolicy, then
// the DLQ's backlog. Failures are logged and counted as dependency errors;
// the gauges keep their last value.
func (c *sqsConsumer) sampleQueueDepth(ctx context.Context, api queueAttributesAPI, failing map[string]string) {
	attrs, ok := c.queueAttributes(ctx, api, failing, c.queueURL,
		sqstypes.QueueAttributeNameApproximateNumberOfMessages, sqstypes.QueueAttributeNameRedrivePolicy)
	if !ok {
		return
	}
	if n, err := strconv.ParseFloat(attrs[string(sqstypes.QueueAttributeNameApproximateNumberOfMessages)], 64); err == nil {
		metrics.SetQueueDepth(c.queueURL, n)
	}

	policy := attrs[string(sqstypes.QueueAttributeNameRedrivePolicy)]
	if strings.TrimSpace(policy) == "" {
		return
	}
	dlqURL, ok := dlqURLFromPolicy(c.queueURL, policy)
	if !ok {
		return
	}
	dlqAttrs, ok := c.queueAttributes(ctx, api, failing, dlqURL, sqstypes.QueueAttributeNameApproximateNumberOfMessages)
	if !ok {
		return
	}
	if n, err := strconv.ParseFloat(dlqAttrs[string(sqstypes.QueueAttributeNameApproximateNumberOfMessages)], 64); err == nil {
		metrics.SetDLQDepth(c.queueURL, n)
	}
}

// queueAttributes reads names from queueURL. A persistent failure (typically a
// missing sqs:GetQueueAttributes permission) is logged once — and again only
// if the error changes — with one recovery message, instead of on every poll;
// every failure is still counted in platform_dependency_request_seconds.
func (c *sqsConsumer) queueAttributes(ctx context.Context, api queueAttributesAPI, failing map[string]string, queueURL string, names ...sqstypes.QueueAttributeName) (map[string]string, bool) {
	callCtx, cancel := context.WithTimeout(ctx, queueDepthCallTimeout)
	defer cancel()
	start := time.Now()
	out, err := api.GetQueueAttributes(callCtx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(queueURL), AttributeNames: names})
	if ctx.Err() != nil {
		return nil, false // stopping — not a dependency failure
	}
	metrics.ObserveDependency("sqs", "get_queue_attributes", err, time.Since(start))
	if err != nil {
		if c.logger != nil && failing[queueURL] != err.Error() {
			c.logger.Warn("sqs: queue depth sample failed (logged once until it recovers)", map[string]any{"queue": queueURL, "error": err.Error()})
		}
		failing[queueURL] = err.Error()
		return nil, false
	}
	if _, was := failing[queueURL]; was {
		delete(failing, queueURL)
		if c.logger != nil {
			c.logger.Info("sqs: queue depth sampling recovered", map[string]any{"queue": queueURL})
		}
	}
	return out.Attributes, true
}

// dlqURLFromPolicy derives the DLQ's URL from the source queue URL and its
// RedrivePolicy. SQS requires a DLQ in the same account and region as its
// source queue, so the URL is the source URL with the queue name replaced —
// no GetQueueUrl call (or permission) is needed.
func dlqURLFromPolicy(sourceQueueURL, policy string) (string, bool) {
	arn, err := ParseRedrivePolicy(policy)
	if err != nil {
		return "", false
	}
	name, _, _, err := ParseSQSQueueARN(arn)
	if err != nil {
		return "", false
	}
	i := strings.LastIndex(sourceQueueURL, "/")
	if i < 0 {
		return "", false
	}
	return sourceQueueURL[:i+1] + name, true
}
