package sns_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/adapter/outbound/metrics"
	internalsns "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/adapter/outbound/sns"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/domain"
)

func dependencyCount(t *testing.T, p *metrics.Platform, op, outcome string) uint64 {
	t.Helper()
	m := &dto.Metric{}
	require.NoError(t, p.DependencyRequests.WithLabelValues("sns", op, outcome).(prometheus.Histogram).Write(m))
	return m.GetHistogram().GetSampleCount()
}

// Tier 1: each event counts once in platform_messages_published_total with the
// topic NAME (never the ARN); each SNS API call is one dependency request.
func TestPublisher_PlatformMetrics(t *testing.T) {
	prev := metrics.CurrentPlatform()
	t.Cleanup(func() { metrics.ReplacePlatform(prev) })
	_, err := metrics.InitWithIdentity(metrics.Identity{Domain: "iam", Service: "svc", Environment: "test"}, prometheus.NewRegistry())
	require.NoError(t, err)

	fail := false
	client := &mockSNSClient{
		publishFn: func(context.Context, *sns.PublishInput, ...func(*sns.Options)) (*sns.PublishOutput, error) {
			if fail {
				return nil, errors.New("throttled")
			}
			return &sns.PublishOutput{MessageId: aws.String("m")}, nil
		},
		publishBatchFn: func(_ context.Context, in *sns.PublishBatchInput, _ ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
			out := &sns.PublishBatchOutput{}
			for _, e := range in.PublishBatchRequestEntries {
				out.Successful = append(out.Successful, snstypes.PublishBatchResultEntry{Id: e.Id, MessageId: aws.String("m")})
			}
			return out, nil
		},
	}
	pub, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123:iam-events", client, nil)
	require.NoError(t, err)

	require.NoError(t, pub.Publish(context.Background(), makeEnv("iam.user.created")))
	fail = true
	require.Error(t, pub.Publish(context.Background(), makeEnv("iam.user.created")))
	require.NoError(t, pub.PublishBatch(context.Background(), []domain.Envelope[json.RawMessage]{makeEnv("iam.user.updated"), makeEnv("iam.user.updated")}))

	p := metrics.CurrentPlatform()
	assert.InDelta(t, 1, testutil.ToFloat64(p.MessagesPublished.WithLabelValues("iam-events", "iam.user.created", "success")), 0)
	assert.InDelta(t, 1, testutil.ToFloat64(p.MessagesPublished.WithLabelValues("iam-events", "iam.user.created", "error")), 0)
	assert.InDelta(t, 2, testutil.ToFloat64(p.MessagesPublished.WithLabelValues("iam-events", "iam.user.updated", "success")), 0, "a batch counts each message")
	assert.Equal(t, uint64(1), dependencyCount(t, p, "publish", "success"))
	assert.Equal(t, uint64(1), dependencyCount(t, p, "publish", "error"))
	assert.Equal(t, uint64(1), dependencyCount(t, p, "publish_batch", "success"), "one API call for the batch")
}
