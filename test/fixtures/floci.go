package fixtures

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Floci — the open-source (MIT), always-free AWS emulator the platform uses
// in place of LocalStack (same as iam-org-membership). It serves every
// service on :4566 (SNS, SQS, Glue Schema Registry, …) without a SERVICES
// selection or an auth token.
const (
	// FlociImage is the emulator image the integration and e2e suites run.
	// The docker-compose dev stack uses the -compat variant (AWS CLI bundled
	// for its healthcheck and init hook).
	// Digest-pinned like every image this repository runs (refreshed by
	// `make pin-base-images`, recorded in .docker-digests).
	FlociImage = "floci/floci:2.1.0@sha256:f5aa8c18302cedb4f2385f5c4e455b3efc77fee6bf7b6e5d1712b2817ba102db"
	// FlociRegion and FlociAccount are the emulator's default region and
	// account; floci treats region as an isolation boundary, so clients must
	// use FlociRegion.
	FlociRegion  = "us-east-1"
	FlociAccount = "000000000000"
)

// Floci holds a running floci container and AWS clients for testing.
type Floci struct {
	Container   testcontainers.Container
	EndpointURL string
	SNSClient   *sns.Client
	SQSClient   *sqs.Client

	mu    sync.Mutex
	names map[string]string // resource name → test that created it
}

// One floci container per test binary (package), like iam-org-membership:
// starting a container per test made the suites spend most of their time
// booting emulators. Tests stay isolated because CreateTopic / CreateQueue
// delete what they created when the test ends, and fail a test that reuses a
// name another live test holds.
var shared struct {
	once sync.Once
	emu  *Floci
	err  error
}

// StartFloci returns the package's shared floci container, starting it on
// first use. Skipped under -short (no Docker needed). Call
// TerminateSharedFloci from the package's TestMain: CI disables the
// testcontainers reaper (Ryuk), so nothing else removes the container.
func StartFloci(ctx context.Context, t *testing.T) *Floci {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test — requires Docker (pass -short to skip)")
	}
	shared.once.Do(func() { shared.emu, shared.err = startFloci() })
	if shared.err != nil {
		t.Fatalf("floci: %v", shared.err)
	}
	return shared.emu
}

// TerminateSharedFloci stops the package's shared container, if one was
// started. Call it from TestMain after m.Run.
func TerminateSharedFloci() {
	if shared.emu != nil {
		_ = shared.emu.Container.Terminate(context.Background())
	}
}

// flociStartupTimeout bounds pulling and booting the emulator.
const flociStartupTimeout = 2 * time.Minute

func startFloci() (*Floci, error) {
	// Not the calling test's ctx: the container outlives that test.
	ctx, cancel := context.WithTimeout(context.Background(), flociStartupTimeout)
	defer cancel()

	req := testcontainers.ContainerRequest{
		Image:        FlociImage,
		ExposedPorts: []string{"4566/tcp"},
		Env: map[string]string{
			"FLOCI_DEFAULT_REGION":     FlociRegion,
			"FLOCI_DEFAULT_ACCOUNT_ID": FlociAccount,
		},
		WaitingFor: wait.ForLog("Ready.").WithOccurrence(1).WithStartupTimeout(flociStartupTimeout),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start container: %w", err)
	}
	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(context.Background())
		return nil, fmt.Errorf("failed to get host: %w", err)
	}
	port, err := container.MappedPort(ctx, "4566")
	if err != nil {
		_ = container.Terminate(context.Background())
		return nil, fmt.Errorf("failed to get port: %w", err)
	}
	endpointURL := fmt.Sprintf("http://%s:%s", host, port.Port())

	// floci accepts any credentials, but the SDK signs requests, so it needs
	// some — static test credentials, never the developer's real ones.
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(FlociRegion),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		_ = container.Terminate(context.Background())
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}
	return &Floci{
		Container:   container,
		EndpointURL: endpointURL,
		SNSClient:   sns.NewFromConfig(awsCfg, func(o *sns.Options) { o.BaseEndpoint = aws.String(endpointURL) }),
		SQSClient:   sqs.NewFromConfig(awsCfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String(endpointURL) }),
		names:       map[string]string{},
	}, nil
}

// claim records that t owns name, failing t if another live test already
// created it in this shared container (SNS/SQS creates are idempotent, so a
// reused name would silently share a topic or queue — and its messages —
// between tests). The name is released when t's cleanup deletes the resource.
func (f *Floci) claim(t *testing.T, kind, name string) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	key := kind + "/" + name
	if owner, ok := f.names[key]; ok {
		t.Fatalf("floci: %s %q already created by %s in this package's shared container — use a unique name", kind, name, owner)
	}
	f.names[key] = t.Name()
}

func (f *Floci) release(kind, name string) {
	f.mu.Lock()
	delete(f.names, kind+"/"+name)
	f.mu.Unlock()
}

// CreateTopic creates an SNS topic and returns its ARN.
func (f *Floci) CreateTopic(ctx context.Context, t *testing.T, name string) string {
	t.Helper()
	f.claim(t, "topic", name)
	out, err := f.SNSClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String(name),
	})
	if err != nil {
		f.release("topic", name)
		t.Fatalf("floci: CreateTopic %q failed: %v", name, err)
	}
	arn := aws.ToString(out.TopicArn)
	// Delete on cleanup (subscriptions go with it) so the next test — or a
	// repeat run of this one (-count=N) — starts from an empty topic.
	t.Cleanup(func() {
		_, _ = f.SNSClient.DeleteTopic(context.Background(), &sns.DeleteTopicInput{TopicArn: aws.String(arn)})
		f.release("topic", name)
	})
	return arn
}

// CreateQueue creates an SQS queue and returns its URL.
func (f *Floci) CreateQueue(ctx context.Context, t *testing.T, name string) string {
	t.Helper()
	f.claim(t, "queue", name)
	out, err := f.SQSClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(name),
	})
	if err != nil {
		f.release("queue", name)
		t.Fatalf("floci: CreateQueue %q failed: %v", name, err)
	}
	url := aws.ToString(out.QueueUrl)
	// Delete on cleanup so no message outlives the test that sent it.
	t.Cleanup(func() {
		_, _ = f.SQSClient.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: aws.String(url)})
		f.release("queue", name)
	})
	return url
}

// SubscribeQueueToTopic subscribes an SQS queue to an SNS topic with raw
// message delivery, as production subscriptions must be.
func (f *Floci) SubscribeQueueToTopic(ctx context.Context, t *testing.T, topicARN, queueURL string) {
	t.Helper()
	attrs, err := f.SQSClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("floci: GetQueueAttributes failed: %v", err)
	}
	queueARN := attrs.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]

	out, err := f.SNSClient.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: aws.String(topicARN),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueARN),
		// RawMessageDelivery = true so the SQS message body is our Envelope JSON
		// directly, not wrapped in the SNS notification envelope.
		Attributes: map[string]string{"RawMessageDelivery": "true"},
	})
	if err != nil {
		t.Fatalf("floci: Subscribe failed: %v", err)
	}
	if aws.ToString(out.SubscriptionArn) == "pending confirmation" {
		t.Logf("floci: subscription pending confirmation (unexpected for SNS→SQS)")
	}
}
