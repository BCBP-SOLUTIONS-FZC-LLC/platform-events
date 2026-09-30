package fixtures

import (
	"context"
	"fmt"
	"testing"

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
	FlociImage = "floci/floci:2.1.0"
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
}

// StartFloci starts a floci container and returns the helper. The container
// is terminated when t.Cleanup runs. Skipped under -short (no Docker needed).
func StartFloci(ctx context.Context, t *testing.T) *Floci {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test — requires Docker (pass -short to skip)")
	}

	req := testcontainers.ContainerRequest{
		Image:        FlociImage,
		ExposedPorts: []string{"4566/tcp"},
		Env: map[string]string{
			"FLOCI_DEFAULT_REGION":     FlociRegion,
			"FLOCI_DEFAULT_ACCOUNT_ID": FlociAccount,
		},
		WaitingFor: wait.ForLog("Ready.").WithOccurrence(1),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("floci: failed to start container: %v", err)
	}

	t.Cleanup(func() {
		_ = container.Terminate(context.Background())
	})

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("floci: failed to get host: %v", err)
	}
	port, err := container.MappedPort(ctx, "4566")
	if err != nil {
		t.Fatalf("floci: failed to get port: %v", err)
	}

	endpointURL := fmt.Sprintf("http://%s:%s", host, port.Port())

	// floci accepts any credentials, but the SDK signs requests, so it needs
	// some — static test credentials, never the developer's real ones.
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(FlociRegion),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("floci: failed to load AWS config: %v", err)
	}

	snsClient := sns.NewFromConfig(awsCfg, func(o *sns.Options) {
		o.BaseEndpoint = aws.String(endpointURL)
	})
	sqsClient := sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(endpointURL)
	})

	return &Floci{
		Container:   container,
		EndpointURL: endpointURL,
		SNSClient:   snsClient,
		SQSClient:   sqsClient,
	}
}

// CreateTopic creates an SNS topic and returns its ARN.
func (f *Floci) CreateTopic(ctx context.Context, t *testing.T, name string) string {
	t.Helper()
	out, err := f.SNSClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String(name),
	})
	if err != nil {
		t.Fatalf("floci: CreateTopic %q failed: %v", name, err)
	}
	return aws.ToString(out.TopicArn)
}

// CreateQueue creates an SQS queue and returns its URL.
func (f *Floci) CreateQueue(ctx context.Context, t *testing.T, name string) string {
	t.Helper()
	out, err := f.SQSClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(name),
	})
	if err != nil {
		t.Fatalf("floci: CreateQueue %q failed: %v", name, err)
	}
	return aws.ToString(out.QueueUrl)
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
