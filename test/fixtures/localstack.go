package fixtures

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// LocalStack holds a running LocalStack container and AWS clients for testing.
type LocalStack struct {
	Container    testcontainers.Container
	EndpointURL  string
	SNSClient    *sns.Client
	SQSClient    *sqs.Client
}

// StartLocalStack starts a LocalStack container and returns the helper.
// The container is terminated when t.Cleanup runs.
func StartLocalStack(ctx context.Context, t *testing.T) *LocalStack {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test — requires Docker (pass -short to skip)")
	}

	req := testcontainers.ContainerRequest{
		Image:        "localstack/localstack:3",
		ExposedPorts: []string{"4566/tcp"},
		Env:          map[string]string{"SERVICES": "sns,sqs"},
		WaitingFor:   wait.ForLog("Ready.").WithOccurrence(1),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("localstack: failed to start container: %v", err)
	}

	t.Cleanup(func() {
		_ = container.Terminate(context.Background())
	})

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("localstack: failed to get host: %v", err)
	}
	port, err := container.MappedPort(ctx, "4566")
	if err != nil {
		t.Fatalf("localstack: failed to get port: %v", err)
	}

	endpointURL := fmt.Sprintf("http://%s:%s", host, port.Port())

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(aws.AnonymousCredentials{}),
	)
	if err != nil {
		t.Fatalf("localstack: failed to load AWS config: %v", err)
	}

	snsClient := sns.NewFromConfig(awsCfg, func(o *sns.Options) {
		o.BaseEndpoint = aws.String(endpointURL)
	})
	sqsClient := sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(endpointURL)
	})

	return &LocalStack{
		Container:   container,
		EndpointURL: endpointURL,
		SNSClient:   snsClient,
		SQSClient:   sqsClient,
	}
}

// CreateTopic creates an SNS topic and returns its ARN.
func (ls *LocalStack) CreateTopic(ctx context.Context, t *testing.T, name string) string {
	t.Helper()
	out, err := ls.SNSClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String(name),
	})
	if err != nil {
		t.Fatalf("localstack: CreateTopic %q failed: %v", name, err)
	}
	return aws.ToString(out.TopicArn)
}

// CreateQueue creates an SQS queue and returns its URL.
func (ls *LocalStack) CreateQueue(ctx context.Context, t *testing.T, name string) string {
	t.Helper()
	out, err := ls.SQSClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(name),
	})
	if err != nil {
		t.Fatalf("localstack: CreateQueue %q failed: %v", name, err)
	}
	return aws.ToString(out.QueueUrl)
}

// SubscribeQueueToTopic subscribes an SQS queue to an SNS topic.
func (ls *LocalStack) SubscribeQueueToTopic(ctx context.Context, t *testing.T, topicARN, queueURL string) {
	t.Helper()
	// Derive the queue ARN from the URL.
	attrs, err := ls.SQSClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("localstack: GetQueueAttributes failed: %v", err)
	}
	queueARN := attrs.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]

	out, err := ls.SNSClient.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: aws.String(topicARN),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueARN),
		// RawMessageDelivery = true so the SQS message body is our Envelope JSON
		// directly, not wrapped in the SNS notification envelope.
		Attributes: map[string]string{"RawMessageDelivery": "true"},
	})
	if err != nil {
		t.Fatalf("localstack: Subscribe failed: %v", err)
	}

	// Confirm the subscription if LocalStack returns a confirmation URL.
	if aws.ToString(out.SubscriptionArn) == "pending confirmation" {
		t.Logf("localstack: subscription pending confirmation (unusual in LocalStack)")
	}
}
