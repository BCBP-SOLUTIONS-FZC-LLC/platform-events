package sqs_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
)

// W3C baggage carried in the message attributes reaches the handler context.
func TestDispatch_BaggagePropagatedToHandler(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	env := domain.NewEnvelope("iam.user.created", "svc", json.RawMessage(`{}`))
	msg := makeSQSMessage(env)
	msg.MessageAttributes = map[string]sqstypes.MessageAttributeValue{
		"baggage": {DataType: aws.String("String"), StringValue: aws.String("feature=beta,region=eu")},
	}
	got := make(chan baggage.Baggage, 1)
	consumeOnce(t, msg, func(ctx context.Context, _ domain.Envelope[json.RawMessage]) error {
		got <- baggage.FromContext(ctx)
		return nil
	})
	select {
	case b := <-got:
		assert.Equal(t, "beta", b.Member("feature").Value())
		assert.Equal(t, "eu", b.Member("region").Value())
	case <-time.After(5 * time.Second):
		t.Fatal("handler not called")
	}
}
