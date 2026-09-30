# Testing in consuming services

Mocks for `Publisher`, `Consumer` and `DLQPublisher`, and wiring them in tests. One of the detailed guides linked from the [project README](../../README.md#contributing).

---

## Testing in consuming services

`pkg/events/mock` ships ready-made, thread-safe test doubles so consuming services never need to stand up an AWS emulator (floci) or SNS just to run a unit test.

```go
import "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events/mock"
```

### Mock Publisher

```go
func TestOrderService_PlaceOrder_PublishesEvent(t *testing.T) {
    pub := &mock.Publisher{}
    svc := NewOrderService(pub)

    err := svc.PlaceOrder(ctx, Order{ID: "ord-1", Amount: 99})
    require.NoError(t, err)

    published := pub.Published()
    require.Len(t, published, 1)
    assert.Equal(t, "order.placed", published[0].Type)
    assert.Equal(t, "acme", published[0].TenantID)
}

func TestOrderService_PlaceOrder_PublishError_IsHandled(t *testing.T) {
    pub := &mock.Publisher{}
    pub.SetError(errors.New("sns unavailable"))

    svc := NewOrderService(pub)
    err := svc.PlaceOrder(ctx, Order{ID: "ord-2", Amount: 50})

    // Your service decides whether to propagate or swallow the publish error.
    require.Error(t, err)
    assert.Empty(t, pub.Published()) // nothing was recorded
}
```

The `mock.Publisher` API:

| Method | What it does |
|--------|-------------|
| `Publish(ctx, env)` | Records the envelope; returns any error set via `SetError` |
| `PublishBatch(ctx, envs)` | Calls `Publish` for each envelope |
| `Published()` | Returns a copy of all recorded envelopes (thread-safe) |
| `SetError(err)` | Makes all subsequent `Publish` calls return `err` |
| `Reset()` | Clears recorded envelopes and any configured error |

### Mock Consumer

```go
func TestNotificationWorker_HandlesUserCreated(t *testing.T) {
    consumer := &mock.Consumer{}
    worker := NewNotificationWorker(consumer)

    // Start the worker (mock Start() is a no-op — no goroutine needed).
    require.NoError(t, consumer.Start(ctx))
    worker.RegisterHandlers()

    // Inject an event directly — no SQS, no network.
    env := events.NewEnvelope("iam.user.created", "platform-iam",
        json.RawMessage(`{"user_id":"u-123","email":"alice@example.com"}`),
        events.WithTenantID("acme"),
    )
    err := consumer.Inject(env)
    require.NoError(t, err)

    // Assert your handler's side effects.
    assert.True(t, worker.EmailWasSentTo("alice@example.com"))
}
```

The `mock.Consumer` API:

| Method | What it does |
|--------|-------------|
| `Start(ctx)` | No-op; marks consumer as running |
| `Stop()` | No-op; marks consumer as stopped |
| `SetHandler(fn)` | Registers the handler called by `Inject` |
| `Inject(env)` | Delivers the envelope synchronously to the registered handler |
| `IsRunning()` | Reports whether `Start` has been called without a matching `Stop` |

### Mock DLQPublisher

```go
func TestOrderHandler_ForwardsPoisonMessage(t *testing.T) {
    dlq := &mock.DLQPublisher{}
    h := NewOrderHandler(repo, dlq, "https://sqs.test/orders")

    env := events.NewEnvelope("order.shipped", "orders", json.RawMessage(`{"order_id":"missing"}`))
    require.NoError(t, h.Handle(ctx, env))

    sent := dlq.Sent()
    require.Len(t, sent, 1)
    assert.Equal(t, "order not found", sent[0].Reason)
}

func TestOrderHandler_DLQFailure_KeepsMessage(t *testing.T) {
    dlq := &mock.DLQPublisher{}
    dlq.SetError(events.ErrDLQNotConfigured)
    h := NewOrderHandler(repo, dlq, "https://sqs.test/orders")

    env := events.NewEnvelope("order.shipped", "orders", json.RawMessage(`{"order_id":"missing"}`))
    require.ErrorIs(t, h.Handle(ctx, env), events.ErrDLQNotConfigured) // original stays on the queue
}
```

The `mock.DLQPublisher` API:

| Method | What it does |
|--------|-------------|
| `SendToDLQ(ctx, sourceQueueURL, body, attrs, reason)` | Records a copy of the message as a `DLQMessage`; returns any error set via `SetError` |
| `ResolveDLQ(ctx, sourceQueueURL)` | Returns the `DLQURL` field (default `mock://dlq`), or the configured error |
| `Sent()` | Returns a copy of all recorded `DLQMessage` values (`SourceQueueURL`, `Body`, `Attrs`, `Reason`) |
| `SetError(err)` | Makes subsequent calls return `err` |
| `Reset()` | Clears recorded messages and any configured error |

To test the real SQS resolution logic instead, use `events.NewSQSDLQPublisherWithClient` with a fake `events.DLQClientLike`.

### Wiring in tests

All three mocks satisfy the `events.Publisher`, `events.Consumer` and `events.DLQPublisher` interfaces, so they drop in wherever the real implementations are used:

```go
// Production wiring:
publisher, _ := events.NewSNSPublisher(events.SNSConfig{TopicARN: ..., Region: ...})
svc := NewOrderService(publisher)

// Test wiring — identical constructor, no AWS needed:
pub := &mock.Publisher{}
svc := NewOrderService(pub)
```

> **Why not write your own mock?** `events.Publisher` is only two methods, and writing a local stub is perfectly valid Go. The benefit of `pkg/events/mock` is the `SetError` / `Reset` / thread-safety boilerplate that every service would otherwise duplicate — and the guarantee that the mock's signature tracks the library's interface across version bumps.

> **Testing the consumer loop itself, not just your handler?** `mock.Consumer` bypasses SQS entirely — it can't exercise retry, visibility-extension, or dead-letter behaviour. For that, use `events.NewSQSConsumerWithClient` with a fake `events.SQSClientLike` — see [SQS Consumer § Testing with an injected client](consuming.md#testing-with-an-injected-client).

