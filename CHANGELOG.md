# Changelog

All notable changes to `platform-events` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

---

## [1.0.0] - 2026-05-29

### Added

- Initial implementation of `platform-events` shared library
- `pkg/events` — typed `Envelope[T]` with UUID v7 IDs, `NewEnvelope`, `ParseEnvelope`, `JSON()`
- `pkg/events` — `Publisher` interface and `NewSNSPublisher` (AWS SNS, FIFO support, batch)
- `pkg/events` — `Consumer` interface and `NewSQSConsumer` (long-poll, concurrency, drain)
- `pkg/events` — HMAC helpers: `Sign`, `Verify`, `SignEnvelope`, `VerifyEnvelope`
- `pkg/events` — Prometheus metrics: `Init`, `InitWithRegisterer`
- `pkg/events/mock` — `MockPublisher` and `MockConsumer` for unit testing
- `pkg/outbox` — transactional outbox `Runner`, `Enqueue`, `ApplySchema`
- `internal/core` — domain entities, port interfaces, HMAC and outbox services
- `internal/adapter/outbound` — SNS publisher, SQS consumer, Postgres outbox store, Zap logger, Prometheus metrics
- `internal/config` — environment-variable loading with defaults
- OTel tracing on SNS publish and SQS receive spans
- GUC injection into SQS handler context for `platform-pgcommon` RLS
- GitHub Actions CI/CD workflows (validate, CI, release)
