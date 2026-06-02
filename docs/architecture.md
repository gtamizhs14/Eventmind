# EventMind — Architecture

## Overview

EventMind is a stateful event-processing platform where each business event triggers an LLM reasoning step that decides and executes a business action. The core pipeline is:

```
Event source → Kafka (events topic) → Worker (consumer) → Agent (LLM) → Action executor
                                                                 ↓
                                                          PostgreSQL (decisions)
                                                          MongoDB (raw events)
                                                          Redis (hot cache)
```

## Components

### cmd/api — API Server

Exposes REST and GraphQL endpoints. Two responsibilities:
1. Accepts events via `POST /api/v1/events` and publishes them to Kafka
2. Serves query endpoints for decisions and events (reads from Postgres/Redis)

The API server does **not** process events itself — it's a write-forward + read layer only.

### cmd/worker — Kafka Consumer Worker

Runs the main consume loop plus the DLQ retry loop. Two goroutines:
1. `Consumer.Run()` — reads from `events` topic, calls `Agent.Process()` for each
2. `DLQWorker.Run()` — reads from `events.dlq` topic, retries with backoff

Both loops block until context cancellation.

### internal/agent — AI Agent

The reasoning core. `Agent.Process()`:
1. Builds a structured prompt including event type, payload, and business context
2. Calls `llm.Provider.Complete()` with the prompt
3. Parses the JSON response `{"action": "...", "reasoning": "..."}`
4. Validates the action against the allowed tool set
5. Calls `executeAction()` for the chosen tool
6. Returns a `Decision` record for persistence

### pkg/llm — Provider abstraction

Single `Provider` interface with `Complete(ctx, prompt) (string, error)`. Three concrete implementations:
- `claudeProvider` — calls Anthropic Messages API
- `groqProvider` — calls Groq's OpenAI-compatible endpoint  
- `openAIProvider` — calls OpenAI Chat Completions API
- `mockProvider` — returns deterministic response for tests

Selected at startup via `LLM_PROVIDER` env var.

### internal/messaging — Kafka

**Producer:** Used by the API to publish ingested events. Configures SASL/SSL when `KAFKA_USE_TLS=true` (Confluent Cloud) or plain text for local dev.

**Consumer:** Reads events with at-least-once delivery. Commits offsets after successful processing. On failure, publishes to DLQ before committing.

**DLQ Worker:** Separate consumer group on `events.dlq` topic. Reads failed events, applies exponential backoff (1s, 2s, 4s, 8s, 16s), retries processing, and marks permanently failed after max attempts.

### internal/storage — PostgreSQL

Two tables:
- `events` — append-only log of all ingested events
- `decisions` — one row per agent decision with full reasoning, action, and timing

Uses pgx/v5 connection pool. Schema migrations run at startup.

### internal/storage — MongoDB

Raw event documents stored in the `events` collection, indexed by `type` and `timestamp`. Used for ad-hoc querying and event replay. The same event is stored both here (full doc) and in Postgres (structured metadata).

### internal/cache — Redis

Used for:
- Caching recent decisions to avoid DB hits on the query path (5min TTL)
- Preventing duplicate event processing (idempotency key = event ID, 24h TTL)

### internal/metrics — Prometheus

All metrics registered at startup. The API server exposes `/metrics` on a separate port (9091) to keep it off the main API port. The worker exposes metrics on port 9092.

## Data flow

### Happy path

```
1. Client POST /api/v1/events {type, payload}
2. API validates event type, generates UUID, saves to Postgres events table
3. API publishes event to Kafka events topic
4. API returns {id, type, timestamp} to client

5. Worker consumer reads event from Kafka
6. Worker checks Redis idempotency key — skip if already processed
7. Worker calls Agent.Process(ctx, event)
8. Agent builds prompt from event context
9. Agent calls LLM (records latency in Prometheus histogram)
10. Agent parses JSON response {action, reasoning}
11. Agent calls executeAction(action, payload) — logs, sends downstream call
12. Agent returns Decision{...}
13. Worker saves Decision to Postgres decisions table
14. Worker saves raw event doc to MongoDB
15. Worker stores decision in Redis cache (5min TTL)
16. Worker increments Prometheus counters
17. Worker commits Kafka offset
```

### Failure path

```
7a. Agent.Process fails (LLM error, action execution error, timeout)
8a. Worker increments DLQ counter in Prometheus
9a. Worker publishes failed event to events.dlq topic
10a. Worker commits offset on main topic (event is not lost — it's in DLQ)

DLQ Worker:
11a. DLQ consumer reads failed event
12a. Checks retry_count header
13a. Waits backoff(retry_count) duration
14a. Re-attempts Agent.Process
15a. On success: marks decision success in Postgres, commits DLQ offset
16a. On failure with retries remaining: increments retry_count, re-publishes to DLQ
17a. On max retries: marks decision permanently_failed in Postgres, commits offset
```

## Local dev vs production

| Service | Local (docker-compose) | Production |
|---------|----------------------|------------|
| Kafka | bitnami/kafka KRaft | Confluent Cloud |
| Postgres | postgres:16-alpine | Supabase |
| Redis | redis:7-alpine | Upstash |
| MongoDB | mongo:7 | MongoDB Atlas or self-hosted |
| Metrics | Prometheus + Grafana | Same (in-cluster) |

## Scaling considerations

- **Worker:** Kafka consumer groups allow horizontal scaling. Add replicas and Kafka rebalances partitions. Keep worker replicas ≤ partition count.
- **API:** Stateless, scales freely. HPA configured on CPU (70% target).
- **DLQ:** Single replica is fine — retry throughput is low by design.
- **LLM rate limits:** The agent adds per-request latency (typically 1-3s). This is the bottleneck. Kafka buffers handle burst; sustained throughput is bounded by LLM API rate limits.
