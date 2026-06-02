# EventMind

Real-time AI agent platform. Kafka events come in, an LLM decides what to do, stuff happens.

## What this is

EventMind processes business events as they arrive and uses an LLM to decide what to do with each one. Not just routing — the agent reasons about context, picks an action, executes it, and logs everything. Five event types, five tool actions, fully observable.

Built in Go. Uses Confluent Cloud for Kafka, Supabase for Postgres, Upstash for Redis in production. Local dev runs everything in Docker.

## Architecture

```
┌───────────────────────────────────────────────────────────────────────┐
│                            EventMind                                   │
│                                                                        │
│   Client                                                               │
│     │  REST POST /api/v1/events                                        │
│     │  GraphQL mutations                                               │
│     ▼                                                                  │
│  ┌──────────┐    events topic     ┌──────────────────────────────────┐ │
│  │  Kafka   │ ──────────────────► │            Worker                │ │
│  │ Producer │                     │  Consumer → Agent → Tool Exec    │ │
│  └──────────┘                     └──────┬───────────┬───────────────┘ │
│                                          │           │                  │
│                                          ▼           ▼                  │
│                                     PostgreSQL    MongoDB               │
│                                    (decisions)  (raw events)           │
│                                                                        │
│   Failed events ──► DLQ topic ──► Retry worker (exp backoff, 5x)     │
│                                                                        │
│   Redis (hot cache)        Prometheus + Grafana (metrics)             │
│                                                                        │
│   REST API (gin)           GraphQL API (gqlgen)                       │
└───────────────────────────────────────────────────────────────────────┘
```

## What the agent does

| Event type | LLM reasons about | Action taken |
|---|---|---|
| `order_placed` | order value, customer history | `send_notification` |
| `support_ticket_created` | priority, sentiment | `escalate_ticket` or `flag_for_review` |
| `payment_failed` | attempt count, amount | `flag_for_review` + `send_notification` |
| `user_signup` | plan, acquisition source | `send_welcome_sequence` |
| `inventory_low` | stock level vs threshold | `update_inventory` |

The agent builds a structured prompt with event context, sends it to the LLM, expects back `{"action": "...", "reasoning": "..."}`, executes the action, and writes the full decision record to Postgres.

## Prerequisites

- Go 1.22+
- Docker + Docker Compose
- [Confluent Cloud](https://confluent.io) free tier — Kafka broker
- [Supabase](https://supabase.com) free tier — Postgres
- [Upstash](https://upstash.com) free tier — Redis
- LLM API key (Claude default; Groq and OpenAI also supported)

> **Note:** The Kafka consumer uses `confluent-kafka-go` which requires CGo + librdkafka.
> `make docker-up` handles this automatically. For local builds outside Docker, you need
> `librdkafka-dev` installed (or use WSL on Windows).

## Quickstart

```bash
git clone https://github.com/gtamizhs14/eventmind
cd eventmind

cp .env.example .env
# edit .env — add your API keys and connection strings

make docker-up      # postgres, mongo, redis, local kafka, prometheus, grafana
make run-api        # http://localhost:8080
make run-worker     # separate terminal
make seed           # fires 50 sample events
```

## LLM providers

Two env vars to switch:

```bash
# Claude (default, recommended)
LLM_PROVIDER=claude
LLM_API_KEY=sk-ant-...

# Groq (fast, cheap for dev/testing)
LLM_PROVIDER=groq
LLM_API_KEY=gsk_...

# OpenAI
LLM_PROVIDER=openai
LLM_API_KEY=sk-...
```

Claude is the primary supported provider. Its structured output handling is more reliable for the agent's JSON parsing, and the claude-3-5-sonnet model hits the right balance of speed and reasoning quality for this workload.

## API

### REST

```bash
# Ingest an event
curl -X POST http://localhost:8080/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "type": "order_placed",
    "payload": {
      "order_id": "ord_123",
      "amount": 99.99,
      "customer_id": "cust_456",
      "items": [{"sku": "WIDGET-1", "quantity": 2, "price": 49.99}]
    }
  }'

# List recent decisions
curl "http://localhost:8080/api/v1/decisions?limit=10&offset=0"

# Get a specific decision
curl http://localhost:8080/api/v1/decisions/dec_abc123

# Health check
curl http://localhost:8080/health
```

### GraphQL

Playground at `http://localhost:8080/graphql`

```graphql
query RecentDecisions {
  decisions(limit: 10) {
    id
    eventType
    action
    reasoning
    durationMs
    processedAt
  }
}

mutation IngestEvent {
  ingestEvent(
    type: "user_signup"
    payload: "{\"user_id\":\"u1\",\"email\":\"x@y.com\",\"plan\":\"pro\",\"source\":\"referral\"}"
  ) {
    id
    type
    timestamp
  }
}
```

## Make commands

```
make build         compile api and worker to bin/
make test          unit tests
make test-int      integration tests (needs services running)
make run-api       start API server on :8080
make run-worker    start Kafka consumer worker
make seed          generate and fire 50 sample events
make docker-up     start all local services
make docker-down   stop everything
make docker-logs   follow service logs
make lint          golangci-lint
make gen           regenerate gqlgen GraphQL code
make clean         delete build artifacts
```

## Observability

- **Prometheus:** `http://localhost:9090`
- **Grafana:** `http://localhost:3000` (admin/admin)
- **Kafka UI:** `http://localhost:8090`

Key metrics:

| Metric | Type | Labels |
|---|---|---|
| `eventmind_events_processed_total` | Counter | `event_type`, `status` |
| `eventmind_actions_taken_total` | Counter | `action` |
| `eventmind_llm_request_duration_seconds` | Histogram | `provider` |
| `eventmind_dlq_events_total` | Counter | `event_type` |
| `eventmind_retry_attempts_total` | Counter | `outcome` |

## Dead letter queue

Failed events go to `events.dlq`. The retry worker polls this topic and retries with exponential backoff: 1s, 2s, 4s, 8s, 16s. After 5 failures the event is marked permanently failed in Postgres and dropped from the queue.

## Project layout

```
eventmind/
├── cmd/
│   ├── api/           REST + GraphQL server entry point
│   └── worker/        Kafka consumer + retry worker entry point
├── internal/
│   ├── agent/         LLM reasoning + tool execution
│   ├── events/        event type definitions
│   ├── storage/       postgres + mongo clients
│   ├── cache/         redis client
│   ├── messaging/     kafka producer, consumer, DLQ handler
│   └── metrics/       prometheus metric registrations
├── pkg/
│   ├── llm/           provider-agnostic LLM client (claude/groq/openai)
│   └── logger/        zerolog setup
├── api/
│   ├── graphql/       gqlgen schema + resolvers
│   └── rest/          gin handlers + middleware
├── infrastructure/
│   └── terraform/     AWS EKS + ECR + VPC (us-east-1)
├── deployments/
│   ├── docker-compose.yml
│   └── k8s/           Kubernetes manifests
└── scripts/
    └── seed.go        generates 50 sample events across all types
```

## AWS deployment

Terraform in `infrastructure/terraform/` targets EKS on us-east-1. Kafka stays on Confluent Cloud, Postgres on Supabase, Redis on Upstash.

```bash
cd infrastructure/terraform
cp prod.tfvars.example prod.tfvars  # fill in values
terraform init
terraform plan -var-file=prod.tfvars
terraform apply -var-file=prod.tfvars

# After apply, update image references in k8s manifests:
# sed -i "s|<ECR_API_URL>|$(terraform output -raw ecr_api_url)|g" ../../deployments/k8s/api-deployment.yaml
```

Images are built and deployed via GitHub Actions on push to `main`.

## Database schema

**Postgres** — stores agent decisions and event metadata:
- `events` — ingested event log (id, type, payload, timestamp, source)
- `decisions` — agent decisions (event_id, action, reasoning, success, error, duration_ms)

**MongoDB** — stores raw event documents exactly as received, indexed by event type and timestamp.
