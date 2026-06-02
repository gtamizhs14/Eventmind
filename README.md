# EventMind

Real-time AI agent platform. Kafka events come in, an LLM decides what to do, stuff happens.

## What this is

EventMind processes business events as they arrive and uses an LLM to decide what to do with each one. Not just routing — the agent reasons about context, picks an action, executes it, and logs everything with full reasoning. Five event types, five tool actions, fully observable.

Built in Go. Uses Confluent Cloud for Kafka, Supabase for Postgres, Upstash for Redis in production. Local dev runs everything in Docker — no cloud accounts needed to try it.

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
| `order_placed` | order value, items, customer | `send_notification` or `flag_for_review` |
| `support_ticket_created` | priority, sentiment | `escalate_ticket` or `flag_for_review` |
| `payment_failed` | attempt count, failure reason | `flag_for_review` + `send_notification` |
| `user_signup` | plan tier, acquisition source | `send_welcome_sequence` |
| `inventory_low` | stock level vs threshold | `update_inventory` |

The agent builds a structured prompt with event context, calls the LLM, parses `{"action": "...", "reasoning": "..."}` from the response, executes the action, and writes the full decision record (including LLM prompt, response, and reasoning) to Postgres.

## Prerequisites

- Go 1.22+
- Docker + Docker Compose
- LLM API key — Groq (free at console.groq.com), Claude, or OpenAI

> **Note:** `confluent-kafka-go` requires CGo + librdkafka. Docker handles this automatically.
> For local builds outside Docker on Windows, use WSL or install MinGW + librdkafka-dev.

## Quickstart

```bash
git clone https://github.com/gtamizhs14/Eventmind
cd Eventmind

# 1. One-time setup
go mod tidy          # generates go.sum
make gen             # generates GraphQL code from schema (requires go mod tidy first)

# 2. Configure environment
cp .env.example .env
cp .env deployments/.env
# edit both .env files — set LLM_PROVIDER and LLM_API_KEY at minimum

# 3. Build Docker images
docker build --build-arg BINARY=api    -t eventmind-api:latest    .
docker build --build-arg BINARY=worker -t eventmind-worker:latest .

# 4. Start everything
docker compose -f deployments/docker-compose.yml up -d

# 5. Fire sample events
make seed   # or: go run ./scripts/seed.go
```

Everything runs in Docker. API at `http://localhost:8080`, Grafana at `http://localhost:3000`.

## LLM providers

Two env vars to switch providers, zero code changes:

```bash
# Groq — fast, free tier available (recommended for local dev)
LLM_PROVIDER=groq
LLM_API_KEY=gsk_...

# Claude — primary supported provider, best JSON reliability
LLM_PROVIDER=claude
LLM_API_KEY=sk-ant-...

# OpenAI
LLM_PROVIDER=openai
LLM_API_KEY=sk-...
```

The LLM client is a single `Complete(ctx, prompt) (string, error)` interface — adding a new provider is one file.

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
      "customer_id": "cust_456",
      "amount": 99.99,
      "items": [{"sku": "WIDGET-1", "quantity": 2, "price": 49.99}]
    },
    "source": "checkout-service"
  }'

# List recent decisions (with optional event_type filter)
curl "http://localhost:8080/api/v1/decisions?limit=10&offset=0&event_type=payment_failed"

# Get a specific decision
curl http://localhost:8080/api/v1/decisions/<decision-id>

# Health check
curl http://localhost:8080/health
```

### GraphQL

Playground at `http://localhost:8080/graphql/playground`

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

# Live subscription — streams decisions as they are made
subscription {
  onDecision {
    eventType
    action
    reasoning
  }
}
```

## Make commands

```
make build         compile api and worker binaries to bin/
make test          unit tests (no services needed)
make test-int      integration tests (requires DATABASE_URL + REDIS_URL)
make run-api       start API server locally on :8080
make run-worker    start Kafka consumer worker locally
make seed          generate and fire 50 sample events
make docker-up     start all local infra (shortcut for docker compose up)
make docker-down   stop everything
make docker-logs   follow all service logs
make lint          golangci-lint
make gen           regenerate gqlgen GraphQL code from schema.graphql
make clean         delete build artifacts
```

## Observability

| Service | URL | Credentials |
|---|---|---|
| API | `http://localhost:8080` | — |
| GraphQL Playground | `http://localhost:8080/graphql/playground` | — |
| Kafka UI | `http://localhost:8090` | — |
| Prometheus | `http://localhost:9090` | — |
| Grafana | `http://localhost:3000` | admin / admin |

Prometheus scrapes the API at `:9091` and the worker at `:9092` — separate ports so metrics from each process are labeled independently.

Key metrics:

| Metric | Type | Labels |
|---|---|---|
| `eventmind_events_processed_total` | Counter | `event_type`, `status` |
| `eventmind_actions_taken_total` | Counter | `action` |
| `eventmind_llm_request_duration_seconds` | Histogram | `provider` |
| `eventmind_dlq_events_total` | Counter | `event_type` |
| `eventmind_retry_attempts_total` | Counter | `outcome` |

## Dead letter queue

Failed events go to `events.dlq`. The retry worker polls this topic and retries with exponential backoff: 1s → 2s → 4s → 8s → 16s (capped at 30s). After 5 failures the event is marked `permanently_failed` in Postgres. Tested end-to-end by intentionally triggering LLM auth failures.

## Project layout

```
eventmind/
├── cmd/
│   ├── api/           REST + GraphQL server entry point
│   └── worker/        Kafka consumer + DLQ retry worker entry point
├── internal/
│   ├── agent/         LLM reasoning + tool execution + decision struct
│   ├── events/        event type definitions and payload shapes
│   ├── storage/       postgres client (decisions) + mongo client (raw events)
│   ├── cache/         redis client — idempotency + decision cache
│   ├── messaging/     kafka producer, consumer, DLQ handler
│   └── metrics/       prometheus counter/histogram registrations
├── pkg/
│   ├── llm/           provider-agnostic LLM client (claude/groq/openai/mock)
│   └── logger/        zerolog setup
├── api/
│   ├── graphql/       gqlgen schema, resolvers, generated code
│   └── rest/          gin handlers + middleware (logger, recovery, request-id)
├── infrastructure/
│   └── terraform/     AWS EKS + ECR + VPC (us-east-1)
├── deployments/
│   ├── docker-compose.yml
│   └── k8s/           Kubernetes manifests (Deployment, Service, HPA, Ingress)
└── scripts/
    └── seed.go        generates 50 shuffled sample events across all 5 types
```

## AWS deployment

Terraform in `infrastructure/terraform/` targets EKS on us-east-1. Kafka stays on Confluent Cloud, Postgres on Supabase, Redis on Upstash — no managed databases to provision.

```bash
cd infrastructure/terraform
cp prod.tfvars.example prod.tfvars   # fill in region, instance types, etc.
terraform init
terraform plan  -var-file=prod.tfvars
terraform apply -var-file=prod.tfvars

# Update k8s image references with ECR URLs from terraform output
sed -i "s|<ECR_API_URL>|$(terraform output -raw ecr_api_url)|g"    ../../deployments/k8s/api-deployment.yaml
sed -i "s|<ECR_WORKER_URL>|$(terraform output -raw ecr_worker_url)|g" ../../deployments/k8s/worker-deployment.yaml
```

The GitHub Actions pipeline (`.github/workflows/ci.yml`) is written to build images, push to ECR, and roll out to EKS on every push to `main` — it requires `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` set as GitHub Secrets and an existing S3 bucket (`eventmind-tfstate`) for Terraform state. The pipeline has not been run against a live AWS account — infrastructure is IaC-ready, not provisioned.

## Database schema

**Postgres** (`decisions` + `events` tables, auto-migrated at startup with advisory lock):
- `events` — id, type, payload (JSONB), source, timestamp
- `decisions` — id, event_id, event_type, action, reasoning, llm_prompt, llm_response, success, error, duration_ms, llm_duration_ms, retry_count, status, processed_at

**MongoDB** — raw event documents stored with payload as nested BSON (queryable fields), indexed on `type + timestamp`.
