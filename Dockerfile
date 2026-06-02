FROM golang:1.22 AS builder

# confluent-kafka-go needs CGo + librdkafka headers
RUN apt-get update && apt-get install -y librdkafka-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# BINARY arg selects which cmd to build: api or worker
ARG BINARY=api
RUN CGO_ENABLED=1 go build -o bin/${BINARY} ./cmd/${BINARY}

# ── Runtime image ─────────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates librdkafka1 && rm -rf /var/lib/apt/lists/*

ARG BINARY=api
COPY --from=builder /app/bin/${BINARY} /app/server

EXPOSE 8080 9091

CMD ["/app/server"]
