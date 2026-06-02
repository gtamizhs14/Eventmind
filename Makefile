.PHONY: build test test-int run-api run-worker seed docker-up docker-down docker-logs lint gen clean

BINARY_API    := bin/api
BINARY_WORKER := bin/worker
COMPOSE       := docker-compose -f deployments/docker-compose.yml

build:
	go build -o $(BINARY_API) ./cmd/api
	go build -o $(BINARY_WORKER) ./cmd/worker

test:
	go test ./... -v -short -count=1 -race

test-int:
	go test ./... -v -run Integration -count=1

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker

seed:
	go run ./scripts/seed.go

docker-up:
	$(COMPOSE) up -d
	@echo "Services starting — Kafka UI at http://localhost:8090, Grafana at http://localhost:3000"

docker-down:
	$(COMPOSE) down

docker-logs:
	$(COMPOSE) logs -f

lint:
	golangci-lint run ./...

gen:
	go run github.com/99designs/gqlgen generate

clean:
	rm -rf bin/

.DEFAULT_GOAL := build
