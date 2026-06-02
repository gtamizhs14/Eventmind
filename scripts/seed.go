package main

import (
	"context"
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

// Seed script — generates 50 sample business events across all types.
// Usage: go run ./scripts/seed.go
// Implemented in step 12.

func main() {
	_ = godotenv.Load()
	ctx := context.Background()

	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9094"
	}

	fmt.Printf("seeding events to Kafka at %s\n", brokers)

	// TODO: implement in step 12
	_ = ctx
	panic("not implemented — see step 12")
}
