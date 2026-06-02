package agent_test

// Integration tests for the full agent → storage pipeline.
// Run with: DATABASE_URL=... REDIS_URL=... go test ./internal/agent/ -run Integration -v

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/gtamizhs14/eventmind/internal/agent"
	"github.com/gtamizhs14/eventmind/internal/cache"
	"github.com/gtamizhs14/eventmind/internal/events"
	"github.com/gtamizhs14/eventmind/internal/storage"
)

// pipelineMock is a local mock LLM for integration tests — no real API call needed.
type pipelineMock struct{}

func (p *pipelineMock) Complete(_ context.Context, _ string) (string, error) {
	return `{"action":"send_notification","reasoning":"integration test"}`, nil
}
func (p *pipelineMock) Name() string { return "mock" }

func IntegrationTestAgentPipeline(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	redisURL := os.Getenv("REDIS_URL")
	if dsn == "" || redisURL == "" {
		t.Skip("DATABASE_URL and REDIS_URL required")
	}

	ctx := context.Background()

	db, err := storage.New(ctx, dsn)
	if err != nil {
		t.Fatalf("postgres: %v", err)
	}
	defer db.Close()

	rdb, err := cache.New(redisURL)
	if err != nil {
		t.Fatalf("redis: %v", err)
	}
	defer rdb.Close()

	ag := agent.New(&pipelineMock{}, nil)

	t.Run("ProcessAndPersist", func(t *testing.T) {
		payload, _ := json.Marshal(map[string]any{
			"order_id":    "ord_inttest_001",
			"customer_id": "cust_inttest",
			"amount":      129.99,
		})
		ev := &events.Event{
			ID:        uuid.New().String(),
			Type:      events.OrderPlaced,
			Payload:   payload,
			Source:    "integration-test",
			Timestamp: time.Now().UTC(),
		}

		if err := db.SaveEvent(ctx, ev); err != nil {
			t.Fatalf("SaveEvent: %v", err)
		}

		d, err := ag.Process(ctx, ev)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if d.Action != agent.ActionSendNotification {
			t.Errorf("action: got %q", d.Action)
		}

		if err := db.SaveDecision(ctx, d); err != nil {
			t.Fatalf("SaveDecision: %v", err)
		}

		got, err := db.GetDecision(ctx, d.ID)
		if err != nil {
			t.Fatalf("GetDecision: %v", err)
		}
		if got.EventID != ev.ID {
			t.Errorf("event_id mismatch: got %q want %q", got.EventID, ev.ID)
		}
	})

	t.Run("IdempotencyViaRedis", func(t *testing.T) {
		id := uuid.New().String()

		seen1, err := rdb.Seen(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if seen1 {
			t.Error("first call: should not be seen")
		}
		seen2, err := rdb.Seen(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if !seen2 {
			t.Error("second call: should be seen now")
		}
	})

	t.Run("DecisionCacheRoundTrip", func(t *testing.T) {
		id := uuid.New().String()
		blob := `{"id":"` + id + `","action":"flag_for_review"}`

		if err := rdb.CacheDecision(ctx, id, blob); err != nil {
			t.Fatalf("CacheDecision: %v", err)
		}
		got, err := rdb.GetDecision(ctx, id)
		if err != nil {
			t.Fatalf("GetDecision from cache: %v", err)
		}
		if got != blob {
			t.Errorf("cache round-trip: got %q want %q", got, blob)
		}
	})
}
