package agent

// Integration tests for the agent → storage pipeline.
// Run with: DATABASE_URL=... REDIS_URL=... go test ./internal/agent/ -run Integration -v

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gtamizhs14/eventmind/internal/cache"
	"github.com/gtamizhs14/eventmind/internal/events"
	"github.com/gtamizhs14/eventmind/internal/storage"
)

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

	// use mock LLM so tests don't need a real API key
	ag := New(&testProvider{resp: `{"action":"send_notification","reasoning":"integration test"}`}, nil)

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

		// save event to postgres first (simulating what the API does)
		if err := db.SaveEvent(ctx, ev); err != nil {
			t.Fatalf("SaveEvent: %v", err)
		}

		d, err := ag.Process(ctx, ev)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if d == nil {
			t.Fatal("expected decision")
		}
		if d.Action != ActionSendNotification {
			t.Errorf("action: got %q", d.Action)
		}

		if err := db.SaveDecision(ctx, d); err != nil {
			t.Fatalf("SaveDecision: %v", err)
		}

		// verify it was persisted
		got, err := db.GetDecision(ctx, d.ID)
		if err != nil {
			t.Fatalf("GetDecision: %v", err)
		}
		if got.EventID != ev.ID {
			t.Errorf("event_id mismatch: got %q want %q", got.EventID, ev.ID)
		}
		if got.Action != d.Action {
			t.Errorf("action mismatch")
		}
	})

	t.Run("IdempotencyViaRedis", func(t *testing.T) {
		ev := &events.Event{
			ID:        uuid.New().String(),
			Type:      events.UserSignup,
			Payload:   []byte(`{"user_id":"u1","email":"test@test.com","plan":"free","source":"test"}`),
			Timestamp: time.Now().UTC(),
		}

		seen1, err := rdb.Seen(ctx, ev.ID)
		if err != nil {
			t.Fatal(err)
		}
		if seen1 {
			t.Error("first call: should not be seen yet")
		}

		seen2, err := rdb.Seen(ctx, ev.ID)
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
			t.Errorf("cache round-trip failed: got %q want %q", got, blob)
		}
	})
}
