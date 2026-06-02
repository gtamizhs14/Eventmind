package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gtamizhs14/eventmind/internal/agent"
	"github.com/gtamizhs14/eventmind/internal/events"
)

// These tests hit a real database. Run with:
//   DATABASE_URL=postgres://... go test ./internal/storage/ -run Integration -v

func IntegrationTestPGStore(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set")
	}

	ctx := context.Background()
	store, err := New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close()

	t.Run("SaveAndGetEvent", func(t *testing.T) {
		ev := testEvent(t)
		if err := store.SaveEvent(ctx, ev); err != nil {
			t.Fatalf("SaveEvent: %v", err)
		}
		// idempotent — second insert should not error
		if err := store.SaveEvent(ctx, ev); err != nil {
			t.Fatalf("SaveEvent (duplicate): %v", err)
		}

		rows, err := store.ListEvents(ctx, 10, 0)
		if err != nil {
			t.Fatalf("ListEvents: %v", err)
		}
		found := false
		for _, r := range rows {
			if r.ID == ev.ID {
				found = true
				break
			}
		}
		if !found {
			t.Error("saved event not found in listing")
		}
	})

	t.Run("SaveAndGetDecision", func(t *testing.T) {
		ev := testEvent(t)
		if err := store.SaveEvent(ctx, ev); err != nil {
			t.Fatalf("setup event: %v", err)
		}

		dec := testDecision(t, ev.ID, events.Type(ev.Type))
		if err := store.SaveDecision(ctx, dec); err != nil {
			t.Fatalf("SaveDecision: %v", err)
		}

		got, err := store.GetDecision(ctx, dec.ID)
		if err != nil {
			t.Fatalf("GetDecision: %v", err)
		}
		if got.Action != dec.Action {
			t.Errorf("action: got %q want %q", got.Action, dec.Action)
		}
		if got.Reasoning != dec.Reasoning {
			t.Errorf("reasoning mismatch")
		}
		if got.DurationMs != dec.DurationMs {
			t.Errorf("duration: got %d want %d", got.DurationMs, dec.DurationMs)
		}
	})

	t.Run("ListDecisionsFilter", func(t *testing.T) {
		ev := testEvent(t)
		_ = store.SaveEvent(ctx, ev)
		dec := testDecision(t, ev.ID, ev.Type)
		_ = store.SaveDecision(ctx, dec)

		all, err := store.ListDecisions(ctx, 50, 0, "")
		if err != nil {
			t.Fatal(err)
		}
		filtered, err := store.ListDecisions(ctx, 50, 0, string(ev.Type))
		if err != nil {
			t.Fatal(err)
		}
		if len(filtered) > len(all) {
			t.Error("filtered set larger than unfiltered — shouldn't happen")
		}
	})

	t.Run("SaveDecisionWithError", func(t *testing.T) {
		ev := testEvent(t)
		_ = store.SaveEvent(ctx, ev)

		dec := testDecision(t, ev.ID, ev.Type)
		dec.Success = false
		dec.Error = "llm rate limit exceeded"
		dec.Status = "failed"

		if err := store.SaveDecision(ctx, dec); err != nil {
			t.Fatalf("SaveDecision with error: %v", err)
		}
		got, err := store.GetDecision(ctx, dec.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Error != dec.Error {
			t.Errorf("error field: got %q want %q", got.Error, dec.Error)
		}
	})
}

func testEvent(t *testing.T) *events.Event {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{
		"order_id":    fmt.Sprintf("ord-%d", time.Now().UnixNano()),
		"customer_id": "cust-test",
		"amount":      49.99,
	})
	return &events.Event{
		ID:        fmt.Sprintf("evt-%d", time.Now().UnixNano()),
		Type:      events.OrderPlaced,
		Payload:   payload,
		Source:    "test",
		Timestamp: time.Now().UTC(),
	}
}

func testDecision(t *testing.T, eventID string, eventType events.Type) *agent.Decision {
	t.Helper()
	return &agent.Decision{
		ID:          fmt.Sprintf("dec-%d", time.Now().UnixNano()),
		EventID:     eventID,
		EventType:   eventType,
		Action:      agent.ActionSendNotification,
		Reasoning:   "test reasoning: order placed successfully",
		LLMPrompt:   "process this order event",
		LLMResponse: `{"action":"send_notification","reasoning":"test"}`,
		Success:     true,
		DurationMs:  142,
		RetryCount:  0,
		Status:      "completed",
		ProcessedAt: time.Now().UTC(),
	}
}
