package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gtamizhs14/eventmind/internal/events"
)

// Run with: MONGODB_URI=mongodb://localhost:27017 go test ./internal/storage/ -run Integration -v

func IntegrationTestMongoStore(t *testing.T) {
	uri := os.Getenv("MONGODB_URI")
	if uri == "" {
		t.Skip("MONGODB_URI not set")
	}

	ctx := context.Background()
	store, err := NewMongo(ctx, uri, "eventmind_test")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close(ctx)

	// clean up test collection after run
	defer store.coll.Drop(ctx)

	t.Run("SaveAndGetEvent", func(t *testing.T) {
		ev := mongoTestEvent(t, events.OrderPlaced)
		if err := store.SaveEvent(ctx, ev); err != nil {
			t.Fatalf("SaveEvent: %v", err)
		}

		doc, err := store.GetEvent(ctx, ev.ID)
		if err != nil {
			t.Fatalf("GetEvent: %v", err)
		}
		if doc == nil {
			t.Fatal("expected document, got nil")
		}
		if doc["type"] != string(events.OrderPlaced) {
			t.Errorf("type: got %v", doc["type"])
		}
		// payload should be stored as a nested object, not a string
		if _, ok := doc["payload"].(map[string]any); !ok {
			t.Errorf("payload should be a document, got %T", doc["payload"])
		}
	})

	t.Run("Upsert_Idempotent", func(t *testing.T) {
		ev := mongoTestEvent(t, events.UserSignup)
		if err := store.SaveEvent(ctx, ev); err != nil {
			t.Fatal(err)
		}
		// second save should not error (upsert)
		if err := store.SaveEvent(ctx, ev); err != nil {
			t.Fatalf("second SaveEvent: %v", err)
		}
	})

	t.Run("GetEvent_NotFound", func(t *testing.T) {
		doc, err := store.GetEvent(ctx, "does-not-exist")
		if err != nil {
			t.Fatal(err)
		}
		if doc != nil {
			t.Error("expected nil for missing document")
		}
	})

	t.Run("ListEvents", func(t *testing.T) {
		// insert events of two types
		for i := 0; i < 3; i++ {
			_ = store.SaveEvent(ctx, mongoTestEvent(t, events.OrderPlaced))
			_ = store.SaveEvent(ctx, mongoTestEvent(t, events.PaymentFailed))
		}

		all, err := store.ListEvents(ctx, 20, 0, "")
		if err != nil {
			t.Fatal(err)
		}
		if len(all) == 0 {
			t.Error("expected some events")
		}

		filtered, err := store.ListEvents(ctx, 20, 0, string(events.PaymentFailed))
		if err != nil {
			t.Fatal(err)
		}
		for _, doc := range filtered {
			if doc["type"] != string(events.PaymentFailed) {
				t.Errorf("filter returned wrong type: %v", doc["type"])
			}
		}
	})

	t.Run("CountByType", func(t *testing.T) {
		counts, err := store.CountByType(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(counts) == 0 {
			t.Error("expected counts")
		}
		for typ, count := range counts {
			if count <= 0 {
				t.Errorf("type %s has count %d", typ, count)
			}
		}
	})
}

func mongoTestEvent(t *testing.T, typ events.Type) *events.Event {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{
		"order_id":    fmt.Sprintf("ord-%d", time.Now().UnixNano()),
		"customer_id": "cust-test",
		"amount":      79.99,
	})
	return &events.Event{
		ID:        fmt.Sprintf("evt-mongo-%d", time.Now().UnixNano()),
		Type:      typ,
		Payload:   payload,
		Source:    "test",
		Timestamp: time.Now().UTC(),
	}
}
