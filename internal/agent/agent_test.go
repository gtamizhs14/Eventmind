package agent

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/gtamizhs14/eventmind/internal/events"
)

// testProvider lets each test control exactly what the LLM returns.
type testProvider struct {
	resp string
	err  error
}

func (t *testProvider) Complete(_ context.Context, _ string) (string, error) {
	return t.resp, t.err
}
func (t *testProvider) Name() string { return "test" }

func sampleEvent(typ events.Type) *events.Event {
	payload, _ := json.Marshal(map[string]any{
		"order_id":    "ord_001",
		"customer_id": "cust_001",
		"amount":      49.99,
	})
	return &events.Event{
		ID:        "evt_001",
		Type:      typ,
		Payload:   payload,
		Timestamp: time.Now(),
	}
}

func TestAgent_HappyPath(t *testing.T) {
	ag := New(&testProvider{resp: `{"action":"send_notification","reasoning":"order confirmed"}`}, nil)
	d, err := ag.Process(context.Background(), sampleEvent(events.OrderPlaced))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.Action != ActionSendNotification {
		t.Errorf("action: got %q want %q", d.Action, ActionSendNotification)
	}
	if d.Reasoning != "order confirmed" {
		t.Errorf("reasoning: got %q", d.Reasoning)
	}
	if !d.Success {
		t.Error("expected success=true")
	}
	if d.Status != "completed" {
		t.Errorf("status: got %q want completed", d.Status)
	}
	if d.ID == "" {
		t.Error("decision ID should be set")
	}
}

// LLMs sometimes wrap JSON in markdown code blocks — extractJSON should handle it.
func TestAgent_MarkdownWrappedJSON(t *testing.T) {
	resp := "```json\n{\"action\":\"flag_for_review\",\"reasoning\":\"suspicious payment\"}\n```"
	ag := New(&testProvider{resp: resp}, nil)
	d, err := ag.Process(context.Background(), sampleEvent(events.PaymentFailed))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.Action != ActionFlagForReview {
		t.Errorf("action: got %q", d.Action)
	}
}

func TestAgent_InvalidAction(t *testing.T) {
	ag := New(&testProvider{resp: `{"action":"send_money","reasoning":"bad"}`}, nil)
	d, err := ag.Process(context.Background(), sampleEvent(events.OrderPlaced))
	if err == nil {
		t.Fatal("expected error for invalid action")
	}
	if d == nil {
		t.Fatal("decision should be set even on failure")
	}
	if d.Success {
		t.Error("expected success=false")
	}
	if d.Status != "failed" {
		t.Errorf("status: got %q want failed", d.Status)
	}
}

func TestAgent_LLMError(t *testing.T) {
	ag := New(&testProvider{err: errors.New("rate limited")}, nil)
	d, err := ag.Process(context.Background(), sampleEvent(events.UserSignup))
	if err == nil {
		t.Fatal("expected error")
	}
	if d == nil {
		t.Fatal("should still get a decision record")
	}
	if d.Error == "" {
		t.Error("error field should be populated")
	}
}

func TestAgent_BadJSON(t *testing.T) {
	ag := New(&testProvider{resp: "not json at all"}, nil)
	_, err := ag.Process(context.Background(), sampleEvent(events.SupportTicketCreated))
	if err == nil {
		t.Fatal("expected parse error")
	}
}

// ── unit tests for helpers ────────────────────────────────────────────────────

func TestExtractJSON(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{`{"action":"x"}`, `{"action":"x"}`},
		{"```json\n{\"action\":\"x\"}\n```", `{"action":"x"}`},
		{"here is the json: {\"action\":\"x\"} done", `{"action":"x"}`},
		{"no json here", "no json here"},
	}
	for _, tc := range cases {
		if got := extractJSON(tc.in); got != tc.want {
			t.Errorf("extractJSON(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestActionValid(t *testing.T) {
	valid := []Action{
		ActionSendNotification, ActionEscalateTicket,
		ActionFlagForReview, ActionUpdateInventory, ActionSendWelcomeSeq,
	}
	for _, a := range valid {
		if !a.Valid() {
			t.Errorf("%q should be valid", a)
		}
	}
	if Action("make_money").Valid() {
		t.Error("made-up action should not be valid")
	}
}

func TestBuildPrompt(t *testing.T) {
	ev := sampleEvent(events.InventoryLow)
	prompt := buildPrompt(ev)
	if prompt == "" {
		t.Fatal("prompt should not be empty")
	}
	// prompt should include the event type and available actions
	for _, substr := range []string{string(ev.Type), "send_notification", "escalate_ticket"} {
		if !contains(prompt, substr) {
			t.Errorf("prompt missing %q", substr)
		}
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsStr(s, sub))
}

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
