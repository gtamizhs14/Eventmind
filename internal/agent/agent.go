package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/gtamizhs14/eventmind/internal/events"
	"github.com/gtamizhs14/eventmind/internal/metrics"
	"github.com/gtamizhs14/eventmind/pkg/llm"
)

type Decision struct {
	ID            string
	EventID       string
	EventType     events.Type
	Action        Action
	Reasoning     string
	LLMPrompt     string
	LLMResponse   string
	Success       bool
	Error         string
	DurationMs    int64
	LLMDurationMs int64
	RetryCount    int
	Status        string // completed | failed | permanently_failed
	ProcessedAt   time.Time
}

type Agent struct {
	llm llm.Provider
	m   *metrics.Metrics
}

func New(provider llm.Provider, m *metrics.Metrics) *Agent {
	return &Agent{llm: provider, m: m}
}

func (a *Agent) ProviderName() string { return a.llm.Name() }

// Process sends the event to the LLM, parses the response, executes the action,
// and returns a Decision record ready to be persisted.
func (a *Agent) Process(ctx context.Context, ev *events.Event) (*Decision, error) {
	start := time.Now()
	prompt := buildPrompt(ev)

	llmStart := time.Now()
	resp, err := a.llm.Complete(ctx, prompt)
	llmMs := time.Since(llmStart).Milliseconds()

	if a.m != nil {
		a.m.LLMDuration.WithLabelValues(a.llm.Name()).Observe(float64(llmMs) / 1000)
	}

	if err != nil {
		return a.failedDecision(ev, prompt, "", err.Error(), start, llmMs), fmt.Errorf("llm: %w", err)
	}

	var parsed struct {
		Action    string `json:"action"`
		Reasoning string `json:"reasoning"`
	}
	if err := json.Unmarshal([]byte(extractJSON(resp)), &parsed); err != nil {
		msg := fmt.Sprintf("LLM returned non-JSON: %s", truncateStr(resp, 200))
		return a.failedDecision(ev, prompt, resp, msg, start, llmMs), fmt.Errorf("parse LLM response: %w", err)
	}

	action := Action(parsed.Action)
	if !action.Valid() {
		msg := fmt.Sprintf("LLM returned unknown action %q", parsed.Action)
		return a.failedDecision(ev, prompt, resp, msg, start, llmMs), fmt.Errorf("%s", msg)
	}

	result := executeAction(action, ev.Payload)

	d := &Decision{
		ID:            uuid.New().String(),
		EventID:       ev.ID,
		EventType:     ev.Type,
		Action:        action,
		Reasoning:     parsed.Reasoning,
		LLMPrompt:     prompt,
		LLMResponse:   resp,
		Success:       result.Success,
		DurationMs:    time.Since(start).Milliseconds(),
		LLMDurationMs: llmMs,
		Status:        "completed",
		ProcessedAt:   time.Now().UTC(),
	}
	if !result.Success {
		d.Status = "failed"
		d.Error = result.Details
	}
	return d, nil
}

func (a *Agent) failedDecision(ev *events.Event, prompt, llmResp, errMsg string, start time.Time, llmMs int64) *Decision {
	return &Decision{
		ID:            uuid.New().String(),
		EventID:       ev.ID,
		EventType:     ev.Type,
		LLMPrompt:     prompt,
		LLMResponse:   llmResp,
		Success:       false,
		Error:         errMsg,
		DurationMs:    time.Since(start).Milliseconds(),
		LLMDurationMs: llmMs,
		Status:        "failed",
		ProcessedAt:   time.Now().UTC(),
	}
}

func buildPrompt(ev *events.Event) string {
	return fmt.Sprintf(`You are an AI agent processing business events. Analyze the event and decide the best action.

Event type: %s
Event payload: %s

Available actions:
- send_notification     notify the customer about this event
- escalate_ticket       escalate a support issue to a human agent
- flag_for_review       flag the event for manual review
- update_inventory      trigger an inventory adjustment or reorder
- send_welcome_sequence start the onboarding email sequence for new users

Reply with valid JSON only — no markdown, no extra text:
{"action": "<one of the actions above>", "reasoning": "<one sentence why>"}`,
		ev.Type, string(ev.Payload))
}

// extractJSON pulls the JSON object out of the LLM response.
// LLMs sometimes wrap output in markdown code blocks — this strips that.
func extractJSON(s string) string {
	s = strings.TrimSpace(s)
	i := strings.Index(s, "{")
	j := strings.LastIndex(s, "}")
	if i >= 0 && j > i {
		return s[i : j+1]
	}
	return s
}

func truncateStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
