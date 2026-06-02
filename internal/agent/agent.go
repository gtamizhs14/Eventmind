package agent

import (
	"context"
	"time"

	"github.com/gtamizhs14/eventmind/internal/events"
	"github.com/gtamizhs14/eventmind/pkg/llm"
)

// Decision is the full record written to Postgres after the agent processes an event.
type Decision struct {
	ID          string
	EventID     string
	EventType   events.Type
	Action      Action
	Reasoning   string
	LLMPrompt   string
	LLMResponse string
	Success     bool
	Error       string
	DurationMs  int64
	RetryCount  int
	Status      string // completed | failed | permanently_failed
	ProcessedAt time.Time
}

type Agent struct {
	llm llm.Provider
}

func New(provider llm.Provider) *Agent {
	return &Agent{llm: provider}
}

// Process reasons about an event and returns the decision.
// The caller (worker) handles persistence — agent is pure reasoning + action.
// Implemented in step 4.
func (a *Agent) Process(ctx context.Context, ev *events.Event) (*Decision, error) {
	panic("not implemented — see step 4")
}
