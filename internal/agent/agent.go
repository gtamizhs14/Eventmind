package agent

import (
	"context"
	"time"

	"github.com/gtamizhs14/eventmind/internal/events"
	"github.com/gtamizhs14/eventmind/pkg/llm"
)

// Decision is the full record of what the agent decided for one event.
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
	ProcessedAt time.Time
}

// Agent processes events using an LLM and executes the decided action.
// Implemented in step 4.
type Agent struct {
	llm llm.Provider
	// storage, cache, metrics wired in step 4
}

func New(provider llm.Provider) *Agent {
	return &Agent{llm: provider}
}

// Process reasons about an event and executes the appropriate action.
// Returns the decision record that should be persisted.
func (a *Agent) Process(ctx context.Context, ev *events.Event) (*Decision, error) {
	panic("not implemented — see step 4")
}
