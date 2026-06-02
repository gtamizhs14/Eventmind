package llm

import (
	"context"
	"fmt"
	"os"
)

// Provider is the only interface the rest of the system talks to.
// Switch providers by changing LLM_PROVIDER + LLM_API_KEY env vars.
type Provider interface {
	Complete(ctx context.Context, prompt string) (string, error)
	Name() string
}

// New builds a provider from env vars. Defaults to claude.
func New() (Provider, error) {
	p := os.Getenv("LLM_PROVIDER")
	if p == "" {
		p = "claude"
	}
	key := os.Getenv("LLM_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("LLM_API_KEY not set")
	}
	switch p {
	case "claude":
		return newClaude(key), nil
	case "groq":
		return newGroq(key), nil
	case "openai":
		return newOpenAI(key), nil
	case "mock":
		return newMock(), nil
	default:
		return nil, fmt.Errorf("unknown LLM_PROVIDER %q — valid: claude, groq, openai", p)
	}
}
