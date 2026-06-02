package llm

import (
	"context"
	"net/http"
	"os"
	"time"
)

const (
	groqBase         = "https://api.groq.com/openai/v1/chat/completions"
	defaultGroqModel = "llama-3.3-70b-versatile"
)

type groqProvider struct {
	apiKey  string
	model   string
	client  *http.Client
	baseURL string
}

func newGroq(apiKey string) *groqProvider {
	model := os.Getenv("LLM_MODEL")
	if model == "" {
		model = defaultGroqModel
	}
	return &groqProvider{
		apiKey:  apiKey,
		model:   model,
		client:  &http.Client{Timeout: 30 * time.Second},
		baseURL: groqBase,
	}
}

func (g *groqProvider) Name() string { return "groq" }

func (g *groqProvider) Complete(ctx context.Context, prompt string) (string, error) {
	return chatCompletions(ctx, g.client, g.baseURL, g.apiKey, g.model, prompt)
}
