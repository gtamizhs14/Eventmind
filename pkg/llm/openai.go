package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

const (
	openAIBase         = "https://api.openai.com/v1/chat/completions"
	defaultOpenAIModel = "gpt-4o-mini"
)

type openAIProvider struct {
	apiKey  string
	model   string
	client  *http.Client
	baseURL string
}

func newOpenAI(apiKey string) *openAIProvider {
	model := os.Getenv("LLM_MODEL")
	if model == "" {
		model = defaultOpenAIModel
	}
	return &openAIProvider{
		apiKey:  apiKey,
		model:   model,
		client:  &http.Client{Timeout: 30 * time.Second},
		baseURL: openAIBase,
	}
}

func (o *openAIProvider) Name() string { return "openai" }

func (o *openAIProvider) Complete(ctx context.Context, prompt string) (string, error) {
	return chatCompletions(ctx, o.client, o.baseURL, o.apiKey, o.model, prompt)
}

// chatCompletions handles both OpenAI and Groq — same wire format.
func chatCompletions(ctx context.Context, client *http.Client, url, apiKey, model, prompt string) (string, error) {
	body, _ := json.Marshal(map[string]any{
		"model":      model,
		"max_tokens": 1024,
		"messages":   []map[string]string{{"role": "user", "content": prompt}},
	})

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("content-type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("%s: %s", resp.Status, data)
	}

	var out struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return "", fmt.Errorf("parse: %w", err)
	}
	if len(out.Choices) == 0 {
		return "", fmt.Errorf("empty response")
	}
	return out.Choices[0].Message.Content, nil
}
