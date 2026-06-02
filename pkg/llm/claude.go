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

const defaultClaudeModel = "claude-3-5-sonnet-20241022"

type claudeProvider struct {
	apiKey  string
	model   string
	client  *http.Client
	baseURL string
}

func newClaude(apiKey string) *claudeProvider {
	model := os.Getenv("LLM_MODEL")
	if model == "" {
		model = defaultClaudeModel
	}
	return &claudeProvider{
		apiKey:  apiKey,
		model:   model,
		client:  &http.Client{Timeout: 30 * time.Second},
		baseURL: "https://api.anthropic.com/v1/messages",
	}
}

func (c *claudeProvider) Name() string { return "claude" }

func (c *claudeProvider) Complete(ctx context.Context, prompt string) (string, error) {
	body, _ := json.Marshal(map[string]any{
		"model":      c.model,
		"max_tokens": 1024,
		"messages":   []map[string]string{{"role": "user", "content": prompt}},
	})

	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("x-api-key", c.apiKey)
	req.Header.Set("anthropic-version", "2023-06-01")
	req.Header.Set("content-type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("claude: %w", err)
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("claude: %s: %s", resp.Status, data)
	}

	var out struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return "", fmt.Errorf("claude: parse: %w", err)
	}
	if len(out.Content) == 0 {
		return "", fmt.Errorf("claude: empty response")
	}
	return out.Content[0].Text, nil
}
