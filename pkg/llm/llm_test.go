package llm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

// ── factory ───────────────────────────────────────────────────────────────────

func TestNew(t *testing.T) {
	cases := []struct {
		provider string
		wantName string
		wantErr  bool
	}{
		{"claude", "claude", false},
		{"groq", "groq", false},
		{"openai", "openai", false},
		{"mock", "mock", false},
		{"bad", "", true},
	}

	for _, tc := range cases {
		t.Run(tc.provider, func(t *testing.T) {
			t.Setenv("LLM_PROVIDER", tc.provider)
			t.Setenv("LLM_API_KEY", "test-key")

			p, err := New()
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if p.Name() != tc.wantName {
				t.Errorf("got %q want %q", p.Name(), tc.wantName)
			}
		})
	}
}

func TestNew_MissingKey(t *testing.T) {
	os.Unsetenv("LLM_API_KEY")
	t.Setenv("LLM_PROVIDER", "claude")
	_, err := New()
	if err == nil {
		t.Fatal("expected error for missing API key")
	}
}

// ── claude ────────────────────────────────────────────────────────────────────

func TestClaude_Complete(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") == "" {
			t.Error("missing x-api-key")
		}
		if r.Header.Get("anthropic-version") == "" {
			t.Error("missing anthropic-version")
		}
		json.NewEncoder(w).Encode(map[string]any{
			"content": []map[string]any{{"type": "text", "text": "hello from claude"}},
		})
	}))
	defer srv.Close()

	p := newClaude("sk-test")
	p.baseURL = srv.URL

	got, err := p.Complete(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	if got != "hello from claude" {
		t.Errorf("got %q", got)
	}
}

func TestClaude_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(429)
		w.Write([]byte(`{"error":"rate limited"}`))
	}))
	defer srv.Close()

	p := newClaude("sk-test")
	p.baseURL = srv.URL

	_, err := p.Complete(context.Background(), "test")
	if err == nil {
		t.Fatal("expected error on 429")
	}
}

func TestClaude_EmptyContent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"content": []any{}})
	}))
	defer srv.Close()

	p := newClaude("sk-test")
	p.baseURL = srv.URL

	_, err := p.Complete(context.Background(), "test")
	if err == nil {
		t.Fatal("expected error for empty content")
	}
}

// ── openai / groq (shared wire format) ───────────────────────────────────────

func chatCompletionServer(t *testing.T, text string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			t.Error("missing Authorization header")
		}
		json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{
				{"message": map[string]string{"content": text}},
			},
		})
	}))
}

func TestOpenAI_Complete(t *testing.T) {
	srv := chatCompletionServer(t, "hello from openai")
	defer srv.Close()

	p := newOpenAI("sk-test")
	p.baseURL = srv.URL

	got, err := p.Complete(context.Background(), "test")
	if err != nil || got != "hello from openai" {
		t.Errorf("err=%v got=%q", err, got)
	}
}

func TestGroq_Complete(t *testing.T) {
	srv := chatCompletionServer(t, "hello from groq")
	defer srv.Close()

	p := newGroq("gsk-test")
	p.baseURL = srv.URL

	got, err := p.Complete(context.Background(), "test")
	if err != nil || got != "hello from groq" {
		t.Errorf("err=%v got=%q", err, got)
	}
}

func TestChatCompletions_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		w.Write([]byte("internal error"))
	}))
	defer srv.Close()

	p := newOpenAI("sk-test")
	p.baseURL = srv.URL

	_, err := p.Complete(context.Background(), "test")
	if err == nil {
		t.Fatal("expected error on 500")
	}
}

func TestChatCompletions_EmptyChoices(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"choices": []any{}})
	}))
	defer srv.Close()

	p := newOpenAI("sk-test")
	p.baseURL = srv.URL

	_, err := p.Complete(context.Background(), "test")
	if err == nil {
		t.Fatal("expected error for empty choices")
	}
}

// ── mock ──────────────────────────────────────────────────────────────────────

func TestMock_Complete(t *testing.T) {
	p := newMock()
	got, err := p.Complete(context.Background(), "anything")
	if err != nil {
		t.Fatal(err)
	}
	// must be valid JSON with action + reasoning
	var resp struct {
		Action    string `json:"action"`
		Reasoning string `json:"reasoning"`
	}
	if err := json.Unmarshal([]byte(got), &resp); err != nil {
		t.Fatalf("mock response isn't valid agent JSON: %v", err)
	}
	if resp.Action == "" {
		t.Error("mock response missing action")
	}
}
