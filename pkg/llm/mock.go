package llm

import "context"

// mockProvider returns a fixed valid agent response.
// Used in CI via LLM_PROVIDER=mock — avoids real API calls.
type mockProvider struct{}

func newMock() *mockProvider { return &mockProvider{} }

func (m *mockProvider) Name() string { return "mock" }

func (m *mockProvider) Complete(_ context.Context, _ string) (string, error) {
	return `{"action":"send_notification","reasoning":"mock provider"}`, nil
}
