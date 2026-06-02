package messaging

import (
	"errors"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestHeaderInt(t *testing.T) {
	headers := []kafka.Header{
		{Key: "retry-count", Value: []byte("3")},
		{Key: "other", Value: []byte("ignored")},
	}
	if got := headerInt(headers, "retry-count"); got != 3 {
		t.Errorf("got %d want 3", got)
	}
	if got := headerInt(headers, "missing"); got != 0 {
		t.Errorf("missing key should return 0, got %d", got)
	}
}

func TestMergeHeaders(t *testing.T) {
	orig := []kafka.Header{
		{Key: "retry-count", Value: []byte("1")},
		{Key: "original-topic", Value: []byte("events")},
		{Key: "error", Value: []byte("old error")},
	}
	merged := mergeHeaders(orig, 2, errors.New("new error"))

	byKey := map[string]string{}
	for _, h := range merged {
		byKey[h.Key] = string(h.Value)
	}

	if byKey["retry-count"] != "2" {
		t.Errorf("retry-count: got %q want \"2\"", byKey["retry-count"])
	}
	if byKey["error"] != "new error" {
		t.Errorf("error: got %q want \"new error\"", byKey["error"])
	}
	if byKey["original-topic"] != "events" {
		t.Error("original-topic should be preserved")
	}
	if _, ok := byKey["last-retry-at"]; !ok {
		t.Error("last-retry-at should be set")
	}
}

func TestDLQWorker_Backoff(t *testing.T) {
	w := &DLQWorker{baseMs: 1000, maxRetry: 5}

	cases := []struct {
		retry int
		wantMs int
	}{
		{0, 1000},
		{1, 2000},
		{2, 4000},
		{3, 8000},
		{4, 16000},
		{10, 30000}, // capped at 30s
	}
	for _, tc := range cases {
		got := w.backoff(tc.retry)
		if int(got.Milliseconds()) != tc.wantMs {
			t.Errorf("backoff(%d) = %dms, want %dms", tc.retry, got.Milliseconds(), tc.wantMs)
		}
	}
}

func TestTruncate(t *testing.T) {
	if truncate("hello", 10) != "hello" {
		t.Error("short string should not be truncated")
	}
	got := truncate("hello world", 5)
	if got != "hello..." {
		t.Errorf("got %q", got)
	}
}
