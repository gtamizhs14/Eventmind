package messaging

import (
	"context"

	"github.com/gtamizhs14/eventmind/internal/events"
)

// DLQWorker consumes from the dead letter topic and retries with backoff.
// Implemented in step 3.
type DLQWorker struct {
	consumer *Consumer
	producer *Producer
	dlqTopic string
	maxRetry int
	baseMs   int
}

func NewDLQWorker(consumer *Consumer, producer *Producer, dlqTopic string, maxRetry, baseMs int) *DLQWorker {
	return &DLQWorker{
		consumer: consumer,
		producer: producer,
		dlqTopic: dlqTopic,
		maxRetry: maxRetry,
		baseMs:   baseMs,
	}
}

// Run starts the DLQ retry loop.
func (w *DLQWorker) Run(ctx context.Context, handler func(context.Context, *events.Event) error) error {
	panic("not implemented — see step 3")
}
