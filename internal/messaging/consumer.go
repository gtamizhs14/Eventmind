package messaging

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gtamizhs14/eventmind/internal/events"
)

type Consumer struct {
	c     *kafka.Consumer
	topic string
}

// NewConsumer creates a Kafka consumer in the given group.
// Implemented in step 3.
func NewConsumer(brokers, groupID, topic string) (*Consumer, error) {
	panic("not implemented — see step 3")
}

// Run starts the consume loop, calling handler for each event.
// Blocks until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context, handler func(context.Context, *events.Event) error) error {
	panic("not implemented — see step 3")
}

func (c *Consumer) Close() {
	c.c.Close()
}
