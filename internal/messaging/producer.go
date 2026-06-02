package messaging

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gtamizhs14/eventmind/internal/events"
)

type Producer struct {
	p     *kafka.Producer
	topic string
}

// NewProducer creates a Kafka producer.
// Implemented in step 3.
func NewProducer(brokers, topic string) (*Producer, error) {
	panic("not implemented — see step 3")
}

// Publish encodes the event as JSON and sends it to the events topic.
func (p *Producer) Publish(ctx context.Context, ev *events.Event) error {
	panic("not implemented — see step 3")
}

func (p *Producer) Close() {
	p.p.Close()
}
