package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog"

	"github.com/gtamizhs14/eventmind/internal/events"
)

type Consumer struct {
	c        *kafka.Consumer
	dlq      *Producer
	dlqTopic string
	log      zerolog.Logger
}

func NewConsumer(brokers, groupID, topic, dlqTopic string, log zerolog.Logger) (*Consumer, error) {
	cfg := buildKafkaConfig(brokers, groupID)

	c, err := kafka.NewConsumer(&cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	if err := c.Subscribe(topic, nil); err != nil {
		c.Close()
		return nil, fmt.Errorf("subscribe %s: %w", topic, err)
	}

	dlqProd, err := NewProducer(brokers, dlqTopic, log)
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("dlq producer: %w", err)
	}

	return &Consumer{c: c, dlq: dlqProd, dlqTopic: dlqTopic, log: log}, nil
}

// Run is the main consume loop. Blocks until ctx is cancelled.
// On handler failure: sends to DLQ, then commits. We never lose the message.
func (c *Consumer) Run(ctx context.Context, handler Handler) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		msg, err := c.c.ReadMessage(500 * time.Millisecond)
		if err != nil {
			if isTimeout(err) {
				continue
			}
			c.log.Error().Err(err).Msg("kafka read error")
			continue
		}

		var ev events.Event
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			c.log.Error().Err(err).Str("raw", truncate(string(msg.Value), 200)).Msg("unparseable event — skipping")
			c.commit(msg)
			continue
		}

		c.log.Debug().Str("id", ev.ID).Str("type", string(ev.Type)).Msg("processing event")

		if err := handler(ctx, &ev); err != nil {
			c.log.Error().Err(err).Str("event_id", ev.ID).Msg("handler failed — sending to DLQ")
			if dlqErr := c.toDLQ(msg, err); dlqErr != nil {
				c.log.Error().Err(dlqErr).Msg("DLQ write failed")
			}
		}

		c.commit(msg)
	}
}

func (c *Consumer) toDLQ(msg *kafka.Message, handlerErr error) error {
	headers := []kafka.Header{
		{Key: "retry-count", Value: []byte("0")},
		{Key: "error", Value: []byte(handlerErr.Error())},
		{Key: "original-topic", Value: []byte(*msg.TopicPartition.Topic)},
		{Key: "failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	}
	return c.dlq.publishRaw(c.dlqTopic, msg.Key, msg.Value, headers)
}

func (c *Consumer) commit(msg *kafka.Message) {
	if _, err := c.c.CommitMessage(msg); err != nil {
		c.log.Warn().Err(err).Msg("offset commit failed")
	}
}

func (c *Consumer) Close() {
	c.dlq.Close()
	c.c.Close()
}

func isTimeout(err error) bool {
	kerr, ok := err.(kafka.Error)
	return ok && kerr.Code() == kafka.ErrTimedOut
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
