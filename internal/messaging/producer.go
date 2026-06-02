package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/gtamizhs14/eventmind/internal/events"
	"github.com/rs/zerolog"
)

// Handler is the function signature both Consumer and DLQWorker expect.
type Handler func(ctx context.Context, ev *events.Event) error

type Producer struct {
	p     *kafka.Producer
	topic string
	log   zerolog.Logger
}

func NewProducer(brokers, topic string, log zerolog.Logger) (*Producer, error) {
	cfg := kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"acks":              "all",
		"retries":           3,
		"retry.backoff.ms":  500,
		"compression.type":  "snappy",
	}
	applyTLS(&cfg)

	p, err := kafka.NewProducer(&cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}

	prod := &Producer{p: p, topic: topic, log: log}

	// drain delivery reports — we care about delivery errors
	go func() {
		for e := range p.Events() {
			m, ok := e.(*kafka.Message)
			if ok && m.TopicPartition.Error != nil {
				log.Error().
					Err(m.TopicPartition.Error).
					Str("topic", *m.TopicPartition.Topic).
					Msg("kafka delivery failed")
			}
		}
	}()

	return prod, nil
}

func (p *Producer) Publish(ctx context.Context, ev *events.Event) error {
	data, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	return p.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Key:            []byte(ev.ID),
		Value:          data,
	}, nil)
}

// publishRaw is used internally to re-queue messages with existing headers intact.
func (p *Producer) publishRaw(topic string, key, value []byte, headers []kafka.Header) error {
	return p.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          value,
		Headers:        headers,
	}, nil)
}

func (p *Producer) Close() {
	p.p.Flush(5000)
	p.p.Close()
}

// applyTLS wires SASL_SSL for Confluent Cloud when KAFKA_USE_TLS=true.
func applyTLS(cfg *kafka.ConfigMap) {
	if os.Getenv("KAFKA_USE_TLS") != "true" {
		return
	}
	cfg.SetKey("security.protocol", "SASL_SSL")
	cfg.SetKey("sasl.mechanisms", "PLAIN")
	cfg.SetKey("sasl.username", os.Getenv("KAFKA_SASL_USERNAME"))
	cfg.SetKey("sasl.password", os.Getenv("KAFKA_SASL_PASSWORD"))
}

// buildKafkaConfig returns a base consumer config, shared between Consumer and DLQWorker.
func buildKafkaConfig(brokers, groupID string) kafka.ConfigMap {
	cfg := kafka.ConfigMap{
		"bootstrap.servers":   brokers,
		"group.id":            groupID,
		"auto.offset.reset":   "earliest",
		"enable.auto.commit":  false,
		"session.timeout.ms":  30000,
		// give the handler plenty of time before Kafka considers this consumer dead
		"max.poll.interval.ms": 300000,
	}
	applyTLS(&cfg)
	return cfg
}

// headerInt reads an integer from a Kafka message header, returns 0 if missing.
func headerInt(headers []kafka.Header, key string) int {
	for _, h := range headers {
		if h.Key == key {
			n := 0
			fmt.Sscanf(string(h.Value), "%d", &n)
			return n
		}
	}
	return 0
}

// mergeHeaders rebuilds a header slice, replacing retry-count and error fields.
func mergeHeaders(headers []kafka.Header, retryCount int, err error) []kafka.Header {
	out := make([]kafka.Header, 0, len(headers)+3)
	for _, h := range headers {
		switch h.Key {
		case "retry-count", "error", "last-retry-at":
			// will be overwritten below
		default:
			out = append(out, h)
		}
	}
	return append(out,
		kafka.Header{Key: "retry-count", Value: []byte(fmt.Sprintf("%d", retryCount))},
		kafka.Header{Key: "error", Value: []byte(err.Error())},
		kafka.Header{Key: "last-retry-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	)
}
