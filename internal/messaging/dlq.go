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

type DLQWorker struct {
	consumer *kafka.Consumer
	producer *Producer
	dlqTopic string
	maxRetry int
	baseMs   int
	log      zerolog.Logger
}

func NewDLQWorker(brokers, groupID, dlqTopic string, maxRetry, baseMs int, log zerolog.Logger) (*DLQWorker, error) {
	// separate consumer group so DLQ offsets are tracked independently
	cfg := buildKafkaConfig(brokers, groupID+"-dlq-retry")

	c, err := kafka.NewConsumer(&cfg)
	if err != nil {
		return nil, fmt.Errorf("dlq consumer: %w", err)
	}
	if err := c.Subscribe(dlqTopic, nil); err != nil {
		c.Close()
		return nil, fmt.Errorf("dlq subscribe: %w", err)
	}

	prod, err := NewProducer(brokers, dlqTopic, log)
	if err != nil {
		c.Close()
		return nil, err
	}

	return &DLQWorker{
		consumer: c,
		producer: prod,
		dlqTopic: dlqTopic,
		maxRetry: maxRetry,
		baseMs:   baseMs,
		log:      log,
	}, nil
}

// Run processes the dead letter queue with exponential backoff.
// Backoff schedule (default baseMs=1000): 1s, 2s, 4s, 8s, 16s.
// After maxRetry attempts the event is permanently failed — caller should
// update the decision record in Postgres accordingly.
func (w *DLQWorker) Run(ctx context.Context, handler Handler) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		msg, err := w.consumer.ReadMessage(500 * time.Millisecond)
		if err != nil {
			if isTimeout(err) {
				continue
			}
			w.log.Error().Err(err).Msg("dlq read error")
			continue
		}

		retryCount := headerInt(msg.Headers, "retry-count")

		if retryCount >= w.maxRetry {
			w.log.Warn().
				Str("event_key", string(msg.Key)).
				Int("retry_count", retryCount).
				Msg("max retries exceeded — marking permanently failed")
			// TODO step 4: agent.MarkPermanentlyFailed(ctx, eventID)
			w.commit(msg)
			continue
		}

		delay := w.backoff(retryCount)
		w.log.Info().
			Str("event_key", string(msg.Key)).
			Int("attempt", retryCount+1).
			Int("max", w.maxRetry).
			Dur("backoff", delay).
			Msg("retrying event")

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}

		var ev events.Event
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			w.log.Error().Err(err).Msg("dlq parse failed — discarding")
			w.commit(msg)
			continue
		}

		if err := handler(ctx, &ev); err != nil {
			w.log.Warn().
				Err(err).
				Str("event_id", ev.ID).
				Int("next_attempt", retryCount+2).
				Msg("retry failed — re-queuing")

			headers := mergeHeaders(msg.Headers, retryCount+1, err)
			if pubErr := w.producer.publishRaw(w.dlqTopic, msg.Key, msg.Value, headers); pubErr != nil {
				w.log.Error().Err(pubErr).Msg("failed to re-queue to DLQ")
			}
		} else {
			w.log.Info().Str("event_id", ev.ID).Int("attempt", retryCount+1).Msg("retry succeeded")
		}

		w.commit(msg)
	}
}

// backoff returns baseMs * 2^retryCount, capped at 30s to avoid extreme delays.
func (w *DLQWorker) backoff(retryCount int) time.Duration {
	ms := w.baseMs * (1 << retryCount)
	if ms > 30000 {
		ms = 30000
	}
	return time.Duration(ms) * time.Millisecond
}

func (w *DLQWorker) commit(msg *kafka.Message) {
	if _, err := w.consumer.CommitMessage(msg); err != nil {
		w.log.Warn().Err(err).Msg("dlq commit failed")
	}
}

func (w *DLQWorker) Close() {
	w.producer.Close()
	w.consumer.Close()
}
