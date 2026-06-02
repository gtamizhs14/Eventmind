package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/joho/godotenv"

	"github.com/gtamizhs14/eventmind/internal/agent"
	"github.com/gtamizhs14/eventmind/internal/cache"
	"github.com/gtamizhs14/eventmind/internal/events"
	"github.com/gtamizhs14/eventmind/internal/messaging"
	"github.com/gtamizhs14/eventmind/internal/metrics"
	"github.com/gtamizhs14/eventmind/internal/storage"
	"github.com/gtamizhs14/eventmind/pkg/llm"
	"github.com/gtamizhs14/eventmind/pkg/logger"
)

func main() {
	_ = godotenv.Load()
	log := logger.New()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	db, err := storage.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal().Err(err).Msg("postgres init failed")
	}
	defer db.Close()

	mdb, err := storage.NewMongo(ctx, os.Getenv("MONGODB_URI"), os.Getenv("MONGODB_DATABASE"))
	if err != nil {
		log.Fatal().Err(err).Msg("mongo init failed")
	}
	defer mdb.Close(ctx)

	rdb, err := cache.New(os.Getenv("REDIS_URL"))
	if err != nil {
		log.Fatal().Err(err).Msg("redis init failed")
	}
	defer rdb.Close()

	provider, err := llm.New()
	if err != nil {
		log.Fatal().Err(err).Msg("llm init failed")
	}

	m := metrics.New()
	ag := agent.New(provider, m)

	brokers := os.Getenv("KAFKA_BROKERS")
	groupID := os.Getenv("KAFKA_GROUP_ID")
	topic := os.Getenv("KAFKA_TOPIC_EVENTS")
	dlqTopic := os.Getenv("KAFKA_TOPIC_DLQ")

	consumer, err := messaging.NewConsumer(brokers, groupID, topic, dlqTopic, log)
	if err != nil {
		log.Fatal().Err(err).Msg("kafka consumer init failed")
	}
	defer consumer.Close()

	dlqWorker, err := messaging.NewDLQWorker(brokers, groupID, dlqTopic, envInt("RETRY_MAX_ATTEMPTS", 5), envInt("RETRY_BASE_DELAY_MS", 1000), log)
	if err != nil {
		log.Fatal().Err(err).Msg("dlq worker init failed")
	}
	defer dlqWorker.Close()

	dlqWorker.OnPermanentFailure(func(ctx context.Context, raw []byte) {
		var ev events.Event
		if err := json.Unmarshal(raw, &ev); err != nil {
			log.Error().Err(err).Msg("could not parse permanently failed event")
			return
		}
		d := &agent.Decision{
			ID:          uuid.New().String(),
			EventID:     ev.ID,
			EventType:   ev.Type,
			Success:     false,
			Error:       "max retries exceeded",
			Status:      "permanently_failed",
			ProcessedAt: time.Now().UTC(),
		}
		if err := db.SaveDecision(ctx, d); err != nil {
			log.Error().Err(err).Str("event_id", ev.ID).Msg("failed to save permanent failure")
		}
		m.RetryAttempts.WithLabelValues("permanent_failure").Inc()
	})

	handler := func(ctx context.Context, ev *events.Event) error {
		// raw document to Mongo regardless of processing outcome
		if err := mdb.SaveEvent(ctx, ev); err != nil {
			log.Warn().Err(err).Str("event_id", ev.ID).Msg("mongo save failed — non-fatal")
		}

		seen, err := rdb.Seen(ctx, ev.ID)
		if err != nil {
			log.Warn().Err(err).Str("event_id", ev.ID).Msg("idempotency check failed — processing anyway")
		} else if seen {
			log.Debug().Str("event_id", ev.ID).Msg("duplicate — skipping")
			return nil
		}

		d, procErr := ag.Process(ctx, ev)

		if d != nil {
			if saveErr := db.SaveDecision(ctx, d); saveErr != nil {
				log.Error().Err(saveErr).Str("event_id", ev.ID).Msg("failed to persist decision")
			}

			status := "success"
			if !d.Success {
				status = "failure"
			}
			m.EventsProcessed.WithLabelValues(string(ev.Type), status).Inc()
			if d.Success {
				m.ActionsTaken.WithLabelValues(string(d.Action)).Inc()
			}

			log.Info().
				Str("event_id", ev.ID).
				Str("type", string(ev.Type)).
				Str("action", string(d.Action)).
				Bool("success", d.Success).
				Int64("duration_ms", d.DurationMs).
				Int64("llm_ms", d.LLMDurationMs).
				Msg("event processed")
		}

		if procErr != nil {
			m.DLQEvents.WithLabelValues(string(ev.Type)).Inc()
		}
		return procErr
	}

	log.Info().
		Str("brokers", brokers).
		Str("topic", topic).
		Str("llm", ag.ProviderName()).
		Msg("worker started")

	errCh := make(chan error, 2)
	go func() { errCh <- consumer.Run(ctx, handler) }()
	go func() { errCh <- dlqWorker.Run(ctx, handler) }()

	select {
	case err := <-errCh:
		if err != nil {
			log.Error().Err(err).Msg("worker goroutine failed")
		}
		cancel()
	case <-ctx.Done():
	}

	log.Info().Msg("shutdown complete")
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
