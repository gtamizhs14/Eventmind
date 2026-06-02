package main

import (
	"context"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/joho/godotenv"

	"github.com/gtamizhs14/eventmind/internal/cache"
	"github.com/gtamizhs14/eventmind/internal/events"
	"github.com/gtamizhs14/eventmind/internal/messaging"
	"github.com/gtamizhs14/eventmind/internal/storage"
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

	rdb, err := cache.New(os.Getenv("REDIS_URL"))
	if err != nil {
		log.Fatal().Err(err).Msg("redis init failed")
	}
	defer rdb.Close()

	brokers := os.Getenv("KAFKA_BROKERS")
	groupID := os.Getenv("KAFKA_GROUP_ID")
	topic := os.Getenv("KAFKA_TOPIC_EVENTS")
	dlqTopic := os.Getenv("KAFKA_TOPIC_DLQ")

	consumer, err := messaging.NewConsumer(brokers, groupID, topic, dlqTopic, log)
	if err != nil {
		log.Fatal().Err(err).Msg("kafka consumer init failed")
	}
	defer consumer.Close()

	maxRetry, _ := strconv.Atoi(os.Getenv("RETRY_MAX_ATTEMPTS"))
	if maxRetry == 0 {
		maxRetry = 5
	}
	baseMs, _ := strconv.Atoi(os.Getenv("RETRY_BASE_DELAY_MS"))
	if baseMs == 0 {
		baseMs = 1000
	}

	dlqWorker, err := messaging.NewDLQWorker(brokers, groupID, dlqTopic, maxRetry, baseMs, log)
	if err != nil {
		log.Fatal().Err(err).Msg("dlq worker init failed")
	}
	defer dlqWorker.Close()

	// TODO step 4: replace with agent.Process() — for now just ack events so we can
	// verify the consumer pipeline is wired correctly
	handler := func(ctx context.Context, ev *events.Event) error {
		log.Info().Str("id", ev.ID).Str("type", string(ev.Type)).Msg("event received — agent not wired yet")
		_ = db
		_ = rdb
		return nil
	}

	log.Info().
		Str("brokers", brokers).
		Str("topic", topic).
		Str("dlq", dlqTopic).
		Msg("worker started")

	errCh := make(chan error, 2)
	go func() { errCh <- consumer.Run(ctx, handler) }()
	go func() { errCh <- dlqWorker.Run(ctx, handler) }()

	select {
	case err := <-errCh:
		if err != nil {
			log.Error().Err(err).Msg("worker exited with error")
		}
		cancel()
	case <-ctx.Done():
	}

	log.Info().Msg("shutdown complete")
}
