package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"

	"github.com/gtamizhs14/eventmind/internal/cache"
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
	log.Info().Msg("postgres connected")

	rdb, err := cache.New(os.Getenv("REDIS_URL"))
	if err != nil {
		log.Fatal().Err(err).Msg("redis init failed")
	}
	defer rdb.Close()
	log.Info().Msg("redis connected")

	// Kafka consumer + agent wired in steps 3 and 4
	_ = db
	_ = rdb

	log.Info().Msg("worker starting — kafka consumer wired in step 3")
	<-ctx.Done()
	log.Info().Msg("shutdown complete")
}
