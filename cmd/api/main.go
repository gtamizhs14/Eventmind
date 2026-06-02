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

	// REST + GraphQL server wired in steps 6 and 7
	_ = db
	_ = mdb
	_ = rdb

	log.Info().Str("port", os.Getenv("PORT")).Msg("api starting — server wired in step 6")
	<-ctx.Done()
	log.Info().Msg("shutdown complete")
}
