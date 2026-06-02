package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"

	"github.com/gtamizhs14/eventmind/pkg/logger"
)

func main() {
	_ = godotenv.Load()
	log := logger.New()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// TODO: wired up in steps 2, 6, 7, 8
	_ = ctx
	log.Info().Str("port", os.Getenv("PORT")).Msg("api server starting")

	// block until signal
	<-ctx.Done()
	log.Info().Msg("shutting down")
}
