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

	// TODO: wired up in steps 2, 3, 4, 5
	_ = ctx
	log.Info().Msg("worker starting")

	<-ctx.Done()
	log.Info().Msg("shutting down")
}
