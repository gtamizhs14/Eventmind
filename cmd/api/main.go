package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/gtamizhs14/eventmind/api/graphql/generated"
	gqlresolver "github.com/gtamizhs14/eventmind/api/graphql"
	"github.com/gtamizhs14/eventmind/api/rest"
	"github.com/gtamizhs14/eventmind/internal/cache"
	"github.com/gtamizhs14/eventmind/internal/messaging"
	"github.com/gtamizhs14/eventmind/internal/metrics"
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
	_ = mdb

	rdb, err := cache.New(os.Getenv("REDIS_URL"))
	if err != nil {
		log.Fatal().Err(err).Msg("redis init failed")
	}
	defer rdb.Close()

	producer, err := messaging.NewProducer(os.Getenv("KAFKA_BROKERS"), os.Getenv("KAFKA_TOPIC_EVENTS"), log)
	if err != nil {
		log.Fatal().Err(err).Msg("kafka producer init failed")
	}
	defer producer.Close()

	m := metrics.New()

	if os.Getenv("ENV") == "production" {
		gin.SetMode(gin.ReleaseMode)
	}
	router := gin.New()
	router.Use(rest.RequestID(), rest.RequestLogger(log), rest.Recovery(log))

	// REST
	restHandler := rest.NewHandler(db, rdb, producer, m, log)
	restHandler.RegisterRoutes(router)

	// GraphQL — requires `make gen` to generate api/graphql/generated/
	resolver := gqlresolver.NewResolver(db, rdb, producer)
	gqlSrv := handler.NewDefaultServer(generated.NewExecutableSchema(generated.Config{
		Resolvers: resolver,
	}))
	gqlSrv.AddTransport(transport.Websocket{
		KeepAlivePingInterval: 10 * time.Second,
	})
	gqlSrv.AddTransport(transport.POST{})
	gqlSrv.AddTransport(transport.GET{})

	router.GET("/graphql/playground", func(c *gin.Context) {
		playground.Handler("EventMind GraphQL", "/graphql")(c.Writer, c.Request)
	})
	router.Any("/graphql", func(c *gin.Context) {
		gqlSrv.ServeHTTP(c.Writer, c.Request)
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	metricsPort := os.Getenv("METRICS_PORT")
	if metricsPort == "" {
		metricsPort = "9091"
	}

	metricsSrv := &http.Server{
		Addr:    fmt.Sprintf(":%s", metricsPort),
		Handler: promhttp.Handler(),
	}
	go func() {
		log.Info().Str("port", metricsPort).Msg("metrics server started")
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error().Err(err).Msg("metrics server error")
		}
	}()

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%s", port),
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	go func() {
		log.Info().Str("port", port).Msg("api server started")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error().Err(err).Msg("api server error")
			cancel()
		}
	}()

	<-ctx.Done()
	log.Info().Msg("shutting down")

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutCancel()
	_ = srv.Shutdown(shutCtx)
	_ = metricsSrv.Shutdown(shutCtx)

	log.Info().Msg("shutdown complete")
}
