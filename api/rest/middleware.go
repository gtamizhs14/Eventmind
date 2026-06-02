package rest

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

func RequestLogger(log zerolog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		status := c.Writer.Status()
		evt := log.Info()
		if status >= 500 {
			evt = log.Error()
		} else if status >= 400 {
			evt = log.Warn()
		}

		evt.
			Str("method", c.Request.Method).
			Str("path", c.Request.URL.Path).
			Int("status", status).
			Dur("latency", time.Since(start)).
			Str("ip", c.ClientIP()).
			Str("request_id", c.GetString("request_id")).
			Msg("request")
	}
}

// RequestID stamps every request with a UUID, either from the incoming
// X-Request-ID header or freshly generated. Echoed back in the response.
func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader("X-Request-ID")
		if id == "" {
			id = uuid.New().String()
		}
		c.Set("request_id", id)
		c.Header("X-Request-ID", id)
		c.Next()
	}
}

func Recovery(log zerolog.Logger) gin.HandlerFunc {
	return gin.RecoveryWithWriter(nil, func(c *gin.Context, err any) {
		log.Error().
			Interface("panic", err).
			Str("path", c.Request.URL.Path).
			Msg("recovered from panic")
		c.AbortWithStatusJSON(500, errResp("internal server error"))
	})
}
