package rest

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"

	"github.com/gtamizhs14/eventmind/internal/agent"
	"github.com/gtamizhs14/eventmind/internal/cache"
	"github.com/gtamizhs14/eventmind/internal/events"
	"github.com/gtamizhs14/eventmind/internal/messaging"
	"github.com/gtamizhs14/eventmind/internal/metrics"
	"github.com/gtamizhs14/eventmind/internal/storage"
)

type Handler struct {
	db       *storage.PGStore
	cache    *cache.Cache
	producer *messaging.Producer
	m        *metrics.Metrics
	log      zerolog.Logger
}

func NewHandler(db *storage.PGStore, c *cache.Cache, prod *messaging.Producer, m *metrics.Metrics, log zerolog.Logger) *Handler {
	return &Handler{db: db, cache: c, producer: prod, m: m, log: log}
}

func (h *Handler) RegisterRoutes(r *gin.Engine) {
	r.GET("/health", h.health)

	v1 := r.Group("/api/v1")
	v1.POST("/events", h.ingestEvent)
	v1.GET("/events", h.listEvents)
	v1.GET("/decisions", h.listDecisions)
	v1.GET("/decisions/:id", h.getDecision)
}

func (h *Handler) health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok", "time": time.Now().UTC()})
}

func (h *Handler) ingestEvent(c *gin.Context) {
	var req struct {
		Type    string          `json:"type"    binding:"required"`
		Payload json.RawMessage `json:"payload" binding:"required"`
		Source  string          `json:"source"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, errResp(err.Error()))
		return
	}

	evType := events.Type(req.Type)
	if !evType.Valid() {
		c.JSON(http.StatusBadRequest, errResp(fmt.Sprintf("unknown event type %q", req.Type)))
		return
	}

	ev := &events.Event{
		ID:        uuid.New().String(),
		Type:      evType,
		Payload:   req.Payload,
		Source:    req.Source,
		Timestamp: time.Now().UTC(),
	}

	if err := h.db.SaveEvent(c.Request.Context(), ev); err != nil {
		h.log.Error().Err(err).Msg("SaveEvent failed")
		c.JSON(http.StatusInternalServerError, errResp("failed to save event"))
		return
	}

	if err := h.producer.Publish(c.Request.Context(), ev); err != nil {
		// Event is in Postgres so it's not lost. Kafka failure is non-fatal to the
		// client but means the event won't be processed until manually replayed.
		// A transactional outbox pattern would fix this properly.
		h.log.Error().Err(err).Str("event_id", ev.ID).Msg("kafka publish failed after postgres save")
	}

	c.JSON(http.StatusCreated, ev)
}

func (h *Handler) listDecisions(c *gin.Context) {
	limit := clamp(queryInt(c, "limit", 20), 1, 100)
	offset := max0(queryInt(c, "offset", 0))
	eventType := c.Query("event_type")

	decisions, err := h.db.ListDecisions(c.Request.Context(), limit, offset, eventType)
	if err != nil {
		h.log.Error().Err(err).Msg("ListDecisions failed")
		c.JSON(http.StatusInternalServerError, errResp("failed to fetch decisions"))
		return
	}
	if decisions == nil {
		decisions = []*agent.Decision{}
	}

	c.JSON(http.StatusOK, gin.H{
		"items":  decisions,
		"limit":  limit,
		"offset": offset,
	})
}

func (h *Handler) getDecision(c *gin.Context) {
	id := c.Param("id")

	// cache hit
	if cached, err := h.cache.GetDecision(c.Request.Context(), id); err == nil && cached != "" {
		c.Data(http.StatusOK, "application/json; charset=utf-8", []byte(cached))
		return
	}

	d, err := h.db.GetDecision(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			c.JSON(http.StatusNotFound, errResp("decision not found"))
			return
		}
		h.log.Error().Err(err).Str("id", id).Msg("GetDecision failed")
		c.JSON(http.StatusInternalServerError, errResp("failed to fetch decision"))
		return
	}

	// populate cache for subsequent reads
	if data, err := json.Marshal(d); err == nil {
		_ = h.cache.CacheDecision(c.Request.Context(), id, string(data))
	}

	c.JSON(http.StatusOK, d)
}

func (h *Handler) listEvents(c *gin.Context) {
	limit := clamp(queryInt(c, "limit", 20), 1, 100)
	offset := max0(queryInt(c, "offset", 0))

	evs, err := h.db.ListEvents(c.Request.Context(), limit, offset)
	if err != nil {
		h.log.Error().Err(err).Msg("ListEvents failed")
		c.JSON(http.StatusInternalServerError, errResp("failed to fetch events"))
		return
	}
	if evs == nil {
		evs = []*events.Event{}
	}

	c.JSON(http.StatusOK, gin.H{
		"items":  evs,
		"limit":  limit,
		"offset": offset,
	})
}

// ── helpers ───────────────────────────────────────────────────────────────────

func errResp(msg string) gin.H { return gin.H{"error": msg} }

func queryInt(c *gin.Context, key string, def int) int {
	if v := c.Query(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func max0(v int) int {
	if v < 0 {
		return 0
	}
	return v
}
