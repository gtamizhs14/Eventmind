package storage

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gtamizhs14/eventmind/internal/agent"
	"github.com/gtamizhs14/eventmind/internal/events"
)

const schema = `
CREATE TABLE IF NOT EXISTS events (
	id         TEXT PRIMARY KEY,
	type       TEXT NOT NULL,
	payload    JSONB NOT NULL,
	source     TEXT,
	ts         TIMESTAMPTZ NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS decisions (
	id           TEXT PRIMARY KEY,
	event_id     TEXT NOT NULL REFERENCES events(id),
	event_type   TEXT NOT NULL,
	action       TEXT NOT NULL,
	reasoning    TEXT NOT NULL,
	llm_prompt   TEXT NOT NULL,
	llm_response TEXT NOT NULL,
	success         BOOLEAN NOT NULL,
	error            TEXT,
	duration_ms      BIGINT NOT NULL,
	llm_duration_ms  BIGINT NOT NULL DEFAULT 0,
	retry_count      INT NOT NULL DEFAULT 0,
	status       TEXT NOT NULL DEFAULT 'completed',
	processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_decisions_event_type  ON decisions(event_type);
CREATE INDEX IF NOT EXISTS idx_decisions_processed_at ON decisions(processed_at DESC);
`

type PGStore struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, dsn string) (*PGStore, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres config: %w", err)
	}

	cfg.MaxConns = int32(envInt("DATABASE_MAX_CONNS", 10))
	cfg.MinConns = 2
	cfg.MaxConnLifetime = time.Hour
	cfg.MaxConnIdleTime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres ping: %w", err)
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres migrate: %w", err)
	}
	return &PGStore{pool: pool}, nil
}

func (s *PGStore) Close() { s.pool.Close() }

func (s *PGStore) SaveEvent(ctx context.Context, ev *events.Event) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO events (id, type, payload, source, ts)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (id) DO NOTHING`,
		ev.ID, string(ev.Type), ev.Payload, ev.Source, ev.Timestamp,
	)
	return err
}

func (s *PGStore) SaveDecision(ctx context.Context, d *agent.Decision) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO decisions
		  (id, event_id, event_type, action, reasoning, llm_prompt, llm_response,
		   success, error, duration_ms, llm_duration_ms, retry_count, status, processed_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
		d.ID, d.EventID, string(d.EventType), string(d.Action),
		d.Reasoning, d.LLMPrompt, d.LLMResponse,
		d.Success, nilIfEmpty(d.Error),
		d.DurationMs, d.LLMDurationMs, d.RetryCount, d.Status, d.ProcessedAt,
	)
	return err
}

func (s *PGStore) GetDecision(ctx context.Context, id string) (*agent.Decision, error) {
	return scanDecision(s.pool.QueryRow(ctx, decisionSelect+` WHERE id = $1`, id))
}

func (s *PGStore) ListDecisions(ctx context.Context, limit, offset int, eventType string) ([]*agent.Decision, error) {
	var rows pgx.Rows
	var err error

	if eventType != "" {
		rows, err = s.pool.Query(ctx,
			decisionSelect+` WHERE event_type = $1 ORDER BY processed_at DESC LIMIT $2 OFFSET $3`,
			eventType, limit, offset,
		)
	} else {
		rows, err = s.pool.Query(ctx,
			decisionSelect+` ORDER BY processed_at DESC LIMIT $1 OFFSET $2`,
			limit, offset,
		)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*agent.Decision
	for rows.Next() {
		d, err := scanDecision(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *PGStore) GetEventByID(ctx context.Context, id string) (*events.Event, error) {
	var ev events.Event
	var typ string
	err := s.pool.QueryRow(ctx,
		`SELECT id, type, payload, source, ts FROM events WHERE id = $1`, id,
	).Scan(&ev.ID, &typ, &ev.Payload, &ev.Source, &ev.Timestamp)
	if err != nil {
		return nil, err
	}
	ev.Type = events.Type(typ)
	return &ev, nil
}

func (s *PGStore) ListEvents(ctx context.Context, limit, offset int) ([]*events.Event, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, type, payload, source, ts FROM events ORDER BY ts DESC LIMIT $1 OFFSET $2`,
		limit, offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*events.Event
	for rows.Next() {
		var ev events.Event
		var typ string
		if err := rows.Scan(&ev.ID, &typ, &ev.Payload, &ev.Source, &ev.Timestamp); err != nil {
			return nil, err
		}
		ev.Type = events.Type(typ)
		out = append(out, &ev)
	}
	return out, rows.Err()
}

const decisionSelect = `
	SELECT id, event_id, event_type, action, reasoning, llm_prompt, llm_response,
	       success, error, duration_ms, llm_duration_ms, retry_count, status, processed_at
	FROM decisions`

// scanner covers both pgx.Row and pgx.Rows — both have the same Scan signature.
type scanner interface{ Scan(dest ...any) error }

func scanDecision(row scanner) (*agent.Decision, error) {
	var d agent.Decision
	var typ, action string
	var errStr *string
	err := row.Scan(
		&d.ID, &d.EventID, &typ, &action, &d.Reasoning,
		&d.LLMPrompt, &d.LLMResponse, &d.Success, &errStr,
		&d.DurationMs, &d.LLMDurationMs, &d.RetryCount, &d.Status, &d.ProcessedAt,
	)
	if err != nil {
		return nil, err
	}
	d.EventType = events.Type(typ)
	d.Action = agent.Action(action)
	if errStr != nil {
		d.Error = *errStr
	}
	return &d, nil
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
