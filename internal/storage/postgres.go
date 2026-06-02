package storage

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PGStore struct {
	pool *pgxpool.Pool
}

// New connects to Postgres and runs schema migrations.
// Implemented in step 2.
func New(ctx context.Context, dsn string) (*PGStore, error) {
	panic("not implemented — see step 2")
}

func (s *PGStore) Close() {
	s.pool.Close()
}
