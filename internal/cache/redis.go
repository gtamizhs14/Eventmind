package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	seenTTL     = 24 * time.Hour
	decisionTTL = 5 * time.Minute
)

type Cache struct {
	rdb *redis.Client
}

func New(url string) (*Cache, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("redis: bad url: %w", err)
	}
	c := redis.NewClient(opts)
	if err := c.Ping(context.Background()).Err(); err != nil {
		c.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return &Cache{rdb: c}, nil
}

func (c *Cache) Close() error { return c.rdb.Close() }

// Seen returns true if this event ID has already been processed.
// Uses SET NX as an idempotency guard — first caller wins, sets a 24h key.
func (c *Cache) Seen(ctx context.Context, eventID string) (bool, error) {
	set, err := c.rdb.SetNX(ctx, "em:seen:"+eventID, 1, seenTTL).Result()
	if err != nil {
		return false, err
	}
	// SetNX returns true when key was just set (first time), false if it already existed
	return !set, nil
}

// CacheDecision stores a decision JSON blob for fast reads on the query path.
func (c *Cache) CacheDecision(ctx context.Context, id, jsonBlob string) error {
	return c.rdb.Set(ctx, "em:dec:"+id, jsonBlob, decisionTTL).Err()
}

// GetDecision retrieves a cached decision. Returns ("", nil) on miss.
func (c *Cache) GetDecision(ctx context.Context, id string) (string, error) {
	v, err := c.rdb.Get(ctx, "em:dec:"+id).Result()
	if err == redis.Nil {
		return "", nil
	}
	return v, err
}

func (c *Cache) Get(ctx context.Context, key string) (string, error) {
	v, err := c.rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", nil
	}
	return v, err
}

func (c *Cache) Set(ctx context.Context, key, val string, ttl time.Duration) error {
	return c.rdb.Set(ctx, key, val, ttl).Err()
}

func (c *Cache) Del(ctx context.Context, keys ...string) error {
	return c.rdb.Del(ctx, keys...).Err()
}
