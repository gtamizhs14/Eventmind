// Package graphql implements the gqlgen resolvers.
// Run `make gen` once before building to generate api/graphql/generated/ and api/graphql/model/.
package graphql

import (
	"sync"

	"github.com/google/uuid"

	"github.com/gtamizhs14/eventmind/internal/cache"
	"github.com/gtamizhs14/eventmind/internal/messaging"
	"github.com/gtamizhs14/eventmind/internal/storage"
)

// Resolver is the root resolver injected into the gqlgen server.
type Resolver struct {
	db       *storage.PGStore
	cache    *cache.Cache
	producer *messaging.Producer
	subs     *subscriptionBroker
}

func NewResolver(db *storage.PGStore, c *cache.Cache, prod *messaging.Producer) *Resolver {
	return &Resolver{
		db:       db,
		cache:    c,
		producer: prod,
		subs:     newSubscriptionBroker(),
	}
}

// subscriptionBroker does in-process fan-out to GraphQL subscription clients.
// In production with multiple API replicas you'd back this with Redis pub/sub.
type subscriptionBroker struct {
	mu   sync.RWMutex
	subs map[string]chan any
}

func newSubscriptionBroker() *subscriptionBroker {
	return &subscriptionBroker{subs: make(map[string]chan any)}
}

func (b *subscriptionBroker) subscribe() (string, <-chan any) {
	b.mu.Lock()
	defer b.mu.Unlock()
	id := uuid.New().String()
	ch := make(chan any, 16)
	b.subs[id] = ch
	return id, ch
}

func (b *subscriptionBroker) unsubscribe(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ch, ok := b.subs[id]; ok {
		close(ch)
		delete(b.subs, id)
	}
}

// Publish sends a value to all active subscribers. Slow clients are dropped.
func (b *subscriptionBroker) Publish(v any) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, ch := range b.subs {
		select {
		case ch <- v:
		default:
		}
	}
}
