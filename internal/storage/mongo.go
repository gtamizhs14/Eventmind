package storage

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
)

type MongoStore struct {
	client *mongo.Client
	db     *mongo.Database
}

// NewMongo connects to MongoDB.
// Implemented in step 5.
func NewMongo(ctx context.Context, uri, dbName string) (*MongoStore, error) {
	panic("not implemented — see step 5")
}

func (m *MongoStore) Close(ctx context.Context) error {
	return m.client.Disconnect(ctx)
}
