package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/gtamizhs14/eventmind/internal/events"
)

type MongoStore struct {
	client *mongo.Client
	coll   *mongo.Collection
}

func NewMongo(ctx context.Context, uri, dbName string) (*MongoStore, error) {
	opts := options.Client().
		ApplyURI(uri).
		SetConnectTimeout(10 * time.Second).
		SetServerSelectionTimeout(10 * time.Second)

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("mongo connect: %w", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		client.Disconnect(ctx)
		return nil, fmt.Errorf("mongo ping: %w", err)
	}

	coll := client.Database(dbName).Collection("events")
	ensureIndexes(ctx, coll)

	return &MongoStore{client: client, coll: coll}, nil
}

func ensureIndexes(ctx context.Context, coll *mongo.Collection) {
	// compound index for the most common query pattern: filter by type, sort by time
	_, _ = coll.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "type", Value: 1}, {Key: "timestamp", Value: -1}}},
		{Keys: bson.D{{Key: "source", Value: 1}}},
	})
}

func (m *MongoStore) Close(ctx context.Context) error {
	return m.client.Disconnect(ctx)
}

// SaveEvent upserts the raw event document. The JSON payload is stored as a
// nested object (not a string) so individual fields are queryable in Mongo.
func (m *MongoStore) SaveEvent(ctx context.Context, ev *events.Event) error {
	var payload any
	if err := json.Unmarshal(ev.Payload, &payload); err != nil {
		payload = string(ev.Payload) // fallback: store as string if not valid JSON
	}

	doc := bson.M{
		"_id":       ev.ID,
		"type":      string(ev.Type),
		"payload":   payload,
		"source":    ev.Source,
		"timestamp": ev.Timestamp,
		"stored_at": time.Now().UTC(),
	}

	_, err := m.coll.ReplaceOne(
		ctx,
		bson.M{"_id": ev.ID},
		doc,
		options.Replace().SetUpsert(true),
	)
	return err
}

// GetEvent returns the raw event document by ID. Returns nil, nil if not found.
func (m *MongoStore) GetEvent(ctx context.Context, id string) (bson.M, error) {
	var doc bson.M
	err := m.coll.FindOne(ctx, bson.M{"_id": id}).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	return doc, err
}

// ListEvents returns event documents, optionally filtered by type.
func (m *MongoStore) ListEvents(ctx context.Context, limit, offset int, eventType string) ([]bson.M, error) {
	filter := bson.M{}
	if eventType != "" {
		filter["type"] = eventType
	}

	cur, err := m.coll.Find(ctx, filter, options.Find().
		SetSort(bson.D{{Key: "timestamp", Value: -1}}).
		SetLimit(int64(limit)).
		SetSkip(int64(offset)),
	)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var docs []bson.M
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	return docs, nil
}

// CountByType returns a count breakdown of events per type — handy for dashboards.
func (m *MongoStore) CountByType(ctx context.Context) (map[string]int64, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$group", Value: bson.M{"_id": "$type", "count": bson.M{"$sum": 1}}}},
	}
	cur, err := m.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	out := make(map[string]int64)
	for cur.Next(ctx) {
		var row struct {
			ID    string `bson:"_id"`
			Count int64  `bson:"count"`
		}
		if err := cur.Decode(&row); err != nil {
			continue
		}
		out[row.ID] = row.Count
	}
	return out, cur.Err()
}
