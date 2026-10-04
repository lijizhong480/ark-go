package mongodb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

type Metric struct {
	Operation, Endpoint, Database, Collection string
	Success                                   bool
	Duration                                  time.Duration
}
type MetricHook func(Metric)

type Backend interface {
	InsertOne(context.Context, any, bool) (any, error)
	InsertMany(context.Context, []any, bool) ([]any, error)
	UpdateOne(context.Context, any, any, bool) (int64, error)
	UpdateMany(context.Context, any, any, bool) (int64, error)
	DeleteMany(context.Context, any, bool) (int64, error)
	Find(context.Context, any, int64, int64, any, string) ([]bson.M, error)
	Count(context.Context, any, string) (int64, error)
	Aggregate(context.Context, any, string) ([]bson.M, error)
}
type Connection interface {
	Ping(context.Context) error
	Collection(string) Backend
	Close(context.Context) error
}
type Dialer func(context.Context, string, InstanceConfig) (Connection, error)

type Registry struct {
	mu        sync.RWMutex
	instances map[string]registered
	closed    bool
	metric    MetricHook
}
type registered struct {
	connection                         Connection
	database, endpoint, readPreference string
}

func Open(ctx context.Context, cfg Config, metric MetricHook) (*Registry, error) {
	return OpenWithDialer(ctx, cfg, metric, defaultDialer)
}
func OpenWithDialer(ctx context.Context, cfg Config, metric MetricHook, dialer Dialer) (*Registry, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if dialer == nil {
		return nil, errors.New("mongodb: nil dialer")
	}
	registry := &Registry{instances: map[string]registered{}, metric: metric}
	for name, instance := range cfg.Instances {
		uri, err := cfg.uri(instance)
		if err != nil {
			registry.Close(ctx)
			return nil, fmt.Errorf("mongodb: %s: %w", name, err)
		}
		connection, err := dialer(ctx, uri, instance)
		if err != nil {
			registry.Close(ctx)
			return nil, fmt.Errorf("mongodb: dial %s: %w", name, err)
		}
		registry.instances[name] = registered{connection: connection, database: instance.Database, endpoint: instance.ClusterEndpoint, readPreference: instance.ReadPreference}
		if err := connection.Ping(ctx); err != nil {
			registry.Close(ctx)
			return nil, fmt.Errorf("mongodb: ping %s: %w", name, err)
		}
	}
	return registry, nil
}
func (r *Registry) Collection(instance, name string) (*Collection, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil, errors.New("mongodb: registry closed")
	}
	selected, ok := r.instances[instance]
	if !ok {
		return nil, fmt.Errorf("mongodb: unknown instance %q", instance)
	}
	if name == "" {
		return nil, errors.New("mongodb: empty collection")
	}
	pref := selected.readPreference
	if pref == "" {
		pref = "secondaryPreferred"
	}
	return &Collection{backend: selected.connection.Collection(name), metric: r.metric, endpoint: selected.endpoint, database: selected.database, name: name, readPreference: pref}, nil
}
func (r *Registry) Close(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	var first error
	for name, instance := range r.instances {
		if err := instance.connection.Close(ctx); err != nil && first == nil {
			first = fmt.Errorf("mongodb: close %s: %w", name, err)
		}
	}
	return first
}

type driverConnection struct {
	client   *mongo.Client
	database *mongo.Database
}

func defaultDialer(ctx context.Context, uri string, cfg InstanceConfig) (Connection, error) {
	opts := options.Client().ApplyURI(uri)
	if cfg.ConnectTimeout > 0 {
		opts.SetConnectTimeout(cfg.ConnectTimeout)
	}
	if cfg.ReadTimeout > 0 {
		opts.SetSocketTimeout(cfg.ReadTimeout)
	}
	if cfg.ServerSelectionTimeout > 0 {
		opts.SetServerSelectionTimeout(cfg.ServerSelectionTimeout)
	}
	if cfg.MaxPoolSize > 0 {
		opts.SetMaxPoolSize(cfg.MaxPoolSize)
	}
	if cfg.MinPoolSize > 0 {
		opts.SetMinPoolSize(cfg.MinPoolSize)
	}
	if cfg.MaxConnIdleTime > 0 {
		opts.SetMaxConnIdleTime(cfg.MaxConnIdleTime)
	}
	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &driverConnection{client: client, database: client.Database(cfg.Database)}, nil
}
func (d *driverConnection) Ping(ctx context.Context) error {
	return d.client.Ping(ctx, readpref.Primary())
}
func (d *driverConnection) Collection(name string) Backend {
	return driverBackend{database: d.database, name: name}
}
func (d *driverConnection) Close(ctx context.Context) error { return d.client.Disconnect(ctx) }

type driverBackend struct {
	database *mongo.Database
	name     string
}

func (d driverBackend) collection(pref string, majority bool) (*mongo.Collection, error) {
	opts := options.Collection()
	if pref != "" {
		read, err := readpref.ModeFromString(pref)
		if err != nil {
			return nil, err
		}
		selected, err := readpref.New(read)
		if err != nil {
			return nil, err
		}
		opts.SetReadPreference(selected)
	}
	if majority {
		opts.SetWriteConcern(writeconcern.Majority())
	}
	return d.database.Collection(d.name, opts), nil
}
func (d driverBackend) InsertOne(ctx context.Context, doc any, majority bool) (any, error) {
	coll, err := d.collection("", majority)
	if err != nil {
		return nil, err
	}
	res, err := coll.InsertOne(ctx, doc)
	if err != nil {
		return nil, err
	}
	return res.InsertedID, nil
}
func (d driverBackend) InsertMany(ctx context.Context, docs []any, majority bool) ([]any, error) {
	coll, err := d.collection("", majority)
	if err != nil {
		return nil, err
	}
	res, err := coll.InsertMany(ctx, docs)
	if err != nil {
		return nil, err
	}
	return res.InsertedIDs, nil
}
func (d driverBackend) UpdateOne(ctx context.Context, filter, update any, majority bool) (int64, error) {
	coll, err := d.collection("", majority)
	if err != nil {
		return 0, err
	}
	res, err := coll.UpdateOne(ctx, filter, update)
	if err != nil {
		return 0, err
	}
	return res.ModifiedCount, nil
}
func (d driverBackend) UpdateMany(ctx context.Context, filter, update any, majority bool) (int64, error) {
	coll, err := d.collection("", majority)
	if err != nil {
		return 0, err
	}
	res, err := coll.UpdateMany(ctx, filter, update)
	if err != nil {
		return 0, err
	}
	return res.ModifiedCount, nil
}
func (d driverBackend) DeleteMany(ctx context.Context, filter any, majority bool) (int64, error) {
	coll, err := d.collection("", majority)
	if err != nil {
		return 0, err
	}
	res, err := coll.DeleteMany(ctx, filter)
	if err != nil {
		return 0, err
	}
	return res.DeletedCount, nil
}
func (d driverBackend) Find(ctx context.Context, filter any, skip, limit int64, sort any, pref string) ([]bson.M, error) {
	coll, err := d.collection(pref, false)
	if err != nil {
		return nil, err
	}
	opts := options.Find().SetSkip(skip)
	if limit > 0 {
		opts.SetLimit(limit)
	}
	if sort != nil {
		opts.SetSort(sort)
	}
	cursor, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var docs []bson.M
	err = cursor.All(ctx, &docs)
	return docs, err
}
func (d driverBackend) Count(ctx context.Context, filter any, pref string) (int64, error) {
	coll, err := d.collection(pref, false)
	if err != nil {
		return 0, err
	}
	return coll.CountDocuments(ctx, filter)
}
func (d driverBackend) Aggregate(ctx context.Context, pipeline any, pref string) ([]bson.M, error) {
	coll, err := d.collection(pref, false)
	if err != nil {
		return nil, err
	}
	cursor, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var docs []bson.M
	err = cursor.All(ctx, &docs)
	return docs, err
}
