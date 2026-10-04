package mongodb

import (
	"context"
	"errors"
	"math/big"
	"reflect"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type fakeBackend struct {
	calls               []string
	findSkip, findLimit int64
	pref                string
	majority            bool
	aggregatePipeline   any
	aggregateDocs       []bson.M
}

func (f *fakeBackend) InsertOne(_ context.Context, _ any, majority bool) (any, error) {
	f.calls = append(f.calls, "insert")
	f.majority = majority
	return "id", nil
}
func (f *fakeBackend) InsertMany(_ context.Context, _ []any, majority bool) ([]any, error) {
	f.calls = append(f.calls, "insertMany")
	f.majority = majority
	return []any{"id"}, nil
}
func (f *fakeBackend) UpdateOne(_ context.Context, _, _ any, majority bool) (int64, error) {
	f.calls = append(f.calls, "updateOne")
	f.majority = majority
	return 1, nil
}
func (f *fakeBackend) UpdateMany(_ context.Context, _, _ any, majority bool) (int64, error) {
	f.calls = append(f.calls, "updateMany")
	f.majority = majority
	return 2, nil
}
func (f *fakeBackend) DeleteMany(_ context.Context, _ any, majority bool) (int64, error) {
	f.calls = append(f.calls, "deleteMany")
	f.majority = majority
	return 1, nil
}
func (f *fakeBackend) Find(_ context.Context, _ any, skip, limit int64, _ any, pref string) ([]bson.M, error) {
	f.calls = append(f.calls, "find")
	f.findSkip = skip
	f.findLimit = limit
	f.pref = pref
	return []bson.M{{"name": "first"}}, nil
}
func (f *fakeBackend) Count(_ context.Context, _ any, pref string) (int64, error) {
	f.calls = append(f.calls, "count")
	f.pref = pref
	return 12, nil
}
func (f *fakeBackend) Aggregate(_ context.Context, pipeline any, pref string) ([]bson.M, error) {
	f.calls = append(f.calls, "aggregate")
	f.aggregatePipeline = pipeline
	f.pref = pref
	return f.aggregateDocs, nil
}

type fakeConnection struct {
	backend *fakeBackend
	pingErr error
	closed  bool
}

func (f *fakeConnection) Ping(context.Context) error  { return f.pingErr }
func (f *fakeConnection) Collection(string) Backend   { return f.backend }
func (f *fakeConnection) Close(context.Context) error { f.closed = true; return nil }
func config() Config {
	return Config{Instances: map[string]InstanceConfig{"main": {URI: "mongodb://localhost:27017", Database: "orders", ClusterEndpoint: "host", ReadPreference: "primary"}}}
}
func TestRegistryPageAndMetrics(t *testing.T) {
	fake := &fakeConnection{backend: &fakeBackend{}}
	var metrics []Metric
	registry, err := OpenWithDialer(context.Background(), config(), func(m Metric) { metrics = append(metrics, m) }, func(_ context.Context, _ string, _ InstanceConfig) (Connection, error) { return fake, nil })
	if err != nil {
		t.Fatal(err)
	}
	collection, err := registry.Collection("main", "events")
	if err != nil {
		t.Fatal(err)
	}
	type event struct {
		Name string `bson:"name"`
	}
	page, err := FindPage[event](context.Background(), collection, bson.M{"status": "ok"}, 5, 3, bson.D{{Key: "name", Value: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if page.Total != 12 || len(page.Items) != 1 || page.Items[0].Name != "first" {
		t.Fatalf("page: %+v", page)
	}
	if !reflect.DeepEqual(fake.backend.calls, []string{"count", "find"}) || fake.backend.findSkip != 5 || fake.backend.findLimit != 3 || fake.backend.pref != "primary" {
		t.Fatalf("backend: %+v", fake.backend)
	}
	if len(metrics) != 2 || metrics[0].Operation != "count" || metrics[1].Operation != "find" || metrics[0].Database != "orders" || !metrics[1].Success {
		t.Fatalf("metrics: %+v", metrics)
	}
	if _, err := collection.WithMajority().InsertOne(context.Background(), bson.M{"x": 1}); err != nil || !fake.backend.majority {
		t.Fatalf("majority: %v", err)
	}
	if err := registry.Close(context.Background()); err != nil || !fake.closed {
		t.Fatalf("close: %v", err)
	}
	if _, err := registry.Collection("main", "events"); err == nil {
		t.Fatal("closed registry served collection")
	}
}
func TestAggregations(t *testing.T) {
	fake := &fakeBackend{}
	c := &Collection{backend: fake, name: "events", readPreference: "secondaryPreferred"}
	decimal, err := primitive.ParseDecimal128("123.45")
	if err != nil {
		t.Fatal(err)
	}
	fake.aggregateDocs = []bson.M{{"sum": decimal}}
	sum, err := c.Sum(context.Background(), bson.M{"status": 1}, "amount")
	if err != nil || sum.Cmp(new(big.Rat).SetFrac64(2469, 20)) != 0 {
		t.Fatalf("sum: %v %v", sum, err)
	}
	pipeline, ok := fake.aggregatePipeline.(bson.A)
	if !ok || len(pipeline) != 2 {
		t.Fatalf("sum pipeline: %#v", fake.aggregatePipeline)
	}
	fake.aggregateDocs = []bson.M{{"_id": "A", "count": int32(3)}}
	grouped, err := c.GroupCount(context.Background(), nil, "status")
	if err != nil || len(grouped) != 1 || grouped[0].Count != 3 {
		t.Fatalf("group: %+v %v", grouped, err)
	}
	fake.aggregateDocs = []bson.M{{"_id": bson.M{"status": "A", "type": "x"}, "count": int64(4)}}
	multi, err := c.GroupCountByFields(context.Background(), nil, []string{"status", "type"})
	if err != nil || len(multi) != 1 || multi[0].GroupFields["status"] != "A" {
		t.Fatalf("multi: %+v %v", multi, err)
	}
	if _, err := c.GroupCountByFields(context.Background(), nil, nil); err == nil {
		t.Fatal("expected empty fields error")
	}
	if _, err := c.Sum(context.Background(), nil, "$bad"); err == nil {
		t.Fatal("expected invalid field error")
	}
}
func TestConfigTemplateDecryptAndPingCleanup(t *testing.T) {
	cfg := Config{Instances: map[string]InstanceConfig{"main": {Template: "mongodb://%s:%s@%s/%s?readPreference=%s", Username: "user", Password: "ENC(secret)", ClusterEndpoint: "localhost:27017", Database: "orders", ReadPreference: "secondaryPreferred"}}, Decrypt: func(value string) (string, error) {
		if value != "secret" {
			t.Fatal(value)
		}
		return "p@ss", nil
	}}
	var uri string
	fake := &fakeConnection{backend: &fakeBackend{}, pingErr: errors.New("down")}
	_, err := OpenWithDialer(context.Background(), cfg, nil, func(_ context.Context, got string, _ InstanceConfig) (Connection, error) { uri = got; return fake, nil })
	if err == nil || !strings.Contains(err.Error(), "ping") || !fake.closed {
		t.Fatalf("startup cleanup: %v", err)
	}
	if !strings.Contains(uri, "p%40ss") {
		t.Fatalf("password not escaped: %s", uri)
	}
}
