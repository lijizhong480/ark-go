package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redisv8 "github.com/go-redis/redis/v8"
)

func testOperations(t *testing.T, codec Codec) (*Operations, *redisv8.Client, *miniredis.Miniredis) {
	t.Helper()
	s := miniredis.RunT(t)
	client := redisv8.NewClient(&redisv8.Options{Addr: s.Addr()})
	t.Cleanup(func() { client.Close() })
	ops, err := NewOperations(client, codec)
	if err != nil {
		t.Fatal(err)
	}
	return ops, client, s
}

func TestOperationsAcrossDataStructures(t *testing.T) {
	ctx := context.Background()
	o, _, _ := testOperations(t, StringCodec{})
	if err := o.Values().Set(ctx, "value", "hello", time.Minute); err != nil {
		t.Fatal(err)
	}
	var value string
	if found, err := o.Values().Get(ctx, "value", &value); err != nil || !found || value != "hello" {
		t.Fatalf("get: %q %v %v", value, found, err)
	}
	if found, err := o.Values().Get(ctx, "missing", &value); err != nil || found {
		t.Fatalf("missing: %v %v", found, err)
	}
	if ok, err := o.Values().SetIfAbsent(ctx, "value", "other", 0); err != nil || ok {
		t.Fatalf("setnx: %v %v", ok, err)
	}
	if ttl, err := o.TTL(ctx, "value"); err != nil || ttl <= 0 {
		t.Fatalf("ttl: %v %v", ttl, err)
	}
	if err := o.Hashes().Put(ctx, "hash", "field", "entry"); err != nil {
		t.Fatal(err)
	}
	if found, err := o.Hashes().Get(ctx, "hash", "field", &value); err != nil || !found || value != "entry" {
		t.Fatalf("hget: %q %v %v", value, found, err)
	}
	if _, err := o.Lists().RightPush(ctx, "list", "one", "two"); err != nil {
		t.Fatal(err)
	}
	if got, err := o.Lists().Range(ctx, "list", 0, -1); err != nil || len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Fatalf("lrange: %v %v", got, err)
	}
	if found, err := o.Lists().LeftPop(ctx, "list", &value); err != nil || !found || value != "one" {
		t.Fatalf("lpop: %q %v %v", value, found, err)
	}
	if _, err := o.Sets().Add(ctx, "set", "a", "b"); err != nil {
		t.Fatal(err)
	}
	if yes, err := o.Sets().IsMember(ctx, "set", "b"); err != nil || !yes {
		t.Fatalf("sismember: %v %v", yes, err)
	}
	if _, err := o.SortedSets().Add(ctx, "zset", 3.5, "member"); err != nil {
		t.Fatal(err)
	}
	if score, err := o.SortedSets().Score(ctx, "zset", "member"); err != nil || score != 3.5 {
		t.Fatalf("zscore: %v %v", score, err)
	}
}

func TestJSONCodecRoundTripAndStringRejection(t *testing.T) {
	ctx := context.Background()
	o, _, _ := testOperations(t, JSONCodec{})
	type record struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	if err := o.Values().Set(ctx, "record", record{"x", 2}, 0); err != nil {
		t.Fatal(err)
	}
	var got record
	if found, err := o.Values().Get(ctx, "record", &got); err != nil || !found || got.Name != "x" || got.Count != 2 {
		t.Fatalf("json: %+v %v %v", got, found, err)
	}
	if _, err := (StringCodec{}).Marshal(42); err == nil {
		t.Fatal("string codec accepted integer")
	}
}

func TestLockOwnershipAndExpiry(t *testing.T) {
	ctx := context.Background()
	_, client, server := testOperations(t, StringCodec{})
	first, ok, err := AcquireLock(ctx, client, "lock", time.Second)
	if err != nil || !ok {
		t.Fatalf("first acquire: %v %v", ok, err)
	}
	if _, ok, err := AcquireLock(ctx, client, "lock", time.Second); err != nil || ok {
		t.Fatalf("second acquire: %v %v", ok, err)
	}
	server.FastForward(2 * time.Second)
	second, ok, err := AcquireLock(ctx, client, "lock", time.Second)
	if err != nil || !ok {
		t.Fatalf("after expiry: %v %v", ok, err)
	}
	if released, err := first.Release(ctx); err != nil || released {
		t.Fatalf("stale release: %v %v", released, err)
	}
	if released, err := second.Release(ctx); err != nil || !released {
		t.Fatalf("owner release: %v %v", released, err)
	}
}

func TestCollectionReadsDecodeJacksonStringValues(t *testing.T) {
	ctx := context.Background()
	o, _, _ := testOperations(t, JacksonStringCodec{})
	if err := o.Hashes().Put(ctx, "hash", "field", "logical"); err != nil {
		t.Fatal(err)
	}
	if got, err := o.Hashes().Entries(ctx, "hash"); err != nil || got["field"] != "logical" {
		t.Fatalf("decoded hash: %v %v", got, err)
	}
	if raw, err := o.Hashes().RawEntries(ctx, "hash"); err != nil || raw["field"] != `"logical"` {
		t.Fatalf("raw hash: %v %v", raw, err)
	}
	if _, err := o.Lists().RightPush(ctx, "list", "logical"); err != nil {
		t.Fatal(err)
	}
	if got, err := o.Lists().Range(ctx, "list", 0, -1); err != nil || len(got) != 1 || got[0] != "logical" {
		t.Fatalf("decoded list: %v %v", got, err)
	}
	if raw, err := o.Lists().RawRange(ctx, "list", 0, -1); err != nil || len(raw) != 1 || raw[0] != `"logical"` {
		t.Fatalf("raw list: %v %v", raw, err)
	}
	if _, err := o.Sets().Add(ctx, "set", "logical"); err != nil {
		t.Fatal(err)
	}
	if got, err := o.Sets().Members(ctx, "set"); err != nil || len(got) != 1 || got[0] != "logical" {
		t.Fatalf("decoded set: %v %v", got, err)
	}
	if raw, err := o.Sets().RawMembers(ctx, "set"); err != nil || len(raw) != 1 || raw[0] != `"logical"` {
		t.Fatalf("raw set: %v %v", raw, err)
	}
	if _, err := o.SortedSets().Add(ctx, "zset", 2.5, "logical"); err != nil {
		t.Fatal(err)
	}
	if got, err := o.SortedSets().Range(ctx, "zset", 0, -1); err != nil || len(got) != 1 || got[0] != "logical" {
		t.Fatalf("decoded zset: %v %v", got, err)
	}
	if got, err := o.SortedSets().RangeWithScores(ctx, "zset", 0, -1); err != nil || len(got) != 1 || got[0].Member != "logical" || got[0].Score != 2.5 {
		t.Fatalf("decoded scored zset: %v %v", got, err)
	}
	if raw, err := o.SortedSets().RawRangeWithScores(ctx, "zset", 0, -1); err != nil || len(raw) != 1 || raw[0].Member != `"logical"` {
		t.Fatalf("raw scored zset: %v %v", raw, err)
	}
}
