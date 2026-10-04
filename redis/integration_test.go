package redis

import (
	"context"
	"net"
	"os"
	"strconv"
	"testing"
	"time"
)

// TestRealRedis is opt-in: ARK_REDIS_TEST_ADDR=host:port go test -run TestRealRedis.
// It verifies server TTL and Lua lock behavior beyond miniredis simulation.
func TestRealRedis(t *testing.T) {
	address := os.Getenv("ARK_REDIS_TEST_ADDR")
	if address == "" { t.Skip("set ARK_REDIS_TEST_ADDR for a real Redis integration test") }
	host, portText, err := net.SplitHostPort(address)
	if err != nil { t.Fatal(err) }
	port, err := strconv.Atoi(portText)
	if err != nil { t.Fatal(err) }
	cfg := DefaultConfig()
	cfg.Host, cfg.Port, cfg.Primary = host, port, true
	clients, err := NewClients(map[string]Config{"main": cfg}, nil)
	if err != nil { t.Fatal(err) }
	defer clients.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := clients.Ping(ctx); err != nil { t.Fatal(err) }
	client, _ := clients.Primary()
	ops, err := NewOperations(client, StringCodec{})
	if err != nil { t.Fatal(err) }
	key := "ark-go-test:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	defer ops.Delete(context.Background(), key, key+":hash", key+":lock")
	if err := ops.Values().Set(ctx, key, "value", 2*time.Second); err != nil { t.Fatal(err) }
	var value string
	if ok, err := ops.Values().Get(ctx, key, &value); err != nil || !ok || value != "value" { t.Fatalf("value=%q ok=%v err=%v", value, ok, err) }
	if ttl, err := ops.TTL(ctx, key); err != nil || ttl <= 0 || ttl > 2*time.Second { t.Fatalf("ttl=%v err=%v", ttl, err) }
	if err := ops.Hashes().Put(ctx, key+":hash", "field", "hello"); err != nil { t.Fatal(err) }
	if entries, err := ops.Hashes().Entries(ctx, key+":hash"); err != nil || entries["field"] != "hello" { t.Fatalf("entries=%v err=%v", entries, err) }
	lock, acquired, err := AcquireLock(ctx, client, key+":lock", time.Second)
	if err != nil || !acquired { t.Fatalf("lock acquired=%v err=%v", acquired, err) }
	if _, again, err := AcquireLock(ctx, client, key+":lock", time.Second); err != nil || again { t.Fatalf("double lock acquired=%v err=%v", again, err) }
	if released, err := lock.Release(ctx); err != nil || !released { t.Fatalf("release=%v err=%v", released, err) }
}
