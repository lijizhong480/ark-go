package config

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
)

func TestSourcePrecedenceAndFileFormats(t *testing.T) {
	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "app.yaml")
	propertiesPath := filepath.Join(dir, "override.properties")
	if err := os.WriteFile(yamlPath, []byte("server:\n  port: 8080\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(propertiesPath, []byte("server.port=9090\n"), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := Load(context.Background(), FileSource{yamlPath}, FileSource{propertiesPath})
	if err != nil {
		t.Fatal(err)
	}
	if got := s.String("server.port", ""); got != "9090" {
		t.Fatalf("port=%q", got)
	}
}

func TestConcurrentPublishKeepsWatcherAtCurrentSnapshot(t *testing.T) {
	initial, _ := Load(context.Background(), MapSource{"version": "0"})
	store := NewStore(initial)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updates, err := store.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 1; i <= 100; i++ {
		wg.Add(1)
		go func(version int) {
			defer wg.Done()
			s, _ := Load(context.Background(), MapSource{"version": strconv.Itoa(version)})
			store.Publish(s)
		}(i)
	}
	wg.Wait()
	select {
	case last := <-updates:
		if last.String("version", "") != store.Current().String("version", "") {
			t.Fatalf("watcher=%v current=%v", last.Map(), store.Current().Map())
		}
	default:
		t.Fatal("no watcher update")
	}
}

func TestStoreSnapshotIsImmutable(t *testing.T) {
	initial, _ := Load(context.Background(), MapSource{"mode": "old"})
	store := NewStore(initial)
	copyValues := store.Current().Map()
	copyValues["mode"] = "mutated"
	if got := store.Current().String("mode", ""); got != "old" {
		t.Fatalf("mode=%q", got)
	}
}
