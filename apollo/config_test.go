package apollo

import (
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
)

type memorySource struct {
	mu     sync.RWMutex
	values map[string]map[string]string
	reads  int
}

func (s *memorySource) Lookup(namespace, key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	value, ok := s.values[namespace][key]
	return value, ok
}
func (s *memorySource) Set(namespace, key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.values[namespace] == nil {
		s.values[namespace] = map[string]string{}
	}
	s.values[namespace][key] = value
}
func (s *memorySource) Reads() int { s.mu.RLock(); defer s.mu.RUnlock(); return s.reads }

func TestNamespacePriorityAndFallback(t *testing.T) {
	source := &memorySource{values: map[string]map[string]string{
		"first":             {"mode": "", "count": "12"},
		"second":            {"mode": "secondary", "list": "1, 2,3"},
		CommonRuleNamespace: {"shared": "yes", "mode": "common"},
	}}
	cfg, err := New(source, []string{"first", "second", "first", CommonRuleNamespace}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Namespaces(); !reflect.DeepEqual(got, []string{"first", "second", CommonRuleNamespace}) {
		t.Fatalf("namespaces: %v", got)
	}
	if got, ok := cfg.Lookup("mode"); !ok || got != "" {
		t.Fatalf("empty value lost: %q, %v", got, ok)
	}
	if got := cfg.String("shared", "fallback"); got != "yes" {
		t.Fatalf("common fallback: %q", got)
	}
	if got := cfg.String("missing", "fallback"); got != "fallback" {
		t.Fatalf("default: %q", got)
	}
	if got, err := cfg.Int("count", 0); err != nil || got != 12 {
		t.Fatalf("int: %d, %v", got, err)
	}
	if got, err := cfg.ContainsUID(2, "list", ",", nil); err != nil || !got {
		t.Fatalf("uid: %v, %v", got, err)
	}
	var obj struct {
		Enabled bool `json:"enabled"`
	}
	source.Set("first", "json", `{"enabled":true}`)
	if found, err := cfg.JSON("json", &obj); err != nil || !found || !obj.Enabled {
		t.Fatalf("json: %v, %v, %+v", found, err, obj)
	}
}

func TestTypedErrorsAndDefaults(t *testing.T) {
	source := &memorySource{values: map[string]map[string]string{"main": {"bad": "abc", "flag": "true", "upper": "TRUE", "one": "1", "json": "{"}}}
	cfg, err := New(source, []string{"main"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := cfg.Bool("flag", false); err != nil || !got {
		t.Fatalf("bool: %v, %v", got, err)
	}
	for _, key := range []string{"upper", "one"} {
		if got, err := cfg.Bool(key, true); err != nil || got {
			t.Fatalf("Java boolean semantics for %s: %t %v", key, got, err)
		}
	}
	if _, err := cfg.Int64("bad", 0); err == nil {
		t.Fatal("expected parse error")
	}
	if got, err := cfg.Float64("missing", 1.25); err != nil || got != 1.25 {
		t.Fatalf("float default: %v, %v", got, err)
	}
	if _, err := cfg.List("missing", "", nil); err == nil {
		t.Fatal("expected delimiter error")
	}
	if found, err := cfg.JSON("json", &struct{}{}); !found || err == nil {
		t.Fatalf("expected JSON error: %v, %v", found, err)
	}
}

func TestCacheInvalidationTTLAndWatch(t *testing.T) {
	source := &memorySource{values: map[string]map[string]string{"main": {"key": "old", "flag": "false", "list": "a,b"}}}
	cfg, err := New(source, []string{"main"}, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.CachedString("key", "x"); got != "old" {
		t.Fatal(got)
	}
	reads := source.Reads()
	if got := cfg.CachedString("key", "x"); got != "old" || source.Reads() != reads {
		t.Fatal("cache miss")
	}
	source.Set("main", "key", "new")
	if got := cfg.CachedString("key", "x"); got != "old" {
		t.Fatal("TTL cache changed prematurely")
	}
	called := 0
	unwatch := cfg.Watch(func(change Change) {
		called++
		if change.Namespace != "main" {
			t.Fatal(change)
		}
	})
	cfg.Notify(Change{Namespace: "main", Keys: []string{"key"}})
	if got := cfg.CachedString("key", "x"); got != "new" || called != 1 {
		t.Fatalf("invalidation: %q %d", got, called)
	}
	unwatch()
	cfg.Notify(Change{Namespace: "main"})
	if called != 1 {
		t.Fatalf("unwatch failed: %d", called)
	}
	source.Set("main", "key", "latest")
	time.Sleep(25 * time.Millisecond)
	if got := cfg.CachedString("key", "x"); got != "latest" {
		t.Fatalf("TTL expiration: %q", got)
	}
	list, err := cfg.CachedList("list", ",", nil)
	if err != nil {
		t.Fatal(err)
	}
	list[0] = "mutated"
	list, err = cfg.CachedList("list", ",", nil)
	if err != nil || list[0] != "a" {
		t.Fatalf("cache returned mutable alias: %v %v", list, err)
	}
	if err := cfg.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cfg.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestListRegexAndTrailingEmpty(t *testing.T) {
	source := &memorySource{values: map[string]map[string]string{"main": {"v": "a|b||", "empty": ""}}}
	cfg, err := New(source, []string{"main"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, err := cfg.List("v", `\|`, nil)
	if err != nil || !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("regex split: %v %v", got, err)
	}
	got, err = cfg.List("empty", ",", nil)
	if err != nil || !reflect.DeepEqual(got, []string{""}) {
		t.Fatalf("empty split: %v %v", got, err)
	}
}
func TestCacheLimit(t *testing.T) {
	source := &memorySource{values: map[string]map[string]string{}}
	cfg, err := New(source, nil, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1100; i++ {
		cfg.CachedString(strconv.Itoa(i), "")
	}
	if len(cfg.cache) > 1000 {
		t.Fatalf("cache size: %d", len(cfg.cache))
	}
}
