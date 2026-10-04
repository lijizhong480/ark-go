// Package apollo provides ordered Apollo namespace lookup with typed accessors.
package apollo

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const CommonRuleNamespace = "common-rule"

// Source is the minimal backend contract. A missing key must return false;
// an explicitly empty value must return true.
type Source interface {
	Lookup(namespace, key string) (string, bool)
}

// Change describes a namespace update. A nil Keys slice means the whole
// namespace may have changed.
type Change struct {
	Namespace string
	Keys      []string
}

type cacheEntry struct {
	value   any
	expires time.Time
}

// Config is safe for concurrent use. Namespace order is significant.
type Config struct {
	source      Source
	namespaces  []string
	ttl         time.Duration
	mu          sync.RWMutex
	cache       map[string]cacheEntry
	generation  uint64
	watchers    map[uint64]func(Change)
	nextWatcher uint64
	closed      bool
	closeSource func()
}

// New creates a config reader. The caller retains ownership of source.
// A nonpositive TTL disables the read cache.
func New(source Source, namespaces []string, ttl time.Duration) (*Config, error) {
	if source == nil {
		return nil, errors.New("apollo: nil source")
	}
	seen := map[string]bool{}
	ordered := make([]string, 0, len(namespaces)+1)
	for _, namespace := range append(append([]string(nil), namespaces...), CommonRuleNamespace) {
		namespace = strings.TrimSpace(namespace)
		if namespace == "" {
			return nil, errors.New("apollo: empty namespace")
		}
		if !seen[namespace] {
			ordered = append(ordered, namespace)
			seen[namespace] = true
		}
	}
	return &Config{source: source, namespaces: ordered, ttl: ttl, cache: make(map[string]cacheEntry), watchers: make(map[uint64]func(Change))}, nil
}

func (c *Config) Namespaces() []string { return append([]string(nil), c.namespaces...) }

// Lookup distinguishes a missing key from an explicitly empty value.
func (c *Config) Lookup(key string) (string, bool) {
	for _, ns := range c.namespaces {
		if value, ok := c.source.Lookup(ns, key); ok {
			return value, true
		}
	}
	return "", false
}

func (c *Config) String(key, fallback string) string {
	if value, ok := c.Lookup(key); ok {
		return value
	}
	return fallback
}

func (c *Config) Bool(key string, fallback bool) (bool, error) {
	value, ok := c.Lookup(key)
	if !ok {
		return fallback, nil
	}
	return value == "true", nil
}
func (c *Config) Int(key string, fallback int) (int, error) {
	value, ok := c.Lookup(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback, fmt.Errorf("apollo: %q: %w", key, err)
	}
	return parsed, nil
}
func (c *Config) Int64(key string, fallback int64) (int64, error) {
	value, ok := c.Lookup(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback, fmt.Errorf("apollo: %q: %w", key, err)
	}
	return parsed, nil
}
func (c *Config) Float64(key string, fallback float64) (float64, error) {
	value, ok := c.Lookup(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fallback, fmt.Errorf("apollo: %q: %w", key, err)
	}
	return parsed, nil
}
func (c *Config) List(key, delimiter string, fallback []string) ([]string, error) {
	if delimiter == "" {
		return nil, errors.New("apollo: empty list delimiter")
	}
	value, ok := c.Lookup(key)
	if !ok {
		return append([]string(nil), fallback...), nil
	}
	pattern, err := regexp.Compile(delimiter)
	if err != nil {
		return nil, fmt.Errorf("apollo: invalid list delimiter %q: %w", delimiter, err)
	}
	parts := pattern.Split(value, -1)
	if value != "" {
		for len(parts) > 0 && parts[len(parts)-1] == "" {
			parts = parts[:len(parts)-1]
		}
	}
	return parts, nil
}
func (c *Config) ContainsUID(uid int64, key, delimiter string, fallback []string) (bool, error) {
	values, err := c.List(key, delimiter, fallback)
	if err != nil {
		return false, err
	}
	target := strconv.FormatInt(uid, 10)
	for _, value := range values {
		if strings.TrimSpace(value) == target {
			return true, nil
		}
	}
	return false, nil
}
func (c *Config) JSON(key string, target any) (bool, error) {
	value, ok := c.Lookup(key)
	if !ok {
		return false, nil
	}
	if err := json.Unmarshal([]byte(value), target); err != nil {
		return true, fmt.Errorf("apollo: %q: %w", key, err)
	}
	return true, nil
}

// CachedString uses the configured TTL. Default values participate in the
// cache key, so separate callers cannot contaminate each other's fallback.
func (c *Config) CachedString(key, fallback string) string {
	cacheKey := "s\x00" + key + "\x00" + fallback
	if value, ok := c.cached(cacheKey); ok {
		return value.(string)
	}
	generation := c.cacheGeneration()
	value := c.String(key, fallback)
	c.putCache(cacheKey, value, generation)
	return value
}
func (c *Config) CachedBool(key string, fallback bool) (bool, error) {
	cacheKey := fmt.Sprintf("b\x00%s\x00%t", key, fallback)
	if value, ok := c.cached(cacheKey); ok {
		return value.(bool), nil
	}
	generation := c.cacheGeneration()
	value, err := c.Bool(key, fallback)
	if err == nil {
		c.putCache(cacheKey, value, generation)
	}
	return value, err
}
func (c *Config) CachedList(key, delimiter string, fallback []string) ([]string, error) {
	cacheKey := "l\x00" + key + "\x00" + delimiter + "\x00" + strings.Join(fallback, "\x00")
	if value, ok := c.cached(cacheKey); ok {
		return append([]string(nil), value.([]string)...), nil
	}
	generation := c.cacheGeneration()
	value, err := c.List(key, delimiter, fallback)
	if err == nil {
		c.putCache(cacheKey, append([]string(nil), value...), generation)
	}
	return value, err
}
func (c *Config) cached(key string) (any, bool) {
	if c.ttl <= 0 {
		return nil, false
	}
	c.mu.RLock()
	entry, ok := c.cache[key]
	c.mu.RUnlock()
	if !ok || !time.Now().Before(entry.expires) {
		return nil, false
	}
	return entry.value, true
}
func (c *Config) cacheGeneration() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.generation
}
func (c *Config) putCache(key string, value any, generation uint64) {
	if c.ttl <= 0 {
		return
	}
	c.mu.Lock()
	if !c.closed && c.generation == generation {
		prefix := key[:1]
		count := 0
		for existing := range c.cache {
			if strings.HasPrefix(existing, prefix) {
				count++
			}
		}
		if count >= 1000 {
			for existing := range c.cache {
				if strings.HasPrefix(existing, prefix) {
					delete(c.cache, existing)
					break
				}
			}
		}
		c.cache[key] = cacheEntry{value: value, expires: time.Now().Add(c.ttl)}
	}
	c.mu.Unlock()
}

// Watch registers a callback. The returned function removes it. Callbacks run
// synchronously after cache invalidation and must return promptly.
func (c *Config) Watch(fn func(Change)) func() {
	if fn == nil {
		return func() {}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return func() {}
	}
	c.nextWatcher++
	id := c.nextWatcher
	c.watchers[id] = fn
	return func() { c.mu.Lock(); delete(c.watchers, id); c.mu.Unlock() }
}

// Notify invalidates cached values and broadcasts a backend change.
func (c *Config) Notify(change Change) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.generation++
	c.cache = make(map[string]cacheEntry)
	callbacks := make([]func(Change), 0, len(c.watchers))
	for _, fn := range c.watchers {
		callbacks = append(callbacks, fn)
	}
	c.mu.Unlock()
	change.Keys = append([]string(nil), change.Keys...)
	for _, fn := range callbacks {
		fn(change)
	}
}

// Close removes SDK listeners and closes an SDK client owned by NewApollo.
func (c *Config) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.cache = nil
	c.watchers = nil
	closeSource := c.closeSource
	c.mu.Unlock()
	if closeSource != nil {
		closeSource()
	}
	return nil
}
