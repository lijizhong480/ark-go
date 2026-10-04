// Package cache contains bounded local caches compatible with Ark's LRU and
// Caffeine cache configuration behavior.
package cache

import (
	"container/list"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Strategy string

const (
	AfterWrite       Strategy = "afterWrite"
	AfterAccess      Strategy = "afterAccess"
	LegacyAfterAcess Strategy = "afterAcess" // Java enum spelling.
)

type Spec struct {
	MaxSize  int
	Duration time.Duration
	Strategy Strategy
}

func DefaultSpec() Spec {
	return Spec{MaxSize: 50, Duration: time.Minute, Strategy: AfterWrite}
}

func (s Spec) validate() error {
	if s.MaxSize <= 0 {
		return errors.New("max size must be positive")
	}
	if s.Duration < 0 {
		return errors.New("duration cannot be negative")
	}
	if s.Strategy != "" && s.Strategy != AfterWrite && s.Strategy != AfterAccess && s.Strategy != LegacyAfterAcess {
		return fmt.Errorf("unknown expiry strategy %q", s.Strategy)
	}
	return nil
}

type EvictionListener[K comparable, V any] func(K, V)

type item[K comparable, V any] struct {
	key      K
	value    V
	written  time.Time
	accessed time.Time
}

type Cache[K comparable, V any] struct {
	mu      sync.Mutex
	spec    Spec
	order   *list.List
	entries map[K]*list.Element
	onEvict EvictionListener[K, V]
}

func New[K comparable, V any](spec Spec, listener EvictionListener[K, V]) (*Cache[K, V], error) {
	if err := spec.validate(); err != nil {
		return nil, err
	}
	return &Cache[K, V]{spec: spec, order: list.New(), entries: make(map[K]*list.Element), onEvict: listener}, nil
}

func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	e, found := c.entries[key]
	if !found {
		c.mu.Unlock()
		var zero V
		return zero, false
	}
	entry := e.Value.(*item[K, V])
	now := time.Now()
	if c.expired(entry, now) {
		c.remove(e)
		c.mu.Unlock()
		if c.onEvict != nil {
			c.onEvict(entry.key, entry.value)
		}
		var zero V
		return zero, false
	}
	entry.accessed = now
	c.order.MoveToFront(e)
	value := entry.value
	c.mu.Unlock()
	return value, true
}

// Put returns the prior value and whether the key was present.
func (c *Cache[K, V]) Put(key K, value V) (V, bool) {
	c.mu.Lock()
	now := time.Now()
	if e, found := c.entries[key]; found {
		entry := e.Value.(*item[K, V])
		if c.expired(entry, now) {
			c.remove(e)
			c.entries[key] = c.order.PushFront(&item[K, V]{key: key, value: value, written: now, accessed: now})
			c.mu.Unlock()
			if c.onEvict != nil {
				c.onEvict(entry.key, entry.value)
			}
			var zero V
			return zero, false
		}
		prior := entry.value
		entry.value, entry.written, entry.accessed = value, now, now
		c.order.MoveToFront(e)
		c.mu.Unlock()
		return prior, true
	}
	entry := &item[K, V]{key: key, value: value, written: now, accessed: now}
	c.entries[key] = c.order.PushFront(entry)
	var evicted *item[K, V]
	if c.order.Len() > c.spec.MaxSize {
		evicted = c.order.Back().Value.(*item[K, V])
		c.remove(c.order.Back())
	}
	c.mu.Unlock()
	if evicted != nil && c.onEvict != nil {
		c.onEvict(evicted.key, evicted.value)
	}
	var zero V
	return zero, false
}

func (c *Cache[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}

func (c *Cache[K, V]) expired(entry *item[K, V], now time.Time) bool {
	if c.spec.Duration == 0 {
		return false
	}
	base := entry.written
	if c.spec.Strategy == AfterAccess || c.spec.Strategy == LegacyAfterAcess {
		base = entry.accessed
	}
	return now.Sub(base) >= c.spec.Duration
}

func (c *Cache[K, V]) remove(e *list.Element) {
	delete(c.entries, e.Value.(*item[K, V]).key)
	c.order.Remove(e)
}

type Manager[K comparable, V any] struct{ caches map[string]*Cache[K, V] }

func NewManager[K comparable, V any](specs map[string]Spec) (*Manager[K, V], error) {
	m := &Manager[K, V]{caches: make(map[string]*Cache[K, V], len(specs))}
	for name, spec := range specs {
		if name == "" {
			return nil, errors.New("cache name is empty")
		}
		c, err := New[K, V](spec, nil)
		if err != nil {
			return nil, fmt.Errorf("cache %s: %w", name, err)
		}
		m.caches[name] = c
	}
	return m, nil
}

func (m *Manager[K, V]) Cache(name string) (*Cache[K, V], bool) {
	c, ok := m.caches[name]
	return c, ok
}
