// Package config merges ordered configuration sources and publishes immutable
// snapshots for services that need dynamic updates.
package config

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type Source interface {
	Load(context.Context) (map[string]string, error)
}

// Load merges sources left to right; later sources override earlier ones.
func Load(ctx context.Context, sources ...Source) (Snapshot, error) {
	values := make(map[string]string)
	for i, source := range sources {
		if source == nil {
			return Snapshot{}, fmt.Errorf("configuration source %d is nil", i)
		}
		entries, err := source.Load(ctx)
		if err != nil {
			return Snapshot{}, fmt.Errorf("configuration source %d: %w", i, err)
		}
		for key, value := range entries {
			values[key] = value
		}
	}
	return Snapshot{values: values}, nil
}

type Snapshot struct{ values map[string]string }

func (s Snapshot) Lookup(key string) (string, bool) { value, ok := s.values[key]; return value, ok }

func (s Snapshot) String(key, fallback string) string {
	if value, ok := s.Lookup(key); ok {
		return value
	}
	return fallback
}

func (s Snapshot) Bool(key string, fallback bool) (bool, error) {
	value, ok := s.Lookup(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("configuration key %q: %w", key, err)
	}
	return parsed, nil
}

func (s Snapshot) Int(key string, fallback int) (int, error) {
	value, ok := s.Lookup(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("configuration key %q: %w", key, err)
	}
	return parsed, nil
}

func (s Snapshot) Required(keys ...string) error {
	for _, key := range keys {
		value, ok := s.Lookup(key)
		if !ok || strings.TrimSpace(value) == "" {
			return fmt.Errorf("required configuration key %q is missing", key)
		}
	}
	return nil
}

func (s Snapshot) Map() map[string]string {
	result := make(map[string]string, len(s.values))
	for key, value := range s.values {
		result[key] = value
	}
	return result
}

type Store struct {
	current  atomic.Pointer[Snapshot]
	mu       sync.Mutex
	watchers map[chan Snapshot]struct{}
}

func NewStore(initial Snapshot) *Store {
	s := &Store{watchers: make(map[chan Snapshot]struct{})}
	s.Publish(initial)
	return s
}

func (s *Store) Current() Snapshot {
	p := s.current.Load()
	if p == nil {
		return Snapshot{values: map[string]string{}}
	}
	return Snapshot{values: p.Map()}
}

func (s *Store) Publish(next Snapshot) {
	copySnapshot := Snapshot{values: next.Map()}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.current.Store(&copySnapshot)
	for ch := range s.watchers {
		select {
		case ch <- copySnapshot:
		default:
			// Keep the latest state rather than blocking a remote config callback.
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- copySnapshot:
			default:
			}
		}
	}
}

func (s *Store) Watch(ctx context.Context) (<-chan Snapshot, error) {
	if ctx == nil {
		return nil, errors.New("watch context is nil")
	}
	ch := make(chan Snapshot, 1)
	s.mu.Lock()
	s.watchers[ch] = struct{}{}
	s.mu.Unlock()
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		delete(s.watchers, ch)
		close(ch)
		s.mu.Unlock()
	}()
	return ch, nil
}
