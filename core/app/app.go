package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Component is explicitly assembled by the service, in startup order.
type Component interface {
	Name() string
	Start(context.Context) error
	Stop(context.Context) error
}

type App struct {
	mu              sync.Mutex
	components      []Component
	started         []Component
	running         bool
	starting        bool
	stopping        bool
	rollbackTimeout time.Duration
}

func New(components ...Component) (*App, error) {
	seen := make(map[string]bool, len(components))
	for i, c := range components {
		if c == nil {
			return nil, fmt.Errorf("component %d is nil", i)
		}
		name := c.Name()
		if name == "" {
			return nil, fmt.Errorf("component %d has empty name", i)
		}
		if seen[name] {
			return nil, fmt.Errorf("duplicate component name %q", name)
		}
		seen[name] = true
	}
	return &App{components: append([]Component(nil), components...), rollbackTimeout: 5 * time.Second}, nil
}

func (a *App) Running() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.running
}

// Start runs components in order. On failure, it stops the failed component
// (which may be partially initialized), then prior components in reverse order.
func (a *App) Start(ctx context.Context) error {
	a.mu.Lock()
	if a.running || a.starting || a.stopping {
		a.mu.Unlock()
		return errors.New("app already started or starting")
	}
	a.starting = true
	a.mu.Unlock()
	started := make([]Component, 0, len(a.components))
	for _, c := range a.components {
		if err := ctx.Err(); err != nil {
			return a.rollback(ctx, started, nil, err)
		}
		if err := c.Start(ctx); err != nil {
			return a.rollback(ctx, started, c, fmt.Errorf("start %s: %w", c.Name(), err))
		}
		started = append(started, c)
	}
	a.mu.Lock()
	a.started = started
	a.running = true
	a.starting = false
	a.mu.Unlock()
	return nil
}

func (a *App) rollback(ctx context.Context, started []Component, failed Component, cause error) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), a.rollbackTimeout)
	defer cancel()
	if failed != nil {
		started = append(started, failed)
	}
	stopErr := stopReverse(cleanupCtx, started)
	a.mu.Lock()
	a.starting = false
	a.mu.Unlock()
	return errors.Join(cause, stopErr)
}

// Stop is idempotent and closes components in reverse startup order.
func (a *App) Stop(ctx context.Context) error {
	a.mu.Lock()
	if a.starting {
		a.mu.Unlock()
		return errors.New("app is starting")
	}
	if a.stopping {
		a.mu.Unlock()
		return errors.New("app is stopping")
	}
	if !a.running {
		a.mu.Unlock()
		return nil
	}
	started := a.started
	a.started = nil
	a.running = false
	a.stopping = true
	a.mu.Unlock()
	err := stopReverse(ctx, started)
	a.mu.Lock()
	a.stopping = false
	a.mu.Unlock()
	return err
}

func stopReverse(ctx context.Context, components []Component) error {
	var errs []error
	for i := len(components) - 1; i >= 0; i-- {
		if err := components[i].Stop(ctx); err != nil {
			errs = append(errs, fmt.Errorf("stop %s: %w", components[i].Name(), err))
		}
	}
	return errors.Join(errs...)
}
