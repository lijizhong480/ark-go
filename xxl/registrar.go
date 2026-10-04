package xxl

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Registrar renews and removes the executor registration. Start it only after
// the callback transport is listening at Address.
type Registrar struct {
	Admin            AdminClient
	AppName, Address string
	Interval         time.Duration
	mu               sync.Mutex
	cancel           context.CancelFunc
	done             chan struct{}
}

func (r *Registrar) Start(ctx context.Context) error {
	if r.AppName == "" || r.Address == "" {
		return errors.New("xxl: app name and callback address required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done != nil {
		return nil
	}
	interval := r.Interval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	child, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.done = make(chan struct{})
	done := r.done
	go func() {
		defer close(done)
		_ = r.Admin.Register(child, r.AppName, r.Address)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-child.Done():
				return
			case <-ticker.C:
				_ = r.Admin.Register(child, r.AppName, r.Address)
			}
		}
	}()
	return nil
}
func (r *Registrar) Stop(ctx context.Context) error {
	r.mu.Lock()
	cancel := r.cancel
	done := r.done
	r.cancel = nil
	r.done = nil
	r.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	<-done
	return r.Admin.Unregister(ctx, r.AppName, r.Address)
}
