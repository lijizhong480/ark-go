package executor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type RejectPolicy string

const (
	Abort         RejectPolicy = "ABORT"
	CallerRuns    RejectPolicy = "CALLER_RUNS"
	DiscardOldest RejectPolicy = "DISCARD_OLDEST"
	Discard       RejectPolicy = "DISCARD"
)

var ErrRejected = errors.New("executor task rejected")
var ErrClosed = errors.New("executor closed")

// Config maps ThreadPoolConfigProperties.ThreadPoolConfig. NamePrefix is
// retained for pool identification; Go goroutines have no thread name.
type Config struct {
	CoreSize         int
	MaxSize          int
	QueueCapacity    int
	RejectHandler    RejectPolicy
	AwaitTermination time.Duration
	KeepAlive        time.Duration
	NamePrefix       string
}

type Task func(context.Context)

type job struct {
	ctx  context.Context
	task Task
}

type Pool struct {
	mu          sync.Mutex
	config      Config
	queue       chan job
	workers     int
	closed      bool
	workersDone chan struct{}
	wg          sync.WaitGroup
}

func New(config Config) (*Pool, error) {
	if config.CoreSize < 0 || config.MaxSize < 1 || config.CoreSize > config.MaxSize || config.QueueCapacity < 0 {
		return nil, fmt.Errorf("invalid executor sizes: core=%d max=%d queue=%d", config.CoreSize, config.MaxSize, config.QueueCapacity)
	}
	if config.KeepAlive < 0 || config.AwaitTermination < 0 {
		return nil, errors.New("executor durations must not be negative")
	}
	switch config.RejectHandler {
	case Abort, CallerRuns, DiscardOldest, Discard:
	default:
		return nil, fmt.Errorf("unknown reject handler %q", config.RejectHandler)
	}
	p := &Pool{config: config, queue: make(chan job, config.QueueCapacity), workersDone: make(chan struct{})}
	for i := 0; i < config.CoreSize; i++ {
		p.spawn(nil)
	}
	return p, nil
}

// Submit carries the caller's context, including request and trace values,
// into the task. Queued tasks with canceled contexts are skipped.
func (p *Pool) Submit(ctx context.Context, task Task) error {
	if ctx == nil || task == nil {
		return errors.New("context and task are required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	j := job{ctx: ctx, task: task}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrClosed
	}
	if p.workers == 0 {
		p.spawn(&j)
		p.mu.Unlock()
		return nil
	}
	select {
	case p.queue <- j:
		p.mu.Unlock()
		return nil
	default:
	}
	if p.workers < p.config.MaxSize {
		p.spawn(&j)
		p.mu.Unlock()
		return nil
	}
	switch p.config.RejectHandler {
	case Abort:
		p.mu.Unlock()
		return ErrRejected
	case Discard:
		p.mu.Unlock()
		return nil
	case DiscardOldest:
		select {
		case <-p.queue:
		default:
		}
		select {
		case p.queue <- j:
			p.mu.Unlock()
			return nil
		default:
			p.mu.Unlock()
			return ErrRejected
		}
	case CallerRuns:
		p.mu.Unlock()
		if ctx.Err() == nil {
			task(ctx)
		}
		return ctx.Err()
	default:
		p.mu.Unlock()
		return ErrRejected
	}
}

// spawn must be called with p.mu held, except during New before publication.
func (p *Pool) spawn(initial *job) {
	p.workers++
	p.wg.Add(1)
	go p.worker(initial)
}

func (p *Pool) worker(initial *job) {
	defer p.wg.Done()
	if initial != nil {
		run(*initial)
	}
	for {
		p.mu.Lock()
		extra := p.workers > p.config.CoreSize
		keepAlive := p.config.KeepAlive
		p.mu.Unlock()
		if !extra || keepAlive == 0 {
			j, ok := <-p.queue
			if !ok {
				p.workerExited()
				return
			}
			run(j)
			continue
		}
		timer := time.NewTimer(keepAlive)
		select {
		case j, ok := <-p.queue:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			if !ok {
				p.workerExited()
				return
			}
			run(j)
		case <-timer.C:
			p.mu.Lock()
			if !p.closed && p.workers > p.config.CoreSize {
				p.workers--
				p.mu.Unlock()
				return
			}
			p.mu.Unlock()
		}
	}
}

func (p *Pool) workerExited() {
	p.mu.Lock()
	p.workers--
	p.mu.Unlock()
}

func run(j job) {
	if j.ctx.Err() == nil {
		j.task(j.ctx)
	}
}

// Shutdown closes submission, drains queued tasks and waits for workers.
// If ctx expires, workers continue draining in the background.
func (p *Pool) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.queue)
		go func() { p.wg.Wait(); close(p.workersDone) }()
	}
	done := p.workersDone
	p.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
