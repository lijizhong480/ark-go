package xxl

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

type Handler func(context.Context, string) ReturnT[string]
type Lifecycle struct {
	Init    func(context.Context) error
	Destroy func(context.Context) error
}
type Metric struct {
	JobName  string
	JobID    int
	Code     int
	Success  bool
	Duration time.Duration
}
type MetricHook func(Metric)
type CallbackHook func(context.Context, []CallbackParam) error

type handlerEntry struct {
	run       Handler
	lifecycle Lifecycle
}
type queuedJob struct {
	trigger TriggerParam
	handler handlerEntry
}
type jobState struct {
	cancel            context.CancelFunc
	queue             []queuedJob
	running           bool
	kind, handlerName string
	version           int64
	queuedLogIDs      map[int64]bool
	stopReason        string
}
type logKey struct{ id, date int64 }
type logState struct {
	lines []string
	done  bool
}
type Executor struct {
	mu                  sync.Mutex
	handlers            map[string]handlerEntry
	handlerOrder        []string
	initialized         []handlerEntry
	jobs                map[int]*jobState
	logs                map[logKey]*logState
	started, closed     bool
	starting, destroyed bool
	stopDone            chan struct{}
	stopErr             error
	metric              MetricHook
	callback            CallbackHook
	active              sync.WaitGroup
	callbackFailures    []CallbackFailure
	logDir              string
	scriptDir           string
}

type CallbackFailure struct {
	Param    CallbackParam
	Attempts int
	Error    string
}

func NewExecutor(metric MetricHook, callback CallbackHook) *Executor {
	return &Executor{handlers: map[string]handlerEntry{}, jobs: map[int]*jobState{}, logs: map[logKey]*logState{}, metric: metric, callback: callback}
}
func (e *Executor) Register(name string, handler Handler, lifecycle Lifecycle) error {
	name = strings.TrimSpace(name)
	if name == "" || handler == nil {
		return errors.New("xxl: handler name and function required")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.started || e.starting || e.closed {
		return errors.New("xxl: register handlers before start")
	}
	if _, exists := e.handlers[name]; exists {
		return fmt.Errorf("xxl: duplicate handler %q", name)
	}
	e.handlers[name] = handlerEntry{run: handler, lifecycle: lifecycle}
	e.handlerOrder = append(e.handlerOrder, name)
	return nil
}

func callLifecycle(ctx context.Context, hook func(context.Context) error) (err error) {
	if hook == nil {
		return nil
	}
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("xxl: lifecycle panic: %v", value)
		}
	}()
	return hook(ctx)
}

func (e *Executor) Start(ctx context.Context) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return errors.New("xxl: executor closed")
	}
	if e.started {
		e.mu.Unlock()
		return nil
	}
	if e.starting {
		e.mu.Unlock()
		return errors.New("xxl: executor starting")
	}
	e.starting = true
	handlers := make([]handlerEntry, 0, len(e.handlerOrder))
	for _, name := range e.handlerOrder {
		handlers = append(handlers, e.handlers[name])
	}
	e.mu.Unlock()
	initialized := []handlerEntry{}
	for _, handler := range handlers {
		if err := callLifecycle(ctx, handler.lifecycle.Init); err != nil {
			for i := len(initialized) - 1; i >= 0; i-- {
				_ = callLifecycle(ctx, initialized[i].lifecycle.Destroy)
			}
			e.mu.Lock()
			e.starting = false
			e.mu.Unlock()
			return err
		}
		initialized = append(initialized, handler)
	}
	e.mu.Lock()
	e.started = true
	e.starting = false
	e.initialized = initialized
	e.mu.Unlock()
	return nil
}
func (e *Executor) Beat() ReturnT[string] { return success("") }
func (e *Executor) IdleBeat(jobID int) ReturnT[string] {
	e.mu.Lock()
	defer e.mu.Unlock()
	state := e.jobs[jobID]
	if state != nil && (state.running || len(state.queue) > 0) {
		return failure[string]("job thread is running or has trigger queue.")
	}
	return success("")
}
func (e *Executor) Run(trigger TriggerParam) ReturnT[string] {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.started || e.closed {
		return failure[string]("executor not running")
	}
	var handler handlerEntry
	switch trigger.GlueType {
	case "BEAN":
		var ok bool
		handler, ok = e.handlers[trigger.ExecutorHandler]
		if !ok {
			return failure[string](fmt.Sprintf("job handler [%s] not found.", trigger.ExecutorHandler))
		}
	case "GLUE_SHELL", "GLUE_PYTHON", "GLUE_PHP", "GLUE_NODEJS", "GLUE_POWERSHELL":
		handler = handlerEntry{run: e.scriptHandler(trigger)}
	case "GLUE_GROOVY":
		return failure[string]("GLUE_GROOVY requires a JVM executor")
	default:
		return failure[string](fmt.Sprintf("glueType[%s] is not valid.", trigger.GlueType))
	}
	state := e.jobs[trigger.JobID]
	if state == nil {
		state = &jobState{queuedLogIDs: map[int64]bool{}}
		e.jobs[trigger.JobID] = state
	}
	if state.kind != "" && (state.kind != trigger.GlueType || state.version != trigger.GlueUpdatetime || state.handlerName != trigger.ExecutorHandler && trigger.GlueType == "BEAN") {
		if state.cancel != nil {
			state.cancel()
			state.stopReason = "change job source or glue type, and terminate the old job thread."
		}
		e.dropQueued(state, "change job source or glue type, and terminate the old job thread.")
		state.queue = nil
		clear(state.queuedLogIDs)
	}
	state.kind, state.version, state.handlerName = trigger.GlueType, trigger.GlueUpdatetime, trigger.ExecutorHandler
	if state.running || len(state.queue) > 0 {
		switch trigger.ExecutorBlockStrategy {
		case "DISCARD_LATER":
			return failure[string]("block strategy effect：Discard Later")
		case "COVER_EARLY":
			if state.cancel != nil {
				state.cancel()
				state.stopReason = "block strategy effect：Cover Early"
			}
			e.dropQueued(state, "block strategy effect：Cover Early")
			state.queue = nil
			clear(state.queuedLogIDs)
		}
	}
	if state.queuedLogIDs[trigger.LogID] {
		return failure[string](fmt.Sprintf("repeate trigger job, logId:%d", trigger.LogID))
	}
	state.queuedLogIDs[trigger.LogID] = true
	state.queue = append(state.queue, queuedJob{trigger: trigger, handler: handler})
	if !state.running {
		state.running = true
		e.active.Add(1)
		go e.executeLoop(trigger.JobID)
	}
	return success("")
}
func (e *Executor) executeLoop(jobID int) {
	defer e.active.Done()
	for {
		e.mu.Lock()
		state := e.jobs[jobID]
		if state == nil || len(state.queue) == 0 || e.closed {
			if state != nil {
				state.running = false
			}
			e.mu.Unlock()
			return
		}
		queued := state.queue[0]
		state.queue = state.queue[1:]
		trigger := queued.trigger
		delete(state.queuedLogIDs, trigger.LogID)
		handler := queued.handler
		ctx, cancel := context.WithCancel(context.Background())
		if trigger.ExecutorTimeout > 0 {
			ctx, cancel = context.WithTimeout(context.Background(), time.Duration(trigger.ExecutorTimeout)*time.Second)
		}
		state.cancel = cancel
		key := logKey{trigger.LogID, trigger.LogDateTime}
		ctx = context.WithValue(ctx, jobLogContext{}, jobLogRef{executor: e, key: key})
		e.logs[key] = &logState{}
		e.mu.Unlock()
		start := time.Now()
		result := ReturnT[string]{Code: FailCode}
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					result = failure[string](fmt.Sprint(recovered))
				}
			}()
			result = handler.run(ctx, trigger.ExecutorParams)
		}()
		if ctx.Err() != nil && result.Code == SuccessCode {
			result = failure[string](ctx.Err().Error())
		}
		cancel()
		e.mu.Lock()
		if state.stopReason != "" {
			result = failure[string](state.stopReason + " [job running, killed]")
			state.stopReason = ""
		}
		state.cancel = nil
		e.mu.Unlock()
		e.appendJobLog(key, result.Msg)
		e.mu.Lock()
		e.logs[key].done = true
		e.mu.Unlock()
		e.reportMetric(Metric{JobName: trigger.ExecutorHandler, JobID: trigger.JobID, Code: result.Code, Success: result.Code == SuccessCode, Duration: time.Since(start)})
		if e.callback != nil {
			param := CallbackParam{LogID: trigger.LogID, LogDateTim: trigger.LogDateTime, ExecuteResult: result}
			if err := e.reportCallback(context.Background(), param); err != nil {
				e.mu.Lock()
				e.callbackFailures = append(e.callbackFailures, CallbackFailure{Param: param, Attempts: 3, Error: err.Error()})
				e.mu.Unlock()
			}
		}
	}
}
func (e *Executor) Kill(jobID int) ReturnT[string] {
	e.mu.Lock()
	defer e.mu.Unlock()
	state := e.jobs[jobID]
	if state == nil || !state.running {
		return ReturnT[string]{Code: SuccessCode, Msg: "job thread already killed."}
	}
	if state.cancel != nil {
		state.cancel()
		state.stopReason = "scheduling center kill job."
	}
	e.dropQueued(state, "scheduling center kill job.")
	state.queue = nil
	clear(state.queuedLogIDs)
	return success("")
}
func (e *Executor) Log(param LogParam) ReturnT[LogResult] {
	if result, ok := e.readFileLog(param); ok {
		return success(result)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	state := e.logs[logKey{param.LogID, param.LogDateTime}]
	if state == nil {
		return success(LogResult{FromLineNum: param.FromLineNum, ToLineNum: param.FromLineNum, IsEnd: true})
	}
	start := param.FromLineNum - 1
	if start < 0 {
		start = 0
	}
	if start > len(state.lines) {
		start = len(state.lines)
	}
	return success(LogResult{FromLineNum: param.FromLineNum, ToLineNum: len(state.lines), LogContent: strings.Join(state.lines[start:], "\n"), IsEnd: state.done})
}
func (e *Executor) Stop(ctx context.Context) error {
	e.mu.Lock()
	if !e.closed {
		if e.starting {
			e.mu.Unlock()
			return errors.New("xxl: executor starting")
		}
		e.closed = true
		for _, job := range e.jobs {
			if job.cancel != nil {
				job.cancel()
				job.stopReason = "executor stop"
			}
			e.dropQueued(job, "executor stop")
			job.queue = nil
			clear(job.queuedLogIDs)
		}
	}
	if e.destroyed {
		err := e.stopErr
		e.mu.Unlock()
		return err
	}
	if e.stopDone != nil {
		done := e.stopDone
		e.mu.Unlock()
		select {
		case <-done:
			e.mu.Lock()
			err := e.stopErr
			e.mu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	e.mu.Unlock()
	done := make(chan struct{})
	go func() { e.active.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	e.mu.Lock()
	if e.destroyed {
		err := e.stopErr
		e.mu.Unlock()
		return err
	}
	if e.stopDone != nil {
		done := e.stopDone
		e.mu.Unlock()
		select {
		case <-done:
			e.mu.Lock()
			err := e.stopErr
			e.mu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	e.stopDone = make(chan struct{})
	e.destroyed = true
	handlers := append([]handlerEntry(nil), e.initialized...)
	e.mu.Unlock()
	var first error
	for _, handler := range handlers {
		if err := callLifecycle(ctx, handler.lifecycle.Destroy); err != nil && first == nil {
			first = err
		}
	}
	e.cleanScriptDir()
	e.mu.Lock()
	e.stopErr = first
	close(e.stopDone)
	e.mu.Unlock()
	return first
}

func (e *Executor) reportMetric(metric Metric) {
	if e.metric == nil {
		return
	}
	defer func() { _ = recover() }()
	e.metric(metric)
}

func (e *Executor) reportCallback(ctx context.Context, param CallbackParam) error {
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		func() {
			defer func() {
				if p := recover(); p != nil {
					last = fmt.Errorf("callback panic: %v", p)
				}
			}()
			callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			last = e.callback(callCtx, []CallbackParam{param})
		}()
		if last == nil {
			return nil
		}
		if attempt < 2 {
			timer := time.NewTimer(time.Duration(1<<attempt) * 100 * time.Millisecond)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			}
		}
	}
	return last
}

// FailedCallbacks returns in-memory results that could not reach the admin.
func (e *Executor) FailedCallbacks() []CallbackFailure {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]CallbackFailure(nil), e.callbackFailures...)
}

// RetryFailedCallbacks redelivers retained results; failures remain available.
func (e *Executor) RetryFailedCallbacks(ctx context.Context) {
	e.mu.Lock()
	pending := e.callbackFailures
	e.callbackFailures = nil
	e.mu.Unlock()
	for _, failure := range pending {
		if err := e.reportCallback(ctx, failure.Param); err != nil {
			failure.Attempts += 3
			failure.Error = err.Error()
			e.mu.Lock()
			e.callbackFailures = append(e.callbackFailures, failure)
			e.mu.Unlock()
		}
	}
}

// dropQueued is called with e.mu held. Java sends a failure callback for each
// trigger left in a stopped JobThread queue.
func (e *Executor) dropQueued(state *jobState, reason string) {
	if e.callback == nil {
		return
	}
	for _, job := range state.queue {
		param := CallbackParam{LogID: job.trigger.LogID, LogDateTim: job.trigger.LogDateTime, ExecuteResult: failure[string](reason + " [job not executed, in the job queue, killed.]")}
		e.active.Add(1)
		go func() {
			defer e.active.Done()
			if err := e.reportCallback(context.Background(), param); err != nil {
				e.mu.Lock()
				e.callbackFailures = append(e.callbackFailures, CallbackFailure{Param: param, Attempts: 3, Error: err.Error()})
				e.mu.Unlock()
			}
		}()
	}
}
