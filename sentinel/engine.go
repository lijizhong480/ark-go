package sentinel

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type BlockKind string

const (
	FlowBlocked    BlockKind = "flow"
	HotBlocked     BlockKind = "hot"
	DegradeBlocked BlockKind = "degrade"
)

type Block struct {
	Kind     BlockKind
	Resource string
}

func (b *Block) Error() string { return fmt.Sprintf("%s blocked: %s", b.Kind, b.Resource) }

type Identity struct {
	UID      int64
	DeviceID string
}

// HotResources reproduces ArkExtendHotSlot's four name/value pairs. Empty DID
// and non-positive UID have no value and cannot trigger a hot rule.
func HotResources(path string, identity Identity) map[string]string {
	result := map[string]string{}
	if identity.UID > 0 {
		uid := strconv.FormatInt(identity.UID, 10)
		result["hot_uid"] = uid
		result["uid_"+path] = uid
	}
	if strings.TrimSpace(identity.DeviceID) != "" {
		result["hot_did"] = identity.DeviceID
		result["did_"+path] = identity.DeviceID
	}
	return result
}

type rollingWindow struct{ buckets map[int64]int }
type hotTokens struct {
	lastAdd   time.Time
	remaining int64
}
type queuedFlow struct{ latest time.Time }
type breaker struct {
	start, openUntil              time.Time
	total, errors, slow           int
	open, halfOpen, probeInFlight bool
}

type Engine struct {
	mu         sync.Mutex
	store      *Store
	now        func() time.Time
	flow       map[string]*rollingWindow
	active     map[string]int
	hot        map[string]*hotTokens
	degrade    map[string]*breaker
	queue      map[string]*queuedFlow
	generation uint64
}

func NewEngine() *Engine {
	return &Engine{store: NewStore(), now: time.Now, flow: map[string]*rollingWindow{}, active: map[string]int{}, hot: map[string]*hotTokens{}, degrade: map[string]*breaker{}, queue: map[string]*queuedFlow{}}
}

func (e *Engine) Update(flowJSON, hotJSON, degradeJSON string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.store.Update(flowJSON, hotJSON, degradeJSON); err != nil {
		return err
	}
	e.flow = map[string]*rollingWindow{}
	e.hot = map[string]*hotTokens{}
	e.degrade = map[string]*breaker{}
	e.queue = map[string]*queuedFlow{}
	e.generation++
	return nil
}

// Admit is a limited local equivalent of Sentinel's default reject strategy.
// Call Done exactly once for admitted requests to release concurrency permits
// and report outcome to local circuit breakers.
func (e *Engine) Admit(path string, identity Identity) (Done func(success bool, duration time.Duration), blocked *Block) {
	e.mu.Lock()
	defer e.mu.Unlock()
	rules := e.store.Snapshot()
	generation := e.generation
	now := e.now()
	hotValues := HotResources(path, identity)
	probes := []string{}
	type queueReservation struct {
		key       string
		scheduled time.Time
	}
	reservations := []queueReservation{}
	waitUntil := now
	for i, rule := range rules.Degrade {
		if rule.Resource != path {
			continue
		}
		key := fmt.Sprintf("degrade:%d:%s", i, rule.Resource)
		state := e.degrade[key]
		if state != nil && ((state.open && now.Before(state.openUntil)) || (state.halfOpen && state.probeInFlight)) {
			return nil, &Block{Kind: DegradeBlocked, Resource: rule.Resource}
		}
		if state != nil && state.open && !now.Before(state.openUntil) {
			probes = append(probes, key)
		}
	}
	for i, rule := range rules.Flow {
		if rule.Resource != path && !(rule.Resource == NoLoginResource && identity.UID <= 0) {
			continue
		}
		if rule.Grade == 0 {
			if float64(e.active[path]+1) > rule.Count {
				return nil, &Block{Kind: FlowBlocked, Resource: rule.Resource}
			}
			continue
		}
		if rule.ControlBehavior == 2 {
			key := fmt.Sprintf("flow:%d:%s", i, rule.Resource)
			latest := time.Time{}
			if state := e.queue[key]; state != nil {
				latest = state.latest
			}
			scheduled, accepted := queueScheduled(latest, now, rule.Count, time.Duration(rule.MaxQueueingTimeMs)*time.Millisecond)
			if !accepted {
				return nil, &Block{Kind: FlowBlocked, Resource: rule.Resource}
			}
			reservations = append(reservations, queueReservation{key: key, scheduled: scheduled})
			if scheduled.After(waitUntil) {
				waitUntil = scheduled
			}
			continue
		}
		if !canFlow(e.flow, path, now, rule.Count) {
			return nil, &Block{Kind: FlowBlocked, Resource: rule.Resource}
		}
	}
	for i, rule := range rules.Hot {
		value := hotValues[rule.Resource]
		if value == "" {
			continue
		}
		key := fmt.Sprintf("hot:%d:%s:%s", i, rule.Resource, value)
		if !allowHot(e.hot, key, now, time.Duration(rule.DurationInSec)*time.Second, int64(rule.Count)) {
			return nil, &Block{Kind: HotBlocked, Resource: rule.Resource}
		}
	}
	for _, reservation := range reservations {
		e.queue[reservation.key] = &queuedFlow{latest: waitUntil}
	}
	if delay := waitUntil.Sub(now); delay > 0 {
		e.mu.Unlock()
		time.Sleep(delay)
		e.mu.Lock()
		if generation != e.generation {
			return nil, &Block{Kind: FlowBlocked, Resource: path}
		}
		now = e.now()
	}
	recordFlow(e.flow, path, now)
	e.active[path]++
	for _, key := range probes {
		state := e.degrade[key]
		state.open = false
		state.halfOpen = true
		state.probeInFlight = true
	}
	var once sync.Once
	return func(success bool, duration time.Duration) {
		once.Do(func() {
			e.mu.Lock()
			defer e.mu.Unlock()
			e.active[path]--
			if generation == e.generation {
				e.reportDegrade(rules.Degrade, path, e.now(), success, duration, probes)
			}
		})
	}, nil
}

func queueScheduled(latest, now time.Time, count float64, maxWait time.Duration) (time.Time, bool) {
	interval := time.Duration(math.Round(float64(time.Second) / count))
	if interval < 1 {
		interval = 1
	}
	scheduled := now
	if !latest.IsZero() && latest.Add(interval).After(now) {
		scheduled = latest.Add(interval)
	}
	return scheduled, scheduled.Sub(now) <= maxWait
}

// Sentinel's default StatisticNode has a 1000 ms interval with two 500 ms
// samples. A sample expires when its aligned start is 1000 ms old.
func canFlow(states map[string]*rollingWindow, key string, now time.Time, limit float64) bool {
	state := states[key]
	if state == nil {
		state = &rollingWindow{buckets: map[int64]int{}}
		states[key] = state
	}
	ms := now.UnixMilli()
	used := 0
	for bucket, count := range state.buckets {
		if ms-bucket >= 1000 {
			delete(state.buckets, bucket)
		} else {
			used += count
		}
	}
	return float64(used+1) <= limit
}

func recordFlow(states map[string]*rollingWindow, key string, now time.Time) {
	if states[key] == nil {
		states[key] = &rollingWindow{buckets: map[int64]int{}}
	}
	start := now.UnixMilli()
	start -= start % 500
	states[key].buckets[start]++
}

// ParamFlowChecker uses a token counter keyed by rule and parameter value.
// It refills only after elapsed time is strictly greater than duration.
func allowHot(states map[string]*hotTokens, key string, now time.Time, duration time.Duration, limit int64) bool {
	if limit <= 0 || duration <= 0 {
		return false
	}
	state := states[key]
	if state == nil {
		states[key] = &hotTokens{lastAdd: now, remaining: limit - 1}
		return true
	}
	elapsed := now.Sub(state.lastAdd)
	if elapsed > duration {
		refill := int64(elapsed) * limit / int64(duration)
		available := state.remaining + refill
		if available > limit {
			available = limit
		}
		if available < 1 {
			return false
		}
		state.lastAdd = now
		state.remaining = available - 1
		return true
	}
	if state.remaining < 1 {
		return false
	}
	state.remaining--
	return true
}

func (e *Engine) reportDegrade(rules []DegradeRule, path string, now time.Time, success bool, duration time.Duration, probes []string) {
	probeSet := map[string]bool{}
	for _, key := range probes {
		probeSet[key] = true
	}
	for i, rule := range rules {
		if rule.Resource != path {
			continue
		}
		key := fmt.Sprintf("degrade:%d:%s", i, path)
		state := e.degrade[key]
		period := time.Duration(rule.StatIntervalMs) * time.Millisecond
		if period <= 0 {
			period = time.Second
		}
		if state == nil {
			state = &breaker{start: now.Truncate(period)}
			e.degrade[key] = state
		}
		if probeSet[key] {
			state.probeInFlight = false
			state.halfOpen = false
			probePassed := (rule.Grade == 0 && duration <= time.Duration(rule.Count*float64(time.Millisecond))) || (rule.Grade != 0 && success)
			if probePassed {
				state.start = now.Truncate(period)
				state.total, state.errors, state.slow = 0, 0, 0
			} else {
				state.open = true
				state.openUntil = now.Add(time.Duration(rule.TimeWindow) * time.Second)
			}
			continue
		}
		if !state.start.Equal(now.Truncate(period)) {
			state.start = now.Truncate(period)
			state.total, state.errors, state.slow = 0, 0, 0
		}
		if state.open || state.halfOpen {
			continue
		}
		state.total++
		if !success {
			state.errors++
		}
		if duration > time.Duration(rule.Count*float64(time.Millisecond)) {
			state.slow++
		}
		minimum := rule.MinRequestAmount
		if minimum <= 0 {
			minimum = 5
		}
		if state.total < minimum {
			continue
		}
		trip := false
		switch rule.Grade {
		case 0:
			trip = float64(state.slow)/float64(state.total) > rule.SlowRatioThreshold || (rule.SlowRatioThreshold == 1 && state.slow == state.total)
		case 1:
			trip = float64(state.errors)/float64(state.total) > rule.Count
		case 2:
			trip = float64(state.errors) > rule.Count
		}
		if trip {
			state.open = true
			state.openUntil = now.Add(time.Duration(rule.TimeWindow) * time.Second)
		}
	}
}

func WriteFallback(w http.ResponseWriter, block *Block) error {
	code, message := 100410, "Request are too frequent, please try again later"
	if block != nil && block.Kind == DegradeBlocked {
		code, message = 100503, "Request degraded"
	}
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	return json.NewEncoder(w).Encode(struct {
		Code      int    `json:"code"`
		Msg       string `json:"msg"`
		Timestamp int64  `json:"timestamp"`
	}{code, message, time.Now().UnixMilli()})
}
