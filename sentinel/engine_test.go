package sentinel

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestApolloDefaultsAndAtomicValidation(t *testing.T) {
	e := NewEngine()
	if Namespace("orders") != "orders-sentinel" {
		t.Fatal("namespace mismatch")
	}
	if err := e.Update(`[{"resource":"/orders","count":1}]`, `[{"resource":"hot_uid","count":1}]`, `[]`); err != nil {
		t.Fatal(err)
	}
	s := e.store.Snapshot()
	if s.Flow[0].Grade != 1 || s.Hot[0].Grade != 1 || s.Hot[0].DurationInSec != 1 {
		t.Fatalf("Java defaults missing: %+v %+v", s.Flow[0], s.Hot[0])
	}
	if err := e.Update(`[{"resource":"/orders","count":1,"controlBehavior":1}]`, `[]`, `[]`); err == nil {
		t.Fatal("unsupported warm-up accepted")
	}
	if err := e.Update(`[]`, `[{"resource":"hot_uid","count":1,"paramFlowItemList":[{"object":"7","count":9}]}]`, `[]`); err == nil {
		t.Fatal("unsupported hot item override accepted")
	}
	if got := e.store.Snapshot(); len(got.Flow) != 1 || got.Flow[0].ControlBehavior != 0 {
		t.Fatal("failed update replaced active rules")
	}
	if level, ok := APILevel("/orders"); ok || level != "" {
		t.Fatal("placeholder API level invented")
	}
}

func TestFlowHotAndNoLoginResources(t *testing.T) {
	e := NewEngine()
	if err := e.Update(`[{"resource":"no_login_all","count":1},{"resource":"/orders","count":3}]`, `[{"resource":"hot_uid","count":1},{"resource":"did_/orders","count":1}]`, `[]`); err != nil {
		t.Fatal(err)
	}
	done, blocked := e.Admit("/orders", Identity{})
	if blocked != nil {
		t.Fatal(blocked)
	}
	done(true, 0)
	_, blocked = e.Admit("/orders", Identity{})
	if blocked == nil || blocked.Kind != FlowBlocked || blocked.Resource != NoLoginResource {
		t.Fatalf("no-login block = %v", blocked)
	}
	done, blocked = e.Admit("/orders", Identity{UID: 7, DeviceID: "D"})
	if blocked != nil {
		t.Fatal(blocked)
	}
	done(true, 0)
	_, blocked = e.Admit("/orders", Identity{UID: 7, DeviceID: "D"})
	if blocked == nil || blocked.Kind != HotBlocked || blocked.Resource != "hot_uid" {
		t.Fatalf("hot block = %v", blocked)
	}
	names := HotResources("/orders", Identity{UID: 7, DeviceID: "D"})
	if names["uid_/orders"] != "7" || names["did_/orders"] != "D" {
		t.Fatalf("resources = %v", names)
	}
}

func TestConcurrentFlowAndDegradeFallback(t *testing.T) {
	e := NewEngine()
	if err := e.Update(`[{"resource":"/slow","count":1,"grade":0}]`, `[]`, `[{"resource":"/slow","grade":2,"count":1,"timeWindow":1,"minRequestAmount":1}]`); err != nil {
		t.Fatal(err)
	}
	done, blocked := e.Admit("/slow", Identity{UID: 1})
	if blocked != nil {
		t.Fatal(blocked)
	}
	_, blocked = e.Admit("/slow", Identity{UID: 1})
	if blocked == nil || blocked.Kind != FlowBlocked {
		t.Fatalf("concurrency block = %v", blocked)
	}
	done(false, 10*time.Millisecond)
	done, blocked = e.Admit("/slow", Identity{UID: 1})
	if blocked != nil {
		t.Fatal(blocked)
	}
	done(false, 10*time.Millisecond)
	_, blocked = e.Admit("/slow", Identity{UID: 1})
	if blocked == nil || blocked.Kind != DegradeBlocked {
		t.Fatalf("degrade block = %v", blocked)
	}
	w := httptest.NewRecorder()
	if err := WriteFallback(w, blocked); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"code":100503`) || !strings.Contains(w.Body.String(), "Request degraded") {
		t.Fatalf("fallback = %d %s", w.Code, w.Body.String())
	}
}

func TestDoneAfterUpdateRetainsConcurrencyAccounting(t *testing.T) {
	e := NewEngine()
	rule := `[{"resource":"/slow","count":1,"grade":0}]`
	if err := e.Update(rule, `[]`, `[]`); err != nil {
		t.Fatal(err)
	}
	oldDone, blocked := e.Admit("/slow", Identity{})
	if blocked != nil {
		t.Fatal(blocked)
	}
	if err := e.Update(rule, `[]`, `[]`); err != nil {
		t.Fatal(err)
	}
	oldDone(true, 0)
	firstDone, blocked := e.Admit("/slow", Identity{})
	if blocked != nil {
		t.Fatalf("first request after old Done blocked: %v", blocked)
	}
	_, blocked = e.Admit("/slow", Identity{})
	if blocked == nil || blocked.Kind != FlowBlocked {
		t.Fatalf("second concurrent request not blocked: %v", blocked)
	}
	firstDone(true, 0)
}

func TestThrottlingQueueReservationAndTimeout(t *testing.T) {
	e := NewEngine()
	if err := e.Update(`[{"resource":"/queue","count":10,"grade":1,"controlBehavior":2,"maxQueueingTimeMs":150}]`, `[]`, `[]`); err != nil {
		t.Fatal(err)
	}
	first, blocked := e.Admit("/queue", Identity{})
	if blocked != nil {
		t.Fatal(blocked)
	}
	first(true, 0)
	result := make(chan *Block, 1)
	go func() {
		done, block := e.Admit("/queue", Identity{})
		if done != nil {
			done(true, 0)
		}
		result <- block
	}()
	deadline := time.Now().Add(50 * time.Millisecond)
	for {
		e.mu.Lock()
		latest := e.queue["flow:0:/queue"].latest
		e.mu.Unlock()
		if latest.Sub(time.Now()) > 50*time.Millisecond {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second request did not reserve queue slot")
		}
		time.Sleep(time.Millisecond)
	}
	_, blocked = e.Admit("/queue", Identity{})
	if blocked == nil || blocked.Kind != FlowBlocked {
		t.Fatalf("over-limit queue should block: %v", blocked)
	}
	if block := <-result; block != nil {
		t.Fatalf("queued request blocked: %v", block)
	}
	if err := e.Update(`[{"resource":"/queue","count":10,"controlBehavior":1}]`, `[]`, `[]`); err == nil {
		t.Fatal("unsupported warm-up accepted")
	}
}

func TestThrottlingQueueWaitBoundary(t *testing.T) {
	now := time.Unix(0, 0)
	latest := now.Add(100 * time.Millisecond)
	if got, ok := queueScheduled(latest, now, 10, 200*time.Millisecond); !ok || !got.Equal(now.Add(200*time.Millisecond)) {
		t.Fatalf("exact wait should pass: %v %v", got, ok)
	}
	if _, ok := queueScheduled(latest, now, 10, 199*time.Millisecond); ok {
		t.Fatal("one millisecond over maximum should block")
	}
	if got, ok := queueScheduled(latest, now.Add(250*time.Millisecond), 10, 0); !ok || !got.Equal(now.Add(250*time.Millisecond)) {
		t.Fatalf("idle queue should pass immediately: %v %v", got, ok)
	}
}

func TestFlowTwoAlignedSamplesAtSecondBoundary(t *testing.T) {
	e := NewEngine()
	current := time.UnixMilli(900)
	e.now = func() time.Time { return current }
	if err := e.Update(`[{"resource":"/a","count":1}]`, `[]`, `[]`); err != nil {
		t.Fatal(err)
	}
	done, block := e.Admit("/a", Identity{UID: 1})
	if block != nil {
		t.Fatal(block)
	}
	done(true, 0)
	current = time.UnixMilli(1100)
	_, block = e.Admit("/a", Identity{UID: 1})
	if block == nil {
		t.Fatal("previous 500 ms sample lost at 1-second boundary")
	}
	current = time.UnixMilli(1500)
	done, block = e.Admit("/a", Identity{UID: 1})
	if block != nil {
		t.Fatalf("expired sample still blocks: %v", block)
	}
	done(true, 0)
}

func TestHotTokenRefillUsesStrictDuration(t *testing.T) {
	e := NewEngine()
	current := time.UnixMilli(1000)
	e.now = func() time.Time { return current }
	if err := e.Update(`[]`, `[{"resource":"hot_uid","count":1,"durationInSec":1}]`, `[]`); err != nil {
		t.Fatal(err)
	}
	done, block := e.Admit("/a", Identity{UID: 7})
	if block != nil {
		t.Fatal(block)
	}
	done(true, 0)
	current = time.UnixMilli(2000)
	_, block = e.Admit("/a", Identity{UID: 7})
	if block == nil {
		t.Fatal("token refilled at elapsed == duration")
	}
	current = time.UnixMilli(2001)
	done, block = e.Admit("/a", Identity{UID: 7})
	if block != nil {
		t.Fatalf("token not refilled after duration: %v", block)
	}
	done(true, 0)
}

func TestHotBlockDoesNotCountAsFlowPass(t *testing.T) {
	e := NewEngine()
	e.now = func() time.Time { return time.UnixMilli(1000) }
	if err := e.Update(`[{"resource":"/a","count":2}]`, `[{"resource":"hot_uid","count":1}]`, `[]`); err != nil {
		t.Fatal(err)
	}
	done, block := e.Admit("/a", Identity{UID: 7})
	if block != nil {
		t.Fatal(block)
	}
	done(true, 0)
	_, block = e.Admit("/a", Identity{UID: 7})
	if block == nil || block.Kind != HotBlocked {
		t.Fatalf("expected hot block, got %v", block)
	}
	done, block = e.Admit("/a", Identity{UID: 8})
	if block != nil {
		t.Fatalf("hot rejection consumed path QPS: %v", block)
	}
	done(true, 0)
}

func TestNoLoginRuleUsesOriginalPathNode(t *testing.T) {
	e := NewEngine()
	e.now = func() time.Time { return time.UnixMilli(1000) }
	if err := e.Update(`[{"resource":"no_login_all","count":1}]`, `[]`, `[]`); err != nil {
		t.Fatal(err)
	}
	done, block := e.Admit("/a", Identity{UID: 7})
	if block != nil {
		t.Fatal(block)
	}
	done(true, 0)
	_, block = e.Admit("/a", Identity{})
	if block == nil || block.Resource != NoLoginResource {
		t.Fatalf("logged-in traffic did not count in original path node: %v", block)
	}
	done, block = e.Admit("/b", Identity{})
	if block != nil {
		t.Fatalf("unrelated path incorrectly shares no-login node: %v", block)
	}
	done(true, 0)
}

func TestDegradeHalfOpenSingleProbe(t *testing.T) {
	e := NewEngine()
	current := time.UnixMilli(1000)
	e.now = func() time.Time { return current }
	if err := e.Update(`[]`, `[]`, `[{"resource":"/a","grade":2,"count":1,"timeWindow":1,"minRequestAmount":1}]`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		done, block := e.Admit("/a", Identity{})
		if block != nil {
			t.Fatal(block)
		}
		done(false, 0)
	}
	_, block := e.Admit("/a", Identity{})
	if block == nil {
		t.Fatal("breaker did not open after count exceeded")
	}
	current = time.UnixMilli(2000)
	probe, block := e.Admit("/a", Identity{})
	if block != nil {
		t.Fatalf("probe blocked at timeout: %v", block)
	}
	_, block = e.Admit("/a", Identity{})
	if block == nil {
		t.Fatal("second concurrent half-open probe allowed")
	}
	probe(true, 0)
	done, block := e.Admit("/a", Identity{})
	if block != nil {
		t.Fatalf("breaker failed to close: %v", block)
	}
	done(true, 0)
}

func TestExceptionRatioStrictThresholdAndAlignedReset(t *testing.T) {
	e := NewEngine()
	current := time.UnixMilli(1900)
	e.now = func() time.Time { return current }
	if err := e.Update(`[]`, `[]`, `[{"resource":"/a","grade":1,"count":0.5,"timeWindow":1,"minRequestAmount":2,"statIntervalMs":1000}]`); err != nil {
		t.Fatal(err)
	}
	for _, success := range []bool{true, false} {
		done, block := e.Admit("/a", Identity{})
		if block != nil {
			t.Fatal(block)
		}
		done(success, 0)
	}
	current = time.UnixMilli(2000)
	done, block := e.Admit("/a", Identity{})
	if block != nil {
		t.Fatalf("exact 0.5 ratio should not open: %v", block)
	}
	done(false, 0) // aligned new window contains only one request.
	current = time.UnixMilli(2001)
	done, block = e.Admit("/a", Identity{})
	if block != nil {
		t.Fatal(block)
	}
	done(true, 0) // ratio is exactly 0.5 again.
	done, block = e.Admit("/a", Identity{})
	if block != nil {
		t.Fatalf("exact threshold opened breaker: %v", block)
	}
	done(false, 0)
	_, block = e.Admit("/a", Identity{})
	if block == nil || block.Kind != DegradeBlocked {
		t.Fatalf("ratio above threshold did not open: %v", block)
	}
}
