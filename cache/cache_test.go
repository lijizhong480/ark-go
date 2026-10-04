package cache

import (
	"reflect"
	"testing"
	"time"
)

func TestLRUEvictsLeastRecentlyUsed(t *testing.T) {
	var evicted []string
	c, err := New[string, string](Spec{MaxSize: 2}, func(k, v string) { evicted = append(evicted, k+"="+v) })
	if err != nil {
		t.Fatal(err)
	}
	c.Put("a", "1")
	c.Put("b", "2")
	c.Get("a")
	c.Put("c", "3")
	if _, ok := c.Get("b"); ok {
		t.Fatal("least recent item remains")
	}
	if !reflect.DeepEqual(evicted, []string{"b=2"}) {
		t.Fatal(evicted)
	}
}

func TestLegacyAfterAccessSpelling(t *testing.T) {
	c, err := New[string, int](Spec{MaxSize: 1, Duration: 20 * time.Millisecond, Strategy: LegacyAfterAcess}, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.Put("a", 1)
	time.Sleep(12 * time.Millisecond)
	if _, ok := c.Get("a"); !ok {
		t.Fatal("entry expired before access")
	}
	time.Sleep(12 * time.Millisecond)
	if _, ok := c.Get("a"); !ok {
		t.Fatal("access did not extend TTL")
	}
	time.Sleep(25 * time.Millisecond)
	if _, ok := c.Get("a"); ok {
		t.Fatal("expired entry found")
	}
}

func TestPutAfterExpiryReportsMissing(t *testing.T) {
	c, err := New[string, int](Spec{MaxSize: 1, Duration: time.Millisecond, Strategy: AfterWrite}, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.Put("k", 1)
	time.Sleep(3 * time.Millisecond)
	if prior, found := c.Put("k", 2); found || prior != 0 {
		t.Fatalf("prior=%d found=%v", prior, found)
	}
}
