package metrics

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestEventJavaColumnOrderAndDefaults(t *testing.T) {
	e := NewEvent("orders", string(BackBiz), "paid", 42, "", "", map[string]any{"amount": 5})
	e.Time = time.UnixMilli(1728000000123)
	line, err := e.Line()
	if err != nil {
		t.Fatal(err)
	}
	want := "backend|@|-1|@|orders|@|-1|@|42|@|-1|@|-1|@|back_biz|@|paid|@|{\"amount\":5}|@|1728000000123|@|-1|@|-1|@|-1"
	if line != want {
		t.Fatalf("line = %q, want %q", line, want)
	}
}

func TestMetricColumnCountAndWrite(t *testing.T) {
	m := Metric{StartTime: time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC), TraceID: "trace", ServerName: "orders", CostMillis: 12, Result: BusinessError, Code: 100400}
	line := m.Line()
	if got := len(strings.Split(line, Delimiter)); got != 13 {
		t.Fatalf("columns = %d", got)
	}
	var b bytes.Buffer
	if err := WriteLine(&b, line); err != nil {
		t.Fatal(err)
	}
	if b.String() != line+"\n" {
		t.Fatalf("output = %q", b.String())
	}
}
