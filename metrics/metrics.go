package metrics

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

const Delimiter = "|@|"
const Default = "-1"

type EventType string

const (
	BackABTest EventType = "back_abtest"
	BackBiz    EventType = "back_biz"
	BackRisk   EventType = "back_risk"
)

type Event struct {
	Server   string
	Version  string
	Channel  string
	Net      string
	UID      int64
	DeviceID string
	Lang     string
	Type     string
	Name     string
	Props    any
	Time     time.Time
	Cost     string
	TraceID  string
	SpanID   string
}

func NewEvent(project, eventType, name string, uid int64, deviceID, lang string, props any) Event {
	return Event{Server: "backend", Version: Default, Channel: project, Net: Default,
		UID: uid, DeviceID: defaultIfBlank(deviceID), Lang: defaultIfBlank(lang),
		Type: eventType, Name: name, Props: props, Time: time.Now(), Cost: Default,
		TraceID: Default, SpanID: Default}
}

// Line returns EventMetrics' exact field order and delimiter. Props is JSON
// encoded as one field, as in the Java constructor.
func (e Event) Line() (string, error) {
	props, err := json.Marshal(e.Props)
	if err != nil {
		return "", err
	}
	return join(e.Server, e.Version, e.Channel, e.Net, e.UID, e.DeviceID, e.Lang,
		e.Type, e.Name, string(props), e.Time.UnixMilli(), e.Cost,
		defaultIfBlank(e.TraceID), defaultIfBlank(e.SpanID)), nil
}

type Result int

const (
	Success       Result = 0
	SystemError   Result = 1
	BusinessError Result = 2
)

type Metric struct {
	StartTime      time.Time
	TraceID        string
	ServerName     string
	ServerID       string
	InvokeServer   string
	InvokeServerID string
	CostMillis     int64
	Result         Result
	Code           int
	App            string
	Action         string
	Detail         string
	SpanID         string
}

func (m Metric) Line() string {
	// Java Date.toString uses this English locale order.
	start := "null"
	if !m.StartTime.IsZero() {
		start = m.StartTime.Format("Mon Jan 02 15:04:05 MST 2006")
	}
	return join(start, m.TraceID, m.ServerName, m.ServerID, m.InvokeServer,
		m.InvokeServerID, m.CostMillis, int(m.Result), m.Code, m.App,
		m.Action, m.Detail, m.SpanID)
}

func WriteLine(w io.Writer, line string) error {
	_, err := io.WriteString(w, line+"\n")
	return err
}

func defaultIfBlank(value string) string {
	if strings.TrimSpace(value) == "" {
		return Default
	}
	return value
}

func join(values ...any) string {
	fields := make([]string, len(values))
	for i, value := range values {
		if value == nil {
			fields[i] = "null"
		} else {
			fields[i] = fmt.Sprint(value)
		}
	}
	return strings.Join(fields, Delimiter)
}
