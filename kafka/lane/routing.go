// Package lane preserves the Kafka swim-lane header and consumer routing rules.
package lane

import (
	"context"
	"strings"
	"sync/atomic"
	"time"
)

const (
	RouterTagHeader      = "x-router-tag"
	TraceIDHeader        = "traceId"
	EnabledConfigKey     = "lane.routing.enabled"
	InstanceTagConfigKey = "routing.swimlane-tag"
	FallbackConfigKey    = "lane.fallback.enabled"
)

type Active interface{ IsActive(tag string) bool }

type Router struct {
	Enabled     bool
	InstanceTag string
	Fallback    bool
	Registry    Active
}

// ProducerHeaders copies headers before adding the instance lane tag and the
// traceId used by ArkKafkaTemplate. An untagged instance leaves the lane unset.
func (r Router) ProducerHeaders(headers map[string][]byte, traceID string) map[string][]byte {
	result := make(map[string][]byte, len(headers)+2)
	for key, value := range headers {
		result[key] = append([]byte(nil), value...)
	}
	if traceID != "" {
		result[TraceIDHeader] = []byte(traceID)
	}
	if r.Enabled && strings.TrimSpace(r.InstanceTag) != "" {
		result[RouterTagHeader] = []byte(r.InstanceTag)
	}
	return result
}

func (r Router) ShouldProcess(messageTag string) bool {
	if !r.Enabled {
		return true
	}
	instanceIsTrunk := strings.TrimSpace(r.InstanceTag) == ""
	messageIsTrunk := strings.TrimSpace(messageTag) == ""
	if instanceIsTrunk {
		if r.Fallback && !messageIsTrunk && r.Registry != nil && !r.Registry.IsActive(messageTag) {
			return true
		}
		return messageIsTrunk
	}
	return r.InstanceTag == messageTag
}

// Registry keeps one immutable active-lane snapshot per Kafka cluster. Before
// the first refresh, it assumes lanes are active to avoid startup fallback.
type Registry struct{ current atomic.Pointer[snapshot] }
type snapshot struct{ active map[string]bool }

func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) IsActive(tag string) bool {
	current := r.current.Load()
	if current == nil {
		return true
	}
	return current.active[tag]
}

func (r *Registry) Update(active map[string]bool) {
	copyMap := make(map[string]bool, len(active))
	for tag, value := range active {
		copyMap[tag] = value
	}
	r.current.Store(&snapshot{active: copyMap})
}

// Refresh leaves the last snapshot in place when the broker query fails.
func (r *Registry) Refresh(ctx context.Context, interval time.Duration, fetch func(context.Context) (map[string]bool, error)) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	refresh := func() {
		if active, err := fetch(ctx); err == nil {
			r.Update(active)
		}
	}
	refresh()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refresh()
		}
	}
}
