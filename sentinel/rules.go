package sentinel

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

const NamespaceSuffix = "-sentinel"
const EmptyRules = "[]"
const NoLoginResource = "no_login_all"

func Namespace(app string) string { return app + NamespaceSuffix }

// These fields retain Sentinel 1.8.8 Apollo JSON names. Unsupported
// algorithms are rejected by Update rather than silently approximated.
type FlowRule struct {
	Resource          string  `json:"resource"`
	Count             float64 `json:"count"`
	Grade             int     `json:"grade"` // 0 concurrent threads, 1 QPS
	Strategy          int     `json:"strategy"`
	ControlBehavior   int     `json:"controlBehavior"`
	WarmUpPeriodSec   int     `json:"warmUpPeriodSec"`
	MaxQueueingTimeMs int     `json:"maxQueueingTimeMs"`
	RefResource       string  `json:"refResource"`
	LimitApp          string  `json:"limitApp"`
	ClusterMode       bool    `json:"clusterMode"`
	Regex             bool    `json:"regex"`
}

func (r *FlowRule) UnmarshalJSON(data []byte) error {
	type wire FlowRule
	v := wire{Grade: 1, LimitApp: "default", WarmUpPeriodSec: 10, MaxQueueingTimeMs: 500}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	*r = FlowRule(v)
	return nil
}

type HotRule struct {
	Resource          string            `json:"resource"`
	ParamIdx          *int              `json:"paramIdx"`
	Count             float64           `json:"count"`
	Grade             int               `json:"grade"`
	DurationInSec     int64             `json:"durationInSec"`
	ControlBehavior   int               `json:"controlBehavior"`
	MaxQueueingTimeMs int               `json:"maxQueueingTimeMs"`
	BurstCount        int               `json:"burstCount"`
	ParamFlowItemList []json.RawMessage `json:"paramFlowItemList"`
	ClusterMode       bool              `json:"clusterMode"`
	LimitApp          string            `json:"limitApp"`
	Regex             bool              `json:"regex"`
}

func (r *HotRule) UnmarshalJSON(data []byte) error {
	type wire HotRule
	v := wire{Grade: 1, DurationInSec: 1}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	*r = HotRule(v)
	return nil
}

type DegradeRule struct {
	Resource           string  `json:"resource"`
	Grade              int     `json:"grade"` // 0 slow ratio, 1 exception ratio, 2 exception count
	Count              float64 `json:"count"`
	TimeWindow         int     `json:"timeWindow"`
	MinRequestAmount   int     `json:"minRequestAmount"`
	SlowRatioThreshold float64 `json:"slowRatioThreshold"`
	StatIntervalMs     int     `json:"statIntervalMs"`
	LimitApp           string  `json:"limitApp"`
	Regex              bool    `json:"regex"`
}

func (r *DegradeRule) UnmarshalJSON(data []byte) error {
	type wire DegradeRule
	v := wire{MinRequestAmount: 5, SlowRatioThreshold: 1, StatIntervalMs: 1000}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	*r = DegradeRule(v)
	return nil
}

type RuleSet struct {
	Flow    []FlowRule
	Hot     []HotRule
	Degrade []DegradeRule
}

type Store struct {
	mu    sync.RWMutex
	rules RuleSet
}

func NewStore() *Store { return &Store{} }

// Update replaces all three rule sets atomically after validating each JSON
// array. Apollo adapters can call this on each complete configuration update.
func (s *Store) Update(flowJSON, hotJSON, degradeJSON string) error {
	if flowJSON == "" {
		flowJSON = EmptyRules
	}
	if hotJSON == "" {
		hotJSON = EmptyRules
	}
	if degradeJSON == "" {
		degradeJSON = EmptyRules
	}
	var next RuleSet
	if err := json.Unmarshal([]byte(flowJSON), &next.Flow); err != nil {
		return fmt.Errorf("flow JSON: %w", err)
	}
	if err := json.Unmarshal([]byte(hotJSON), &next.Hot); err != nil {
		return fmt.Errorf("hot JSON: %w", err)
	}
	if err := json.Unmarshal([]byte(degradeJSON), &next.Degrade); err != nil {
		return fmt.Errorf("degrade JSON: %w", err)
	}
	if next.Flow == nil || next.Hot == nil || next.Degrade == nil {
		return errors.New("rules must be JSON arrays")
	}
	for _, r := range next.Flow {
		if r.Resource == "" || r.Count <= 0 || (r.Grade != 0 && r.Grade != 1) || r.Strategy != 0 || (r.ControlBehavior != 0 && r.ControlBehavior != 2) || (r.ControlBehavior == 2 && (r.Grade != 1 || r.MaxQueueingTimeMs < 0)) || r.RefResource != "" || r.ClusterMode || r.Regex || (r.LimitApp != "" && r.LimitApp != "default") {
			return fmt.Errorf("unsupported flow rule for %q", r.Resource)
		}
	}
	for _, r := range next.Hot {
		if r.Resource == "" || r.Count <= 0 || r.Grade != 1 || (r.ParamIdx != nil && *r.ParamIdx != 0) || r.ControlBehavior != 0 || r.MaxQueueingTimeMs != 0 || r.BurstCount != 0 || len(r.ParamFlowItemList) != 0 || r.ClusterMode || r.Regex || (r.LimitApp != "" && r.LimitApp != "default") {
			return fmt.Errorf("unsupported hot rule for %q", r.Resource)
		}
	}
	for _, r := range next.Degrade {
		if r.Resource == "" || r.Count <= 0 || r.TimeWindow <= 0 || r.Grade < 0 || r.Grade > 2 || r.Regex || (r.LimitApp != "" && r.LimitApp != "default") {
			return fmt.Errorf("unsupported degrade rule for %q", r.Resource)
		}
	}
	s.mu.Lock()
	s.rules = next
	s.mu.Unlock()
	return nil
}

func (s *Store) Snapshot() RuleSet {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return RuleSet{Flow: append([]FlowRule(nil), s.rules.Flow...), Hot: append([]HotRule(nil), s.rules.Hot...), Degrade: append([]DegradeRule(nil), s.rules.Degrade...)}
}

// Java ApiLevelFlowRuleManager and ApiLevelDegradeRuleManager currently return
// null from getApiLevel; this intentionally has no inferred mapping.
func APILevel(string) (string, bool) { return "", false }
