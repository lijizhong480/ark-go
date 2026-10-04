package gateway

import (
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strings"
)

const (
	ParamClientIP       = 0
	ParamHost           = 1
	ParamHeader         = 2
	ParamURL            = 3
	ParamCookie         = 4
	MatchExact          = 0
	MatchPrefix         = 1
	MatchRegex          = 2
	MatchContains       = 3
	GatewayDefaultParam = "$D"
	GatewayNotMatched   = "$NM"
)

// GatewayParamItem carries the Java GatewayParamFlowItem configuration.
// Index is assigned by GatewayRuleManager; when absent, ParseRouteParams
// assigns it in input order for deterministic Go configuration.
type GatewayParamItem struct {
	Index         *int   `json:"index,omitempty"`
	ParseStrategy int    `json:"parseStrategy"`
	FieldName     string `json:"fieldName,omitempty"`
	Pattern       string `json:"pattern,omitempty"`
	MatchStrategy int    `json:"matchStrategy,omitempty"`
}

type GatewayRule struct {
	Resource     string            `json:"resource"`
	ResourceMode int               `json:"resourceMode"`
	ParamItem    *GatewayParamItem `json:"paramItem,omitempty"`
}

// ParseRouteParams reproduces GatewayParamParser's route-mode behavior.
// A mixed route/custom-API parameter rule set produces an empty slice in Java.
func ParseRouteParams(route string, request *http.Request, rules []GatewayRule) ([]any, error) {
	if route == "" || request == nil {
		return []any{}, nil
	}
	items := []GatewayParamItem{}
	hasDefault := false
	for _, rule := range rules {
		if rule.Resource != route {
			continue
		}
		if rule.ParamItem == nil {
			hasDefault = true
			continue
		}
		if rule.ResourceMode != 0 {
			return []any{}, nil
		}
		items = append(items, *rule.ParamItem)
	}
	if len(items) == 0 && !hasDefault {
		return []any{}, nil
	}
	result := make([]any, len(items))
	used := make(map[int]bool, len(items))
	for i, item := range items {
		index := i
		if item.Index != nil {
			index = *item.Index
		}
		if index < 0 || index >= len(items) || used[index] {
			return nil, fmt.Errorf("gateway: invalid parameter index %d", index)
		}
		used[index] = true
		value, err := parseParam(request, item)
		if err != nil {
			return nil, err
		}
		if value != nil {
			result[index] = *value
		}
	}
	if hasDefault {
		result = append(result, GatewayDefaultParam)
	}
	return result, nil
}

func parseParam(r *http.Request, item GatewayParamItem) (*string, error) {
	var value *string
	switch item.ParseStrategy {
	case ParamClientIP:
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err == nil {
			value = &host
		} else if net.ParseIP(r.RemoteAddr) != nil {
			v := r.RemoteAddr
			value = &v
		}
	case ParamHost:
		if v := r.Header.Get("Host"); v != "" {
			value = &v
		} else if r.Host != "" {
			v := r.Host
			value = &v
		}
	case ParamHeader:
		if item.FieldName == "" {
			return nil, fmt.Errorf("gateway: header fieldName required")
		}
		if values, ok := r.Header[http.CanonicalHeaderKey(item.FieldName)]; ok && len(values) > 0 {
			v := values[0]
			value = &v
		}
	case ParamURL:
		if item.FieldName == "" {
			return nil, fmt.Errorf("gateway: URL fieldName required")
		}
		if values, ok := r.URL.Query()[item.FieldName]; ok && len(values) > 0 {
			v := values[0]
			value = &v
		}
	case ParamCookie:
		if item.FieldName == "" {
			return nil, fmt.Errorf("gateway: cookie fieldName required")
		}
		if cookie, err := r.Cookie(item.FieldName); err == nil {
			v := cookie.Value
			value = &v
		}
	default:
		return nil, fmt.Errorf("gateway: unsupported parseStrategy %d", item.ParseStrategy)
	}
	if value == nil || item.Pattern == "" {
		return value, nil
	}
	matched := false
	switch item.MatchStrategy {
	case MatchExact:
		matched = *value == item.Pattern
	case MatchPrefix:
		matched = strings.HasPrefix(*value, item.Pattern)
	case MatchContains:
		matched = strings.Contains(*value, item.Pattern)
	case MatchRegex:
		pattern, err := regexp.Compile(item.Pattern)
		if err != nil {
			return nil, fmt.Errorf("gateway: invalid parameter regex: %w", err)
		}
		loc := pattern.FindStringIndex(*value)
		matched = loc != nil && loc[0] == 0 && loc[1] == len(*value)
	default:
		return nil, fmt.Errorf("gateway: unsupported matchStrategy %d", item.MatchStrategy)
	}
	if !matched {
		v := GatewayNotMatched
		return &v, nil
	}
	return value, nil
}
