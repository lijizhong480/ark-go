package web

import (
	"encoding/json"
	"fmt"
	"strings"
)

// StringConfig is implemented by the Apollo project's Config. Pass a reader
// configured for the appName + "-config" namespace to load Java rules.
type StringConfig interface {
	String(key, fallback string) string
}

// LoadDesensitizeConfig reads the Java app specific rule keys. A parse error
// leaves the existing AccessFilter snapshot intact until the caller updates it.
func LoadDesensitizeConfig(source StringConfig) (DesensitizeConfig, error) {
	var cfg DesensitizeConfig
	if source == nil {
		return cfg, fmt.Errorf("web: nil desensitize config")
	}
	if err := json.Unmarshal([]byte(source.String("app.desensitize.white-list", "[]")), &cfg.WhiteURLs); err != nil {
		return cfg, fmt.Errorf("web: white-list: %w", err)
	}
	if err := json.Unmarshal([]byte(source.String("app.desensitize.black-list", "[]")), &cfg.BlackURLs); err != nil {
		return cfg, fmt.Errorf("web: black-list: %w", err)
	}
	if err := json.Unmarshal([]byte(source.String("app.desensitize.sensitive-field-list", "[]")), &cfg.Rules); err != nil {
		return cfg, fmt.Errorf("web: sensitive-field-list: %w", err)
	}
	cfg.Mode = BlacklistMode
	if source.String("app.desensitize.mode", "") == string(WhitelistMode) {
		cfg.Mode = WhitelistMode
	}
	return cfg, nil
}

// LoadPermitURLs reads the Java permit.urls key used by permit only mode.
func LoadPermitURLs(source StringConfig) []string {
	if source == nil {
		return nil
	}
	raw := source.String("permit.urls", "")
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
