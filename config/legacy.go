package config

import (
	"errors"
	"regexp"
	"strconv"
)

var envPrefixPattern = regexp.MustCompile(`pre|prod|gray|stage`)

// Legacy provides the observable ConfigUtils behavior without Java's global
// StaticConfigUtil state. Pass it explicitly to a service at construction.
type Legacy struct {
	Snapshot Snapshot
	Profile  string
}

func NewLegacy(snapshot Snapshot, activeProfiles ...string) (Legacy, error) {
	if len(activeProfiles) == 0 || activeProfiles[0] == "" {
		return Legacy{}, errors.New("first active profile is required")
	}
	return Legacy{Snapshot: snapshot, Profile: activeProfiles[0]}, nil
}

func (c Legacy) Env() string { return c.Profile }
func (c Legacy) EnvPrefix() string {
	if match := envPrefixPattern.FindString(c.Profile); match != "" {
		return match
	}
	return c.Profile
}
func (c Legacy) IsProductEnv() bool {
	switch c.Profile {
	case "prod", "gray", "stage":
		return true
	}
	return false
}
func (c Legacy) IsGrayEnv() bool  { return c.Profile == "gray" || c.Profile == "stage" }
func (c Legacy) IsLocalEnv() bool { return c.Profile == "local" }

func (c Legacy) String(key, fallback string) string { return c.Snapshot.String(key, fallback) }

// Java ConfigUtils treats only the exact string "true" as true.
func (c Legacy) Bool(key string, fallback bool) bool {
	value, ok := c.Snapshot.Lookup(key)
	if !ok || value == "" {
		return fallback
	}
	return value == "true"
}

// Java ConfigUtils returns the fallback for missing or malformed integers.
func (c Legacy) Int(key string, fallback int) int {
	value, ok := c.Snapshot.Lookup(key)
	if !ok || value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func (c Legacy) ServerRole() string  { return c.String("server.role", "") }
func (c Legacy) ProjectName() string { return c.String("project.name", "") }
func (c Legacy) AppID() string       { return c.String("server.appId", "") }
