package config

import (
	"context"
	"testing"
)

func TestLegacyConfigUtils(t *testing.T) {
	s, err := Load(context.Background(), MapSource{"flag": "TRUE", "bad": "oops", "project.name": "orders", "server.appId": "a"})
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewLegacy(s, "xx-gray-canary", "prod")
	if err != nil {
		t.Fatal(err)
	}
	if c.EnvPrefix() != "gray" || c.IsProductEnv() || c.IsGrayEnv() || c.Bool("flag", true) || c.Int("bad", 7) != 7 || c.ProjectName() != "orders" || c.AppID() != "a" {
		t.Fatalf("legacy behavior: %+v", c)
	}
	prod, _ := NewLegacy(s, "prod")
	if !prod.IsProductEnv() || prod.IsGrayEnv() || prod.IsLocalEnv() {
		t.Fatal("prod classification")
	}
	if _, err := NewLegacy(s); err == nil {
		t.Fatal("missing profile")
	}
}
