package sensitive

type Mode string

const (
	WhitelistMode Mode = "WHITELIST_MODE"
	BlacklistMode Mode = "BLACKLIST_MODE"
)

type WordFilterKey struct {
	Path        string   `json:"path"`
	FilterField []string `json:"filterField"`
	IsHash      bool     `json:"isHash"`
}

// Config mirrors the Java desensitization model. The HTTP policy that uses
// this configuration belongs to the separate web project.
type Config struct {
	Mode                      Mode                     `json:"mode"`
	BlackURL                  []string                 `json:"blackUrl"`
	WhiteURL                  []string                 `json:"whiteUrl"`
	SensitiveWordFilterKeyMap map[string]WordFilterKey `json:"sensitiveWordFilterKeyMap"`
}

func NewConfig() Config {
	return Config{Mode: BlacklistMode, BlackURL: []string{}, WhiteURL: []string{}, SensitiveWordFilterKeyMap: map[string]WordFilterKey{}}
}

func (c Config) FilterKey(path string) (WordFilterKey, bool) {
	v, ok := c.SensitiveWordFilterKeyMap[path]
	return v, ok
}
