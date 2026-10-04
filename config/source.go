package config

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type MapSource map[string]string

func (s MapSource) Load(context.Context) (map[string]string, error) {
	result := make(map[string]string, len(s))
	for key, value := range s {
		result[key] = value
	}
	return result, nil
}

// EnvSource maps PREFIX_FOO_BAR to foo.bar. Explicit aliases should be used
// for legacy keys containing underscores.
type EnvSource struct{ Prefix string }

func (s EnvSource) Load(context.Context) (map[string]string, error) {
	result := make(map[string]string)
	for _, entry := range os.Environ() {
		key, value, ok := strings.Cut(entry, "=")
		if !ok || !strings.HasPrefix(key, s.Prefix) {
			continue
		}
		key = strings.TrimPrefix(key, s.Prefix)
		if key == "" {
			continue
		}
		result[strings.ToLower(strings.ReplaceAll(key, "_", "."))] = value
	}
	return result, nil
}

type FileSource struct{ Path string }

func (s FileSource) Load(context.Context) (map[string]string, error) {
	f, err := os.Open(s.Path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if strings.HasSuffix(strings.ToLower(s.Path), ".properties") {
		return readProperties(f)
	}
	var data map[string]any
	if err := yaml.NewDecoder(f).Decode(&data); err != nil {
		return nil, err
	}
	result := make(map[string]string)
	flatten("", data, result)
	return result, nil
}

func readProperties(r io.Reader) (map[string]string, error) {
	result := make(map[string]string)
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "!") {
			continue
		}
		index := strings.IndexAny(line, "=:")
		if index < 0 {
			return nil, fmt.Errorf("invalid properties line %q", line)
		}
		result[strings.TrimSpace(line[:index])] = strings.TrimSpace(line[index+1:])
	}
	return result, scanner.Err()
}

func flatten(prefix string, input map[string]any, result map[string]string) {
	for key, value := range input {
		path := key
		if prefix != "" {
			path = prefix + "." + key
		}
		switch nested := value.(type) {
		case map[string]any:
			flatten(path, nested, result)
		default:
			result[path] = fmt.Sprint(value)
		}
	}
}
