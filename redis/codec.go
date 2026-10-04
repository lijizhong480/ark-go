package redis

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Codec controls value and hash-value bytes. Redis keys and hash fields remain
// UTF-8 strings, matching the Java string and JSON serializer configurations.
type Codec interface {
	Marshal(value any) ([]byte, error)
	Unmarshal(data []byte, target any) error
}

// StringCodec accepts strings and byte slices. It does not stringify arbitrary
// Go values, since that would silently change persisted Java Redis bytes.
type StringCodec struct{}

func (StringCodec) Marshal(value any) ([]byte, error) {
	switch v := value.(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return append([]byte(nil), v...), nil
	default:
		return nil, fmt.Errorf("string codec: unsupported value %T", value)
	}
}

func (StringCodec) Unmarshal(data []byte, target any) error {
	switch v := target.(type) {
	case *string:
		if v == nil {
			return errors.New("string codec: nil target")
		}
		*v = string(data)
	case *[]byte:
		if v == nil {
			return errors.New("string codec: nil target")
		}
		*v = append((*v)[:0], data...)
	default:
		return fmt.Errorf("string codec: unsupported target %T", target)
	}
	return nil
}

// JSONCodec uses standard JSON. Jackson's optional polymorphic @class field
// requires application-specific Go types and must be tested with Java vectors.
type JSONCodec struct{}

func (JSONCodec) Marshal(value any) ([]byte, error)       { return json.Marshal(value) }
func (JSONCodec) Unmarshal(data []byte, target any) error { return json.Unmarshal(data, target) }

// JacksonStringCodec is limited to Java String values serialized by
// GenericJackson2JsonRedisSerializer. The Java String type is final, so
// DefaultTyping.NON_FINAL does not add a @class field to this scalar.
type JacksonStringCodec struct{}

func (JacksonStringCodec) Marshal(value any) ([]byte, error) {
	stringValue, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("Jackson string codec: unsupported value %T", value)
	}
	return json.Marshal(stringValue)
}

func (JacksonStringCodec) Unmarshal(data []byte, target any) error {
	stringTarget, ok := target.(*string)
	if !ok || stringTarget == nil {
		return fmt.Errorf("Jackson string codec: unsupported target %T", target)
	}
	return json.Unmarshal(data, stringTarget)
}

// NewJavaCompatibleCodec selects only byte formats validated against the Java
// serializer. A typed JSON map, JDK object stream, or unknown custom format
// needs a separate codec with application-specific cross-language vectors.
func NewJavaCompatibleCodec(kind string, jsonUseTypeMapper bool, custom Codec) (Codec, error) {
	switch kind {
	case "string":
		return StringCodec{}, nil
	case "json":
		if jsonUseTypeMapper {
			return JacksonStringCodec{}, nil
		}
		return JSONCodec{}, nil
	case "jdk":
		return nil, errors.New("JDK Redis serialization is unsupported; migrate values before using Go")
	case "custom":
		if custom == nil {
			return nil, errors.New("custom Redis serializer requires a codec validated with Java byte vectors")
		}
		return custom, nil
	default:
		return nil, fmt.Errorf("unsupported Redis serializer %q", kind)
	}
}
