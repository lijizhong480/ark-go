package sensitive

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf16"
)

// Mask uses Java UTF-16 code units when selecting the visible prefix/suffix.
func Mask(value string) string {
	units := utf16.Encode([]rune(value))
	n := len(units)
	if n == 0 {
		return ""
	}
	if n <= 6 {
		return strings.Repeat("*", n)
	}
	visible := 6
	if n <= 10 {
		visible = 2
	} else if n <= 20 {
		visible = 4
	}
	return string(utf16.Decode(units[:visible/2])) + strings.Repeat("*", n-visible) + string(utf16.Decode(units[n-visible/2:]))
}

func encode(value, key string) string {
	v, k := []byte(value), []byte(key)
	encoded := make([]byte, len(v))
	for i := range v {
		encoded[i] = v[i] ^ k[i%len(k)]
	}
	return key + base64.StdEncoding.EncodeToString(encoded)
}

func decode(encoded, key string) (string, error) {
	if !strings.HasPrefix(encoded, key) {
		return "", errors.New("sensitive field prefix mismatch")
	}
	b, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(encoded, key))
	if err != nil {
		return "", err
	}
	k := []byte(key)
	for i := range b {
		b[i] ^= k[i%len(k)]
	}
	return string(b), nil
}

type String string

func (v String) Value() string     { return string(v) }
func (v String) String() string    { return Mask(string(v)) }
func (v String) Serialize() string { return encode(string(v), "SSTR") }
func ParseString(encoded string) (String, error) {
	v, err := decode(encoded, "SSTR")
	return String(v), err
}
func (v String) MarshalJSON() ([]byte, error) { return json.Marshal(v.Serialize()) }
func (v *String) UnmarshalJSON(data []byte) error {
	var encoded string
	if err := json.Unmarshal(data, &encoded); err != nil {
		return err
	}
	decoded, err := ParseString(encoded)
	if err != nil {
		return err
	}
	*v = decoded
	return nil
}

type Long int64

func (v Long) Value() int64      { return int64(v) }
func (v Long) String() string    { return Mask(strconv.FormatInt(int64(v), 10)) }
func (v Long) Serialize() string { return encode(strconv.FormatInt(int64(v), 10), "SLONG") }
func ParseLong(encoded string) (Long, error) {
	v, err := decode(encoded, "SLONG")
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid sensitive long: %w", err)
	}
	return Long(n), nil
}
func (v Long) MarshalJSON() ([]byte, error) { return json.Marshal(v.Serialize()) }
func (v *Long) UnmarshalJSON(data []byte) error {
	var encoded string
	if err := json.Unmarshal(data, &encoded); err != nil {
		return err
	}
	decoded, err := ParseLong(encoded)
	if err != nil {
		return err
	}
	*v = decoded
	return nil
}
