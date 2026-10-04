package legacycrypto

import (
	"encoding/base64"
	"strings"
)

// Base64Bytes uses the padded standard alphabet used by Java Base64Util.
func Base64Bytes(data []byte) string { return base64.StdEncoding.EncodeToString(data) }

func Base64Text(text string) string {
	if strings.TrimSpace(text) == "" {
		return text
	}
	return Base64Bytes([]byte(text))
}

func DecodeBase64Bytes(text string) ([]byte, error) {
	if strings.TrimSpace(text) == "" {
		return []byte{}, nil
	}
	return base64.StdEncoding.DecodeString(text)
}

func DecodeBase64Text(text string) (string, error) {
	if strings.TrimSpace(text) == "" {
		return text, nil
	}
	b, err := DecodeBase64Bytes(text)
	return string(b), err
}
