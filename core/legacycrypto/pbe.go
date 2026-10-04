package legacycrypto

import (
	"errors"
	"strings"
)

// StringDecryptor corresponds to the injected Jasypt StringEncryptor. The
// Java utility specifies no PBE algorithm or password; callers must supply
// an implementation with matching settings.
type StringDecryptor interface{ Decrypt(string) (string, error) }

// DecryptENC preserves PBEDecryptorUtil's wrapper behavior. Plain values pass
// through; ENC() returns an empty string; the final byte is removed exactly
// as Java does once the string starts with ENC( and is longer than five bytes.
func DecryptENC(decryptor StringDecryptor, secret string) (string, error) {
	if secret == "" || !strings.HasPrefix(secret, "ENC(") {
		return secret, nil
	}
	if len(secret) <= 5 {
		return "", nil
	}
	if decryptor == nil {
		return "", errors.New("ENC value requires a decryptor")
	}
	return decryptor.Decrypt(secret[4 : len(secret)-1])
}
