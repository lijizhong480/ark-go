package legacycrypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha1"
	"fmt"
)

// LegacyAESEncrypt preserves Java AesUtil's ciphertext format: a 128-bit AES
// key from Sun SHA1PRNG seeded with the UTF-8 key bytes, those bytes as the
// GCM nonce, a 128-bit tag, and padded standard Base64 output. The repeated
// nonce makes this unsuitable for new encryption protocols.
func LegacyAESEncrypt(plaintext []byte, key string) (string, error) {
	gcm, nonce, err := legacyGCM(key)
	if err != nil {
		return "", err
	}
	return Base64Bytes(gcm.Seal(nil, nonce, plaintext, nil)), nil
}

func LegacyAESEncryptText(plaintext, key string) (string, error) {
	return LegacyAESEncrypt([]byte(plaintext), key)
}

func LegacyAESDecrypt(ciphertext, key string) (string, error) {
	data, err := DecodeBase64Bytes(ciphertext)
	if err != nil {
		return "", err
	}
	gcm, nonce, err := legacyGCM(key)
	if err != nil {
		return "", err
	}
	plain, err := gcm.Open(nil, nonce, data, nil)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}

func legacyGCM(key string) (cipher.AEAD, []byte, error) {
	nonce := []byte(key)
	if len(nonce) == 0 {
		return nil, nil, fmt.Errorf("AES key/nonce must not be empty")
	}
	// Sun SHA1PRNG: engineSetSeed computes SHA1(seed), and the first
	// engineNextBytes block computes SHA1(state). Java's AES KeyGenerator
	// consumes the first 16 bytes for its default 128-bit key.
	state := sha1.Sum(nonce)
	first := sha1.Sum(state[:])
	block, err := aes.NewCipher(first[:16])
	if err != nil {
		return nil, nil, err
	}
	gcm, err := cipher.NewGCMWithNonceSize(block, len(nonce))
	if err != nil {
		return nil, nil, err
	}
	return gcm, nonce, nil
}
