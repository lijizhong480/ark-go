package legacycrypto

import (
	"errors"
	"testing"
)

func TestAESMatchesJavaVector(t *testing.T) {
	const key = "0123456789abcdef"
	const plaintext = "hello世界"
	const javaCipher = "DxjRZ576Fp10vyrND/rkbRMC8hXFgh0gOypu"
	actual, err := LegacyAESEncryptText(plaintext, key)
	if err != nil || actual != javaCipher {
		t.Fatalf("cipher = %q, %v", actual, err)
	}
	decoded, err := LegacyAESDecrypt(javaCipher, key)
	if err != nil || decoded != plaintext {
		t.Fatalf("plaintext = %q, %v", decoded, err)
	}
	if _, err := LegacyAESDecrypt(javaCipher, "fedcba9876543210"); err == nil {
		t.Fatal("wrong key accepted")
	}
}

func TestBase64JavaBlankBehavior(t *testing.T) {
	if Base64Text(" ") != " " {
		t.Fatal("blank text changed")
	}
	if Base64Text("世界") != "5LiW55WM" {
		t.Fatal("UTF-8 Base64 mismatch")
	}
	decoded, err := DecodeBase64Text("5LiW55WM")
	if err != nil || decoded != "世界" {
		t.Fatalf("decoded = %q, %v", decoded, err)
	}
	if _, err := DecodeBase64Bytes("_"); err == nil {
		t.Fatal("URL alphabet accepted")
	}
}

type decryptor func(string) (string, error)

func (d decryptor) Decrypt(s string) (string, error) { return d(s) }

func TestPBEWrapperDelegatesWithoutChoosingAlgorithm(t *testing.T) {
	called := false
	d := decryptor(func(s string) (string, error) {
		called = true
		if s != "cipher" {
			t.Fatalf("payload = %q", s)
		}
		return "plain", nil
	})
	plain, err := DecryptENC(d, "ENC(cipher)")
	if err != nil || plain != "plain" || !called {
		t.Fatalf("result = %q, %v, called=%v", plain, err, called)
	}
	plain, err = DecryptENC(nil, "ENC()")
	if err != nil || plain != "" {
		t.Fatalf("empty wrapper = %q, %v", plain, err)
	}
	plain, err = DecryptENC(nil, "plain")
	if err != nil || plain != "plain" {
		t.Fatalf("passthrough = %q, %v", plain, err)
	}
	if _, err := DecryptENC(nil, "ENC(cipher)"); err == nil {
		t.Fatal("missing decryptor accepted")
	}
	boom := errors.New("bad password")
	_, err = DecryptENC(decryptor(func(string) (string, error) { return "", boom }), "ENC(cipher)")
	if !errors.Is(err, boom) {
		t.Fatalf("decrypt error = %v", err)
	}
}
