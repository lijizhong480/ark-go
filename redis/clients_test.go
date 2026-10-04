package redis

import "testing"

func TestNamedClientsAndPrimary(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Primary = true
	clients, err := NewClients(map[string]Config{"main": cfg}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer clients.Close()
	if _, ok := clients.Primary(); !ok {
		t.Fatal("primary missing")
	}
	if _, ok := clients.Client("unknown"); ok {
		t.Fatal("unknown Redis client found")
	}
}

func TestEncryptedPasswordRequiresDecryptor(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Password = "ENC(ciphertext)"
	if _, err := NewClients(map[string]Config{"main": cfg}, nil); err == nil {
		t.Fatal("encrypted password accepted without decryptor")
	}
}
