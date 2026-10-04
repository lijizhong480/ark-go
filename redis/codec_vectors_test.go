package redis

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"os"
	"strings"
	"testing"
)

func javaVectors(t *testing.T) map[string][]byte {
	t.Helper()
	f, err := os.Open("testdata/redis_vectors.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	vectors := make(map[string][]byte)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		name, encoded, ok := strings.Cut(scanner.Text(), "=")
		if !ok {
			t.Fatal("invalid Java Redis vector")
		}
		value, err := hex.DecodeString(encoded)
		if err != nil {
			t.Fatal(err)
		}
		vectors[name] = value
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return vectors
}

func TestJavaRedisByteVectors(t *testing.T) {
	vectors := javaVectors(t)
	stringCodec, err := NewJavaCompatibleCodec("string", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	value, err := stringCodec.Marshal("héllo")
	if err != nil || !bytes.Equal(value, vectors["string"]) {
		t.Fatalf("string: %x %v", value, err)
	}
	var decoded string
	if err := stringCodec.Unmarshal(vectors["string"], &decoded); err != nil || decoded != "héllo" {
		t.Fatalf("string decode: %q %v", decoded, err)
	}

	typedCodec, err := NewJavaCompatibleCodec("json", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	value, err = typedCodec.Marshal("hello")
	if err != nil || !bytes.Equal(value, vectors["jsonString"]) {
		t.Fatalf("typed JSON String: %x %v", value, err)
	}
	if err := typedCodec.Unmarshal(vectors["jsonString"], &decoded); err != nil || decoded != "hello" {
		t.Fatalf("typed JSON String decode: %q %v", decoded, err)
	}
	if _, err := typedCodec.Marshal(map[string]any{"name": "Ark"}); err == nil {
		t.Fatal("typed map accepted without @class support")
	}

	plainCodec, err := NewJavaCompatibleCodec("json", false, nil)
	if err != nil {
		t.Fatal(err)
	}
	plain := struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}{"Ark", 2}
	value, err = plainCodec.Marshal(plain)
	if err != nil || !bytes.Equal(value, vectors["jsonMapPlain"]) {
		t.Fatalf("plain JSON map: %x %v", value, err)
	}
	if err := plainCodec.Unmarshal(vectors["jsonMapPlain"], &plain); err != nil || plain.Name != "Ark" || plain.Count != 2 {
		t.Fatalf("plain JSON decode: %+v %v", plain, err)
	}

	if !bytes.HasPrefix(vectors["jdkString"], []byte{0xac, 0xed, 0x00, 0x05}) {
		t.Fatal("JDK vector missing stream header")
	}
	if _, err := NewJavaCompatibleCodec("jdk", false, nil); err == nil {
		t.Fatal("JDK codec accepted")
	}
	if _, err := NewJavaCompatibleCodec("custom", false, nil); err == nil {
		t.Fatal("unvalidated custom codec accepted")
	}
	if _, err := NewJavaCompatibleCodec("bogus", false, nil); err == nil {
		t.Fatal("unknown codec accepted")
	}
}
