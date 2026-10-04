package sensitive

import (
	"encoding/json"
	"testing"
)

func TestSensitiveStringJavaWireAndMask(t *testing.T) {
	v := String("12345678901")
	if v.String() != "12*******01" {
		t.Fatalf("mask = %q", v.String())
	}
	if v.Serialize() != "SSTRYmFnZmZlY2pqY2U=" {
		t.Fatalf("encoded = %q", v.Serialize())
	}
	b, err := json.Marshal(v)
	if err != nil || string(b) != `"SSTRYmFnZmZlY2pqY2U="` {
		t.Fatalf("json = %s, %v", b, err)
	}
	var got String
	if err := json.Unmarshal(b, &got); err != nil || got.Value() != v.Value() {
		t.Fatalf("decoded = %q, %v", got.Value(), err)
	}
	if _, err := ParseString("BADabc"); err == nil {
		t.Fatal("wrong prefix accepted")
	}
}

func TestSensitiveLongRoundTrip(t *testing.T) {
	v := Long(123456789)
	got, err := ParseLong(v.Serialize())
	if err != nil || got != v {
		t.Fatalf("decoded = %d, %v", got, err)
	}
	if Mask("123456") != "******" {
		t.Fatal("short mask mismatch")
	}
	if NewConfig().Mode != BlacklistMode {
		t.Fatal("default mode mismatch")
	}
}
