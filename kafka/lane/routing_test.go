package lane

import "testing"

func TestTrunkFallbackAndIsolation(t *testing.T) {
	registry := NewRegistry()
	trunk := Router{Enabled: true, Fallback: true, Registry: registry}
	if trunk.ShouldProcess("blue") {
		t.Fatal("fallback before first refresh")
	}
	registry.Update(map[string]bool{"blue": true})
	if trunk.ShouldProcess("blue") {
		t.Fatal("trunk consumed active lane")
	}
	if !trunk.ShouldProcess("red") {
		t.Fatal("trunk failed to consume inactive lane")
	}
	if !trunk.ShouldProcess("") {
		t.Fatal("trunk message skipped")
	}
	blue := Router{Enabled: true, InstanceTag: "blue"}
	if !blue.ShouldProcess("blue") || blue.ShouldProcess("red") || blue.ShouldProcess("") {
		t.Fatal("lane isolation")
	}
}

func TestProducerHeadersAreCopied(t *testing.T) {
	original := map[string][]byte{"existing": []byte("x")}
	result := (Router{Enabled: true, InstanceTag: "blue"}).ProducerHeaders(original, "trace-1")
	if string(result[RouterTagHeader]) != "blue" || string(result[TraceIDHeader]) != "trace-1" {
		t.Fatal(result)
	}
	if _, ok := original[RouterTagHeader]; ok {
		t.Fatal("original mutated")
	}
}
