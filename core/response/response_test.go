package response

import (
	"encoding/json"
	"testing"
)

func TestJavaWireContract(t *testing.T) {
	r := OK(map[string]int{"count": 2})
	r.Timestamp = 1728000000123
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"code":0,"timestamp":1728000000123,"data":{"count":2}}`
	if string(b) != want {
		t.Fatalf("wire response = %s, want %s", b, want)
	}
	if !r.IsSuccess() {
		t.Fatal("success code must be successful")
	}
	explicit := NewWithData(Success, "", map[string]int{"count": 2})
	explicit.Timestamp = 1728000000123
	explicit.SetID("")
	explicit.SetDataType("")
	b, err = json.Marshal(explicit)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"code":0,"id":"","msg":"","timestamp":1728000000123,"dataType":"","data":{"count":2}}` {
		t.Fatalf("explicit empty fields omitted: %s", b)
	}
	failure := Failure(InternalServerError, "failed")
	failure.Timestamp = 1
	b, err = json.Marshal(failure)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"code":100500,"msg":"failed","timestamp":1}` {
		t.Fatalf("failure wire response = %s", b)
	}
}

func TestJavaDuplicateCodeLookup(t *testing.T) {
	d, ok := Lookup(APIKeyIllegal)
	if !ok || d.Name != "MQ_404" {
		t.Fatalf("duplicate code lookup = %+v, %v", d, ok)
	}
	if d.I18nMsgKey() != "errorCode_MQ_404" {
		t.Fatalf("i18n key = %s", d.I18nMsgKey())
	}
}
