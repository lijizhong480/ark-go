package openapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandler(t *testing.T) {
	routes := []Route{{Path: "/api/users", Method: "GET", Package: "app/api", OperationID: "users"}, {Path: "/private", Method: "POST", Package: "other"}}
	path, h, err := Handler(Config{Enable: true, Title: "Ark", BasePackage: "app"}, routes)
	if err != nil || path != DefaultPath {
		t.Fatalf("handler: %q %v", path, err)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	var doc Document
	if err := json.Unmarshal(w.Body.Bytes(), &doc); err != nil || doc.OpenAPI != "3.0.3" || doc.Info.Version != "1.0.0" || len(doc.Paths) != 1 {
		t.Fatalf("document: %s %v", w.Body.String(), err)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status: %d", w.Code)
	}
	path, h, err = Handler(Config{}, routes)
	if err != nil || h != nil || path != "" {
		t.Fatal("disabled")
	}
}

func TestDuplicate(t *testing.T) {
	_, err := Build(Config{}, []Route{{Path: "/x", Method: "GET"}, {Path: "/x", Method: "get"}})
	if err == nil {
		t.Fatal("duplicate accepted")
	}
}

func TestExplicitSchemas(t *testing.T) {
	request := &RequestBody{Required: true, Content: map[string]MediaType{"application/json": {Schema: Schema{Type: "object", Properties: map[string]Schema{"name": {Type: "string"}}, Required: []string{"name"}}}}}
	doc, err := Build(Config{}, []Route{{Path: "/users", Method: "POST", RequestBody: request, Responses: map[string]Response{"201": {Description: "Created", Content: map[string]MediaType{"application/json": {Schema: Schema{Type: "object"}}}}}}})
	if err != nil {
		t.Fatal(err)
	}
	op := doc.Paths["/users"]["post"]
	if op.RequestBody == nil || !op.RequestBody.Required || op.Responses["201"].Content["application/json"].Schema.Type != "object" {
		t.Fatalf("schema: %+v", op)
	}
}
