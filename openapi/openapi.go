// Package openapi exposes an OpenAPI 3 document for explicitly registered Go routes.
package openapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

const DefaultPath = "/v3/api-docs"

type Config struct {
	Enable      bool
	Title       string
	Description string
	Version     string
	BasePackage string // Java swagger.basePackage maps to Route.Package.
	Path        string
}

type Route struct {
	Path        string
	Method      string
	Package     string
	OperationID string
	Summary     string
	Description string
	Tags        []string
	Parameters  []Parameter
	RequestBody *RequestBody
	Responses   map[string]Response
}

type Document struct {
	OpenAPI string                          `json:"openapi"`
	Info    Info                            `json:"info"`
	Paths   map[string]map[string]Operation `json:"paths"`
}

type Info struct {
	Title       string `json:"title"`
	Description string `json:"description,omitempty"`
	Version     string `json:"version"`
}

type Operation struct {
	OperationID string              `json:"operationId,omitempty"`
	Summary     string              `json:"summary,omitempty"`
	Description string              `json:"description,omitempty"`
	Tags        []string            `json:"tags,omitempty"`
	Parameters  []Parameter         `json:"parameters,omitempty"`
	RequestBody *RequestBody        `json:"requestBody,omitempty"`
	Responses   map[string]Response `json:"responses"`
}

type Response struct {
	Description string               `json:"description"`
	Content     map[string]MediaType `json:"content,omitempty"`
}

type MediaType struct {
	Schema Schema `json:"schema"`
}

type Schema struct {
	Ref         string            `json:"$ref,omitempty"`
	Type        string            `json:"type,omitempty"`
	Format      string            `json:"format,omitempty"`
	Description string            `json:"description,omitempty"`
	Properties  map[string]Schema `json:"properties,omitempty"`
	Items       *Schema           `json:"items,omitempty"`
	Required    []string          `json:"required,omitempty"`
}

type Parameter struct {
	Name        string `json:"name"`
	In          string `json:"in"`
	Required    bool   `json:"required,omitempty"`
	Description string `json:"description,omitempty"`
	Schema      Schema `json:"schema"`
}

type RequestBody struct {
	Description string               `json:"description,omitempty"`
	Required    bool                 `json:"required,omitempty"`
	Content     map[string]MediaType `json:"content"`
}

// Build checks duplicate methods and path shapes before publishing a document.
func Build(cfg Config, routes []Route) (Document, error) {
	if cfg.Version == "" {
		cfg.Version = "1.0.0"
	}
	doc := Document{OpenAPI: "3.0.3", Info: Info{Title: cfg.Title, Description: cfg.Description, Version: cfg.Version}, Paths: make(map[string]map[string]Operation)}
	for _, r := range routes {
		if cfg.BasePackage != "" && r.Package != cfg.BasePackage && !strings.HasPrefix(r.Package, cfg.BasePackage+"/") && !strings.HasPrefix(r.Package, cfg.BasePackage+".") {
			continue
		}
		method := strings.ToLower(strings.TrimSpace(r.Method))
		if r.Path == "" || r.Path[0] != '/' || !validMethod(method) {
			return Document{}, errors.New("invalid OpenAPI route path or method")
		}
		if doc.Paths[r.Path] == nil {
			doc.Paths[r.Path] = make(map[string]Operation)
		}
		if _, exists := doc.Paths[r.Path][method]; exists {
			return Document{}, errors.New("duplicate OpenAPI route")
		}
		responses := r.Responses
		if len(responses) == 0 {
			responses = map[string]Response{"200": {Description: "Success"}}
		}
		doc.Paths[r.Path][method] = Operation{OperationID: r.OperationID, Summary: r.Summary, Description: r.Description, Tags: append([]string(nil), r.Tags...), Parameters: append([]Parameter(nil), r.Parameters...), RequestBody: r.RequestBody, Responses: responses}
	}
	return doc, nil
}

func validMethod(m string) bool {
	switch m {
	case "get", "put", "post", "delete", "options", "head", "patch", "trace":
		return true
	}
	return false
}

// Handler returns a standalone endpoint. Disabled documentation has no route.
func Handler(cfg Config, routes []Route) (string, http.Handler, error) {
	if !cfg.Enable {
		return "", nil, nil
	}
	doc, err := Build(cfg, routes)
	if err != nil {
		return "", nil, err
	}
	body, err := json.Marshal(doc)
	if err != nil {
		return "", nil, err
	}
	path := cfg.Path
	if path == "" {
		path = DefaultPath
	}
	if path[0] != '/' {
		return "", nil, errors.New("OpenAPI path must start with /")
	}
	return path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Write(body)
	}), nil
}
