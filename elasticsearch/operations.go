package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

// Save performs the same update-with-upsert operation as Java EsProxy.save.
func (c *Client) Save(ctx context.Context, index, id string, document any, refresh bool) error {
	if err := validSegment(id); err != nil {
		return err
	}
	body, err := encodeJSON(map[string]any{"doc": document, "upsert": document})
	if err != nil {
		return err
	}
	suffix := "/_update/" + url.PathEscape(id)
	if refresh {
		suffix += "?refresh=true"
	}
	_, err = c.doIndex(ctx, index, http.MethodPost, suffix, body, "application/json")
	return err
}

// Get returns found=false when the document does not exist.
func (c *Client) Get(ctx context.Context, index, id string) (json.RawMessage, bool, error) {
	if err := validSegment(id); err != nil {
		return nil, false, err
	}
	cluster, err := c.ClusterForIndex(ctx, index)
	if err != nil {
		return nil, false, err
	}
	status, data, err := c.request(ctx, cluster, http.MethodGet, "/"+url.PathEscape(index)+"/_doc/"+url.PathEscape(id), nil, "")
	if err != nil {
		return nil, false, err
	}
	if status == http.StatusNotFound {
		return nil, false, nil
	}
	if status < 200 || status >= 300 {
		return nil, false, &StatusError{status, string(data)}
	}
	var result struct {
		Source json.RawMessage `json:"_source"`
		Found  bool            `json:"found"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, false, err
	}
	return result.Source, result.Found, nil
}

type BulkItem struct {
	Operation string          `json:"operation"`
	ID        string          `json:"id"`
	Status    int             `json:"status"`
	Error     json.RawMessage `json:"error,omitempty"`
}
type BulkResult struct {
	Took   int64      `json:"took"`
	Errors bool       `json:"errors"`
	Items  []BulkItem `json:"items"`
}

func (c *Client) BulkSave(ctx context.Context, index string, documents map[string]any) (BulkResult, error) {
	if len(documents) == 0 {
		return BulkResult{}, nil
	}
	ids := make([]string, 0, len(documents))
	for id := range documents {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var body bytes.Buffer
	for _, id := range ids {
		if err := validSegment(id); err != nil {
			return BulkResult{}, err
		}
		meta, _ := json.Marshal(map[string]any{"update": map[string]any{"_index": index, "_id": id}})
		doc, err := json.Marshal(map[string]any{"doc": documents[id], "upsert": documents[id]})
		if err != nil {
			return BulkResult{}, err
		}
		body.Write(meta)
		body.WriteByte('\n')
		body.Write(doc)
		body.WriteByte('\n')
	}
	return c.bulk(ctx, index, body.Bytes())
}

func (c *Client) BulkDelete(ctx context.Context, index string, ids []string) (BulkResult, error) {
	if len(ids) == 0 {
		return BulkResult{}, nil
	}
	var body bytes.Buffer
	for _, id := range ids {
		if err := validSegment(id); err != nil {
			return BulkResult{}, err
		}
		meta, _ := json.Marshal(map[string]any{"delete": map[string]any{"_index": index, "_id": id}})
		body.Write(meta)
		body.WriteByte('\n')
	}
	return c.bulk(ctx, index, body.Bytes())
}

func (c *Client) bulk(ctx context.Context, index string, body []byte) (BulkResult, error) {
	data, err := c.doIndex(ctx, index, http.MethodPost, "/_bulk", body, "application/x-ndjson")
	if err != nil {
		return BulkResult{}, err
	}
	var raw struct {
		Took   int64 `json:"took"`
		Errors bool  `json:"errors"`
		Items  []map[string]struct {
			ID     string          `json:"_id"`
			Status int             `json:"status"`
			Error  json.RawMessage `json:"error"`
		} `json:"items"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return BulkResult{}, err
	}
	result := BulkResult{Took: raw.Took, Errors: raw.Errors}
	for _, item := range raw.Items {
		for op, detail := range item {
			result.Items = append(result.Items, BulkItem{Operation: op, ID: detail.ID, Status: detail.Status, Error: detail.Error})
		}
	}
	if raw.Errors {
		return result, errors.New("Elasticsearch bulk response contains item failures")
	}
	return result, nil
}

type Hit struct {
	Index  string            `json:"_index"`
	ID     string            `json:"_id"`
	Score  *float64          `json:"_score"`
	Source json.RawMessage   `json:"_source"`
	Sort   []json.RawMessage `json:"sort"`
}
type SearchResult struct {
	Took         int64  `json:"took"`
	ScrollID     string `json:"_scroll_id"`
	Total        int64
	Hits         []Hit
	Aggregations json.RawMessage `json:"aggregations"`
}

func parseSearch(data []byte) (SearchResult, error) {
	var raw struct {
		Took     int64  `json:"took"`
		ScrollID string `json:"_scroll_id"`
		Hits     struct {
			Total json.RawMessage `json:"total"`
			Hits  []Hit           `json:"hits"`
		} `json:"hits"`
		Aggregations json.RawMessage `json:"aggregations"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return SearchResult{}, err
	}
	result := SearchResult{Took: raw.Took, ScrollID: raw.ScrollID, Hits: raw.Hits.Hits, Aggregations: raw.Aggregations}
	if len(raw.Hits.Total) != 0 {
		if raw.Hits.Total[0] == '{' {
			var total struct {
				Value int64 `json:"value"`
			}
			if err := json.Unmarshal(raw.Hits.Total, &total); err != nil {
				return SearchResult{}, err
			}
			result.Total = total.Value
		} else if err := json.Unmarshal(raw.Hits.Total, &result.Total); err != nil {
			return SearchResult{}, err
		}
	}
	return result, nil
}

func (c *Client) Search(ctx context.Context, index string, query any) (SearchResult, error) {
	body, err := encodeJSON(query)
	if err != nil {
		return SearchResult{}, err
	}
	data, err := c.doIndex(ctx, index, http.MethodPost, "/_search", body, "application/json")
	if err != nil {
		return SearchResult{}, err
	}
	return parseSearch(data)
}

func (c *Client) Count(ctx context.Context, index string, query any) (int64, error) {
	body, err := encodeJSON(query)
	if err != nil {
		return 0, err
	}
	data, err := c.doIndex(ctx, index, http.MethodPost, "/_count", body, "application/json")
	if err != nil {
		return 0, err
	}
	var result struct {
		Count int64 `json:"count"`
	}
	err = json.Unmarshal(data, &result)
	return result.Count, err
}

func (c *Client) StartScroll(ctx context.Context, index string, query any, keepAlive string) (SearchResult, error) {
	if keepAlive == "" {
		return SearchResult{}, errors.New("scroll keep alive is empty")
	}
	body, err := encodeJSON(query)
	if err != nil {
		return SearchResult{}, err
	}
	data, err := c.doIndex(ctx, index, http.MethodPost, "/_search?scroll="+url.QueryEscape(keepAlive), body, "application/json")
	if err != nil {
		return SearchResult{}, err
	}
	return parseSearch(data)
}

func (c *Client) NextScroll(ctx context.Context, index, scrollID, keepAlive string) (SearchResult, error) {
	if scrollID == "" || keepAlive == "" {
		return SearchResult{}, errors.New("scroll ID and keep alive are required")
	}
	cluster, err := c.ClusterForIndex(ctx, index)
	if err != nil {
		return SearchResult{}, err
	}
	body, _ := json.Marshal(map[string]string{"scroll": keepAlive, "scroll_id": scrollID})
	status, data, err := c.request(ctx, cluster, http.MethodPost, "/_search/scroll", body, "application/json")
	if err != nil {
		return SearchResult{}, err
	}
	if status < 200 || status >= 300 {
		return SearchResult{}, &StatusError{status, string(data)}
	}
	return parseSearch(data)
}

func (c *Client) ClearScroll(ctx context.Context, index, scrollID string) error {
	if scrollID == "" {
		return errors.New("scroll ID is empty")
	}
	cluster, err := c.ClusterForIndex(ctx, index)
	if err != nil {
		return err
	}
	body, _ := json.Marshal(map[string][]string{"scroll_id": {scrollID}})
	status, data, err := c.request(ctx, cluster, http.MethodDelete, "/_search/scroll", body, "application/json")
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return &StatusError{status, string(data)}
	}
	return nil
}

// SearchAfterAll pages until no hits remain. The query must contain a stable
// sort. The last hit's sort values become the next page's search_after.
func (c *Client) SearchAfterAll(ctx context.Context, index string, query map[string]any) ([]Hit, error) {
	if query == nil || query["sort"] == nil {
		return nil, errors.New("search_after requires a stable sort")
	}
	page := make(map[string]any, len(query)+1)
	for k, v := range query {
		page[k] = v
	}
	var all []Hit
	previousCursor := ""
	for {
		result, err := c.Search(ctx, index, page)
		if err != nil {
			return nil, err
		}
		if len(result.Hits) == 0 {
			return all, nil
		}
		all = append(all, result.Hits...)
		last := result.Hits[len(result.Hits)-1]
		if len(last.Sort) == 0 {
			return nil, fmt.Errorf("last hit has no sort values")
		}
		cursor := string(joinSortValues(last.Sort))
		if cursor == previousCursor {
			return nil, errors.New("search_after cursor did not advance")
		}
		previousCursor = cursor
		var sortValues []any
		for _, value := range last.Sort {
			var decoded any
			if err := json.Unmarshal(value, &decoded); err != nil {
				return nil, err
			}
			sortValues = append(sortValues, decoded)
		}
		page["search_after"] = sortValues
		if size, ok := page["size"].(int); ok && len(result.Hits) < size {
			return all, nil
		}
	}
}

func joinSortValues(values []json.RawMessage) []byte {
	joined, _ := json.Marshal(values)
	return joined
}

// SQL uses the OpenDistro _opendistro/_sql endpoint used by Java EsProxy.
// It accepts string, number, boolean and nil parameters for positional ?.
func (c *Client) SQL(ctx context.Context, index, statement string, params []any) (json.RawMessage, error) {
	if strings.TrimSpace(statement) == "" {
		return nil, errors.New("SQL statement is empty")
	}
	cluster, err := c.ClusterForIndex(ctx, index)
	if err != nil {
		return nil, err
	}
	for _, param := range params {
		if !strings.Contains(statement, "?") {
			return nil, errors.New("too many SQL parameters")
		}
		var literal string
		switch value := param.(type) {
		case string:
			literal = "'" + strings.ReplaceAll(value, "'", "''") + "'"
		case bool:
			literal = strconv.FormatBool(value)
		case nil:
			literal = "NULL"
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, json.Number:
			literal = fmt.Sprint(value)
		default:
			return nil, fmt.Errorf("unsupported SQL parameter %T", param)
		}
		statement = strings.Replace(statement, "?", literal, 1)
	}
	if strings.Contains(statement, "?") {
		return nil, errors.New("missing SQL parameter")
	}
	body, err := json.Marshal(map[string]any{"query": statement})
	if err != nil {
		return nil, err
	}
	status, data, err := c.request(ctx, cluster, http.MethodPost, "/_opendistro/_sql?format=json", body, "application/json")
	if err != nil {
		return nil, err
	}
	if status < 200 || status >= 300 {
		return nil, &StatusError{status, string(data)}
	}
	return data, nil
}
