package mongodb

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var fieldPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.]*$`)

type Collection struct {
	backend                  Backend
	metric                   MetricHook
	endpoint, database, name string
	readPreference           string
	majority                 bool
}

func (c *Collection) WithReadPreference(pref string) *Collection {
	copy := *c
	copy.readPreference = pref
	return &copy
}
func (c *Collection) WithMajority() *Collection { copy := *c; copy.majority = true; return &copy }
func (c *Collection) record(operation string, start time.Time, err error) {
	if c.metric != nil {
		c.metric(Metric{Operation: operation, Endpoint: c.endpoint, Database: c.database, Collection: c.name, Success: err == nil, Duration: time.Since(start)})
	}
}
func (c *Collection) InsertOne(ctx context.Context, document any) (id any, err error) {
	start := time.Now()
	defer func() { c.record("insert", start, err) }()
	return c.backend.InsertOne(ctx, document, c.majority)
}
func (c *Collection) InsertMany(ctx context.Context, documents []any) (ids []any, err error) {
	start := time.Now()
	defer func() { c.record("insertMany", start, err) }()
	return c.backend.InsertMany(ctx, documents, c.majority)
}
func (c *Collection) UpdateOne(ctx context.Context, filter, update any) (modified int64, err error) {
	start := time.Now()
	defer func() { c.record("updateFirst", start, err) }()
	return c.backend.UpdateOne(ctx, filter, update, c.majority)
}
func (c *Collection) UpdateMany(ctx context.Context, filter, update any) (modified int64, err error) {
	start := time.Now()
	defer func() { c.record("updateMulti", start, err) }()
	return c.backend.UpdateMany(ctx, filter, update, c.majority)
}
func (c *Collection) DeleteMany(ctx context.Context, filter any) (deleted int64, err error) {
	start := time.Now()
	defer func() { c.record("remove", start, err) }()
	return c.backend.DeleteMany(ctx, filter, true)
}
func (c *Collection) Find(ctx context.Context, filter any, skip, limit int64, sort any) (items []bson.M, err error) {
	if skip < 0 || limit < 0 {
		return nil, errors.New("mongodb: negative skip or limit")
	}
	start := time.Now()
	defer func() { c.record("find", start, err) }()
	return c.backend.Find(ctx, filter, skip, limit, sort, c.readPreference)
}
func (c *Collection) Count(ctx context.Context, filter any) (total int64, err error) {
	start := time.Now()
	defer func() { c.record("count", start, err) }()
	return c.backend.Count(ctx, filter, c.readPreference)
}
func (c *Collection) Aggregate(ctx context.Context, pipeline any) (items []bson.M, err error) {
	start := time.Now()
	defer func() { c.record("aggregate", start, err) }()
	return c.backend.Aggregate(ctx, pipeline, c.readPreference)
}

type Page[T any] struct {
	Total int64 `json:"total"`
	Items []T   `json:"items"`
}

// FindPage counts using the original filter, then applies skip/limit only to
// the item query. It never mutates caller-owned filter or options.
func FindPage[T any](ctx context.Context, c *Collection, filter any, skip, limit int64, sort any) (Page[T], error) {
	var page Page[T]
	if skip < 0 || limit <= 0 {
		return page, errors.New("mongodb: skip must be nonnegative and limit positive")
	}
	total, err := c.Count(ctx, filter)
	if err != nil {
		return page, err
	}
	docs, err := c.Find(ctx, filter, skip, limit, sort)
	if err != nil {
		return page, err
	}
	page.Total = total
	page.Items = make([]T, 0, len(docs))
	for _, doc := range docs {
		raw, err := bson.Marshal(doc)
		if err != nil {
			return Page[T]{}, err
		}
		var item T
		if err := bson.Unmarshal(raw, &item); err != nil {
			return Page[T]{}, err
		}
		page.Items = append(page.Items, item)
	}
	return page, nil
}

func (c *Collection) Sum(ctx context.Context, filter any, field string) (*big.Rat, error) {
	if !fieldPattern.MatchString(field) {
		return nil, fmt.Errorf("mongodb: invalid sum field %q", field)
	}
	pipeline := bson.A{}
	if filter != nil {
		pipeline = append(pipeline, bson.D{{Key: "$match", Value: filter}})
	}
	pipeline = append(pipeline, bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: nil}, {Key: "sum", Value: bson.D{{Key: "$sum", Value: bson.D{{Key: "$toDecimal", Value: "$" + field}}}}}}}})
	docs, err := c.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	if len(docs) == 0 {
		return new(big.Rat), nil
	}
	return numberRat(docs[0]["sum"])
}
func (c *Collection) GroupCount(ctx context.Context, filter any, field string) ([]GroupResult, error) {
	if !fieldPattern.MatchString(field) {
		return nil, fmt.Errorf("mongodb: invalid group field %q", field)
	}
	pipeline := groupPipeline(filter, "$"+field)
	docs, err := c.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	out := make([]GroupResult, 0, len(docs))
	for _, doc := range docs {
		count, err := numberInt64(doc["count"])
		if err != nil {
			return nil, err
		}
		out = append(out, GroupResult{Key: doc["_id"], Count: count})
	}
	return out, nil
}
func (c *Collection) GroupCountByFields(ctx context.Context, filter any, fields []string) ([]MultiFieldGroupResult, error) {
	if len(fields) == 0 {
		return nil, errors.New("mongodb: empty group fields")
	}
	id := bson.D{}
	for _, field := range fields {
		if !fieldPattern.MatchString(field) {
			return nil, fmt.Errorf("mongodb: invalid group field %q", field)
		}
		id = append(id, bson.E{Key: field, Value: "$" + field})
	}
	docs, err := c.Aggregate(ctx, groupPipeline(filter, id))
	if err != nil {
		return nil, err
	}
	out := make([]MultiFieldGroupResult, 0, len(docs))
	for _, doc := range docs {
		count, err := numberInt64(doc["count"])
		if err != nil {
			return nil, err
		}
		group := bson.M{}
		switch id := doc["_id"].(type) {
		case bson.M:
			group = id
		case map[string]any:
			for k, v := range id {
				group[k] = v
			}
		default:
			return nil, fmt.Errorf("mongodb: unexpected group key %T", doc["_id"])
		}
		out = append(out, MultiFieldGroupResult{GroupFields: group, Count: count})
	}
	return out, nil
}
func groupPipeline(filter, id any) bson.A {
	pipeline := bson.A{}
	if filter != nil {
		pipeline = append(pipeline, bson.D{{Key: "$match", Value: filter}})
	}
	return append(pipeline, bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: id}, {Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}}}}})
}

type GroupResult struct {
	Key   any   `json:"key"`
	Count int64 `json:"count"`
}
type MultiFieldGroupResult struct {
	GroupFields bson.M `json:"groupFields"`
	Count       int64  `json:"count"`
}

func numberInt64(value any) (int64, error) {
	switch n := value.(type) {
	case int:
		return int64(n), nil
	case int32:
		return int64(n), nil
	case int64:
		return n, nil
	case float64:
		return int64(n), nil
	default:
		return 0, fmt.Errorf("mongodb: nonnumeric count %T", value)
	}
}
func numberRat(value any) (*big.Rat, error) {
	switch n := value.(type) {
	case primitive.Decimal128:
		bigint, exp, err := n.BigInt()
		if err != nil {
			return nil, err
		}
		r := new(big.Rat).SetInt(bigint)
		if exp < 0 {
			return r.Quo(r, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-exp)), nil))), nil
		}
		return r.Mul(r, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(exp)), nil))), nil
	case int:
		return new(big.Rat).SetInt64(int64(n)), nil
	case int32:
		return new(big.Rat).SetInt64(int64(n)), nil
	case int64:
		return new(big.Rat).SetInt64(n), nil
	case float64:
		return new(big.Rat).SetFloat64(n), nil
	default:
		return nil, fmt.Errorf("mongodb: nonnumeric sum %T", value)
	}
}
