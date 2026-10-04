package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
)

var fakeOnce sync.Once
var fakeState struct {
	sync.Mutex
	opened, closed, begun, committed int
}

type fakeDriver struct{}

func (fakeDriver) Open(dsn string) (driver.Conn, error) {
	fakeState.Lock()
	fakeState.opened++
	fakeState.Unlock()
	return &fakeConn{}, nil
}

type fakeConn struct{}

func (*fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unused") }
func (*fakeConn) Close() error                        { fakeState.Lock(); fakeState.closed++; fakeState.Unlock(); return nil }
func (*fakeConn) Begin() (driver.Tx, error) {
	fakeState.Lock()
	fakeState.begun++
	fakeState.Unlock()
	return fakeTx{}, nil
}
func (*fakeConn) Ping(context.Context) error { return nil }
func (*fakeConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return (&fakeConn{}).Begin()
}

type fakeTx struct{}

func (fakeTx) Commit() error   { fakeState.Lock(); fakeState.committed++; fakeState.Unlock(); return nil }
func (fakeTx) Rollback() error { return nil }
func fakeOpen(_ string, dsn string) (*sql.DB, error) {
	fakeOnce.Do(func() { sql.Register("ark-fake", fakeDriver{}) })
	return sql.Open("ark-fake", dsn)
}

func TestParseJavaPropertiesAndDSN(t *testing.T) {
	cfg, err := ParseJavaProperties(map[string]string{
		"spring.datasource.dynamic.primary":                                 "serverWrite",
		"spring.datasource.dynamic.strict":                                  "false",
		"spring.datasource.dynamic.datasource.serverWrite.url":              "jdbc:mysql://localhost:3306/orders?allowMultiQueries=true&serverTimezone=GMT%2B8&useSSL=false",
		"spring.datasource.dynamic.datasource.serverWrite.username":         "writer",
		"spring.datasource.dynamic.datasource.serverWrite.password":         "secret",
		"spring.datasource.dynamic.datasource.serverWrite.druid.max-active": "50",
		"spring.datasource.dynamic.datasource.serverWrite.druid.min-idle":   "30",
		"spring.datasource.dynamic.datasource.serverRead.url":               "jdbc:mysql://localhost:3306/orders",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Primary != "serverWrite" || cfg.Sources["serverWrite"].MaxOpenConns != 50 || cfg.Sources["serverWrite"].MaxIdleConns != 30 {
		t.Fatalf("parsed config: %+v", cfg)
	}
	dsn, err := makeDSN(cfg.Sources["serverWrite"])
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.User != "writer" || parsed.Passwd != "secret" || parsed.DBName != "orders" || !parsed.MultiStatements || parsed.Loc.String() != "Asia/Shanghai" {
		t.Fatalf("DSN: %+v", parsed)
	}
}
func TestClusterRoutingTransactionClose(t *testing.T) {
	cfg := Config{Primary: "serverWrite", Sources: map[string]SourceConfig{"serverWrite": {DSN: "writer@tcp(localhost:3306)/orders"}, "serverRead": {DSN: "reader@tcp(localhost:3306)/orders"}}}
	cluster, err := OpenWithOpener(context.Background(), cfg, fakeOpen)
	if err != nil {
		t.Fatal(err)
	}
	write, _ := cluster.WriteDB()
	read, _ := cluster.ReadDB()
	if write == read {
		t.Fatal("read did not route to replica")
	}
	fallback, err := cluster.DB("unknown")
	if err != nil || fallback != write {
		t.Fatalf("non-strict fallback: %v", err)
	}
	tx, err := cluster.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	fakeState.Lock()
	begun, committed := fakeState.begun, fakeState.committed
	fakeState.Unlock()
	if begun == 0 || committed == 0 {
		t.Fatalf("transaction: %d %d", begun, committed)
	}
	if err := cluster.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.DB(""); err == nil {
		t.Fatal("closed cluster served DB")
	}
}
func TestStrictAndFailureValidation(t *testing.T) {
	cfg := Config{Primary: "serverWrite", Strict: true, Sources: map[string]SourceConfig{"serverWrite": {DSN: "writer@tcp(localhost:3306)/orders"}}}
	cluster, err := OpenWithOpener(context.Background(), cfg, fakeOpen)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	if _, err := cluster.DB("unknown"); err == nil {
		t.Fatal("expected unknown source error")
	}
	if _, err := ParseJavaProperties(map[string]string{"spring.datasource.dynamic.primary": "missing"}); err == nil {
		t.Fatal("expected validation error")
	}
	if _, err := makeDSN(SourceConfig{DSN: "jdbc:mysql://localhost"}); err == nil {
		t.Fatal("expected malformed JDBC URL error")
	}
	if _, err := makeDSN(SourceConfig{DSN: "jdbc:mysql://localhost:3306/orders?serverTimezone=Typo%2FInvalid"}); err == nil {
		t.Fatal("invalid timezone must be rejected to avoid silent data time shifts")
	}
	if _, err := ParseJavaProperties(map[string]string{"spring.datasource.dynamic.primary": "a", "spring.datasource.dynamic.datasource.a.url": "db", "spring.datasource.dynamic.datasource.a.druid.max-active": "oops"}); err == nil || !strings.Contains(err.Error(), "max-active") {
		t.Fatalf("expected pool parse error: %v", err)
	}
}
