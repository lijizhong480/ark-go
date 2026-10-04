package mysql

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestRealMySQL is opt-in and runs only against an isolated test database.
// Set ARK_MYSQL_TEST_DSN to a Go MySQL DSN with DDL privileges.
func TestRealMySQL(t *testing.T) {
	dsn := os.Getenv("ARK_MYSQL_TEST_DSN")
	if dsn == "" { t.Skip("set ARK_MYSQL_TEST_DSN for a real MySQL integration test") }
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cluster, err := Open(ctx, Config{Primary: "serverWrite", Write: "serverWrite", Read: "serverRead", Strict: true, Sources: map[string]SourceConfig{"serverWrite": {DSN: dsn}, "serverRead": {DSN: dsn}}})
	if err != nil { t.Fatal(err) }
	defer cluster.Close()
	write, err := cluster.WriteDB()
	if err != nil { t.Fatal(err) }
	read, err := cluster.ReadDB()
	if err != nil { t.Fatal(err) }
	if write == read { t.Fatal("read/write routing selected same pool") }
	table := "ark_go_test_" + time.Now().Format("150405000000")
	if _, err := write.ExecContext(ctx, "CREATE TABLE "+table+" (id BIGINT PRIMARY KEY, note VARCHAR(40))"); err != nil { t.Fatal(err) }
	defer write.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+table)
	tx, err := cluster.BeginTx(ctx, nil)
	if err != nil { t.Fatal(err) }
	if _, err := tx.ExecContext(ctx, "INSERT INTO "+table+" (id, note) VALUES (1, 'ok')"); err != nil { tx.Rollback(); t.Fatal(err) }
	if err := tx.Commit(); err != nil { t.Fatal(err) }
	var note string
	if err := read.QueryRowContext(ctx, "SELECT note FROM "+table+" WHERE id=1").Scan(&note); err != nil || note != "ok" { t.Fatalf("note=%q err=%v", note, err) }
}
