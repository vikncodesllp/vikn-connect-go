// Package testdb opens the scratch database DB-backed tests use. Tests skip
// when CONNECT_TEST_DSN is unset so `go test ./...` passes on a laptop with
// no Postgres; CI sets it. Each caller gets the listed tables recreated.
package testdb

import (
	"os"
	"testing"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func Open(t *testing.T, models ...any) *gorm.DB {
	t.Helper()
	dsn := os.Getenv("CONNECT_TEST_DSN")
	if dsn == "" {
		t.Skip("CONNECT_TEST_DSN not set")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Discard})
	if err != nil {
		t.Fatalf("open %s: %v", dsn, err)
	}
	if err := db.Migrator().DropTable(models...); err != nil {
		t.Fatalf("drop tables: %v", err)
	}
	if err := db.AutoMigrate(models...); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db
}
