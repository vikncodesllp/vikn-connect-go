package link

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/vikncodesllp/vikn-connect-go/internal/testdb"
)

func TestValidate(t *testing.T) {
	l := Link{OrganizationID: uuid.New(), LocalType: "issue", LocalID: uuid.New(), RemoteApp: "vikn-desk", RemoteType: "ticket", Direction: DirectionInbound}
	if err := l.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	l.Direction = "sideways"
	if err := l.Validate(); err == nil {
		t.Fatal("expected a bad direction to fail")
	}
}

func TestUpsertKeepsTheIdAndRefreshesTheRemoteSide(t *testing.T) {
	db := testdb.Open(t, Models()...)
	org, issue, ticket := uuid.New(), uuid.New(), uuid.New()
	first, err := Upsert(db, Link{OrganizationID: org, LocalType: "issue", LocalID: issue, LocalRef: "PROJ-1",
		RemoteApp: "vikn-desk", RemoteType: "ticket", RemoteID: &ticket, RemoteRef: "TKT-1", Direction: DirectionInbound})
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	second, err := Upsert(db, Link{OrganizationID: org, LocalType: "issue", LocalID: issue, LocalRef: "PROJ-1",
		RemoteApp: "vikn-desk", RemoteType: "ticket", RemoteID: &ticket, RemoteRef: "TKT-1", RemoteStatus: "open", Direction: DirectionInbound})
	if err != nil {
		t.Fatalf("Upsert again: %v", err)
	}
	if second.ID != first.ID || second.RemoteStatus != "open" {
		t.Fatalf("second = %+v", second)
	}
	links, err := ForLocal(db, org, "issue", issue)
	if err != nil || len(links) != 1 {
		t.Fatalf("ForLocal = %d, %v", len(links), err)
	}
	at := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	if err := SetRemoteStatus(db, org, first.ID, "done", at); err != nil {
		t.Fatalf("SetRemoteStatus: %v", err)
	}
	got, _ := FindByID(db, org, first.ID)
	if got.RemoteStatus != "done" || got.RemoteUpdatedAt == nil || !got.RemoteUpdatedAt.Equal(at) {
		t.Fatalf("got = %+v", got)
	}
	if err := SetRemoteStatus(db, org, uuid.New(), "x", at); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("unknown link: %v", err)
	}
	// A second active row for the same key is refused by the partial index.
	if err := db.Create(&Link{OrganizationID: org, LocalType: "issue", LocalID: issue, RemoteApp: "vikn-desk", RemoteType: "ticket", Direction: DirectionInbound}).Error; err == nil {
		t.Fatal("duplicate active link should violate the unique index")
	}
	// Soft-deleting frees the key.
	db.Model(&Link{}).Where("id = ?", first.ID).Update("entry_status", 2)
	if _, err := Find(db, Key{org, "issue", issue, "vikn-desk", "ticket"}); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("deleted link should not be found: %v", err)
	}
}
