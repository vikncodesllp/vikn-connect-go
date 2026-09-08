// Package link is the keystone record of vikn-connect.md §4: this local
// record ↔ that remote record, stored on both sides in each app's own
// database and correlated by a link id the initiating app mints.
package link

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	connect "github.com/vikncodesllp/vikn-connect-go"
)

const (
	// DirectionOutbound: this app minted the link (it initiated the pair).
	DirectionOutbound = "outbound"
	// DirectionInbound: this app mirrored a link the far side minted.
	DirectionInbound = "inbound"
)

// Link is present in every participating app with the same shape.
type Link struct {
	ID             uuid.UUID `gorm:"type:uuid;primaryKey" json:"id"`
	OrganizationID uuid.UUID `gorm:"type:uuid;not null;index;uniqueIndex:idx_integration_links_active,where:entry_status <> 2" json:"organization_id"`

	LocalType string    `gorm:"type:varchar(60);not null;uniqueIndex:idx_integration_links_active,where:entry_status <> 2" json:"local_type"`
	LocalID   uuid.UUID `gorm:"type:uuid;not null;index;uniqueIndex:idx_integration_links_active,where:entry_status <> 2" json:"local_id"`
	LocalRef  string    `gorm:"type:varchar(120);not null;default:''" json:"local_ref"`

	RemoteApp  string     `gorm:"type:varchar(60);not null;uniqueIndex:idx_integration_links_active,where:entry_status <> 2" json:"remote_app"`
	RemoteType string     `gorm:"type:varchar(60);not null;uniqueIndex:idx_integration_links_active,where:entry_status <> 2" json:"remote_type"`
	RemoteID   *uuid.UUID `gorm:"type:uuid;index" json:"remote_id,omitempty"`
	RemoteRef  string     `gorm:"type:varchar(120);not null;default:''" json:"remote_ref"`

	// Denormalized cache maintained by inbound events. Never authoritative;
	// anything a user drills into is fetched live (§6).
	RemoteStatus    string     `gorm:"type:varchar(80);not null;default:''" json:"remote_status"`
	RemoteUpdatedAt *time.Time `json:"remote_updated_at,omitempty"`

	Direction   string     `gorm:"type:varchar(12);not null" json:"direction"`
	RuleID      *uuid.UUID `gorm:"type:uuid" json:"rule_id,omitempty"`
	EntryStatus int        `gorm:"type:int;not null;default:0" json:"entry_status"`
	CreatedAt   time.Time  `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt   time.Time  `gorm:"autoUpdateTime" json:"updated_at"`
}

func (Link) TableName() string { return "integration_links" }

func (l *Link) BeforeCreate(tx *gorm.DB) error {
	if l.ID == uuid.Nil {
		l.ID = uuid.New()
	}
	return l.Validate()
}

// Validate applies the shape rules before a row is written.
func (l Link) Validate() error {
	switch {
	case l.OrganizationID == uuid.Nil:
		return errors.New("link organization id is required")
	case l.LocalType == "" || l.LocalID == uuid.Nil:
		return errors.New("link local type and id are required")
	case l.RemoteApp == "" || l.RemoteType == "":
		return errors.New("link remote app and type are required")
	case l.RemoteID != nil && *l.RemoteID == uuid.Nil:
		return errors.New("link remote id must be nil or a uuid")
	case l.Direction != DirectionOutbound && l.Direction != DirectionInbound:
		return fmt.Errorf("link direction %q must be %s or %s", l.Direction, DirectionOutbound, DirectionInbound)
	}
	return nil
}

// Key is the uniqueness of an active link.
type Key struct {
	OrganizationID uuid.UUID
	LocalType      string
	LocalID        uuid.UUID
	RemoteApp      string
	RemoteType     string
}

func (k Key) scope(db *gorm.DB) *gorm.DB {
	return db.Where("organization_id = ? AND local_type = ? AND local_id = ? AND remote_app = ? AND remote_type = ? AND entry_status <> ?",
		k.OrganizationID, k.LocalType, k.LocalID, k.RemoteApp, k.RemoteType, connect.EntryStatusDeleted)
}

// Find returns the active link for a key, or gorm.ErrRecordNotFound.
func Find(db *gorm.DB, key Key) (Link, error) {
	var l Link
	err := key.scope(db).First(&l).Error
	return l, err
}

// FindByID returns an active link by its id, or gorm.ErrRecordNotFound.
func FindByID(db *gorm.DB, organizationID, id uuid.UUID) (Link, error) {
	var l Link
	err := db.Where("id = ? AND organization_id = ? AND entry_status <> ?", id, organizationID, connect.EntryStatusDeleted).First(&l).Error
	return l, err
}

// ForLocal lists active links for one local record, newest first.
func ForLocal(db *gorm.DB, organizationID uuid.UUID, localType string, localID uuid.UUID) ([]Link, error) {
	var links []Link
	err := db.Where("organization_id = ? AND local_type = ? AND local_id = ? AND entry_status <> ?",
		organizationID, localType, localID, connect.EntryStatusDeleted).Order("created_at DESC").Find(&links).Error
	return links, err
}

// ErrDeleted is returned by Upsert when the link id named by the caller
// was deliberately deleted here: a replayed announcement of it from the far
// side does not bring it back.
var ErrDeleted = errors.New("link was deleted")

// Upsert creates the link, or refreshes the remote side of the active row
// that already exists for its key. The id of an existing row wins, so the
// far side's mirror keeps pointing at the same link. A row that exists
// under the given id but was deleted is left alone and reported as
// ErrDeleted, so a consumer replaying history skips it.
func Upsert(db *gorm.DB, l Link) (Link, error) {
	if err := l.Validate(); err != nil {
		return Link{}, err
	}
	existing, err := Find(db, Key{l.OrganizationID, l.LocalType, l.LocalID, l.RemoteApp, l.RemoteType})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		if l.ID != uuid.Nil {
			var count int64
			if err := db.Model(&Link{}).Where("id = ?", l.ID).Count(&count).Error; err != nil {
				return Link{}, err
			}
			if count > 0 {
				return Link{}, ErrDeleted
			}
		}
		if err := db.Create(&l).Error; err != nil {
			return Link{}, err
		}
		return l, nil
	}
	if err != nil {
		return Link{}, err
	}
	updates := map[string]any{
		"remote_id": l.RemoteID, "remote_ref": l.RemoteRef, "local_ref": l.LocalRef,
	}
	if l.RemoteStatus != "" {
		updates["remote_status"], updates["remote_updated_at"] = l.RemoteStatus, l.RemoteUpdatedAt
	}
	if l.RuleID != nil {
		updates["rule_id"] = l.RuleID
	}
	if err := db.Model(&existing).Updates(updates).Error; err != nil {
		return Link{}, err
	}
	return FindByID(db, existing.OrganizationID, existing.ID)
}

// SetRemoteStatus refreshes the cached far-side state on an active link.
func SetRemoteStatus(db *gorm.DB, organizationID, id uuid.UUID, status string, at time.Time) error {
	result := db.Model(&Link{}).Where("id = ? AND organization_id = ? AND entry_status <> ?", id, organizationID, connect.EntryStatusDeleted).
		Updates(map[string]any{"remote_status": status, "remote_updated_at": at.UTC()})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

// Models lists what an app passes to AutoMigrate.
func Models() []any { return []any{&Link{}} }
