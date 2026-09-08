// Package delivery is the generalized idempotency and history record of
// vikn-connect.md §7: for one inbound event and one action, what happened.
package delivery

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Outcomes every app reports the same way. Apps may add their own for
// action-specific results (e.g. "issue_created").
const (
	OutcomeIgnoredNoRule    = "ignored_no_rule"
	OutcomeIgnoredDisabled  = "ignored_disabled"
	OutcomeIgnoredActor     = "ignored_integration_actor"
	OutcomeIgnoredUnhandled = "ignored_unhandled"
	OutcomeFailed           = "failed"
)

// Record is one (event, action) delivery. The (event_id, action_key) pair
// is unique, so an event that two rules act on has two rows, and a
// redelivery after a crash finds each action already done.
type Record struct {
	ID             uuid.UUID `gorm:"type:uuid;primaryKey" json:"id"`
	EventID        string    `gorm:"type:varchar(100);not null;uniqueIndex:idx_integration_deliveries_event_action" json:"event_id"`
	ActionKey      string    `gorm:"type:varchar(80);not null;default:'';uniqueIndex:idx_integration_deliveries_event_action" json:"action_key"`
	OrganizationID uuid.UUID `gorm:"type:uuid;not null;index" json:"organization_id"`
	EventType      string    `gorm:"type:varchar(220);not null;index" json:"event_type"`
	SourceApp      string    `gorm:"type:varchar(60);not null;default:''" json:"source_app"`

	// What the event was about, in the source app's terms.
	SourceType string     `gorm:"type:varchar(60);not null;default:''" json:"source_type"`
	SourceID   *uuid.UUID `gorm:"type:uuid" json:"source_id,omitempty"`
	SourceRef  string     `gorm:"type:varchar(120);not null;default:''" json:"source_ref"`

	// What the action produced here, when it produced something.
	RuleID     *uuid.UUID `gorm:"type:uuid;index" json:"rule_id,omitempty"`
	TargetType string     `gorm:"type:varchar(60);not null;default:''" json:"target_type"`
	TargetID   *uuid.UUID `gorm:"type:uuid" json:"target_id,omitempty"`
	TargetRef  string     `gorm:"type:varchar(120);not null;default:''" json:"target_ref"`

	Outcome        string  `gorm:"type:varchar(40);not null;index" json:"outcome"`
	Error          *string `gorm:"type:text" json:"error,omitempty"`
	StreamSequence uint64  `gorm:"not null;default:0" json:"stream_sequence"`
	DeliveryCount  uint64  `gorm:"not null;default:0" json:"delivery_count"`
	EntryStatus    int     `gorm:"type:int;not null;default:0" json:"entry_status"`

	ReceivedAt  time.Time `gorm:"not null" json:"received_at"`
	ProcessedAt time.Time `gorm:"not null" json:"processed_at"`
}

func (Record) TableName() string { return "integration_deliveries" }

func (r *Record) BeforeCreate(tx *gorm.DB) error {
	if r.ID == uuid.Nil {
		r.ID = uuid.New()
	}
	return nil
}

// Exists reports whether this (event, action) was already handled.
func Exists(db *gorm.DB, eventID, actionKey string) (bool, error) {
	var count int64
	err := db.Model(&Record{}).Where("event_id = ? AND action_key = ?", eventID, actionKey).Count(&count).Error
	return count > 0, err
}

// Models lists what an app passes to AutoMigrate.
func Models() []any { return []any{&Record{}} }
