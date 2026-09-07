// Package outbox is the transactional outbox every producing app shares: a
// row written in the business transaction, published after commit by a
// dispatcher, so an event is never lost when the broker is briefly away.
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"gorm.io/gorm"

	connect "github.com/vikncodesllp/vikn-connect-go"
)

const (
	StatusPending    = "pending"
	StatusPublishing = "publishing"
	StatusPublished  = "published"
	StatusFailed     = "failed"

	// MaxBackoffAttempt caps the exponential retry delay at 2^7 minutes.
	MaxBackoffAttempt = 8
)

// Event is one outbox row. The table name is integration_outboxes in every
// app so operators find the same thing everywhere.
type Event struct {
	ID             uuid.UUID    `gorm:"type:uuid;primaryKey" json:"id"`
	OrganizationID uuid.UUID    `gorm:"type:uuid;not null;index" json:"organization_id"`
	Source         string       `gorm:"type:varchar(100);not null" json:"source"`
	EventType      string       `gorm:"type:varchar(180);not null;index" json:"event_type"`
	Subject        string       `gorm:"type:text;not null" json:"subject"`
	Data           connect.JSON `gorm:"type:jsonb;not null" json:"data"`
	OccurredAt     time.Time    `gorm:"not null;index" json:"occurred_at"`

	// LinkID and Actor become ce-viknlink and ce-viknactor on the wire.
	LinkID *uuid.UUID `gorm:"type:uuid" json:"link_id,omitempty"`
	Actor  string     `gorm:"type:varchar(160);not null;default:''" json:"actor"`

	Status       string     `gorm:"type:varchar(20);not null;default:'pending';index" json:"status"`
	AttemptCount int        `gorm:"not null;default:0" json:"attempt_count"`
	AvailableAt  time.Time  `gorm:"not null;index" json:"available_at"`
	LockedAt     *time.Time `json:"locked_at,omitempty"`
	PublishedAt  *time.Time `json:"published_at,omitempty"`
	LastError    *string    `gorm:"type:text" json:"last_error,omitempty"`
	CreatedAt    time.Time  `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt    time.Time  `gorm:"autoUpdateTime" json:"updated_at"`
}

func (Event) TableName() string { return "integration_outboxes" }

func (e *Event) BeforeCreate(tx *gorm.DB) error {
	if e.ID == uuid.Nil {
		e.ID = uuid.New()
	}
	if e.Status == "" {
		e.Status = StatusPending
	}
	now := time.Now().UTC()
	if e.OccurredAt.IsZero() {
		e.OccurredAt = now
	}
	if e.AvailableAt.IsZero() {
		e.AvailableAt = now
	}
	return nil
}

// Envelope is the wire form of the row.
func (e Event) Envelope() connect.Envelope {
	actor, _ := connect.ParseActor(e.Actor)
	return connect.Envelope{
		ID:             e.ID.String(),
		Source:         e.Source,
		Type:           e.EventType,
		Subject:        e.Subject,
		Time:           e.OccurredAt,
		ContentType:    connect.ContentTypeJSON,
		Data:           json.RawMessage(e.Data),
		OrganizationID: e.OrganizationID,
		LinkID:         e.LinkID,
		Actor:          actor,
	}
}

// New builds a row from typed fields, marshalling data for the caller.
func New(organizationID uuid.UUID, source, eventType, subject string, actor connect.Actor, data any) (Event, error) {
	payload, err := json.Marshal(data)
	if err != nil {
		return Event{}, fmt.Errorf("marshal %s data: %w", eventType, err)
	}
	now := time.Now().UTC()
	return Event{
		ID: uuid.New(), OrganizationID: organizationID, Source: source, EventType: eventType, Subject: subject,
		Data: connect.JSON(payload), OccurredAt: now, AvailableAt: now, Actor: actor.String(), Status: StatusPending,
	}, nil
}

// Enqueue records an event in the caller's transaction.
//
// It is a no-op when no broker is configured: without NATS_URL nothing would
// ever drain the row and the table would grow with every write. Every typed
// enqueue in an app must go through here so that gate cannot be missed.
// The row is validated as an envelope first, so a producer cannot store what
// the publisher would later refuse.
func Enqueue(tx *gorm.DB, event Event) error {
	if !connect.BusConfigured() {
		return nil
	}
	if event.ID == uuid.Nil {
		event.ID = uuid.New()
	}
	if err := event.Envelope().Validate(); err != nil {
		return fmt.Errorf("outbox event %s: %w", event.EventType, err)
	}
	return tx.Create(&event).Error
}

// Publisher is implemented by the NATS adapter and by test fakes.
type Publisher interface {
	Publish(context.Context, Event) error
}

// Dispatcher claims ready rows and hands them to a publisher. A single
// worker per process is enough; the claim is a conditional update so two
// workers cannot publish the same row.
type Dispatcher struct {
	DB        *gorm.DB
	Publisher Publisher
	Now       func() time.Time
}

func NewDispatcher(db *gorm.DB, publisher Publisher) *Dispatcher {
	return &Dispatcher{DB: db, Publisher: publisher, Now: time.Now}
}

// DispatchOnce publishes up to limit ready events and returns how many went
// out. A failed publish is kept and retried with bounded exponential backoff.
func (d *Dispatcher) DispatchOnce(ctx context.Context, limit int) (int, error) {
	if d.Publisher == nil {
		return 0, errors.New("outbox publisher is not configured")
	}
	if limit <= 0 {
		limit = 50
	}
	now := d.Now().UTC()
	ready := []string{StatusPending, StatusFailed}
	var events []Event
	if err := d.DB.WithContext(ctx).Where("status IN ? AND available_at <= ?", ready, now).
		Order("occurred_at ASC").Limit(limit).Find(&events).Error; err != nil {
		return 0, err
	}
	published := 0
	for _, event := range events {
		claimed := d.DB.WithContext(ctx).Model(&Event{}).
			Where("id = ? AND status IN ?", event.ID, ready).
			Updates(map[string]any{"status": StatusPublishing, "locked_at": now, "attempt_count": event.AttemptCount + 1})
		if claimed.Error != nil {
			return published, claimed.Error
		}
		if claimed.RowsAffected == 0 {
			continue
		}
		event.Status = StatusPublishing
		event.AttemptCount++
		if err := d.Publisher.Publish(ctx, event); err != nil {
			message := err.Error()
			if updateErr := d.DB.WithContext(ctx).Model(&Event{}).Where("id = ?", event.ID).Updates(map[string]any{
				"status": StatusFailed, "available_at": now.Add(Backoff(event.AttemptCount)), "last_error": &message, "locked_at": nil,
			}).Error; updateErr != nil {
				return published, updateErr
			}
			continue
		}
		if err := d.DB.WithContext(ctx).Model(&Event{}).Where("id = ?", event.ID).Updates(map[string]any{
			"status": StatusPublished, "published_at": now, "locked_at": nil, "last_error": nil,
		}).Error; err != nil {
			return published, err
		}
		published++
	}
	return published, nil
}

// Backoff is 1, 2, 4 … 128 minutes.
func Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > MaxBackoffAttempt {
		attempt = MaxBackoffAttempt
	}
	return time.Minute * time.Duration(1<<(attempt-1))
}

// Run is the worker loop an API process starts when the bus is configured.
func Run(ctx context.Context, dispatcher *Dispatcher, interval time.Duration, batchSize int, report func(error)) {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	dispatch := func() {
		if _, err := dispatcher.DispatchOnce(ctx, batchSize); err != nil && report != nil {
			report(err)
		}
	}
	dispatch()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			dispatch()
		}
	}
}

// NATSPublisher publishes rows in CloudEvents binary mode. The Nats-Msg-Id
// header carries the row id so a retry of the same row is deduplicated by
// the broker.
type NATSPublisher struct {
	connection    *nats.Conn
	js            nats.JetStreamContext
	subjectPrefix string
}

// NewNATSPublisherFromEnv returns nil, nil when NATS_URL is unset so the
// rollout is opt-in per process.
func NewNATSPublisherFromEnv(clientName string) (*NATSPublisher, error) {
	if !connect.BusConfigured() {
		return nil, nil
	}
	return NewNATSPublisher(connect.BusURL(), connect.SubjectPrefixFromEnv(), clientName)
}

func NewNATSPublisher(url, subjectPrefix, clientName string) (*NATSPublisher, error) {
	connection, js, err := connect.ConnectURL(url, clientName)
	if err != nil {
		return nil, err
	}
	return &NATSPublisher{connection: connection, js: js, subjectPrefix: subjectPrefix}, nil
}

func (p *NATSPublisher) Publish(ctx context.Context, event Event) error {
	env := event.Envelope()
	if err := env.Validate(); err != nil {
		return err
	}
	_, err := p.js.PublishMsg(env.NewMsg(p.subjectPrefix), nats.Context(ctx))
	return err
}

func (p *NATSPublisher) Close() {
	if p != nil && p.connection != nil {
		p.connection.Close()
	}
}

// Models lists what an app passes to AutoMigrate.
func Models() []any { return []any{&Event{}} }
