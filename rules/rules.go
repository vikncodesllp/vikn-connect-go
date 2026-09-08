// Package rules is the tenant opt-in of vikn-connect.md §7, generalized:
// "when event X from app A arrives, do action K with this config". Rules
// are per app; each app registers the actions it can perform, validates a
// rule's config against the action when the rule is saved, and lets the
// Router run matching rules when events arrive.
package rules

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	connect "github.com/vikncodesllp/vikn-connect-go"
	"github.com/vikncodesllp/vikn-connect-go/consumer"
	"github.com/vikncodesllp/vikn-connect-go/delivery"
)

// Rule is one tenant's opt-in. One active rule per (org, source event type,
// action), so the same event can drive several actions but never the same
// action twice.
type Rule struct {
	ID              uuid.UUID    `gorm:"type:uuid;primaryKey" json:"id"`
	OrganizationID  uuid.UUID    `gorm:"type:uuid;not null;uniqueIndex:idx_integration_rules_active,where:entry_status <> 2" json:"organization_id"`
	SourceApp       string       `gorm:"type:varchar(60);not null;default:''" json:"source_app"`
	SourceEventType string       `gorm:"type:varchar(220);not null;uniqueIndex:idx_integration_rules_active,where:entry_status <> 2" json:"source_event_type"`
	ActionKey       string       `gorm:"type:varchar(80);not null;uniqueIndex:idx_integration_rules_active,where:entry_status <> 2" json:"action_key"`
	Config          connect.JSON `gorm:"type:jsonb;not null;default:'{}'" json:"config"`
	Enabled         bool         `gorm:"not null;default:false" json:"enabled"`
	EntryStatus     int          `gorm:"type:int;not null;default:0" json:"entry_status"`
	CreatedBy       uuid.UUID    `gorm:"type:uuid;not null" json:"created_by"`
	UpdatedBy       uuid.UUID    `gorm:"type:uuid;not null" json:"updated_by"`
	CreatedAt       time.Time    `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt       time.Time    `gorm:"autoUpdateTime" json:"updated_at"`
}

func (Rule) TableName() string { return "integration_rules" }

func (r *Rule) BeforeCreate(tx *gorm.DB) error {
	if r.ID == uuid.Nil {
		r.ID = uuid.New()
	}
	if r.UpdatedBy == uuid.Nil {
		r.UpdatedBy = r.CreatedBy
	}
	if len(r.Config) == 0 {
		r.Config = connect.JSON("{}")
	}
	return nil
}

// Field describes one config value an action needs, for a settings screen
// to render and validate before the rule is saved.
type Field struct {
	Name        string   `json:"name"`
	Label       string   `json:"label"`
	Type        string   `json:"type"` // "uuid" | "string" | "enum" | "int" | "bool"
	Required    bool     `json:"required"`
	Options     []string `json:"options,omitempty"` // for "enum"
	Default     string   `json:"default,omitempty"`
	Description string   `json:"description,omitempty"`
	// Source names an in-app picker the UI can offer (e.g. "projects",
	// "project_members", "ticket_statuses"); free-form, app-defined.
	Source string `json:"source,omitempty"`
}

// Descriptor is what an action publishes about itself.
type Descriptor struct {
	Key         string   `json:"key"`
	Label       string   `json:"label"`
	Description string   `json:"description"`
	SourceApps  []string `json:"source_apps,omitempty"` // apps whose events this action expects, "" = any
	EventTypes  []string `json:"event_types"`           // event types this action can handle
	Fields      []Field  `json:"fields"`
}

// Outcome is what an action reports after running.
type Outcome struct {
	Code       string // e.g. "issue_created"; "" means the action chose to do nothing
	TargetType string
	TargetID   *uuid.UUID
	TargetRef  string
}

// Action is one thing an app can do in response to an event.
type Action interface {
	Describe() Descriptor
	// Validate checks a config against this app's data (a project exists, a
	// user is active). Return a *ValidationError for per-field messages.
	Validate(ctx context.Context, db *gorm.DB, organizationID uuid.UUID, config json.RawMessage) error
	// Execute runs inside a transaction with the delivery row written by the
	// Router afterwards. Return an error to have the event redelivered, or
	// consumer.Permanent(err) to record and drop it.
	Execute(ctx context.Context, tx *gorm.DB, rule Rule, d consumer.Delivery) (Outcome, error)
}

// ValidationError carries per-field messages for a settings screen.
type ValidationError struct {
	Fields map[string]string `json:"fields"`
}

func (e *ValidationError) Error() string {
	keys := make([]string, 0, len(e.Fields))
	for k := range e.Fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+": "+e.Fields[k])
	}
	return "invalid rule config: " + strings.Join(parts, "; ")
}

// Registry holds an app's actions by key.
type Registry struct {
	mu      sync.RWMutex
	actions map[string]Action
}

func NewRegistry(actions ...Action) *Registry {
	r := &Registry{actions: map[string]Action{}}
	for _, a := range actions {
		r.Register(a)
	}
	return r
}

func (r *Registry) Register(a Action) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actions[a.Describe().Key] = a
}

func (r *Registry) Get(key string) (Action, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	a, ok := r.actions[key]
	return a, ok
}

// Descriptors lists every registered action, sorted by key: the catalog a
// settings screen renders.
func (r *Registry) Descriptors() []Descriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Descriptor, 0, len(r.actions))
	for _, a := range r.actions {
		out = append(out, a.Describe())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// CheckShape validates a config against the descriptor's fields: required
// present, enums within options, uuids parse. Apps call it from Validate
// before their own semantic checks so every action gets the same basics.
func CheckShape(d Descriptor, config json.RawMessage) error {
	var values map[string]any
	if len(config) == 0 {
		values = map[string]any{}
	} else if err := json.Unmarshal(config, &values); err != nil {
		return &ValidationError{Fields: map[string]string{"config": "must be a JSON object"}}
	}
	problems := map[string]string{}
	for _, f := range d.Fields {
		raw, present := values[f.Name]
		text := ""
		switch v := raw.(type) {
		case string:
			text = strings.TrimSpace(v)
		case nil:
		default:
			b, _ := json.Marshal(v)
			text = string(b)
		}
		if !present || text == "" {
			if f.Required {
				problems[f.Name] = "is required"
			}
			continue
		}
		switch f.Type {
		case "uuid":
			if _, err := uuid.Parse(text); err != nil {
				problems[f.Name] = "must be a valid id"
			}
		case "enum":
			ok := false
			for _, option := range f.Options {
				if option == text {
					ok = true
					break
				}
			}
			if !ok {
				problems[f.Name] = "must be one of " + strings.Join(f.Options, ", ")
			}
		case "int":
			if _, isNumber := raw.(float64); !isNumber {
				problems[f.Name] = "must be a number"
			}
		case "bool":
			if _, isBool := raw.(bool); !isBool {
				problems[f.Name] = "must be true or false"
			}
		}
	}
	if len(problems) > 0 {
		return &ValidationError{Fields: problems}
	}
	return nil
}

// Validate is the save-time check: the action exists, the event type is one
// it handles, the config passes the shape check and the action's own rules.
func (r *Registry) Validate(ctx context.Context, db *gorm.DB, rule Rule) error {
	action, ok := r.Get(rule.ActionKey)
	if !ok {
		return &ValidationError{Fields: map[string]string{"action_key": "unknown action " + rule.ActionKey}}
	}
	d := action.Describe()
	if !connect.ValidEventType(rule.SourceEventType) {
		return &ValidationError{Fields: map[string]string{"source_event_type": "is not a Vikn event type"}}
	}
	if len(d.EventTypes) > 0 && !contains(d.EventTypes, rule.SourceEventType) {
		return &ValidationError{Fields: map[string]string{"source_event_type": rule.ActionKey + " does not handle " + rule.SourceEventType}}
	}
	if rule.SourceApp != "" && len(d.SourceApps) > 0 && !contains(d.SourceApps, rule.SourceApp) {
		return &ValidationError{Fields: map[string]string{"source_app": rule.ActionKey + " does not accept events from " + rule.SourceApp}}
	}
	if err := CheckShape(d, json.RawMessage(rule.Config)); err != nil {
		return err
	}
	return action.Validate(ctx, db, rule.OrganizationID, json.RawMessage(rule.Config))
}

func contains(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}

// Active returns the active rules for an org (all, or one event type).
func Active(db *gorm.DB, organizationID uuid.UUID, eventType string) ([]Rule, error) {
	q := db.Where("organization_id = ? AND entry_status <> ?", organizationID, connect.EntryStatusDeleted)
	if eventType != "" {
		q = q.Where("source_event_type = ?", eventType)
	}
	var out []Rule
	err := q.Order("created_at ASC").Find(&out).Error
	return out, err
}

// Upsert creates or replaces the active rule for (org, event type, action).
func Upsert(db *gorm.DB, rule Rule) (Rule, error) {
	var existing Rule
	err := db.Where("organization_id = ? AND source_event_type = ? AND action_key = ? AND entry_status <> ?",
		rule.OrganizationID, rule.SourceEventType, rule.ActionKey, connect.EntryStatusDeleted).First(&existing).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		if err := db.Create(&rule).Error; err != nil {
			return Rule{}, err
		}
		return rule, nil
	}
	if err != nil {
		return Rule{}, err
	}
	if len(rule.Config) == 0 {
		rule.Config = connect.JSON("{}")
	}
	if err := db.Model(&existing).Updates(map[string]any{
		"source_app": rule.SourceApp, "config": rule.Config, "enabled": rule.Enabled, "updated_by": rule.UpdatedBy,
	}).Error; err != nil {
		return Rule{}, err
	}
	err = db.First(&existing, "id = ?", existing.ID).Error
	return existing, err
}

// Delete soft-deletes a rule.
func Delete(db *gorm.DB, organizationID, id, by uuid.UUID) error {
	result := db.Model(&Rule{}).Where("id = ? AND organization_id = ? AND entry_status <> ?", id, organizationID, connect.EntryStatusDeleted).
		Updates(map[string]any{"entry_status": connect.EntryStatusDeleted, "enabled": false, "updated_by": by})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

// Router runs the matching rules for each delivery and keeps the delivery
// history. Plug its Handle into consumer.Options.Handler, with the
// integration-actor gate closed (the harness skips those; Skipped can
// record them through RecordSkipped).
type Router struct {
	DB       *gorm.DB
	Registry *Registry
	Now      func() time.Time
	// Source, when set, extracts what the event was about for the history
	// row (type, id, ref). Optional; apps that care about the columns set it.
	Source func(d consumer.Delivery) (sourceType string, sourceID *uuid.UUID, sourceRef string)
}

func NewRouter(db *gorm.DB, registry *Registry) *Router {
	return &Router{DB: db, Registry: registry, Now: time.Now}
}

func (r *Router) now() time.Time {
	if r.Now == nil {
		return time.Now().UTC()
	}
	return r.Now().UTC()
}

func (r *Router) base(d consumer.Delivery, rule *Rule, outcome string) delivery.Record {
	env := d.Envelope
	rec := delivery.Record{
		EventID: env.ID, OrganizationID: env.OrganizationID, EventType: env.Type, SourceApp: sourceApp(env.Source),
		Outcome: outcome, StreamSequence: d.StreamSequence, DeliveryCount: d.DeliveryCount,
		ReceivedAt: d.ReceivedAt, ProcessedAt: r.now(),
	}
	if rec.ReceivedAt.IsZero() {
		rec.ReceivedAt = rec.ProcessedAt
	}
	if rule != nil {
		id := rule.ID
		rec.RuleID, rec.ActionKey = &id, rule.ActionKey
	}
	if r.Source != nil {
		rec.SourceType, rec.SourceID, rec.SourceRef = r.Source(d)
	}
	return rec
}

// sourceApp turns a CloudEvents source like "vikn.desk" into the App.slug
// convention "vikn-desk"; anything else is passed through.
func sourceApp(source string) string {
	return strings.ReplaceAll(strings.TrimSpace(source), ".", "-")
}

// Handle is the consumer.Handler. Every enabled rule for the event's type
// runs in its own transaction with its own delivery row; an event with no
// rule is recorded once as ignored_no_rule. An action error before the
// last delivery is returned so the harness redelivers; rows already
// written for other rules make the retry idempotent.
func (r *Router) Handle(ctx context.Context, d consumer.Delivery) error {
	env := d.Envelope
	if env.OrganizationID == uuid.Nil {
		return consumer.Permanent(errors.New("event carries no organization"))
	}
	matching, err := Active(r.DB.WithContext(ctx), env.OrganizationID, env.Type)
	if err != nil {
		return err
	}
	if len(matching) == 0 {
		return r.record(ctx, r.base(d, nil, delivery.OutcomeIgnoredNoRule))
	}
	var firstErr error
	for i := range matching {
		rule := matching[i]
		done, err := delivery.Exists(r.DB.WithContext(ctx), env.ID, rule.ActionKey)
		if err != nil {
			return err
		}
		if done {
			continue
		}
		if !rule.Enabled {
			if err := r.record(ctx, r.base(d, &rule, delivery.OutcomeIgnoredDisabled)); err != nil {
				return err
			}
			continue
		}
		action, ok := r.Registry.Get(rule.ActionKey)
		if !ok {
			if err := r.record(ctx, r.base(d, &rule, delivery.OutcomeIgnoredUnhandled)); err != nil {
				return err
			}
			continue
		}
		err = r.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			outcome, execErr := action.Execute(ctx, tx, rule, d)
			if execErr != nil {
				return execErr
			}
			rec := r.base(d, &rule, outcome.Code)
			if rec.Outcome == "" {
				rec.Outcome = delivery.OutcomeIgnoredUnhandled
			}
			rec.TargetType, rec.TargetID, rec.TargetRef = outcome.TargetType, outcome.TargetID, outcome.TargetRef
			return tx.Create(&rec).Error
		})
		if err != nil {
			var permanent *consumer.PermanentError
			if errors.As(err, &permanent) {
				// Record why, so the settings screen shows it, then let the
				// harness terminate the message.
				message := permanent.Err.Error()
				rec := r.base(d, &rule, delivery.OutcomeFailed)
				rec.Error = &message
				_ = r.record(ctx, rec)
				return err
			}
			if firstErr == nil {
				firstErr = fmt.Errorf("rule %s (%s): %w", rule.ID, rule.ActionKey, err)
			}
		}
	}
	return firstErr
}

// RecordSkipped is the consumer.Options.Skipped hook: history for events the
// harness acked without running (integration actor).
func (r *Router) RecordSkipped(ctx context.Context, d consumer.Delivery, reason string) error {
	if d.Envelope.OrganizationID == uuid.Nil {
		return nil
	}
	rec := r.base(d, nil, "ignored_"+reason)
	rec.ActionKey = "skipped:" + reason
	return r.record(ctx, rec)
}

func (r *Router) record(ctx context.Context, rec delivery.Record) error {
	exists, err := delivery.Exists(r.DB.WithContext(ctx), rec.EventID, rec.ActionKey)
	if err != nil || exists {
		return err
	}
	return r.DB.WithContext(ctx).Create(&rec).Error
}

// Models lists what an app passes to AutoMigrate.
func Models() []any { return []any{&Rule{}} }
