package rules

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	connect "github.com/vikncodesllp/vikn-connect-go"
	"github.com/vikncodesllp/vikn-connect-go/consumer"
	"github.com/vikncodesllp/vikn-connect-go/delivery"
	"github.com/vikncodesllp/vikn-connect-go/internal/testdb"
)

const eventType = "com.vikn.desk.ticket.created.v1"

type fakeAction struct {
	calls   int
	fail    error
	created uuid.UUID
}

func (f *fakeAction) Describe() Descriptor {
	return Descriptor{Key: "project.issue.create", Label: "Create an issue", EventTypes: []string{eventType},
		Fields: []Field{
			{Name: "project_id", Type: "uuid", Required: true},
			{Name: "priority", Type: "enum", Options: []string{"low", "medium", "high"}, Default: "medium"},
			{Name: "notify", Type: "bool"},
		}}
}

func (f *fakeAction) Validate(_ context.Context, _ *gorm.DB, _ uuid.UUID, config json.RawMessage) error {
	var cfg struct {
		ProjectID uuid.UUID `json:"project_id"`
	}
	_ = json.Unmarshal(config, &cfg)
	if cfg.ProjectID == uuid.MustParse("00000000-0000-0000-0000-000000000009") {
		return &ValidationError{Fields: map[string]string{"project_id": "project is archived"}}
	}
	return nil
}

func (f *fakeAction) Execute(_ context.Context, _ *gorm.DB, _ Rule, _ consumer.Delivery) (Outcome, error) {
	f.calls++
	if f.fail != nil {
		return Outcome{}, f.fail
	}
	f.created = uuid.New()
	id := f.created
	return Outcome{Code: "issue_created", TargetType: "issue", TargetID: &id, TargetRef: "PROJ-1"}, nil
}

func deliveryFor(org uuid.UUID, id string) consumer.Delivery {
	return consumer.Delivery{Envelope: connect.Envelope{ID: id, Source: "vikn.desk", Type: eventType, Time: time.Now().UTC(),
		Data: []byte(`{"ticket_id":"x"}`), OrganizationID: org}, StreamSequence: 3, DeliveryCount: 1, ReceivedAt: time.Now().UTC()}
}

func TestCheckShape(t *testing.T) {
	d := (&fakeAction{}).Describe()
	if err := CheckShape(d, json.RawMessage(`{"project_id":"`+uuid.NewString()+`","priority":"high","notify":true}`)); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	err := CheckShape(d, json.RawMessage(`{"priority":"urgent","notify":"yes"}`))
	var ve *ValidationError
	if !errors.As(err, &ve) || ve.Fields["project_id"] != "is required" || ve.Fields["priority"] == "" || ve.Fields["notify"] == "" {
		t.Fatalf("err = %v", err)
	}
	if err := CheckShape(d, json.RawMessage(`[]`)); err == nil {
		t.Fatal("a non-object config must fail")
	}
}

func TestRegistryValidate(t *testing.T) {
	reg := NewRegistry(&fakeAction{})
	org := uuid.New()
	good := Rule{OrganizationID: org, SourceApp: "vikn-desk", SourceEventType: eventType, ActionKey: "project.issue.create",
		Config: connect.JSON(`{"project_id":"` + uuid.NewString() + `"}`)}
	if err := reg.Validate(context.Background(), nil, good); err != nil {
		t.Fatalf("good rule: %v", err)
	}
	var ve *ValidationError
	bad := good
	bad.ActionKey = "nope"
	if err := reg.Validate(context.Background(), nil, bad); !errors.As(err, &ve) || ve.Fields["action_key"] == "" {
		t.Fatalf("unknown action: %v", err)
	}
	bad = good
	bad.SourceEventType = "com.vikn.desk.ticket.closed.v1"
	if err := reg.Validate(context.Background(), nil, bad); !errors.As(err, &ve) || ve.Fields["source_event_type"] == "" {
		t.Fatalf("unhandled event type: %v", err)
	}
	bad = good
	bad.Config = connect.JSON(`{"project_id":"00000000-0000-0000-0000-000000000009"}`)
	if err := reg.Validate(context.Background(), nil, bad); !errors.As(err, &ve) || ve.Fields["project_id"] != "project is archived" {
		t.Fatalf("semantic check: %v", err)
	}
	if len(reg.Descriptors()) != 1 || reg.Descriptors()[0].Key != "project.issue.create" {
		t.Fatalf("descriptors = %+v", reg.Descriptors())
	}
}

func TestRouterRunsRulesIdempotently(t *testing.T) {
	db := testdb.Open(t, append(Models(), delivery.Models()...)...)
	org, by := uuid.New(), uuid.New()
	action := &fakeAction{}
	router := NewRouter(db, NewRegistry(action))
	router.Source = func(d consumer.Delivery) (string, *uuid.UUID, string) { return "ticket", nil, "TKT-1" }

	// No rule: recorded once, action untouched, redelivery is a no-op.
	for i := 0; i < 2; i++ {
		if err := router.Handle(context.Background(), deliveryFor(org, "evt-none")); err != nil {
			t.Fatalf("no rule: %v", err)
		}
	}
	var rows []delivery.Record
	db.Where("event_id = ?", "evt-none").Find(&rows)
	if len(rows) != 1 || rows[0].Outcome != delivery.OutcomeIgnoredNoRule || rows[0].SourceRef != "TKT-1" || rows[0].SourceApp != "vikn-desk" {
		t.Fatalf("rows = %+v", rows)
	}

	rule, err := Upsert(db, Rule{OrganizationID: org, SourceApp: "vikn-desk", SourceEventType: eventType, ActionKey: "project.issue.create",
		Config: connect.JSON(`{"project_id":"` + uuid.NewString() + `"}`), Enabled: false, CreatedBy: by})
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := router.Handle(context.Background(), deliveryFor(org, "evt-disabled")); err != nil {
		t.Fatal(err)
	}
	db.Where("event_id = ?", "evt-disabled").Find(&rows)
	if len(rows) != 1 || rows[0].Outcome != delivery.OutcomeIgnoredDisabled || rows[0].RuleID == nil || *rows[0].RuleID != rule.ID {
		t.Fatalf("disabled rows = %+v", rows)
	}

	rule.Enabled = true
	rule.UpdatedBy = by
	if rule, err = Upsert(db, rule); err != nil || !rule.Enabled {
		t.Fatalf("enable: %v %+v", err, rule)
	}
	for i := 0; i < 2; i++ {
		if err := router.Handle(context.Background(), deliveryFor(org, "evt-live")); err != nil {
			t.Fatalf("enabled rule: %v", err)
		}
	}
	if action.calls != 1 {
		t.Fatalf("action ran %d times, want once", action.calls)
	}
	db.Where("event_id = ?", "evt-live").Find(&rows)
	if len(rows) != 1 || rows[0].Outcome != "issue_created" || rows[0].TargetID == nil || *rows[0].TargetID != action.created || rows[0].TargetRef != "PROJ-1" {
		t.Fatalf("live rows = %+v", rows)
	}

	// A transient failure is returned for redelivery and leaves no row; the
	// retry then succeeds.
	action.fail = errors.New("db hiccup")
	if err := router.Handle(context.Background(), deliveryFor(org, "evt-retry")); err == nil {
		t.Fatal("expected the transient error")
	}
	db.Where("event_id = ?", "evt-retry").Find(&rows)
	if len(rows) != 0 {
		t.Fatalf("transient failure must not record: %+v", rows)
	}
	action.fail = nil
	if err := router.Handle(context.Background(), deliveryFor(org, "evt-retry")); err != nil {
		t.Fatal(err)
	}

	// A permanent failure is recorded as failed with the reason and surfaces
	// as permanent so the harness terminates the message.
	action.fail = consumer.Permanent(errors.New("project archived"))
	err = router.Handle(context.Background(), deliveryFor(org, "evt-dead"))
	var permanent *consumer.PermanentError
	if !errors.As(err, &permanent) {
		t.Fatalf("expected a permanent error, got %v", err)
	}
	db.Where("event_id = ?", "evt-dead").Find(&rows)
	if len(rows) != 1 || rows[0].Outcome != delivery.OutcomeFailed || rows[0].Error == nil {
		t.Fatalf("dead rows = %+v", rows)
	}

	if err := router.RecordSkipped(context.Background(), deliveryFor(org, "evt-skip"), "integration_actor"); err != nil {
		t.Fatal(err)
	}
	db.Where("event_id = ?", "evt-skip").Find(&rows)
	if len(rows) != 1 || rows[0].Outcome != delivery.OutcomeIgnoredActor {
		t.Fatalf("skip rows = %+v", rows)
	}

	if err := Delete(db, org, rule.ID, by); err != nil {
		t.Fatal(err)
	}
	if active, _ := Active(db, org, eventType); len(active) != 0 {
		t.Fatalf("deleted rule still active: %+v", active)
	}
	if err := Delete(db, org, rule.ID, by); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("double delete: %v", err)
	}
}
