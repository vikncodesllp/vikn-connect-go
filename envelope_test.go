package connect

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

func sample() Envelope {
	link := uuid.MustParse("6a2e7a0e-3d1a-4d3e-9a52-3fd3d3f7ef0b")
	return Envelope{
		ID: "evt-1", Source: "vikn.desk", Type: "com.vikn.desk.ticket.created.v1",
		Subject: "organizations/o/tickets/t", Time: time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC),
		Data:           []byte(`{"organization_id":"2f8c6b8e-5b0f-4a6b-9a3e-1b2c3d4e5f60","ticket_id":"t"}`),
		OrganizationID: uuid.MustParse("2f8c6b8e-5b0f-4a6b-9a3e-1b2c3d4e5f60"),
		LinkID:         &link,
		Actor:          IntegrationActor("desk-connect"),
	}
}

func TestEnvelopeRoundTrip(t *testing.T) {
	want := sample()
	if err := want.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	msg := want.NewMsg("vikn")
	if msg.Subject != "vikn.desk.ticket.created.v1" {
		t.Fatalf("subject = %q", msg.Subject)
	}
	if msg.Header.Get(HeaderMsgID) != "evt-1" || msg.Header.Get(HeaderOrganization) != want.OrganizationID.String() {
		t.Fatalf("headers = %v", msg.Header)
	}
	got, err := ParseMsg(msg)
	if err != nil {
		t.Fatalf("ParseMsg: %v", err)
	}
	if got.ID != want.ID || got.Type != want.Type || got.OrganizationID != want.OrganizationID || !got.Time.Equal(want.Time) {
		t.Fatalf("got %+v", got)
	}
	if got.LinkID == nil || *got.LinkID != *want.LinkID {
		t.Fatalf("link = %v", got.LinkID)
	}
	if !got.Actor.IsIntegration() || got.Actor.ID != "desk-connect" {
		t.Fatalf("actor = %+v", got.Actor)
	}
	if got.OrganizationFromBody {
		t.Fatal("organization should come from the header")
	}
}

func TestParseMsgFallsBackToBodyOrganization(t *testing.T) {
	msg := sample().NewMsg("vikn")
	msg.Header.Del(HeaderOrganization)
	msg.Header.Del(HeaderActor)
	msg.Header.Del(HeaderLink)
	got, err := ParseMsg(msg)
	if err != nil {
		t.Fatalf("ParseMsg: %v", err)
	}
	if !got.OrganizationFromBody || got.OrganizationID != sample().OrganizationID {
		t.Fatalf("fallback failed: %+v", got)
	}
	if !got.Actor.IsZero() || got.LinkID != nil {
		t.Fatalf("pre-extension event should have zero actor and no link: %+v", got)
	}
}

func TestParseMsgRejects(t *testing.T) {
	cases := map[string]func(*nats.Msg){
		"missing id":       func(m *nats.Msg) { m.Header.Del(HeaderID) },
		"wrong spec":       func(m *nats.Msg) { m.Header.Set(HeaderSpecVersion, "0.3") },
		"bad json":         func(m *nats.Msg) { m.Data = []byte("nope") },
		"bad time":         func(m *nats.Msg) { m.Header.Set(HeaderTime, "yesterday") },
		"bad org":          func(m *nats.Msg) { m.Header.Set(HeaderOrganization, "org-1") },
		"bad link":         func(m *nats.Msg) { m.Header.Set(HeaderLink, "link-1") },
		"bad actor kind":   func(m *nats.Msg) { m.Header.Set(HeaderActor, "robot:1") },
		"bad actor uuid":   func(m *nats.Msg) { m.Header.Set(HeaderActor, "user:not-a-uuid") },
		"actor without id": func(m *nats.Msg) { m.Header.Set(HeaderActor, "integration:") },
	}
	for name, mutate := range cases {
		msg := sample().NewMsg("vikn")
		mutate(msg)
		if _, err := ParseMsg(msg); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestValidateRequiresCatalogShape(t *testing.T) {
	env := sample()
	env.Type = "ticket.created"
	if err := env.Validate(); err == nil {
		t.Fatal("expected a non-catalog type to fail")
	}
	env = sample()
	env.OrganizationID = uuid.Nil
	if err := env.Validate(); err == nil {
		t.Fatal("expected a missing organization to fail")
	}
}

func TestValidEventType(t *testing.T) {
	for value, want := range map[string]bool{
		"com.vikn.desk.ticket.created.v1":          true,
		"com.vikn.project.issue.status.changed.v2": true,
		"com.vikn.desk.ticket.created":             false,
		"vikn.desk.ticket.created.v1":              false,
		"com.vikn.Desk.ticket.created.v1":          false,
		"com.vikn.desk.v1":                         false,
	} {
		if got := ValidEventType(value); got != want {
			t.Errorf("ValidEventType(%q) = %v, want %v", value, got, want)
		}
	}
}

func TestSubject(t *testing.T) {
	if got := Subject("vikn", "com.vikn.desk.ticket.created.v1"); got != "vikn.desk.ticket.created.v1" {
		t.Fatalf("Subject = %q", got)
	}
	if got := Subject("events", "com.vikn.desk.ticket.created.v1"); got != "events.vikn.desk.ticket.created.v1" {
		t.Fatalf("Subject = %q", got)
	}
}

func TestActor(t *testing.T) {
	user := uuid.New()
	if got := UserActor(user).String(); got != "user:"+user.String() {
		t.Fatalf("UserActor = %q", got)
	}
	if UserActor(uuid.Nil).String() != "" || IntegrationActor(" ").String() != "" {
		t.Fatal("nil actors should render empty")
	}
	parsed, err := ParseActor("integration:desk-connect")
	if err != nil || !parsed.IsIntegration() {
		t.Fatalf("ParseActor = %+v, %v", parsed, err)
	}
	if _, err := ParseActor(""); err != nil {
		t.Fatalf("empty actor should be zero, got %v", err)
	}
}

func TestBusConfigured(t *testing.T) {
	t.Setenv("NATS_URL", " ")
	if BusConfigured() {
		t.Fatal("blank NATS_URL should not count")
	}
	t.Setenv("NATS_URL", "nats://127.0.0.1:4222")
	if !BusConfigured() {
		t.Fatal("NATS_URL should count")
	}
	t.Setenv("NATS_SUBJECT_PREFIX", ".events.")
	if SubjectPrefixFromEnv() != "events" {
		t.Fatalf("prefix = %q", SubjectPrefixFromEnv())
	}
}
