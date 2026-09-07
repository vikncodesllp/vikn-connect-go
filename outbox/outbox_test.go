package outbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	connect "github.com/vikncodesllp/vikn-connect-go"
	"github.com/vikncodesllp/vikn-connect-go/internal/testdb"
)

func TestBackoffIsBoundedExponential(t *testing.T) {
	for attempt, want := range map[int]time.Duration{0: time.Minute, 1: time.Minute, 2: 2 * time.Minute, 8: 128 * time.Minute, 9: 128 * time.Minute} {
		if got := Backoff(attempt); got != want {
			t.Errorf("Backoff(%d) = %s, want %s", attempt, got, want)
		}
	}
}

func TestEnqueueIsNoOpWithoutBus(t *testing.T) {
	t.Setenv("NATS_URL", "")
	if err := Enqueue(nil, Event{}); err != nil {
		t.Fatalf("Enqueue without a bus should be a no-op, got %v", err)
	}
}

func TestNewNATSPublisherFromEnvIsDisabledWithoutURL(t *testing.T) {
	t.Setenv("NATS_URL", "")
	publisher, err := NewNATSPublisherFromEnv("test")
	if err != nil || publisher != nil {
		t.Fatalf("publisher = %v, err = %v", publisher, err)
	}
}

func TestNewBuildsAValidEnvelope(t *testing.T) {
	org := uuid.New()
	event, err := New(org, "vikn.desk", "com.vikn.desk.ticket.created.v1", "organizations/x", connect.UserActor(uuid.New()), map[string]any{"ticket_id": "t"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := event.Envelope().Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if event.Envelope().Actor.Kind != connect.ActorUser {
		t.Fatalf("actor = %+v", event.Envelope().Actor)
	}
}

type fakePublisher struct {
	fail      bool
	published []Event
}

func (f *fakePublisher) Publish(_ context.Context, e Event) error {
	if f.fail {
		return errors.New("broker away")
	}
	f.published = append(f.published, e)
	return nil
}

func TestDispatcherClaimsPublishesAndRetries(t *testing.T) {
	db := testdb.Open(t, Models()...)
	t.Setenv("NATS_URL", "nats://test")
	org := uuid.New()
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	event, _ := New(org, "vikn.desk", "com.vikn.desk.ticket.created.v1", "s", connect.Actor{}, map[string]any{"organization_id": org})
	event.OccurredAt, event.AvailableAt = now, now
	if err := Enqueue(db, event); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	bad, _ := New(org, "vikn.desk", "ticket.created", "s", connect.Actor{}, nil)
	if err := Enqueue(db, bad); err == nil {
		t.Fatal("a non-catalog type must not be stored")
	}

	publisher := &fakePublisher{fail: true}
	dispatcher := &Dispatcher{DB: db, Publisher: publisher, Now: func() time.Time { return now }}
	if n, err := dispatcher.DispatchOnce(context.Background(), 10); err != nil || n != 0 {
		t.Fatalf("first dispatch: n=%d err=%v", n, err)
	}
	var row Event
	db.First(&row, "id = ?", event.ID)
	if row.Status != StatusFailed || row.AttemptCount != 1 || row.LastError == nil || !row.AvailableAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("after failure: %+v", row)
	}

	publisher.fail = false
	if n, _ := dispatcher.DispatchOnce(context.Background(), 10); n != 0 {
		t.Fatal("a row in backoff must not be retried early")
	}
	dispatcher.Now = func() time.Time { return now.Add(2 * time.Minute) }
	if n, err := dispatcher.DispatchOnce(context.Background(), 10); err != nil || n != 1 {
		t.Fatalf("retry: n=%d err=%v", n, err)
	}
	db.First(&row, "id = ?", event.ID)
	if row.Status != StatusPublished || row.PublishedAt == nil || row.LastError != nil || row.AttemptCount != 2 {
		t.Fatalf("after publish: %+v", row)
	}
	if len(publisher.published) != 1 || publisher.published[0].Envelope().OrganizationID != org {
		t.Fatalf("published = %+v", publisher.published)
	}
}
