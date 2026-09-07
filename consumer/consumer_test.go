package consumer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"

	connect "github.com/vikncodesllp/vikn-connect-go"
)

type fakeAck struct{ calls []string }

func (f *fakeAck) Ack(...nats.AckOpt) error  { f.calls = append(f.calls, "ack"); return nil }
func (f *fakeAck) Nak(...nats.AckOpt) error  { f.calls = append(f.calls, "nak"); return nil }
func (f *fakeAck) Term(...nats.AckOpt) error { f.calls = append(f.calls, "term"); return nil }
func (f *fakeAck) NakWithDelay(d time.Duration, _ ...nats.AckOpt) error {
	f.calls = append(f.calls, "nak-delay")
	return nil
}

type memStore struct{ rejections []Rejection }

func (m *memStore) RecordRejection(_ context.Context, r Rejection) error {
	m.rejections = append(m.rejections, r)
	return nil
}

func message(actor connect.Actor) *nats.Msg {
	env := connect.Envelope{
		ID: "evt-1", Source: "vikn.desk", Type: "com.vikn.desk.ticket.created.v1", Subject: "s",
		Time: time.Now().UTC(), Data: []byte(`{"organization_id":"2f8c6b8e-5b0f-4a6b-9a3e-1b2c3d4e5f60"}`),
		OrganizationID: uuid.MustParse("2f8c6b8e-5b0f-4a6b-9a3e-1b2c3d4e5f60"), Actor: actor,
	}
	return env.NewMsg("vikn")
}

func meta(delivered uint64) *nats.MsgMetadata {
	return &nats.MsgMetadata{Sequence: nats.SequencePair{Stream: 7, Consumer: 3}, NumDelivered: delivered}
}

func TestHandleAcksSuccess(t *testing.T) {
	ack := &fakeAck{}
	var seen Delivery
	opts := Options{Handler: func(_ context.Context, d Delivery) error { seen = d; return nil }}
	if err := opts.handle(context.Background(), message(connect.UserActor(uuid.New())), meta(1), ack); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(ack.calls) != 1 || ack.calls[0] != "ack" {
		t.Fatalf("calls = %v", ack.calls)
	}
	if seen.StreamSequence != 7 || seen.DeliveryCount != 1 || seen.Envelope.ID != "evt-1" {
		t.Fatalf("delivery = %+v", seen)
	}
}

func TestHandleSkipsIntegrationActor(t *testing.T) {
	ack := &fakeAck{}
	called, skipped := false, ""
	opts := Options{
		Handler: func(context.Context, Delivery) error { called = true; return nil },
		Skipped: func(_ context.Context, _ Delivery, reason string) error { skipped = reason; return nil },
	}
	if err := opts.handle(context.Background(), message(connect.IntegrationActor("projects")), meta(1), ack); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if called {
		t.Fatal("handler must not run for an integration actor")
	}
	if skipped != "integration_actor" || ack.calls[0] != "ack" {
		t.Fatalf("skipped = %q, calls = %v", skipped, ack.calls)
	}

	ack = &fakeAck{}
	called = false
	opts.AllowIntegrationActor = true
	if err := opts.handle(context.Background(), message(connect.IntegrationActor("projects")), meta(1), ack); err != nil || !called {
		t.Fatalf("observer should see integration events: called=%v err=%v", called, err)
	}
}

func TestHandleNaksWithDelayBeforeLastDelivery(t *testing.T) {
	ack := &fakeAck{}
	store := &memStore{}
	opts := Options{Store: store, Handler: func(context.Context, Delivery) error { return errors.New("db down") }}
	if err := opts.handle(context.Background(), message(connect.Actor{}), meta(3), ack); err == nil {
		t.Fatal("expected the handler error to surface")
	}
	if ack.calls[0] != "nak-delay" || len(store.rejections) != 0 {
		t.Fatalf("calls = %v, rejections = %d", ack.calls, len(store.rejections))
	}
}

func TestHandleRecordsPoisonOnLastDelivery(t *testing.T) {
	ack := &fakeAck{}
	store := &memStore{}
	opts := Options{Store: store, MaxDeliver: 5, Handler: func(context.Context, Delivery) error { return errors.New("still broken") }}
	if err := opts.handle(context.Background(), message(connect.Actor{}), meta(5), ack); err == nil {
		t.Fatal("expected an error")
	}
	if ack.calls[0] != "term" {
		t.Fatalf("calls = %v", ack.calls)
	}
	if len(store.rejections) != 1 || store.rejections[0].EventID != "evt-1" || store.rejections[0].DeliveryCount != 5 {
		t.Fatalf("rejections = %+v", store.rejections)
	}
}

func TestHandleRecordsPermanentErrorAtOnce(t *testing.T) {
	ack := &fakeAck{}
	store := &memStore{}
	opts := Options{Store: store, Handler: func(context.Context, Delivery) error { return Permanent(errors.New("not my event")) }}
	if err := opts.handle(context.Background(), message(connect.Actor{}), meta(1), ack); err == nil {
		t.Fatal("expected an error")
	}
	if ack.calls[0] != "term" || len(store.rejections) != 1 || !strings.Contains(store.rejections[0].Reason, "not my event") {
		t.Fatalf("calls = %v, rejections = %+v", ack.calls, store.rejections)
	}
	if Permanent(nil) != nil {
		t.Fatal("Permanent(nil) must be nil")
	}
}

func TestHandleRejectsMalformed(t *testing.T) {
	ack := &fakeAck{}
	store := &memStore{}
	msg := message(connect.Actor{})
	msg.Data = []byte("not json")
	opts := Options{Store: store, Handler: func(context.Context, Delivery) error { t.Fatal("must not run"); return nil }}
	if err := opts.handle(context.Background(), msg, meta(1), ack); err == nil {
		t.Fatal("expected an error")
	}
	if ack.calls[0] != "term" || len(store.rejections) != 1 || string(store.rejections[0].Payload) != "not json" {
		t.Fatalf("calls = %v, rejections = %+v", ack.calls, store.rejections)
	}
}

func TestRedeliveryDelayIsCapped(t *testing.T) {
	if redeliveryDelay(1) != time.Second || redeliveryDelay(4) != 8*time.Second {
		t.Fatal("delay should double per delivery")
	}
	if redeliveryDelay(50) != 5*time.Minute {
		t.Fatalf("delay = %s", redeliveryDelay(50))
	}
}
