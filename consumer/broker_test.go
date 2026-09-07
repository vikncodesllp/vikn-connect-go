package consumer

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	connect "github.com/vikncodesllp/vikn-connect-go"
)

// TestHarnessAgainstABroker drives Subscribe/Handle through a real JetStream
// server so the durable, stream binding, MaxDeliver and nak backoff are
// exercised for real. Set CONNECT_TEST_NATS_URL to run it, e.g. after
// `nats-server -js -p 42224`.
func TestHarnessAgainstABroker(t *testing.T) {
	url := os.Getenv("CONNECT_TEST_NATS_URL")
	if url == "" {
		t.Skip("CONNECT_TEST_NATS_URL not set")
	}
	conn, js, err := connect.ConnectURL(url, "harness-test")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()
	stream := "VIKNTEST" + uuid.New().String()[:8]
	prefix := "t" + uuid.New().String()[:8]
	if err := connect.EnsureStream(js, stream, prefix+".>"); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	defer js.DeleteStream(stream)

	org := uuid.New()
	publish := func(id string, actor connect.Actor) {
		env := connect.Envelope{ID: id, Source: "vikn.desk", Type: "com.vikn.desk.ticket.created.v1", Subject: "s",
			Time: time.Now().UTC(), Data: []byte(`{"ticket_id":"x"}`), OrganizationID: org, Actor: actor}
		if _, err := js.PublishMsg(env.NewMsg(prefix)); err != nil {
			t.Fatalf("publish %s: %v", id, err)
		}
	}

	var mu sync.Mutex
	handled := map[string]int{}
	skipped := []string{}
	store := &memStore{}
	done := make(chan struct{}, 8)
	opts := Options{
		Subject: prefix + ".vikn.desk.ticket.created.v1", Durable: "harness-test", Stream: stream, MaxDeliver: 2,
		Store: store,
		Handler: func(_ context.Context, d Delivery) error {
			mu.Lock()
			handled[d.Envelope.ID]++
			mu.Unlock()
			done <- struct{}{}
			if d.Envelope.ID == "poison" {
				return errors.New("always fails")
			}
			return nil
		},
		Skipped: func(_ context.Context, d Delivery, reason string) error {
			mu.Lock()
			skipped = append(skipped, d.Envelope.ID+":"+reason)
			mu.Unlock()
			done <- struct{}{}
			return nil
		},
	}
	sub, err := Subscribe(js, opts)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	publish("good", connect.UserActor(uuid.New()))
	publish("loop", connect.IntegrationActor("other-app"))
	publish("poison", connect.Actor{})

	// good: 1 handler call. loop: 1 skip. poison: 2 handler calls (MaxDeliver 2, ~1s nak delay).
	deadline := time.After(15 * time.Second)
	for events := 0; events < 4; {
		select {
		case <-done:
			events++
		case <-deadline:
			t.Fatalf("timed out: handled=%v skipped=%v", handled, skipped)
		}
	}
	time.Sleep(500 * time.Millisecond) // let the final term/rejection land
	mu.Lock()
	defer mu.Unlock()
	if handled["good"] != 1 || handled["poison"] != 2 || handled["loop"] != 0 {
		t.Fatalf("handled = %v", handled)
	}
	if len(skipped) != 1 || skipped[0] != "loop:integration_actor" {
		t.Fatalf("skipped = %v", skipped)
	}
	if len(store.rejections) != 1 || store.rejections[0].EventID != "poison" || store.rejections[0].DeliveryCount != 2 {
		t.Fatalf("rejections = %+v", store.rejections)
	}
	info, err := js.ConsumerInfo(stream, "harness-test")
	if err != nil {
		t.Fatalf("ConsumerInfo: %v", err)
	}
	if info.NumPending != 0 || info.NumAckPending != 0 {
		t.Fatalf("consumer should be drained: pending=%d ackpending=%d", info.NumPending, info.NumAckPending)
	}
}
