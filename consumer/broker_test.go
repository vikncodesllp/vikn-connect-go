package consumer

import (
	"context"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"

	connect "github.com/vikncodesllp/vikn-connect-go"
)

// startBroker runs a throwaway nats-server with JetStream on a free port.
// Skips when the binary is not on PATH; CONNECT_TEST_NATS_URL points at an
// already-running broker instead.
func startBroker(t *testing.T) string {
	t.Helper()
	if url := os.Getenv("CONNECT_TEST_NATS_URL"); url != "" {
		return url
	}
	bin, err := exec.LookPath("nats-server")
	if err != nil {
		t.Skip("nats-server not installed and CONNECT_TEST_NATS_URL not set")
	}
	port := 42200 + int(time.Now().UnixNano()%300)
	dir := t.TempDir()
	cmd := exec.Command(bin, "-js", "-sd", dir, "-a", "127.0.0.1", "-p", strconv.Itoa(port))
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() })
	url := "nats://127.0.0.1:" + strconv.Itoa(port)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if nc, err := nats.Connect(url); err == nil {
			nc.Close()
			return url
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("broker did not come up")
	return ""
}

func publish(t *testing.T, js nats.JetStreamContext, id string) {
	t.Helper()
	env := connect.Envelope{ID: id, Source: "vikn.desk", Type: "com.vikn.desk.ticket.created.v1", Subject: "s", Time: time.Now().UTC(),
		Data: []byte(`{}`), OrganizationID: uuid.New()}
	if _, err := js.PublishMsg(env.NewMsg("vikn")); err != nil {
		t.Fatal(err)
	}
}

type collector struct {
	mu  sync.Mutex
	ids []string
}

func (c *collector) handler(_ context.Context, d Delivery) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ids = append(c.ids, d.Envelope.ID)
	return nil
}

func (c *collector) wait(t *testing.T, n int) []string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		got := append([]string(nil), c.ids...)
		c.mu.Unlock()
		if len(got) >= n {
			return got
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.ids...)
}

// A brand-new rules durable must not replay the stream; an observer that
// asks for history gets it; an existing durable binds regardless of policy.
func TestSubscribeDeliverPolicy(t *testing.T) {
	url := startBroker(t)
	nc, js, err := connect.ConnectURL(url, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	if err := connect.EnsureStream(js, "VIKN", "vikn.>"); err != nil {
		t.Fatal(err)
	}
	publish(t, js, "old-1")
	publish(t, js, "old-2")

	rules := &collector{}
	sub, err := Subscribe(js, Options{Subject: "vikn.>", Durable: "rules-v1", Stream: "VIKN", Handler: rules.handler})
	if err != nil {
		t.Fatalf("rules subscribe: %v", err)
	}
	audit := &collector{}
	auditSub, err := Subscribe(js, Options{Subject: "vikn.>", Durable: "audit-v1", Stream: "VIKN", Handler: audit.handler, ReplayHistory: true, AllowIntegrationActor: true})
	if err != nil {
		t.Fatalf("audit subscribe: %v", err)
	}
	publish(t, js, "new-1")

	if got := rules.wait(t, 1); len(got) != 1 || got[0] != "new-1" {
		t.Fatalf("rules consumer must start at new events, got %v", got)
	}
	if got := audit.wait(t, 3); len(got) != 3 {
		t.Fatalf("observer must replay history, got %v", got)
	}

	// Rebinding the existing rules durable, even with ReplayHistory set,
	// keeps its position instead of failing or starting over.
	_ = sub.Unsubscribe()
	_ = auditSub.Unsubscribe()
	rebound := &collector{}
	if _, err := Subscribe(js, Options{Subject: "vikn.>", Durable: "rules-v1", Stream: "VIKN", Handler: rebound.handler, ReplayHistory: true}); err != nil {
		t.Fatalf("rebind existing durable: %v", err)
	}
	publish(t, js, "new-2")
	if got := rebound.wait(t, 1); len(got) != 1 || got[0] != "new-2" {
		t.Fatalf("rebound durable should continue from its position, got %v", got)
	}
}
