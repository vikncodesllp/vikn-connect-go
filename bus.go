package connect

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// Connect opens a NATS connection and JetStream context using NATS_URL.
// It returns ErrBusNotConfigured when the variable is unset so callers can
// make the bus opt-in per process.
func Connect(clientName string) (*nats.Conn, nats.JetStreamContext, error) {
	url := BusURL()
	if url == "" {
		return nil, nil, ErrBusNotConfigured
	}
	return ConnectURL(url, clientName)
}

// ConnectURL is Connect for an explicit URL.
func ConnectURL(url, clientName string) (*nats.Conn, nats.JetStreamContext, error) {
	connection, err := nats.Connect(url, nats.Name(clientName), nats.Timeout(5*time.Second))
	if err != nil {
		return nil, nil, err
	}
	js, err := connection.JetStream()
	if err != nil {
		connection.Close()
		return nil, nil, err
	}
	return connection, js, nil
}

// EnsureStream provisions the Vikn stream when it is absent. An existing
// stream is left untouched: retention and replication are operator-owned
// settings that a process must not silently change.
func EnsureStream(js nats.JetStreamContext, stream, subject string) error {
	if strings.TrimSpace(stream) == "" {
		stream = DefaultStream
	}
	if strings.TrimSpace(subject) == "" {
		subject = DefaultAuditSubject
	}
	if _, err := js.StreamInfo(stream); err == nil {
		return nil
	} else if !errors.Is(err, nats.ErrStreamNotFound) {
		return fmt.Errorf("inspect stream %s: %w", stream, err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:      stream,
		Subjects:  []string{subject},
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		Discard:   nats.DiscardOld,
	}); err != nil {
		return fmt.Errorf("create stream %s: %w", stream, err)
	}
	return nil
}
