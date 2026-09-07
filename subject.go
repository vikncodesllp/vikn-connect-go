package connect

import (
	"errors"
	"os"
	"regexp"
	"strings"
)

const (
	// SpecVersion is the only CloudEvents version the bus speaks.
	SpecVersion = "1.0"
	// DefaultStream is the JetStream stream every Vikn subject lives in.
	DefaultStream = "VIKN"
	// DefaultSubjectPrefix is prepended to the event type minus `com.`:
	// com.vikn.desk.ticket.created.v1 → vikn.desk.ticket.created.v1.
	DefaultSubjectPrefix = "vikn"
	// DefaultAuditSubject matches every event on the bus.
	DefaultAuditSubject = "vikn.>"
	// DefaultMaxDeliver is how many times JetStream redelivers before the
	// consumer harness records the message as poison and terminates it.
	DefaultMaxDeliver = 20
	// EventTypePrefix is the reverse-DNS root of every Vikn event type.
	EventTypePrefix = "com.vikn."
)

// eventTypePattern is com.vikn.<app>.<entity>[.<more>].<action>.v<N>.
var eventTypePattern = regexp.MustCompile(`^com\.vikn\.[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)+\.v[1-9][0-9]*$`)

// ValidEventType reports whether a type follows the catalog naming rule in
// vikn-connect.md §5.2. Breaking a payload means a new vN, never an edit.
func ValidEventType(eventType string) bool {
	return eventTypePattern.MatchString(eventType)
}

// Subject maps an event type to the NATS subject it is published on.
func Subject(prefix, eventType string) string {
	name := strings.TrimPrefix(eventType, "com.")
	prefix = strings.Trim(strings.TrimSpace(prefix), ".")
	if prefix == "" || strings.HasPrefix(name, prefix+".") {
		return name
	}
	return prefix + "." + name
}

// SubjectPrefixFromEnv honours NATS_SUBJECT_PREFIX, defaulting to "vikn".
func SubjectPrefixFromEnv() string {
	prefix := strings.Trim(strings.TrimSpace(os.Getenv("NATS_SUBJECT_PREFIX")), ".")
	if prefix == "" {
		return DefaultSubjectPrefix
	}
	return prefix
}

// BusURL is NATS_URL, trimmed. Empty means no broker for this process.
func BusURL() string { return strings.TrimSpace(os.Getenv("NATS_URL")) }

// BusConfigured is the single predicate behind both enqueue and dispatch, so
// an app can never record events that nothing will drain.
func BusConfigured() bool { return BusURL() != "" }

// ErrBusNotConfigured is returned by connection helpers when NATS_URL is unset.
var ErrBusNotConfigured = errors.New("NATS_URL is not configured")
