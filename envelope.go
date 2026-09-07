package connect

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

// Header names for the CloudEvents binary-mode envelope on NATS.
const (
	HeaderSpecVersion  = "ce-specversion"
	HeaderID           = "ce-id"
	HeaderSource       = "ce-source"
	HeaderType         = "ce-type"
	HeaderSubject      = "ce-subject"
	HeaderTime         = "ce-time"
	HeaderContentType  = "content-type"
	HeaderOrganization = "ce-viknorg"   // organization id: routing and audit without parsing the body
	HeaderLink         = "ce-viknlink"  // link id, when the event concerns a linked pair
	HeaderActor        = "ce-viknactor" // user:<uuid> | integration:<client_id>
	HeaderMsgID        = "Nats-Msg-Id"  // broker-side dedupe of a retried outbox row

	// ContentTypeJSON is the only data content type the bus carries.
	ContentTypeJSON = "application/json"
)

// Envelope is one CloudEvent as it travels on the bus, with the three Vikn
// extension attributes promoted to fields.
type Envelope struct {
	ID          string
	Source      string
	Type        string
	Subject     string
	Time        time.Time
	ContentType string
	Data        json.RawMessage

	OrganizationID uuid.UUID
	LinkID         *uuid.UUID
	Actor          Actor

	// OrganizationFromBody is set by ParseMsg when ce-viknorg was absent and
	// the organization came from `data.organization_id`. That fallback exists
	// for one release, for events published before the extension; a consumer
	// can log or count it, and it is never set for events this module wrote.
	OrganizationFromBody bool
}

// Validate applies the producer-side rules: every required attribute, a
// catalog-shaped type, JSON data, and a well-formed actor.
func (e Envelope) Validate() error {
	switch {
	case strings.TrimSpace(e.ID) == "":
		return errors.New("event id is required")
	case strings.TrimSpace(e.Source) == "":
		return errors.New("event source is required")
	case !ValidEventType(e.Type):
		return fmt.Errorf("event type %q is not com.vikn.<app>.<entity>.<action>.vN", e.Type)
	case e.OrganizationID == uuid.Nil:
		return errors.New("organization id is required")
	case e.Time.IsZero():
		return errors.New("event time is required")
	case len(e.Data) > 0 && !json.Valid(e.Data):
		return errors.New("event data is not valid JSON")
	}
	if e.LinkID != nil && *e.LinkID == uuid.Nil {
		return errors.New("link id must be nil or a uuid")
	}
	return e.Actor.Validate()
}

// ApplyTo writes the envelope onto a NATS message in binary mode. The
// message subject is set from the event type when it is empty.
func (e Envelope) ApplyTo(msg *nats.Msg, subjectPrefix string) {
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
	if msg.Subject == "" {
		msg.Subject = Subject(subjectPrefix, e.Type)
	}
	msg.Data = append([]byte(nil), e.Data...)
	if len(msg.Data) == 0 {
		msg.Data = []byte("{}")
	}
	contentType := e.ContentType
	if contentType == "" {
		contentType = ContentTypeJSON
	}
	msg.Header.Set(HeaderMsgID, e.ID)
	msg.Header.Set(HeaderContentType, contentType)
	msg.Header.Set(HeaderSpecVersion, SpecVersion)
	msg.Header.Set(HeaderID, e.ID)
	msg.Header.Set(HeaderSource, e.Source)
	msg.Header.Set(HeaderType, e.Type)
	msg.Header.Set(HeaderSubject, e.Subject)
	msg.Header.Set(HeaderTime, e.Time.UTC().Format(time.RFC3339Nano))
	msg.Header.Set(HeaderOrganization, e.OrganizationID.String())
	if e.LinkID != nil && *e.LinkID != uuid.Nil {
		msg.Header.Set(HeaderLink, e.LinkID.String())
	}
	if !e.Actor.IsZero() {
		msg.Header.Set(HeaderActor, e.Actor.String())
	}
}

// NewMsg builds a ready-to-publish message for the envelope.
func (e Envelope) NewMsg(subjectPrefix string) *nats.Msg {
	msg := nats.NewMsg(Subject(subjectPrefix, e.Type))
	e.ApplyTo(msg, subjectPrefix)
	return msg
}

// ParseMsg reads a binary-mode CloudEvent off the wire. It is strict about
// the CloudEvents core (id, source, type, specversion 1.0, RFC 3339 time, JSON
// data) and lenient about the Vikn extensions: a missing ce-viknorg falls
// back to data.organization_id for one release, a missing actor is zero.
func ParseMsg(msg *nats.Msg) (Envelope, error) {
	if msg == nil {
		return Envelope{}, errors.New("message is nil")
	}
	header := func(name string) string { return strings.TrimSpace(msg.Header.Get(name)) }
	env := Envelope{
		ID:          header(HeaderID),
		Source:      header(HeaderSource),
		Type:        header(HeaderType),
		Subject:     header(HeaderSubject),
		ContentType: header(HeaderContentType),
		Data:        append(json.RawMessage(nil), msg.Data...),
	}
	if env.ID == "" || env.Source == "" || env.Type == "" {
		return Envelope{}, errors.New("ce-id, ce-source and ce-type are required")
	}
	if version := header(HeaderSpecVersion); version != SpecVersion {
		return Envelope{}, fmt.Errorf("unsupported ce-specversion %q", version)
	}
	if env.ContentType == "" {
		env.ContentType = ContentTypeJSON
	}
	if !json.Valid(env.Data) {
		return Envelope{}, errors.New("event data is not valid JSON")
	}
	var err error
	if env.Time, err = time.Parse(time.RFC3339Nano, header(HeaderTime)); err != nil {
		return Envelope{}, fmt.Errorf("invalid ce-time: %w", err)
	}

	if raw := header(HeaderOrganization); raw != "" {
		if env.OrganizationID, err = uuid.Parse(raw); err != nil {
			return Envelope{}, fmt.Errorf("invalid %s: %w", HeaderOrganization, err)
		}
	} else if id, ok := organizationFromBody(env.Data); ok {
		env.OrganizationID = id
		env.OrganizationFromBody = true
	}
	if raw := header(HeaderLink); raw != "" {
		id, parseErr := uuid.Parse(raw)
		if parseErr != nil {
			return Envelope{}, fmt.Errorf("invalid %s: %w", HeaderLink, parseErr)
		}
		env.LinkID = &id
	}
	if env.Actor, err = ParseActor(header(HeaderActor)); err != nil {
		return Envelope{}, fmt.Errorf("invalid %s: %w", HeaderActor, err)
	}
	return env, nil
}

// organizationFromBody is the one-release fallback for events published
// before ce-viknorg existed. Every event type in the catalog so far carries
// organization_id at the top level of its data.
func organizationFromBody(data []byte) (uuid.UUID, bool) {
	var body struct {
		OrganizationID string `json:"organization_id"`
	}
	if err := json.Unmarshal(data, &body); err != nil || body.OrganizationID == "" {
		return uuid.Nil, false
	}
	id, err := uuid.Parse(body.OrganizationID)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, false
	}
	return id, true
}

// Decode unmarshals the event data into v.
func (e Envelope) Decode(v any) error {
	if len(e.Data) == 0 {
		return errors.New("event has no data")
	}
	return json.Unmarshal(e.Data, v)
}
