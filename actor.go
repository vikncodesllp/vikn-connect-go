package connect

import (
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// ActorKind says what kind of principal caused an event.
type ActorKind string

const (
	// ActorUser is a person acting through an app's own UI or API token.
	ActorUser ActorKind = "user"
	// ActorIntegration is another Vikn app acting with a client-credentials
	// token. Consumers must not fire tenant rules on these events, or two
	// apps reacting to each other will ping-pong forever.
	ActorIntegration ActorKind = "integration"
)

// Actor is the `ce-viknactor` extension: who caused the event. It is the
// loop breaker for the whole bus, so producers must always set it.
type Actor struct {
	Kind ActorKind
	ID   string
}

// UserActor identifies a person by their auth_go user id.
func UserActor(userID uuid.UUID) Actor {
	if userID == uuid.Nil {
		return Actor{}
	}
	return Actor{Kind: ActorUser, ID: userID.String()}
}

// IntegrationActor identifies another app by its OAuth client id.
func IntegrationActor(clientID string) Actor {
	clientID = strings.TrimSpace(clientID)
	if clientID == "" {
		return Actor{}
	}
	return Actor{Kind: ActorIntegration, ID: clientID}
}

// ParseActor reads the header form `user:<uuid>` or `integration:<client>`.
// An empty string is a zero Actor, not an error, because events published
// before the extension existed carry no actor at all.
func ParseActor(value string) (Actor, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return Actor{}, nil
	}
	kind, id, found := strings.Cut(value, ":")
	if !found || strings.TrimSpace(id) == "" {
		return Actor{}, fmt.Errorf("actor %q must be <kind>:<id>", value)
	}
	actor := Actor{Kind: ActorKind(strings.ToLower(strings.TrimSpace(kind))), ID: strings.TrimSpace(id)}
	switch actor.Kind {
	case ActorUser:
		if _, err := uuid.Parse(actor.ID); err != nil {
			return Actor{}, fmt.Errorf("user actor id %q is not a uuid", actor.ID)
		}
	case ActorIntegration:
	default:
		return Actor{}, fmt.Errorf("unknown actor kind %q", actor.Kind)
	}
	return actor, nil
}

// String renders the header form. A zero Actor renders as "".
func (a Actor) String() string {
	if a.IsZero() {
		return ""
	}
	return string(a.Kind) + ":" + a.ID
}

func (a Actor) IsZero() bool { return a.Kind == "" || a.ID == "" }

// IsIntegration reports whether the event was caused by another app rather
// than a person. This is the check every consumer harness applies before
// letting a tenant rule fire.
func (a Actor) IsIntegration() bool { return a.Kind == ActorIntegration }

// Validate rejects a malformed non-zero actor.
func (a Actor) Validate() error {
	if a.IsZero() {
		if a.Kind != "" || a.ID != "" {
			return errors.New("actor kind and id must both be set")
		}
		return nil
	}
	_, err := ParseActor(a.String())
	return err
}
