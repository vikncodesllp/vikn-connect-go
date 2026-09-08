// Package consumer is the subscribe/handle harness every consuming app
// shares. It owns the parts that drift when copied: parsing, the actor
// check that stops two apps reacting to each other, ack discipline, and
// what happens to a message that keeps failing.
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"gorm.io/gorm"

	connect "github.com/vikncodesllp/vikn-connect-go"
)

const maxRejectedPayloadBytes = 64 * 1024

// Delivery is one message as handed to an app handler.
type Delivery struct {
	Envelope         connect.Envelope
	NATSSubject      string
	StreamSequence   uint64
	ConsumerSequence uint64
	DeliveryCount    uint64
	ReceivedAt       time.Time
}

// Handler processes one delivery. Return nil to ack; return an error to
// have it redelivered (or, on the last delivery, recorded and terminated).
type Handler func(ctx context.Context, delivery Delivery) error

// Rejection is the per-app dead-letter row: a malformed event, or one that
// exhausted its deliveries. Visible, rather than silently dropped.
type Rejection struct {
	ID             uuid.UUID    `gorm:"type:uuid;primaryKey" json:"id"`
	EventID        string       `gorm:"type:varchar(100);index" json:"event_id,omitempty"`
	NATSSubject    string       `gorm:"type:varchar(255);not null;index" json:"nats_subject"`
	Headers        connect.JSON `gorm:"type:jsonb;not null" json:"-"`
	Payload        []byte       `gorm:"type:bytea" json:"-"`
	Reason         string       `gorm:"type:text;not null" json:"reason"`
	StreamSequence uint64       `gorm:"not null" json:"stream_sequence"`
	DeliveryCount  uint64       `gorm:"not null" json:"delivery_count"`
	ReceivedAt     time.Time    `gorm:"not null;index" json:"received_at"`
}

func (Rejection) TableName() string { return "integration_event_rejections" }

func (r *Rejection) BeforeCreate(tx *gorm.DB) error {
	if r.ID == uuid.Nil {
		r.ID = uuid.New()
	}
	return nil
}

// Store records rejections. GormStore is the usual implementation.
type Store interface {
	RecordRejection(context.Context, Rejection) error
}

type GormStore struct{ DB *gorm.DB }

func (s GormStore) RecordRejection(ctx context.Context, rejection Rejection) error {
	return s.DB.WithContext(ctx).Create(&rejection).Error
}

// Options configures one durable subscription.
type Options struct {
	Subject string // e.g. vikn.desk.ticket.created.v1, or vikn.> for everything
	Durable string // durable consumer name; stable across restarts
	Stream  string // defaults to VIKN

	// MaxDeliver is how many attempts a message gets before it is recorded
	// as a rejection and terminated. Defaults to connect.DefaultMaxDeliver.
	MaxDeliver int

	Handler Handler
	Store   Store

	// ReplayHistory makes a durable that does not exist yet start at the
	// beginning of the stream instead of at new events. Leave it false for
	// anything that fires tenant rules: a rule saved today must not run over
	// last month's events the moment a new consumer is created. Set it for
	// pure observers and cache maintainers (audit, link mirroring), which
	// want the whole history. An existing durable keeps the position it
	// has; this only decides where a brand-new one begins.
	ReplayHistory bool

	// AllowIntegrationActor lets events caused by another app through.
	// Leave it false for anything that fires tenant rules; set it for pure
	// observers such as the audit consumer.
	AllowIntegrationActor bool

	// Skipped, when set, is told about events the harness acked without
	// calling Handler (integration actor). Apps use it to keep history.
	Skipped func(ctx context.Context, delivery Delivery, reason string) error

	Now func() time.Time
	Log func(format string, args ...any)
}

func (o Options) withDefaults() Options {
	if strings.TrimSpace(o.Stream) == "" {
		o.Stream = connect.DefaultStream
	}
	if o.MaxDeliver <= 0 {
		o.MaxDeliver = connect.DefaultMaxDeliver
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Log == nil {
		o.Log = func(string, ...any) {}
	}
	return o
}

// Subscribe opens the durable queue subscription and routes every message
// through Handle. The returned subscription is drained by the caller.
func Subscribe(js nats.JetStreamContext, opts Options) (*nats.Subscription, error) {
	opts = opts.withDefaults()
	if strings.TrimSpace(opts.Subject) == "" || strings.TrimSpace(opts.Durable) == "" {
		return nil, errors.New("consumer subject and durable name are required")
	}
	if opts.Handler == nil {
		return nil, errors.New("consumer handler is required")
	}
	ctx := context.Background()
	handler := func(msg *nats.Msg) {
		if err := opts.Handle(ctx, msg); err != nil {
			opts.Log("connect consumer %s: %v", opts.Durable, err)
		}
	}
	// The durable is created here, explicitly, and then bound to. Letting
	// QueueSubscribe create it would make the client delete it again on
	// Unsubscribe/Drain — every clean shutdown would forget the position,
	// and the next start would either replay the whole stream or skip
	// everything published while the process was down. A consumer this
	// code created is never deleted by it.
	if _, err := js.ConsumerInfo(opts.Stream, opts.Durable); err != nil {
		if !errors.Is(err, nats.ErrConsumerNotFound) {
			return nil, fmt.Errorf("inspect consumer %s: %w", opts.Durable, err)
		}
		cfg := &nats.ConsumerConfig{
			Durable:        opts.Durable,
			DeliverSubject: nats.NewInbox(),
			DeliverGroup:   opts.Durable,
			FilterSubject:  opts.Subject,
			AckPolicy:      nats.AckExplicitPolicy,
			MaxDeliver:     opts.MaxDeliver,
			ReplayPolicy:   nats.ReplayInstantPolicy,
			DeliverPolicy:  nats.DeliverNewPolicy,
		}
		if opts.ReplayHistory {
			cfg.DeliverPolicy = nats.DeliverAllPolicy
		}
		if _, err := js.AddConsumer(opts.Stream, cfg); err != nil {
			return nil, fmt.Errorf("create consumer %s: %w", opts.Durable, err)
		}
	}
	return js.QueueSubscribe(opts.Subject, opts.Durable, handler, nats.Bind(opts.Stream, opts.Durable), nats.ManualAck())
}

// PermanentError marks a handler failure that no retry can fix: a
// well-formed event this app cannot use, a payload that fails validation.
// The harness records it and terminates the message on the first delivery
// instead of burning through MaxDeliver.
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return "permanent: " + e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent wraps err so the harness does not retry it.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}

// acker is the slice of *nats.Msg the harness needs, so tests can drive it
// with messages that are not bound to a live subscription.
type acker interface {
	Ack(...nats.AckOpt) error
	Nak(...nats.AckOpt) error
	NakWithDelay(time.Duration, ...nats.AckOpt) error
	Term(...nats.AckOpt) error
}

// Handle applies the harness rules to one message:
//
//  1. Malformed CloudEvent → record a rejection, terminate. Storage failure
//     → nak, so the record is attempted again.
//  2. Actor is an integration client and AllowIntegrationActor is false →
//     ack without calling Handler. This is the loop breaker.
//  3. Handler error before the last delivery → nak with backoff.
//  4. Handler error on the last delivery, or a PermanentError on any
//     delivery → record a rejection, terminate.
//  5. Handler nil → ack.
func (o Options) Handle(ctx context.Context, msg *nats.Msg) error {
	metadata, err := msg.Metadata()
	if err != nil {
		return fmt.Errorf("JetStream metadata: %w", err)
	}
	return o.handle(ctx, msg, metadata, msg)
}

func (o Options) handle(ctx context.Context, msg *nats.Msg, metadata *nats.MsgMetadata, ack acker) error {
	o = o.withDefaults()
	receivedAt := o.Now().UTC()
	env, err := connect.ParseMsg(msg)
	if err != nil {
		return o.reject(ctx, msg, metadata, receivedAt, ack, fmt.Errorf("invalid CloudEvent: %w", err))
	}
	delivery := Delivery{
		Envelope: env, NATSSubject: msg.Subject, ReceivedAt: receivedAt,
		StreamSequence: metadata.Sequence.Stream, ConsumerSequence: metadata.Sequence.Consumer, DeliveryCount: metadata.NumDelivered,
	}
	if env.Actor.IsIntegration() && !o.AllowIntegrationActor {
		if o.Skipped != nil {
			if skipErr := o.Skipped(ctx, delivery, "integration_actor"); skipErr != nil {
				_ = ack.Nak()
				return fmt.Errorf("record skipped event %s: %w", env.ID, skipErr)
			}
		}
		return ack.Ack()
	}
	if o.Handler == nil {
		return ack.Ack()
	}
	if handleErr := o.Handler(ctx, delivery); handleErr != nil {
		var permanent *PermanentError
		if errors.As(handleErr, &permanent) {
			return o.reject(ctx, msg, metadata, receivedAt, ack, fmt.Errorf("handler refused event: %w", permanent.Err))
		}
		if int(metadata.NumDelivered) >= o.MaxDeliver {
			return o.reject(ctx, msg, metadata, receivedAt, ack,
				fmt.Errorf("handler failed on delivery %d of %d: %w", metadata.NumDelivered, o.MaxDeliver, handleErr))
		}
		_ = ack.NakWithDelay(redeliveryDelay(metadata.NumDelivered))
		return fmt.Errorf("event %s (delivery %d): %w", env.ID, metadata.NumDelivered, handleErr)
	}
	return ack.Ack()
}

func (o Options) reject(ctx context.Context, msg *nats.Msg, metadata *nats.MsgMetadata, receivedAt time.Time, ack acker, cause error) error {
	if o.Store != nil {
		if storeErr := o.Store.RecordRejection(ctx, NewRejection(msg, metadata, cause, receivedAt)); storeErr != nil {
			_ = ack.Nak()
			return fmt.Errorf("record rejected event: %w (cause: %v)", storeErr, cause)
		}
	}
	_ = ack.Term()
	return cause
}

// redeliveryDelay backs off 1s, 2s, 4s … capped at 5 minutes.
func redeliveryDelay(delivered uint64) time.Duration {
	if delivered < 1 {
		delivered = 1
	}
	if delivered > 10 {
		delivered = 10
	}
	delay := time.Second * time.Duration(1<<(delivered-1))
	if delay > 5*time.Minute {
		delay = 5 * time.Minute
	}
	return delay
}

// NewRejection captures what an operator needs to see why a message died:
// the CloudEvents headers, the payload (capped), and the reason.
func NewRejection(msg *nats.Msg, metadata *nats.MsgMetadata, cause error, receivedAt time.Time) Rejection {
	headers := map[string][]string{}
	rejection := Rejection{Reason: cause.Error(), ReceivedAt: receivedAt}
	if metadata != nil {
		rejection.StreamSequence, rejection.DeliveryCount = metadata.Sequence.Stream, metadata.NumDelivered
	}
	if msg != nil {
		for key, values := range msg.Header {
			normalized := strings.ToLower(strings.TrimSpace(key))
			if strings.HasPrefix(normalized, "ce-") || normalized == "content-type" {
				headers[key] = append([]string(nil), values...)
			}
		}
		rejection.EventID = strings.TrimSpace(msg.Header.Get(connect.HeaderID))
		rejection.NATSSubject = msg.Subject
		payload := msg.Data
		if len(payload) > maxRejectedPayloadBytes {
			payload = payload[:maxRejectedPayloadBytes]
		}
		rejection.Payload = append([]byte(nil), payload...)
	}
	headerJSON, _ := json.Marshal(headers)
	rejection.Headers = connect.JSON(headerJSON)
	return rejection
}

// Models lists what an app passes to AutoMigrate.
func Models() []any { return []any{&Rejection{}} }
