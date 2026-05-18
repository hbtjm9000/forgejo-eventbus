// Package bus provides the NATS JetStream event bus for the Forgejo event system.
// Connects to an external standalone NATS server — no more embedded server.
// Uses JetStream for at-least-once delivery, replay, and durability.
package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/hbtjm9000/forgejo-eventbus/internal/events"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Bus wraps a NATS client connection and a JetStream context.
// Use PublishIssueEvent / SubscribeIssueEvent and PublishKanbanEvent / SubscribeKanbanEvent
// for typed event handling on their respective streams.
type Bus struct {
	nc *nats.Conn
	js jetstream.JetStream
}

// Options configures the connection to the external NATS server.
type Options struct {
	URL   string // NATS server URL (default nats://localhost:4222)
	Debug bool  // Enable client debug logging
}

// DefaultOptions returns sensible defaults.
func DefaultOptions() Options {
	return Options{
		URL:   "nats://localhost:4222",
		Debug: false,
	}
}

// New connects to an external NATS server and creates a JetStream context.
// The server must already be running — this no longer starts an embedded instance.
// Verifies the forgejo-events stream exists on connection.
func New(opts Options) (*Bus, error) {
	if opts.URL == "" {
		opts = DefaultOptions()
	}

	nc, err := nats.Connect(
		opts.URL,
		nats.Name("forgejo-eventbus"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*1000*1000*1000), // 2s between reconnects
	)
	if err != nil {
		return nil, fmt.Errorf("NATS connect to %s: %w", opts.URL, err)
	}
	log.Printf("[bus] Connected to NATS at %s", opts.URL)

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("create JetStream context: %w", err)
	}
	log.Printf("[bus] JetStream context created")

	// Verify the forgejo-events stream exists
	_, err = js.Stream(context.Background(), events.StreamForgejoEvents)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("stream %q not found — run bootstrap-streams.sh first: %w", events.StreamForgejoEvents, err)
	}
	log.Printf("[bus] Verified stream %q exists", events.StreamForgejoEvents)

	return &Bus{nc: nc, js: js}, nil
}

// Client returns the raw NATS connection for direct use.
func (b *Bus) Client() *nats.Conn {
	return b.nc
}

// JetStream returns the JetStream context for advanced operations.
func (b *Bus) JetStream() jetstream.JetStream {
	return b.js
}

// PublishIssueEvent marshals an event and publishes it to the JetStream forgejo-events stream
// on the subject derived from the event action.
func (b *Bus) PublishIssueEvent(event any) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	// Decode just enough to get the action for subject routing
	var raw struct {
		Action  string `json:"action"`
		EventID string `json:"event_id"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("unmarshal for routing: %w", err)
	}

	// Map action → subject
	var subject string
	switch raw.Action {
	case "opened", "created", "reopened":
		subject = "forgejo.issue.created"
	case "closed":
		subject = "forgejo.issue.closed"
	case "labeled", "unlabeled":
		subject = "forgejo.issue.labeled"
	default:
		subject = "forgejo.issue.updated"
	}

	// Publish via JetStream with Nats-Msg-Id for deduplication
	ack, err := b.js.PublishMsg(context.Background(), &nats.Msg{
		Subject: subject,
		Data:    data,
		Header: nats.Header{
			"Nats-Msg-Id": []string{raw.EventID},
		},
	})
	if err != nil {
		return fmt.Errorf("JetStream publish %s: %w", subject, err)
	}
	log.Printf("[bus] Published: action=%q subject=%s stream=%q seq=%d",
		raw.Action, subject, ack.Stream, ack.Sequence)
	return nil
}

// PublishKanbanEvent marshals an event and publishes it to the JetStream kanban-events stream
// on the subject derived from the event action. Events with origin "forgejo-webhook" are
// skipped (loop prevention — these are re-syncs from Forgejo issue changes).
func (b *Bus) PublishKanbanEvent(event any) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	// Decode just enough to get action + origin for routing
	var raw struct {
		Action  string `json:"action"`
		EventID string `json:"event_id"`
		Origin  string `json:"origin"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("unmarshal for routing: %w", err)
	}

	// Loop prevention: skip publishing if this event originated from a Forgejo webhook
	// (it will be handled by the forward sync — FocalBoard handler on forgejo-events)
	if raw.Origin == "forgejo-webhook" {
		log.Printf("[bus] Skipping kanban publish — origin=%q (loop prevention)", raw.Origin)
		return nil
	}

	// Map action → subject
	var subject string
	switch raw.Action {
	case "moved":
		subject = events.SubjectCardMoved
	case "created":
		subject = events.SubjectCardCreated
	case "updated":
		subject = events.SubjectCardUpdated
	case "deleted":
		subject = events.SubjectCardDeleted
	default:
		subject = events.SubjectCardUpdated
	}

	ack, err := b.js.PublishMsg(context.Background(), &nats.Msg{
		Subject: subject,
		Data:    data,
		Header: nats.Header{
			"Nats-Msg-Id": []string{raw.EventID},
		},
	})
	if err != nil {
		return fmt.Errorf("JetStream publish %s: %w", subject, err)
	}
	log.Printf("[bus] Published kanban: action=%q subject=%s stream=%q seq=%d",
		raw.Action, subject, ack.Stream, ack.Sequence)
	return nil
}

// subscribeToStream is a general-purpose JetStream consumer creator.
// Used internally by SubscribeIssueEvent and SubscribeKanbanEvent.
func (b *Bus) subscribeToStream(streamName, durableName, filterSubject string, handler func([]byte) error) (func(), error) {
	stream, err := b.js.Stream(context.Background(), streamName)
	if err != nil {
		return nil, fmt.Errorf("get stream %s: %w", streamName, err)
	}

	cons, err := stream.CreateOrUpdateConsumer(context.Background(), jetstream.ConsumerConfig{
		Name:          durableName,
		Durable:       durableName,
		Description:   fmt.Sprintf("forgejo-eventbus %s worker", durableName),
		FilterSubject: filterSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		MaxDeliver:    10,
	})
	if err != nil {
		return nil, fmt.Errorf("create consumer %s: %w", durableName, err)
	}

	msgCh, err := cons.Messages()
	if err != nil {
		return nil, fmt.Errorf("start consumer %s: %w", durableName, err)
	}

	stopCh := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopCh:
				msgCh.Stop()
				return
			default:
				msg, err := msgCh.Next()
				if err != nil {
					log.Printf("[bus] consumer %s read error: %v", durableName, err)
					return
				}
				if err := handler(msg.Data()); err != nil {
					log.Printf("[bus] consumer %s handler error: %v", durableName, err)
					msg.Nak()
				} else {
					msg.Ack()
					meta, _ := msg.Metadata()
					if meta != nil {
						log.Printf("[bus] consumer %s processed: subject=%s stream=%s seq=%d",
							durableName, msg.Subject(), meta.Stream, meta.Sequence.Stream)
					}
				}
			}
		}
	}()

	log.Printf("[bus] Consumer %s active on %s (filter: %s)", durableName, streamName, filterSubject)
	return func() { close(stopCh) }, nil
}

// SubscribeIssueEvent creates a durable consumer on forgejo-events stream.
func (b *Bus) SubscribeIssueEvent(durableName string, handler func([]byte) error) (func(), error) {
	return b.subscribeToStream(events.StreamForgejoEvents, durableName, "forgejo.issue.*", handler)
}

// SubscribeKanbanEvent creates a durable consumer on kanban-events stream.
func (b *Bus) SubscribeKanbanEvent(durableName string, handler func([]byte) error) (func(), error) {
	return b.subscribeToStream(events.StreamKanbanEvents, durableName, "kanban.card.*", handler)
}

// Shutdown gracefully closes the NATS connection.
func (b *Bus) Shutdown() error {
	b.nc.Close()
	log.Printf("[bus] Disconnected from NATS")
	return nil
}
