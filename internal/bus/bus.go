// Package bus provides the embedded NATS event bus for the Forgejo event system.
// The NATS server runs as a goroutine inside this process — no separate container or binary.
package bus

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// Bus wraps an embedded NATS server and a NATS client connection.
// All publishers and subscribers use the Client() method to interact with the bus.
type Bus struct {
	ns   *server.Server
	nc   *nats.Conn
	opts Options
}

// Options configures the embedded NATS server.
type Options struct {
	Port         int           // TCP port to listen on (default 4222)
	Debug        bool          // Enable NATS server debug logging
	MaxConns     int           // Max client connections (default 64)
	MaxPayload   int           // Max message payload bytes (default 8MB)
	JetStream    bool          // Enable JetStream persistence (not used in v1)
	ClusterPort  int           // For future cluster mode (0 = single server)
}

// DefaultOptions returns sensible defaults for single-instance embedded NATS.
func DefaultOptions() Options {
	return Options{
		Port:       4222,
		Debug:      false,
		MaxConns:   64,
		MaxPayload: 8 * 1024 * 1024, // 8MB
		JetStream:  false,
	}
}

// New creates and starts the embedded NATS server, then connects a client.
// The server runs in a goroutine managed by the Bus struct.
// Callers must invoke Bus.Shutdown() on exit.
func New(opts Options) (*Bus, error) {
	if opts.Port == 0 {
		opts = DefaultOptions()
	}

	nsOpts := &server.Options{
		Port:         opts.Port,
		MaxConn:      opts.MaxConns,
		MaxPayload:   int32(opts.MaxPayload),
		Debug:        opts.Debug,
		Trace:        opts.Debug,
		NoSigs:       true, // Don't listen for signals — we manage lifecycle explicitly
	}

	ns, err := server.NewServer(nsOpts)
	if err != nil {
		return nil, fmt.Errorf("create NATS server: %w", err)
	}

	// Start server in background goroutine
	go ns.Start()

	// Wait for server to be ready (up to 5 seconds)
	if !ns.ReadyForConnections(5 * time.Second) {
		return nil, fmt.Errorf("NATS server failed to start on port %d", opts.Port)
	}
	log.Printf("[bus] NATS server started on port %d", opts.Port)

	// Connect client to localhost — uses in-process pipe when localhost
	nc, err := nats.Connect(
		fmt.Sprintf("nats://localhost:%d", opts.Port),
		nats.Name("forgejo-eventbus-client"),
		nats.MaxReconnects(-1),
	)
	if err != nil {
		ns.Shutdown()
		return nil, fmt.Errorf("NATS client connect failed: %w", err)
	}
	log.Printf("[bus] Client connected to NATS")

	return &Bus{ns: ns, nc: nc, opts: opts}, nil
}

// Client returns the NATS connection for publishing and subscribing.
func (b *Bus) Client() *nats.Conn {
	return b.nc
}

// Server returns the underlying NATS server for inspection.
func (b *Bus) Server() *server.Server {
	return b.ns
}

// Publish serializes an event to JSON and publishes it to the given subject.
// Use the typed wrappers below (PublishIssueEvent) for type safety.
func (b *Bus) Publish(subject string, event any) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event for %s: %w", subject, err)
	}
	if err := b.nc.Publish(subject, data); err != nil {
		return fmt.Errorf("publish %s: %w", subject, err)
	}
	return nil
}

// PublishIssueEvent routes an IssueEvent to its correct subject and publishes it.
func (b *Bus) PublishIssueEvent(event any) error {
	// Convert to JSON for routing
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	// Decode just enough to get the action
	var raw struct {
		Action string `json:"action"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

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

	if err := b.nc.Publish(subject, data); err != nil {
		return fmt.Errorf("publish issue event: %w", err)
	}
	log.Printf("[bus] Published issue event: action=%q subject=%s", raw.Action, subject)
	return nil
}

// Subscribe subscribes a handler function to a subject.
// The handler runs on NATS internal callbacks — keep it fast or offload to a goroutine.
// Returns a nats.Subscription — call sub.Unsubscribe() to stop.
func (b *Bus) Subscribe(subject string, handler func([]byte) error) (*nats.Subscription, error) {
	sub, err := b.nc.Subscribe(subject, func(msg *nats.Msg) {
		if err := handler(msg.Data); err != nil {
			log.Printf("[bus] subscriber error on %s: %v", subject, err)
		}
	})
	if err != nil {
		return nil, fmt.Errorf("subscribe %s: %w", subject, err)
	}
	log.Printf("[bus] Subscribed to %s", subject)
	return sub, nil
}

// SubscribeIssueEvent subscribes a typed IssueEvent handler to a subject.
// offload=true runs the handler in a goroutine (non-blocking NATS callback).
func (b *Bus) SubscribeIssueEvent(subject string, handler func([]byte) error, offload bool) (*nats.Subscription, error) {
	sub, err := b.nc.Subscribe(subject, func(msg *nats.Msg) {
		if offload {
			go func() {
				if err := handler(msg.Data); err != nil {
					log.Printf("[bus] handler error on %s: %v", subject, err)
				}
			}()
		} else {
			if err := handler(msg.Data); err != nil {
				log.Printf("[bus] handler error on %s: %v", subject, err)
			}
		}
	})
	if err != nil {
		return nil, fmt.Errorf("subscribe %s: %w", subject, err)
	}
	log.Printf("[bus] Subscribed to %s", subject)
	return sub, nil
}

// Shutdown gracefully stops the NATS server and closes the client connection.
func (b *Bus) Shutdown() error {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.nc.Close()
		b.ns.Shutdown()
		b.ns.WaitForShutdown()
	}()
	wg.Wait()
	log.Printf("[bus] Shutdown complete")
	return nil
}
