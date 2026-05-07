// forgejo-eventbus — Embedded NATS event bus for Forgejo issue events.
// Run with:
//   go run .                    — start server + webhook receiver
//   go run . --test             — publish test events instead of starting webhook server
//   go run . --version          — print version
package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hbtjm9000/forgejo-eventbus/internal/bus"
	"github.com/hbtjm9000/forgejo-eventbus/internal/events"
	"github.com/hbtjm9000/forgejo-eventbus/internal/handlers"
	"github.com/nats-io/nats.go"
)

var (
	version    = "0.1.0"
	listenAddr = flag.String("listen", ":9092", "HTTP listen address for webhook receiver")
	natsPort   = flag.Int("nats-port", 4222, "Embedded NATS server port")
	testMode   = flag.Bool("test", false, "Run in test mode: publish sample events instead of webhook server")
)

func main() {
	flag.Parse()

	if *testMode {
		runTestMode()
		return
	}

	log.Printf("forgejo-eventbus v%s starting...", version)

	// 1. Start embedded NATS bus
	eventBus, err := bus.New(bus.Options{
		Port:  *natsPort,
		Debug: false,
	})
	if err != nil {
		log.Fatalf("Failed to start event bus: %v", err)
	}
	defer eventBus.Shutdown()

	// 2. Register subscribers (consumers)
	fb := handlers.NewFocalBoard()
	rn := handlers.NewRikiNotify()

	// FocalBoard handles all issue events
	if _, err := eventBus.SubscribeIssueEvent(events.SubjectAnyIssue,
		func(data []byte) error {
			var e events.IssueEvent
			if err := json.Unmarshal(data, &e); err != nil {
				return err
			}
			return fb.Handle(e)
		}, true); err != nil {
		log.Printf("Warning: FocalBoard subscriber failed: %v", err)
	}

	// RikiNotify handles all issue events
	if _, err := eventBus.SubscribeIssueEvent(events.SubjectAnyIssue,
		func(data []byte) error {
			var e events.IssueEvent
			if err := json.Unmarshal(data, &e); err != nil {
				return err
			}
			return rn.Handle(e)
		}, true); err != nil {
		log.Printf("Warning: RikiNotify subscriber failed: %v", err)
	}

	// 3. Start webhook HTTP server
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/webhook", makeWebhookHandler(eventBus))

	log.Printf("Webhook receiver listening on %s", *listenAddr)
	log.Printf("NATS bus on nats://localhost:%d", *natsPort)

	// Graceful shutdown on SIGINT/SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := http.ListenAndServe(*listenAddr, nil); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	<-sigCh
	log.Println("Shutting down...")
}

// makeWebhookHandler returns an http.HandlerFunc that parses Forgejo webhooks
// and publishes typed IssueEvent structs to the event bus.
func makeWebhookHandler(e *bus.Bus) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "405 Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("webhook: read error: %v", err)
			http.Error(w, "400 Bad Request", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		// Parse into typed event
		var e2 events.IssueEvent
		if err := json.Unmarshal(body, &e2); err != nil {
			log.Printf("webhook: parse error: %v", err)
			http.Error(w, "400 Bad Request", http.StatusBadRequest)
			return
		}

		// Assign event ID if missing
		if e2.EventID == "" {
			e2.EventID = randomID()
		}
		if e2.Timestamp.IsZero() {
			e2.Timestamp = time.Now().UTC()
		}

		// Publish to bus (routes to correct subject based on action)
		if err := e.PublishIssueEvent(e2); err != nil {
			log.Printf("webhook: publish error: %v", err)
			http.Error(w, "500 Internal Server Error", http.StatusInternalServerError)
			return
		}

		fmt.Fprintln(w, "OK")
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "OK")
}

// runTestMode publishes sample Forgejo events to the bus for verification
// without starting the webhook HTTP server.
func runTestMode() {
	log.Println("Running in TEST MODE — publishing sample events")

	eventBus, err := bus.New(bus.Options{Port: *natsPort, Debug: false})
	if err != nil {
		log.Fatalf("Failed to start event bus: %v", err)
	}
	defer eventBus.Shutdown()

	// Subscribe to see what flows through
	sub, _ := eventBus.Client().Subscribe("forgejo.issue.*", func(msg *nats.Msg) {
		log.Printf("[test] received on %s: %s", msg.Subject, string(msg.Data))
	})
	defer sub.Unsubscribe()

	// Publish sample events via JSON (avoids struct literal tag mismatch with events.IssueEvent)
	samplesJSON := []string{
		`{"event_id":"","action":"opened","repository":{"full_name":"hal/forgejo-migration"},"issue":{"number":42,"title":"Test issue from event bus","body":"Testing the embedded NATS event bus","state":"open","url":"http://localhost:3000/hal/forgejo-migration/issues/42"},"sender":{"login":"hal"},"label":{},"changes":{}}`,
		`{"event_id":"","action":"labeled","repository":{"full_name":"hal/forgejo-migration"},"issue":{"number":42,"title":"Test issue from event bus","state":"open","url":"http://localhost:3000/hal/forgejo-migration/issues/42"},"sender":{"login":"hal"},"label":{"name":"in-progress"},"changes":{}}`,
		`{"event_id":"","action":"closed","repository":{"full_name":"hal/forgejo-migration"},"issue":{"number":42,"title":"Test issue from event bus","state":"closed","url":"http://localhost:3000/hal/forgejo-migration/issues/42"},"sender":{"login":"hal"},"label":{},"changes":{}}`,
	}

	for _, sj := range samplesJSON {
		var e events.IssueEvent
		json.Unmarshal([]byte(sj), &e)
		e.EventID = randomID()
		e.Timestamp = time.Now().UTC()
		if err := eventBus.PublishIssueEvent(e); err != nil {
			log.Printf("test: publish error: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	log.Println("Test events published. Waiting 2s for subscribers...")
	time.Sleep(2 * time.Second)
	log.Println("Test complete.")
}

// randomID generates a random hex string ID (16 bytes = 32 hex chars).
func randomID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}
