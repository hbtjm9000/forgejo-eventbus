// forgejo-eventbus — NATS JetStream event bus for Forgejo issue events.
// Connects to an external standalone NATS server with JetStream.
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
)

var (
	version   = "0.2.0"
	listen    = flag.String("listen", ":9092", "HTTP listen address for webhook receiver")
	natsURL   = flag.String("nats-url", "nats://localhost:4222", "NATS server URL")
	testMode  = flag.Bool("test", false, "Run in test mode: publish sample events instead of webhook server")
)

func main() {
	flag.Parse()

	if *testMode {
		runTestMode()
		return
	}

	log.Printf("forgejo-eventbus v%s starting...", version)
	log.Printf("NATS URL: %s", *natsURL)

	// 1. Connect to external NATS + JetStream
	eventBus, err := bus.New(bus.Options{
		URL: *natsURL,
	})
	if err != nil {
		log.Fatalf("Failed to connect to event bus: %v", err)
	}
	defer eventBus.Shutdown()

	// 2. Register subscribers (JetStream consumers)
	fb := handlers.NewFocalBoard()
	rn := handlers.NewRikiNotify()
	fj := handlers.NewForgejoAPI()
	cr := handlers.NewCrumbs(
		envDefault("CRUMBS_URL", "http://127.0.0.1:8090"),
		os.Getenv("CRUMBS_TOKEN"),
	)

	// FocalBoard handles all issue events — durable consumer survives restarts
	fbCancel, err := eventBus.SubscribeIssueEvent("focalboard-worker",
		func(data []byte) error {
			var e events.IssueEvent
			if err := json.Unmarshal(data, &e); err != nil {
				return err
			}
			return fb.Handle(e)
		})
	if err != nil {
		log.Fatalf("Failed to create FocalBoard consumer: %v", err)
	}
	defer fbCancel()

	// RikiNotify handles all issue events
	rnCancel, err := eventBus.SubscribeIssueEvent("riki-notify-worker",
		func(data []byte) error {
			var e events.IssueEvent
			if err := json.Unmarshal(data, &e); err != nil {
				return err
			}
			return rn.Handle(e)
		})
	if err != nil {
		log.Fatalf("Failed to create RikiNotify consumer: %v", err)
	}
	defer rnCancel()

	// Crumbs handles issue events from crumbs-worker subscription
	crCancel, err := eventBus.SubscribeIssueEvent("crumbs-worker",
		func(data []byte) error {
			var e events.IssueEvent
			if err := json.Unmarshal(data, &e); err != nil {
				return err
			}
			return cr.Handle(e)
		})
	if err != nil {
		log.Fatalf("Failed to create Crumbs consumer: %v", err)
	}
	defer crCancel()

	// ForgejoAPI handles kanban card events (reverse sync)
	fjCancel, err := eventBus.SubscribeKanbanEvent("forgejo-reverse-worker",
		func(data []byte) error {
			var e events.KanbanCardEvent
			if err := json.Unmarshal(data, &e); err != nil {
				return err
			}
			return fj.Handle(e)
		})
	if err != nil {
		log.Fatalf("Failed to create Forgejo reverse consumer: %v", err)
	}
	defer fjCancel()

	// 3. Start webhook HTTP server
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/webhook", makeWebhookHandler(eventBus))
	http.HandleFunc("/webhook/kanban", makeKanbanWebhookHandler(eventBus))

	log.Printf("Webhook receiver listening on %s", *listen)

	// Graceful shutdown on SIGINT/SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := http.ListenAndServe(*listen, nil); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	<-sigCh
	log.Println("Shutting down...")
}

// makeWebhookHandler returns an http.HandlerFunc that parses Forgejo webhooks
// and publishes typed IssueEvent structs to the event bus via JetStream.
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

		// Publish via JetStream to forgejo-events stream
		if err := e.PublishIssueEvent(e2); err != nil {
			log.Printf("webhook: publish error: %v", err)
			http.Error(w, "500 Internal Server Error", http.StatusInternalServerError)
			return
		}

		fmt.Fprintln(w, "OK")
	}
}

// makeKanbanWebhookHandler returns an http.HandlerFunc that receives FocalBoard block-change
// webhooks at /webhook/kanban and publishes typed KanbanCardEvent structs to the kanban-events
// JetStream stream.
func makeKanbanWebhookHandler(e *bus.Bus) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "405 Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("webhook/kanban: read error: %v", err)
			http.Error(w, "400 Bad Request", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		// FocalBoard webhook sends the full Block object as JSON
		// https://github.com/mattermost/focalboard/blob/main/server/model/block.go
		var block struct {
			ID        string                 `json:"id"`
			BoardID   string                 `json:"boardId"`
			Type      string                 `json:"type"`
			Title     string                 `json:"title"`
			Fields    map[string]interface{} `json:"fields"`
			UpdateAt  int64                  `json:"updateAt"`
			DeleteAt  int64                  `json:"deleteAt"`
		}
		if err := json.Unmarshal(body, &block); err != nil {
			log.Printf("webhook/kanban: parse error: %v", err)
			http.Error(w, "400 Bad Request", http.StatusBadRequest)
			return
		}

		// Only process card-type blocks
		if block.Type != "card" {
			fmt.Fprintln(w, "OK") // Accept but ignore non-card blocks
			return
		}

		// Extract status from fields.properties (modern FocalBoard schema)
		statusID := ""
		if block.Fields != nil {
			if props, ok := block.Fields["properties"].(map[string]interface{}); ok {
				if s, ok := props["status"]; ok {
					statusID, _ = s.(string)
				}
			}
		}

		// Parse issue reference from card title
		owner, repo, issueNum, cleanTitle := handlers.ParseKanbanTitle(block.Title)

		// Determine action
		action := "updated"
		if block.DeleteAt > 0 {
			action = "deleted"
		} else if statusID != "" {
			action = "moved"
		}

		// Map status ID to human-readable name
		statusName := ""
		if mapped, ok := events.StatusIDToLabel[statusID]; ok {
			statusName = mapped
		}

		// Build and publish event
		event := events.KanbanCardEvent{
			EventID:    randomID(),
			Timestamp:  time.Now().UTC(),
			Origin:     "focalboard-webhook",
			Action:     action,
			CardID:     block.ID,
			BoardID:    block.BoardID,
			Owner:      owner,
			Repo:       repo,
			IssueNum:   issueNum,
			Title:      cleanTitle,
			Status:     statusID,
			StatusName: statusName,
		}

		if err := e.PublishKanbanEvent(event); err != nil {
			log.Printf("webhook/kanban: publish error: %v", err)
			http.Error(w, "500 Internal Server Error", http.StatusInternalServerError)
			return
		}

		log.Printf("[webhook/kanban] %s: %s/%s#%d → %s", action, owner, repo, issueNum, statusName)
		fmt.Fprintln(w, "OK")
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "OK")
}

// runTestMode publishes sample Forgejo events to the bus for verification
// without starting the webhook HTTP server.
func runTestMode() {
	log.Println("Running in TEST MODE — publishing sample events to JetStream")

	eventBus, err := bus.New(bus.Options{URL: *natsURL})
	if err != nil {
		log.Fatalf("Failed to connect to event bus: %v", err)
	}
	defer eventBus.Shutdown()

	// Subscribe to watch events flow through
	cancel, err := eventBus.SubscribeIssueEvent("test-worker",
		func(data []byte) error {
			var e events.IssueEvent
			json.Unmarshal(data, &e)
			log.Printf("[test] received: action=%s repo=%s issue=%s#%d",
				e.Action, e.Repository.FullName, e.Repository.FullName, e.Issue.Number)
			return nil
		})
	if err != nil {
		log.Printf("Warning: test consumer: %v", err)
	} else {
		defer cancel()
	}

	// Publish sample events
	samplesJSON := []string{
		`{"event_id":"","action":"opened","repository":{"full_name":"hal/forgejo-migration"},"issue":{"number":42,"title":"Test issue from event bus","body":"Testing the NATS JetStream event bus","state":"open","url":"http://localhost:3020/hal/forgejo-migration/issues/42"},"sender":{"login":"hal"},"label":{},"changes":{}}`,
		`{"event_id":"","action":"labeled","repository":{"full_name":"hal/forgejo-migration"},"issue":{"number":42,"title":"Test issue from event bus","state":"open","url":"http://localhost:3020/hal/forgejo-migration/issues/42"},"sender":{"login":"hal"},"label":{"name":"in-progress"},"changes":{}}`,
		`{"event_id":"","action":"closed","repository":{"full_name":"hal/forgejo-migration"},"issue":{"number":42,"title":"Test issue from event bus","state":"closed","url":"http://localhost:3020/hal/forgejo-migration/issues/42"},"sender":{"login":"hal"},"label":{},"changes":{}}`,
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

// envDefault returns the value of the environment variable or a default if empty.
func envDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
