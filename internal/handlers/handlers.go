// Package handlers contains the event subscribers (consumers) for the Forgejo event bus.
// Each subscriber implements a specific side-effect: writing to FocalBoard, notifying Riki, etc.
// Adding a new consumer = add a new file here + register in main.go.
package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/hbtjm9000/forgejo-eventbus/internal/events"
)

// ------------------------------------------------------------------
// FocalBoard
// ------------------------------------------------------------------

// FocalBoard publishes Forgejo events as cards in a FocalBoard board.
type FocalBoard struct {
	URL    string // e.g. "http://localhost:9090"
	Token  string // session token
	Board  string // board ID
	client *http.Client
}

func NewFocalBoard() *FocalBoard {
	return &FocalBoard{
		URL:    getEnv("FOCALBOARD_URL", "http://localhost:9090"),
	Token:  getEnv("FOCALBOARD_TOKEN", "kdxomtbqfu78dbfp6ieux3andwo"),
	Board:  getEnv("FOCALBOARD_BOARD", "bkdkyfr45x7bg3x41o5g3eimgtr"),
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

// Label → FocalBoard Status option ID mapping.
var fbStatusMap = map[string]string{
	"in progress": "status-inprogress",
	"in-progress":  "status-inprogress",
	"wip":          "status-inprogress",
	"review":       "status-review",
	"needs review": "status-review",
	"in review":    "status-review",
	"done":         "status-done",
	"closed":       "status-done",
	"complete":     "status-done",
	"backlog":      "status-backlog",
}

func (fb *FocalBoard) Handle(e events.IssueEvent) error {
	switch e.Action {
	case "opened", "created", "reopened":
		return fb.createCard(e)
	case "closed":
		if err := fb.createCard(e); err != nil {
			return err
		}
		return fb.updateStatus(e.Repository.FullName, e.Issue.Number, "status-done")
	case "labeled", "unlabeled":
		label := strings.ToLower(strings.TrimSpace(e.Label.Name))
		if statusID, ok := fbStatusMap[label]; ok {
			return fb.updateStatus(e.Repository.FullName, e.Issue.Number, statusID)
		}
		log.Printf("[focalboard] label %q not in status map — ignored", label)
		return nil
	default:
		log.Printf("[focalboard] action %q not handled", e.Action)
		return nil
	}
}

func (fb *FocalBoard) createCard(e events.IssueEvent) error {
	var icon string
	switch e.Issue.State {
	case "open":
		icon = "📋"
	case "closed":
		icon = "✅"
	default:
		icon = "📌"
	}

	repo := e.Repository.FullName
	if repo == "" {
		repo = "unknown/repo"
	}

	title := fmt.Sprintf("%s %s#%d: %s", icon, repo, e.Issue.Number, e.Issue.Title)
	if len(title) > 255 {
		title = title[:252] + "..."
	}

	body := e.Issue.Body
	if body == "" {
		body = "_No description provided_"
	}
	body += fmt.Sprintf("\n\n---\n**Source:** [%s#%d](%s) • @%s",
		repo, e.Issue.Number, e.Issue.URL, e.Sender.Login)

	payload := map[string]any{"title": title}
	payloadBytes, _ := json.Marshal(payload)

	url := fmt.Sprintf("%s/api/v2/boards/%s/cards", fb.URL, fb.Board)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payloadBytes))
	if err != nil {
		return fmt.Errorf("create card request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+fb.Token)
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("Content-Type", "application/json")

	resp, err := fb.client.Do(req)
	if err != nil {
		return fmt.Errorf("create card: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		log.Printf("[focalboard] create card %d: %s", resp.StatusCode, string(bodyBytes))
		return nil // non-fatal
	}

	var card struct {
		ID string `json:"id"`
	}
	json.Unmarshal(bodyBytes, &card)
	log.Printf("[focalboard] Created card: id=%s title=%q", card.ID, title)
	return nil
}

func (fb *FocalBoard) updateStatus(repo string, issueNum int, statusID string) error {
	// Search board for existing card
	searchURL := fmt.Sprintf("%s/api/v2/boards/%s/blocks?type=card", fb.URL, fb.Board)
	req, _ := http.NewRequest(http.MethodGet, searchURL, nil)
	req.Header.Set("Authorization", "Bearer "+fb.Token)
	req.Header.Set("X-Requested-With", "XMLHttpRequest")

	resp, err := fb.client.Do(req)
	if err != nil {
		return fmt.Errorf("search blocks: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("search blocks %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var blocks []map[string]any
	json.Unmarshal(bodyBytes, &blocks)

	// Search pattern: repo#num: prefix (emoji-stripped)
	search := fmt.Sprintf("%s#%d:", repo, issueNum)
	var cardID string
	for _, b := range blocks {
		if b["type"] != "card" {
			continue
		}
		// Title is at root level; strip emoji chars for reliable matching
		titleRaw, _ := b["title"].(string)
		title := strings.TrimLeft(titleRaw, "📋✅📌🔴🟠🟡🟢🔵🟣⚠️🔥✨💡🎯🚀 ") // strip common emoji
		if strings.Contains(title, search) {
			cardID, _ = b["id"].(string)
			break
		}
		// Fallback: check fields.contentOrder[0] (old format)
		if cardID == "" {
			fields, _ := b["fields"].(map[string]any)
			if contentOrder, ok := fields["contentOrder"].([]any); ok && len(contentOrder) > 0 {
				if t, ok := contentOrder[0].(string); ok && strings.Contains(t, search) {
					cardID, _ = b["id"].(string)
					break
				}
			}
		}
	}

	if cardID == "" {
		log.Printf("[focalboard] card not found for %s#%d — skipping status update", repo, issueNum)
		return nil
	}

	// Update status via PATCH
	patchURL := fmt.Sprintf("%s/api/v2/boards/%s/blocks/%s", fb.URL, fb.Board, cardID)
	patchBody := map[string]any{
		"fields": map[string]any{
			"status": statusID,
		},
	}
	patchBytes, _ := json.Marshal(patchBody)
	patchReq, _ := http.NewRequest(http.MethodPatch, patchURL, bytes.NewReader(patchBytes))
	patchReq.Header.Set("Authorization", "Bearer "+fb.Token)
	patchReq.Header.Set("X-Requested-With", "XMLHttpRequest")
	patchReq.Header.Set("Content-Type", "application/json")

	patchResp, err := fb.client.Do(patchReq)
	if err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	defer patchResp.Body.Close()

	respBody, _ := io.ReadAll(patchResp.Body)
	if patchResp.StatusCode >= 400 {
		log.Printf("[focalboard] update status %d: %s", patchResp.StatusCode, string(respBody))
	} else {
		log.Printf("[focalboard] Updated card %s → Status=%s", cardID, statusID)
	}
	return nil
}

// ------------------------------------------------------------------
// Riki Notification
// ------------------------------------------------------------------

// RikiNotify sends Forgejo events to Riki (Hermes) via webhook or writes to a queue file.
// In production this would POST to a Riki webhook endpoint or publish to a NATS subject
// that Hermes subscribes to.
type RikiNotify struct {
	WebhookURL string // Riki's inbound webhook URL (e.g. Hermes Telegram DM endpoint)
	client    *http.Client
	queuePath string // fallback: write events to a queue file if webhook fails
}

func NewRikiNotify() *RikiNotify {
	return &RikiNotify{
		WebhookURL: getEnv("RIKI_WEBHOOK_URL", ""),
		client:     &http.Client{Timeout: 5 * time.Second},
		queuePath:  getEnv("RIKI_QUEUE_PATH", "/home/hbtjm/Riki/event_queue"),
	}
}

func (rn *RikiNotify) Handle(e events.IssueEvent) error {
	// Format a compact notification message
	msg := rn.formatMessage(e)
	log.Printf("[riki] %s", msg)

	if rn.WebhookURL != "" {
		payload := map[string]string{"text": msg}
		payloadBytes, _ := json.Marshal(payload)
		req, _ := http.NewRequest(http.MethodPost, rn.WebhookURL, bytes.NewReader(payloadBytes))
		req.Header.Set("Content-Type", "application/json")
		resp, err := rn.client.Do(req)
		if err != nil {
			log.Printf("[riki] webhook failed: %v — queuing locally", err)
			return rn.queueEvent(e)
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 400 {
			log.Printf("[riki] webhook returned %d — queuing locally", resp.StatusCode)
			return rn.queueEvent(e)
		}
	} else {
		return rn.queueEvent(e)
	}
	return nil
}

func (rn *RikiNotify) formatMessage(e events.IssueEvent) string {
	repo := e.Repository.FullName
	if repo == "" {
		repo = "?"
	}
	switch e.Action {
	case "opened", "created", "reopened":
		return fmt.Sprintf("[%s#%d] opened by @%s — %s", repo, e.Issue.Number, e.Sender.Login, e.Issue.Title)
	case "closed":
		return fmt.Sprintf("[%s#%d] closed by @%s — %s", repo, e.Issue.Number, e.Sender.Login, e.Issue.Title)
	case "labeled":
		return fmt.Sprintf("[%s#%d] labeled '%s' by @%s — %s", repo, e.Issue.Number, e.Label.Name, e.Sender.Login, e.Issue.Title)
	case "unlabeled":
		return fmt.Sprintf("[%s#%d] label '%s' removed by @%s — %s", repo, e.Issue.Number, e.Label.Name, e.Sender.Login, e.Issue.Title)
	default:
		return fmt.Sprintf("[%s#%d] %s by @%s — %s", repo, e.Issue.Number, e.Action, e.Sender.Login, e.Issue.Title)
	}
}

func (rn *RikiNotify) queueEvent(e events.IssueEvent) error {
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	// Append as one JSON line to queue file
	f, err := os.OpenFile(rn.queuePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open queue file: %w", err)
	}
	defer f.Close()
	_, err = f.Write(append(data, '\n'))
	return err
}

// ------------------------------------------------------------------
// Utilities
// ------------------------------------------------------------------

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
