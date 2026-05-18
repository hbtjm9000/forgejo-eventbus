// Package handlers contains event handlers for the forgejo-eventbus pipeline.
package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/hbtjm9000/forgejo-eventbus/internal/events"
)

// ForgejoAPI syncs kanban card events back to Forgejo via the REST API.
// This is the reverse sync direction: FocalBoard/Crumbs card changes → Forgejo issue updates.
type ForgejoAPI struct {
	baseURL   string // e.g., "http://localhost:3020"
	token     string // Forgejo access token
	client    *http.Client
}

// NewForgejoAPI creates a new Forgejo API client from environment variables.
func NewForgejoAPI() *ForgejoAPI {
	token := os.Getenv("FORGEJO_EVENTBUS_TOKEN")
	if token == "" {
		log.Println("[forgejo] WARNING: FORGEJO_EVENTBUS_TOKEN not set — reverse sync will fail")
	}
	return &ForgejoAPI{
		baseURL: "http://localhost:3020",
		token:   token,
		client:  &http.Client{},
	}
}

// Handle processes a KanbanCardEvent from the kanban-events stream.
// For "moved" events, it updates the corresponding Forgejo issue label.
func (f *ForgejoAPI) Handle(e events.KanbanCardEvent) error {
	// Skip events that originated from Forgejo (loop prevention)
	if e.Origin == "forgejo-webhook" {
		return nil
	}

	switch e.Action {
	case "moved":
		return f.syncStatus(e)
	case "created":
		return f.syncCreate(e)
	case "deleted":
		return f.syncClose(e)
	}
	return nil
}

// syncStatus maps a FocalBoard status change to a Forgejo label update.
func (f *ForgejoAPI) syncStatus(e events.KanbanCardEvent) error {
	if e.Owner == "" || e.Repo == "" || e.IssueNum == 0 {
		log.Printf("[forgejo] syncStatus: no valid issue ref in card — owner=%q repo=%q issue=%d",
			e.Owner, e.Repo, e.IssueNum)
		return nil // Not an issue-linked card, skip silently
	}

	// Map status ID to Forgejo label name
	label := events.StatusIDToLabel[e.Status]
	if label == "" {
		log.Printf("[forgejo] syncStatus: unknown status %q for issue %s/%s#%d",
			e.Status, e.Owner, e.Repo, e.IssueNum)
		return fmt.Errorf("unknown status: %s", e.Status)
	}

	// PATCH the issue labels: set the new label, clear others
	url := fmt.Sprintf("%s/api/v1/repos/%s/%s/issues/%d/labels",
		f.baseURL, e.Owner, e.Repo, e.IssueNum)

	payload := map[string]interface{}{
		"labels": []string{label},
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequest("PUT", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "token "+f.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := f.client.Do(req)
	if err != nil {
		return fmt.Errorf("PUT %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("PUT %s: HTTP %d", url, resp.StatusCode)
	}

	log.Printf("[forgejo] syncStatus: %s/%s#%d → label=%q (status=%s)",
		e.Owner, e.Repo, e.IssueNum, label, e.Status)
	return nil
}

// syncCreate creates a new Forgejo issue from a kanban card.
func (f *ForgejoAPI) syncCreate(e events.KanbanCardEvent) error {
	if e.Owner == "" || e.Repo == "" {
		return nil // Need repo context to create an issue
	}

	url := fmt.Sprintf("%s/api/v1/repos/%s/%s/issues",
		f.baseURL, e.Owner, e.Repo)

	payload := map[string]interface{}{
		"title": e.Title,
		"body":  "Created from kanban card",
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "token "+f.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := f.client.Do(req)
	if err != nil {
		return fmt.Errorf("POST %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("POST %s: HTTP %d", url, resp.StatusCode)
	}

	log.Printf("[forgejo] syncCreate: created issue in %s/%s — %q", e.Owner, e.Repo, e.Title)
	return nil
}

// syncClose closes a Forgejo issue when its kanban card is deleted.
func (f *ForgejoAPI) syncClose(e events.KanbanCardEvent) error {
	if e.Owner == "" || e.Repo == "" || e.IssueNum == 0 {
		return nil
	}

	url := fmt.Sprintf("%s/api/v1/repos/%s/%s/issues/%d",
		f.baseURL, e.Owner, e.Repo, e.IssueNum)

	payload := map[string]interface{}{
		"state": "closed",
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequest("PATCH", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "token "+f.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := f.client.Do(req)
	if err != nil {
		return fmt.Errorf("PATCH %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("PATCH %s: HTTP %d", url, resp.StatusCode)
	}

	log.Printf("[forgejo] syncClose: %s/%s#%d closed", e.Owner, e.Repo, e.IssueNum)
	return nil
}

// ParseKanbanTitle extracts repo owner, name, and issue number from a kanban card title.
// Expected format: "📋 owner/repo#123: Issue Title"
// Also handles: "📋 owner/repo#123" and "📋 repo#123: Title"
func ParseKanbanTitle(title string) (owner, repo string, issueNum int, cleanTitle string) {
	// Strip leading icon/emoji (e.g., "📋 ", "🔧 ")
	title = strings.TrimSpace(title)
	if idx := strings.Index(title, " "); idx > 0 && idx < 5 {
		title = strings.TrimSpace(title[idx+1:])
	}

	// Find the issue reference pattern (owner/repo#N or repo#N)
	end := strings.Index(title, ": ")
	if end < 0 {
		end = len(title)
	}

	ref := title[:end]
	cleanTitle = strings.TrimSpace(title[end+2:])

	// Parse repo#N format
	if parts := strings.SplitN(ref, "#", 2); len(parts) == 2 {
		fmt.Sscanf(parts[1], "%d", &issueNum)
		repoPart := parts[0]

		// Check for owner/repo format
		if slashParts := strings.SplitN(repoPart, "/", 2); len(slashParts) == 2 {
			owner = slashParts[0]
			repo = slashParts[1]
		} else {
			repo = repoPart
		}
	}

	return
}
