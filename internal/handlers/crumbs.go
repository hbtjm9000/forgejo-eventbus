// Package handlers — Crumbs kanban sync handler.
// Subscribes to Forgejo IssueEvents and mirrors them to the Crumbs kanban API.
package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/hbtjm9000/forgejo-eventbus/internal/events"
)

// crumbsColumnMap maps Forgejo issue labels to Crumbs kanban column names.
// Used when a label change should move the card to a specific column.
var crumbsColumnMap = map[string]string{
	"in-progress":  "In Progress",
	"in progress":  "In Progress",
	"wip":          "In Progress",
	"review":       "Review",
	"needs review": "Review",
	"in review":    "Review",
	"done":         "Done",
	"closed":       "Done",
	"complete":     "Done",
	"backlog":      "Backlog",
	"blocked":      "Blocked",
	"cancelled":    "Cancelled",
	"todo":         "To Do",
}

// Crumbs syncs Forgejo issue events to a Crumbs kanban board via the REST API.
// It creates new cards for new issues, moves cards to columns on label/state changes,
// and mirrors the lifecycle of a Forgejo issue on the Crumbs board.
type Crumbs struct {
	URL     string        // Crumbs API base URL (e.g. "http://127.0.0.1:8090")
	Token   string        // Bearer auth token for Crumbs API
	BoardID string        // Crumbs board ID to create cards in
	client  *http.Client
}

// NewCrumbs creates a new Crumbs handler.
// crumbsURL: Crumbs API base URL (default: http://127.0.0.1:8090).
// crumbsToken: Bearer token for authenticated requests.
// Board ID is read from the CRUMBS_BOARD_ID environment variable.
func NewCrumbs(crumbsURL, crumbsToken string) *Crumbs {
	boardID := getEnv("CRUMBS_BOARD_ID", "")
	if boardID == "" {
		log.Println("[crumbs] WARNING: CRUMBS_BOARD_ID not set — card creation will be skipped")
	}
	return &Crumbs{
		URL:     crumbsURL,
		Token:   crumbsToken,
		BoardID: boardID,
		client:  &http.Client{Timeout: 10 * time.Second},
	}
}

// Handle processes an IssueEvent and syncs it to the Crumbs kanban board.
func (c *Crumbs) Handle(e events.IssueEvent) error {
	switch e.Action {
	case "opened", "created":
		return c.createCard(e)
	case "closed":
		return c.moveCard(e, "Done")
	case "reopened":
		return c.moveCard(e, "In Progress")
	case "labeled", "unlabeled", "updated":
		return c.syncLabel(e)
	default:
		log.Printf("[crumbs] action %q not handled", e.Action)
		return nil
	}
}

// ---------------------------------------------------------------------------
// Card creation
// ---------------------------------------------------------------------------

// createCard creates a new card in Crumbs from a Forgejo issue event.
// The card title follows the same convention as the FocalBoard handler
// so that both systems can find cards by the same search pattern.
func (c *Crumbs) createCard(e events.IssueEvent) error {
	if c.BoardID == "" {
		log.Printf("[crumbs] CRUMBS_BOARD_ID not set — skipping card creation")
		return nil
	}

	repo := e.Repository.FullName
	if repo == "" {
		repo = "unknown/repo"
	}

	icon := "📋"
	if e.Issue.State == "closed" {
		icon = "✅"
	}
	title := fmt.Sprintf("%s %s#%d: %s", icon, repo, e.Issue.Number, e.Issue.Title)
	if len(title) > 255 {
		title = title[:252] + "..."
	}

	// Determine initial column from labels or state
	column := c.resolveColumn(e)
	if column == "" && e.Issue.State == "open" {
		column = "To Do"
	}

	payload := map[string]interface{}{
		"title":  title,
		"type":   "card",
		"column": column,
	}
	if column != "" {
		payload["column"] = column
	}
	payloadBytes, _ := json.Marshal(payload)

	url := fmt.Sprintf("%s/api/v1/boards/%s/blocks", c.URL, c.BoardID)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payloadBytes))
	if err != nil {
		return fmt.Errorf("crumbs create card request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("crumbs create card: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		log.Printf("[crumbs] create card %d: %s", resp.StatusCode, string(bodyBytes))
		return nil // non-fatal — don't block the pipeline
	}

	var card struct {
		ID string `json:"id"`
	}
	json.Unmarshal(bodyBytes, &card)
	log.Printf("[crumbs] Created card: id=%s title=%q column=%q", card.ID, title, column)
	return nil
}

// ---------------------------------------------------------------------------
// Card movement
// ---------------------------------------------------------------------------

// moveCard moves an existing card to the given column. If the card does not
// exist yet, it creates the card first, then moves it.
func (c *Crumbs) moveCard(e events.IssueEvent, column string) error {
	if c.BoardID == "" {
		log.Printf("[crumbs] CRUMBS_BOARD_ID not set — skipping move")
		return nil
	}

	repo := e.Repository.FullName
	if repo == "" {
		repo = "unknown/repo"
	}

	cardID, found, err := c.findCardByIssue(repo, e.Issue.Number)
	if err != nil {
		return fmt.Errorf("crumbs find card: %w", err)
	}
	if !found {
		// Card doesn't exist yet — create it
		if err := c.createCard(e); err != nil {
			return err
		}
		// Re-find after creation
		cardID, found, err = c.findCardByIssue(repo, e.Issue.Number)
		if err != nil {
			return fmt.Errorf("crumbs re-find card after create: %w", err)
		}
		if !found {
			log.Printf("[crumbs] Created card not found for %s#%d — skipping move", repo, e.Issue.Number)
			return nil
		}
	}

	// PATCH the card column
	payload := map[string]interface{}{
		"column": column,
	}
	payloadBytes, _ := json.Marshal(payload)

	url := fmt.Sprintf("%s/api/v1/blocks/%s", c.URL, cardID)
	req, err := http.NewRequest(http.MethodPatch, url, bytes.NewReader(payloadBytes))
	if err != nil {
		return fmt.Errorf("crumbs update card request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("crumbs update card: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		log.Printf("[crumbs] update card %d: %s", resp.StatusCode, string(bodyBytes))
		return nil // non-fatal
	}

	log.Printf("[crumbs] Moved card %s for %s#%d → column=%q", cardID, repo, e.Issue.Number, column)
	return nil
}

// ---------------------------------------------------------------------------
// Label-based column sync
// ---------------------------------------------------------------------------

// syncLabel looks up the label in the column map and moves the card
// to the corresponding column. Creates the card first if it doesn't exist.
func (c *Crumbs) syncLabel(e events.IssueEvent) error {
	label := strings.ToLower(strings.TrimSpace(e.Label.Name))
	if label == "" && len(e.Issue.Labels) > 0 {
		label = strings.ToLower(strings.TrimSpace(e.Issue.Labels[0].Name))
	}
	if label == "" {
		log.Printf("[crumbs] syncLabel: no label found in event")
		return nil
	}

	column, ok := crumbsColumnMap[label]
	if !ok {
		log.Printf("[crumbs] label %q not in column map — ignored", label)
		return nil
	}

	return c.moveCard(e, column)
}

// ---------------------------------------------------------------------------
// Card lookup
// ---------------------------------------------------------------------------

// findCardByIssue searches the Crumbs board for a card matching the given
// issue reference. Returns the card ID and whether it was found.
func (c *Crumbs) findCardByIssue(repo string, issueNum int) (string, bool, error) {
	search := fmt.Sprintf("%s#%d:", repo, issueNum)

	url := fmt.Sprintf("%s/api/v1/boards/%s/blocks", c.URL, c.BoardID)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return "", false, fmt.Errorf("list cards request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)

	resp, err := c.client.Do(req)
	if err != nil {
		return "", false, fmt.Errorf("list cards: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return "", false, fmt.Errorf("list cards %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var list struct {
		Blocks []struct {
			ID    string `json:"id"`
			Title string `json:"title"`
		} `json:"blocks"`
	}
	if err := json.Unmarshal(bodyBytes, &list); err != nil {
		return "", false, fmt.Errorf("list cards unmarshal: %w", err)
	}

	for _, block := range list.Blocks {
		if strings.Contains(block.Title, search) {
			return block.ID, true, nil
		}
	}

	log.Printf("[crumbs] No existing card found for %s#%d", repo, issueNum)
	return "", false, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// resolveColumn determines the initial column for a card based on the
// issue's labels, falling back to state-based defaults.
func (c *Crumbs) resolveColumn(e events.IssueEvent) string {
	if len(e.Issue.Labels) > 0 {
		label := strings.ToLower(strings.TrimSpace(e.Issue.Labels[0].Name))
		if col, ok := crumbsColumnMap[label]; ok {
			return col
		}
	}
	return ""
}
