// Package events defines typed event structs for the Forgejo event bus.
// All events published to NATS use these types — no raw webhook payloads.
package events

import "time"

// Subject constants — NATS subjects for pub/sub routing.
const (
	// Forgejo issue event subjects (forgejo-events stream)
	SubjectIssueCreated = "forgejo.issue.created"
	SubjectIssueClosed  = "forgejo.issue.closed"
	SubjectIssueUpdated = "forgejo.issue.updated"
	SubjectIssueLabeled = "forgejo.issue.labeled"
	SubjectAnyIssue     = "forgejo.issue.*" // wildcard for subscribers wanting all issue events

	// Kanban card event subjects (kanban-events stream)
	SubjectCardMoved   = "kanban.card.moved"
	SubjectCardCreated = "kanban.card.created"
	SubjectCardUpdated = "kanban.card.updated"
	SubjectCardDeleted = "kanban.card.deleted"
	SubjectAnyCard     = "kanban.card.*"
)

// Forgejo event stream name
const StreamForgejoEvents = "forgejo-events"

// Kanban event stream name
const StreamKanbanEvents = "kanban-events"

// Label is a simple struct for issue labels in the Forgejo webhook payload.
type Label struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

// IssueEvent is the canonical event emitted when a Forgejo issue changes.
type IssueEvent struct {
	EventID    string    `json:"event_id"`    // unique ID for deduplication
	Timestamp  time.Time `json:"timestamp"`   // when the event was created
	Action     string    `json:"action"`      // "opened" | "closed" | "reopened" | "labeled" | "unlabeled" | "updated"
	Repository struct {
		FullName string `json:"full_name"` // "owner/repo"
	} `json:"repository"`
	Issue struct {
		Number int      `json:"number"`
		Title  string   `json:"title"`
		Body   string   `json:"body"`
		State  string   `json:"state"`
		URL    string   `json:"url"`
		Labels []Label  `json:"labels"`
	} `json:"issue"`
	Sender struct {
		Login string `json:"login"` // username of actor
	} `json:"sender"`
	Label struct {
		Name string `json:"name"` // label name, populated on labeled/unlabeled
	} `json:"label"`
	Changes struct {
		Label struct {
			Old struct {
				Name string `json:"name"`
			} `json:"old"`
			New struct {
				Name string `json:"name"`
			} `json:"new"`
		} `json:"label"`
	} `json:"changes"`
}

// ToSubject maps an IssueEvent action to its NATS subject.
func (e *IssueEvent) ToSubject() string {
	switch e.Action {
	case "opened", "created", "reopened":
		return SubjectIssueCreated
	case "closed":
		return SubjectIssueClosed
	case "labeled", "unlabeled":
		return SubjectIssueLabeled
	default:
		return SubjectIssueUpdated
	}
}

// KanbanCardEvent is published when a FocalBoard/Crumbs card changes.
// Origin tracks where the event came from for loop prevention.
type KanbanCardEvent struct {
	EventID   string    `json:"event_id"`   // unique ID for deduplication
	Timestamp time.Time `json:"timestamp"`  // when the event was created
	Origin    string    `json:"origin"`     // "focalboard-webhook" | "forgejo-webhook"
	Action    string    `json:"action"`     // "moved" | "created" | "updated" | "deleted"
	CardID    string    `json:"card_id"`    // FocalBoard block ID
	BoardID   string    `json:"board_id"`   // FocalBoard board ID

	// Parsed issue reference from card title
	Owner     string `json:"owner"`      // repo owner (e.g., "hal")
	Repo      string `json:"repo"`       // repo name (e.g., "forgejo-migration")
	IssueNum  int    `json:"issue_num"`  // Forgejo issue number
	Title     string `json:"title"`      // card/issue title (without icon/repo prefix)

	// Kanban state
	Status     string `json:"status"`     // FocalBoard status ID (e.g., "status-inprogress")
	StatusName string `json:"status_name"` // Human-readable status (e.g., "In Progress")
	OldStatus  string `json:"old_status"` // Previous status ID (empty if created)
}

// ToSubject maps a KanbanCardEvent action to its NATS subject.
func (e *KanbanCardEvent) ToSubject() string {
	switch e.Action {
	case "moved":
		return SubjectCardMoved
	case "created":
		return SubjectCardCreated
	case "updated":
		return SubjectCardUpdated
	case "deleted":
		return SubjectCardDeleted
	default:
		return SubjectCardUpdated
	}
}

// StatusIDToLabel maps FocalBoard status property IDs to Forgejo label names.
// FocalBoard option IDs are bare strings matching this board's cardProperties.
var StatusIDToLabel = map[string]string{
	"backlog":     "backlog",
	"todo":        "todo",
	"in-progress": "in-progress",
	"review":      "review",
	"done":        "done",
	"blocked":     "blocked",
	"cancelled":   "cancelled",
}

// LabelToStatusID maps Forgejo label names to FocalBoard status property IDs.
var LabelToStatusID = map[string]string{
	"backlog":     "backlog",
	"todo":        "todo",
	"in-progress": "in-progress",
	"in progress": "in-progress",
	"wip":         "in-progress",
	"review":      "review",
	"needs review": "review",
	"in review":   "review",
	"done":        "done",
	"closed":      "done",
	"complete":    "done",
	"cancelled":   "cancelled",
}
