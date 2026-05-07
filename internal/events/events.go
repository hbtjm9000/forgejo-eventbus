// Package events defines typed event structs for the Forgejo event bus.
// All events published to NATS use these types — no raw webhook payloads.
package events

import "time"

// Subject constants — NATS subjects for pub/sub routing.
const (
	SubjectIssueCreated = "forgejo.issue.created"
	SubjectIssueClosed  = "forgejo.issue.closed"
	SubjectIssueUpdated = "forgejo.issue.updated"
	SubjectIssueLabeled = "forgejo.issue.labeled"
	SubjectAnyIssue     = "forgejo.issue.*" // wildcard for subscribers wanting all issue events
)

// IssueEvent is the canonical event emitted when a Forgejo issue changes.
type IssueEvent struct {
	EventID    string    `json:"event_id"`    // unique ID for deduplication
	Timestamp  time.Time `json:"timestamp"`   // when the event was created
	Action     string    `json:"action"`      // "opened" | "closed" | "reopened" | "labeled" | "unlabeled" | "updated"
	Repository struct {
		FullName string `json:"full_name"` // "owner/repo"
	} `json:"repository"`
	Issue struct {
		Number int    `json:"number"`
		Title  string `json:"title"`
		Body   string `json:"body"`
		State  string `json:"state"` // "open" | "closed"
		URL    string `json:"url"`
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
// Use this in publishers to route events correctly.
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
