// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package statemachine

import "fmt"

// Status represents a PolicyVersion approval status.
type Status string

const (
	StatusDraft      Status = "draft"
	StatusInReview   Status = "in_review"
	StatusApproved   Status = "approved"
	StatusScheduled  Status = "scheduled"
	StatusPublished  Status = "published"
	StatusSuperseded Status = "superseded"
	StatusRejected   Status = "rejected"
	StatusWithdrawn  Status = "withdrawn"
	StatusArchived   Status = "archived"
)

// Event represents a trigger that causes a status transition.
type Event string

const (
	EventSubmit         Event = "submit"
	EventAllApproved    Event = "all_approved"
	EventReject         Event = "reject"
	EventRequestChanges Event = "request_changes"
	EventWithdraw       Event = "withdraw"
	EventSchedule       Event = "schedule"  // effective_date is in the future
	EventPublish        Event = "publish"   // publish now (or timer fired)
	EventSupersede      Event = "supersede" // newer version published
	EventRetire         Event = "retire"    // retire / archive any live version
)

// transitions defines every valid (from, event) → to mapping.
var transitions = map[Status]map[Event]Status{
	StatusDraft: {
		EventSubmit: StatusInReview,
	},
	StatusInReview: {
		EventAllApproved:    StatusApproved,
		EventReject:         StatusRejected,
		EventRequestChanges: StatusDraft,
		EventWithdraw:       StatusWithdrawn,
	},
	StatusApproved: {
		EventSchedule: StatusScheduled,
		EventPublish:  StatusPublished,
		EventRetire:   StatusArchived,
	},
	StatusScheduled: {
		EventPublish: StatusPublished,
		EventRetire:  StatusArchived,
	},
	StatusPublished: {
		EventSupersede: StatusSuperseded,
		EventRetire:    StatusArchived,
	},
	// Terminal states: Rejected, Withdrawn, Superseded, Archived — no outgoing transitions.
}

// StatusToEvent maps a target status string to the Event that produces it.
// Used by the worker SDK handler to validate transitions before calling the Policy service.
// Returns EventSubmit as a sentinel for unknown statuses (caller should check Transition result).
func StatusToEvent(target Status) Event {
	switch target {
	case StatusInReview:
		return EventSubmit
	case StatusApproved:
		return EventAllApproved
	case StatusRejected:
		return EventReject
	case StatusScheduled:
		return EventSchedule
	case StatusPublished:
		return EventPublish
	case StatusSuperseded:
		return EventSupersede
	case StatusArchived:
		return EventRetire
	case StatusWithdrawn:
		return EventWithdraw
	case StatusDraft:
		return EventRequestChanges
	default:
		return EventSubmit
	}
}

// Transition returns the next Status given the current status and event,
// or an error if the transition is not allowed.
func Transition(from Status, event Event) (Status, error) {
	events, ok := transitions[from]
	if !ok {
		return "", fmt.Errorf("no transitions defined from status %q (terminal or unknown)", from)
	}
	next, ok := events[event]
	if !ok {
		return "", fmt.Errorf("event %q is not valid from status %q", event, from)
	}
	return next, nil
}
