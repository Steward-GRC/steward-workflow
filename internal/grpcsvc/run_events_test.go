// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

// capturingPublisher records the routing key + body of each Publish call.
type capturingPublisher struct {
	routingKey string
	body       []byte
	err        error
}

func (p *capturingPublisher) Publish(_ context.Context, routingKey string, body []byte) error {
	p.routingKey = routingKey
	p.body = body
	return p.err
}

func TestRunEventEmitter_StartedPublishesJSONOnJobsKey(t *testing.T) {
	pub := &capturingPublisher{}
	em := NewRunEventEmitter(pub)

	evt := RunStartedEvent{
		EventType:         "workflow.started",
		RunID:             "run-1",
		PolicyID:          "pol-9",
		PolicyVersionID:   "pv-1",
		SubmittedByUserID: "sub-1",
		WorkflowName:      "Policy review & publish",
		CurrentStep:       "Legal review",
	}
	if err := em.NotifyRunStarted(context.Background(), evt); err != nil {
		t.Fatalf("NotifyRunStarted: %v", err)
	}
	if pub.routingKey != "workflow.started" {
		t.Errorf("routing key: want workflow.started, got %q", pub.routingKey)
	}
	var got RunStartedEvent
	if err := json.Unmarshal(pub.body, &got); err != nil {
		t.Fatalf("body is not the marshaled event: %v", err)
	}
	if got != evt {
		t.Errorf("round-trip mismatch:\n want %+v\n got  %+v", evt, got)
	}
}

func TestRunEventEmitter_DeniedPublishesJSONOnJobsKey(t *testing.T) {
	pub := &capturingPublisher{}
	em := NewRunEventEmitter(pub)

	evt := RunDeniedEvent{
		EventType:         "workflow.denied",
		RunID:             "run-2",
		PolicyID:          "pol-9",
		PolicyVersionID:   "pv-2",
		SubmittedByUserID: "sub-1",
		ReviewedByUserID:  "rev-1",
		WorkflowName:      "Policy review & publish",
		Reason:            "Section 4 conflicts with the retention schedule.",
	}
	if err := em.NotifyRunDenied(context.Background(), evt); err != nil {
		t.Fatalf("NotifyRunDenied: %v", err)
	}
	if pub.routingKey != "workflow.denied" {
		t.Errorf("routing key: want workflow.denied, got %q", pub.routingKey)
	}
	var got RunDeniedEvent
	if err := json.Unmarshal(pub.body, &got); err != nil {
		t.Fatalf("body is not the marshaled event: %v", err)
	}
	if got != evt {
		t.Errorf("round-trip mismatch:\n want %+v\n got  %+v", evt, got)
	}
}

func TestRunEventEmitter_WrapsPublishError(t *testing.T) {
	sentinel := errors.New("broker down")
	em := NewRunEventEmitter(&capturingPublisher{err: sentinel})
	if err := em.NotifyRunStarted(context.Background(), RunStartedEvent{}); !errors.Is(err, sentinel) {
		t.Fatalf("started: want wrapped publish error, got %v", err)
	}
	if err := em.NotifyRunDenied(context.Background(), RunDeniedEvent{}); !errors.Is(err, sentinel) {
		t.Fatalf("denied: want wrapped publish error, got %v", err)
	}
}
