// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package saga

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

// capturingPublisher records the routing key and body of each Publish call.
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

func TestApprovalEmitter_PublishesJSONOnJobsRoutingKey(t *testing.T) {
	pub := &capturingPublisher{}
	em := NewApprovalEmitter(pub)

	evt := ApprovalRequestedEvent{
		EventType:       "workflow.approval_requested",
		TaskID:          "pv-1:0",
		PolicyID:        "pol-9",
		PolicyVersionID: "pv-1",
		StageIndex:      0,
		ApproverUserID:  "u1",
		WorkflowName:    "Policy review & publish",
	}
	if err := em.NotifyApprovalRequested(context.Background(), evt); err != nil {
		t.Fatalf("NotifyApprovalRequested: %v", err)
	}

	if pub.routingKey != "workflow.approval_requested" {
		t.Errorf("routing key: want workflow.approval_requested, got %q", pub.routingKey)
	}
	var got ApprovalRequestedEvent
	if err := json.Unmarshal(pub.body, &got); err != nil {
		t.Fatalf("published body is not the marshaled event: %v", err)
	}
	if got != evt {
		t.Errorf("round-tripped event mismatch:\n want %+v\n got  %+v", evt, got)
	}
}

func TestApprovalEmitter_WrapsPublishError(t *testing.T) {
	sentinel := errors.New("broker down")
	em := NewApprovalEmitter(&capturingPublisher{err: sentinel})
	err := em.NotifyApprovalRequested(context.Background(), ApprovalRequestedEvent{})
	if !errors.Is(err, sentinel) {
		t.Fatalf("want wrapped publish error, got %v", err)
	}
}
