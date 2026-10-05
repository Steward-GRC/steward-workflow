// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package saga

import (
	"context"
	"encoding/json"
	"fmt"
)

// approvalRequestedRoutingKey is the "jobs" routing key obligations binds to.
const approvalRequestedRoutingKey = "workflow.approval_requested"

type eventPublisher interface {
	Publish(ctx context.Context, routingKey string, body []byte) error
}

// ApprovalEmitter publishes ApprovalRequestedEvents as JSON.
type ApprovalEmitter struct {
	pub eventPublisher
}

// NewApprovalEmitter wraps a "jobs" exchange publisher.
func NewApprovalEmitter(pub eventPublisher) *ApprovalEmitter {
	return &ApprovalEmitter{pub: pub}
}

// NotifyApprovalRequested publishes evt on workflow.approval_requested.
func (e *ApprovalEmitter) NotifyApprovalRequested(ctx context.Context, evt ApprovalRequestedEvent) error {
	body, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("approval emitter: marshal event: %w", err)
	}
	if err := e.pub.Publish(ctx, approvalRequestedRoutingKey, body); err != nil {
		return fmt.Errorf("approval emitter: publish %s: %w", approvalRequestedRoutingKey, err)
	}
	return nil
}
