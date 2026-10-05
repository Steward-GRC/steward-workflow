// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc

import (
	"context"
	"encoding/json"
	"fmt"
)

// The "jobs" routing keys obligations binds to for the submitter's notices.
const (
	runStartedRoutingKey = "workflow.started"
	runDeniedRoutingKey  = "workflow.denied"
)

// RunStartedEvent tells obligations a submitter's run started.
type RunStartedEvent struct {
	EventType         string `json:"event_type"`
	RunID             string `json:"run_id"`
	PolicyID          string `json:"policy_id"`
	PolicyVersionID   string `json:"policy_version_id"`
	SubmittedByUserID string `json:"submitted_by_user_id"`
	WorkflowName      string `json:"workflow_name"`
	// CurrentStep is the first stage's name.
	CurrentStep string `json:"current_step,omitempty"`
}

// RunDeniedEvent tells obligations a submitter's run was rejected.
type RunDeniedEvent struct {
	EventType         string `json:"event_type"`
	RunID             string `json:"run_id"`
	PolicyID          string `json:"policy_id"`
	PolicyVersionID   string `json:"policy_version_id"`
	SubmittedByUserID string `json:"submitted_by_user_id"`
	ReviewedByUserID  string `json:"reviewed_by_user_id"`
	WorkflowName      string `json:"workflow_name"`
	// Reason is the rejecting comment.
	Reason string `json:"reason,omitempty"`
}

type runNotifier interface {
	NotifyRunStarted(ctx context.Context, e RunStartedEvent) error
	NotifyRunDenied(ctx context.Context, e RunDeniedEvent) error
}

type eventPublisher interface {
	Publish(ctx context.Context, routingKey string, body []byte) error
}

// RunEventEmitter publishes the run events as JSON on the "jobs" exchange.
type RunEventEmitter struct {
	pub eventPublisher
}

// NewRunEventEmitter wraps a "jobs" publisher.
func NewRunEventEmitter(pub eventPublisher) *RunEventEmitter {
	return &RunEventEmitter{pub: pub}
}

// NotifyRunStarted publishes evt on workflow.started.
func (e *RunEventEmitter) NotifyRunStarted(ctx context.Context, evt RunStartedEvent) error {
	return e.publish(ctx, runStartedRoutingKey, evt)
}

// NotifyRunDenied publishes evt on workflow.denied.
func (e *RunEventEmitter) NotifyRunDenied(ctx context.Context, evt RunDeniedEvent) error {
	return e.publish(ctx, runDeniedRoutingKey, evt)
}

func (e *RunEventEmitter) publish(ctx context.Context, routingKey string, evt any) error {
	body, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("run event emitter: marshal %s: %w", routingKey, err)
	}
	if err := e.pub.Publish(ctx, routingKey, body); err != nil {
		return fmt.Errorf("run event emitter: publish %s: %w", routingKey, err)
	}
	return nil
}
