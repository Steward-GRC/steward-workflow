// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc

import (
	"context"

	log "github.com/Bugs5382/go-log"

	"github.com/Steward-GRC/steward-workflow/internal/audit"
)

type auditSink interface {
	Emit(ctx context.Context, ev audit.Event) error
}

// AuditAdapter is the AuditEmitter on the AuditEvent emitter. A publish
// failure is logged, never returned: the action it records already happened.
type AuditAdapter struct {
	emitter auditSink
	log     log.Logger
}

// NewAuditAdapter wraps emitter.
func NewAuditAdapter(emitter auditSink, lg log.Logger) *AuditAdapter {
	return &AuditAdapter{emitter: emitter, log: lg}
}

// Emit records an event with no person behind it.
func (a *AuditAdapter) Emit(ctx context.Context, action, subject string) {
	a.EmitActorAttrs(ctx, action, subject, "", "", nil)
}

// EmitActor records an event with its actor.
func (a *AuditAdapter) EmitActor(ctx context.Context, action, subject, actorUserID string) {
	a.EmitActorAttrs(ctx, action, subject, actorUserID, "", nil)
}

// EmitActorAttrs records an event with its actor, category and attributes.
func (a *AuditAdapter) EmitActorAttrs(ctx context.Context, action, subject, actorUserID, groupID string, attrs map[string]string) {
	if err := a.emitter.Emit(ctx, audit.Event{
		Tier:        audit.TierAudit,
		Action:      action,
		Subject:     subject,
		ActorUserID: actorUserID,
		GroupID:     groupID,
		Attributes:  attrs,
	}); err != nil {
		a.log.Ctx(ctx).Error(err, "audit event not published", log.F("action", action), log.F("subject", subject))
	}
}
