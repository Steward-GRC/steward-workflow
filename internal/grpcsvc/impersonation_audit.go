// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc

import (
	"context"
	"maps"

	grpcactor "github.com/Bugs5382/go-grpc-actor"
)

type impersonationAuditEmitter struct{ inner AuditEmitter }

// NewImpersonationAuditEmitter credits every event to the real admin during
// act-as, keeping the account acted as in impersonated_user_id. Only the
// request path uses it: the outage reconciler's system events never carry an
// actor to rewrite.
func NewImpersonationAuditEmitter(inner AuditEmitter) AuditEmitter {
	if inner == nil {
		return nil
	}
	return impersonationAuditEmitter{inner: inner}
}

func (e impersonationAuditEmitter) Emit(ctx context.Context, action, subject string) {
	e.EmitActorAttrs(ctx, action, subject, "", "", nil)
}

func (e impersonationAuditEmitter) EmitActor(ctx context.Context, action, subject, actorUserID string) {
	e.EmitActorAttrs(ctx, action, subject, actorUserID, "", nil)
}

func (e impersonationAuditEmitter) EmitActorAttrs(ctx context.Context, action, subject, actorUserID, groupID string, attrs map[string]string) {
	a, ok := grpcactor.FromContext(ctx)
	if !ok || !a.Impersonated() {
		e.inner.EmitActorAttrs(ctx, action, subject, actorUserID, groupID, attrs)
		return
	}
	out := make(map[string]string, len(attrs)+1)
	maps.Copy(out, attrs)
	if actorUserID != "" {
		out["impersonated_user_id"] = actorUserID
	}
	e.inner.EmitActorAttrs(ctx, action, subject, a.Impersonator, groupID, out)
}
