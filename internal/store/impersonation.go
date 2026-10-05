// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"

	grpcactor "github.com/Bugs5382/go-grpc-actor"
)

// Act-as attribution. During act-as the request runs as the target (every
// permission and seat decision evaluates as them) and go-grpc-actor carries
// the real admin as the actor's Impersonator. Everything that records who did
// something names the admin, so the store applies it on every audited write
// rather than leaving each caller to remember.

// ActingAdmin returns the real admin during act-as, and ok=false for an
// ordinary request or a background context (the outage reconciler, the saga
// timer), where no act-as can be in flight.
func ActingAdmin(ctx context.Context) (string, bool) {
	a, ok := grpcactor.FromContext(ctx)
	if !ok || !a.Impersonated() {
		return "", false
	}
	return a.Impersonator, true
}

// Attribution returns the (actor_user_id, impersonated_user_id) pair to store
// for actorUserID, the effective actor. An ordinary request stores
// (actorUserID, nil); during act-as, (admin, &actorUserID). A blank effective
// actor during act-as stores the admin and no target, since "" would read as a
// user id.
func Attribution(ctx context.Context, actorUserID string) (actor string, impersonated *string) {
	admin, ok := ActingAdmin(ctx)
	if !ok {
		return actorUserID, nil
	}
	if actorUserID == "" {
		return admin, nil
	}
	target := actorUserID
	return admin, &target
}

func nullableText(s string) any {
	if s == "" {
		return nil
	}
	return s
}
