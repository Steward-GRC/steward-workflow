// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"testing"

	grpcactor "github.com/Bugs5382/go-grpc-actor"
	"github.com/stretchr/testify/require"

	"github.com/Steward-GRC/steward-workflow/internal/grpcsvc"
)

// During act-as every permission check runs as the target, so the extractor
// returns the subject; the admin is recorded from the same actor by the store
// and the audit emitter.
func TestActorFromContextIsTheEffectiveSubject(t *testing.T) {
	ctx := grpcactor.WithActor(context.Background(), grpcactor.Actor{Subject: "user-carol", Impersonator: "user-alice"})
	got, ok := actorFromContext(ctx)
	require.True(t, ok)
	require.Equal(t, grpcsvc.ActorClaims{UserID: "user-carol"}, got)
}

func TestActorFromContextWithoutAnActor(t *testing.T) {
	_, ok := actorFromContext(context.Background())
	require.False(t, ok, "a call with no admitted actor has no caller")
}
