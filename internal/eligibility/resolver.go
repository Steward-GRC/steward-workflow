// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package eligibility works out who may hold a stage's individual seats.
package eligibility

import (
	"slices"

	"github.com/Steward-GRC/steward-workflow/internal/builder"
)

// ResolveByCategory returns the stage's individual approvers for a policy
// whose categories run from home to root in lineage: the first category with
// an entry in ApproversByCategory. It returns an empty slice when none has
// one. The result is a copy.
func ResolveByCategory(stage builder.Stage, lineage []string) []string {
	for _, id := range lineage {
		if ids, ok := stage.ApproversByCategory[id]; ok {
			return slices.Clone(ids)
		}
	}
	return []string{}
}

// StagePool returns the stage's individual approvers for the lineage: the
// category entry, else ApproverIDs.
func StagePool(stage builder.Stage, lineage []string) []string {
	if pool := ResolveByCategory(stage, lineage); len(pool) > 0 {
		return pool
	}
	return slices.Clone(stage.ApproverIDs)
}

// Contains reports whether userID is in pool.
func Contains(pool []string, userID string) bool {
	return slices.Contains(pool, userID)
}
