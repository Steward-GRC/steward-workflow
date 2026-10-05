// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package eligibility

import (
	"reflect"
	"testing"

	"github.com/Steward-GRC/steward-workflow/internal/builder"
)

// A stage's individual approvers for a policy are the entry for its home
// category, else the nearest ancestor's. The lineage runs from the home
// category to the root.
func TestResolveByCategory(t *testing.T) {
	stage := builder.Stage{
		ApproversByCategory: map[string][]string{
			"cat-facilities": {"user-carol", "user-dave"},
			"cat-workplace":  {"user-grace"},
		},
	}

	tests := []struct {
		name    string
		lineage []string
		want    []string
	}{
		{name: "home category present", lineage: []string{"cat-facilities", "cat-workplace", "cat-root"}, want: []string{"user-carol", "user-dave"}},
		{name: "home absent, first ancestor present", lineage: []string{"cat-workplace", "cat-root"}, want: []string{"user-grace"}},
		{name: "no category in lineage present", lineage: []string{"cat-other"}, want: []string{}},
		{name: "home absent, walks to a present ancestor", lineage: []string{"cat-desks", "cat-facilities", "cat-root"}, want: []string{"user-carol", "user-dave"}},
		{name: "empty lineage", lineage: nil, want: []string{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ResolveByCategory(stage, tt.lineage)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("ResolveByCategory(%v): got %v, want %v", tt.lineage, got, tt.want)
			}
		})
	}
}

func TestResolveByCategory_NilMap(t *testing.T) {
	if got := ResolveByCategory(builder.Stage{}, []string{"cat-facilities"}); len(got) != 0 {
		t.Fatalf("expected empty, got %v", got)
	}
}

func TestResolveByCategory_ReturnsACopy(t *testing.T) {
	stage := builder.Stage{ApproversByCategory: map[string][]string{"cat-facilities": {"user-carol"}}}
	got := ResolveByCategory(stage, []string{"cat-facilities"})
	got[0] = "user-mallory"
	if stage.ApproversByCategory["cat-facilities"][0] != "user-carol" {
		t.Fatal("the stage's list was changed through the result")
	}
}

func TestContains(t *testing.T) {
	if !Contains([]string{"user-carol", "user-dave"}, "user-dave") {
		t.Fatal("want true")
	}
	if Contains([]string{"user-carol"}, "user-erin") {
		t.Fatal("want false")
	}
	if Contains(nil, "user-carol") {
		t.Fatal("want false on nil")
	}
}

func TestStagePool(t *testing.T) {
	stage := builder.Stage{
		ApproverIDs:         []string{"user-erin"},
		ApproversByCategory: map[string][]string{"cat-facilities": {"user-carol"}},
	}
	if got := StagePool(stage, []string{"cat-facilities"}); !reflect.DeepEqual(got, []string{"user-carol"}) {
		t.Fatalf("category entry: got %v", got)
	}
	if got := StagePool(stage, []string{"cat-travel"}); !reflect.DeepEqual(got, []string{"user-erin"}) {
		t.Fatalf("fallback to approver_ids: got %v", got)
	}
}
