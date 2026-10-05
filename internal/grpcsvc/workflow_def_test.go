// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc

import (
	"context"
	"testing"

	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	"github.com/Steward-GRC/steward-workflow/internal/builder"
)

// ---------------------------------------------------------------------------
// fake def store (covers the expanded WorkflowDefStoreReader interface)
// ---------------------------------------------------------------------------

type fakeDefStore struct {
	// seeded defs by id
	defs    map[string]builder.WorkflowDef
	nextID  string
	listErr error
	getErr  error
	crtErr  error
	updErr  error
	arcErr  error
	gcErr   error
}

func newFakeDefStore() *fakeDefStore {
	return &fakeDefStore{
		defs:   make(map[string]builder.WorkflowDef),
		nextID: "def-fake-1",
	}
}

func (f *fakeDefStore) Get(_ context.Context, id string) (builder.WorkflowDef, error) {
	if f.getErr != nil {
		return builder.WorkflowDef{}, f.getErr
	}
	wd, ok := f.defs[id]
	if !ok {
		return builder.WorkflowDef{}, f.getErr
	}
	return wd, nil
}

func (f *fakeDefStore) Publish(_ context.Context, _ string) error { return nil }

func (f *fakeDefStore) GetVersion(_ context.Context, id string, version int) (builder.WorkflowDef, error) {
	wd, ok := f.defs[id]
	if !ok {
		return builder.WorkflowDef{}, nil
	}
	wd.Version = version
	return wd, nil
}

func (f *fakeDefStore) GCVersion(_ context.Context, _ string, _ int) error { return f.gcErr }

func (f *fakeDefStore) Create(_ context.Context, wd builder.WorkflowDef) (string, error) {
	if f.crtErr != nil {
		return "", f.crtErr
	}
	id := f.nextID
	wd.ID = id
	wd.Version = 1
	f.defs[id] = wd
	return id, nil
}

func (f *fakeDefStore) List(_ context.Context) ([]builder.WorkflowDef, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	out := make([]builder.WorkflowDef, 0, len(f.defs))
	for _, wd := range f.defs {
		out = append(out, wd)
	}
	return out, nil
}

func (f *fakeDefStore) Update(_ context.Context, id string, wd builder.WorkflowDef) (builder.WorkflowDef, error) {
	if f.updErr != nil {
		return builder.WorkflowDef{}, f.updErr
	}
	cur, ok := f.defs[id]
	if !ok {
		return builder.WorkflowDef{}, f.updErr
	}
	wd.ID = id
	wd.Version = cur.Version + 1
	f.defs[id] = wd
	return wd, nil
}

func (f *fakeDefStore) Archive(_ context.Context, _ string) error { return f.arcErr }

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func newServerWithFakeDefStore(fds *fakeDefStore) *WorkflowServer {
	return NewWorkflowServer(WorkflowServerOpts{
		Resolver:    &stubWorkflowResolver{defID: "def-1"},
		DefStore:    fds,
		RunStore:    &stubRunStore{},
		SagaClient:  &stubSagaClient{},
		AuditEmit:   &stubAuditEmitter{},
		Assignments: newStubAssignments(),
		CompileOpts: builder.CompileOptions{StageSLAHours: 72, StageReminderHours: 48},
		ActorExtractor: func(_ context.Context) (ActorClaims, bool) {
			return ActorClaims{UserID: "test-actor", Roles: []string{"admin"}}, true
		},
	})
}

func stageWithApprovers(name string, approvers ...string) *workflowv1.WorkflowStage {
	return &workflowv1.WorkflowStage{
		Id:          "ws-" + name,
		Name:        name,
		ApproverIds: approvers,
		Quorum:      "one",
		SlaDays:     3,
	}
}

// ---------------------------------------------------------------------------
// CreateWorkflowDef
// ---------------------------------------------------------------------------

func TestCreateWorkflowDef_ReturnsVersionOne(t *testing.T) {
	fds := newFakeDefStore()
	srv := newServerWithFakeDefStore(fds)

	resp, err := srv.CreateWorkflowDef(context.Background(), &workflowv1.CreateWorkflowDefRequest{
		Name:        "My Workflow",
		Description: "desc",
		Stages:      []*workflowv1.WorkflowStage{stageWithApprovers("Review", "u1", "u2")},
	})
	if err != nil {
		t.Fatalf("CreateWorkflowDef: %v", err)
	}
	if resp.Def == nil {
		t.Fatal("expected non-nil def in response")
	}
	if resp.Def.Version != 1 {
		t.Errorf("expected version=1, got %d", resp.Def.Version)
	}
	if resp.Def.Name != "My Workflow" {
		t.Errorf("expected name='My Workflow', got %q", resp.Def.Name)
	}
	if resp.Def.Id == "" {
		t.Error("expected non-empty id")
	}
}

func TestCreateWorkflowDef_EmptyNameFails(t *testing.T) {
	fds := newFakeDefStore()
	srv := newServerWithFakeDefStore(fds)

	_, err := srv.CreateWorkflowDef(context.Background(), &workflowv1.CreateWorkflowDefRequest{
		Name:   "",
		Stages: []*workflowv1.WorkflowStage{stageWithApprovers("Review", "u1")},
	})
	if err == nil {
		t.Fatal("expected error for empty name")
	}
}

func TestCreateWorkflowDef_NoStagesFails(t *testing.T) {
	fds := newFakeDefStore()
	srv := newServerWithFakeDefStore(fds)

	_, err := srv.CreateWorkflowDef(context.Background(), &workflowv1.CreateWorkflowDefRequest{
		Name:   "Workflow",
		Stages: nil,
	})
	if err == nil {
		t.Fatal("expected error for empty stages")
	}
}

// ---------------------------------------------------------------------------
// UpdateWorkflowDef
// ---------------------------------------------------------------------------

func TestUpdateWorkflowDef_BumpsToVersionTwo(t *testing.T) {
	fds := newFakeDefStore()
	srv := newServerWithFakeDefStore(fds)

	// Create v1 first.
	createResp, err := srv.CreateWorkflowDef(context.Background(), &workflowv1.CreateWorkflowDefRequest{
		Name:   "Workflow",
		Stages: []*workflowv1.WorkflowStage{stageWithApprovers("S0", "u1")},
	})
	if err != nil {
		t.Fatalf("CreateWorkflowDef: %v", err)
	}
	id := createResp.Def.Id

	// Update → v2.
	updateResp, err := srv.UpdateWorkflowDef(context.Background(), &workflowv1.UpdateWorkflowDefRequest{
		Id:     id,
		Name:   "Workflow v2",
		Stages: []*workflowv1.WorkflowStage{stageWithApprovers("S0b", "u2")},
	})
	if err != nil {
		t.Fatalf("UpdateWorkflowDef: %v", err)
	}
	if updateResp.Def.Version != 2 {
		t.Errorf("expected version=2, got %d", updateResp.Def.Version)
	}
	if updateResp.Def.Name != "Workflow v2" {
		t.Errorf("expected name='Workflow v2', got %q", updateResp.Def.Name)
	}
}

func TestUpdateWorkflowDef_EmptyIDFails(t *testing.T) {
	fds := newFakeDefStore()
	srv := newServerWithFakeDefStore(fds)

	_, err := srv.UpdateWorkflowDef(context.Background(), &workflowv1.UpdateWorkflowDefRequest{
		Id:     "",
		Name:   "Workflow",
		Stages: []*workflowv1.WorkflowStage{stageWithApprovers("S0", "u1")},
	})
	if err == nil {
		t.Fatal("expected error for empty id")
	}
}

// ---------------------------------------------------------------------------
// GetWorkflowDef
// ---------------------------------------------------------------------------

func TestGetWorkflowDef_ReturnsExistingDef(t *testing.T) {
	fds := newFakeDefStore()
	srv := newServerWithFakeDefStore(fds)

	createResp, _ := srv.CreateWorkflowDef(context.Background(), &workflowv1.CreateWorkflowDefRequest{
		Name:   "My Workflow",
		Stages: []*workflowv1.WorkflowStage{stageWithApprovers("S0", "u1")},
	})
	id := createResp.Def.Id

	getResp, err := srv.GetWorkflowDef(context.Background(), &workflowv1.GetWorkflowDefRequest{Id: id})
	if err != nil {
		t.Fatalf("GetWorkflowDef: %v", err)
	}
	if getResp.Def.Id != id {
		t.Errorf("expected id=%q, got %q", id, getResp.Def.Id)
	}
	if getResp.Def.Name != "My Workflow" {
		t.Errorf("expected name='My Workflow', got %q", getResp.Def.Name)
	}
}

func TestGetWorkflowDef_EmptyIDFails(t *testing.T) {
	fds := newFakeDefStore()
	srv := newServerWithFakeDefStore(fds)

	_, err := srv.GetWorkflowDef(context.Background(), &workflowv1.GetWorkflowDefRequest{Id: ""})
	if err == nil {
		t.Fatal("expected error for empty id")
	}
}

// ---------------------------------------------------------------------------
// ListWorkflowDefs
// ---------------------------------------------------------------------------

func TestListWorkflowDefs_ReturnsAll(t *testing.T) {
	fds := newFakeDefStore()
	srv := newServerWithFakeDefStore(fds)

	// Seed two defs.
	fds.nextID = "def-a"
	_, _ = srv.CreateWorkflowDef(context.Background(), &workflowv1.CreateWorkflowDefRequest{
		Name:   "Alpha",
		Stages: []*workflowv1.WorkflowStage{stageWithApprovers("S0", "u1")},
	})
	fds.nextID = "def-b"
	_, _ = srv.CreateWorkflowDef(context.Background(), &workflowv1.CreateWorkflowDefRequest{
		Name:   "Beta",
		Stages: []*workflowv1.WorkflowStage{stageWithApprovers("S0", "u2")},
	})

	resp, err := srv.ListWorkflowDefs(context.Background(), &workflowv1.ListWorkflowDefsRequest{})
	if err != nil {
		t.Fatalf("ListWorkflowDefs: %v", err)
	}
	if len(resp.Defs) != 2 {
		t.Errorf("expected 2 defs, got %d", len(resp.Defs))
	}
}

func TestListWorkflowDefs_Empty(t *testing.T) {
	fds := newFakeDefStore()
	srv := newServerWithFakeDefStore(fds)

	resp, err := srv.ListWorkflowDefs(context.Background(), &workflowv1.ListWorkflowDefsRequest{})
	if err != nil {
		t.Fatalf("ListWorkflowDefs: %v", err)
	}
	if len(resp.Defs) != 0 {
		t.Errorf("expected 0 defs, got %d", len(resp.Defs))
	}
}

// ---------------------------------------------------------------------------
// ArchiveWorkflowDef
// ---------------------------------------------------------------------------

func TestArchiveWorkflowDef_Succeeds(t *testing.T) {
	fds := newFakeDefStore()
	srv := newServerWithFakeDefStore(fds)

	createResp, _ := srv.CreateWorkflowDef(context.Background(), &workflowv1.CreateWorkflowDefRequest{
		Name:   "Workflow",
		Stages: []*workflowv1.WorkflowStage{stageWithApprovers("S0", "u1")},
	})

	_, err := srv.ArchiveWorkflowDef(context.Background(), &workflowv1.ArchiveWorkflowDefRequest{
		Id: createResp.Def.Id,
	})
	if err != nil {
		t.Fatalf("ArchiveWorkflowDef: %v", err)
	}
}

func TestArchiveWorkflowDef_EmptyIDFails(t *testing.T) {
	fds := newFakeDefStore()
	srv := newServerWithFakeDefStore(fds)

	_, err := srv.ArchiveWorkflowDef(context.Background(), &workflowv1.ArchiveWorkflowDefRequest{Id: ""})
	if err == nil {
		t.Fatal("expected error for empty id")
	}
}

// ---------------------------------------------------------------------------
// Proto <-> builder mapping round-trip
// ---------------------------------------------------------------------------

func TestStageRoundTrip_ProtoToBuilderAndBack(t *testing.T) {
	proto := &workflowv1.WorkflowStage{
		Id:                "ws-1",
		Name:              "Review",
		ApproverIds:       []string{"u1", "u2"},
		Quorum:            "majority",
		SlaDays:           5,
		RejectOnSlaBreach: true,
		PinnedLast:        true,
	}

	// proto → builder
	stage := stageFromProto(proto)
	if stage.ID != "ws-1" {
		t.Errorf("ID: got %q want 'ws-1'", stage.ID)
	}
	if stage.Name != "Review" {
		t.Errorf("Name: got %q want 'Review'", stage.Name)
	}
	if len(stage.ApproverIDs) != 2 || stage.ApproverIDs[0] != "u1" || stage.ApproverIDs[1] != "u2" {
		t.Errorf("ApproverIDs: got %v", stage.ApproverIDs)
	}
	if stage.Quorum != builder.QuorumMajority {
		t.Errorf("Quorum: got %q want QuorumMajority", stage.Quorum)
	}
	if stage.SLADays != 5 {
		t.Errorf("SLADays: got %d want 5", stage.SLADays)
	}
	if !stage.RejectOnSLABreach {
		t.Error("RejectOnSLABreach: got false want true")
	}
	if !stage.PinnedLast {
		t.Error("PinnedLast: got false want true")
	}

	// builder → proto
	back := stageToProto(stage)
	if back.Id != proto.Id {
		t.Errorf("ID roundtrip: got %q want %q", back.Id, proto.Id)
	}
	if back.Quorum != proto.Quorum {
		t.Errorf("Quorum roundtrip: got %q want %q", back.Quorum, proto.Quorum)
	}
	if back.SlaDays != proto.SlaDays {
		t.Errorf("SlaDays roundtrip: got %d want %d", back.SlaDays, proto.SlaDays)
	}
	if back.RejectOnSlaBreach != proto.RejectOnSlaBreach {
		t.Errorf("RejectOnSlaBreach roundtrip: got %v want %v", back.RejectOnSlaBreach, proto.RejectOnSlaBreach)
	}
	if back.PinnedLast != proto.PinnedLast {
		t.Errorf("PinnedLast roundtrip: got %v want %v", back.PinnedLast, proto.PinnedLast)
	}
}

func TestQuorumMapping(t *testing.T) {
	cases := []struct {
		protoStr string
		want     builder.Quorum
	}{
		{"one", builder.QuorumAny},
		{"majority", builder.QuorumMajority},
		{"all", builder.QuorumAll},
		{"unknown", builder.QuorumAny}, // default fallback
	}
	for _, tc := range cases {
		proto := &workflowv1.WorkflowStage{
			Id:          "ws",
			Name:        "S",
			ApproverIds: []string{"u1"},
			Quorum:      tc.protoStr,
		}
		got := stageFromProto(proto).Quorum
		if got != tc.want {
			t.Errorf("quorum %q: got %q want %q", tc.protoStr, got, tc.want)
		}
	}
}

// quorumToProto mapping
func TestQuorumToProto(t *testing.T) {
	cases := []struct {
		q    builder.Quorum
		want string
	}{
		{builder.QuorumAny, "one"},
		{builder.QuorumMajority, "majority"},
		{builder.QuorumAll, "all"},
		{builder.QuorumNofM, "majority"}, // NofM falls back
	}
	for _, tc := range cases {
		stage := builder.Stage{ID: "x", Name: "S", ApproverIDs: []string{"u"}, Quorum: tc.q}
		got := stageToProto(stage).Quorum
		if got != tc.want {
			t.Errorf("QuorumToProto(%q): got %q want %q", tc.q, got, tc.want)
		}
	}
}

// TestDefToProto_FieldMapping checks that defToProto maps all fields correctly.
func TestDefToProto_FieldMapping(t *testing.T) {
	wd := builder.WorkflowDef{
		ID:      "def-1",
		Name:    "Workflow",
		Version: 3,
		Stages: []builder.Stage{
			{
				ID:          "ws-a",
				Name:        "Approve",
				ApproverIDs: []string{"u1"},
				Quorum:      builder.QuorumAny,
				SLADays:     2,
			},
		},
	}
	p := defToProto(wd)
	if p.Id != "def-1" {
		t.Errorf("Id: got %q want 'def-1'", p.Id)
	}
	if p.Version != 3 {
		t.Errorf("Version: got %d want 3", p.Version)
	}
	if len(p.Stages) != 1 {
		t.Fatalf("Stages len: got %d want 1", len(p.Stages))
	}
	if p.Stages[0].Id != "ws-a" {
		t.Errorf("Stage.Id: got %q want 'ws-a'", p.Stages[0].Id)
	}
	if p.Stages[0].SlaDays != 2 {
		t.Errorf("Stage.SlaDays: got %d want 2", p.Stages[0].SlaDays)
	}
}

// TestWorkflowDefActorFlowsToAudit verifies the actor_user_id from the request
// reaches the audit emitter for the admin-side workflow-def mutations
// (workflow_def.created/updated/archived). Regression guard for the "empty
// actor on admin audit rows" bug.
func TestWorkflowDefActorFlowsToAudit(t *testing.T) {
	fds := newFakeDefStore()
	ae := &stubAuditEmitter{}
	srv := NewWorkflowServer(WorkflowServerOpts{
		Resolver:    &stubWorkflowResolver{defID: "def-1"},
		DefStore:    fds,
		RunStore:    &stubRunStore{},
		SagaClient:  &stubSagaClient{},
		AuditEmit:   ae,
		Assignments: newStubAssignments(),
		CompileOpts: builder.CompileOptions{StageSLAHours: 72, StageReminderHours: 48},
		ActorExtractor: func(_ context.Context) (ActorClaims, bool) {
			return ActorClaims{UserID: "test-actor", Roles: []string{"admin"}}, true
		},
	})
	ctx := context.Background()
	actor := "actor-123"

	cr, err := srv.CreateWorkflowDef(ctx, &workflowv1.CreateWorkflowDefRequest{
		Name:        "WF",
		Stages:      []*workflowv1.WorkflowStage{stageWithApprovers("Review", "u1")},
		ActorUserId: actor,
	})
	if err != nil {
		t.Fatalf("CreateWorkflowDef: %v", err)
	}
	id := cr.Def.Id

	if _, err := srv.UpdateWorkflowDef(ctx, &workflowv1.UpdateWorkflowDefRequest{
		Id:          id,
		Name:        "WF2",
		Stages:      []*workflowv1.WorkflowStage{stageWithApprovers("Review", "u1")},
		ActorUserId: actor,
	}); err != nil {
		t.Fatalf("UpdateWorkflowDef: %v", err)
	}
	if _, err := srv.ArchiveWorkflowDef(ctx, &workflowv1.ArchiveWorkflowDefRequest{
		Id: id, ActorUserId: actor,
	}); err != nil {
		t.Fatalf("ArchiveWorkflowDef: %v", err)
	}

	for _, action := range []string{"workflow_def.created", "workflow_def.updated", "workflow_def.archived"} {
		got, ok := ae.actors[action]
		if !ok {
			t.Fatalf("expected an audit event for %q", action)
		}
		if got != actor {
			t.Fatalf("action %q: actor got %q want %q", action, got, actor)
		}
	}
}
