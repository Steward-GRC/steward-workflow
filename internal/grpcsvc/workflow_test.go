// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// --- stubs ---

type stubWorkflowResolver struct {
	defID           string
	lastPolicyID    string
	lastAncestorIDs []string
	err             error
}

func (s *stubWorkflowResolver) Resolve(_ context.Context, policyID string, ancestorIDs []string) (string, error) {
	s.lastPolicyID = policyID
	s.lastAncestorIDs = ancestorIDs
	return s.defID, s.err
}

type stubWorkflowDefStore struct {
	wd  builder.WorkflowDef
	err error
}

func (s *stubWorkflowDefStore) Get(_ context.Context, _ string) (builder.WorkflowDef, error) {
	return s.wd, s.err
}
func (s *stubWorkflowDefStore) Publish(_ context.Context, _ string) error { return nil }
func (s *stubWorkflowDefStore) GetVersion(_ context.Context, _ string, _ int) (builder.WorkflowDef, error) {
	return s.wd, s.err
}
func (s *stubWorkflowDefStore) GCVersion(_ context.Context, _ string, _ int) error { return nil }

type stubRunStore struct {
	tracked      *store.ApprovalRun
	runStatus    string
	trackErr     error
	getErr       error
	notFoundPV   string // when set, GetRun(notFoundPV) returns "not found" (stale-run path)
	abortedForPV []string
	abortErr     error
	activeRuns   []store.ApprovalRun // returned by ListActiveRuns (upcoming-tasks tests)
}

func (s *stubRunStore) AbortActiveRuns(_ context.Context, pvID string) (int64, error) {
	if s.abortErr != nil {
		return 0, s.abortErr
	}
	s.abortedForPV = append(s.abortedForPV, pvID)
	return 0, nil
}

func (s *stubRunStore) TrackRun(_ context.Context, r store.ApprovalRun) error {
	if s.trackErr != nil {
		return s.trackErr
	}
	s.tracked = &r
	return nil
}
func (s *stubRunStore) GetRun(_ context.Context, pvID string) (store.ApprovalRun, error) {
	if s.getErr != nil {
		return store.ApprovalRun{}, s.getErr
	}
	if s.tracked != nil && s.tracked.PolicyVersionID == pvID {
		return *s.tracked, nil
	}
	// Signal resolves the CURRENT run by policy_version_id (the PK). Synthesize a
	// run so happy-path Signal tests that pass only a run_id (legacy fallback,
	// which maps run_id "run-X" → pv "pv-run-X") and tests that pass a
	// policy_version_id need not seed approval_runs. notFoundPV opts out so the
	// stale/not-current path can be exercised.
	if s.notFoundPV != "" && s.notFoundPV == pvID {
		return store.ApprovalRun{}, fmt.Errorf("not found")
	}
	// The legacy fallback maps run_id → pv as "pv-<runID>"; recover the run id.
	runID := pvID
	if len(pvID) > 3 && pvID[:3] == "pv-" {
		runID = pvID[3:]
	}
	return store.ApprovalRun{RunID: runID, PolicyVersionID: pvID, Status: "in_review"}, nil
}
func (s *stubRunStore) GetRunByRunID(_ context.Context, runID string) (store.ApprovalRun, error) {
	if s.getErr != nil {
		return store.ApprovalRun{}, s.getErr
	}
	if s.tracked != nil && s.tracked.RunID == runID {
		return *s.tracked, nil
	}
	// Synthesize a run so happy-path Signal tests need not seed approval_runs.
	return store.ApprovalRun{RunID: runID, PolicyVersionID: "pv-" + runID, Status: "in_review"}, nil
}
func (s *stubRunStore) UpdateRunStatus(_ context.Context, _ string, status string) error {
	s.runStatus = status
	return nil
}

func (s *stubRunStore) ListActiveRuns(_ context.Context) ([]store.ApprovalRun, error) {
	return s.activeRuns, nil
}

type stubSagaClient struct {
	startedRunID     string
	startedWorkflow  string
	startedInputs    map[string]any
	signaled         bool
	lastSignalName   string
	lastSignalRunID  string
	lastSignalInputs map[string]any
	publishedDef     *domain.WorkflowDefinition
	pendingTasks     []PendingTaskInfo
	lastApprover     string
	startErr         error
	publishErr       error
	signalErr        error
	listErr          error

	// cancellation tracking
	cancelledRuns       []string
	cancelledForVersion []string
	cancelActiveCount   int
	cancelRunErr        error
	cancelForVersionErr error
}

func (s *stubSagaClient) StartRun(_ context.Context, workflowID string, inputs map[string]any) (string, error) {
	if s.startErr != nil {
		return "", s.startErr
	}
	s.startedWorkflow = workflowID
	s.startedInputs = inputs
	if s.startedRunID == "" {
		s.startedRunID = "run-from-saga"
	}
	return s.startedRunID, nil
}
func (s *stubSagaClient) Signal(_ context.Context, runID, signal string, inputs map[string]any) error {
	if s.signalErr != nil {
		return s.signalErr
	}
	s.signaled = true
	s.lastSignalRunID = runID
	s.lastSignalName = signal
	s.lastSignalInputs = inputs
	return nil
}
func (s *stubSagaClient) ListPendingTasks(_ context.Context, approverUserID string) ([]PendingTaskInfo, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	s.lastApprover = approverUserID
	return s.pendingTasks, nil
}
func (s *stubSagaClient) PublishWorkflow(_ context.Context, def domain.WorkflowDefinition) error {
	if s.publishErr != nil {
		return s.publishErr
	}
	s.publishedDef = &def
	return nil
}
func (s *stubSagaClient) CancelRun(_ context.Context, runID string) error {
	if s.cancelRunErr != nil {
		return s.cancelRunErr
	}
	s.cancelledRuns = append(s.cancelledRuns, runID)
	return nil
}
func (s *stubSagaClient) CancelActiveRunsForPolicyVersion(_ context.Context, pvID string) (int, error) {
	if s.cancelForVersionErr != nil {
		return 0, s.cancelForVersionErr
	}
	s.cancelledForVersion = append(s.cancelledForVersion, pvID)
	return s.cancelActiveCount, nil
}

type stubAuditEmitter struct {
	emitted  []string
	actors   map[string]string            // action -> last actor user id
	subjects map[string]string            // action -> last subject
	attrs    map[string]map[string]string // action -> last attributes
}

func (s *stubAuditEmitter) Emit(ctx context.Context, action, subject string) {
	s.EmitActorAttrs(ctx, action, subject, "", "", nil)
}

func (s *stubAuditEmitter) EmitActor(ctx context.Context, action, subject, actorUserID string) {
	s.EmitActorAttrs(ctx, action, subject, actorUserID, "", nil)
}

func (s *stubAuditEmitter) EmitActorAttrs(_ context.Context, action, subject, actorUserID, _ string, attrs map[string]string) {
	s.emitted = append(s.emitted, action)
	if s.actors == nil {
		s.actors = map[string]string{}
	}
	if s.subjects == nil {
		s.subjects = map[string]string{}
	}
	if s.attrs == nil {
		s.attrs = map[string]map[string]string{}
	}
	s.actors[action] = actorUserID
	s.subjects[action] = subject
	s.attrs[action] = attrs
}

// stubAssignments is a minimal in-memory AssignmentStore for unit testing the
// handlers. Keyed by (pv, stage, user) — order-insensitive.
type stubAssignments struct {
	rows           map[string]store.AssignmentRow
	history        []store.HistoryEntry
	createErr      error
	getErr         error
	decideErr      error
	supersedeErr   error
	swapErr        error
	historyErr     error
	findPendingErr error
	terminateErr   error
	reassignErr    error
}

// PendingSeats returns the user's pending seats, individual first,
// synthesizing an individual seat on stage 0 when none was seeded so
// happy-path Signal tests work.
func (s *stubAssignments) PendingSeats(_ context.Context, pv, user string) ([]store.AssignmentRow, error) {
	if s.findPendingErr != nil {
		return nil, nil
	}
	var out []store.AssignmentRow
	for _, r := range s.rows {
		if r.PolicyVersionID == pv && r.UserID == user && r.State == "pending" {
			out = append(out, r)
		}
	}
	if len(out) == 0 {
		row := store.AssignmentRow{ID: "a-" + user, PolicyVersionID: pv, StageIndex: 0, UserID: user, State: "pending"}
		s.rows[keyOf(pv, 0, user)] = row
		return []store.AssignmentRow{row}, nil
	}
	slices.SortFunc(out, func(a, b store.AssignmentRow) int {
		if a.StageIndex != b.StageIndex {
			return a.StageIndex - b.StageIndex
		}
		return strings.Compare(a.GroupID, b.GroupID)
	})
	return out, nil
}

func newStubAssignments() *stubAssignments {
	return &stubAssignments{rows: map[string]store.AssignmentRow{}}
}

func keyOf(pv string, stage int, user string) string {
	return seatKey(pv, stage, user, "")
}

func seatKey(pv string, stage int, user, group string) string {
	return fmt.Sprintf("%s|%d|%s|%s", pv, stage, user, group)
}

func (s *stubAssignments) CreateAssignment(_ context.Context, row store.AssignmentRow, actor string) (store.AssignmentRow, error) {
	if s.createErr != nil {
		return store.AssignmentRow{}, s.createErr
	}
	if row.ID == "" {
		row.ID = fmt.Sprintf("a-%d", len(s.rows)+1)
	}
	row.State = "pending"
	s.rows[seatKey(row.PolicyVersionID, row.StageIndex, row.UserID, row.GroupID)] = row
	s.history = append(s.history, store.HistoryEntry{
		AssignmentID: row.ID, Event: "created", ActorUserID: actor,
	})
	return row, nil
}

func (s *stubAssignments) HasPendingAssignment(_ context.Context, pv string, stage int, user string) (bool, error) {
	for _, r := range s.rows {
		if r.PolicyVersionID == pv && r.StageIndex == stage && r.UserID == user && r.State == "pending" {
			return true, nil
		}
	}
	return false, nil
}

func (s *stubAssignments) GetAssignment(_ context.Context, pv string, stage int, user, group string) (store.AssignmentRow, error) {
	if s.getErr != nil {
		return store.AssignmentRow{}, s.getErr
	}
	r, ok := s.rows[seatKey(pv, stage, user, group)]
	if !ok {
		return store.AssignmentRow{}, fmt.Errorf("not found")
	}
	return r, nil
}

func (s *stubAssignments) ListStageAssignments(_ context.Context, pv string, stage int) ([]store.AssignmentRow, error) {
	var out []store.AssignmentRow
	for k, v := range s.rows {
		if k[:len(pv)] == pv {
			out = append(out, v)
		}
	}
	return out, nil
}

func (s *stubAssignments) CurrentStageIndex(_ context.Context, pv string) (int, bool, error) {
	min, ok := 0, false
	for _, r := range s.rows {
		if r.PolicyVersionID == pv && r.State == "pending" && (!ok || r.StageIndex < min) {
			min, ok = r.StageIndex, true
		}
	}
	return min, ok, nil
}

func (s *stubAssignments) DecideAssignment(_ context.Context, in store.DecideAssignmentInput) error {
	if s.decideErr != nil {
		return s.decideErr
	}
	for k, r := range s.rows {
		if r.ID == in.AssignmentID {
			r.State = in.Decision
			now := time.Now().UTC()
			r.DecidedAt = &now
			c := in.Comment
			r.DecidedComment = &c
			r.BulkBatchID = in.BulkBatchID
			s.rows[k] = r
			s.history = append(s.history, store.HistoryEntry{
				AssignmentID: in.AssignmentID, Event: in.Decision, ActorUserID: in.ActorUserID, Reason: in.Comment,
			})
			return nil
		}
	}
	return fmt.Errorf("assignment not found")
}

func (s *stubAssignments) SupersedePendingPeers(_ context.Context, pv string, stage int, exclude string, actor string) (int64, error) {
	if s.supersedeErr != nil {
		return 0, s.supersedeErr
	}
	var n int64
	for k, r := range s.rows {
		if r.PolicyVersionID == pv && r.StageIndex == stage && r.ID != exclude && r.State == "pending" {
			r.State = "superseded"
			s.rows[k] = r
			s.history = append(s.history, store.HistoryEntry{
				AssignmentID: r.ID, Event: "rejected", ActorUserID: actor, Reason: "stage_rejected",
			})
			n++
		}
	}
	return n, nil
}

func (s *stubAssignments) TerminatePendingAssignments(_ context.Context, pv, state, actor, reason string) (int64, error) {
	if s.terminateErr != nil {
		return 0, s.terminateErr
	}
	var n int64
	for k, r := range s.rows {
		if r.PolicyVersionID == pv && r.State == "pending" {
			r.State = state
			now := time.Now().UTC()
			r.DecidedAt = &now
			s.rows[k] = r
			s.history = append(s.history, store.HistoryEntry{
				AssignmentID: r.ID, Event: state, ActorUserID: actor, Reason: reason,
			})
			n++
		}
	}
	return n, nil
}

func (s *stubAssignments) Swap(_ context.Context, in store.SwapInput) (store.AssignmentRow, error) {
	if s.swapErr != nil {
		return store.AssignmentRow{}, s.swapErr
	}
	var oldRow store.AssignmentRow
	var oldKey string
	for k, r := range s.rows {
		if r.ID == in.OldAssignmentID {
			oldRow = r
			oldKey = k
			break
		}
	}
	if oldKey == "" {
		return store.AssignmentRow{}, fmt.Errorf("old assignment not found")
	}
	oldRow.State = "swapped_out"
	s.rows[oldKey] = oldRow
	s.history = append(s.history, store.HistoryEntry{
		AssignmentID: oldRow.ID, Event: "swapped_out", ActorUserID: in.ActorUserID,
		ActorRole: in.ActorRole, PreviousUserID: oldRow.UserID, NewUserID: in.NewUserID,
		OutOfEligibility: in.OutOfEligibility, Reason: in.Reason,
	})

	newRow := store.AssignmentRow{
		ID:              fmt.Sprintf("a-%d", len(s.rows)+1),
		PolicyVersionID: oldRow.PolicyVersionID,
		StageIndex:      oldRow.StageIndex,
		UserID:          in.NewUserID,
		GroupID:         oldRow.GroupID,
		SLADeadlineAt:   in.NewSLADeadlineAt,
		ReminderAt:      in.NewReminderAt,
		State:           "pending",
	}
	s.rows[seatKey(newRow.PolicyVersionID, newRow.StageIndex, newRow.UserID, newRow.GroupID)] = newRow
	s.history = append(s.history, store.HistoryEntry{
		AssignmentID: newRow.ID, Event: "swapped_in", ActorUserID: in.ActorUserID,
		ActorRole: in.ActorRole, PreviousUserID: oldRow.UserID, NewUserID: in.NewUserID,
		OutOfEligibility: in.OutOfEligibility, Reason: in.Reason,
	})
	return newRow, nil
}

func (s *stubAssignments) ListHistory(_ context.Context, pv string, stage int) ([]store.HistoryEntry, error) {
	if s.historyErr != nil {
		return nil, s.historyErr
	}
	// All history for any assignment in the stage.
	wantIDs := map[string]bool{}
	for _, r := range s.rows {
		if r.PolicyVersionID == pv && r.StageIndex == stage {
			wantIDs[r.ID] = true
		}
	}
	out := []store.HistoryEntry{}
	for _, h := range s.history {
		if wantIDs[h.AssignmentID] {
			out = append(out, h)
		}
	}
	return out, nil
}

func (s *stubAssignments) ListActiveRunsByDef(_ context.Context, _ string, _ int) ([]store.ApprovalRun, error) {
	// Default stub: no active runs (allows GC).
	return nil, nil
}

// ReassignUserItems models the merge re-point over the in-memory rows: it moves
// pending/paused seats from->to, deduping when the target already holds a seat
// on that (pv, stage). Run submitter re-point is exercised by the store tests.
func (s *stubAssignments) ReassignUserItems(_ context.Context, from, to, actor string, dryRun bool) (store.ReassignResult, error) {
	if s.reassignErr != nil {
		return store.ReassignResult{}, s.reassignErr
	}
	var res store.ReassignResult
	for k, r := range s.rows {
		if r.UserID != from || (r.State != "pending" && r.State != "paused") {
			continue
		}
		_, targetHas := s.rows[seatKey(r.PolicyVersionID, r.StageIndex, to, r.GroupID)]
		if targetHas {
			res.AssignmentsDeduped++
			if !dryRun {
				r.State = "swapped_out"
				s.rows[k] = r
				s.history = append(s.history, store.HistoryEntry{
					AssignmentID: r.ID, Event: "reassigned", ActorUserID: actor, PreviousUserID: from, NewUserID: to,
				})
			}
			continue
		}
		res.AssignmentsReassigned++
		if !dryRun {
			delete(s.rows, k)
			r.UserID = to
			s.rows[seatKey(r.PolicyVersionID, r.StageIndex, to, r.GroupID)] = r
			s.history = append(s.history, store.HistoryEntry{
				AssignmentID: r.ID, Event: "reassigned", ActorUserID: actor, PreviousUserID: from, NewUserID: to,
			})
		}
	}
	return res, nil
}

// --- helpers ---

func newTestServer() *WorkflowServer {
	def := builder.WorkflowDef{
		ID: "def-1", Name: "Test", Version: 1, AuthorUserID: "author-1",
		Stages: []builder.Stage{
			{Name: "s", Quorum: builder.QuorumAny, ApproverIDs: []string{"u1", "u2", "u3"}},
		},
	}
	return NewWorkflowServer(WorkflowServerOpts{
		Resolver:    &stubWorkflowResolver{defID: "def-1"},
		DefStore:    &stubWorkflowDefStore{wd: def},
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

// Silence unused import — domain is used by stubSagaClient.PublishWorkflow.
var _ domain.WorkflowDefinition

// --- tests ---

func TestSubmit_StartsSagaAndTracksRun(t *testing.T) {
	srv := newTestServer()
	resp, err := srv.Submit(context.Background(), &workflowv1.SubmitRequest{
		PolicyVersionId:     "pv-1",
		PolicyId:            "pol-1",
		CategoryId:          "g1",
		SubmittedBy:         "user-1",
		AncestorCategoryIds: []string{"g1", "root"},
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if resp.RunId == "" {
		t.Fatal("expected non-empty RunId")
	}
	rs := srv.runStore.(*stubRunStore)
	if rs.tracked == nil {
		t.Fatal("expected run to be tracked")
	}
	if rs.tracked.PolicyVersionID != "pv-1" {
		t.Fatalf("PolicyVersionID: %q", rs.tracked.PolicyVersionID)
	}
	if rs.tracked.RunID != resp.RunId {
		t.Fatalf("tracked RunID %q != response RunId %q", rs.tracked.RunID, resp.RunId)
	}
	if rs.tracked.WorkflowDefID != "def-1" {
		t.Fatalf("WorkflowDefID: %q", rs.tracked.WorkflowDefID)
	}
	if rs.tracked.WorkflowVersion != 1 {
		t.Fatalf("WorkflowVersion: %d", rs.tracked.WorkflowVersion)
	}
	if rs.tracked.Status != "in_review" {
		t.Fatalf("Status: %q", rs.tracked.Status)
	}

	// Resolver received ancestor IDs.
	res := srv.resolver.(*stubWorkflowResolver)
	if res.lastPolicyID != "pol-1" {
		t.Fatalf("resolver got policy_id %q", res.lastPolicyID)
	}
	if len(res.lastAncestorIDs) != 2 || res.lastAncestorIDs[0] != "g1" {
		t.Fatalf("resolver got ancestor_group_ids %v", res.lastAncestorIDs)
	}

	// Saga client received the compiled workflow + inputs with policy fields.
	sc := srv.sagaClient.(*stubSagaClient)
	if sc.publishedDef == nil {
		t.Fatal("expected PublishWorkflow to be called")
	}
	if sc.startedWorkflow != sc.publishedDef.ID {
		t.Fatalf("StartRun workflowID %q != published def ID %q", sc.startedWorkflow, sc.publishedDef.ID)
	}
	if sc.startedInputs["policy_version_id"] != "pv-1" {
		t.Fatalf("inputs[policy_version_id] = %v", sc.startedInputs["policy_version_id"])
	}
	if sc.startedInputs["policy_id"] != "pol-1" {
		t.Fatalf("inputs[policy_id] = %v", sc.startedInputs["policy_id"])
	}
	if sc.startedInputs["group_id"] != "g1" {
		t.Fatalf("inputs[group_id] = %v", sc.startedInputs["group_id"])
	}
	if sc.startedInputs["submitted_by"] != "user-1" {
		t.Fatalf("inputs[submitted_by] = %v", sc.startedInputs["submitted_by"])
	}

	// Audit emitted.
	ae := srv.auditEmit.(*stubAuditEmitter)
	if len(ae.emitted) == 0 || ae.emitted[0] != "workflow.submitted" {
		t.Fatalf("audit emissions: %v", ae.emitted)
	}
}

func TestSubmit_NoWorkflowConfigured_FailedPrecondition(t *testing.T) {
	srv := newTestServer()
	srv.resolver = &stubWorkflowResolver{defID: ""} // no workflow resolved/inherited

	resp, err := srv.Submit(context.Background(), &workflowv1.SubmitRequest{
		PolicyVersionId:     "pv-2",
		PolicyId:            "pol-2",
		AncestorCategoryIds: []string{"g1"},
	})
	// A policy whose group has no attached/inherited workflow cannot be submitted —
	// the server rejects rather than silently publishing without review.
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got resp=%v err=%v", resp, err)
	}
	sc := srv.sagaClient.(*stubSagaClient)
	if sc.publishedDef != nil {
		t.Fatal("did not expect PublishWorkflow when no workflow configured")
	}
	if sc.startedWorkflow != "" {
		t.Fatal("did not expect StartRun when no workflow configured")
	}
	rs := srv.runStore.(*stubRunStore)
	if rs.tracked != nil {
		t.Fatal("did not expect run to be tracked when no workflow")
	}
	ae := srv.auditEmit.(*stubAuditEmitter)
	if len(ae.emitted) == 0 || ae.emitted[0] != "workflow.submit.no_workflow" {
		t.Fatalf("audit emissions: %v", ae.emitted)
	}
}

func TestResolveWorkflow_DelegatesToResolver(t *testing.T) {
	srv := newTestServer()
	srv.resolver = &stubWorkflowResolver{defID: "def-eff"}

	// Workflow attached/inherited → returns its id (submittable).
	resp, err := srv.ResolveWorkflow(context.Background(), &workflowv1.ResolveWorkflowRequest{
		PolicyId:            "pol-1",
		AncestorCategoryIds: []string{"leaf", "root"},
	})
	if err != nil {
		t.Fatalf("ResolveWorkflow: %v", err)
	}
	if resp.GetWorkflowDefId() != "def-eff" {
		t.Fatalf("WorkflowDefId: %q", resp.GetWorkflowDefId())
	}
	res := srv.resolver.(*stubWorkflowResolver)
	if res.lastPolicyID != "pol-1" || len(res.lastAncestorIDs) != 2 {
		t.Fatalf("resolver not called with the lineage: policy=%q ancestors=%v", res.lastPolicyID, res.lastAncestorIDs)
	}

	// No workflow anywhere up the chain → empty id (not submittable). It must NOT
	// start a run — ResolveWorkflow is read-only.
	srv.resolver = &stubWorkflowResolver{defID: ""}
	resp, err = srv.ResolveWorkflow(context.Background(), &workflowv1.ResolveWorkflowRequest{PolicyId: "pol-2"})
	if err != nil {
		t.Fatalf("ResolveWorkflow(empty): %v", err)
	}
	if resp.GetWorkflowDefId() != "" {
		t.Fatalf("expected empty WorkflowDefId, got %q", resp.GetWorkflowDefId())
	}
	if srv.runStore.(*stubRunStore).tracked != nil {
		t.Fatal("ResolveWorkflow must not start/track a run")
	}
}

func TestSubmit_PassesApproverListsAsInputs(t *testing.T) {
	srv := newTestServer()
	// newTestServer has stage with ApproverIDs: ["u1","u2","u3"].

	_, err := srv.Submit(context.Background(), &workflowv1.SubmitRequest{
		PolicyVersionId:     "pv-3",
		PolicyId:            "pol-3",
		AncestorCategoryIds: []string{"g"},
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	sc := srv.sagaClient.(*stubSagaClient)
	got, ok := sc.startedInputs["approvers_s0"].([]string)
	if !ok {
		t.Fatalf("inputs[approvers_s0] type %T", sc.startedInputs["approvers_s0"])
	}
	if len(got) != 3 || got[0] != "u1" || got[1] != "u2" || got[2] != "u3" {
		t.Fatalf("inputs[approvers_s0] = %v", got)
	}
	// The old per-group keys must not be present.
	if _, present := sc.startedInputs["approvers_s0_g0"]; present {
		t.Fatal("expected per-group key approvers_s0_g0 to be absent")
	}
}

// TestSubmit_ApproverIDsPassedAsPoolNotAssignedDirectly asserts that Submit
// resolves stage 0's ApproverIDs into the "approvers_s0" run input but does NOT
// create approval_assignments rows itself. Assignment creation now happens in
// the saga's policy.assign_stage action (s0_assign, which is the run's Start), so
// every stage — including stage 0 — is assigned by the same code path. This is
// the fix for the multi-stage stall: stage 0 was the only stage Submit assigned,
// leaving later stages with no rows. SLA-timing behavior is covered by the
// StageAssigner unit tests and builder.CompileOptions.StageSLA.
func TestSubmit_ApproverIDsPassedAsPoolNotAssignedDirectly(t *testing.T) {
	const slaDays = 3
	srv := newTestServer()
	// Override the def to have ApproverIDs with a per-stage SLA.
	srv.defStore = &stubWorkflowDefStore{wd: builder.WorkflowDef{
		ID: "def-sla", Name: "SLA test", Version: 1,
		Stages: []builder.Stage{{
			Name: "review", Quorum: builder.QuorumAny,
			ApproverIDs: []string{"ua", "ub"},
			SLADays:     slaDays,
		}},
	}}
	srv.resolver = &stubWorkflowResolver{defID: "def-sla"}

	_, err := srv.Submit(context.Background(), &workflowv1.SubmitRequest{
		PolicyVersionId:     "pv-sla",
		PolicyId:            "pol-sla",
		AncestorCategoryIds: []string{"g"},
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// Submit must NOT create assignments directly — that is now the saga's job.
	as := srv.assignments.(*stubAssignments)
	for _, uid := range []string{"ua", "ub"} {
		if _, ok := as.rows[keyOf("pv-sla", 0, uid)]; ok {
			t.Fatalf("Submit created an assignment for %q directly; assignment creation moved to policy.assign_stage", uid)
		}
	}

	// The resolved pool must be handed to the saga as approvers_s0 (the assign
	// action reads it), not a per-group key.
	sc := srv.sagaClient.(*stubSagaClient)
	pool, ok := sc.startedInputs["approvers_s0"].([]string)
	if !ok || len(pool) != 2 {
		t.Fatalf("approvers_s0 = %v", sc.startedInputs["approvers_s0"])
	}
}

func TestSubmit_NofMMisconfiguredAtStageStart(t *testing.T) {
	// With user-based ApproverIDs, QuorumN > len(ApproverIDs) is caught statically
	// by WorkflowDef.Validate() inside Compile(), so Submit returns a compile error.
	// The runtime audit path (workflow.stage.misconfigured) fires for the edge case
	// where a stored def somehow has QuorumN > current pool — impossible with the
	// static check, but we preserve the runtime guard and test the static path.
	srv := newTestServer()
	srv.defStore = &stubWorkflowDefStore{wd: builder.WorkflowDef{
		ID: "def-1", Name: "x", Version: 1,
		Stages: []builder.Stage{{
			Name: "s", Quorum: builder.QuorumNofM, QuorumN: 5,
			ApproverIDs: []string{"u1"},
		}},
	}}

	_, err := srv.Submit(context.Background(), &workflowv1.SubmitRequest{
		PolicyVersionId:     "pv-bad",
		PolicyId:            "pol-bad",
		AncestorCategoryIds: []string{"g"},
	})
	// Compile rejects the def (QuorumN=5 > len(ApproverIDs)=1) — Submit must fail.
	if err == nil {
		t.Fatal("expected error for nofm with N > pool")
	}
}

func TestSubmit_UnstaffedStage_FailedPrecondition(t *testing.T) {
	// an empty-approver stage is a valid DRAFT (compiles), but a run
	// must NOT start against it — Submit refuses with FailedPrecondition so an
	// unusable workflow can never run.
	srv := newTestServer()
	srv.defStore = &stubWorkflowDefStore{wd: builder.WorkflowDef{
		ID: "def-draft", Name: "x", Version: 1,
		Stages: []builder.Stage{{Name: "unstaffed", Quorum: builder.QuorumAny}}, // no approvers
	}}

	_, err := srv.Submit(context.Background(), &workflowv1.SubmitRequest{
		PolicyVersionId:     "pv-draft",
		PolicyId:            "pol-draft",
		AncestorCategoryIds: []string{"g"},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition for an unstaffed stage, got %v", err)
	}
}

func TestSubmit_ResolveError(t *testing.T) {
	srv := newTestServer()
	srv.resolver = &stubWorkflowResolver{err: fmt.Errorf("boom")}
	_, err := srv.Submit(context.Background(), &workflowv1.SubmitRequest{PolicyVersionId: "pv-x"})
	if err == nil {
		t.Fatal("expected error from resolver")
	}
}

func TestSignal_DeliversToSagaAndAudits(t *testing.T) {
	srv := newTestServer()
	_, err := srv.Signal(context.Background(), &workflowv1.SignalRequest{
		RunId:       "run-1",
		TaskId:      "task-1",
		Signal:      workflowv1.SignalType_SIGNAL_TYPE_APPROVE,
		ActorUserId: "u1",
		Comment:     "lgtm",
	})
	if err != nil {
		t.Fatalf("Signal: %v", err)
	}
	sc := srv.sagaClient.(*stubSagaClient)
	if !sc.signaled {
		t.Fatal("expected saga Signal to be called")
	}
	if sc.lastSignalRunID != "run-1" {
		t.Fatalf("signal run id: %q", sc.lastSignalRunID)
	}
	if sc.lastSignalName != "approve" {
		t.Fatalf("signal name: %q", sc.lastSignalName)
	}
	if sc.lastSignalInputs["task_id"] != "task-1" {
		t.Fatalf("signal inputs task_id: %v", sc.lastSignalInputs["task_id"])
	}
	if sc.lastSignalInputs["actor_user_id"] != "u1" {
		t.Fatalf("signal inputs actor: %v", sc.lastSignalInputs["actor_user_id"])
	}
	if sc.lastSignalInputs["comment"] != "lgtm" {
		t.Fatalf("signal inputs comment: %v", sc.lastSignalInputs["comment"])
	}
	ae := srv.auditEmit.(*stubAuditEmitter)
	// Recording the decision emits workflow.decision.approved; unpausing the saga
	// then emits workflow.signal.approve.
	if !slices.Contains(ae.emitted, "workflow.decision.approved") || !slices.Contains(ae.emitted, "workflow.signal.approve") {
		t.Fatalf("audit emissions: %v", ae.emitted)
	}
	// The signal audit record must carry a clean subject ("policy_version:<uuid>",
	// never "run:<id>" or a stage mashed in), the acting user, and the run id in
	// attributes.
	if got := ae.subjects["workflow.signal.approve"]; got != "policy_version:pv-run-1" {
		t.Fatalf("signal subject: %q", got)
	}
	if got := ae.actors["workflow.signal.approve"]; got != "u1" {
		t.Fatalf("signal actor: %q", got)
	}
	if got := ae.attrs["workflow.signal.approve"]["run_id"]; got != "run-1" {
		t.Fatalf("signal run_id attr: %q", got)
	}
	// The decision audit record must carry the clean subject with the stage in
	// attributes (not mashed into the subject) and the deciding user.
	if got := ae.subjects["workflow.decision.approved"]; got != "policy_version:pv-run-1" {
		t.Fatalf("decision subject: %q", got)
	}
	if got := ae.actors["workflow.decision.approved"]; got != "u1" {
		t.Fatalf("decision actor: %q", got)
	}
	if got := ae.attrs["workflow.decision.approved"]["stage"]; got != "0" {
		t.Fatalf("decision stage attr: %q", got)
	}
}

func TestSignal_RecordsApproveDecision(t *testing.T) {
	srv := newTestServer()
	as := srv.assignments.(*stubAssignments)
	// Seed a pending assignment for the approver at stage 0.
	as.rows[keyOf("pv-run-9", 0, "u-app")] = store.AssignmentRow{
		ID: "asg-1", PolicyVersionID: "pv-run-9", StageIndex: 0, UserID: "u-app", State: "pending",
	}
	_, err := srv.Signal(context.Background(), &workflowv1.SignalRequest{
		RunId: "run-9", Signal: workflowv1.SignalType_SIGNAL_TYPE_APPROVE, ActorUserId: "u-app", Comment: "ok",
	})
	if err != nil {
		t.Fatalf("Signal: %v", err)
	}
	got := as.rows[keyOf("pv-run-9", 0, "u-app")]
	if got.State != "approved" {
		t.Fatalf("assignment state = %q, want approved", got.State)
	}
	if got.DecidedComment == nil || *got.DecidedComment != "ok" {
		t.Fatalf("decided comment not recorded: %+v", got.DecidedComment)
	}
}

func TestSignal_RejectSupersedesPeers(t *testing.T) {
	srv := newTestServer()
	as := srv.assignments.(*stubAssignments)
	as.rows[keyOf("pv-run-9", 0, "u-rej")] = store.AssignmentRow{
		ID: "asg-r", PolicyVersionID: "pv-run-9", StageIndex: 0, UserID: "u-rej", State: "pending",
	}
	as.rows[keyOf("pv-run-9", 0, "u-peer")] = store.AssignmentRow{
		ID: "asg-p", PolicyVersionID: "pv-run-9", StageIndex: 0, UserID: "u-peer", State: "pending",
	}
	_, err := srv.Signal(context.Background(), &workflowv1.SignalRequest{
		RunId: "run-9", Signal: workflowv1.SignalType_SIGNAL_TYPE_REJECT, ActorUserId: "u-rej", Comment: "no",
	})
	if err != nil {
		t.Fatalf("Signal: %v", err)
	}
	if got := as.rows[keyOf("pv-run-9", 0, "u-rej")]; got.State != "rejected" {
		t.Fatalf("rejecter state = %q, want rejected", got.State)
	}
	if got := as.rows[keyOf("pv-run-9", 0, "u-peer")]; got.State != "superseded" {
		t.Fatalf("peer state = %q, want superseded", got.State)
	}
}

func TestSignal_NoPendingAssignment(t *testing.T) {
	srv := newTestServer()
	srv.assignments.(*stubAssignments).findPendingErr = fmt.Errorf("no rows")
	_, err := srv.Signal(context.Background(), &workflowv1.SignalRequest{
		RunId: "run-9", Signal: workflowv1.SignalType_SIGNAL_TYPE_APPROVE, ActorUserId: "ghost", Comment: "x",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("want FailedPrecondition for missing assignment, got %v", err)
	}
}

func TestSignal_RequiresCommentForApprove(t *testing.T) {
	srv := newTestServer()
	_, err := srv.Signal(context.Background(), &workflowv1.SignalRequest{
		RunId: "r", Signal: workflowv1.SignalType_SIGNAL_TYPE_APPROVE,
	})
	if err == nil {
		t.Fatal("expected InvalidArgument for missing comment")
	}
}

func TestSignal_RequiresCommentForReject(t *testing.T) {
	srv := newTestServer()
	_, err := srv.Signal(context.Background(), &workflowv1.SignalRequest{
		RunId: "r", Signal: workflowv1.SignalType_SIGNAL_TYPE_REJECT,
	})
	if err == nil {
		t.Fatal("expected InvalidArgument for missing comment")
	}
}

func TestSignal_MapsAllSignalTypes(t *testing.T) {
	cases := []struct {
		in   workflowv1.SignalType
		want string
	}{
		{workflowv1.SignalType_SIGNAL_TYPE_APPROVE, "approve"},
		{workflowv1.SignalType_SIGNAL_TYPE_REJECT, "reject"},
		{workflowv1.SignalType_SIGNAL_TYPE_REQUEST_CHANGES, "request_changes"},
		{workflowv1.SignalType_SIGNAL_TYPE_WITHDRAW, "withdraw"},
		{workflowv1.SignalType_SIGNAL_TYPE_RETIRE, "retire"},
		{workflowv1.SignalType_SIGNAL_TYPE_UNSPECIFIED, "unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			if got := signalName(tc.in); got != tc.want {
				t.Fatalf("signalName(%v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestSignal_SagaError(t *testing.T) {
	srv := newTestServer()
	srv.sagaClient = &stubSagaClient{signalErr: fmt.Errorf("conn refused")}
	_, err := srv.Signal(context.Background(), &workflowv1.SignalRequest{
		RunId:   "run-1",
		Signal:  workflowv1.SignalType_SIGNAL_TYPE_APPROVE,
		Comment: "lgtm",
	})
	if err == nil {
		t.Fatal("expected error from saga signal")
	}
}

func TestGetStatus_ReturnsTrackedStatus(t *testing.T) {
	srv := newTestServer()
	rs := srv.runStore.(*stubRunStore)
	rs.tracked = &store.ApprovalRun{PolicyVersionID: "pv-2", RunID: "run-x", WorkflowDefID: "def-1", Status: "in_review"}

	resp, err := srv.GetStatus(context.Background(), &workflowv1.GetStatusRequest{PolicyVersionId: "pv-2"})
	if err != nil {
		t.Fatalf("GetStatus: %v", err)
	}
	if resp.RunId != "run-x" {
		t.Fatalf("RunId: %q", resp.RunId)
	}
	if resp.Status != workflowv1.ApprovalStatus_APPROVAL_STATUS_IN_REVIEW {
		t.Fatalf("Status: %v", resp.Status)
	}
	// Stage names come from the run's resolved workflow def (one stage "s").
	if got := resp.GetStageNames(); len(got) != 1 || got[0] != "s" {
		t.Fatalf("StageNames: %v", got)
	}
}

func TestGetStatus_CurrentStageIdxFromPendingStage(t *testing.T) {
	srv := newTestServer()
	srv.runStore.(*stubRunStore).tracked = &store.ApprovalRun{
		PolicyVersionID: "pv-3", RunID: "run-y", WorkflowDefID: "def-1", Status: "in_review",
	}
	// The run is awaiting stage 1 (a pending stage-1 assignment). Before the fix
	// GetStatus never set CurrentStageIdx, so it was hardcoded 0 ("stage 1").
	sa := srv.assignments.(*stubAssignments)
	if _, err := sa.CreateAssignment(context.Background(),
		store.AssignmentRow{PolicyVersionID: "pv-3", StageIndex: 1, UserID: "u1"}, "sys"); err != nil {
		t.Fatalf("seed assignment: %v", err)
	}

	resp, err := srv.GetStatus(context.Background(), &workflowv1.GetStatusRequest{PolicyVersionId: "pv-3"})
	if err != nil {
		t.Fatalf("GetStatus: %v", err)
	}
	if resp.GetCurrentStageIdx() != 1 {
		t.Fatalf("CurrentStageIdx = %d, want 1 (awaiting stage 1)", resp.GetCurrentStageIdx())
	}
}

func TestGetStatus_NotFound(t *testing.T) {
	srv := newTestServer()
	srv.runStore = &stubRunStore{getErr: fmt.Errorf("not found")}
	_, err := srv.GetStatus(context.Background(), &workflowv1.GetStatusRequest{PolicyVersionId: "missing"})
	if err == nil {
		t.Fatal("expected error from missing run")
	}
}

func TestStatusToProto_AllKnownValues(t *testing.T) {
	cases := map[string]workflowv1.ApprovalStatus{
		"draft":      workflowv1.ApprovalStatus_APPROVAL_STATUS_DRAFT,
		"in_review":  workflowv1.ApprovalStatus_APPROVAL_STATUS_IN_REVIEW,
		"approved":   workflowv1.ApprovalStatus_APPROVAL_STATUS_APPROVED,
		"scheduled":  workflowv1.ApprovalStatus_APPROVAL_STATUS_SCHEDULED,
		"published":  workflowv1.ApprovalStatus_APPROVAL_STATUS_PUBLISHED,
		"superseded": workflowv1.ApprovalStatus_APPROVAL_STATUS_SUPERSEDED,
		"rejected":   workflowv1.ApprovalStatus_APPROVAL_STATUS_REJECTED,
		"withdrawn":  workflowv1.ApprovalStatus_APPROVAL_STATUS_WITHDRAWN,
		"archived":   workflowv1.ApprovalStatus_APPROVAL_STATUS_ARCHIVED,
	}
	for in, want := range cases {
		if got := statusToProto(in); got != want {
			t.Errorf("statusToProto(%q) = %v, want %v", in, got, want)
		}
	}
	if got := statusToProto("garbage"); got != workflowv1.ApprovalStatus_APPROVAL_STATUS_UNSPECIFIED {
		t.Errorf("statusToProto(garbage) = %v, want UNSPECIFIED", got)
	}
}

func TestListPendingTasks_ReturnsConvertedTasks(t *testing.T) {
	srv := newTestServer()
	srv.sagaClient = &stubSagaClient{pendingTasks: []PendingTaskInfo{
		{TaskID: "t1", RunID: "r1", PolicyVersionID: "pv1", StageIndex: 0},
		{TaskID: "t2", RunID: "r2", PolicyVersionID: "pv2", StageIndex: 1},
	}}
	// Seed pending assignments so u1 owns both tasks (the inbox now filters by
	// the authoritative approval_assignments store).
	as := srv.assignments.(*stubAssignments)
	as.rows[keyOf("pv1", 0, "u1")] = store.AssignmentRow{ID: "a-pv1", PolicyVersionID: "pv1", StageIndex: 0, UserID: "u1", State: "pending"}
	as.rows[keyOf("pv2", 1, "u1")] = store.AssignmentRow{ID: "a-pv2", PolicyVersionID: "pv2", StageIndex: 1, UserID: "u1", State: "pending"}

	resp, err := srv.ListPendingTasks(context.Background(), &workflowv1.ListPendingTasksRequest{ApproverUserId: "u1"})
	if err != nil {
		t.Fatalf("ListPendingTasks: %v", err)
	}
	if len(resp.Tasks) != 2 {
		t.Fatalf("len(tasks) = %d", len(resp.Tasks))
	}
	if resp.Tasks[0].TaskId != "t1" || resp.Tasks[1].TaskId != "t2" {
		t.Fatalf("task ids: %q %q", resp.Tasks[0].TaskId, resp.Tasks[1].TaskId)
	}
	if resp.Tasks[1].StageIndex != 1 {
		t.Fatalf("stage index: %d", resp.Tasks[1].StageIndex)
	}
	if srv.sagaClient.(*stubSagaClient).lastApprover != "u1" {
		t.Fatalf("expected approver id to be forwarded")
	}
}

func TestListPendingTasks_Empty(t *testing.T) {
	srv := newTestServer()
	resp, err := srv.ListPendingTasks(context.Background(), &workflowv1.ListPendingTasksRequest{ApproverUserId: "u1"})
	if err != nil {
		t.Fatalf("ListPendingTasks: %v", err)
	}
	if len(resp.Tasks) != 0 {
		t.Fatalf("expected no tasks, got %d", len(resp.Tasks))
	}
}

func TestListPendingTasks_SagaError(t *testing.T) {
	srv := newTestServer()
	srv.sagaClient = &stubSagaClient{listErr: fmt.Errorf("rpc fail")}
	_, err := srv.ListPendingTasks(context.Background(), &workflowv1.ListPendingTasksRequest{ApproverUserId: "u1"})
	if err == nil {
		t.Fatal("expected error from saga client")
	}
}

// TestSubmit_CancelsPriorRunsBeforeStarting encodes the no-accumulation
// requirement at the handler seam: every Submit cancels any active runs for the
// version (CancelActiveRunsForPolicyVersion) BEFORE starting a fresh one, so a
// re-submit can never pile up paused runs.
func TestSubmit_CancelsPriorRunsBeforeStarting(t *testing.T) {
	srv := newTestServer()
	sc := srv.sagaClient.(*stubSagaClient)
	sc.cancelActiveCount = 6 // simulate 6 orphaned paused runs for this version

	_, err := srv.Submit(context.Background(), &workflowv1.SubmitRequest{
		PolicyVersionId:     "pv-resubmit",
		PolicyId:            "pol-1",
		AncestorCategoryIds: []string{"g"},
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if len(sc.cancelledForVersion) != 1 || sc.cancelledForVersion[0] != "pv-resubmit" {
		t.Fatalf("expected one cancel-for-version call for pv-resubmit, got %v", sc.cancelledForVersion)
	}
	if sc.startedWorkflow == "" {
		t.Fatal("expected a fresh run to be started after cancelling priors")
	}
}

// TestSubmit_CancelPriorRunsError surfaces a cancellation failure rather than
// silently starting a duplicate run.
func TestSubmit_CancelPriorRunsError(t *testing.T) {
	srv := newTestServer()
	srv.sagaClient = &stubSagaClient{cancelForVersionErr: fmt.Errorf("store down")}
	_, err := srv.Submit(context.Background(), &workflowv1.SubmitRequest{
		PolicyVersionId: "pv-x", PolicyId: "pol-1", AncestorCategoryIds: []string{"g"},
	})
	if err == nil {
		t.Fatal("expected error when cancelling prior runs fails")
	}
	if sc := srv.sagaClient.(*stubSagaClient); sc.startedWorkflow != "" {
		t.Fatal("must not start a run when prior-run cancellation failed")
	}
}

// TestSignal_WithdrawCancelsSagaRun encodes that withdraw drives the saga run
// terminal (CancelRun) so its task leaves the inbox — the engine has no
// withdraw step, so without this the run would stay paused forever.
func TestSignal_WithdrawCancelsSagaRun(t *testing.T) {
	srv := newTestServer()
	srv.coreStatus = &stubCoreStatus{}
	rs := srv.runStore.(*stubRunStore)
	rs.tracked = &store.ApprovalRun{RunID: "run-w", PolicyVersionID: "pv-w", Status: "in_review"}

	_, err := srv.Signal(context.Background(), &workflowv1.SignalRequest{
		RunId: "run-w", Signal: workflowv1.SignalType_SIGNAL_TYPE_WITHDRAW, ActorUserId: "owner",
	})
	if err != nil {
		t.Fatalf("Signal(withdraw): %v", err)
	}
	sc := srv.sagaClient.(*stubSagaClient)
	if len(sc.cancelledRuns) != 1 || sc.cancelledRuns[0] != "run-w" {
		t.Fatalf("expected withdraw to cancel run-w, got %v", sc.cancelledRuns)
	}
}

// TestSignal_RejectEnsuresRunTerminal encodes the reject-terminates requirement:
// after a reject signal the handler calls CancelRun to guarantee the run is not
// left paused in the inbox.
func TestSignal_RejectEnsuresRunTerminal(t *testing.T) {
	srv := newTestServer()
	as := srv.assignments.(*stubAssignments)
	as.rows[keyOf("pv-rej", 0, "u-rej")] = store.AssignmentRow{
		ID: "asg-r", PolicyVersionID: "pv-rej", StageIndex: 0, UserID: "u-rej", State: "pending",
	}
	rs := srv.runStore.(*stubRunStore)
	rs.tracked = &store.ApprovalRun{RunID: "run-rej", PolicyVersionID: "pv-rej", Status: "in_review"}

	_, err := srv.Signal(context.Background(), &workflowv1.SignalRequest{
		RunId: "run-rej", Signal: workflowv1.SignalType_SIGNAL_TYPE_REJECT, ActorUserId: "u-rej", Comment: "no",
	})
	if err != nil {
		t.Fatalf("Signal(reject): %v", err)
	}
	sc := srv.sagaClient.(*stubSagaClient)
	if len(sc.cancelledRuns) != 1 || sc.cancelledRuns[0] != "run-rej" {
		t.Fatalf("expected reject to ensure run-rej terminal, got %v", sc.cancelledRuns)
	}
}

// TestSignal_ApproveDoesNotCancelRun confirms approve does NOT force-cancel —
// an approved run completes naturally; only reject/withdraw cancel.
func TestSignal_ApproveDoesNotCancelRun(t *testing.T) {
	srv := newTestServer()
	_, err := srv.Signal(context.Background(), &workflowv1.SignalRequest{
		RunId: "run-a", TaskId: "t", Signal: workflowv1.SignalType_SIGNAL_TYPE_APPROVE, ActorUserId: "u1", Comment: "ok",
	})
	if err != nil {
		t.Fatalf("Signal(approve): %v", err)
	}
	if sc := srv.sagaClient.(*stubSagaClient); len(sc.cancelledRuns) != 0 {
		t.Fatalf("approve must not cancel the run, got %v", sc.cancelledRuns)
	}
}

// TestListPendingTasks_FiltersByApprover encodes the per-approver inbox: when
// the saga returns two group-level tasks but only one has a pending assignment
// for the calling approver, only that one is returned.
func TestListPendingTasks_FiltersByApprover(t *testing.T) {
	srv := newTestServer()
	srv.sagaClient = &stubSagaClient{pendingTasks: []PendingTaskInfo{
		{TaskID: "tA", RunID: "rA", PolicyVersionID: "pvA", StageIndex: 0}, // owned by approver A
		{TaskID: "tB", RunID: "rB", PolicyVersionID: "pvB", StageIndex: 0}, // owned by approver B
	}}
	as := srv.assignments.(*stubAssignments)
	as.rows[keyOf("pvA", 0, "approverA")] = store.AssignmentRow{ID: "aA", PolicyVersionID: "pvA", StageIndex: 0, UserID: "approverA", State: "pending"}
	as.rows[keyOf("pvB", 0, "approverB")] = store.AssignmentRow{ID: "aB", PolicyVersionID: "pvB", StageIndex: 0, UserID: "approverB", State: "pending"}

	respA, err := srv.ListPendingTasks(context.Background(), &workflowv1.ListPendingTasksRequest{ApproverUserId: "approverA"})
	if err != nil {
		t.Fatalf("ListPendingTasks(A): %v", err)
	}
	if len(respA.Tasks) != 1 || respA.Tasks[0].PolicyVersionId != "pvA" {
		t.Fatalf("approver A should see only pvA, got %+v", respA.Tasks)
	}

	respB, err := srv.ListPendingTasks(context.Background(), &workflowv1.ListPendingTasksRequest{ApproverUserId: "approverB"})
	if err != nil {
		t.Fatalf("ListPendingTasks(B): %v", err)
	}
	if len(respB.Tasks) != 1 || respB.Tasks[0].PolicyVersionId != "pvB" {
		t.Fatalf("approver B should see only pvB, got %+v", respB.Tasks)
	}
}

func TestNewWorkflowServer_WiresFields(t *testing.T) {
	res := &stubWorkflowResolver{}
	ds := &stubWorkflowDefStore{}
	rs := &stubRunStore{}
	sc := &stubSagaClient{}
	ae := &stubAuditEmitter{}
	as := newStubAssignments()
	opts := builder.CompileOptions{StageSLAHours: 24, StageReminderHours: 12}

	srv := NewWorkflowServer(WorkflowServerOpts{
		Resolver: res, DefStore: ds, RunStore: rs, SagaClient: sc, AuditEmit: ae,
		Assignments: as, CompileOpts: opts,
	})
	if srv == nil {
		t.Fatal("nil server")
	}
	if srv.resolver != res || srv.defStore != ds || srv.runStore != rs ||
		srv.sagaClient != sc || srv.auditEmit != ae ||
		srv.assignments != as {
		t.Fatal("dependencies not wired through")
	}
	if srv.compileOpts != opts {
		t.Fatalf("compileOpts: %+v", srv.compileOpts)
	}
}

// --- Swap handler ---

func setupServerForSwap(t *testing.T) (*WorkflowServer, *stubAssignments) {
	srv := newTestServer()
	// newTestServer stage has ApproverIDs: [u1, u2, u3] — eligible pool for swap checks.
	as := srv.assignments.(*stubAssignments)
	// seed run + assignment for u1
	rs := srv.runStore.(*stubRunStore)
	rs.tracked = &store.ApprovalRun{PolicyVersionID: "pv-swap", WorkflowDefID: "def-1", RunID: "r", Status: "in_review"}
	now := time.Now().UTC()
	if _, err := as.CreateAssignment(context.Background(), store.AssignmentRow{
		PolicyVersionID: "pv-swap", StageIndex: 0, UserID: "u1",
		SLADeadlineAt: now.Add(72 * time.Hour), ReminderAt: now.Add(48 * time.Hour),
	}, "system"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return srv, as
}

func TestSwap_AdminInPool_OK(t *testing.T) {
	srv, _ := setupServerForSwap(t)
	resp, err := srv.SwapAssignee(context.Background(), &workflowv1.SwapAssigneeRequest{
		PolicyVersionId: "pv-swap", StageIndex: 0, CurrentUserId: "u1", NewUserId: "u2",
		Reason: "PTO", InitiatorRole: workflowv1.InitiatorRole_INITIATOR_ROLE_ADMIN,
	})
	if err != nil {
		t.Fatalf("Swap: %v", err)
	}
	if resp.NewAssignmentId == "" {
		t.Fatal("expected new assignment id")
	}
	ae := srv.auditEmit.(*stubAuditEmitter)
	if len(ae.emitted) == 0 || ae.emitted[0] != "workflow.assignment.swapped" {
		t.Fatalf("audit: %v", ae.emitted)
	}
}

func TestSwap_AdminOutOfPool_NoAck_Fails(t *testing.T) {
	srv, _ := setupServerForSwap(t)
	_, err := srv.SwapAssignee(context.Background(), &workflowv1.SwapAssigneeRequest{
		PolicyVersionId: "pv-swap", StageIndex: 0, CurrentUserId: "u1", NewUserId: "outsider",
		Reason: "PTO", InitiatorRole: workflowv1.InitiatorRole_INITIATOR_ROLE_ADMIN,
	})
	if err == nil {
		t.Fatal("expected FailedPrecondition without ack")
	}
}

func TestSwap_AdminOutOfPool_WithAck_OK(t *testing.T) {
	srv, _ := setupServerForSwap(t)
	resp, err := srv.SwapAssignee(context.Background(), &workflowv1.SwapAssigneeRequest{
		PolicyVersionId: "pv-swap", StageIndex: 0, CurrentUserId: "u1", NewUserId: "outsider",
		Reason: "PTO", InitiatorRole: workflowv1.InitiatorRole_INITIATOR_ROLE_ADMIN,
		OutOfEligibilityAck: true,
	})
	if err != nil {
		t.Fatalf("Swap: %v", err)
	}
	if resp.NewAssignmentId == "" {
		t.Fatal("expected new assignment")
	}
	ae := srv.auditEmit.(*stubAuditEmitter)
	if len(ae.emitted) == 0 || ae.emitted[0] != "workflow.assignment.swapped.out_of_eligibility" {
		t.Fatalf("audit: %v", ae.emitted)
	}
}

func TestSwap_SelfDelegate_MismatchedActor_Fails(t *testing.T) {
	srv, _ := setupServerForSwap(t)
	srv.actorExtractor = func(_ context.Context) (ActorClaims, bool) {
		return ActorClaims{UserID: "u2"}, true // not u1
	}
	_, err := srv.SwapAssignee(context.Background(), &workflowv1.SwapAssigneeRequest{
		PolicyVersionId: "pv-swap", StageIndex: 0, CurrentUserId: "u1", NewUserId: "u2",
		Reason: "delegate", InitiatorRole: workflowv1.InitiatorRole_INITIATOR_ROLE_SELF_DELEGATE,
	})
	if err == nil {
		t.Fatal("expected PermissionDenied")
	}
}

func TestSwap_SelfDelegate_MatchedActor_OK(t *testing.T) {
	srv, _ := setupServerForSwap(t)
	srv.actorExtractor = func(_ context.Context) (ActorClaims, bool) {
		return ActorClaims{UserID: "u1"}, true
	}
	_, err := srv.SwapAssignee(context.Background(), &workflowv1.SwapAssigneeRequest{
		PolicyVersionId: "pv-swap", StageIndex: 0, CurrentUserId: "u1", NewUserId: "u2",
		Reason: "delegate", InitiatorRole: workflowv1.InitiatorRole_INITIATOR_ROLE_SELF_DELEGATE,
	})
	if err != nil {
		t.Fatalf("Swap: %v", err)
	}
}

func TestSwap_WorkflowAuthor_OK(t *testing.T) {
	srv, _ := setupServerForSwap(t)
	srv.actorExtractor = func(_ context.Context) (ActorClaims, bool) {
		return ActorClaims{UserID: "author-1"}, true
	}
	_, err := srv.SwapAssignee(context.Background(), &workflowv1.SwapAssigneeRequest{
		PolicyVersionId: "pv-swap", StageIndex: 0, CurrentUserId: "u1", NewUserId: "u2",
		Reason: "author override", InitiatorRole: workflowv1.InitiatorRole_INITIATOR_ROLE_WORKFLOW_AUTHOR,
	})
	if err != nil {
		t.Fatalf("Swap: %v", err)
	}
}

func TestSwap_WorkflowAuthor_NotAuthor_Fails(t *testing.T) {
	srv, _ := setupServerForSwap(t)
	srv.actorExtractor = func(_ context.Context) (ActorClaims, bool) {
		return ActorClaims{UserID: "random-user"}, true
	}
	_, err := srv.SwapAssignee(context.Background(), &workflowv1.SwapAssigneeRequest{
		PolicyVersionId: "pv-swap", StageIndex: 0, CurrentUserId: "u1", NewUserId: "u2",
		Reason: "x", InitiatorRole: workflowv1.InitiatorRole_INITIATOR_ROLE_WORKFLOW_AUTHOR,
	})
	if err == nil {
		t.Fatal("expected PermissionDenied")
	}
}

func TestSwap_MissingReason_Fails(t *testing.T) {
	srv, _ := setupServerForSwap(t)
	_, err := srv.SwapAssignee(context.Background(), &workflowv1.SwapAssigneeRequest{
		PolicyVersionId: "pv-swap", StageIndex: 0, CurrentUserId: "u1", NewUserId: "u2",
		InitiatorRole: workflowv1.InitiatorRole_INITIATOR_ROLE_ADMIN,
	})
	if err == nil {
		t.Fatal("expected InvalidArgument for missing reason")
	}
}

func TestSwap_NoActor_Unauthenticated(t *testing.T) {
	srv, _ := setupServerForSwap(t)
	srv.actorExtractor = func(_ context.Context) (ActorClaims, bool) {
		return ActorClaims{}, false
	}
	_, err := srv.SwapAssignee(context.Background(), &workflowv1.SwapAssigneeRequest{
		PolicyVersionId: "pv-swap", StageIndex: 0, CurrentUserId: "u1", NewUserId: "u2",
		Reason: "x", InitiatorRole: workflowv1.InitiatorRole_INITIATOR_ROLE_ADMIN,
	})
	if err == nil {
		t.Fatal("expected Unauthenticated")
	}
}

func TestSwap_NotPending_Fails(t *testing.T) {
	srv, as := setupServerForSwap(t)
	// flip u1 row to approved
	k := keyOf("pv-swap", 0, "u1")
	r := as.rows[k]
	r.State = "approved"
	as.rows[k] = r
	_, err := srv.SwapAssignee(context.Background(), &workflowv1.SwapAssigneeRequest{
		PolicyVersionId: "pv-swap", StageIndex: 0, CurrentUserId: "u1", NewUserId: "u2",
		Reason: "x", InitiatorRole: workflowv1.InitiatorRole_INITIATOR_ROLE_ADMIN,
	})
	if err == nil {
		t.Fatal("expected FailedPrecondition for non-pending assignment")
	}
}

// --- BulkDecide handler ---

func setupServerForBulk(t *testing.T) (*WorkflowServer, *stubAssignments) {
	srv := newTestServer()
	srv.actorExtractor = func(_ context.Context) (ActorClaims, bool) {
		return ActorClaims{UserID: "u-bulk"}, true
	}
	as := srv.assignments.(*stubAssignments)
	now := time.Now().UTC()
	for _, pv := range []string{"pv-a", "pv-b"} {
		if _, err := as.CreateAssignment(context.Background(), store.AssignmentRow{
			PolicyVersionID: pv, StageIndex: 0, UserID: "u-bulk",
			SLADeadlineAt: now.Add(72 * time.Hour), ReminderAt: now.Add(48 * time.Hour),
		}, "system"); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	return srv, as
}

func TestBulkDecide_TwoApprovals_OK(t *testing.T) {
	srv, _ := setupServerForBulk(t)
	resp, err := srv.BulkDecide(context.Background(), &workflowv1.BulkDecideRequest{
		Decisions: []*workflowv1.Decision{
			{PolicyVersionId: "pv-a", StageIndex: 0, Decision: workflowv1.DecisionType_DECISION_TYPE_APPROVE, Comment: "ok a"},
			{PolicyVersionId: "pv-b", StageIndex: 0, Decision: workflowv1.DecisionType_DECISION_TYPE_APPROVE, Comment: "ok b"},
		},
	})
	if err != nil {
		t.Fatalf("BulkDecide: %v", err)
	}
	if resp.BulkBatchId == "" {
		t.Fatal("expected batch id")
	}
	for i, r := range resp.Results {
		if !r.Ok {
			t.Errorf("result[%d] not ok: %s", i, r.Error)
		}
	}
}

func TestBulkDecide_EmptyComment_RejectedUpfront(t *testing.T) {
	srv, _ := setupServerForBulk(t)
	_, err := srv.BulkDecide(context.Background(), &workflowv1.BulkDecideRequest{
		Decisions: []*workflowv1.Decision{
			{PolicyVersionId: "pv-a", StageIndex: 0, Decision: workflowv1.DecisionType_DECISION_TYPE_APPROVE, Comment: ""},
		},
	})
	if err == nil {
		t.Fatal("expected InvalidArgument for empty comment")
	}
}

func TestBulkDecide_RejectionCascades(t *testing.T) {
	srv, as := setupServerForBulk(t)
	// add peer for pv-a so reject can cascade
	now := time.Now().UTC()
	if _, err := as.CreateAssignment(context.Background(), store.AssignmentRow{
		PolicyVersionID: "pv-a", StageIndex: 0, UserID: "peer",
		SLADeadlineAt: now.Add(72 * time.Hour), ReminderAt: now.Add(48 * time.Hour),
	}, "system"); err != nil {
		t.Fatalf("seed peer: %v", err)
	}
	_, err := srv.BulkDecide(context.Background(), &workflowv1.BulkDecideRequest{
		Decisions: []*workflowv1.Decision{
			{PolicyVersionId: "pv-a", StageIndex: 0, Decision: workflowv1.DecisionType_DECISION_TYPE_REJECT, Comment: "no"},
		},
	})
	if err != nil {
		t.Fatalf("BulkDecide: %v", err)
	}
	peer, _ := as.GetAssignment(context.Background(), "pv-a", 0, "peer", "")
	if peer.State != "superseded" {
		t.Fatalf("expected peer to be superseded, got %q", peer.State)
	}
}

func TestBulkDecide_NoDecisions_Fails(t *testing.T) {
	srv, _ := setupServerForBulk(t)
	_, err := srv.BulkDecide(context.Background(), &workflowv1.BulkDecideRequest{})
	if err == nil {
		t.Fatal("expected error for empty batch")
	}
}

func TestBulkDecide_NoActor_Unauthenticated(t *testing.T) {
	srv, _ := setupServerForBulk(t)
	srv.actorExtractor = func(_ context.Context) (ActorClaims, bool) { return ActorClaims{}, false }
	_, err := srv.BulkDecide(context.Background(), &workflowv1.BulkDecideRequest{
		Decisions: []*workflowv1.Decision{
			{PolicyVersionId: "pv-a", StageIndex: 0, Decision: workflowv1.DecisionType_DECISION_TYPE_APPROVE, Comment: "x"},
		},
	})
	if err == nil {
		t.Fatal("expected Unauthenticated")
	}
}

// --- GetAssignmentHistory ---

func TestGetAssignmentHistory_Returns(t *testing.T) {
	srv, _ := setupServerForBulk(t)
	resp, err := srv.GetAssignmentHistory(context.Background(), &workflowv1.GetAssignmentHistoryRequest{
		PolicyVersionId: "pv-a", StageIndex: 0,
	})
	if err != nil {
		t.Fatalf("GetAssignmentHistory: %v", err)
	}
	if len(resp.Entries) == 0 {
		t.Fatal("expected at least one history entry")
	}
}

// --- ResolveRun (GC on resolution) ---

// extendedDefStore adds GetVersion + GCVersion to stubWorkflowDefStore so the
// GC test can track whether GCVersion was invoked.
type extendedDefStore struct {
	stubWorkflowDefStore
	currentVersion  int
	gcCalledDefID   string
	gcCalledVersion int
}

func (s *extendedDefStore) GetVersion(_ context.Context, _ string, version int) (builder.WorkflowDef, error) {
	return builder.WorkflowDef{ID: s.wd.ID, Version: version, Name: s.wd.Name, Stages: s.wd.Stages}, nil
}
func (s *extendedDefStore) GCVersion(_ context.Context, defID string, version int) error {
	s.gcCalledDefID = defID
	s.gcCalledVersion = version
	return nil
}

// extendedAssignments extends stubAssignments with ListActiveRunsByDef.
type extendedAssignments struct {
	stubAssignments
	activeRuns []store.ApprovalRun
}

func (s *extendedAssignments) ListActiveRunsByDef(_ context.Context, _ string, _ int) ([]store.ApprovalRun, error) {
	return s.activeRuns, nil
}

// TestResolveRun_GCsUnpinnedVersionWhenLastRun asserts that when a run pinned
// to a non-current version resolves (terminal status), and no other runs are
// active on that version, GCVersion is called on the def store.
func TestResolveRun_GCsUnpinnedVersionWhenLastRun(t *testing.T) {
	// def is at version 2 (current); the run was pinned to version 1.
	defStore := &extendedDefStore{
		stubWorkflowDefStore: stubWorkflowDefStore{wd: builder.WorkflowDef{
			ID: "def-gc", Name: "GC Test", Version: 2,
			Stages: []builder.Stage{{Name: "s", Quorum: builder.QuorumAny, ApproverIDs: []string{"u1"}}},
		}},
		currentVersion: 2,
	}
	runStore := &stubRunStore{
		tracked: &store.ApprovalRun{
			PolicyVersionID: "pv-gc",
			RunID:           "run-gc",
			WorkflowDefID:   "def-gc",
			WorkflowVersion: 1, // pinned to v1 (non-current)
			Status:          "in_review",
		},
	}
	asgmts := &extendedAssignments{
		stubAssignments: *newStubAssignments(),
		activeRuns:      nil, // no other runs on def-gc@v1
	}

	srv := NewWorkflowServer(WorkflowServerOpts{
		Resolver:    &stubWorkflowResolver{defID: "def-gc"},
		DefStore:    defStore,
		RunStore:    runStore,
		SagaClient:  &stubSagaClient{},
		AuditEmit:   &stubAuditEmitter{},
		Assignments: asgmts,
		CompileOpts: builder.CompileOptions{StageSLAHours: 72},
		ActorExtractor: func(_ context.Context) (ActorClaims, bool) {
			return ActorClaims{UserID: "system"}, true
		},
	})

	if err := srv.ResolveRun(context.Background(), "pv-gc", "approved"); err != nil {
		t.Fatalf("ResolveRun: %v", err)
	}

	// GCVersion must have been called for def-gc@v1 because v1 != current(v2) and
	// no other runs pin v1.
	if defStore.gcCalledDefID != "def-gc" || defStore.gcCalledVersion != 1 {
		t.Fatalf("GCVersion not called or wrong args: defID=%q version=%d",
			defStore.gcCalledDefID, defStore.gcCalledVersion)
	}
}

// TestResolveRun_NoGCWhenCurrentVersion asserts that when the run's version
// matches the current version, GCVersion is NOT called (nothing to GC).
func TestResolveRun_NoGCWhenCurrentVersion(t *testing.T) {
	defStore := &extendedDefStore{
		stubWorkflowDefStore: stubWorkflowDefStore{wd: builder.WorkflowDef{
			ID: "def-cur", Name: "Current Version Test", Version: 2,
			Stages: []builder.Stage{{Name: "s", Quorum: builder.QuorumAny, ApproverIDs: []string{"u1"}}},
		}},
		currentVersion: 2,
	}
	runStore := &stubRunStore{
		tracked: &store.ApprovalRun{
			PolicyVersionID: "pv-cur",
			RunID:           "run-cur",
			WorkflowDefID:   "def-cur",
			WorkflowVersion: 2, // pinned to current version
			Status:          "in_review",
		},
	}
	asgmts := &extendedAssignments{stubAssignments: *newStubAssignments()}

	srv := NewWorkflowServer(WorkflowServerOpts{
		Resolver:    &stubWorkflowResolver{defID: "def-cur"},
		DefStore:    defStore,
		RunStore:    runStore,
		SagaClient:  &stubSagaClient{},
		AuditEmit:   &stubAuditEmitter{},
		Assignments: asgmts,
		CompileOpts: builder.CompileOptions{StageSLAHours: 72},
	})

	if err := srv.ResolveRun(context.Background(), "pv-cur", "approved"); err != nil {
		t.Fatalf("ResolveRun: %v", err)
	}

	// GCVersion must NOT be called — the run was on the current version.
	if defStore.gcCalledDefID != "" {
		t.Fatalf("GCVersion should not be called for current version, got defID=%q", defStore.gcCalledDefID)
	}
}

// TestResolveRun_NoGCWhenOtherRunsStillActive asserts that when other runs
// still pin the same non-current version, GCVersion is NOT called.
func TestResolveRun_NoGCWhenOtherRunsStillActive(t *testing.T) {
	defStore := &extendedDefStore{
		stubWorkflowDefStore: stubWorkflowDefStore{wd: builder.WorkflowDef{
			ID: "def-shared", Name: "Shared Version Test", Version: 3,
			Stages: []builder.Stage{{Name: "s", Quorum: builder.QuorumAny, ApproverIDs: []string{"u1"}}},
		}},
		currentVersion: 3,
	}
	runStore := &stubRunStore{
		tracked: &store.ApprovalRun{
			PolicyVersionID: "pv-shared-1",
			RunID:           "run-shared-1",
			WorkflowDefID:   "def-shared",
			WorkflowVersion: 2, // non-current
			Status:          "in_review",
		},
	}
	asgmts := &extendedAssignments{
		stubAssignments: *newStubAssignments(),
		activeRuns: []store.ApprovalRun{
			// Another run is still active on v2.
			{PolicyVersionID: "pv-shared-2", RunID: "run-shared-2",
				WorkflowDefID: "def-shared", WorkflowVersion: 2, Status: "in_review"},
		},
	}

	srv := NewWorkflowServer(WorkflowServerOpts{
		Resolver:    &stubWorkflowResolver{defID: "def-shared"},
		DefStore:    defStore,
		RunStore:    runStore,
		SagaClient:  &stubSagaClient{},
		AuditEmit:   &stubAuditEmitter{},
		Assignments: asgmts,
		CompileOpts: builder.CompileOptions{StageSLAHours: 72},
	})

	if err := srv.ResolveRun(context.Background(), "pv-shared-1", "approved"); err != nil {
		t.Fatalf("ResolveRun: %v", err)
	}

	// GCVersion must NOT be called — another run still pins v2.
	if defStore.gcCalledDefID != "" {
		t.Fatalf("GCVersion should not be called while other runs pin the version, got defID=%q", defStore.gcCalledDefID)
	}
}

// Ensure errors.Is still works for non-pgx in our handlers (we don't
// import pgx into tests).
var _ = errors.Is

// --- withdraw cleanup ---

type stubCoreStatus struct {
	calls []struct{ pvID, status, actor string }
	err   error
}

func (s *stubCoreStatus) SetVersionStatus(_ context.Context, pvID, status, actor string) error {
	if s.err != nil {
		return s.err
	}
	s.calls = append(s.calls, struct{ pvID, status, actor string }{pvID, status, actor})
	return nil
}

func TestSignal_WithdrawTerminatesAssignmentsAndSetsDraft(t *testing.T) {
	srv := newTestServer()
	core := &stubCoreStatus{}
	srv.coreStatus = core

	rs := srv.runStore.(*stubRunStore)
	rs.tracked = &store.ApprovalRun{RunID: "run-w", PolicyVersionID: "pv-w", Status: "in_review"}

	as := srv.assignments.(*stubAssignments)
	// Two pending assignments across stages — both must be terminated.
	as.rows[keyOf("pv-w", 0, "u1")] = store.AssignmentRow{ID: "a1", PolicyVersionID: "pv-w", StageIndex: 0, UserID: "u1", State: "pending"}
	as.rows[keyOf("pv-w", 1, "u2")] = store.AssignmentRow{ID: "a2", PolicyVersionID: "pv-w", StageIndex: 1, UserID: "u2", State: "pending"}

	_, err := srv.Signal(context.Background(), &workflowv1.SignalRequest{
		RunId: "run-w", Signal: workflowv1.SignalType_SIGNAL_TYPE_WITHDRAW, ActorUserId: "owner",
	})
	if err != nil {
		t.Fatalf("Signal(withdraw): %v", err)
	}

	if got := as.rows[keyOf("pv-w", 0, "u1")]; got.State != "withdrawn" {
		t.Fatalf("a1 state = %q, want withdrawn", got.State)
	}
	if got := as.rows[keyOf("pv-w", 1, "u2")]; got.State != "withdrawn" {
		t.Fatalf("a2 state = %q, want withdrawn", got.State)
	}
	if rs.runStatus != "withdrawn" {
		t.Fatalf("run status = %q, want withdrawn", rs.runStatus)
	}
	if len(core.calls) != 1 || core.calls[0].status != "draft" || core.calls[0].pvID != "pv-w" {
		t.Fatalf("core status calls: %+v", core.calls)
	}
	ae := srv.auditEmit.(*stubAuditEmitter)
	if !slices.Contains(ae.emitted, "workflow.withdrawn") {
		t.Fatalf("expected workflow.withdrawn audit, got %v", ae.emitted)
	}
	// The saga must still be signalled to unpause/terminate the run.
	if sc := srv.sagaClient.(*stubSagaClient); !sc.signaled || sc.lastSignalName != "withdraw" {
		t.Fatalf("saga signal: signaled=%v name=%q", sc.signaled, sc.lastSignalName)
	}
}

func TestSignal_WithdrawCoreFailureIsFatal(t *testing.T) {
	srv := newTestServer()
	srv.coreStatus = &stubCoreStatus{err: fmt.Errorf("core down")}
	rs := srv.runStore.(*stubRunStore)
	rs.tracked = &store.ApprovalRun{RunID: "run-w", PolicyVersionID: "pv-w", Status: "in_review"}

	_, err := srv.Signal(context.Background(), &workflowv1.SignalRequest{
		RunId: "run-w", Signal: workflowv1.SignalType_SIGNAL_TYPE_WITHDRAW, ActorUserId: "owner",
	})
	if err == nil {
		t.Fatal("expected error when core SetVersionStatus fails")
	}
	// Saga must NOT be signalled when cleanup fails.
	if sc := srv.sagaClient.(*stubSagaClient); sc.signaled {
		t.Fatal("saga should not be signalled after withdraw cleanup failure")
	}
}

// TestSignal_WithdrawReturnsMultiStageRunToDraft is the D4 regression: a
// multi-stage run that has advanced past stage 0 (sitting at stage 1) must STILL
// return the core policy version to draft on withdraw — the revert is
// unconditional and stage-independent, never skipped for later stages.
func TestSignal_WithdrawReturnsMultiStageRunToDraft(t *testing.T) {
	srv := newTestServer()
	core := &stubCoreStatus{}
	srv.coreStatus = core

	rs := srv.runStore.(*stubRunStore)
	// Stage 0 already approved; the run is paused at stage 1 (second approver).
	rs.tracked = &store.ApprovalRun{RunID: "run-w", PolicyVersionID: "pv-w", Status: "in_review"}

	as := srv.assignments.(*stubAssignments)
	as.rows[keyOf("pv-w", 0, "u1")] = store.AssignmentRow{ID: "a1", PolicyVersionID: "pv-w", StageIndex: 0, UserID: "u1", State: "approved"}
	as.rows[keyOf("pv-w", 1, "u2")] = store.AssignmentRow{ID: "a2", PolicyVersionID: "pv-w", StageIndex: 1, UserID: "u2", State: "pending"}

	_, err := srv.Signal(context.Background(), &workflowv1.SignalRequest{
		RunId: "run-w", Signal: workflowv1.SignalType_SIGNAL_TYPE_WITHDRAW, ActorUserId: "owner",
	})
	if err != nil {
		t.Fatalf("Signal(withdraw) at stage 1: %v", err)
	}

	// The core version MUST have been returned to draft exactly once, keyed on the
	// right policy version and carrying the acting user.
	if len(core.calls) != 1 {
		t.Fatalf("core status calls = %d, want exactly 1: %+v", len(core.calls), core.calls)
	}
	if c := core.calls[0]; c.status != "draft" || c.pvID != "pv-w" || c.actor != "owner" {
		t.Fatalf("core call = %+v, want {pv-w draft owner}", c)
	}
	// Run flipped terminal.
	if rs.runStatus != "withdrawn" {
		t.Fatalf("run status = %q, want withdrawn", rs.runStatus)
	}
}

// TestSignal_WithdrawRequiresCoreSetter guards the D4 root cause: an unwired
// core status setter must FAIL the withdraw (rather than silently skipping the
// revert and leaving the version stuck in-review). A "successful" withdraw that
// never returns the version to draft is the exact bug we are fixing.
func TestSignal_WithdrawRequiresCoreSetter(t *testing.T) {
	srv := newTestServer()
	srv.coreStatus = nil // simulate a wiring regression
	rs := srv.runStore.(*stubRunStore)
	rs.tracked = &store.ApprovalRun{RunID: "run-w", PolicyVersionID: "pv-w", Status: "in_review"}

	_, err := srv.Signal(context.Background(), &workflowv1.SignalRequest{
		RunId: "run-w", Signal: workflowv1.SignalType_SIGNAL_TYPE_WITHDRAW, ActorUserId: "owner",
	})
	if err == nil {
		t.Fatal("expected withdraw to fail when no core status setter is wired")
	}
	if st, _ := status.FromError(err); st.Code() != codes.Unimplemented {
		t.Fatalf("error code = %v, want Unimplemented", st.Code())
	}
	// The saga must NOT be signalled when the revert could not run.
	if sc := srv.sagaClient.(*stubSagaClient); sc.signaled {
		t.Fatal("saga should not be signalled when core setter is unwired")
	}
}

// TestSignal_StaleRunIDResolvesByPolicyVersion is the core robustness guard: a
// reject carries a STALE run_id (a superseded run the inbox cached) but a valid
// policy_version_id + actor. The server must ignore the stale run_id and resolve
// the version's CURRENT tracked run, applying the decision there and signalling
// the current run — never throwing NotFound for the stale id.
func TestSignal_StaleRunIDResolvesByPolicyVersion(t *testing.T) {
	srv := newTestServer()
	rs := srv.runStore.(*stubRunStore)
	// The CURRENT run for pv-stale is "run-current"; the inbox cached "run-OLD".
	rs.tracked = &store.ApprovalRun{RunID: "run-current", PolicyVersionID: "pv-stale", Status: "in_review"}
	as := srv.assignments.(*stubAssignments)
	as.rows[keyOf("pv-stale", 0, "u-app")] = store.AssignmentRow{
		ID: "asg-cur", PolicyVersionID: "pv-stale", StageIndex: 0, UserID: "u-app", State: "pending",
	}
	// The engine's pending task for the current run (a stale task_id is passed).
	srv.sagaClient = &stubSagaClient{pendingTasks: []PendingTaskInfo{
		{TaskID: "task-current", RunID: "run-current", PolicyVersionID: "pv-stale", StageIndex: 0},
	}}

	_, err := srv.Signal(context.Background(), &workflowv1.SignalRequest{
		RunId:           "run-OLD-25954ba1", // stale: not the current run
		TaskId:          "task-OLD",         // stale task id
		PolicyVersionId: "pv-stale",
		Signal:          workflowv1.SignalType_SIGNAL_TYPE_REJECT,
		ActorUserId:     "u-app",
		Comment:         "no",
	})
	if err != nil {
		t.Fatalf("Signal with stale run_id should resolve current run, got: %v", err)
	}
	sc := srv.sagaClient.(*stubSagaClient)
	// Saga must be signalled against the CURRENT run + CURRENT task, not the stale ones.
	if sc.lastSignalRunID != "run-current" {
		t.Fatalf("signalled run = %q, want run-current (stale run_id must be ignored)", sc.lastSignalRunID)
	}
	if sc.lastSignalInputs["task_id"] != "task-current" {
		t.Fatalf("signalled task_id = %v, want task-current (stale task_id must be ignored)", sc.lastSignalInputs["task_id"])
	}
	// The decision landed on the current assignment.
	if got := as.rows[keyOf("pv-stale", 0, "u-app")]; got.State != "rejected" {
		t.Fatalf("current assignment state = %q, want rejected", got.State)
	}
	// Reject ensures the CURRENT run is terminal.
	if len(sc.cancelledRuns) != 1 || sc.cancelledRuns[0] != "run-current" {
		t.Fatalf("reject must cancel the current run, got %v", sc.cancelledRuns)
	}
}

// TestSignal_NoPendingAssignmentReturnsFailedPrecondition confirms that when the
// actor has no pending assignment for the version (already decided / withdrawn /
// not an approver), the server returns FailedPrecondition — never a raw NotFound
// for the stale click.
func TestSignal_NoPendingAssignmentReturnsFailedPrecondition(t *testing.T) {
	srv := newTestServer()
	rs := srv.runStore.(*stubRunStore)
	rs.tracked = &store.ApprovalRun{RunID: "run-current", PolicyVersionID: "pv-done", Status: "in_review"}
	// Actor has NO pending assignment for this version.
	srv.assignments.(*stubAssignments).findPendingErr = fmt.Errorf("no rows in result set")

	_, err := srv.Signal(context.Background(), &workflowv1.SignalRequest{
		RunId:           "run-stale",
		PolicyVersionId: "pv-done",
		Signal:          workflowv1.SignalType_SIGNAL_TYPE_APPROVE,
		ActorUserId:     "ghost",
		Comment:         "x",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("want FailedPrecondition for no pending assignment, got %v (code=%v)", err, status.Code(err))
	}
	if status.Code(err) == codes.NotFound {
		t.Fatal("must never return NotFound for a stale click")
	}
}

// TestSignal_UnknownPolicyVersionReturnsFailedPrecondition confirms that an
// unknown/withdrawn policy_version_id (no current run) yields FailedPrecondition,
// not a raw NotFound — the "refresh your inbox" guard for a fully stale row.
func TestSignal_UnknownPolicyVersionReturnsFailedPrecondition(t *testing.T) {
	srv := newTestServer()
	srv.runStore.(*stubRunStore).notFoundPV = "pv-gone"

	_, err := srv.Signal(context.Background(), &workflowv1.SignalRequest{
		RunId:           "run-stale",
		PolicyVersionId: "pv-gone",
		Signal:          workflowv1.SignalType_SIGNAL_TYPE_APPROVE,
		ActorUserId:     "u1",
		Comment:         "x",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("want FailedPrecondition for unknown policy version, got %v (code=%v)", err, status.Code(err))
	}
}

// --- ReassignUserWorkflowItems (account merge, /) ---

func seedPending(s *stubAssignments, pv string, stage int, user string) {
	s.rows[keyOf(pv, stage, user)] = store.AssignmentRow{
		ID: "a-" + pv + "-" + user, PolicyVersionID: pv, StageIndex: stage, UserID: user, State: "pending",
	}
}

func TestReassignUserWorkflowItems_MovesAndDedupes(t *testing.T) {
	srv := newTestServer()
	sa := srv.assignments.(*stubAssignments)
	seedPending(sa, "pv-1", 0, "src") // moves
	seedPending(sa, "pv-2", 0, "src") // dedupes (target already present)
	seedPending(sa, "pv-2", 0, "tgt") // target seat

	resp, err := srv.ReassignUserWorkflowItems(context.Background(), &workflowv1.ReassignUserWorkflowItemsRequest{
		FromUserId: "src", ToUserId: "tgt", ActorUserId: "admin",
	})
	if err != nil {
		t.Fatalf("ReassignUserWorkflowItems: %v", err)
	}
	if resp.AssignmentsReassigned != 1 || resp.AssignmentsDeduped != 1 || resp.Total != 2 {
		t.Fatalf("resp = %+v, want 1 reassigned / 1 deduped / total 2", resp)
	}
	// Audit event emitted once with the tally.
	ae := srv.auditEmit.(*stubAuditEmitter)
	if !slices.Contains(ae.emitted, "workflow.user.reassigned") {
		t.Fatalf("expected workflow.user.reassigned audit event, got %+v", ae.emitted)
	}
}

func TestReassignUserWorkflowItems_DryRunDoesNotMutate(t *testing.T) {
	srv := newTestServer()
	sa := srv.assignments.(*stubAssignments)
	seedPending(sa, "pv-1", 0, "src")

	resp, err := srv.ReassignUserWorkflowItems(context.Background(), &workflowv1.ReassignUserWorkflowItemsRequest{
		FromUserId: "src", ToUserId: "tgt", ActorUserId: "admin", DryRun: true,
	})
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if resp.AssignmentsReassigned != 1 || resp.Total != 1 {
		t.Fatalf("dry-run resp = %+v", resp)
	}
	if _, ok := sa.rows[keyOf("pv-1", 0, "src")]; !ok {
		t.Fatal("dry-run must not move the source seat")
	}
	// No audit event on a dry run.
	if slices.Contains(srv.auditEmit.(*stubAuditEmitter).emitted, "workflow.user.reassigned") {
		t.Fatal("dry-run must not emit an audit event")
	}
}

func TestReassignUserWorkflowItems_Validation(t *testing.T) {
	srv := newTestServer()
	cases := []struct {
		name     string
		from, to string
		wantCode codes.Code
	}{
		{"empty from", "", "tgt", codes.InvalidArgument},
		{"empty to", "src", "", codes.InvalidArgument},
		{"same", "u", "u", codes.InvalidArgument},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := srv.ReassignUserWorkflowItems(context.Background(), &workflowv1.ReassignUserWorkflowItemsRequest{
				FromUserId: tc.from, ToUserId: tc.to,
			})
			if status.Code(err) != tc.wantCode {
				t.Fatalf("got %v (code=%v), want %v", err, status.Code(err), tc.wantCode)
			}
		})
	}
}

func TestReassignUserWorkflowItems_UnimplementedWithoutStore(t *testing.T) {
	srv := &WorkflowServer{}
	_, err := srv.ReassignUserWorkflowItems(context.Background(), &workflowv1.ReassignUserWorkflowItemsRequest{
		FromUserId: "src", ToUserId: "tgt",
	})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("want Unimplemented without assignment store, got %v", err)
	}
}
