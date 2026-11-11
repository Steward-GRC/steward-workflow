// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc

import (
	"context"
	"slices"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"

	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/store"
	"github.com/Steward-GRC/steward-workflow/internal/tally"
)

// The review model (#39): a rejection is a vote, request changes keeps the
// seat open, and a decision on an ended run is refused. Each test names the
// mock server test it mirrors (steward-mock-server src/product
// author-loop.test.ts and review-snapshots.test.ts).

const reviewPV = "pv-review"

// reviewServer returns a server whose run is pinned to a one-stage definition
// with the given quorum and whose seats start in the given states (user -> state).
func reviewServer(t *testing.T, quorum builder.Quorum, quorumN int, seats map[string]string) *WorkflowServer {
	t.Helper()
	srv := newTestServer()
	srv.defStore = &stubWorkflowDefStore{wd: builder.WorkflowDef{
		ID: "def-1", Name: "Review", Version: 1,
		Stages: []builder.Stage{{Name: "Review", Quorum: quorum, QuorumN: quorumN}},
	}}
	srv.coreStatus = &stubCoreStatus{}
	srv.runStore.(*stubRunStore).tracked = &store.ApprovalRun{
		RunID: "run-r", PolicyVersionID: reviewPV, WorkflowDefID: "def-1", WorkflowVersion: 1,
		Status: "in_review", SubmittedBy: "author",
	}
	as := srv.assignments.(*stubAssignments)
	as.strictSeats = true
	for user, state := range seats {
		as.rows[keyOf(reviewPV, 0, user)] = store.AssignmentRow{
			ID: "asg-" + user, PolicyVersionID: reviewPV, StageIndex: 0, UserID: user, State: state,
		}
	}
	return srv
}

func reviewSignal(srv *WorkflowServer, user string, sig workflowv1.SignalType, comment string) error {
	_, err := srv.Signal(context.Background(), &workflowv1.SignalRequest{
		RunId: "run-r", PolicyVersionId: reviewPV, TaskId: "task-1", Signal: sig, ActorUserId: user, Comment: comment,
	})
	return err
}

func reviewSeat(srv *WorkflowServer, user string) string {
	return srv.assignments.(*stubAssignments).rows[keyOf(reviewPV, 0, user)].State
}

// Mock: "ends the run as soon as the approvals still possible cannot reach the
// quorum", "ends it on a reject after an approve when that leaves the quorum
// out of reach", "counts a rejection as a vote while the quorum can still be
// reached" and "ends the run at once on a rejection when the stage's quorum is
// one".
func TestSignal_RejectIsAVote(t *testing.T) {
	cases := []struct {
		name      string
		quorum    builder.Quorum
		seats     map[string]string
		rejecter  string
		wantEnded bool
		wantPeers map[string]string
	}{
		{
			name: "majority of two, one reject ends it", quorum: builder.QuorumMajority,
			seats: map[string]string{"charlie": "pending", "alice": "pending"}, rejecter: "charlie",
			wantEnded: true, wantPeers: map[string]string{"alice": "superseded"},
		},
		{
			name: "majority of two, approve then reject ends it", quorum: builder.QuorumMajority,
			seats: map[string]string{"charlie": "approved", "alice": "pending"}, rejecter: "alice",
			wantEnded: true, wantPeers: map[string]string{"charlie": "approved"},
		},
		{
			name: "majority of three, one reject keeps the other seats open", quorum: builder.QuorumMajority,
			seats:    map[string]string{"charlie": "pending", "alice": "pending", "dave": "pending"},
			rejecter: "charlie", wantEnded: false,
			wantPeers: map[string]string{"alice": "pending", "dave": "pending"},
		},
		{
			name: "a quorum of one seat ends it at once", quorum: builder.QuorumAny,
			seats: map[string]string{"alice": "pending"}, rejecter: "alice", wantEnded: true,
		},
		{
			name: "any of two, one reject keeps the other seat open", quorum: builder.QuorumAny,
			seats: map[string]string{"charlie": "pending", "alice": "pending"}, rejecter: "charlie",
			wantEnded: false, wantPeers: map[string]string{"alice": "pending"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := reviewServer(t, tc.quorum, 0, tc.seats)
			if err := reviewSignal(srv, tc.rejecter, workflowv1.SignalType_SIGNAL_TYPE_REJECT, "No."); err != nil {
				t.Fatalf("reject: %v", err)
			}
			if got := reviewSeat(srv, tc.rejecter); got != "rejected" {
				t.Fatalf("rejecter seat = %q, want rejected", got)
			}
			for user, want := range tc.wantPeers {
				if got := reviewSeat(srv, user); got != want {
					t.Fatalf("peer %s seat = %q, want %q", user, got, want)
				}
			}
			cancelled := len(srv.sagaClient.(*stubSagaClient).cancelledRuns) > 0
			if cancelled != tc.wantEnded {
				t.Fatalf("run ended = %v, want %v", cancelled, tc.wantEnded)
			}
			denied := srv.auditEmit.(*stubAuditEmitter).emitted
			if !slices.Contains(denied, "workflow.decision.rejected") {
				t.Fatalf("expected the rejection to be audited, got %v", denied)
			}
		})
	}
}

// Mock: "counts a rejection as a vote while the quorum can still be reached":
// the stage is met by the two approvals despite the reject.
func TestSignal_RejectThenTwoApprovalsMeetsMajorityOfThree(t *testing.T) {
	srv := reviewServer(t, builder.QuorumMajority, 0,
		map[string]string{"charlie": "pending", "alice": "pending", "dave": "pending"})
	for _, step := range []struct {
		user string
		sig  workflowv1.SignalType
	}{
		{"charlie", workflowv1.SignalType_SIGNAL_TYPE_REJECT},
		{"alice", workflowv1.SignalType_SIGNAL_TYPE_APPROVE},
		{"dave", workflowv1.SignalType_SIGNAL_TYPE_APPROVE},
	} {
		if err := reviewSignal(srv, step.user, step.sig, "ok"); err != nil {
			t.Fatalf("%s %v: %v", step.user, step.sig, err)
		}
	}
	rows, _ := srv.assignments.ListStageAssignments(context.Background(), reviewPV, 0)
	stage := builder.Stage{Quorum: builder.QuorumMajority}
	if got := tally.Resolve(stage, rows); got != tally.DecisionApprove {
		t.Fatalf("stage outcome = %q, want approve", got)
	}
	if n := len(srv.sagaClient.(*stubSagaClient).cancelledRuns); n != 0 {
		t.Fatalf("the run must not be ended, cancelled %d", n)
	}
}

// Mock: "sending comments: changes no status and keeps the approver's seat
// open" and "lets the approver approve afterwards".
func TestSignal_RequestChangesKeepsTheSeatOpen(t *testing.T) {
	srv := reviewServer(t, builder.QuorumAny, 0, map[string]string{"charlie": "pending", "alice": "pending"})
	if err := reviewSignal(srv, "charlie", workflowv1.SignalType_SIGNAL_TYPE_REQUEST_CHANGES, "See my comments."); err != nil {
		t.Fatalf("request changes: %v", err)
	}
	if got := reviewSeat(srv, "charlie"); got != "pending" {
		t.Fatalf("seat = %q, want pending", got)
	}
	if got := reviewSeat(srv, "alice"); got != "pending" {
		t.Fatalf("peer seat = %q, want pending", got)
	}
	if got := srv.runStore.(*stubRunStore).runStatus; got != "" {
		t.Fatalf("run status was written (%q); request changes changes no status", got)
	}
	if n := len(srv.coreStatus.(*stubCoreStatus).calls); n != 0 {
		t.Fatalf("core status written %d times; request changes changes no status", n)
	}
	sc := srv.sagaClient.(*stubSagaClient)
	if sc.signaled {
		t.Fatal("the saga must not be woken: the stage is unchanged")
	}
	if len(sc.cancelledRuns) != 0 {
		t.Fatalf("the run must stay open, cancelled %v", sc.cancelledRuns)
	}
	as := srv.assignments.(*stubAssignments)
	if row := as.rows[keyOf(reviewPV, 0, "charlie")]; row.DecidedAt != nil || row.DecidedComment != nil {
		t.Fatalf("seat carries a decision: %+v", row)
	}
	// The comment is kept in the seat's history.
	var kept bool
	for _, h := range as.history {
		if h.Event == "changes_requested" && h.Reason == "See my comments." && h.ActorUserID == "charlie" {
			kept = true
		}
	}
	if !kept {
		t.Fatalf("comment not kept in history: %+v", as.history)
	}
	if ae := srv.auditEmit.(*stubAuditEmitter); !slices.Contains(ae.emitted, "workflow.signal.request_changes") {
		t.Fatalf("expected request_changes audit, got %v", ae.emitted)
	}

	// The approver can still approve afterwards.
	if err := reviewSignal(srv, "charlie", workflowv1.SignalType_SIGNAL_TYPE_APPROVE, "Fine now."); err != nil {
		t.Fatalf("approve after request changes: %v", err)
	}
	if got := reviewSeat(srv, "charlie"); got != "approved" {
		t.Fatalf("seat = %q, want approved", got)
	}
}

// Mock: "sending comments: is refused for someone with no seat on the run".
func TestSignal_RequestChangesRefusedWithoutASeat(t *testing.T) {
	srv := reviewServer(t, builder.QuorumAny, 0, map[string]string{"charlie": "pending"})
	err := reviewSignal(srv, "erin", workflowv1.SignalType_SIGNAL_TYPE_REQUEST_CHANGES, "x")
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("want FailedPrecondition, got %v", err)
	}
	if srv.sagaClient.(*stubSagaClient).signaled {
		t.Fatal("the saga must not be signalled for a refused request")
	}
}

// Mock: "refuses to decide a task twice".
func TestSignal_RefusesToDecideATaskTwice(t *testing.T) {
	srv := reviewServer(t, builder.QuorumAll, 0, map[string]string{"charlie": "pending", "alice": "pending"})
	if err := reviewSignal(srv, "charlie", workflowv1.SignalType_SIGNAL_TYPE_APPROVE, "ok"); err != nil {
		t.Fatalf("first decision: %v", err)
	}
	err := reviewSignal(srv, "charlie", workflowv1.SignalType_SIGNAL_TYPE_REJECT, "changed my mind")
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("want FailedPrecondition on the second decision, got %v", err)
	}
	if got := reviewSeat(srv, "charlie"); got != "approved" {
		t.Fatalf("seat = %q, the first decision must stand", got)
	}
}

// Mock: "refuses a decision on a run that has ended", for every signal that
// acts on the run.
func TestSignal_RefusesAnEndedRun(t *testing.T) {
	signals := []workflowv1.SignalType{
		workflowv1.SignalType_SIGNAL_TYPE_APPROVE,
		workflowv1.SignalType_SIGNAL_TYPE_REJECT,
		workflowv1.SignalType_SIGNAL_TYPE_REQUEST_CHANGES,
		workflowv1.SignalType_SIGNAL_TYPE_WITHDRAW,
	}
	for _, runStatus := range []string{"rejected", "published", "withdrawn", "approved"} {
		for _, sig := range signals {
			t.Run(runStatus+"/"+sig.String(), func(t *testing.T) {
				srv := reviewServer(t, builder.QuorumAny, 0, map[string]string{"charlie": "pending"})
				srv.runStore.(*stubRunStore).tracked.Status = runStatus
				err := reviewSignal(srv, "charlie", sig, "x")
				if status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("want FailedPrecondition, got %v", err)
				}
				if srv.sagaClient.(*stubSagaClient).signaled {
					t.Fatal("the saga must not be signalled")
				}
				if n := len(srv.coreStatus.(*stubCoreStatus).calls); n != 0 {
					t.Fatalf("core status written %d times for an ended run", n)
				}
				if got := reviewSeat(srv, "charlie"); got != "pending" {
					t.Fatalf("seat = %q, want untouched", got)
				}
			})
		}
	}
}

// Mock: "wants a comment with an approval" (a blank comment is no comment).
func TestSignal_BlankCommentIsRefused(t *testing.T) {
	for _, sig := range []workflowv1.SignalType{
		workflowv1.SignalType_SIGNAL_TYPE_APPROVE,
		workflowv1.SignalType_SIGNAL_TYPE_REJECT,
	} {
		for _, comment := range []string{"", " ", "\t\n"} {
			t.Run(sig.String(), func(t *testing.T) {
				srv := reviewServer(t, builder.QuorumAny, 0, map[string]string{"charlie": "pending"})
				err := reviewSignal(srv, "charlie", sig, comment)
				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("comment %q: want InvalidArgument, got %v", comment, err)
				}
				if got := reviewSeat(srv, "charlie"); got != "pending" {
					t.Fatalf("seat = %q, want untouched", got)
				}
			})
		}
	}
}

// Mock: "returns a withdrawn submission to Draft": the core status write is
// the version going back to draft, made by the withdrawing actor.
func TestSignal_WithdrawWritesDraftToCore(t *testing.T) {
	srv := reviewServer(t, builder.QuorumAny, 0, map[string]string{"charlie": "pending"})
	if err := reviewSignal(srv, "author", workflowv1.SignalType_SIGNAL_TYPE_WITHDRAW, ""); err != nil {
		t.Fatalf("withdraw: %v", err)
	}
	calls := srv.coreStatus.(*stubCoreStatus).calls
	if len(calls) != 1 || calls[0].pvID != reviewPV || calls[0].status != "draft" || calls[0].actor != "author" {
		t.Fatalf("core status calls = %+v, want one (pv-review, draft, author)", calls)
	}
	if got := srv.runStore.(*stubRunStore).runStatus; got != "withdrawn" {
		t.Fatalf("run status = %q, want withdrawn", got)
	}
	if got := reviewSeat(srv, "charlie"); got != "withdrawn" {
		t.Fatalf("seat = %q, want withdrawn", got)
	}
}

// Without the core setter a withdraw cannot return the version to draft, so it
// is refused before anything changes.
func TestSignal_WithdrawWithoutCoreSetterIsUnimplemented(t *testing.T) {
	srv := reviewServer(t, builder.QuorumAny, 0, map[string]string{"charlie": "pending"})
	srv.coreStatus = nil
	err := reviewSignal(srv, "author", workflowv1.SignalType_SIGNAL_TYPE_WITHDRAW, "")
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("want Unimplemented, got %v", err)
	}
	if got := reviewSeat(srv, "charlie"); got != "pending" {
		t.Fatalf("seat = %q, want untouched", got)
	}
	if got := srv.runStore.(*stubRunStore).runStatus; got != "" {
		t.Fatalf("run status = %q, want untouched", got)
	}
}
