// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package saga_test

// Real-Postgres lifecycle tests for the approval/saga inbox. These exercise the
// WHOLE lifecycle against a real go-saga Postgres store (schema "runtime") plus
// the workflow store (approval_runs / approval_assignments), wired exactly as
// cmd/server wires them: a real EmbeddedSagaClient over sagapg, a real
// WorkflowServer, real Assignments / AssignmentStore / WorkflowDefStore and a
// real DecisionResolver.
//
// They encode the bug-fix REQUIREMENTS that the prior mock-seam unit tests could
// not catch:
//   - re-submit is idempotent (exactly one active run + one pending task);
//   - a submit self-heals N orphaned paused runs down to one;
//   - the inbox is filtered to the calling approver;
//   - withdraw clears the inbox and the run is terminal;
//   - reject drives the run terminal (no longer paused).

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	log "github.com/Bugs5382/go-log"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	sagapg "github.com/Bugs5382/go-saga-orchestration/store/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	pg "github.com/Bugs5382/go-postgres"
	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/grpcsvc"
	sagaclient "github.com/Steward-GRC/steward-workflow/internal/saga"
	"github.com/Steward-GRC/steward-workflow/internal/store"
)

// lifecycleHarness bundles a fully-wired stack over a single Postgres database.
type lifecycleHarness struct {
	pool        *pg.DB
	saga        *sagaclient.EmbeddedSagaClient
	srv         *grpcsvc.WorkflowServer
	defID       string
	assignments *store.Assignments
	policy      *recordingPolicyClient
	coreStatus  *recordingCoreStatus
}

// newLifecycleHarness migrates BOTH the saga "runtime" schema and the workflow
// schema into one DB (mirroring cmd/server), then wires the real stack. A
// single-stage any-quorum workflow def is created and the WorkflowResolver is
// stubbed to return it (the resolver itself needs core; not under test here).
func newLifecycleHarness(t *testing.T) *lifecycleHarness {
	t.Helper()
	ctx := context.Background()
	dsn, pool := newSagaTestDB(t)

	sagaStore, err := sagapg.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open saga store: %v", err)
	}
	t.Cleanup(sagaStore.Close)

	defStore := store.NewWorkflowDefStore(pool)
	assignStore := store.NewAssignmentStore(pool)
	assignments := store.NewAssignments(pool)

	// One stage, ANY quorum, two approvers (so the inbox-filter test has two
	// distinct owners on the same stage).
	wd := builder.WorkflowDef{
		Name: "Lifecycle", Version: 1, Published: true,
		Stages: []builder.Stage{{
			Name: "S0", Quorum: builder.QuorumAny,
			ApproverIDs: []string{"approverA", "approverB"},
		}},
	}
	defID, err := defStore.Create(ctx, wd)
	if err != nil {
		t.Fatalf("create def: %v", err)
	}

	resolver := sagaclient.NewDecisionResolver(assignStore, defStore, assignments)
	assigner := sagaclient.NewStageAssigner(defStore, assignments, builder.CompileOptions{StageSLAHours: 72, StageReminderHours: 48})
	policy := &recordingPolicyClient{decision: "approve"}
	engine, err := sagaclient.Build(sagaStore, policy, resolver, assigner, log.Nop())
	if err != nil {
		t.Fatalf("build engine: %v", err)
	}
	sagaCli := sagaclient.NewEmbedded(engine)

	coreStatus := &recordingCoreStatus{}
	srv := grpcsvc.NewWorkflowServer(grpcsvc.WorkflowServerOpts{
		Resolver:    fixedResolver{defID: defID},
		DefStore:    defStore,
		RunStore:    assignStore,
		SagaClient:  sagaCli,
		AuditEmit:   noopAudit{},
		Assignments: assignments,
		CoreStatus:  coreStatus,
		CompileOpts: builder.CompileOptions{StageSLAHours: 72, StageReminderHours: 48},
	})

	return &lifecycleHarness{
		pool: pool, saga: sagaCli, srv: srv, defID: defID,
		assignments: assignments, policy: policy, coreStatus: coreStatus,
	}
}

// activeRunsForVersion counts non-terminal saga runs pinned to the version.
func (h *lifecycleHarness) activeRunsForVersion(t *testing.T, pvID string) int {
	t.Helper()
	tasks, err := h.saga.ListPendingTasks(context.Background(), "")
	_ = tasks
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}
	// Count via the saga store directly through the pending-task path is
	// paused-only; for a precise active count, query runtime.saga_runs.
	var n int
	err = h.pool.Querier().QueryRow(context.Background(),
		`SELECT count(*) FROM runtime.saga_runs
		   WHERE inputs->>'policy_version_id' = $1
		     AND state NOT IN ('succeeded','failed','cancelled')`,
		pvID,
	).Scan(&n)
	if err != nil {
		t.Fatalf("count active runs: %v", err)
	}
	return n
}

// runStateByID reads a single run's state from the saga store.
func (h *lifecycleHarness) runState(t *testing.T, runID string) domain.RunState {
	t.Helper()
	var s string
	err := h.pool.Querier().QueryRow(context.Background(),
		`SELECT state FROM runtime.saga_runs WHERE id = $1`, runID,
	).Scan(&s)
	if err != nil {
		t.Fatalf("read run state %s: %v", runID, err)
	}
	return domain.RunState(s)
}

// waitForPause blocks until the run pinned to pvID is paused (the manual_approval
// gate), returning its run id. Submit returns before the engine has parked the
// run, so callers must wait.
func (h *lifecycleHarness) waitForPause(t *testing.T, pvID string) string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var runID string
		err := h.pool.Querier().QueryRow(context.Background(),
			`SELECT id FROM runtime.saga_runs
			   WHERE inputs->>'policy_version_id' = $1 AND state = 'paused'
			   ORDER BY started_at DESC LIMIT 1`,
			pvID,
		).Scan(&runID)
		if err == nil && runID != "" {
			return runID
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no paused run appeared for %s", pvID)
	return ""
}

// inbox returns the policy-version ids of the tasks the approver sees.
func (h *lifecycleHarness) inbox(t *testing.T, approver string) []string {
	t.Helper()
	resp, err := h.srv.ListPendingTasks(context.Background(),
		&workflowv1.ListPendingTasksRequest{ApproverUserId: approver})
	if err != nil {
		t.Fatalf("ListPendingTasks(%s): %v", approver, err)
	}
	out := make([]string, 0, len(resp.Tasks))
	for _, tk := range resp.Tasks {
		out = append(out, tk.PolicyVersionId)
	}
	return out
}

func (h *lifecycleHarness) submit(t *testing.T, pvID string) {
	t.Helper()
	_, err := h.srv.Submit(context.Background(), &workflowv1.SubmitRequest{
		PolicyVersionId: pvID, PolicyId: "pol-1", CategoryId: "g1",
		SubmittedBy: "submitter", AncestorCategoryIds: []string{"g1"},
	})
	if err != nil {
		t.Fatalf("Submit(%s): %v", pvID, err)
	}
}

// --- tests ---

// TestLifecycle_ReSubmitIdempotent: submit a version, then submit again →
// exactly ONE active saga run and ONE pending task for the version.
func TestLifecycle_ReSubmitIdempotent(t *testing.T) {
	h := newLifecycleHarness(t)
	const pv = "pv-resubmit"

	h.submit(t, pv)
	h.waitForPause(t, pv)
	if got := h.activeRunsForVersion(t, pv); got != 1 {
		t.Fatalf("after first submit: want 1 active run, got %d", got)
	}

	h.submit(t, pv)
	h.waitForPause(t, pv)
	if got := h.activeRunsForVersion(t, pv); got != 1 {
		t.Fatalf("after re-submit: want 1 active run (no accumulation), got %d", got)
	}

	// approverA owns the (single) stage-0 task; exactly one inbox entry.
	if box := h.inbox(t, "approverA"); len(box) != 1 || box[0] != pv {
		t.Fatalf("approverA inbox: want [%s], got %v", pv, box)
	}
}

// TestLifecycle_SelfHeal: given N pre-existing paused runs for a version, a
// submit leaves exactly one active run.
func TestLifecycle_SelfHeal(t *testing.T) {
	h := newLifecycleHarness(t)
	const pv = "pv-selfheal"

	// Manufacture 6 orphaned paused runs by starting the saga directly (bypassing
	// Submit's cancel-prior step) — exactly the accumulation the live bug produced.
	def, err := builder.Compile(builder.WorkflowDef{
		ID: h.defID, Name: "Lifecycle", Version: 1, Published: true,
		Stages: []builder.Stage{{Name: "S0", Quorum: builder.QuorumAny, ApproverIDs: []string{"approverA", "approverB"}}},
	}, builder.CompileOptions{StageSLAHours: 72, StageReminderHours: 48})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if err := h.saga.PublishWorkflow(context.Background(), def); err != nil {
		t.Fatalf("publish: %v", err)
	}
	for i := range 6 {
		// Seed inputs mirror what Submit injects (policy_version_id +
		// workflow_def_id/version) so the s0_assign stage-entry action can run and
		// the run parks at the manual_approval pause, matching production.
		if _, err := h.saga.StartRun(context.Background(), def.ID, map[string]any{
			"policy_version_id": pv,
			"workflow_def_id":   h.defID,
			"workflow_version":  1,
			"approvers_s0":      []string{"approverA", "approverB"},
		}); err != nil {
			t.Fatalf("seed run %d: %v", i, err)
		}
	}
	// Wait until all 6 have parked at the manual_approval pause.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && h.activeRunsForVersion(t, pv) < 6 {
		time.Sleep(10 * time.Millisecond)
	}
	if got := h.activeRunsForVersion(t, pv); got != 6 {
		t.Fatalf("seed: want 6 orphaned active runs, got %d", got)
	}

	// A single submit must self-heal: cancel the 6 and leave exactly one.
	h.submit(t, pv)
	h.waitForPause(t, pv)
	if got := h.activeRunsForVersion(t, pv); got != 1 {
		t.Fatalf("after self-heal submit: want 1 active run, got %d", got)
	}
}

// TestLifecycle_InboxFilteredByApprover: two approvers each own a task on
// different versions; each approver sees only their own.
func TestLifecycle_InboxFilteredByApprover(t *testing.T) {
	h := newLifecycleHarness(t)

	// pvA is owned by approverA; remove approverB's pending row so the stage's
	// single task belongs to A. pvB the mirror for B.
	h.submit(t, "pvA")
	h.waitForPause(t, "pvA")
	h.submit(t, "pvB")
	h.waitForPause(t, "pvB")

	// Each version seeded approverA + approverB pending. Decide away the other so
	// each version has a single owner. Use the store to terminate the peer.
	mustExec(t, h.pool,
		`UPDATE approval_assignments SET state='superseded'
		   WHERE policy_version_id='pvA' AND user_id='approverB'`)
	mustExec(t, h.pool,
		`UPDATE approval_assignments SET state='superseded'
		   WHERE policy_version_id='pvB' AND user_id='approverA'`)

	if box := h.inbox(t, "approverA"); len(box) != 1 || box[0] != "pvA" {
		t.Fatalf("approverA inbox: want [pvA], got %v", box)
	}
	if box := h.inbox(t, "approverB"); len(box) != 1 || box[0] != "pvB" {
		t.Fatalf("approverB inbox: want [pvB], got %v", box)
	}
}

// TestLifecycle_WithdrawClearsInbox: after withdraw, the approver's inbox is
// empty for the version and the saga run is terminal (cancelled).
func TestLifecycle_WithdrawClearsInbox(t *testing.T) {
	h := newLifecycleHarness(t)
	const pv = "pv-withdraw"

	h.submit(t, pv)
	runID := h.waitForPause(t, pv)
	if box := h.inbox(t, "approverA"); len(box) != 1 {
		t.Fatalf("pre-withdraw inbox: want 1, got %v", box)
	}

	_, err := h.srv.Signal(context.Background(), &workflowv1.SignalRequest{
		RunId: runID, Signal: workflowv1.SignalType_SIGNAL_TYPE_WITHDRAW, ActorUserId: "approverA",
	})
	if err != nil {
		t.Fatalf("withdraw: %v", err)
	}

	if box := h.inbox(t, "approverA"); len(box) != 0 {
		t.Fatalf("post-withdraw inbox: want empty, got %v", box)
	}
	if st := h.runState(t, runID); !st.IsTerminal() {
		t.Fatalf("post-withdraw run state = %s, want terminal", st)
	}
	if got := h.activeRunsForVersion(t, pv); got != 0 {
		t.Fatalf("post-withdraw active runs = %d, want 0", got)
	}
	// D4: the withdraw must have returned the core version to draft.
	if h.coreStatus.calls != 1 || h.coreStatus.lastStatus != "draft" {
		t.Fatalf("post-withdraw core status: calls=%d last=%q, want 1 call to draft",
			h.coreStatus.calls, h.coreStatus.lastStatus)
	}
}

// TestLifecycle_RejectTerminates: a reject decision drives the run terminal
// (no longer paused) so it leaves the inbox.
func TestLifecycle_RejectTerminates(t *testing.T) {
	h := newLifecycleHarness(t)
	const pv = "pv-reject"

	h.submit(t, pv)
	runID := h.waitForPause(t, pv)

	// Find approverA's pending task to carry the task_id (mirrors the gateway).
	taskID := h.pendingTaskID(t, runID)
	_, err := h.srv.Signal(context.Background(), &workflowv1.SignalRequest{
		RunId: runID, TaskId: taskID, Signal: workflowv1.SignalType_SIGNAL_TYPE_REJECT,
		ActorUserId: "approverA", Comment: "no",
	})
	if err != nil {
		t.Fatalf("reject: %v", err)
	}

	if st := h.runState(t, runID); st == domain.RunStatePaused {
		t.Fatalf("post-reject run is still paused; want terminal")
	}
	if box := h.inbox(t, "approverA"); len(box) != 0 {
		t.Fatalf("post-reject inbox: want empty, got %v", box)
	}
}

// TestLifecycle_ApproveCompletes: an approval (ANY quorum, one approver) drives
// the run terminal so it leaves the inbox.
func TestLifecycle_ApproveCompletes(t *testing.T) {
	h := newLifecycleHarness(t)
	const pv = "pv-approve"

	h.submit(t, pv)
	runID := h.waitForPause(t, pv)
	taskID := h.pendingTaskID(t, runID)

	_, err := h.srv.Signal(context.Background(), &workflowv1.SignalRequest{
		RunId: runID, TaskId: taskID, Signal: workflowv1.SignalType_SIGNAL_TYPE_APPROVE,
		ActorUserId: "approverA", Comment: "ok",
	})
	if err != nil {
		t.Fatalf("approve: %v", err)
	}

	// ANY quorum → first approval resolves the stage → run completes.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if h.runState(t, runID) != domain.RunStatePaused {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if st := h.runState(t, runID); st == domain.RunStatePaused {
		t.Fatalf("post-approve run still paused; want terminal")
	}
}

func (h *lifecycleHarness) pendingTaskID(t *testing.T, runID string) string {
	t.Helper()
	rid := uuid.MustParse(runID)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		// Reuse the engine store via a fresh query against the runtime table.
		var id string
		err := h.pool.Querier().QueryRow(context.Background(),
			`SELECT id FROM runtime.saga_user_tasks
			   WHERE run_id = $1 AND submitted_at IS NULL
			   ORDER BY id DESC LIMIT 1`, rid,
		).Scan(&id)
		if err == nil && id != "" {
			return id
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no pending task for run %s", runID)
	return ""
}

// --- test infra (fakes + DB harness) ---

type fixedResolver struct{ defID string }

func (f fixedResolver) Resolve(_ context.Context, _ string, _ []string) (string, error) {
	return f.defID, nil
}

type noopAudit struct{}

func (noopAudit) Emit(context.Context, string, string)              {}
func (noopAudit) EmitActor(context.Context, string, string, string) {}
func (noopAudit) EmitActorAttrs(context.Context, string, string, string, string, map[string]string) {
}

// recordingCoreStatus records the core version-status writes the withdraw path
// makes (the withdraw revert requires a wired CoreStatusSetter).
type recordingCoreStatus struct {
	lastStatus string
	calls      int
}

func (c *recordingCoreStatus) SetVersionStatus(_ context.Context, _, status, _ string) error {
	c.calls++
	c.lastStatus = status
	return nil
}

func mustExec(t *testing.T, pool *pg.DB, sql string) {
	t.Helper()
	if _, err := pool.Querier().Exec(context.Background(), sql); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

// newSagaTestDB provisions one Postgres database with BOTH the workflow schema
// (separate migration table) and the saga "runtime" schema migrated in, exactly
// as cmd/server does. Uses DATABASE_TEST_DSN when set (CI), else a container.
func newSagaTestDB(t *testing.T) (string, *pg.DB) {
	t.Helper()
	ctx := context.Background()

	var dsn string
	if base := os.Getenv("DATABASE_TEST_DSN"); base != "" {
		admin, err := pgxpool.New(ctx, base)
		if err != nil {
			t.Fatalf("connect admin pool: %v", err)
		}
		dbName := uniqueSagaDBName()
		if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %q`, dbName)); err != nil {
			admin.Close()
			t.Fatalf("create db %s: %v", dbName, err)
		}
		admin.Close()
		u, err := url.Parse(base)
		if err != nil {
			t.Fatalf("parse base dsn: %v", err)
		}
		u.Path = "/" + dbName
		dsn = u.String()
		t.Cleanup(func() {
			dropCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			da, err := pgxpool.New(dropCtx, base)
			if err != nil {
				return
			}
			defer da.Close()
			_, _ = da.Exec(dropCtx, fmt.Sprintf(`DROP DATABASE %q WITH (FORCE)`, dbName))
		})
	} else {
		container, err := postgres.Run(ctx, "postgres:16",
			postgres.WithDatabase("workflow_test"),
			postgres.WithUsername("test"),
			postgres.WithPassword("test"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).WithStartupTimeout(60*time.Second),
			),
		)
		if err != nil {
			t.Fatalf("start postgres container: %v", err)
		}
		t.Cleanup(func() { _ = container.Terminate(context.Background()) })
		dsn, err = container.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			t.Fatalf("connection string: %v", err)
		}
	}

	migrationsDir, err := filepath.Abs("../../migrations")
	if err != nil {
		t.Fatalf("resolve migrations dir: %v", err)
	}
	// Workflow schema under its own migration table (mirrors cmd/server) so it
	// does not collide with the saga store's default schema_migrations table.
	if err := pg.MigrateWithTable(dsn, migrationsDir, store.MigrationsTable); err != nil {
		t.Fatalf("migrate workflow schema: %v", err)
	}
	// Saga "runtime" schema.
	if err := sagapg.Migrate(dsn); err != nil {
		t.Fatalf("migrate saga schema: %v", err)
	}

	pool, err := pg.New(ctx, dsn)
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}
	t.Cleanup(pool.Close)
	return dsn, pool
}

func uniqueSagaDBName() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return "saga_test_" + hex.EncodeToString(b[:])
}
