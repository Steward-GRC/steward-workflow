// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package outage implements the identity outage reconciler.
//
// The reconciler ticks on a fixed cadence (30s by default),
// performs a lightweight health check against identity, and flips every
// in-flight workflow run between `running` and `paused_external_dep` based
// on whether the dependency is reachable. To avoid flapping it requires
// `ConsecutiveFailures` consecutive failed health checks (default 3, so
// 90s) before pausing. Single health-check success resumes.
//
// Pausing snapshots each pending assignment's remaining SLA + reminder
// durations; resuming shifts the deadlines forward by the same amount so
// approvers don't get cheated by the outage.
//
// The reconciler is intentionally a standalone goroutine — it does not run
// inside an RPC handler, so its decisions remain consistent even when the
// gRPC surface is partially unavailable.
package outage

import (
	"context"
	"time"
)

// Assignments is the subset of store.Assignments the reconciler touches.
type Assignments interface {
	PauseAllPending(ctx context.Context, now time.Time, actorUserID, reason string) ([]string, error)
	ResumeAllPaused(ctx context.Context, now time.Time, actorUserID string) ([]string, error)
	SetAllRunsState(ctx context.Context, to string, reason string, now time.Time) error
	CountPausedRuns(ctx context.Context) (int, error)
}

// HealthCheck returns nil when identity is reachable, non-nil otherwise.
type HealthCheck func(ctx context.Context) error

// Logger is a 1-arg log surface so callers can wire in their zerolog/zap/etc.
type Logger interface {
	Log(msg string)
}

// LoggerFunc adapts a bare function into a Logger.
type LoggerFunc func(msg string)

// Log implements Logger.
func (f LoggerFunc) Log(msg string) { f(msg) }

// Audit emits a workflow.run.* audit event on each pause/resume transition.
type Audit interface {
	Emit(action, subject string)
}

// AuditFunc adapts a function into an Audit.
type AuditFunc func(action, subject string)

// Emit implements Audit.
func (f AuditFunc) Emit(action, subject string) { f(action, subject) }

// Config bundles all reconciler dependencies.
type Config struct {
	Interval            time.Duration // default 30s
	HealthCheck         HealthCheck
	Assignments         Assignments
	Logger              Logger
	Audit               Audit
	ConsecutiveFailures int // default 3
	// ActorUserID labels the audit / history rows the reconciler writes.
	// Defaults to "system:outage-reconciler".
	ActorUserID string
}

// Reconciler is the long-running goroutine driver.
type Reconciler struct {
	cfg              Config
	consecutiveFails int
	// state mirrors the in-DB workflow_run_state aggregate: "running" or
	// "paused_external_dep". The reconciler is the only writer so a local
	// cache is fine for the transition guard.
	state string
}

// NewReconciler constructs a reconciler with sensible defaults.
func NewReconciler(cfg Config) *Reconciler {
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.ConsecutiveFailures <= 0 {
		cfg.ConsecutiveFailures = 3
	}
	if cfg.ActorUserID == "" {
		cfg.ActorUserID = "system:outage-reconciler"
	}
	if cfg.Logger == nil {
		cfg.Logger = LoggerFunc(func(string) {})
	}
	if cfg.Audit == nil {
		cfg.Audit = AuditFunc(func(string, string) {})
	}
	return &Reconciler{cfg: cfg, state: "running"}
}

// Run blocks until ctx is cancelled. Safe to call once per process.
func (r *Reconciler) Run(ctx context.Context) {
	t := time.NewTicker(r.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.tick(ctx)
		}
	}
}

// tick performs one health-check + transition decision. Extracted from Run
// so tests can step the reconciler deterministically without sleeping.
func (r *Reconciler) tick(ctx context.Context) {
	if r.cfg.HealthCheck == nil || r.cfg.Assignments == nil {
		return
	}
	now := time.Now().UTC()
	err := r.cfg.HealthCheck(ctx)
	if err == nil {
		// Healthy: reset failure counter; if currently paused, resume.
		if r.consecutiveFails > 0 {
			r.cfg.Logger.Log("identity health restored after failures")
		}
		r.consecutiveFails = 0
		if r.state == "paused_external_dep" {
			r.resume(ctx, now)
		}
		return
	}
	// Unhealthy: bump counter, pause if threshold crossed and not already paused.
	r.consecutiveFails++
	r.cfg.Logger.Log("identity health check failed: " + err.Error())
	if r.consecutiveFails >= r.cfg.ConsecutiveFailures && r.state == "running" {
		r.pause(ctx, now)
	}
}

func (r *Reconciler) pause(ctx context.Context, now time.Time) {
	if _, err := r.cfg.Assignments.PauseAllPending(ctx, now, r.cfg.ActorUserID, "identity_svc_unreachable"); err != nil {
		r.cfg.Logger.Log("pause assignments failed: " + err.Error())
		return
	}
	if err := r.cfg.Assignments.SetAllRunsState(ctx, "paused_external_dep", "identity_svc_unreachable", now); err != nil {
		r.cfg.Logger.Log("set runs paused failed: " + err.Error())
		return
	}
	r.state = "paused_external_dep"
	r.cfg.Audit.Emit("workflow.run.paused", "reason:identity_svc_unreachable")
	r.cfg.Logger.Log("paused all in-flight workflow runs")
}

func (r *Reconciler) resume(ctx context.Context, now time.Time) {
	if _, err := r.cfg.Assignments.ResumeAllPaused(ctx, now, r.cfg.ActorUserID); err != nil {
		r.cfg.Logger.Log("resume assignments failed: " + err.Error())
		return
	}
	if err := r.cfg.Assignments.SetAllRunsState(ctx, "running", "", now); err != nil {
		r.cfg.Logger.Log("set runs running failed: " + err.Error())
		return
	}
	r.state = "running"
	r.cfg.Audit.Emit("workflow.run.resumed", "reason:identity_svc_resumed")
	r.cfg.Logger.Log("resumed all paused workflow runs")
}

// State exposes the reconciler's view of the world for tests / metrics.
func (r *Reconciler) State() string { return r.state }
