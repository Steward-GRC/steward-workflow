// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package outage

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type fakeAssignments struct {
	mu        sync.Mutex
	pauseCnt  int
	resumeCnt int
	setState  []string
}

func (f *fakeAssignments) PauseAllPending(_ context.Context, _ time.Time, _, _ string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pauseCnt++
	return []string{"a1"}, nil
}

func (f *fakeAssignments) ResumeAllPaused(_ context.Context, _ time.Time, _ string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resumeCnt++
	return []string{"a1"}, nil
}

func (f *fakeAssignments) SetAllRunsState(_ context.Context, to, _ string, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setState = append(f.setState, to)
	return nil
}

func (f *fakeAssignments) CountPausedRuns(_ context.Context) (int, error) {
	return 0, nil
}

func TestReconciler_HealthyStaysRunning(t *testing.T) {
	fa := &fakeAssignments{}
	r := NewReconciler(Config{
		HealthCheck:         func(context.Context) error { return nil },
		Assignments:         fa,
		ConsecutiveFailures: 3,
	})
	for range 5 {
		r.tick(context.Background())
	}
	if fa.pauseCnt != 0 {
		t.Fatalf("expected 0 pauses, got %d", fa.pauseCnt)
	}
	if r.State() != "running" {
		t.Fatalf("state %q", r.State())
	}
}

func TestReconciler_PausesAfterThreshold(t *testing.T) {
	fa := &fakeAssignments{}
	audited := []string{}
	r := NewReconciler(Config{
		HealthCheck:         func(context.Context) error { return errors.New("down") },
		Assignments:         fa,
		ConsecutiveFailures: 3,
		Audit:               AuditFunc(func(a, _ string) { audited = append(audited, a) }),
	})
	// 2 ticks below threshold — no pause.
	r.tick(context.Background())
	r.tick(context.Background())
	if fa.pauseCnt != 0 {
		t.Fatalf("expected 0 pauses after 2 failures, got %d", fa.pauseCnt)
	}
	// 3rd tick crosses threshold.
	r.tick(context.Background())
	if fa.pauseCnt != 1 {
		t.Fatalf("expected 1 pause, got %d", fa.pauseCnt)
	}
	if r.State() != "paused_external_dep" {
		t.Fatalf("state %q", r.State())
	}
	// 4th tick should not double-pause.
	r.tick(context.Background())
	if fa.pauseCnt != 1 {
		t.Fatalf("expected still 1 pause, got %d", fa.pauseCnt)
	}
	if len(audited) != 1 || audited[0] != "workflow.run.paused" {
		t.Fatalf("audit: %v", audited)
	}
}

func TestReconciler_ResumesOnHealthRestore(t *testing.T) {
	fa := &fakeAssignments{}
	healthy := false
	audited := []string{}
	r := NewReconciler(Config{
		HealthCheck: func(context.Context) error {
			if healthy {
				return nil
			}
			return errors.New("down")
		},
		Assignments:         fa,
		ConsecutiveFailures: 2,
		Audit:               AuditFunc(func(a, _ string) { audited = append(audited, a) }),
	})
	r.tick(context.Background())
	r.tick(context.Background())
	if r.State() != "paused_external_dep" {
		t.Fatalf("expected paused, got %q", r.State())
	}
	healthy = true
	r.tick(context.Background())
	if r.State() != "running" {
		t.Fatalf("expected running after resume, got %q", r.State())
	}
	if fa.resumeCnt != 1 {
		t.Fatalf("expected 1 resume, got %d", fa.resumeCnt)
	}
	if len(audited) != 2 || audited[1] != "workflow.run.resumed" {
		t.Fatalf("audit: %v", audited)
	}
}

func TestReconciler_FailureCounterResetsOnSuccess(t *testing.T) {
	fa := &fakeAssignments{}
	step := 0
	r := NewReconciler(Config{
		HealthCheck: func(context.Context) error {
			step++
			if step == 3 {
				return nil
			}
			return errors.New("down")
		},
		Assignments:         fa,
		ConsecutiveFailures: 4,
	})
	for range 5 {
		r.tick(context.Background())
	}
	if fa.pauseCnt != 0 {
		t.Fatalf("threshold not reached; expected 0 pauses, got %d", fa.pauseCnt)
	}
}

func TestReconciler_DefaultsApplied(t *testing.T) {
	r := NewReconciler(Config{})
	if r.cfg.Interval != 30*time.Second {
		t.Fatalf("interval default: %v", r.cfg.Interval)
	}
	if r.cfg.ConsecutiveFailures != 3 {
		t.Fatalf("failures default: %d", r.cfg.ConsecutiveFailures)
	}
	if r.cfg.ActorUserID == "" {
		t.Fatal("actor default empty")
	}
}

func TestIdentityHealthCheck(t *testing.T) {
	serving := func(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
		return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
	}
	notServing := func(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
		return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_NOT_SERVING}, nil
	}
	unknown := func(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
		return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_UNKNOWN}, nil
	}
	transportErr := func(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
		return nil, errString("connection refused")
	}

	if err := IdentityHealthCheck(serving)(context.Background()); err != nil {
		t.Fatalf("SERVING should be healthy, got %v", err)
	}
	if err := IdentityHealthCheck(notServing)(context.Background()); err == nil {
		t.Fatal("NOT_SERVING should be unhealthy")
	}
	if err := IdentityHealthCheck(unknown)(context.Background()); err == nil {
		t.Fatal("UNKNOWN should be unhealthy")
	}
	if err := IdentityHealthCheck(transportErr)(context.Background()); err == nil {
		t.Fatal("transport error should be unhealthy")
	}
}

// TestIdentityHealthCheck_EmptyServiceRequested asserts the probe queries
// overall server health (empty service name) so it matches the platform's
// default health registration.
func TestIdentityHealthCheck_EmptyServiceRequested(t *testing.T) {
	var gotService string
	call := func(_ context.Context, in *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
		gotService = in.GetService()
		return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
	}
	if err := IdentityHealthCheck(call)(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotService != "" {
		t.Fatalf("expected empty service (overall health), got %q", gotService)
	}
}

type errString string

func (e errString) Error() string { return string(e) }

func TestReconciler_StopOnCtxCancel(t *testing.T) {
	r := NewReconciler(Config{
		Interval:    10 * time.Millisecond,
		HealthCheck: func(context.Context) error { return nil },
		Assignments: &fakeAssignments{},
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.Run(ctx)
		close(done)
	}()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reconciler did not exit")
	}
}
