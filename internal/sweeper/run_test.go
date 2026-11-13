// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package sweeper_test

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	log "github.com/Bugs5382/go-log"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	sagapg "github.com/Bugs5382/go-saga-orchestration/store/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/Steward-GRC/steward-workflow/internal/store"
	"github.com/Steward-GRC/steward-workflow/internal/sweeper"
)

// testPool returns a pool on DATABASE_TEST_DSN when set, otherwise on a
// throwaway Postgres container. Only advisory locks are used, so no schema.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	dsn := os.Getenv("DATABASE_TEST_DSN")
	if dsn == "" {
		c, err := tcpostgres.Run(ctx, "postgres:16",
			tcpostgres.WithDatabase("sweeper_test"), tcpostgres.WithUsername("test"), tcpostgres.WithPassword("test"),
			testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(60*time.Second)))
		require.NoError(t, err)
		t.Cleanup(func() { _ = c.Terminate(context.Background()) })
		dsn, err = c.ConnectionString(ctx, "sslmode=disable")
		require.NoError(t, err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

// countingStore counts the sweeps by the one read each makes.
type countingStore struct {
	fakeStore
	sweeps atomic.Int64
}

func (c *countingStore) ListOverdueSeats(ctx context.Context, now time.Time) ([]store.OverdueSeat, error) {
	c.sweeps.Add(1)
	return c.fakeStore.ListOverdueSeats(ctx, now)
}

func newCounting(t *testing.T) (*countingStore, *sweeper.Sweeper) {
	w := newWorld(t, overdueStage())
	cs := &countingStore{fakeStore: fakeStore(*w)}
	return cs, sweeper.New(sweeper.Deps{
		Store: cs, Defs: (*fakeDefs)(w), Runs: (*fakeRuns)(w), Owners: (*fakeOwners)(w),
		Publisher: (*fakePub)(w), Audit: (*fakeAudit)(w), Clock: w.clk, Logger: log.Nop(),
	})
}

// The sweeper's lock is not the timer's, so a replica holding one never makes
// the other wait.
func TestLockID_IsNotTheTimers(t *testing.T) {
	require.NotEqual(t, engine.TimerAdvisoryLockID, sweeper.LockID)
}

// Of two replicas only the one holding the advisory lock sweeps; when it goes,
// the other takes over.
func TestRun_OnlyTheLeaderSweeps(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	leaderStore, leader := newCounting(t)
	leaderCtx, stopLeader := context.WithCancel(ctx)
	leaderDone := make(chan error, 1)
	go func() { leaderDone <- sweeper.Run(leaderCtx, pool, leader, 10*time.Millisecond, log.Nop()) }()
	require.Eventually(t, func() bool { return leaderStore.sweeps.Load() >= 2 }, 10*time.Second, 5*time.Millisecond, "the first replica sweeps")

	followerStore, follower := newCounting(t)
	followerCtx, stopFollower := context.WithCancel(ctx)
	defer stopFollower()
	followerDone := make(chan error, 1)
	go func() { followerDone <- sweeper.Run(followerCtx, pool, follower, 10*time.Millisecond, log.Nop()) }()

	before := leaderStore.sweeps.Load()
	require.Eventually(t, func() bool { return leaderStore.sweeps.Load() >= before+3 }, 10*time.Second, 5*time.Millisecond)
	require.Zero(t, followerStore.sweeps.Load(), "the second replica waits on the lock and never sweeps")

	stopLeader()
	require.NoError(t, <-leaderDone, "stopping the leader is not an error")
	require.Eventually(t, func() bool { return followerStore.sweeps.Load() >= 2 }, 10*time.Second, 5*time.Millisecond, "the follower takes over")

	stopFollower()
	require.NoError(t, <-followerDone)
}

// A cancelled context while waiting for the lock ends Run cleanly.
func TestRun_StopsWhileWaitingForTheLock(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	release, err := sagapg.AcquireAdvisoryLock(ctx, pool, sweeper.LockID)
	require.NoError(t, err)
	defer release()

	cs, s := newCounting(t)
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- sweeper.Run(runCtx, pool, s, 10*time.Millisecond, log.Nop()) }()
	time.Sleep(100 * time.Millisecond)
	require.Zero(t, cs.sweeps.Load())
	stop()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop")
	}
}
