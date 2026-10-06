// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package readiness_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Bugs5382/go-buildinfo/health"
	"github.com/stretchr/testify/require"

	"github.com/Steward-GRC/steward-workflow/internal/readiness"
)

type fakeDB struct{ down atomic.Bool }

func (f *fakeDB) Ping(context.Context) error {
	if f.down.Load() {
		return errors.New("connection refused")
	}
	return nil
}

func (*fakeDB) ServerVersion(context.Context) (string, error) { return "16.4", nil }

type fakeBroker struct{ down atomic.Bool }

func (f *fakeBroker) Healthy() bool { return !f.down.Load() }

type fakePeer struct{ down atomic.Bool }

func (f *fakePeer) check(context.Context) error {
	if f.down.Load() {
		return errors.New("unavailable")
	}
	return nil
}

func checker(t *testing.T, d readiness.Deps) *health.Checker {
	t.Helper()
	c, err := readiness.New(d, health.WithTTL(time.Millisecond), health.WithTimeout(time.Second))
	require.NoError(t, err)
	return c
}

func dep(t *testing.T, r health.Report, name string) health.DependencyReport {
	t.Helper()
	for _, d := range r.Dependencies {
		if d.Name == name {
			return d
		}
	}
	t.Fatalf("no %q in the report", name)
	return health.DependencyReport{}
}

func deps() (readiness.Deps, *fakeDB, *fakeBroker, *fakePeer, *fakePeer) {
	db, mq, idn, core := &fakeDB{}, &fakeBroker{}, &fakePeer{}, &fakePeer{}
	return readiness.Deps{Postgres: db, Broker: mq, Identity: idn.check, Core: core.check}, db, mq, idn, core
}

func TestEveryDependencyIsReported(t *testing.T) {
	d, _, _, _, _ := deps()
	r := checker(t, d).Report(context.Background())
	require.True(t, r.Ready)
	require.Equal(t, health.StateOK, r.Status)
	require.True(t, dep(t, r, readiness.Postgres).Required)
	require.True(t, dep(t, r, readiness.RabbitMQ).Required)
	require.False(t, dep(t, r, readiness.Identity).Required)
	require.False(t, dep(t, r, readiness.Core).Required)
	require.Equal(t, "16.4", dep(t, r, readiness.Postgres).Version)
}

func TestPostgresDownMakesWorkflowNotReadyAndRecovers(t *testing.T) {
	d, db, _, _, _ := deps()
	c := checker(t, d)
	db.down.Store(true)
	require.Eventually(t, func() bool { return !c.Report(context.Background()).Ready }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, health.StateDown, dep(t, c.Report(context.Background()), readiness.Postgres).State)
	db.down.Store(false)
	require.Eventually(t, func() bool { return c.Report(context.Background()).Ready }, 2*time.Second, 5*time.Millisecond)
}

func TestRabbitMQDownMakesWorkflowNotReadyAndRecovers(t *testing.T) {
	d, _, mq, _, _ := deps()
	c := checker(t, d)
	mq.down.Store(true)
	require.Eventually(t, func() bool { return !c.Report(context.Background()).Ready }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, health.StateDown, dep(t, c.Report(context.Background()), readiness.RabbitMQ).State)
	mq.down.Store(false)
	require.Eventually(t, func() bool { return c.Report(context.Background()).Ready }, 2*time.Second, 5*time.Millisecond)
}

func TestIdentityAndCoreDownDegradeButStayReady(t *testing.T) {
	d, _, _, idn, core := deps()
	c := checker(t, d)
	idn.down.Store(true)
	core.down.Store(true)
	require.Eventually(t, func() bool { return c.Report(context.Background()).Status == health.StateDegraded }, 2*time.Second, 5*time.Millisecond)
	r := c.Report(context.Background())
	require.True(t, r.Ready, "an identity outage pauses runs instead; core calls fail per request")
	require.Equal(t, health.StateDegraded, dep(t, r, readiness.Identity).State)
	require.Equal(t, health.StateDegraded, dep(t, r, readiness.Core).State)
}

func TestJWKSDownMakesWorkflowNotReady(t *testing.T) {
	var down atomic.Bool
	d, _, _, _, _ := deps()
	d.JWKS = func(context.Context) error {
		if down.Load() {
			return errors.New("connection refused")
		}
		return nil
	}
	c := checker(t, d)
	r := c.Report(context.Background())
	require.True(t, r.Ready)
	require.True(t, dep(t, r, readiness.JWKS).Required, "callers can't be verified without the issuer's keys")
	down.Store(true)
	require.Eventually(t, func() bool { return !c.Report(context.Background()).Ready }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, health.StateDown, dep(t, c.Report(context.Background()), readiness.JWKS).State)
}

func TestWorkloadAuthDisabledDegradesButStaysReady(t *testing.T) {
	d, _, _, _, _ := deps()
	d.WorkloadAuthDisabled = true
	r := checker(t, d).Report(context.Background())
	require.True(t, r.Ready)
	require.Equal(t, health.StateDegraded, r.Status)
	wa := dep(t, r, readiness.WorkloadAuth)
	require.False(t, wa.Required)
	require.Equal(t, health.StateDegraded, wa.State)
	for _, x := range r.Dependencies {
		require.NotEqual(t, readiness.JWKS, x.Name, "no key set is checked while authentication is off")
	}
}

func TestRecheckEveryKeepsASuccessAndRetriesAFailure(t *testing.T) {
	now := time.Unix(1000, 0)
	var calls atomic.Int32
	var fail atomic.Bool
	check := readiness.RecheckEvery(func(context.Context) error {
		calls.Add(1)
		if fail.Load() {
			return errors.New("down")
		}
		return nil
	}, time.Minute, func() time.Time { return now })
	ctx := context.Background()
	require.NoError(t, check(ctx))
	require.NoError(t, check(ctx))
	require.Equal(t, int32(1), calls.Load(), "a success is kept for the interval")
	now = now.Add(time.Minute)
	fail.Store(true)
	require.Error(t, check(ctx))
	require.Error(t, check(ctx))
	require.Equal(t, int32(3), calls.Load(), "a failure is retried on the next check")
	fail.Store(false)
	require.NoError(t, check(ctx))
}
