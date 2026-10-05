// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package saga

import (
	"context"
	"fmt"
	"time"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	sagasdk "github.com/Bugs5382/go-saga-orchestration/saga"
	sagapg "github.com/Bugs5382/go-saga-orchestration/store/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RunTimer drives the SLA and wait wakeups until ctx is cancelled. Only one
// replica may tick, or two timers would race on the same due run, so it first
// takes the engine's Postgres advisory lock: every replica calls RunTimer and
// the others wait on the lock until the leader's connection goes away.
func RunTimer(ctx context.Context, sc *sagasdk.Saga, pool *pgxpool.Pool) error {
	release, err := sagapg.AcquireAdvisoryLock(ctx, pool, engine.TimerAdvisoryLockID)
	if err != nil {
		return fmt.Errorf("timer: %w", err)
	}
	defer release()

	timer := &engine.Timer{
		S:         sc.Store(),
		Publisher: advancePublisher{coord: sc.Coordinator()},
		Clock:     clock.SystemClock{},
		Tick:      time.Second,
	}
	return timer.Run(ctx)
}
