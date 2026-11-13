// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package sweeper

import (
	"context"
	"fmt"
	"time"

	log "github.com/Bugs5382/go-log"
	sagapg "github.com/Bugs5382/go-saga-orchestration/store/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

// LockID is the Postgres advisory lock the leader sweeper holds. It is not the
// saga timer's (engine.TimerAdvisoryLockID), so a replica that leads one never
// makes the other wait.
const LockID = int64(0x57A6E500)

// Run sweeps every interval until ctx is cancelled. Only one replica may sweep,
// or two would publish the same notices, so it first takes the advisory lock the
// way the saga timer does: every replica calls Run and the others wait on the
// lock until the leader's connection goes away.
func Run(ctx context.Context, pool *pgxpool.Pool, s *Sweeper, interval time.Duration, lg log.Logger) error {
	if interval <= 0 {
		return fmt.Errorf("stage notice sweeper: interval must be positive, got %s", interval)
	}
	lg.Info("stage notice sweeper: waiting for the leader lock", log.F("interval", interval.String()))
	release, err := sagapg.AcquireAdvisoryLock(ctx, pool, LockID)
	if err != nil {
		if ctx.Err() != nil {
			lg.Debug("stage notice sweeper: stopped while waiting for the leader lock")
			return nil
		}
		return fmt.Errorf("stage notice sweeper: %w", err)
	}
	defer release()
	lg.Info("stage notice sweeper: leader, sweeping", log.F("interval", interval.String()))

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := s.Sweep(ctx); err != nil && ctx.Err() == nil {
			// Already logged; the next tick tries again.
			lg.Warn("stage notice sweeper: sweep failed, trying again at the next tick", log.F("error", err.Error()))
		}
		select {
		case <-ctx.Done():
			lg.Info("stage notice sweeper: stopped")
			return nil
		case <-ticker.C:
		}
	}
}
