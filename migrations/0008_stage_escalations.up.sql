-- Copyright 2026 The Steward Authors
-- SPDX-License-Identifier: Apache-2.0

-- The stage notice sweeper (steward-workflow#76). reminders_sent is the last
-- reminder number published for the seat; a number is recorded only after its
-- publish succeeded, so a failed publish is retried and never counted twice.
-- A seat that is created again (a re-entered stage) starts over at 0.
ALTER TABLE approval_assignments ADD COLUMN reminders_sent int NOT NULL DEFAULT 0;

-- The sweeper reads pending seats whose due date has passed.
CREATE INDEX approval_assignments_overdue_idx
    ON approval_assignments (sla_deadline_at)
    WHERE state = 'pending';

-- A stage whose owners were told it is overdue, once per run, so the notice
-- never repeats across sweeps, restarts or replicas. No foreign key: the run is
-- kept for the record and the row is only ever read by its key.
CREATE TABLE stage_escalations (
    run_id            text        NOT NULL,
    stage_index       int         NOT NULL,
    policy_version_id text        NOT NULL,
    escalated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, stage_index)
);
