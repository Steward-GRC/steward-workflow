-- Copyright 2026 The Steward Authors
-- SPDX-License-Identifier: Apache-2.0

-- A run carries the change type lowering the author asked for at submit
-- (steward-workflow#10), and when the first stage's quorum agreed to it. Core
-- stores the pick and who agreed; the run keeps the lowering so the decision
-- that completes the first stage's quorum knows an agreement is due, and the
-- agreement is written to core once. requested_change_type is empty when core
-- applied the pick at once or no pick was made.
ALTER TABLE approval_runs
    ADD COLUMN requested_change_type text NOT NULL DEFAULT '',
    ADD COLUMN change_type_agreed_at timestamptz;
