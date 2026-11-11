-- Copyright 2026 The Steward Authors
-- SPDX-License-Identifier: Apache-2.0

DELETE FROM assignment_history WHERE event = 'changes_requested';
ALTER TABLE assignment_history DROP CONSTRAINT assignment_history_event_check;
ALTER TABLE assignment_history ADD CONSTRAINT assignment_history_event_check CHECK (event IN
    ('created','swapped_in','swapped_out','approved','rejected','paused','resumed','withdrawn','reassigned'));
