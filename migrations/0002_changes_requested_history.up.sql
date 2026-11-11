-- Copyright 2026 The Steward Authors
-- SPDX-License-Identifier: Apache-2.0

-- Requesting changes keeps the approver's seat pending and records the comment
-- as a history row of its own.
ALTER TABLE assignment_history DROP CONSTRAINT assignment_history_event_check;
ALTER TABLE assignment_history ADD CONSTRAINT assignment_history_event_check CHECK (event IN
    ('created','swapped_in','swapped_out','approved','rejected','paused','resumed','withdrawn','reassigned',
     'changes_requested'));
