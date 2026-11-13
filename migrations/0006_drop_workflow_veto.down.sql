-- Copyright 2026 The Steward Authors
-- SPDX-License-Identifier: Apache-2.0

-- Restores the column, off for every workflow. The old values and the dropped
-- veto_approver_ids and veto_group_ids keys cannot be recovered; the stage
-- veto_user_ids written by 0005 carry the same people.
ALTER TABLE workflow_defs ADD COLUMN veto_enabled boolean NOT NULL DEFAULT false;
