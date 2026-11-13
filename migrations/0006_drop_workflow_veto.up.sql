-- Copyright 2026 The Steward Authors
-- SPDX-License-Identifier: Apache-2.0

-- Cleanup after the per-stage veto people (steward-workflow#64, migration 0005).
--
-- 0005 gave each stage of a workflow whose switch was on a veto_user_ids list
-- (its old veto_approver_ids and the members of its veto_group_ids groups,
-- limited to the stage's own approvers), on the live definition and on every
-- kept version. A workflow whose switch was off got nothing. So the old data is
-- carried over and this migration drops it:
--   - workflow_defs.veto_enabled, which gates nothing;
--   - the old veto_approver_ids and veto_group_ids keys in stages_json, on the
--     live definition and on every kept version.
-- Stage veto_user_ids and approval_assignments.veto (the seat mark) stay.

ALTER TABLE workflow_defs DROP COLUMN veto_enabled;

UPDATE workflow_defs
   SET stages_json = (SELECT COALESCE(jsonb_agg(s.stage - 'veto_approver_ids' - 'veto_group_ids' ORDER BY s.pos), '[]'::jsonb)
                        FROM jsonb_array_elements(stages_json) WITH ORDINALITY AS s(stage, pos))
 WHERE jsonb_typeof(stages_json) = 'array';

UPDATE workflow_def_versions
   SET stages_json = (SELECT COALESCE(jsonb_agg(s.stage - 'veto_approver_ids' - 'veto_group_ids' ORDER BY s.pos), '[]'::jsonb)
                        FROM jsonb_array_elements(stages_json) WITH ORDINALITY AS s(stage, pos))
 WHERE jsonb_typeof(stages_json) = 'array';
