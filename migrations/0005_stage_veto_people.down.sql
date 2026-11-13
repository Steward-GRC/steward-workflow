-- Copyright 2026 The Steward Authors
-- SPDX-License-Identifier: Apache-2.0

-- Drops veto_user_ids from every stage. The old veto_approver_ids and
-- veto_group_ids keys were never removed, so the earlier marks are intact.
UPDATE workflow_defs
   SET stages_json = (SELECT COALESCE(jsonb_agg(s.stage - 'veto_user_ids' ORDER BY s.pos), '[]'::jsonb)
                        FROM jsonb_array_elements(stages_json) WITH ORDINALITY AS s(stage, pos))
 WHERE jsonb_typeof(stages_json) = 'array';

UPDATE workflow_def_versions
   SET stages_json = (SELECT COALESCE(jsonb_agg(s.stage - 'veto_user_ids' ORDER BY s.pos), '[]'::jsonb)
                        FROM jsonb_array_elements(stages_json) WITH ORDINALITY AS s(stage, pos))
 WHERE jsonb_typeof(stages_json) = 'array';
