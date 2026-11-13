-- Copyright 2026 The Steward Authors
-- SPDX-License-Identifier: Apache-2.0

-- The veto belongs to people on a stage, not to the workflow (steward-workflow#64).
-- A stage now carries veto_user_ids in its stages_json, and the workflow-wide
-- switch (workflow_defs.veto_enabled) gates nothing any more.
--
-- For a workflow whose switch is on, each stage gets veto_user_ids: its
-- veto_approver_ids, then the members of the group units named in its
-- veto_group_ids (a group unit stores its members in the stage, so no lookup in
-- identity is needed and the people are exactly the ones the old seating gave a
-- veto), without duplicates, and only people the stage names in its own
-- approver lists (approver_ids, approvers_by_category, group unit members), so
-- the result passes the save-time check. A stage with nobody left gets no key.
-- For a workflow whose switch is off nothing is added: its marks were inert and
-- must not become active. The kept versions of an enabled workflow are changed
-- the same way, so a run pinned to an older version still vetoes. The old
-- veto_approver_ids and veto_group_ids keys stay in the JSON, ignored, until the
-- cleanup change removes them.

-- The people one stage's old marks name, in order, from its own approver lists.
CREATE FUNCTION steward_stage_veto_people(stage jsonb) RETURNS jsonb
LANGUAGE sql AS $$
WITH pool AS (
    SELECT a.id
      FROM jsonb_array_elements_text(COALESCE(stage -> 'approver_ids', '[]'::jsonb)) AS a(id)
    UNION
    SELECT c.id
      FROM jsonb_each(COALESCE(stage -> 'approvers_by_category', '{}'::jsonb)) AS e(category, ids)
      CROSS JOIN LATERAL jsonb_array_elements_text(e.ids) AS c(id)
    UNION
    SELECT m.id
      FROM jsonb_array_elements(COALESCE(stage -> 'group_units', '[]'::jsonb)) AS u(unit)
      CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(u.unit -> 'members', '[]'::jsonb)) AS m(id)
), wanted AS (
    SELECT a.id, a.pos
      FROM jsonb_array_elements_text(COALESCE(stage -> 'veto_approver_ids', '[]'::jsonb))
           WITH ORDINALITY AS a(id, pos)
    UNION ALL
    SELECT m.id, 1000000 + u.pos * 1000 + m.pos
      FROM jsonb_array_elements(COALESCE(stage -> 'group_units', '[]'::jsonb))
           WITH ORDINALITY AS u(unit, pos)
      CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(u.unit -> 'members', '[]'::jsonb))
           WITH ORDINALITY AS m(id, pos)
     WHERE u.unit ->> 'group_id' IN (
         SELECT g.id FROM jsonb_array_elements_text(COALESCE(stage -> 'veto_group_ids', '[]'::jsonb)) AS g(id))
)
SELECT COALESCE(jsonb_agg(d.id ORDER BY d.first_pos), '[]'::jsonb)
  FROM (SELECT w.id, min(w.pos) AS first_pos
          FROM wanted w
         WHERE w.id IN (SELECT p.id FROM pool p)
         GROUP BY w.id) AS d
$$;

-- A stages array with veto_user_ids set on each stage that has people for it.
CREATE FUNCTION steward_stages_with_veto_people(stages jsonb) RETURNS jsonb
LANGUAGE sql AS $$
SELECT COALESCE(jsonb_agg(
           CASE WHEN NOT (s.stage ? 'veto_user_ids')
                 AND jsonb_array_length(steward_stage_veto_people(s.stage)) > 0
                THEN s.stage || jsonb_build_object('veto_user_ids', steward_stage_veto_people(s.stage))
                ELSE s.stage END
           ORDER BY s.pos), '[]'::jsonb)
  FROM jsonb_array_elements(stages) WITH ORDINALITY AS s(stage, pos)
$$;

UPDATE workflow_defs
   SET stages_json = steward_stages_with_veto_people(stages_json)
 WHERE veto_enabled AND jsonb_typeof(stages_json) = 'array';

UPDATE workflow_def_versions v
   SET stages_json = steward_stages_with_veto_people(v.stages_json)
  FROM workflow_defs d
 WHERE d.id = v.def_id AND d.veto_enabled AND jsonb_typeof(v.stages_json) = 'array';

DROP FUNCTION steward_stages_with_veto_people(jsonb);
DROP FUNCTION steward_stage_veto_people(jsonb);
