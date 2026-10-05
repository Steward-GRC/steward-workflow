-- Copyright 2026 The Steward Authors
-- SPDX-License-Identifier: Apache-2.0

-- Admin-authored workflow definitions, in the builder model (not saga JSON).
-- Every update copies the live row into workflow_def_versions and bumps
-- version; a run stays on the version it was submitted under.
CREATE TABLE workflow_defs (
    id             uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    name           text        NOT NULL,
    version        int         NOT NULL DEFAULT 1,
    stages_json    jsonb       NOT NULL,
    published      boolean     NOT NULL DEFAULT false,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    archived_at    timestamptz,
    description    text        NOT NULL DEFAULT '',
    author_user_id text        NOT NULL DEFAULT ''
);

CREATE INDEX workflow_defs_published_idx ON workflow_defs (published);

-- Earlier versions still pinned by a run; removed once no active run uses one.
CREATE TABLE workflow_def_versions (
    def_id      uuid        NOT NULL REFERENCES workflow_defs(id),
    version     int         NOT NULL,
    name        text        NOT NULL,
    stages_json jsonb       NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    description text        NOT NULL DEFAULT '',
    PRIMARY KEY (def_id, version)
);

-- A policy's own workflow, overriding its home category's default. A NULL
-- workflow_def_id means the policy needs no approval.
CREATE TABLE policy_workflow_overrides (
    policy_id        text        PRIMARY KEY,
    workflow_def_id  uuid        REFERENCES workflow_defs(id) ON DELETE SET NULL,
    updated_at       timestamptz NOT NULL DEFAULT now()
);

-- Every approval run of a policy version, the finished ones kept.
CREATE TABLE approval_runs (
    policy_version_id     text        NOT NULL,
    run_id                text        PRIMARY KEY,
    workflow_def_id       uuid        NOT NULL REFERENCES workflow_defs(id),
    workflow_version      int         NOT NULL,
    status                text        NOT NULL DEFAULT 'in_review',
    started_at            timestamptz NOT NULL DEFAULT now(),
    resolved_at           timestamptz,
    home_category_id      text,
    -- The effective submitter: the approval notices go to them and an account
    -- merge moves it. During act-as the real admin is submitted_by_actor_id.
    submitted_by          text,
    policy_id             text,
    submitted_by_actor_id text,
    CONSTRAINT approval_runs_status_check CHECK (status IN
        ('in_review','scheduled','approved','rejected','withdrawn','superseded',
         'published','archived','escalated','draft','aborted','failed'))
);

CREATE INDEX approval_runs_status_idx ON approval_runs (status);

-- At most one active run per version; finished runs don't hold the slot.
CREATE UNIQUE INDEX approval_runs_one_active_per_version
    ON approval_runs (policy_version_id)
    WHERE status IN ('in_review', 'scheduled');

CREATE INDEX approval_runs_submitted_by_actor_idx
    ON approval_runs (submitted_by_actor_id)
    WHERE submitted_by_actor_id IS NOT NULL;

-- One seat per (version, stage, person, group). group_id is empty for an
-- individual seat and names the approve-as-group unit for a group seat, so a
-- person can hold both on one stage and vote each separately.
CREATE TABLE approval_assignments (
    id                                uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    policy_version_id                 text        NOT NULL,
    stage_index                       int         NOT NULL,
    user_id                           text        NOT NULL,
    assigned_at                       timestamptz NOT NULL DEFAULT now(),
    sla_deadline_at                   timestamptz NOT NULL,
    reminder_at                       timestamptz NOT NULL,
    -- superseded: another seat rejected the stage before this one decided.
    state                             text        NOT NULL DEFAULT 'pending',
    decided_at                        timestamptz,
    decided_comment                   text,
    bulk_batch_id                     uuid,
    -- The SLA and reminder time left when the outage reconciler paused the
    -- seat; added back on resume.
    paused_remaining_sla_seconds      bigint,
    paused_remaining_reminder_seconds bigint,
    group_id                          text        NOT NULL DEFAULT '',
    CONSTRAINT approval_assignments_state_check CHECK (state IN
        ('pending','approved','rejected','swapped_out','paused','superseded','withdrawn')),
    CONSTRAINT approval_assignments_unique_seat UNIQUE (policy_version_id, stage_index, user_id, group_id)
);

CREATE INDEX approval_assignments_lookup_idx ON approval_assignments (policy_version_id, stage_index);
CREATE INDEX approval_assignments_user_state_idx ON approval_assignments (user_id, state);
CREATE INDEX approval_assignments_paused_idx ON approval_assignments (state) WHERE state = 'paused';

-- Append-only log of every seat. actor_user_id is who really acted; during
-- act-as that is the admin, and the account they acted as is
-- impersonated_user_id.
CREATE TABLE assignment_history (
    id                   bigserial   PRIMARY KEY,
    assignment_id        uuid        NOT NULL REFERENCES approval_assignments(id) ON DELETE CASCADE,
    event                text        NOT NULL,
    actor_user_id        text        NOT NULL,
    actor_role           text,
    previous_user_id     text,
    new_user_id          text,
    out_of_eligibility   boolean     NOT NULL DEFAULT false,
    reason               text,
    created_at           timestamptz NOT NULL DEFAULT now(),
    impersonated_user_id text,
    CONSTRAINT assignment_history_event_check CHECK (event IN
        ('created','swapped_in','swapped_out','approved','rejected','paused','resumed','withdrawn','reassigned'))
);

CREATE INDEX assignment_history_assignment_idx ON assignment_history (assignment_id);
CREATE INDEX assignment_history_actor_idx ON assignment_history (actor_user_id);
CREATE INDEX assignment_history_impersonated_idx
    ON assignment_history (impersonated_user_id)
    WHERE impersonated_user_id IS NOT NULL;

-- Per-version run state; the outage reconciler moves it to and from
-- paused_external_dep.
CREATE TABLE workflow_run_state (
    policy_version_id  text        PRIMARY KEY,
    state              text        NOT NULL DEFAULT 'running',
    paused_at          timestamptz,
    paused_reason      text,
    resumed_at         timestamptz,
    updated_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT workflow_run_state_state_check CHECK (state IN
        ('running','paused_external_dep','complete'))
);

CREATE INDEX workflow_run_state_state_idx ON workflow_run_state (state);
