-- Copyright 2026 The Steward Authors
-- SPDX-License-Identifier: Apache-2.0

-- A workflow opts in to veto seats; off by default. A seat carries the veto
-- mark only on a run submitted while the workflow was opted in.
ALTER TABLE workflow_defs ADD COLUMN veto_enabled boolean NOT NULL DEFAULT false;
ALTER TABLE approval_assignments ADD COLUMN veto boolean NOT NULL DEFAULT false;
