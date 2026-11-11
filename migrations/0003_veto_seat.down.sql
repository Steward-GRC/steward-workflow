-- Copyright 2026 The Steward Authors
-- SPDX-License-Identifier: Apache-2.0

ALTER TABLE approval_assignments DROP COLUMN veto;
ALTER TABLE workflow_defs DROP COLUMN veto_enabled;
