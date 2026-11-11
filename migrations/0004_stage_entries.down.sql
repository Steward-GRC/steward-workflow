-- Copyright 2026 The Steward Authors
-- SPDX-License-Identifier: Apache-2.0

ALTER TABLE approval_runs DROP COLUMN ancestor_category_ids;
DROP TABLE approval_stage_entries;
