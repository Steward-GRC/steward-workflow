-- Copyright 2026 The Steward Authors
-- SPDX-License-Identifier: Apache-2.0

ALTER TABLE approval_runs
    DROP COLUMN change_type_agreed_at,
    DROP COLUMN requested_change_type;
