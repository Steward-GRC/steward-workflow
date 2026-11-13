-- Copyright 2026 The Steward Authors
-- SPDX-License-Identifier: Apache-2.0

DROP TABLE stage_escalations;
DROP INDEX approval_assignments_overdue_idx;
ALTER TABLE approval_assignments DROP COLUMN reminders_sent;
