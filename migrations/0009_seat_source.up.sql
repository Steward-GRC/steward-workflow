-- Copyright 2026 The Steward Authors
-- SPDX-License-Identifier: Apache-2.0

-- Why each seat was made (steward-workflow#80). Seats made before this column
-- keep the empty default, which GetStatus reports as unspecified.
ALTER TABLE approval_assignments ADD COLUMN seat_source text NOT NULL DEFAULT '';
