// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package saga

import "math"

// toInt32 narrows a stage index, clamped to the int32 range.
func toInt32(v int) int32 {
	if v > math.MaxInt32 {
		return math.MaxInt32
	}
	if v < math.MinInt32 {
		return math.MinInt32
	}
	return int32(v) //nosec G115 -- bounded by the guards above
}
