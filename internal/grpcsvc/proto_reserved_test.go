// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"

	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
)

// The workflow-wide veto is gone from the wire. Its field numbers and names are
// reserved so they can never be given a new meaning.

func reservedNumbers(md protoreflect.MessageDescriptor) map[int32]bool {
	out := map[int32]bool{}
	rs := md.ReservedRanges()
	for i := 0; i < rs.Len(); i++ {
		r := rs.Get(i)
		for n := r[0]; n < r[1]; n++ {
			out[int32(n)] = true
		}
	}
	return out
}

func reservedNames(md protoreflect.MessageDescriptor) map[string]bool {
	out := map[string]bool{}
	names := md.ReservedNames()
	for i := 0; i < names.Len(); i++ {
		out[string(names.Get(i))] = true
	}
	return out
}

func TestRemovedVetoFieldsAreReserved(t *testing.T) {
	stage := (&workflowv1.WorkflowStage{}).ProtoReflect().Descriptor()
	require.True(t, reservedNumbers(stage)[10])
	require.True(t, reservedNumbers(stage)[11])
	require.True(t, reservedNames(stage)["veto_approver_ids"])
	require.True(t, reservedNames(stage)["veto_group_ids"])
	require.Nil(t, stage.Fields().ByName("veto_approver_ids"))
	require.Nil(t, stage.Fields().ByName("veto_group_ids"))
	require.NotNil(t, stage.Fields().ByName("veto_user_ids"), "the per-stage people stay")
	require.EqualValues(t, 12, stage.Fields().ByName("veto_user_ids").Number())

	def := (&workflowv1.WorkflowDef{}).ProtoReflect().Descriptor()
	require.True(t, reservedNumbers(def)[7])
	require.True(t, reservedNames(def)["veto_enabled"])
	require.Nil(t, def.Fields().ByName("veto_enabled"))
}

func TestSetWorkflowVetoEnabledIsRemovedFromTheService(t *testing.T) {
	for _, m := range workflowv1.WorkflowService_ServiceDesc.Methods {
		require.NotEqual(t, "SetWorkflowVetoEnabled", m.MethodName)
	}
}

// The overdue settings are additive: 13 and 14 are new numbers, nothing was
// renumbered (steward-workflow#72).
func TestStageOverdueFieldNumbers(t *testing.T) {
	stage := (&workflowv1.WorkflowStage{}).ProtoReflect().Descriptor()
	require.EqualValues(t, 13, stage.Fields().ByName("remind_every_days").Number())
	require.EqualValues(t, 14, stage.Fields().ByName("escalate_after_days").Number())
	require.EqualValues(t, 5, stage.Fields().ByName("sla_days").Number())
	require.EqualValues(t, 6, stage.Fields().ByName("reject_on_sla_breach").Number())
}
