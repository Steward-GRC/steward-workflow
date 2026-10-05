// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	"github.com/Steward-GRC/steward-workflow/internal/builder"
)

// WorkflowDefStore is the full definition store the admin calls need.
type WorkflowDefStore interface {
	WorkflowDefStoreReader
	Create(ctx context.Context, wd builder.WorkflowDef) (string, error)
	List(ctx context.Context) ([]builder.WorkflowDef, error)
	Update(ctx context.Context, id string, wd builder.WorkflowDef) (builder.WorkflowDef, error)
	Archive(ctx context.Context, id string) error
}

// ListWorkflowDefs returns the definitions that aren't archived.
func (s *WorkflowServer) ListWorkflowDefs(ctx context.Context, _ *workflowv1.ListWorkflowDefsRequest) (*workflowv1.ListWorkflowDefsResponse, error) {
	wds, err := s.mustDefStore().List(ctx)
	if err != nil {
		return nil, internalErr(ctx, "list workflow defs: %w", err)
	}
	out := make([]*workflowv1.WorkflowDef, 0, len(wds))
	for _, wd := range wds {
		out = append(out, defToProto(wd))
	}
	return &workflowv1.ListWorkflowDefsResponse{Defs: out}, nil
}

// GetWorkflowDef returns a definition's live version.
func (s *WorkflowServer) GetWorkflowDef(ctx context.Context, req *workflowv1.GetWorkflowDefRequest) (*workflowv1.GetWorkflowDefResponse, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	wd, err := s.defStore.Get(ctx, req.GetId())
	if err != nil {
		return nil, internalErr(ctx, "get workflow def: %w", err)
	}
	return &workflowv1.GetWorkflowDefResponse{Def: defToProto(wd)}, nil
}

// CreateWorkflowDef creates a definition at version 1; the actor is its
// author.
func (s *WorkflowServer) CreateWorkflowDef(ctx context.Context, req *workflowv1.CreateWorkflowDefRequest) (*workflowv1.CreateWorkflowDefResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if len(req.GetStages()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one stage is required")
	}
	ds := s.mustDefStore()
	id, err := ds.Create(ctx, builder.WorkflowDef{
		Name:         req.GetName(),
		Description:  req.GetDescription(),
		Stages:       stagesToBuilder(req.GetStages()),
		AuthorUserID: req.GetActorUserId(),
	})
	if err != nil {
		return nil, internalErr(ctx, "create workflow def: %w", err)
	}
	created, err := s.defStore.Get(ctx, id)
	if err != nil {
		return nil, internalErr(ctx, "get created workflow def: %w", err)
	}
	s.auditEmit.EmitActor(ctx, "workflow_def.created", "id:"+id, req.GetActorUserId())
	return &workflowv1.CreateWorkflowDefResponse{Def: defToProto(created)}, nil
}

// UpdateWorkflowDef saves a new version; runs stay on theirs. The actor
// becomes the author.
func (s *WorkflowServer) UpdateWorkflowDef(ctx context.Context, req *workflowv1.UpdateWorkflowDefRequest) (*workflowv1.UpdateWorkflowDefResponse, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if len(req.GetStages()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one stage is required")
	}
	updated, err := s.mustDefStore().Update(ctx, req.GetId(), builder.WorkflowDef{
		Name:         req.GetName(),
		Description:  req.GetDescription(),
		Stages:       stagesToBuilder(req.GetStages()),
		AuthorUserID: req.GetActorUserId(),
	})
	if err != nil {
		return nil, internalErr(ctx, "update workflow def: %w", err)
	}
	s.auditEmit.EmitActor(ctx, "workflow_def.updated", "id:"+req.GetId(), req.GetActorUserId())
	return &workflowv1.UpdateWorkflowDefResponse{Def: defToProto(updated)}, nil
}

// ArchiveWorkflowDef hides a definition. A category whose default it is
// keeps pointing at it until the gateway detaches it in core.
func (s *WorkflowServer) ArchiveWorkflowDef(ctx context.Context, req *workflowv1.ArchiveWorkflowDefRequest) (*workflowv1.ArchiveWorkflowDefResponse, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	if err := s.mustDefStore().Archive(ctx, req.GetId()); err != nil {
		return nil, internalErr(ctx, "archive workflow def: %w", err)
	}
	s.auditEmit.EmitActor(ctx, "workflow_def.archived", "id:"+req.GetId(), req.GetActorUserId())
	return &workflowv1.ArchiveWorkflowDefResponse{}, nil
}

// mustDefStore returns the full store; wiring a read-only one is a
// programming error.
func (s *WorkflowServer) mustDefStore() WorkflowDefStore {
	ds, ok := s.defStore.(WorkflowDefStore)
	if !ok {
		panic("defStore does not implement WorkflowDefStore (Create/List/Update/Archive); inject a full-interface store")
	}
	return ds
}

func defToProto(wd builder.WorkflowDef) *workflowv1.WorkflowDef {
	stages := make([]*workflowv1.WorkflowStage, 0, len(wd.Stages))
	for _, s := range wd.Stages {
		stages = append(stages, stageToProto(s))
	}
	return &workflowv1.WorkflowDef{
		Id:           wd.ID,
		Name:         wd.Name,
		Description:  wd.Description,
		Version:      toInt32(wd.Version),
		Stages:       stages,
		AuthorUserId: wd.AuthorUserID,
	}
}

func stageToProto(s builder.Stage) *workflowv1.WorkflowStage {
	var byCategory map[string]*workflowv1.ApproverList
	if len(s.ApproversByCategory) > 0 {
		byCategory = make(map[string]*workflowv1.ApproverList, len(s.ApproversByCategory))
		for id, users := range s.ApproversByCategory {
			byCategory[id] = &workflowv1.ApproverList{UserIds: users}
		}
	}
	var units []*workflowv1.GroupUnit
	for _, u := range s.GroupUnits {
		units = append(units, &workflowv1.GroupUnit{
			GroupId:        u.GroupID,
			InternalQuorum: quorumToProtoString(u.InternalQuorum),
			MemberUserIds:  u.Members,
		})
	}
	return &workflowv1.WorkflowStage{
		Id:                  s.ID,
		Name:                s.Name,
		ApproverIds:         s.ApproverIDs,
		ApproversByCategory: byCategory,
		GroupUnits:          units,
		Quorum:              quorumToProtoString(s.Quorum),
		SlaDays:             toInt32(s.SLADays),
		RejectOnSlaBreach:   s.RejectOnSLABreach,
		PinnedLast:          s.PinnedLast,
	}
}

func stageFromProto(ps *workflowv1.WorkflowStage) builder.Stage {
	var byCategory map[string][]string
	if len(ps.GetApproversByCategory()) > 0 {
		byCategory = make(map[string][]string, len(ps.GetApproversByCategory()))
		for id, list := range ps.GetApproversByCategory() {
			byCategory[id] = list.GetUserIds()
		}
	}
	var units []builder.GroupUnit
	for _, u := range ps.GetGroupUnits() {
		units = append(units, builder.GroupUnit{
			GroupID:        u.GetGroupId(),
			InternalQuorum: quorumFromProtoString(u.GetInternalQuorum()),
			Members:        u.GetMemberUserIds(),
		})
	}
	return builder.Stage{
		ID:                  ps.GetId(),
		Name:                ps.GetName(),
		ApproverIDs:         ps.GetApproverIds(),
		ApproversByCategory: byCategory,
		GroupUnits:          units,
		Quorum:              quorumFromProtoString(ps.GetQuorum()),
		SLADays:             int(ps.GetSlaDays()),
		RejectOnSLABreach:   ps.GetRejectOnSlaBreach(),
		PinnedLast:          ps.GetPinnedLast(),
	}
}

func stagesToBuilder(stages []*workflowv1.WorkflowStage) []builder.Stage {
	out := make([]builder.Stage, 0, len(stages))
	for _, s := range stages {
		out = append(out, stageFromProto(s))
	}
	return out
}

// quorumFromProtoString maps the API's quorum; anything unknown is "one".
func quorumFromProtoString(q string) builder.Quorum {
	switch q {
	case "majority":
		return builder.QuorumMajority
	case "all":
		return builder.QuorumAll
	default:
		return builder.QuorumAny
	}
}

// quorumToProtoString maps a stored quorum; an unknown one shows as majority.
func quorumToProtoString(q builder.Quorum) string {
	switch q {
	case builder.QuorumAny:
		return "one"
	case builder.QuorumMajority:
		return "majority"
	case builder.QuorumAll:
		return "all"
	default:
		return "majority"
	}
}
