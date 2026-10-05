// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc

import (
	"context"
	"reflect"
	"testing"

	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// knownDeferred allowlists specific "<InterfaceName>.<Method>" RPCs that
// legitimately return codes.Unimplemented from a ZERO-VALUE handler by
// design (a required dependency is nil), as opposed to a stub method that
// was never actually implemented (an embedded Unimplemented*Server
// fall-through). Every entry MUST carry a reason distinguishing the two.
var knownDeferred = map[string]string{
	"WorkflowServiceServer.SwapAssignee":              "method returns codes.Unimplemented by design when assignments store unset; not a stub gap",
	"WorkflowServiceServer.BulkDecide":                "method returns codes.Unimplemented by design when assignments store unset; not a stub gap",
	"WorkflowServiceServer.GetAssignmentHistory":      "method returns codes.Unimplemented by design when assignments store unset; not a stub gap",
	"WorkflowServiceServer.ReassignUserWorkflowItems": "method returns codes.Unimplemented by design when assignments store unset; not a stub gap",
}

// TestGRPCRPCCoverage guards against a proto RPC silently being served by the
// embedded Unimplemented*Server stub: that compiles fine (the stub satisfies
// the interface) but returns codes.Unimplemented at runtime for every call,
// even though nothing in `go build`/`go vet` would ever flag it.
//
// For each service interface below we reflect over its unary RPC methods,
// invoke each one on a ZERO-VALUE handler (every dependency nil/zero) with a
// zero-value request, and recover any panic. A panic (typically a nil
// pointer/interface dereference reaching into a real dependency) proves the
// method contains real logic beyond the generated stub, so it's treated as
// "implemented". A clean codes.Unimplemented return with no panic means the
// call fell through to the embedded stub (or a method deliberately mirrors
// it) — that fails the test unless the RPC is allowlisted in knownDeferred.
func TestGRPCRPCCoverage(t *testing.T) {
	cases := []struct {
		name          string
		interfaceType reflect.Type
		handler       any
		wantInvoked   int // sanity check: methods actually exercised by reflection
	}{
		{
			name:          "WorkflowServiceServer",
			interfaceType: reflect.TypeFor[workflowv1.WorkflowServiceServer](),
			handler:       &WorkflowServer{},
			wantInvoked:   16,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handlerVal := reflect.ValueOf(tc.handler)
			invoked := 0
			for m := range tc.interfaceType.Methods() {
				if !isUnaryRPCShape(m.Type) {
					// Skips generated non-RPC interface members such as
					// mustEmbedUnimplementedXxxServer().
					continue
				}
				invoked++
				qualifiedName := tc.name + "." + m.Name
				t.Run(m.Name, func(t *testing.T) {
					code, panicked := invokeZeroHandler(handlerVal, m)
					if panicked {
						return
					}
					if code != codes.Unimplemented {
						return
					}
					if reason, ok := knownDeferred[qualifiedName]; ok {
						t.Logf("allowlisted: %s returns codes.Unimplemented under a nil-dep zero handler: %s", qualifiedName, reason)
						return
					}
					t.Errorf("%s falls through to the embedded Unimplemented*Server stub (codes.Unimplemented) with no panic — "+
						"either the RPC was never implemented, or (if this is a deliberate nil-dependency guard) allowlist it "+
						"in knownDeferred with a reason", qualifiedName)
				})
			}
			if invoked != tc.wantInvoked {
				t.Fatalf("%s: exercised %d unary RPC methods, want %d — the interface shape changed (a method was added/removed) "+
					"or the shape filter is no longer matching; update wantInvoked after confirming the new RPC set is covered",
					tc.name, invoked, tc.wantInvoked)
			}
		})
	}
}

// isUnaryRPCShape reports whether m matches the standard unary gRPC handler
// shape func(context.Context, *XxxRequest) (*XxxResponse, error). This
// excludes generated non-RPC interface members like
// mustEmbedUnimplementedXxxServer(), which take no arguments.
func isUnaryRPCShape(m reflect.Type) bool {
	ctxType := reflect.TypeFor[context.Context]()
	errType := reflect.TypeFor[error]()
	if m.NumIn() != 2 || m.NumOut() != 2 {
		return false
	}
	if !m.In(0).Implements(ctxType) {
		return false
	}
	if m.In(1).Kind() != reflect.Pointer {
		return false
	}
	if m.Out(0).Kind() != reflect.Pointer {
		return false
	}
	return m.Out(1) == errType
}

// invokeZeroHandler calls handlerVal.<m.Name>(context.Background(), new(zero
// request)) and reports the resulting gRPC status code. A panic — expected
// when an implemented method dereferences a nil-wired dependency — is
// recovered and reported via panicked=true instead of propagated, since it
// is itself proof the method is not the bare generated stub.
func invokeZeroHandler(handlerVal reflect.Value, m reflect.Method) (code codes.Code, panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
		}
	}()
	method := handlerVal.MethodByName(m.Name)
	req := reflect.New(m.Type.In(1).Elem())
	out := method.Call([]reflect.Value{reflect.ValueOf(context.Background()), req})
	err, _ := out[1].Interface().(error)
	return status.Code(err), false
}
