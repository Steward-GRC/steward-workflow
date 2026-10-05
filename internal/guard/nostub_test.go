// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package guard holds module-wide invariants enforced as tests.
//
// nostub fails the build if any cmd/**/main.go wires a Stub or noop client: a
// no-op policy client would approve everything and write no status back, so
// approve, reject and withdraw would have no effect on the policy.
package guard_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNoStubClientWiredInMain parses every cmd/**/main.go and fails if any
// references a Stub/noop client constructor or type in actual code (call
// expressions / composite literals). Comments are ignored — the AST carries no
// comment text into expressions — so a doc comment that merely mentions
// "StubPolicyClient" (e.g. explaining why the real client replaced it) does not
// trip the guard; only real wiring does.
func TestNoStubClientWiredInMain(t *testing.T) {
	mains := findMainFiles(t, "../../cmd")
	if len(mains) == 0 {
		t.Fatal("found no cmd/**/main.go files to scan; guard would be a silent no-op")
	}

	for _, path := range mains {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0) // 0 mode: drop comments.
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.CallExpr:
				if name := calleeName(node.Fun); isStubName(name) {
					t.Errorf("%s wires a stub/noop client via call %q — production entrypoints must use the real client", path, name)
				}
			case *ast.CompositeLit:
				if name := typeName(node.Type); isStubName(name) {
					t.Errorf("%s constructs a stub/noop client literal %q — production entrypoints must use the real client", path, name)
				}
			}
			return true
		})
	}
}

// findMainFiles returns every main.go under root (cmd/<svc>/main.go).
func findMainFiles(t *testing.T, root string) []string {
	t.Helper()
	abs, err := filepath.Abs(root)
	if err != nil {
		t.Fatalf("abs %s: %v", root, err)
	}
	var out []string
	err = filepath.WalkDir(abs, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && d.Name() == "main.go" {
			out = append(out, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", abs, err)
	}
	return out
}

// calleeName returns the function name of a call (ident or pkg.Sel), else "".
func calleeName(fun ast.Expr) string {
	switch f := fun.(type) {
	case *ast.Ident:
		return f.Name
	case *ast.SelectorExpr:
		return f.Sel.Name
	}
	return ""
}

// typeName returns the type name of a composite literal (ident or pkg.Sel),
// unwrapping a leading & via the caller. Returns "" for non-named types.
func typeName(typ ast.Expr) string {
	switch t := typ.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.SelectorExpr:
		return t.Sel.Name
	}
	return ""
}

// isStubName reports whether an identifier names a stub/noop client. It matches
// the project's placeholder constructor/type names (NewStubPolicyClient,
// StubPolicyClient) plus a defensive substring check for "stub"/"noop" so a
// future placeholder named in the same spirit is also caught.
func isStubName(name string) bool {
	if name == "" {
		return false
	}
	lower := strings.ToLower(name)
	return strings.Contains(lower, "stub") || strings.Contains(lower, "noop")
}
