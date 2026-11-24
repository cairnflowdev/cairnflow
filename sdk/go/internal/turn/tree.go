// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package turn

import (
	"slices"
	"unicode/utf16"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/wire"
)

// Tree is a suspension tree: what the turn waits for when it ends with `Suspension`
// (docs/protocol-v1.md section 7 and section 8.7). A FIRST node (All false) is satisfied
// by any leaf or child, an ALL node by every leaf and child. Children only occur under a
// FIRST root.
type Tree struct {
	All         bool
	Completions []uint32
	Signals     []uint32
	Names       []string
	Nested      []Tree
}

// Message encodes the tree the way every Cairnflow SDK does: a FIRST tree writes its
// leaves at the root with no combinator field (a single completion, a list of completions
// or a signal encode exactly as before trees existed); an ALL tree wraps its leaves in one
// ALL_COMPLETED child of the root.
func (t Tree) Message() *wire.Suspension {
	if t.All {
		return &wire.Suspension{NestedFutures: []wire.Suspension{t.allNode()}}
	}
	m := t.leaves()
	for _, n := range t.Nested {
		if n.All {
			m.NestedFutures = append(m.NestedFutures, n.allNode())
		} else {
			m.NestedFutures = append(m.NestedFutures, n.leaves())
		}
	}
	return &m
}

func (t Tree) leaves() wire.Suspension {
	return wire.Suspension{WaitingCompletions: t.Completions, WaitingSignals: t.Signals, WaitingNamedSignals: t.Names}
}

func (t Tree) allNode() wire.Suspension {
	m := t.leaves()
	m.CombinatorType = wire.CombinatorAllCompleted
	return m
}

// Satisfied is the runtime's rule for "the tree is satisfied" (`suspension_satisfied_by`):
// FIRST ⇒ any leaf or child, ALL ⇒ every leaf and child. conformance/suspension-vectors.json
// keeps the SDKs and the runtime in agreement.
func Satisfied(t Tree, done func(uint32) bool, hasIdx func(uint32) bool, hasName func(string) bool) bool {
	var leaves []bool
	for _, c := range t.Completions {
		leaves = append(leaves, done(c))
	}
	for _, i := range t.Signals {
		leaves = append(leaves, hasIdx(i))
	}
	for _, n := range t.Names {
		leaves = append(leaves, hasName(n))
	}
	for _, n := range t.Nested {
		leaves = append(leaves, Satisfied(n, done, hasIdx, hasName))
	}
	if t.All {
		return !slices.Contains(leaves, false)
	}
	return slices.Contains(leaves, true)
}

// merge builds the tree of several waits: one wait is exactly its own tree; several make
// a FIRST root merging the leaves of every FIRST node (deduplicated and sorted), each ALL
// node becoming a child.
func merge(nodes []Tree) Tree {
	if len(nodes) == 1 {
		return nodes[0]
	}
	var root Tree
	for _, n := range nodes {
		if n.All {
			root.Nested = append(root.Nested, n)
			continue
		}
		root.Completions = append(root.Completions, n.Completions...)
		root.Signals = append(root.Signals, n.Signals...)
		root.Names = append(root.Names, n.Names...)
	}
	slices.Sort(root.Completions)
	root.Completions = slices.Compact(root.Completions)
	slices.Sort(root.Signals)
	root.Signals = slices.Compact(root.Signals)
	slices.SortFunc(root.Names, compareUTF16)
	root.Names = slices.Compact(root.Names)
	return root
}

// compareUTF16 orders strings by UTF-16 code units, the default string order of
// JavaScript, so names sort as in the TypeScript SDK.
func compareUTF16(a, b string) int {
	x, y := utf16.Encode([]rune(a)), utf16.Encode([]rune(b))
	return slices.Compare(x, y)
}
