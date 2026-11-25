// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package turn

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/core"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/wire"
)

var _ core.Driver = (*Turn)(nil)

type vectorTree struct {
	Combinator  string       `json:"combinator"`
	Completions []uint32     `json:"completions"`
	Signals     []uint32     `json:"signals"`
	Names       []string     `json:"names"`
	Nested      []vectorTree `json:"nested"`
}

func (v vectorTree) tree() Tree {
	t := Tree{All: v.Combinator == "all", Completions: v.Completions, Signals: v.Signals, Names: v.Names}
	for _, n := range v.Nested {
		t.Nested = append(t.Nested, n.tree())
	}
	return t
}

// The shared vectors of conformance/suspension-vectors.json: each tree encodes to exactly
// the bytes the runtime decodes, and Satisfied reaches the runtime's verdict on every
// combination of arrived leaves.
func TestSuspensionVectors(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "conformance", "suspension-vectors.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Vectors []struct {
			Name   string     `json:"name"`
			Tree   vectorTree `json:"tree"`
			Hex    string     `json:"hex"`
			States []struct {
				Completions []uint32 `json:"completions"`
				Signals     []uint32 `json:"signals"`
				Names       []string `json:"names"`
				Satisfied   bool     `json:"satisfied"`
			} `json:"states"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Vectors) < 10 {
		t.Fatalf("only %d vectors", len(doc.Vectors))
	}
	checked := 0
	for _, v := range doc.Vectors {
		tree := v.Tree.tree()
		if got := hex.EncodeToString(wire.Marshal(tree.Message())); got != v.Hex {
			t.Errorf("%s: encodes to %s, want %s", v.Name, got, v.Hex)
		}
		for _, s := range v.States {
			got := Satisfied(tree,
				func(c uint32) bool { return slices.Contains(s.Completions, c) },
				func(i uint32) bool { return slices.Contains(s.Signals, i) },
				func(n string) bool { return slices.Contains(s.Names, n) })
			if got != s.Satisfied {
				t.Errorf("%s: satisfied=%v for %+v, want %v", v.Name, got, s, s.Satisfied)
			}
			checked++
		}
	}
	if checked <= 70 {
		t.Fatalf("only %d states checked", checked)
	}
}

// Several waits merge under a FIRST root: FIRST leaves deduplicated and sorted (names in
// UTF-16 order, as JavaScript sorts), each ALL node a child.
func TestMerge(t *testing.T) {
	one := Tree{All: true, Completions: []uint32{1, 2}}
	if got := merge([]Tree{one}); !treeEqual(got, one) {
		t.Fatalf("one wait is its own tree, got %+v", got)
	}
	got := merge([]Tree{
		{Completions: []uint32{4}, Names: []string{"～", "b"}},
		one,
		{Completions: []uint32{3, 4}, Names: []string{"\U0001F600"}},
	})
	want := Tree{Completions: []uint32{3, 4}, Names: []string{"b", "\U0001F600", "～"}, Nested: []Tree{one}}
	if !treeEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func treeEqual(a, b Tree) bool {
	return hex.EncodeToString(wire.Marshal(a.Message())) == hex.EncodeToString(wire.Marshal(b.Message()))
}
