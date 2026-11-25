// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"encoding/json"
	"math"
	"os"
	"strconv"
	"testing"
)

// The vectors in testdata come from the TypeScript SDK (sdk/go/testdata/golden.ts).

func TestXorshiftMatchesTypeScript(t *testing.T) {
	raw, err := os.ReadFile("testdata/rng.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Seed   string    `json:"seed"`
		Floats []float64 `json:"floats"`
	}
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) == 0 {
		t.Fatal("no vectors")
	}
	for _, c := range cases {
		seed, err := strconv.ParseUint(c.Seed, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		x := NewXorshift(seed)
		for i, want := range c.Floats {
			if got := x.Float64(); got != want {
				t.Errorf("seed %s draw %d: got %v, TypeScript %v", c.Seed, i, got, want)
			}
		}
	}
}

// Float64 is the top 53 bits of the same output Uint64 returns.
func TestXorshiftFloatIsTopBitsOfUint64(t *testing.T) {
	a, b := NewXorshift(7), NewXorshift(7)
	for range 100 {
		u := a.Uint64()
		if got, want := b.Float64(), float64(u>>11)/(1<<53); got != want {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}

func TestMarshalMatchesJSONStringify(t *testing.T) {
	raw, err := os.ReadFile("testdata/json.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Value        any    `json:"value"`
		NegativeZero bool   `json:"negativeZero"`
		JSON         string `json:"json"`
	}
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		v := c.Value
		if c.NegativeZero {
			v = math.Copysign(0, -1)
		}
		got, err := Marshal(v)
		if err != nil {
			t.Errorf("%#v: %v", v, err)
			continue
		}
		if string(got) != c.JSON {
			t.Errorf("%#v: got %q, JSON.stringify %q", v, got, c.JSON)
		}
	}
}

// lineSep is U+2028, written as a rune so the source stays plain ASCII.
var lineSep = string(rune(0x2028))

func TestMarshalPassesBytesThrough(t *testing.T) {
	for _, c := range []struct {
		v    any
		want string
	}{
		{[]byte(`{"raw":true}`), `{"raw":true}`},
		{[]byte(nil), `null`},
		{json.RawMessage(`[1, 2]`), `[1, 2]`},
		{json.RawMessage(nil), `null`},
		{struct {
			A string `json:"a"`
			B []int  `json:"b"`
		}{"<&>", []int{1}}, `{"a":"<&>","b":[1]}`},
		{map[string]any{"z": math.Copysign(0, -1), "a": "x" + lineSep}, `{"a":"x` + lineSep + `","z":0}`},
		{[]float64{-0.5, math.Copysign(0, -1), -10}, `[-0.5,0,-10]`},
		{"-0 inside a string", `"-0 inside a string"`},
		{`\` + "u2028 escaped backslash", `"\\u2028 escaped backslash"`},
	} {
		got, err := Marshal(c.v)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != c.want {
			t.Errorf("%#v: got %s want %s", c.v, got, c.want)
		}
	}
	var b []byte
	if err := Unmarshal([]byte(`not json`), &b); err != nil || string(b) != "not json" {
		t.Errorf("Unmarshal []byte: %q %v", b, err)
	}
	if _, err := Marshal(math.NaN()); err == nil {
		t.Error("NaN must not encode")
	}
	if got := StreamJSON(make(chan int)); string(got) != `{"unserializable":"json: unsupported type: chan int"}` {
		t.Errorf("StreamJSON: %s", got)
	}
}

func TestObjectKeepsInsertionOrder(t *testing.T) {
	var o Object
	if string(o.Bytes()) != "{}" {
		t.Fatal(string(o.Bytes()))
	}
	o.Field("z", 1)
	o.Field("a", "<b>")
	o.Raw("m", []byte(`{"x":null}`))
	if got := string(o.Bytes()); got != `{"z":1,"a":"<b>","m":{"x":null}}` {
		t.Fatal(got)
	}
}

func TestArtifactCacheEvictsOldest(t *testing.T) {
	c := NewArtifactCache(10)
	c.Remember("a", []byte("12345"))
	c.Remember("b", []byte("12345"))
	c.Remember("c", []byte("1"))
	if _, ok := c.Get("a"); ok {
		t.Error("a should be evicted")
	}
	if _, ok := c.Get("c"); !ok {
		t.Error("c should be kept")
	}
	c.Remember("big", make([]byte, 11))
	if _, ok := c.Get("big"); ok {
		t.Error("an item larger than the cache is not kept")
	}
}
