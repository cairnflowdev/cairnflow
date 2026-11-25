// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package core

// Xorshift is the deterministic RNG of an invocation: xorshift64*, seeded from
// `StartMessage.random_seed` (docs/protocol-v1.md §8.4). It produces the same sequence as
// `makeRandom` of the TypeScript SDK and `_Random` of the Python SDK for the same seed,
// so a journal written by one SDK replays identically in another. It implements
// math/rand/v2.Source.
type Xorshift struct{ s uint64 }

// zeroSeed replaces a zero seed (xorshift has no zero state).
const zeroSeed = 0x9E3779B97F4A7C15

// NewXorshift returns the source for seed.
func NewXorshift(seed uint64) *Xorshift {
	if seed == 0 {
		seed = zeroSeed
	}
	return &Xorshift{s: seed}
}

// Uint64 returns the next 64-bit output.
func (x *Xorshift) Uint64() uint64 {
	s := x.s
	s ^= s >> 12
	s ^= s << 25
	s ^= s >> 27
	x.s = s
	return s * 0x2545F4914F6CDD1D
}

// Float64 returns the next value in [0, 1) exactly as the TypeScript SDK's `random()`:
// the top 53 bits of the output. (math/rand/v2's Rand.Float64 uses the low bits, so it
// gives different numbers from the same source.)
func (x *Xorshift) Float64() float64 {
	return float64(x.Uint64()>>11) / (1 << 53)
}
