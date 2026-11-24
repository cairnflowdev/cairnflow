// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package core

import "sync"

// ArtifactMaxBytes caps one artifact: 16 MiB minus 1 KiB, room for the head of the
// `ArtifactPut` frame under the 16 MiB frame cap.
const ArtifactMaxBytes = (16 << 20) - 1024

// artifactCacheMax caps the process's artifact memory in bytes.
const artifactCacheMax = 64 << 20

// ArtifactCache is the process's artifact memory: sha256 hex → bytes, capped in bytes,
// oldest evicted first. `put` remembers what it sent, so a `get` in the same process does
// not download it again.
type ArtifactCache struct {
	mu    sync.Mutex
	items map[string][]byte
	order []string
	bytes int
	max   int
}

// Artifacts is the process-wide cache.
var Artifacts = NewArtifactCache(artifactCacheMax)

// NewArtifactCache returns an empty cache holding at most max bytes.
func NewArtifactCache(max int) *ArtifactCache {
	return &ArtifactCache{items: map[string][]byte{}, max: max}
}

// Get returns the cached bytes of sha.
func (c *ArtifactCache) Get(sha string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.items[sha]
	return b, ok
}

// Remember stores content under sha (the caller does not modify content afterwards).
func (c *ArtifactCache) Remember(sha string, content []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.items[sha]; ok || len(content) > c.max {
		return
	}
	c.items[sha] = content
	c.order = append(c.order, sha)
	c.bytes += len(content)
	for c.bytes > c.max && len(c.order) > 0 {
		old := c.order[0]
		c.order = c.order[1:]
		c.bytes -= len(c.items[old])
		delete(c.items, old)
	}
}
