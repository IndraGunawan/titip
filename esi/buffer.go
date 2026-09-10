package esi

import (
	"bytes"
	"sync"
)

// maxBufferSize defines the maximum buffer capacity (2MB) allowed to be returned to the pool.
// Documents exceeding 2MB are still fully processed and served (up to MaxResponseSize, default 10MB),
// but their oversized buffers are allowed to be garbage-collected rather than retained in the pool indefinitely.
const maxBufferSize = 2 * 1024 * 1024

var bufferPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

// getBuffer retrieves a reusable *bytes.Buffer from the pool.
func getBuffer() *bytes.Buffer {
	return bufferPool.Get().(*bytes.Buffer)
}

// putBuffer resets and returns a *bytes.Buffer to the pool.
// Buffers exceeding 2MB are not returned to the pool (letting the Go runtime GC them)
// to prevent permanent heap retention from rare large responses.
func putBuffer(buf *bytes.Buffer) {
	if buf == nil {
		return
	}
	if buf.Cap() > maxBufferSize {
		return
	}
	buf.Reset()
	bufferPool.Put(buf)
}
