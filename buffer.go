package titip

import (
	"bytes"
	"sync"
)

// maxBufferSize defines the maximum buffer capacity (2MB) allowed to be returned to the pool.
// Buffers exceeding this capacity are discarded to protect against permanent heap retention.
const maxBufferSize = 2 * 1024 * 1024

// --- Byte Buffer Pool ---

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
// Discards buffers exceeding 2MB to prevent heap bloat.
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
