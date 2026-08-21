package titip

import (
	"bytes"
	"fmt"
	"net/http"
	"sync"

	"github.com/pierrec/lz4/v4"
	googleproto "google.golang.org/protobuf/proto"

	pb "github.com/indragunawan/titip/proto"
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

// GetBuffer retrieves a reusable *bytes.Buffer from the pool.
func GetBuffer() *bytes.Buffer {
	return bufferPool.Get().(*bytes.Buffer)
}

// PutBuffer resets and returns a *bytes.Buffer to the pool.
// Discards buffers exceeding 2MB to prevent heap bloat.
func PutBuffer(buf *bytes.Buffer) {
	if buf == nil {
		return
	}
	if buf.Cap() > maxBufferSize {
		return
	}
	buf.Reset()
	bufferPool.Put(buf)
}

// --- Response Recorder Pool ---

// ResponseRecorder intercepts and records HTTP responses from upstream origin handlers.
type ResponseRecorder struct {
	Code        int
	HeaderMap   http.Header
	Body        *bytes.Buffer
	wroteHeader bool
	flushed     bool
}

var responseRecorderPool = sync.Pool{
	New: func() any {
		return &ResponseRecorder{
			HeaderMap: make(http.Header),
			Body:      new(bytes.Buffer),
		}
	},
}

// GetResponseRecorder retrieves a pooled ResponseRecorder.
func GetResponseRecorder() *ResponseRecorder {
	rec := responseRecorderPool.Get().(*ResponseRecorder)
	rec.Reset()
	return rec
}

// PutResponseRecorder cleans and returns a ResponseRecorder to the pool.
func PutResponseRecorder(rec *ResponseRecorder) {
	if rec == nil {
		return
	}
	if rec.Body != nil && rec.Body.Cap() > maxBufferSize {
		rec.Body = new(bytes.Buffer)
	} else if rec.Body != nil {
		rec.Body.Reset()
	} else {
		rec.Body = new(bytes.Buffer)
	}
	rec.Reset()
	responseRecorderPool.Put(rec)
}

// Header returns the response headers map.
func (rec *ResponseRecorder) Header() http.Header {
	if rec.HeaderMap == nil {
		rec.HeaderMap = make(http.Header)
	}
	return rec.HeaderMap
}

// Write writes data to the internal response buffer.
func (rec *ResponseRecorder) Write(b []byte) (int, error) {
	if !rec.wroteHeader {
		rec.WriteHeader(http.StatusOK)
	}
	if rec.Body == nil {
		rec.Body = new(bytes.Buffer)
	}
	return rec.Body.Write(b)
}

// WriteHeader records the HTTP status code.
func (rec *ResponseRecorder) WriteHeader(code int) {
	if rec.wroteHeader {
		return
	}
	rec.Code = code
	rec.wroteHeader = true
}

// Flush implements http.Flusher.
func (rec *ResponseRecorder) Flush() {
	rec.flushed = true
}

// WroteHeader returns true if WriteHeader has been called.
func (rec *ResponseRecorder) WroteHeader() bool {
	return rec.wroteHeader
}

// Reset resets the recorder state for reuse.
func (rec *ResponseRecorder) Reset() {
	rec.Code = http.StatusOK
	rec.wroteHeader = false
	rec.flushed = false
	if rec.HeaderMap != nil {
		clear(rec.HeaderMap)
	}
	if rec.Body != nil {
		rec.Body.Reset()
	}
}

// --- Protobuf Struct Pools ---

var cacheMetadataPool = sync.Pool{
	New: func() any {
		return new(pb.CacheMetadata)
	},
}

var variantInfoPool = sync.Pool{
	New: func() any {
		return new(pb.VariantInfo)
	},
}

// GetCacheMetadata retrieves a pooled proto.CacheMetadata instance.
func GetCacheMetadata() *pb.CacheMetadata {
	return cacheMetadataPool.Get().(*pb.CacheMetadata)
}

// PutCacheMetadata resets and returns a proto.CacheMetadata instance to the pool.
func PutCacheMetadata(m *pb.CacheMetadata) {
	if m == nil {
		return
	}
	googleproto.Reset(m)
	cacheMetadataPool.Put(m)
}

// GetVariantInfo retrieves a pooled proto.VariantInfo instance.
func GetVariantInfo() *pb.VariantInfo {
	return variantInfoPool.Get().(*pb.VariantInfo)
}

// PutVariantInfo resets and returns a proto.VariantInfo instance to the pool.
func PutVariantInfo(v *pb.VariantInfo) {
	if v == nil {
		return
	}
	googleproto.Reset(v)
	variantInfoPool.Put(v)
}

// --- LZ4 Compression Pipeline & Pools ---

var lz4WriterPool = sync.Pool{
	New: func() any {
		return lz4.NewWriter(nil)
	},
}

var lz4ReaderPool = sync.Pool{
	New: func() any {
		return lz4.NewReader(nil)
	},
}

// CompressLZ4 compresses src bytes into the provided dst buffer using pooled LZ4 writers.
func CompressLZ4(src []byte, dst *bytes.Buffer) error {
	if dst == nil {
		return fmt.Errorf("titip: pool: compress: destination buffer is nil")
	}
	zw := lz4WriterPool.Get().(*lz4.Writer)
	defer lz4WriterPool.Put(zw)

	zw.Reset(dst)
	if _, err := zw.Write(src); err != nil {
		return fmt.Errorf("titip: pool: compress: write error: %w", err)
	}
	if err := zw.Close(); err != nil {
		return fmt.Errorf("titip: pool: compress: close error: %w", err)
	}
	return nil
}

var bytesReaderPool = sync.Pool{
	New: func() any {
		return bytes.NewReader(nil)
	},
}

// DecompressLZ4 decompresses LZ4 compressed src bytes into dst buffer using pooled LZ4 readers.
func DecompressLZ4(src []byte, dst *bytes.Buffer) error {
	if dst == nil {
		return fmt.Errorf("titip: pool: decompress: destination buffer is nil")
	}
	zr := lz4ReaderPool.Get().(*lz4.Reader)
	defer lz4ReaderPool.Put(zr)

	br := bytesReaderPool.Get().(*bytes.Reader)
	br.Reset(src)
	defer func() {
		br.Reset(nil)
		bytesReaderPool.Put(br)
	}()

	zr.Reset(br)
	if _, err := dst.ReadFrom(zr); err != nil {
		return fmt.Errorf("titip: pool: decompress: read error: %w", err)
	}
	return nil
}

