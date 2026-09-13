package titip

import (
	"bytes"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/pierrec/lz4/v4"
	"github.com/pquerna/cachecontrol/cacheobject"

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

// --- Response Recorder Pool ---

// responseRecorder intercepts and records HTTP responses from upstream origin handlers.
type responseRecorder struct {
	Code        int
	HeaderMap   http.Header
	Body        *bytes.Buffer
	wroteHeader bool
	flushed     bool
}

var responseRecorderPool = sync.Pool{
	New: func() any {
		return &responseRecorder{
			HeaderMap: make(http.Header),
			Body:      new(bytes.Buffer),
		}
	},
}

// getResponseRecorder retrieves a pooled responseRecorder.
func getResponseRecorder() *responseRecorder {
	rec := responseRecorderPool.Get().(*responseRecorder)
	rec.Reset()
	return rec
}

// putResponseRecorder cleans and returns a responseRecorder to the pool.
func putResponseRecorder(rec *responseRecorder) {
	if rec == nil {
		return
	}
	if rec.Body == nil || rec.Body.Cap() > maxBufferSize {
		rec.Body = new(bytes.Buffer)
	}
	rec.Reset()
	responseRecorderPool.Put(rec)
}

// Header returns the response headers map.
func (rec *responseRecorder) Header() http.Header {
	if rec.HeaderMap == nil {
		rec.HeaderMap = make(http.Header)
	}
	return rec.HeaderMap
}

// Write writes data to the internal response buffer.
func (rec *responseRecorder) Write(b []byte) (int, error) {
	if !rec.wroteHeader {
		rec.WriteHeader(http.StatusOK)
	}
	if rec.Body == nil {
		rec.Body = new(bytes.Buffer)
	}
	return rec.Body.Write(b)
}

// WriteHeader records the HTTP status code.
func (rec *responseRecorder) WriteHeader(code int) {
	if rec.wroteHeader {
		return
	}
	rec.Code = code
	rec.wroteHeader = true
}

// Flush implements http.Flusher.
func (rec *responseRecorder) Flush() {
	rec.flushed = true
}

// WroteHeader returns true if WriteHeader has been called.
func (rec *responseRecorder) WroteHeader() bool {
	return rec.wroteHeader
}

// Reset resets the recorder state for reuse.
func (rec *responseRecorder) Reset() {
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

// compressLZ4 compresses src bytes into the provided dst buffer using pooled LZ4 writers.
func compressLZ4(src []byte, dst *bytes.Buffer) error {
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

// decompressLZ4 decompresses LZ4 compressed src bytes into dst buffer using pooled LZ4 readers.
func decompressLZ4(src []byte, dst *bytes.Buffer) error {
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

// --- Request Context Pool ---

// requestContext holds request-scoped execution state across state transitions.
// Recycled via sync.Pool to maintain zero allocations on hot hit paths.
type requestContext struct {
	w              http.ResponseWriter
	r              *http.Request
	next           http.Handler
	reqCC          *cacheobject.RequestCacheDirectives
	primaryKey     string
	variantKey     string
	meta           *pb.CacheMetadata
	isSoftPurged   bool
	varInfo        *pb.VariantInfo
	freshness   freshnessInfo
	nowNano     int64 // nowNano is the timestamp evaluated during freshness check for RFC 9111 Age calculation.
	isVaryMiss  bool

	// serverTiming indicates whether Server-Timing header generation is active for this request.
	serverTiming bool

	// startNano is the immutable request arrival timestamp used for total duration calculation.
	startNano int64

	// metaDuration is the Stage 1 Redis metadata lookup duration.
	metaDuration time.Duration

	// bodyDuration is the Stage 2 Redis body retrieval and decompression duration.
	bodyDuration time.Duration

	// originDuration is the upstream backend origin fetch duration.
	originDuration time.Duration

	// storeDuration is the cache storage (LZ4 compression + Redis write) duration.
	storeDuration time.Duration

	// esiDuration is the ESI fragment fetching and assembly duration.
	esiDuration time.Duration

	// esiFragments is the count of ESI fragment tags processed in the response.
	esiFragments int
}

// Reset clears all fields before returning the struct to the pool.
func (ctx *requestContext) Reset() {
	ctx.w = nil
	ctx.r = nil
	ctx.next = nil
	ctx.reqCC = nil
	ctx.primaryKey = ""
	ctx.variantKey = ""
	ctx.meta = nil
	ctx.isSoftPurged = false
	ctx.varInfo = nil
	ctx.freshness = freshnessInfo{}
	ctx.nowNano = 0
	ctx.isVaryMiss = false
	ctx.serverTiming = false
	ctx.startNano = 0
	ctx.metaDuration = 0
	ctx.bodyDuration = 0
	ctx.originDuration = 0
	ctx.storeDuration = 0
	ctx.esiDuration = 0
	ctx.esiFragments = 0
}

var requestContextPool = sync.Pool{
	New: func() any {
		return new(requestContext)
	},
}

func acquireRequestContext(w http.ResponseWriter, r *http.Request, next http.Handler) *requestContext {
	ctx := requestContextPool.Get().(*requestContext)
	ctx.w = w
	ctx.r = r
	ctx.next = next
	now := time.Now().UnixNano()
	ctx.startNano = now
	ctx.nowNano = now
	return ctx
}

func releaseRequestContext(ctx *requestContext) {
	if ctx == nil {
		return
	}
	ctx.Reset()
	requestContextPool.Put(ctx)
}
