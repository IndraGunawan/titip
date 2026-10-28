package titip

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/pquerna/cachecontrol/cacheobject"

	pb "github.com/indragunawan/titip/proto"
)

// requestContext holds request-scoped execution state across state transitions.
// Recycled via sync.Pool to maintain zero allocations on hot hit paths.
type requestContext struct {
	w            http.ResponseWriter
	r            *http.Request
	next         http.Handler
	reqCC        *cacheobject.RequestCacheDirectives
	primaryKey   string
	variantKey   string
	meta         *pb.CacheMetadata
	isSoftPurged bool
	varInfo      *pb.VariantInfo
	nowNano      int64 // nowNano is the timestamp evaluated during freshness check for RFC 9111 Age calculation.
	isVaryMiss   bool

	// timing captures optional Server-Timing diagnostics.
	// Embedded by value
	timing serverTimingRecorder
}

// Reset clears all fields before returning the struct to the pool.
func (ctx *requestContext) Reset() {
	*ctx = requestContext{}
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
	ctx.timing.startNano = now
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

func (t *Titip) emitCacheStatus(ctx *requestContext, simpleStatus, rfc9211Value string) {
	w := ctx.w
	switch t.config.cacheStatusMode {
	case CacheStatusRFC9211:
		if rfc9211Value != "" {
			// RFC 9211 §2: Multi-cache chaining - append to existing or set new Cache-Status header
			w.Header().Add(headerCacheStatus, rfc9211Value)
		}
	case CacheStatusSimple:
		// Simple status replaces upstream header with Titip's definitive local status
		w.Header().Set(headerCacheStatus, simpleStatus)
	case CacheStatusNone:
		// Do not emit Cache-Status header
	}

	if ctx.timing.enabled {
		t.config.serverTiming.emit(w, &ctx.timing, simpleStatus)
	}
}

func (t *Titip) recordRequest(ctx *requestContext, status string) {
	if t.metrics == nil {
		return
	}
	var dur time.Duration
	if ctx != nil && ctx.timing.startNano > 0 {
		dur = time.Duration(time.Now().UnixNano() - ctx.timing.startNano)
	}
	t.metrics.recordRequest(status, dur)
}

// loadDecompressed fetches and decompresses the cached variant body.
// Returns varInfo, pooled buffer (caller must putBuffer), ok.
func (t *Titip) loadDecompressed(ctx *requestContext) (*pb.VariantInfo, *bytes.Buffer, bool) {
	varCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx.r.Context()), t.config.storageTimeout)
	varInfo, compBody, err := t.storage.GetVariant(varCtx, ctx.primaryKey, ctx.variantKey)
	cancel()
	if err != nil || varInfo == nil || (varInfo.RawBodySize > 0 && len(compBody) == 0) {
		return nil, nil, false
	}

	codec := varInfo.StorageCompression
	if codec == "" {
		codec = StorageCompressionLZ4
	}
	comp, ok := t.config.compressors[codec]
	if !ok {
		// Do not purge: in a rolling deployment, another node may legitimately own this codec.
		// Simply fail open to origin at debug level to avoid node thrashing.
		if t.logger.Enabled(ctx.r.Context(), slog.LevelDebug) {
			t.logger.DebugContext(ctx.r.Context(), "unknown storage compression codec, failing open to origin",
				slog.String("codec", codec),
				slog.String("key", ctx.primaryKey),
			)
		}
		return nil, nil, false
	}

	buf := getBuffer()
	if varInfo.RawBodySize > 0 {
		buf.Grow(int(varInfo.RawBodySize))
	}
	if err := comp.Decompress(compBody, buf); err != nil || int64(buf.Len()) != varInfo.RawBodySize {
		gotBytes := buf.Len()
		putBuffer(buf)

		// Synchronously soft-purge the corrupted entry so subsequent requests coalesce
		// via singleflight revalidation, preventing origin stampedes.
		purgeCtx, purgeCancel := context.WithTimeout(context.WithoutCancel(ctx.r.Context()), t.config.storageTimeout)
		_, _ = t.storage.Purge(purgeCtx, ctx.primaryKey, true) // soft: true
		purgeCancel()

		if t.logger.Enabled(ctx.r.Context(), slog.LevelWarn) {
			t.logger.WarnContext(ctx.r.Context(), "corrupted cache payload detected, soft-purged and failing open to origin",
				slog.Any("error", err),
				slog.Int64("expected_bytes", varInfo.RawBodySize),
				slog.Int("got_bytes", gotBytes),
				slog.String("codec", codec),
				slog.String("key", ctx.primaryKey),
			)
		}
		return nil, nil, false
	}
	if ctx.timing.enabled {
		ctx.timing.storeCodec = codec
		ctx.timing.storeRawSize = varInfo.RawBodySize
		ctx.timing.storeCompSize = int64(len(compBody))
	}
	return varInfo, buf, true
}

func (t *Titip) spawnSWR(ctx *requestContext) {
	if t.closed.Load() {
		return
	}
	t.swrWG.Add(1)
	reqClone := ctx.r.Clone(context.WithoutCancel(ctx.r.Context()))
	next := ctx.next
	pk, vk := ctx.primaryKey, ctx.variantKey
	go func() {
		defer t.swrWG.Done()
		t.revalidateOriginAsync(reqClone, next, pk, vk)
	}()
}
