package titip

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	pb "github.com/indragunawan/titip/proto"
)

// DefaultVariantKey is used as the variant key when no Vary headers are specified.
const DefaultVariantKey = "default"

// stateFn represents a single state transition in the request processing pipeline.
// Returning nil terminates the state machine loop.
type stateFn func(t *Titip, ctx *requestContext) stateFn

func (t *Titip) serveHTTP(w http.ResponseWriter, r *http.Request, next http.Handler) {
	ctx := acquireRequestContext(w, r, next)
	defer releaseRequestContext(ctx)

	for state := stateCheckBypass; state != nil; {
		state = state(t, ctx)
	}
}

// 1. stateCheckBypass: Protocol bypass guards, stream bypass, mutating method handling
func stateCheckBypass(t *Titip, ctx *requestContext) stateFn {
	defer func() {
		if p := recover(); p != nil {
			if t.logger.Enabled(ctx.r.Context(), slog.LevelError) {
				t.logger.ErrorContext(ctx.r.Context(), "titip: handler panic recovered", "panic", p)
			}
			http.Error(ctx.w, "Internal Server Error", http.StatusInternalServerError)
		}
	}()

	// A. WebSocket Handshake Bypass
	if strings.EqualFold(ctx.r.Header.Get(HeaderUpgrade), UpgradeWebSocket) {
		t.metrics.RecordRequest(StatusBypass)
		t.emitCacheStatus(ctx.w, "BYPASS", "fwd=bypass; detail=websocket-upgrade")
		return stateBypassOrigin
	}

	// B. Server-Sent Events (SSE) Bypass
	if strings.Contains(strings.ToLower(ctx.r.Header.Get(HeaderAccept)), ContentTypeEventStream) {
		t.metrics.RecordRequest(StatusBypass)
		t.emitCacheStatus(ctx.w, "BYPASS", "fwd=bypass; detail=sse-stream")
		return stateBypassOrigin
	}

	// C. Range Byte Request Bypass
	if ctx.r.Header.Get(HeaderRange) != "" {
		t.metrics.RecordRequest(StatusBypass)
		t.emitCacheStatus(ctx.w, "BYPASS", "fwd=bypass; detail=range-request")
		return stateBypassOrigin
	}

	// D. Mutating Methods (POST, PUT, DELETE, PATCH)
	if isMutatingMethod(ctx.r.Method) {
		t.metrics.RecordRequest(StatusBypass)
		t.handleMutatingRequest(ctx.w, ctx.r, ctx.next)
		return nil
	}

	// E. Non-Cacheable Methods (only GET and HEAD are cached)
	if ctx.r.Method != http.MethodGet && ctx.r.Method != http.MethodHead {
		t.metrics.RecordRequest(StatusBypass)
		t.emitCacheStatus(ctx.w, "BYPASS", "fwd=bypass")
		return stateBypassOrigin
	}

	// F. Client Cache-Control: no-store
	if !t.cfg.IgnoreClientCacheControl {
		if cc := ctx.r.Header.Get(HeaderCacheControl); strings.Contains(cc, "no-store") {
			t.metrics.RecordRequest(StatusBypass)
			t.emitCacheStatus(ctx.w, "BYPASS", "fwd=bypass")
			return stateBypassOrigin
		}
	}

	return stateLookupMetadata
}

// 2. stateLookupMetadata: Generates Primary Key and looks up Stage 1 metadata in Redis
func stateLookupMetadata(t *Titip, ctx *requestContext) stateFn {
	ctx.primaryKey = GeneratePrimaryKey(ctx.r, &t.cfg.KeyConfig)

	storeCtx, storeCancel := context.WithTimeout(context.WithoutCancel(ctx.r.Context()), t.cfg.StorageTimeout)
	meta, err := t.storage.GetMeta(storeCtx, ctx.primaryKey)
	storeCancel()

	if err != nil {
		t.metrics.RecordRequest(StatusError)
		if t.logger.Enabled(ctx.r.Context(), slog.LevelError) {
			t.logger.ErrorContext(ctx.r.Context(), "titip: storage error fetching metadata, bypassing to origin", "error", err, "key", ctx.primaryKey)
		}
		t.emitCacheStatus(ctx.w, "BYPASS", "fwd=bypass; detail=storage-fallback")
		return stateBypassOrigin
	}

	ctx.meta = meta
	if meta == nil {
		// Cold URL Miss -> fetch origin directly without singleflight
		return stateFetchOriginCold
	}

	return stateMatchVariant
}

// 3. stateMatchVariant: Evaluates Vary headers and matches variant in metadata
func stateMatchVariant(t *Titip, ctx *requestContext) stateFn {
	ctx.variantKey = GenerateVariantKey(ctx.r, ctx.meta.VaryHeaderNames)
	if ctx.variantKey == "" {
		ctx.variantKey = DefaultVariantKey
	}

	varInfo, exists := ctx.meta.Variants[ctx.variantKey]
	if !exists || varInfo == nil {
		// Variant Miss -> fetch new variant directly without singleflight
		return stateFetchOriginCold
	}
	ctx.varInfo = varInfo

	return stateEvaluateFreshness
}

// 4. stateEvaluateFreshness: Computes RFC-7234 freshness, handles downstream 304, SWR, and stale states
func stateEvaluateFreshness(t *Titip, ctx *requestContext) stateFn {
	ctx.nowNano = time.Now().UnixNano()

	// If entry is soft-purged, refresh synchronously via stale revalidation
	if ctx.meta.IsSoftPurged {
		return stateFetchOriginStale
	}

	isFresh := ctx.nowNano <= ctx.meta.ExpiresAtUnixNano

	// Downstream 304 Not Modified Check (Client <-> Titip)
	// If client provided If-None-Match / If-Modified-Since and it matches, serve 304 with 0 body I/O
	if t.checkConditionalMatch(ctx.r, ctx.varInfo) {
		return stateServe304
	}

	// Fresh Cache Hit
	if isFresh {
		return stateServeCachedHit
	}

	// Stale-While-Revalidate Window
	if ctx.meta.StaleUntilUnixNano > ctx.meta.ExpiresAtUnixNano && ctx.nowNano <= ctx.meta.StaleUntilUnixNano {
		return stateServeSWR
	}

	// Expired -> Synchronous Singleflight Revalidation
	return stateFetchOriginStale
}

// 5. stateServe304: Writes HTTP 304 Not Modified directly with 0 Redis Body I/O
func stateServe304(t *Titip, ctx *requestContext) stateFn {
	t.metrics.RecordRequest(StatusHit)
	t.emitCacheStatus(ctx.w, "HIT", "hit")
	t.copyProtoHeaders(ctx.w, ctx.varInfo.ResponseHeaders)
	ctx.w.WriteHeader(http.StatusNotModified)
	return nil
}

// 6. stateServeCachedHit: Fetches compressed body from Redis, decompresses and writes response
func stateServeCachedHit(t *Titip, ctx *requestContext) stateFn {
	// HEAD Request Handling (0 body I/O)
	if ctx.r.Method == http.MethodHead {
		t.metrics.RecordRequest(StatusHit)
		t.emitCacheStatus(ctx.w, "HIT", fmt.Sprintf("hit; ttl=%d", t.calcTTL(ctx.meta.ExpiresAtUnixNano, ctx.nowNano)))
		t.copyProtoHeaders(ctx.w, ctx.varInfo.ResponseHeaders)
		ctx.w.WriteHeader(int(ctx.varInfo.StatusCode))
		return nil
	}

	varCtx, varCancel := context.WithTimeout(context.WithoutCancel(ctx.r.Context()), t.cfg.StorageTimeout)
	varInfo, compBody, err := t.storage.GetVariant(varCtx, ctx.primaryKey, ctx.variantKey)
	varCancel()

	if err != nil || varInfo == nil || len(compBody) == 0 {
		// Fail-open to origin on body retrieval error
		return stateFetchOriginCold
	}

	dstBuf := GetBuffer()
	defer PutBuffer(dstBuf)

	if err := DecompressLZ4(compBody, dstBuf); err != nil {
		if t.logger.Enabled(ctx.r.Context(), slog.LevelError) {
			t.logger.ErrorContext(ctx.r.Context(), "titip: decompression error, failing open to origin", "error", err)
		}
		return stateFetchOriginCold
	}

	if t.logger.Enabled(ctx.r.Context(), slog.LevelDebug) {
		t.logger.DebugContext(ctx.r.Context(), "titip: payload decompressed",
			slog.String("key", ctx.primaryKey),
			slog.String("variant", ctx.variantKey),
			slog.Int("raw_bytes", int(varInfo.RawBodySize)),
			slog.Int("compressed_bytes", len(compBody)),
		)
	}

	currentAge := (ctx.nowNano - ctx.meta.CreatedAtUnixNano) / int64(time.Second)
	if currentAge < 0 {
		currentAge = 0
	}
	t.metrics.RecordRequest(StatusHit)
	t.copyProtoHeaders(ctx.w, varInfo.ResponseHeaders)
	ctx.w.Header().Set(HeaderAge, fmt.Sprintf("%d", currentAge))
	t.emitCacheStatus(ctx.w, "HIT", fmt.Sprintf("hit; ttl=%d", t.calcTTL(ctx.meta.ExpiresAtUnixNano, ctx.nowNano)))
	ctx.w.WriteHeader(int(varInfo.StatusCode))
	_, _ = ctx.w.Write(dstBuf.Bytes())
	return nil
}

// 7. stateServeSWR: Serves stale cached variant and triggers background revalidation
func stateServeSWR(t *Titip, ctx *requestContext) stateFn {
	varCtx, varCancel := context.WithTimeout(context.WithoutCancel(ctx.r.Context()), t.cfg.StorageTimeout)
	varInfo, compBody, err := t.storage.GetVariant(varCtx, ctx.primaryKey, ctx.variantKey)
	varCancel()

	if err != nil || varInfo == nil || len(compBody) == 0 {
		return stateFetchOriginCold
	}

	dstBuf := GetBuffer()
	defer PutBuffer(dstBuf)

	if err := DecompressLZ4(compBody, dstBuf); err != nil {
		return stateFetchOriginCold
	}

	currentAge := (ctx.nowNano - ctx.meta.CreatedAtUnixNano) / int64(time.Second)
	if currentAge < 0 {
		currentAge = 0
	}
	t.metrics.RecordRequest(StatusStaleHit)
	t.copyProtoHeaders(ctx.w, varInfo.ResponseHeaders)
	ctx.w.Header().Set(HeaderAge, fmt.Sprintf("%d", currentAge))
	t.emitCacheStatus(ctx.w, "STALE", "hit; stale; detail=swr")
	ctx.w.WriteHeader(int(varInfo.StatusCode))
	_, _ = ctx.w.Write(dstBuf.Bytes())

	// Async Background Revalidation
	if !t.closed.Load() {
		t.swrWG.Add(1)
		reqClone := ctx.r.Clone(context.Background())
		nextHandler := ctx.next
		pk := ctx.primaryKey
		m := ctx.meta
		vk := ctx.variantKey
		go func() {
			defer t.swrWG.Done()
			t.revalidateOrigin(reqClone, nextHandler, pk, m, vk)
		}()
	}

	return nil
}

// 8. stateFetchOriginCold: Direct Origin Fetch (NO singleflight) to eliminate Set-Cookie leaks
func stateFetchOriginCold(t *Titip, ctx *requestContext) stateFn {
	detachedCtx := context.WithoutCancel(ctx.r.Context())
	originCtx, cancel := context.WithTimeout(detachedCtx, t.cfg.OriginTimeout)
	defer cancel()

	rec := GetResponseRecorder()
	defer PutResponseRecorder(rec)

	reqTime := time.Now()
	var panicked bool
	func() {
		defer func() {
			if p := recover(); p != nil {
				panicked = true
				if t.logger.Enabled(originCtx, slog.LevelError) {
					t.logger.ErrorContext(originCtx, "titip: cold origin handler panic", "panic", p)
				}
			}
		}()
		ctx.next.ServeHTTP(rec, ctx.r.WithContext(originCtx))
	}()

	if panicked {
		http.Error(ctx.w, "Internal Server Error", http.StatusInternalServerError)
		return nil
	}

	respTime := time.Now()
	bodyBytes := bytes.Clone(rec.Body.Bytes())
	headersClone := rec.Header().Clone()

	// Calculate freshness & evaluate cacheability
	freshness := CalculateFreshness(rec.Code, headersClone, reqTime, respTime, respTime, t.cacheableStatusCodes)

	// Stream / SSE Response Detection
	if strings.Contains(strings.ToLower(headersClone.Get(HeaderContentType)), ContentTypeEventStream) {
		t.metrics.RecordRequest(StatusBypass)
		t.emitCacheStatus(ctx.w, "BYPASS", "fwd=bypass; detail=sse-response")
		for k, vv := range headersClone {
			for _, v := range vv {
				ctx.w.Header().Add(k, v)
			}
		}
		ctx.w.WriteHeader(rec.Code)
		if ctx.r.Method != http.MethodHead {
			_, _ = ctx.w.Write(bodyBytes)
		}
		return nil
	}

	// Cache if eligible and not closed
	if freshness.IsCacheable && !t.closed.Load() {
		t.saveVariantToStorage(originCtx, ctx.primaryKey, ctx.variantKey, rec.Code, ctx.r, headersClone, bodyBytes, freshness, respTime)
	}

	// Serve Response to Client
	for k, vv := range headersClone {
		for _, v := range vv {
			ctx.w.Header().Add(k, v)
		}
	}

	if freshness.EffectiveTTL > 0 && freshness.IsCacheable {
		t.metrics.RecordRequest(StatusMiss)
		t.emitCacheStatus(ctx.w, "MISS", fmt.Sprintf("fwd=uri-miss; fwd-status=%d; stored; ttl=%d", rec.Code, int(freshness.EffectiveTTL.Seconds())))
	} else {
		t.metrics.RecordRequest(StatusBypass)
		t.emitCacheStatus(ctx.w, "BYPASS", fmt.Sprintf("fwd=bypass; fwd-status=%d", rec.Code))
	}

	ctx.w.WriteHeader(rec.Code)
	if ctx.r.Method != http.MethodHead {
		_, _ = ctx.w.Write(bodyBytes)
	}

	return nil
}

// 9. stateFetchOriginStale: Singleflight Coalesced Revalidation (with Upstream 304 support & stale fallback)
func stateFetchOriginStale(t *Titip, ctx *requestContext) stateFn {
	sfKey := ctx.primaryKey + "|" + ctx.variantKey

	// Fetch existing stale variant from Redis for 304 revalidation or 5xx fallback
	var staleVar *pb.VariantInfo
	var staleCompBody []byte
	if ctx.varInfo != nil {
		staleVar = ctx.varInfo
		varCtx, varCancel := context.WithTimeout(context.WithoutCancel(ctx.r.Context()), t.cfg.StorageTimeout)
		_, staleCompBody, _ = t.storage.GetVariant(varCtx, ctx.primaryKey, ctx.variantKey)
		varCancel()
	}

	val, err, _ := t.sf.Do(sfKey, func() (any, error) {
		detachedCtx := context.WithoutCancel(ctx.r.Context())
		originCtx, cancel := context.WithTimeout(detachedCtx, t.cfg.OriginTimeout)
		defer cancel()

		rec := GetResponseRecorder()
		defer PutResponseRecorder(rec)

		// Attach conditional headers for Upstream 304 Revalidation
		revalReq := ctx.r.Clone(originCtx)
		if staleVar != nil {
			if staleVar.Etag != "" {
				revalReq.Header.Set(HeaderIfNoneMatch, staleVar.Etag)
			}
			if staleVar.LastModifiedUnixNano > 0 {
				revalReq.Header.Set(HeaderIfModifiedSince, time.Unix(0, staleVar.LastModifiedUnixNano).UTC().Format(http.TimeFormat))
			}
		}

		reqTime := time.Now()
		var panicked bool
		func() {
			defer func() {
				if p := recover(); p != nil {
					panicked = true
					if t.logger.Enabled(originCtx, slog.LevelError) {
						t.logger.ErrorContext(originCtx, "titip: revalidation origin handler panic", "panic", p)
					}
				}
			}()
			ctx.next.ServeHTTP(rec, revalReq)
		}()

		// Fallback to stale cache on 5xx or panic
		if (panicked || rec.Code >= 500) && staleVar != nil && len(staleCompBody) > 0 {
			return &fetchResult{
				isFallback: true,
				fallback:   &staleFallback{varInfo: staleVar, body: staleCompBody, meta: ctx.meta},
				statusCode: rec.Code,
				panicked:   panicked,
			}, nil
		}

		respTime := time.Now()
		headersClone := rec.Header().Clone()
		bodyBytes := bytes.Clone(rec.Body.Bytes())

		// Upstream 304 Not Modified Handling
		if rec.Code == http.StatusNotModified && staleVar != nil && len(staleCompBody) > 0 {
			freshness := CalculateFreshness(200, headersClone, reqTime, respTime, respTime, t.cacheableStatusCodes)
			if freshness.EffectiveTTL > 0 && !t.closed.Load() {
				// Refresh Redis TTL without rewriting body
				ctx.meta.ExpiresAtUnixNano = respTime.Add(freshness.EffectiveTTL).UnixNano()
				if freshness.StaleWhileRevalidateTTL > 0 {
					ctx.meta.StaleUntilUnixNano = ctx.meta.ExpiresAtUnixNano + int64(freshness.StaleWhileRevalidateTTL)
				}
				ctx.meta.IsSoftPurged = false
				staleWindow := max(freshness.StaleWhileRevalidateTTL, freshness.StaleIfErrorTTL)
				storageTTL := freshness.EffectiveTTL + staleWindow
				if storageTTL <= 0 {
					storageTTL = freshness.EffectiveTTL
				}
				_ = t.storage.SetVariant(originCtx, ctx.primaryKey, ctx.meta, staleVar, staleCompBody, storageTTL)
			}

			return &fetchResult{
				statusCode:  200,
				is304Origin: true,
				fallback:    &staleFallback{varInfo: staleVar, body: staleCompBody, meta: ctx.meta},
				ttl:         freshness.EffectiveTTL,
			}, nil
		}

		freshness := CalculateFreshness(rec.Code, headersClone, reqTime, respTime, respTime, t.cacheableStatusCodes)
		if freshness.IsCacheable && !t.closed.Load() {
			t.saveVariantToStorage(originCtx, ctx.primaryKey, ctx.variantKey, rec.Code, ctx.r, headersClone, bodyBytes, freshness, respTime)
		}

		return &fetchResult{
			statusCode: rec.Code,
			headers:    headersClone,
			body:       bodyBytes,
			ttl:        freshness.EffectiveTTL,
		}, nil
	})

	if err != nil {
		if t.logger.Enabled(ctx.r.Context(), slog.LevelError) {
			t.logger.ErrorContext(ctx.r.Context(), "titip: singleflight execution error", "error", err)
		}
		http.Error(ctx.w, "Internal Server Error", http.StatusInternalServerError)
		return nil
	}

	res, ok := val.(*fetchResult)
	if !ok || res == nil {
		http.Error(ctx.w, "Internal Server Error", http.StatusInternalServerError)
		return nil
	}

	// Serve stale fallback or origin 304 refreshed cached body
	if (res.isFallback || res.is304Origin) && res.fallback != nil {
		dstBuf := GetBuffer()
		defer PutBuffer(dstBuf)

		if err := DecompressLZ4(res.fallback.body, dstBuf); err == nil {
			t.copyProtoHeaders(ctx.w, res.fallback.varInfo.ResponseHeaders)
			if res.is304Origin {
				t.metrics.RecordRequest(StatusMiss)
				t.emitCacheStatus(ctx.w, "MISS", "fwd=uri-miss; fwd-status=304; stored; detail=304-refreshed")
			} else {
				fwdStatus := res.statusCode
				if fwdStatus == 0 {
					fwdStatus = 500
				}
				t.metrics.RecordRequest(StatusStaleHit)
				t.emitCacheStatus(ctx.w, "STALE", fmt.Sprintf("hit; stale; fwd=stale; fwd-status=%d; detail=stale-if-error", fwdStatus))
			}
			ctx.w.WriteHeader(int(res.fallback.varInfo.StatusCode))
			if ctx.r.Method != http.MethodHead {
				_, _ = ctx.w.Write(dstBuf.Bytes())
			}
			return nil
		}
	}

	// Serve fresh response
	for k, vv := range res.headers {
		for _, v := range vv {
			ctx.w.Header().Add(k, v)
		}
	}

	t.metrics.RecordRequest(StatusMiss)
	t.emitCacheStatus(ctx.w, "MISS", fmt.Sprintf("fwd=uri-miss; fwd-status=%d; stored; detail=soft-refreshed", res.statusCode))
	ctx.w.WriteHeader(res.statusCode)
	if ctx.r.Method != http.MethodHead {
		_, _ = ctx.w.Write(res.body)
	}

	return nil
}

// 10. stateBypassOrigin: Transparent Pass-Through
func stateBypassOrigin(t *Titip, ctx *requestContext) stateFn {
	ctx.next.ServeHTTP(ctx.w, ctx.r)
	return nil
}

// --- Helper Functions ---

type staleFallback struct {
	varInfo *pb.VariantInfo
	body    []byte
	meta    *pb.CacheMetadata
}

type fetchResult struct {
	statusCode  int
	headers     http.Header
	body        []byte
	isFallback  bool
	is304Origin bool
	fallback    *staleFallback
	panicked    bool
	ttl         time.Duration
}

func (t *Titip) saveVariantToStorage(
	ctx context.Context,
	primaryKey, variantKey string,
	statusCode int,
	r *http.Request,
	headers http.Header,
	bodyBytes []byte,
	freshness FreshnessInfo,
	respTime time.Time,
) {
	tags := t.extractTags(headers)

	// Compress body payload
	compBuf := GetBuffer()
	_ = CompressLZ4(bodyBytes, compBuf)
	compBytes := bytes.Clone(compBuf.Bytes())
	PutBuffer(compBuf)

	// Build / update metadata
	varNames := t.extractVaryHeaderNames(headers)
	varKey := variantKey
	if varKey == "" || varKey == DefaultVariantKey {
		varKey = GenerateVariantKey(r, varNames)
	}
	if varKey == "" {
		varKey = DefaultVariantKey
	}

	if t.logger.Enabled(r.Context(), slog.LevelDebug) {
		rawLen := len(bodyBytes)
		compLen := len(compBytes)
		ratio := 0.0
		if rawLen > 0 {
			ratio = (1.0 - float64(compLen)/float64(rawLen)) * 100.0
		}
		t.logger.DebugContext(r.Context(), "titip: payload compressed",
			slog.String("key", primaryKey),
			slog.String("variant", varKey),
			slog.Int("raw_bytes", rawLen),
			slog.Int("compressed_bytes", compLen),
			slog.String("savings_pct", fmt.Sprintf("%.2f%%", ratio)),
		)
	}

	newMeta := &pb.CacheMetadata{
		PrimaryKey:        primaryKey,
		VaryHeaderNames:   varNames,
		CreatedAtUnixNano: respTime.UnixNano(),
		ExpiresAtUnixNano: respTime.Add(freshness.EffectiveTTL).UnixNano(),
		Tags:              tags,
		IsSoftPurged:      false,
	}
	if freshness.StaleWhileRevalidateTTL > 0 {
		newMeta.StaleUntilUnixNano = newMeta.ExpiresAtUnixNano + int64(freshness.StaleWhileRevalidateTTL)
	}

	newVariant := &pb.VariantInfo{
		VariantKey:         varKey,
		StatusCode:         int32(statusCode),
		ResponseHeaders:    protoHeadersFromHTTP(headers),
		Etag:               headers.Get(HeaderETag),
		RawBodySize:        uint32(len(bodyBytes)),
		CompressedBodySize: uint32(len(compBytes)),
	}
	if lm, err := ParseDate(headers.Get(HeaderLastModified)); err == nil && !lm.IsZero() {
		newVariant.LastModifiedUnixNano = lm.UnixNano()
	}

	staleWindow := max(freshness.StaleWhileRevalidateTTL, freshness.StaleIfErrorTTL)
	storageTTL := freshness.EffectiveTTL + staleWindow
	if storageTTL <= 0 {
		storageTTL = freshness.EffectiveTTL
	}

	if storeErr := t.storage.SetVariant(ctx, primaryKey, newMeta, newVariant, compBytes, storageTTL); storeErr != nil {
		if t.logger.Enabled(ctx, slog.LevelError) {
			t.logger.ErrorContext(ctx, "titip: storage error saving variant", "error", storeErr, "key", primaryKey)
		}
	}
}

func (t *Titip) revalidateOrigin(r *http.Request, next http.Handler, primaryKey string, meta *pb.CacheMetadata, variantKey string) {
	defer func() {
		if p := recover(); p != nil {
			if t.logger.Enabled(context.Background(), slog.LevelError) {
				t.logger.ErrorContext(context.Background(), "titip: background revalidation panic", "panic", p)
			}
		}
	}()

	bgCtx, cancel := context.WithTimeout(context.Background(), t.cfg.OriginTimeout)
	defer cancel()

	rec := GetResponseRecorder()
	defer PutResponseRecorder(rec)

	reqTime := time.Now()
	next.ServeHTTP(rec, r.WithContext(bgCtx))
	respTime := time.Now()

	headers := rec.Header().Clone()
	bodyBytes := bytes.Clone(rec.Body.Bytes())

	freshness := CalculateFreshness(rec.Code, headers, reqTime, respTime, respTime, t.cacheableStatusCodes)
	if freshness.IsCacheable && !t.closed.Load() {
		t.saveVariantToStorage(bgCtx, primaryKey, variantKey, rec.Code, r, headers, bodyBytes, freshness, respTime)
	}
}

func (t *Titip) handleMutatingRequest(w http.ResponseWriter, r *http.Request, next http.Handler) {
	next.ServeHTTP(w, r)

	if t.cfg.AutoInvalidateMutatingMethods {
		primaryKey := GeneratePrimaryKey(r, &t.cfg.KeyConfig)
		delCtx, delCancel := context.WithTimeout(context.Background(), t.cfg.StorageTimeout)
		defer delCancel()
		_ = t.storage.Delete(delCtx, primaryKey)
	}
}

func isMutatingMethod(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch:
		return true
	default:
		return false
	}
}

func (t *Titip) checkConditionalMatch(r *http.Request, varInfo *pb.VariantInfo) bool {
	// If-None-Match (Weak ETag comparison per RFC-7232 §2.3.2)
	if ifNoneMatch := r.Header.Get(HeaderIfNoneMatch); ifNoneMatch != "" {
		if ifNoneMatch == "*" && varInfo != nil {
			return true
		}
		if varInfo != nil && varInfo.Etag != "" {
			for _, tag := range strings.Split(ifNoneMatch, ",") {
				if ETagMatches(tag, varInfo.Etag) {
					return true
				}
			}
		}
		return false
	}

	// If-Modified-Since
	if ifModSince := r.Header.Get(HeaderIfModifiedSince); ifModSince != "" && varInfo != nil && varInfo.LastModifiedUnixNano > 0 {
		clientTime, err := ParseDate(ifModSince)
		if err == nil {
			cachedTime := time.Unix(0, varInfo.LastModifiedUnixNano)
			return !cachedTime.After(clientTime)
		}
	}

	return false
}

func (t *Titip) calcTTL(expiresAtUnixNano, nowNano int64) int64 {
	ttlSec := (expiresAtUnixNano - nowNano) / int64(time.Second)
	if ttlSec < 0 {
		return 0
	}
	return ttlSec
}

func (t *Titip) emitCacheStatus(w http.ResponseWriter, simpleToken, rfc9211Detail string) {
	switch t.cfg.CacheStatusMode {
	case CacheStatusRFC9211:
		w.Header().Set(HeaderCacheStatus, fmt.Sprintf("titip; %s", rfc9211Detail))
	case CacheStatusSimpleToken:
		w.Header().Set(HeaderCacheStatus, simpleToken)
	case CacheStatusNone:
		// Do not emit Cache-Status header
	}
}

func (t *Titip) extractTags(headers http.Header) []string {
	name := t.cfg.TagHeaderName
	if name == "" {
		name = HeaderCacheTag
	}
	val := headers.Get(name)
	if val == "" {
		return nil
	}
	return splitAndTrimTags(val)
}

func splitAndTrimTags(s string) []string {
	parts := strings.FieldsFunc(s, func(r rune) bool {
		return r == ',' || r == ' '
	})
	var result []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

func (t *Titip) extractVaryHeaderNames(headers http.Header) []string {
	varyHeader := headers.Get(HeaderVary)
	if varyHeader == "" {
		return nil
	}
	parts := strings.Split(varyHeader, ",")
	var names []string
	for _, p := range parts {
		name := strings.TrimSpace(p)
		if name != "" && !slices.Contains(names, name) {
			names = append(names, name)
		}
	}
	return names
}

// isHopByHopHeader checks if a header is a standard hop-by-hop header per RFC 9110 §7.6.1 & RFC 7230 §6.1.
func isHopByHopHeader(k string) bool {
	switch http.CanonicalHeaderKey(k) {
	case "Connection",
		"Keep-Alive",
		"Proxy-Authenticate",
		"Proxy-Authorization",
		"Te",
		"Trailers",
		"Transfer-Encoding",
		"Upgrade":
		return true
	default:
		return false
	}
}

func protoHeadersFromHTTP(h http.Header) map[string]*pb.HeaderValues {
	// Parse Connection header tokens for additional custom hop-by-hop headers
	var connTokens map[string]struct{}
	if conn := h.Get("Connection"); conn != "" {
		tokens := strings.Split(conn, ",")
		connTokens = make(map[string]struct{}, len(tokens))
		for _, tok := range tokens {
			tok = http.CanonicalHeaderKey(strings.TrimSpace(tok))
			if tok != "" {
				connTokens[tok] = struct{}{}
			}
		}
	}

	m := make(map[string]*pb.HeaderValues, len(h))
	for k, vv := range h {
		canon := http.CanonicalHeaderKey(k)
		if isHopByHopHeader(canon) {
			continue
		}
		if connTokens != nil {
			if _, ok := connTokens[canon]; ok {
				continue
			}
		}
		m[k] = &pb.HeaderValues{Values: vv}
	}
	return m
}

func (t *Titip) copyProtoHeaders(w http.ResponseWriter, protoHeaders map[string]*pb.HeaderValues) {
	for k, hv := range protoHeaders {
		for _, v := range hv.Values {
			w.Header().Add(k, v)
		}
	}
}
