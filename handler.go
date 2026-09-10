package titip

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/indragunawan/titip/esi"
	pb "github.com/indragunawan/titip/proto"
	"github.com/pquerna/cachecontrol/cacheobject"
)

// defaultVariantKey is used as the variant key when no Vary headers are specified.
const defaultVariantKey = "default"

// stateFn represents a single state transition in the request processing pipeline.
// Returning nil terminates the state machine loop.
type stateFn func(t *Titip, ctx *requestContext) stateFn

// ServeHTTP executes the Titip caching middleware pipeline for a request and forwards to next on cache miss or revalidation.
func (t *Titip) ServeHTTP(w http.ResponseWriter, r *http.Request, next http.Handler) {
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
				t.logger.ErrorContext(ctx.r.Context(), "handler panic recovered", "panic", p)
			}
			http.Error(ctx.w, "Internal Server Error", http.StatusInternalServerError)
		}
	}()

	// A. WebSocket Handshake Bypass (RFC 6455 / RFC 9110 §7.8)
	if containsToken(ctx.r.Header.Get(headerUpgrade), upgradeWebSocket) {
		t.recordRequest(ctx, statusBypass)
		t.emitCacheStatus(ctx.w, tokenBypass, "fwd=bypass; detail=websocket-upgrade")
		return stateBypassOrigin
	}

	// B. Server-Sent Events (SSE) Bypass
	if strings.Contains(strings.ToLower(ctx.r.Header.Get(headerAccept)), contentTypeEventStream) {
		t.recordRequest(ctx, statusBypass)
		t.emitCacheStatus(ctx.w, tokenBypass, "fwd=bypass; detail=sse-stream")
		return stateBypassOrigin
	}

	// C. Range Byte Request Bypass
	if ctx.r.Header.Get(headerRange) != "" {
		t.recordRequest(ctx, statusBypass)
		t.emitCacheStatus(ctx.w, tokenBypass, "fwd=bypass; detail=range-request")
		return stateBypassOrigin
	}

	// D. Mutating Methods (POST, PUT, DELETE, PATCH)
	if isMutatingMethod(ctx.r.Method) {
		t.recordRequest(ctx, statusBypass)
		t.emitCacheStatus(ctx.w, tokenBypass, "fwd=method")
		t.handleMutatingRequest(ctx.w, ctx.r, ctx.next)
		return nil
	}

	// E. Non-Cacheable Methods (only GET and HEAD are cached)
	if ctx.r.Method != http.MethodGet && ctx.r.Method != http.MethodHead {
		t.recordRequest(ctx, statusBypass)
		t.emitCacheStatus(ctx.w, tokenBypass, "fwd=method")
		return stateBypassOrigin
	}

	// F. Client Cache-Control / Pragma (RFC 9111 §5.2.1 & §5.4)
	if t.config.respectClientCacheControl {
		ccValues := ctx.r.Header.Values(headerCacheControl)
		cc := strings.Join(ccValues, ", ")
		pragma := ctx.r.Header.Get(headerPragma)

		if cc != "" {
			if reqCC, err := cacheobject.ParseRequestCacheControl(cc); err == nil {
				ctx.reqCC = reqCC
				// RFC 9111 §5.2.1.4 / §5.2.1.5: no-store and no-cache
				if reqCC.NoStore || reqCC.NoCache {
					t.recordRequest(ctx, statusBypass)
					t.emitCacheStatus(ctx.w, tokenBypass, "fwd=request; detail=no-store")
					return stateBypassOrigin
				}
				// RFC 9111 §5.2.1.1: max-age=0 forces revalidation / origin bypass
				if reqCC.MaxAge == 0 {
					t.recordRequest(ctx, statusBypass)
					t.emitCacheStatus(ctx.w, tokenBypass, "fwd=request; detail=max-age-0")
					return stateBypassOrigin
				}
			}
		} else if strings.EqualFold(strings.TrimSpace(pragma), "no-cache") {
			// RFC 9111 §5.4: Pragma: no-cache acts as Cache-Control: no-cache
			t.recordRequest(ctx, statusBypass)
			t.emitCacheStatus(ctx.w, tokenBypass, "fwd=request; detail=pragma-no-cache")
			return stateBypassOrigin
		}
	}

	return stateLookupMetadata
}

// 2. stateLookupMetadata: Generates Primary Key and looks up Stage 1 metadata in Redis
func stateLookupMetadata(t *Titip, ctx *requestContext) stateFn {
	ctx.primaryKey = generatePrimaryKey(ctx.r, &t.config.cacheKey)

	storeCtx, storeCancel := context.WithTimeout(context.WithoutCancel(ctx.r.Context()), t.config.storageTimeout)
	meta, isSoftPurged, err := t.storage.GetMeta(storeCtx, ctx.primaryKey)
	storeCancel()

	if err != nil {
		t.recordRequest(ctx, statusError)
		if t.logger.Enabled(ctx.r.Context(), slog.LevelError) {
			t.logger.ErrorContext(ctx.r.Context(), "storage error fetching metadata, bypassing to origin", "error", err, "key", ctx.primaryKey)
		}
		t.emitCacheStatus(ctx.w, tokenBypass, "fwd=bypass; detail=storage-fallback")
		return stateBypassOrigin
	}

	ctx.meta = meta
	ctx.isSoftPurged = isSoftPurged
	if meta == nil {
		// RFC 9111 §5.2.1.7: only-if-cached requires responding with 504 Gateway Timeout on miss
		if ctx.reqCC != nil && ctx.reqCC.OnlyIfCached {
			t.recordRequest(ctx, statusMiss)
			t.emitCacheStatus(ctx.w, tokenMiss, "miss; detail=only-if-cached")
			http.Error(ctx.w, "Gateway Timeout", http.StatusGatewayTimeout)
			return nil
		}
		// URL Miss -> fetch origin directly without singleflight
		ctx.isVaryMiss = false
		return stateFetchOriginMiss
	}

	return stateMatchVariant
}

// 3. stateMatchVariant: Evaluates Vary headers and matches variant in metadata
func stateMatchVariant(t *Titip, ctx *requestContext) stateFn {
	ctx.variantKey = generateVariantKey(ctx.r, ctx.meta.VaryHeaderNames)
	if ctx.variantKey == "" {
		ctx.variantKey = defaultVariantKey
	}

	varInfo, exists := ctx.meta.Variants[ctx.variantKey]
	if !exists || varInfo == nil {
		// RFC 9111 §5.2.1.7: only-if-cached requires responding with 504 Gateway Timeout on miss
		if ctx.reqCC != nil && ctx.reqCC.OnlyIfCached {
			t.recordRequest(ctx, statusMiss)
			t.emitCacheStatus(ctx.w, tokenMiss, "miss; detail=only-if-cached")
			http.Error(ctx.w, "Gateway Timeout", http.StatusGatewayTimeout)
			return nil
		}
		// Variant Miss -> fetch new variant directly without singleflight
		ctx.isVaryMiss = true
		return stateFetchOriginMiss
	}
	ctx.varInfo = varInfo

	return stateEvaluateFreshness
}

// 4. stateEvaluateFreshness: Computes RFC-7234 freshness, handles downstream 304, SWR, and stale states
func stateEvaluateFreshness(t *Titip, ctx *requestContext) stateFn {
	ctx.nowNano = time.Now().UnixNano()

	// If entry is soft-purged, refresh synchronously via stale revalidation
	if ctx.isSoftPurged {
		return stateFetchOriginRevalidate
	}

	isFresh := ctx.nowNano <= ctx.meta.ExpiresAtUnixNano

	// Fresh Cache Hit & Downstream Precondition Evaluation (RFC 9110 §13.2.2 & RFC 9111 §4.3.2)
	// Preconditions are ONLY evaluated if the cached representation is strictly fresh.
	if isFresh {
		hasESI := t.esiProcessor != nil && len(ctx.varInfo.EsiFragments) > 0
		if !hasESI || t.esiProcessor.ShouldPreserveETag() {
			status, proceed := t.evaluatePreconditions(ctx.r, ctx.varInfo)
			if !proceed {
				if status == http.StatusNotModified {
					return stateServe304
				}
				if status == http.StatusPreconditionFailed {
					return stateServe412
				}
			}
		}
		return stateServeCachedHit
	}

	// Stale-While-Revalidate Window
	if ctx.meta.StaleUntilUnixNano > ctx.meta.ExpiresAtUnixNano && ctx.nowNano <= ctx.meta.StaleUntilUnixNano {
		return stateServeSWR
	}

	// If client requested only-if-cached and entry is expired, return 504 per RFC 9111 §5.2.1.7
	if t.config.respectClientCacheControl && strings.Contains(strings.Join(ctx.r.Header.Values(headerCacheControl), ", "), "only-if-cached") {
		t.recordRequest(ctx, statusMiss)
		t.emitCacheStatus(ctx.w, tokenMiss, "miss; detail=only-if-cached-expired")
		http.Error(ctx.w, "Gateway Timeout", http.StatusGatewayTimeout)
		return nil
	}

	// Expired -> Synchronous Singleflight Revalidation
	return stateFetchOriginRevalidate
}

// 5. stateServe304: Writes HTTP 304 Not Modified directly with 0 Redis Body I/O
func stateServe304(t *Titip, ctx *requestContext) stateFn {
	t.recordRequest(ctx, statusHit)
	t.emitCacheStatus(ctx.w, tokenHit, "hit")
	t.copyProtoHeaders(ctx.w, ctx.varInfo.ResponseHeaders)
	t.adjustESIHeaders(ctx.w, ctx.varInfo)
	ctx.w.Header().Set(headerAge, calcAgeString(ctx.meta, ctx.nowNano))
	ctx.w.WriteHeader(http.StatusNotModified)
	return nil
}

// stateServe412 writes HTTP 412 Precondition Failed when an If-Match / If-Unmodified-Since precondition fails.
func stateServe412(t *Titip, ctx *requestContext) stateFn {
	t.recordRequest(ctx, statusBypass)
	t.emitCacheStatus(ctx.w, tokenBypass, "fwd=bypass; detail=precondition-failed")
	http.Error(ctx.w, "Precondition Failed", http.StatusPreconditionFailed)
	return nil
}

// 6. stateServeCachedHit: Fetches compressed body from Redis, decompresses and writes response
func stateServeCachedHit(t *Titip, ctx *requestContext) stateFn {
	// HEAD Request Handling (0 body I/O)
	if ctx.r.Method == http.MethodHead {
		t.recordRequest(ctx, statusHit)
		ttlStr := strconv.FormatInt(int64(t.remainingTTL(ctx.meta.ExpiresAtUnixNano, ctx.nowNano)/time.Second), 10)
		t.emitCacheStatus(ctx.w, tokenHit, "hit; ttl="+ttlStr)
		t.copyProtoHeaders(ctx.w, ctx.varInfo.ResponseHeaders)
		t.adjustESIHeaders(ctx.w, ctx.varInfo)
		ctx.w.WriteHeader(int(ctx.varInfo.StatusCode))
		return nil
	}

	varInfo, dstBuf, ok := t.loadDecompressed(ctx)
	if !ok {
		if dstBuf != nil {
			putBuffer(dstBuf)
		}
		return stateFetchOriginMiss
	}
	defer putBuffer(dstBuf)

	// RFC 9111 §5.1 / §4.2.3: current_age = corrected_initial_age + resident_time
	ttlStr := strconv.FormatInt(int64(t.remainingTTL(ctx.meta.ExpiresAtUnixNano, ctx.nowNano)/time.Second), 10)

	if t.esiProcessor != nil && len(varInfo.EsiFragments) > 0 {
		t.recordRequest(ctx, statusHit)
		protoHeaders := protoHeadersToHTTP(varInfo.ResponseHeaders)
		protoHeaders.Set(headerAge, calcAgeString(ctx.meta, ctx.nowNano))
		t.processESI(ctx, dstBuf.Bytes(), varInfo.EsiFragments, int(varInfo.StatusCode), protoHeaders, tokenHit, "hit; ttl="+ttlStr)
		return nil
	}

	t.recordRequest(ctx, statusHit)
	t.copyProtoHeaders(ctx.w, varInfo.ResponseHeaders)
	ctx.w.Header().Set(headerAge, calcAgeString(ctx.meta, ctx.nowNano))
	t.emitCacheStatus(ctx.w, tokenHit, "hit; ttl="+ttlStr)
	ctx.w.WriteHeader(int(varInfo.StatusCode))
	_, _ = ctx.w.Write(dstBuf.Bytes())
	return nil
}

// 7. stateServeSWR: Serves stale cached variant and triggers background revalidation
func stateServeSWR(t *Titip, ctx *requestContext) stateFn {
	varInfo, dstBuf, ok := t.loadDecompressed(ctx)
	if !ok {
		if dstBuf != nil {
			putBuffer(dstBuf)
		}
		return stateFetchOriginMiss
	}
	defer putBuffer(dstBuf)

	if t.esiProcessor != nil && len(varInfo.EsiFragments) > 0 {
		t.recordRequest(ctx, statusStaleHit)
		protoHeaders := protoHeadersToHTTP(varInfo.ResponseHeaders)
		protoHeaders.Set(headerAge, calcAgeString(ctx.meta, ctx.nowNano))
		t.processESI(ctx, dstBuf.Bytes(), varInfo.EsiFragments, int(varInfo.StatusCode), protoHeaders, tokenUpdating, "hit; stale; detail=swr")
		t.spawnSWR(ctx)
		return nil
	}

	t.recordRequest(ctx, statusStaleHit)
	t.copyProtoHeaders(ctx.w, varInfo.ResponseHeaders)
	ctx.w.Header().Set(headerAge, calcAgeString(ctx.meta, ctx.nowNano))
	t.emitCacheStatus(ctx.w, tokenUpdating, "hit; stale; detail=swr")
	ctx.w.WriteHeader(int(varInfo.StatusCode))
	if ctx.r.Method != http.MethodHead {
		_, _ = ctx.w.Write(dstBuf.Bytes())
	}

	t.spawnSWR(ctx)
	return nil
}

// 8. stateFetchOriginMiss: Direct Origin Fetch (NO singleflight) to eliminate Set-Cookie leaks on cache misses
func stateFetchOriginMiss(t *Titip, ctx *requestContext) stateFn {
	originCtx := ctx.r.Context()

	rec := getResponseRecorder()
	defer putResponseRecorder(rec)

	originReq := ctx.r
	hasConditionalHeaders := ctx.r.Header.Get(headerIfNoneMatch) != "" || ctx.r.Header.Get(headerIfModifiedSince) != ""
	needsClone := (ctx.r.Method == http.MethodHead && t.config.convertHeadToGet) || hasConditionalHeaders || t.esiProcessor != nil
	if needsClone {
		originReq = ctx.r.Clone(originCtx)
		if ctx.r.Method == http.MethodHead && t.config.convertHeadToGet {
			originReq.Method = http.MethodGet
		}
		// Strip conditional validator headers so origin returns full representation body to warm cache
		if hasConditionalHeaders {
			originReq.Header.Del(headerIfNoneMatch)
			originReq.Header.Del(headerIfModifiedSince)
		}
		if t.esiProcessor != nil {
			t.esiProcessor.AddSurrogateCapability(originReq.Header, "titip")
		}
	}

	reqTime := time.Now()
	var panicked bool
	func() {
		defer func() {
			if p := recover(); p != nil {
				panicked = true
				if t.logger.Enabled(originCtx, slog.LevelError) {
					t.logger.ErrorContext(originCtx, "origin handler panic", "panic", p)
				}
			}
		}()
		ctx.next.ServeHTTP(rec, originReq)
	}()
	respTime := time.Now()

	// Origin Panic Recovery (Fail-Open)
	if panicked {
		t.recordRequest(ctx, statusError)
		t.emitCacheStatus(ctx.w, tokenBypass, "fwd=bypass; detail=origin-panic")
		http.Error(ctx.w, "Internal Server Error", http.StatusInternalServerError)
		return nil
	}

	headersClone := rec.Header().Clone()
	bodyBytes := bytes.Clone(rec.Body.Bytes())

	// Strip Set-Cookie from cached metadata (RFC 9111 §3.2 / RFC 7234 §3)
	// If response contains Set-Cookie, it is treated as dynamic / uncacheable
	if len(headersClone.Values(headerSetCookie)) > 0 {
		t.recordRequest(ctx, statusBypass)
		for k, vv := range headersClone {
			for _, v := range vv {
				ctx.w.Header().Add(k, v)
			}
		}
		detail := ""
		if t.config.cacheStatusMode == CacheStatusRFC9211 {
			detail = "fwd=bypass; fwd-status=" + strconv.Itoa(rec.Code) + "; detail=set-cookie"
		}
		t.emitCacheStatus(ctx.w, tokenDynamic, detail)
		ctx.w.WriteHeader(rec.Code)
		if ctx.r.Method != http.MethodHead {
			_, _ = ctx.w.Write(bodyBytes)
		}
		return nil
	}

	// Stream / SSE Response Detection
	if strings.Contains(strings.ToLower(headersClone.Get(headerContentType)), contentTypeEventStream) {
		t.recordRequest(ctx, statusBypass)
		t.emitCacheStatus(ctx.w, tokenDynamic, "fwd=bypass; detail=sse-response")
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

	freshness := calculateFreshness(rec.Code, ctx.r.Header, headersClone, reqTime, respTime, respTime)

	// Cache if eligible and not closed (skip saving 0-byte variant if HEAD and ConvertHeadToGet is disabled)
	shouldCache := freshness.IsCacheable && !t.closed.Load()
	if ctx.r.Method == http.MethodHead && !t.config.convertHeadToGet {
		shouldCache = false
	}
	if shouldCache {
		t.saveVariantToStorage(originCtx, ctx.primaryKey, ctx.variantKey, rec.Code, ctx.r, headersClone, bodyBytes, freshness, respTime)
	}

	// ESI Processing on Cold Miss
	if t.canProcessESI(headersClone) {
		if fragments := esi.Scan(bodyBytes); len(fragments) > 0 {
			var statusToken, rfc9211Detail string
			missReason := "fwd=uri-miss"
			if ctx.isVaryMiss {
				missReason = "fwd=vary-miss"
			}
			if shouldCache && freshness.EffectiveTTL > 0 {
				t.recordRequest(ctx, statusMiss)
				statusToken = tokenMiss
				if t.config.cacheStatusMode == CacheStatusRFC9211 {
					rfc9211Detail = missReason + "; fwd-status=" + strconv.Itoa(rec.Code) + "; stored; ttl=" + strconv.Itoa(int(freshness.EffectiveTTL.Seconds()))
				}
			} else {
				t.recordRequest(ctx, statusBypass)
				statusToken = tokenDynamic
				if t.config.cacheStatusMode == CacheStatusRFC9211 {
					rfc9211Detail = "fwd=bypass; fwd-status=" + strconv.Itoa(rec.Code)
				}
			}
			t.processESI(ctx, bodyBytes, fragments, rec.Code, headersClone, statusToken, rfc9211Detail)
			return nil
		}
	}

	// Serve Response to Client
	for k, vv := range headersClone {
		for _, v := range vv {
			ctx.w.Header().Add(k, v)
		}
	}

	missReason := "fwd=uri-miss"
	if ctx.isVaryMiss {
		missReason = "fwd=vary-miss"
	}
	if shouldCache && freshness.EffectiveTTL > 0 {
		t.recordRequest(ctx, statusMiss)
		detail := ""
		if t.config.cacheStatusMode == CacheStatusRFC9211 {
			detail = missReason + "; fwd-status=" + strconv.Itoa(rec.Code) + "; stored; ttl=" + strconv.Itoa(int(freshness.EffectiveTTL.Seconds()))
		}
		t.emitCacheStatus(ctx.w, tokenMiss, detail)
	} else {
		t.recordRequest(ctx, statusBypass)
		detail := ""
		if t.config.cacheStatusMode == CacheStatusRFC9211 {
			detail = "fwd=bypass; fwd-status=" + strconv.Itoa(rec.Code)
		}
		t.emitCacheStatus(ctx.w, tokenDynamic, detail)
	}

	// Evaluate client preconditions (cache warming: downstream gets 304 if validator matches)
	if hasConditionalHeaders && rec.Code >= 200 && rec.Code < 300 {
		var lmUnix int64
		if lm, err := parseDate(headersClone.Get(headerLastModified)); err == nil && !lm.IsZero() {
			lmUnix = lm.UnixNano()
		}
		status, proceed := t.evaluatePreconditionsHeaders(ctx.r, headersClone.Get(headerETag), lmUnix)
		if !proceed && status == http.StatusNotModified {
			ctx.w.WriteHeader(http.StatusNotModified)
			return nil
		}
	}

	ctx.w.WriteHeader(rec.Code)
	if ctx.r.Method != http.MethodHead {
		_, _ = ctx.w.Write(bodyBytes)
	}

	return nil
}

// 9. stateFetchOriginRevalidate: Singleflight Coalesced Revalidation (with Upstream 304 support & stale fallback)
func stateFetchOriginRevalidate(t *Titip, ctx *requestContext) stateFn {
	sfKey := ctx.primaryKey + ":" + ctx.variantKey

	// Fetch existing stale variant from Redis for 304 revalidation or 5xx fallback
	var staleVar *pb.VariantInfo
	var staleCompBody []byte
	if ctx.varInfo != nil {
		staleVar = ctx.varInfo
		varCtx, varCancel := context.WithTimeout(context.WithoutCancel(ctx.r.Context()), t.config.storageTimeout)
		_, staleCompBody, _ = t.storage.GetVariant(varCtx, ctx.primaryKey, ctx.variantKey)
		varCancel()
	}

	val, err, shared := t.singleflight.Do(sfKey, func() (any, error) {
		// Context Detachment: wrap client context so cancellations don't abort in-flight origin fetch
		// for other concurrent singleflight callers waiting on this result.
		originCtx := context.WithoutCancel(ctx.r.Context())

		rec := getResponseRecorder()
		defer putResponseRecorder(rec)

		// Double-checked freshness check:
		// If another concurrent singleflight already refreshed this entry in storage while we were delayed,
		// load the freshly saved entry from storage directly rather than querying the origin again.
		metaCtx, metaCancel := context.WithTimeout(originCtx, t.config.storageTimeout)
		latestMeta, latestSoftPurged, errMeta := t.storage.GetMeta(metaCtx, ctx.primaryKey)
		metaCancel()
		if errMeta == nil && latestMeta != nil && !latestSoftPurged && latestMeta.ExpiresAtUnixNano > time.Now().UnixNano() {
			varCtx, varCancel := context.WithTimeout(originCtx, t.config.storageTimeout)
			latestVar, compBody, errVar := t.storage.GetVariant(varCtx, ctx.primaryKey, ctx.variantKey)
			varCancel()
			if errVar == nil && latestVar != nil && len(compBody) > 0 {
				return &fetchResult{
					statusCode: int(latestVar.StatusCode),
					isFallback: true,
					fallback:   &staleFallback{varInfo: latestVar, body: compBody, meta: latestMeta},
				}, nil
			}
		}

		// Attach conditional headers for Upstream 304 Revalidation
		revalReq := ctx.r.Clone(originCtx)
		if ctx.r.Method == http.MethodHead && t.config.convertHeadToGet {
			revalReq.Method = http.MethodGet
		}
		if staleVar != nil {
			if staleVar.Etag != "" {
				revalReq.Header.Set(headerIfNoneMatch, staleVar.Etag)
			}
			if staleVar.LastModifiedUnixNano > 0 {
				revalReq.Header.Set(headerIfModifiedSince, time.Unix(0, staleVar.LastModifiedUnixNano).UTC().Format(http.TimeFormat))
			}
		}
		if t.esiProcessor != nil {
			t.esiProcessor.AddSurrogateCapability(revalReq.Header, "titip")
		}

		reqTime := time.Now()
		var panicked bool
		func() {
			defer func() {
				if p := recover(); p != nil {
					panicked = true
					if t.logger.Enabled(originCtx, slog.LevelError) {
						t.logger.ErrorContext(originCtx, "singleflight origin panic", "panic", p)
					}
				}
			}()
			ctx.next.ServeHTTP(rec, revalReq)
		}()

		if panicked {
			if staleVar != nil {
				return &fetchResult{isFallback: true, fallback: &staleFallback{varInfo: staleVar, body: staleCompBody, meta: ctx.meta}}, nil
			}
			return &fetchResult{statusCode: http.StatusInternalServerError}, nil
		}

		respTime := time.Now()

		headersClone := rec.Header().Clone()
		bodyBytes := bytes.Clone(rec.Body.Bytes())
		// Upstream 304 Not Modified Handling
		if rec.Code == http.StatusNotModified && staleVar != nil && len(staleCompBody) > 0 {
			// Merge upstream 304 headers over stored variant headers per RFC 9111 §4.3.4
			mergedHeaders := make(http.Header)
			for k, hv := range staleVar.ResponseHeaders {
				for _, v := range hv.Values {
					mergedHeaders.Add(k, v)
				}
			}
			for k, vv := range headersClone {
				canon := http.CanonicalHeaderKey(k)
				if !isHopByHopHeader(canon) && canon != "Set-Cookie" {
					mergedHeaders[canon] = vv
					staleVar.ResponseHeaders[k] = &pb.HeaderValues{Values: vv}
				}
			}
			if etag := headersClone.Get(headerETag); etag != "" {
				staleVar.Etag = etag
				mergedHeaders.Set(headerETag, etag)
			}
			if lm := headersClone.Get(headerLastModified); lm != "" {
				if t, err := parseDate(lm); err == nil && !t.IsZero() {
					staleVar.LastModifiedUnixNano = t.UnixNano()
					mergedHeaders.Set(headerLastModified, lm)
				}
			}

			freshness := calculateFreshness(200, ctx.r.Header, mergedHeaders, reqTime, respTime, respTime)
			if freshness.EffectiveTTL > 0 && !t.closed.Load() {
				// Refresh Redis TTL without rewriting body
				ctx.meta.ExpiresAtUnixNano = respTime.Add(freshness.EffectiveTTL).UnixNano()
				if freshness.StaleWhileRevalidateTTL > 0 {
					ctx.meta.StaleUntilUnixNano = ctx.meta.ExpiresAtUnixNano + int64(freshness.StaleWhileRevalidateTTL)
				} else {
					ctx.meta.StaleUntilUnixNano = ctx.meta.ExpiresAtUnixNano
				}
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
			}, nil
		}

		// Fallback to stale if origin returns 5xx
		if rec.Code >= 500 && staleVar != nil && len(staleCompBody) > 0 {
			return &fetchResult{
				isFallback: true,
				fallback:   &staleFallback{varInfo: staleVar, body: staleCompBody, meta: ctx.meta},
				statusCode: rec.Code,
			}, nil
		}

		freshness := calculateFreshness(rec.Code, ctx.r.Header, headersClone, reqTime, respTime, respTime)
		shouldCache := freshness.IsCacheable && !t.closed.Load()
		if ctx.r.Method == http.MethodHead && !t.config.convertHeadToGet {
			shouldCache = false
		}
		if shouldCache {
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
			t.logger.ErrorContext(ctx.r.Context(), "singleflight execution error", "error", err)
		}
		http.Error(ctx.w, "Internal Server Error", http.StatusInternalServerError)
		return nil
	}

	res, ok := val.(*fetchResult)
	if !ok || res == nil {
		http.Error(ctx.w, "Internal Server Error", http.StatusInternalServerError)
		return nil
	}

	collapsedToken := ""
	if shared {
		collapsedToken = "; collapsed"
	}

	// Serve stale fallback or origin 304 refreshed cached body
	if (res.isFallback || res.is304Origin) && res.fallback != nil {
		// If origin confirmed 304 and downstream client requested conditional revalidation matching refreshed entry
		if res.is304Origin {
			hasESI := t.esiProcessor != nil && len(res.fallback.varInfo.EsiFragments) > 0
			if !hasESI || t.esiProcessor.ShouldPreserveETag() {
				status, proceed := t.evaluatePreconditions(ctx.r, res.fallback.varInfo)
				if !proceed && status == http.StatusNotModified {
					t.recordRequest(ctx, statusRevalidated)
					detail := ""
					if t.config.cacheStatusMode == CacheStatusRFC9211 {
						detail = "fwd=stale; fwd-status=304" + collapsedToken + "; stored; detail=304-refreshed"
					}
					t.emitCacheStatus(ctx.w, tokenRevalidated, detail)
					t.copyProtoHeaders(ctx.w, res.fallback.varInfo.ResponseHeaders)
					t.adjustESIHeaders(ctx.w, res.fallback.varInfo)
					if res.fallback.meta != nil {
						ctx.w.Header().Set(headerAge, calcAgeString(res.fallback.meta, time.Now().UnixNano()))
					}
					ctx.w.WriteHeader(http.StatusNotModified)
					return nil
				}
			}
		}

		dstBuf := getBuffer()
		defer putBuffer(dstBuf)
		if res.fallback.varInfo != nil && res.fallback.varInfo.RawBodySize > 0 {
			dstBuf.Grow(int(res.fallback.varInfo.RawBodySize))
		}

		if err := decompressLZ4(res.fallback.body, dstBuf); err == nil {
			if t.esiProcessor != nil && len(res.fallback.varInfo.EsiFragments) > 0 {
				protoHeaders := protoHeadersToHTTP(res.fallback.varInfo.ResponseHeaders)
				if res.fallback.meta != nil {
					protoHeaders.Set(headerAge, calcAgeString(res.fallback.meta, time.Now().UnixNano()))
				}
				if res.is304Origin {
					t.recordRequest(ctx, statusRevalidated)
					detail := ""
					if t.config.cacheStatusMode == CacheStatusRFC9211 {
						detail = "fwd=stale; fwd-status=304" + collapsedToken + "; stored; detail=304-refreshed"
					}
					t.processESI(ctx, dstBuf.Bytes(), res.fallback.varInfo.EsiFragments, int(res.fallback.varInfo.StatusCode), protoHeaders, tokenRevalidated, detail)
				} else {
					t.recordRequest(ctx, statusStaleHit)
					t.processESI(ctx, dstBuf.Bytes(), res.fallback.varInfo.EsiFragments, int(res.fallback.varInfo.StatusCode), protoHeaders, tokenStale, "hit; stale; detail=stale-if-error")
				}
				return nil
			}

			t.copyProtoHeaders(ctx.w, res.fallback.varInfo.ResponseHeaders)
			if res.fallback.meta != nil {
				ctx.w.Header().Set(headerAge, calcAgeString(res.fallback.meta, time.Now().UnixNano()))
			}
			if res.is304Origin {
				t.recordRequest(ctx, statusRevalidated)
				detail := ""
				if t.config.cacheStatusMode == CacheStatusRFC9211 {
					detail = "fwd=stale; fwd-status=304" + collapsedToken + "; stored; detail=304-refreshed"
				}
				t.emitCacheStatus(ctx.w, tokenRevalidated, detail)
			} else {
				fwdStatus := res.statusCode
				if fwdStatus == 0 {
					fwdStatus = 500
				}
				t.recordRequest(ctx, statusStaleHit)
				detail := ""
				if t.config.cacheStatusMode == CacheStatusRFC9211 {
					detail = "hit; stale; fwd=stale; fwd-status=" + strconv.Itoa(fwdStatus) + "; detail=stale-if-error"
				}
				t.emitCacheStatus(ctx.w, tokenStale, detail)
			}
			ctx.w.WriteHeader(int(res.fallback.varInfo.StatusCode))
			if ctx.r.Method != http.MethodHead {
				_, _ = ctx.w.Write(dstBuf.Bytes())
			}
			return nil
		}
	}

	// ESI Processing on fresh revalidation
	if t.canProcessESI(res.headers) {
		if fragments := esi.Scan(res.body); len(fragments) > 0 {
			t.recordRequest(ctx, statusMiss)
			detail := ""
			if t.config.cacheStatusMode == CacheStatusRFC9211 {
				detail = "fwd=stale; fwd-status=" + strconv.Itoa(res.statusCode) + collapsedToken + "; stored; detail=soft-refreshed"
			}
			t.processESI(ctx, res.body, fragments, res.statusCode, res.headers, tokenExpired, detail)
			return nil
		}
	}

	// Serve fresh origin response
	if res.statusCode != 0 {
		for k, vv := range res.headers {
			if shared && http.CanonicalHeaderKey(k) == "Set-Cookie" {
				continue
			}
			for _, v := range vv {
				ctx.w.Header().Add(k, v)
			}
		}

		t.recordRequest(ctx, statusMiss)
		detail := ""
		if t.config.cacheStatusMode == CacheStatusRFC9211 {
			detail = "fwd=stale; fwd-status=" + strconv.Itoa(res.statusCode) + collapsedToken + "; stored; detail=soft-refreshed"
		}
		t.emitCacheStatus(ctx.w, tokenExpired, detail)
		ctx.w.WriteHeader(res.statusCode)
		if ctx.r.Method != http.MethodHead {
			_, _ = ctx.w.Write(res.body)
		}
	}

	return nil
}

// 10. stateBypassOrigin: Bypasses Titip cache completely and forwards request directly to next handler
func stateBypassOrigin(t *Titip, ctx *requestContext) stateFn {
	ctx.next.ServeHTTP(ctx.w, ctx.r)
	return nil
}

// --- Helper Structs ---

type staleFallback struct {
	varInfo *pb.VariantInfo
	body    []byte
	meta    *pb.CacheMetadata
}

type fetchResult struct {
	statusCode  int
	is304Origin bool
	isFallback  bool
	fallback    *staleFallback
	headers     http.Header
	body        []byte
	ttl         time.Duration
}

func (t *Titip) saveVariantToStorage(
	ctx context.Context,
	primaryKey, variantKey string,
	statusCode int,
	r *http.Request,
	headers http.Header,
	bodyBytes []byte,
	freshness freshnessInfo,
	respTime time.Time,
) {
	tags := extractTags(headers, t.config.tagHeaderName)

	// Check for ESI directives in body
	var fragments []*pb.EsiFragment
	if t.canProcessESI(headers) {
		fragments = esi.Scan(bodyBytes)
	}

	// Compress body payload
	compBuf := getBuffer()
	_ = compressLZ4(bodyBytes, compBuf)
	compBytes := bytes.Clone(compBuf.Bytes())
	putBuffer(compBuf)

	// Build / update metadata
	varNames := extractVaryHeaderNames(headers)
	varKey := variantKey
	if varKey == "" || varKey == defaultVariantKey {
		varKey = generateVariantKey(r, varNames)
	}
	if varKey == "" {
		varKey = defaultVariantKey
	}

	newMeta := &pb.CacheMetadata{
		PrimaryKey:                 primaryKey,
		VaryHeaderNames:            varNames,
		CreatedAtUnixNano:          respTime.UnixNano(),
		ExpiresAtUnixNano:          respTime.Add(freshness.EffectiveTTL).UnixNano(),
		CorrectedInitialAgeSeconds: int64(freshness.CorrectedInitialAge / time.Second),
		Tags:                       tags,
	}
	if freshness.StaleWhileRevalidateTTL > 0 {
		newMeta.StaleUntilUnixNano = newMeta.ExpiresAtUnixNano + int64(freshness.StaleWhileRevalidateTTL)
	}

	newVariant := &pb.VariantInfo{
		VariantKey:      varKey,
		StatusCode:      int32(statusCode),
		ResponseHeaders: protoHeadersFromHTTP(headers),
		Etag:            headers.Get(headerETag),
		RawBodySize:     int64(len(bodyBytes)),
		EsiFragments:    fragments,
	}
	if lm, err := parseDate(headers.Get(headerLastModified)); err == nil && !lm.IsZero() {
		newVariant.LastModifiedUnixNano = lm.UnixNano()
	}

	staleWindow := max(freshness.StaleWhileRevalidateTTL, freshness.StaleIfErrorTTL)
	storageTTL := freshness.EffectiveTTL + staleWindow
	if storageTTL <= 0 {
		storageTTL = freshness.EffectiveTTL
	}

	if storeErr := t.storage.SetVariant(ctx, primaryKey, newMeta, newVariant, compBytes, storageTTL); storeErr != nil {
		if t.logger.Enabled(ctx, slog.LevelError) {
			t.logger.ErrorContext(ctx, "storage error saving variant", "error", storeErr, "key", primaryKey)
		}
	}
}

func (t *Titip) revalidateOriginAsync(r *http.Request, next http.Handler, primaryKey string, variantKey string) {
	defer func() {
		if p := recover(); p != nil {
			if t.logger.Enabled(context.Background(), slog.LevelError) {
				t.logger.ErrorContext(context.Background(), "background revalidation panic", "panic", p)
			}
		}
	}()

	bgCtx := context.WithoutCancel(r.Context())
	if t.config.backgroundFetchTimeout > 0 {
		var cancel context.CancelFunc
		bgCtx, cancel = context.WithTimeout(bgCtx, t.config.backgroundFetchTimeout)
		defer cancel()
	}

	rec := getResponseRecorder()
	defer putResponseRecorder(rec)

	originReq := r
	needsClone := (r.Method == http.MethodHead && t.config.convertHeadToGet) || t.esiProcessor != nil
	if needsClone {
		originReq = r.Clone(bgCtx)
		if r.Method == http.MethodHead && t.config.convertHeadToGet {
			originReq.Method = http.MethodGet
		}
		if t.esiProcessor != nil {
			t.esiProcessor.AddSurrogateCapability(originReq.Header, "titip")
		}
	} else if originReq.Context() != bgCtx {
		originReq = r.WithContext(bgCtx)
	}

	reqTime := time.Now()
	next.ServeHTTP(rec, originReq)
	respTime := time.Now()

	headers := rec.Header().Clone()
	bodyBytes := bytes.Clone(rec.Body.Bytes())

	freshness := calculateFreshness(rec.Code, r.Header, headers, reqTime, respTime, respTime)
	shouldCache := freshness.IsCacheable && !t.closed.Load()
	if r.Method == http.MethodHead && !t.config.convertHeadToGet {
		shouldCache = false
	}
	if shouldCache {
		t.saveVariantToStorage(bgCtx, primaryKey, variantKey, rec.Code, r, headers, bodyBytes, freshness, respTime)
	}
}

func (t *Titip) handleMutatingRequest(w http.ResponseWriter, r *http.Request, next http.Handler) {
	if !t.config.autoInvalidateMutatingMethods {
		next.ServeHTTP(w, r)
		return
	}

	rec := getResponseRecorder()
	defer putResponseRecorder(rec)
	next.ServeHTTP(rec, r)

	for k, vv := range rec.Header() {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(rec.Code)
	if rec.Body.Len() > 0 {
		_, _ = w.Write(rec.Body.Bytes())
	}

	// Invalidate on successful unsafe request (non-error) per RFC 9111 §4.4
	if rec.Code >= 200 && rec.Code < 400 {
		delCtx, delCancel := context.WithTimeout(context.Background(), t.config.storageTimeout)
		defer delCancel()

		reqTarget := r.URL.String()
		if reqTarget == "" || !strings.Contains(reqTarget, "://") {
			scheme := resolveScheme(r)
			reqTarget = scheme + "://" + r.Host + r.URL.RequestURI()
		}
		_, _ = t.Purge(delCtx, reqTarget)

		// RFC 9111 §4.4: Invalidate URI in Location header (same-origin host check)
		if loc := rec.Header().Get(headerLocation); loc != "" {
			if safeLoc := t.sanitizePurgeURI(loc, r); safeLoc != "" {
				_, _ = t.Purge(delCtx, safeLoc)
			}
		}
		// RFC 9111 §4.4: Invalidate URI in Content-Location header (same-origin host check)
		if cloc := rec.Header().Get(headerContentLocation); cloc != "" {
			if safeCLoc := t.sanitizePurgeURI(cloc, r); safeCLoc != "" {
				_, _ = t.Purge(delCtx, safeCLoc)
			}
		}
	}
}

// sanitizePurgeURI ensures that Location/Content-Location purges are scoped to the request host
// and rejects cross-host invalidations per RFC 9111 §4.4.
func (t *Titip) sanitizePurgeURI(targetURI string, r *http.Request) string {
	trimmed := strings.TrimSpace(targetURI)
	if trimmed == "" {
		return ""
	}

	scheme := resolveScheme(r)
	reqHost := normalizeHost(r.Host, scheme)

	if strings.HasPrefix(trimmed, "/") {
		// Relative path: scope to current request host
		return scheme + "://" + r.Host + trimmed
	}

	parsed, err := url.Parse(trimmed)
	if err != nil {
		return ""
	}

	parsedHost := normalizeHost(parsed.Host, parsed.Scheme)
	if parsedHost != "" && !strings.EqualFold(parsedHost, reqHost) {
		// Reject cross-host purge per RFC 9111 §4.4
		return ""
	}

	return trimmed
}

func isMutatingMethod(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch:
		return true
	default:
		return false
	}
}

// evaluatePreconditions evaluates HTTP conditional request headers per RFC 9110 §13.2.2.
// Returns (statusCode, proceed). If proceed is false, caller MUST terminate with statusCode.
func (t *Titip) evaluatePreconditions(r *http.Request, varInfo *pb.VariantInfo) (int, bool) {
	if varInfo == nil {
		return 0, true
	}
	return t.evaluatePreconditionsHeaders(r, varInfo.Etag, varInfo.LastModifiedUnixNano)
}

// evaluatePreconditionsHeaders evaluates HTTP conditional request headers directly from raw header metadata.
func (t *Titip) evaluatePreconditionsHeaders(r *http.Request, etag string, lastModifiedUnixNano int64) (int, bool) {
	// 1. If-Match (Strong comparison per RFC 9110 §13.1.1 & §13.2.2)
	if ifMatch := r.Header.Get(headerIfMatch); ifMatch != "" {
		trimmed := strings.TrimSpace(ifMatch)
		if trimmed != "*" {
			matched := false
			for tag := range strings.SplitSeq(ifMatch, ",") {
				if strongETagMatches(tag, etag) {
					matched = true
					break
				}
			}
			if !matched {
				return http.StatusPreconditionFailed, false
			}
		}
	} else if ifUnmodSince := r.Header.Get(headerIfUnmodifiedSince); ifUnmodSince != "" && lastModifiedUnixNano > 0 {
		// 2. If-Unmodified-Since (RFC 9110 §13.1.4 & §13.2.2: only if If-Match is absent)
		if clientTime, err := parseDate(ifUnmodSince); err == nil && !clientTime.IsZero() {
			cachedSec := time.Unix(0, lastModifiedUnixNano).Truncate(time.Second)
			clientSec := clientTime.Truncate(time.Second)
			if cachedSec.After(clientSec) {
				return http.StatusPreconditionFailed, false
			}
		}
	}

	// 3. If-None-Match (Weak comparison for GET/HEAD per RFC 9110 §13.1.2 & §13.2.2)
	if ifNoneMatch := r.Header.Get(headerIfNoneMatch); ifNoneMatch != "" {
		trimmed := strings.TrimSpace(ifNoneMatch)
		if trimmed == "*" {
			return http.StatusNotModified, false
		}
		if etag != "" {
			for tag := range strings.SplitSeq(ifNoneMatch, ",") {
				if weakETagMatches(tag, etag) {
					return http.StatusNotModified, false
				}
			}
		}
		return 0, true
	} else if ifModSince := r.Header.Get(headerIfModifiedSince); ifModSince != "" && lastModifiedUnixNano > 0 {
		// 4. If-Modified-Since (RFC 9110 §13.1.3 & §13.2.2: only if If-None-Match is absent)
		if clientTime, err := parseDate(ifModSince); err == nil && !clientTime.IsZero() {
			cachedSec := time.Unix(0, lastModifiedUnixNano).Truncate(time.Second)
			clientSec := clientTime.Truncate(time.Second)
			if !cachedSec.After(clientSec) {
				return http.StatusNotModified, false
			}
		}
	}

	return 0, true
}

func (t *Titip) remainingTTL(expiresAtUnixNano, nowNano int64) time.Duration {
	rem := time.Duration(expiresAtUnixNano - nowNano)
	if rem < 0 {
		return 0
	}
	return rem
}

func (t *Titip) emitCacheStatus(w http.ResponseWriter, simpleToken, rfc9211Detail string) {
	switch t.config.cacheStatusMode {
	case CacheStatusRFC9211:
		titipStatus := "titip; " + rfc9211Detail
		if len(w.Header().Values(headerCacheStatus)) > 0 {
			// RFC 9211 §2: Multi-cache chaining - append to existing Cache-Status header
			w.Header().Add(headerCacheStatus, titipStatus)
		} else {
			w.Header().Set(headerCacheStatus, titipStatus)
		}
	case CacheStatusSimpleToken:
		// Simple token replaces upstream header with Titip's definitive local status
		w.Header().Set(headerCacheStatus, simpleToken)
	case CacheStatusNone:
		// Do not emit Cache-Status header
	}
}

// isHopByHopHeader checks if a header is a standard hop-by-hop header per RFC 9110 §7.6.1 & RFC 7230 §6.1.
func isHopByHopHeader(k string) bool {
	switch http.CanonicalHeaderKey(k) {
	case "Connection",
		"Keep-Alive",
		"Proxy-Authenticate",
		"Proxy-Authorization",
		"Proxy-Connection",
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
		if isHopByHopHeader(canon) || canon == "Set-Cookie" {
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

// calcAgeString computes RFC 9111 §5.1 current_age = corrected_initial_age + resident_time.
func calcAgeString(meta *pb.CacheMetadata, nowNano int64) string {
	if meta == nil {
		return "0"
	}
	residentSec := max((nowNano-meta.CreatedAtUnixNano)/int64(time.Second), 0)
	age := meta.CorrectedInitialAgeSeconds + residentSec
	return strconv.FormatInt(age, 10)
}

func protoHeadersToHTTP(protoHeaders map[string]*pb.HeaderValues) http.Header {
	h := make(http.Header, len(protoHeaders))
	for k, hv := range protoHeaders {
		for _, v := range hv.Values {
			h.Add(k, v)
		}
	}
	return h
}

func (t *Titip) recordRequest(ctx *requestContext, status string) {
	if t.metrics == nil {
		return
	}
	var dur time.Duration
	if ctx != nil && ctx.nowNano > 0 {
		dur = time.Duration(time.Now().UnixNano() - ctx.nowNano)
	}
	t.metrics.recordRequest(status, dur)
}

// loadDecompressed fetches and decompresses the cached variant body.
// Returns varInfo, pooled buffer (caller must putBuffer), ok.
func (t *Titip) loadDecompressed(ctx *requestContext) (*pb.VariantInfo, *bytes.Buffer, bool) {
	varCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx.r.Context()), t.config.storageTimeout)
	varInfo, compBody, err := t.storage.GetVariant(varCtx, ctx.primaryKey, ctx.variantKey)
	cancel()
	if err != nil || varInfo == nil || len(compBody) == 0 {
		return nil, nil, false
	}
	buf := getBuffer()
	if varInfo.RawBodySize > 0 {
		buf.Grow(int(varInfo.RawBodySize))
	}
	if err := decompressLZ4(compBody, buf); err != nil {
		putBuffer(buf)
		if t.logger.Enabled(ctx.r.Context(), slog.LevelError) {
			t.logger.ErrorContext(ctx.r.Context(), "decompression error, failing open to origin", "error", err)
		}
		return nil, nil, false
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

func (t *Titip) canProcessESI(h http.Header) bool {
	if t.esiProcessor == nil {
		return false
	}
	return t.esiProcessor.CanProcess(h)
}
