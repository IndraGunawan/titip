package titip

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	pb "github.com/indragunawan/titip/proto"
)

type staleFallback struct {
	varInfo *pb.VariantInfo
	body    []byte
	meta    *pb.CacheMetadata
}

type fetchResult struct {
	statusCode int
	headers    http.Header
	body       []byte
	isFallback bool
	fallback   *staleFallback
	panicked   bool
	ttl        time.Duration
}

// DefaultVariantKey is used as the variant key when no Vary headers are specified.
const DefaultVariantKey = "default"

func (t *Titip) serveHTTP(w http.ResponseWriter, r *http.Request, next http.Handler) {
	defer func() {
		if p := recover(); p != nil {
			t.logger.Error("titip: top-level handler panic recovered", "panic", p)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		}
	}()

	// 1. Automatic Invalidation on Mutating Methods (RFC-7234 §4.4)
	if isMutatingMethod(r.Method) {
		t.metrics.RecordRequest(StatusBypass)
		t.handleMutatingRequest(w, r, next)
		return
	}

	// 2. Filter non-cacheable methods (only GET and HEAD are cached)
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		t.metrics.RecordRequest(StatusBypass)
		t.emitCacheStatus(w, "BYPASS", "fwd=bypass")
		next.ServeHTTP(w, r)
		return
	}

	// 3. Client Cache-Control Directives
	if !t.cfg.IgnoreClientCacheControl {
		if cc := r.Header.Get("Cache-Control"); strings.Contains(cc, "no-store") {
			t.metrics.RecordRequest(StatusBypass)
			t.emitCacheStatus(w, "BYPASS", "fwd=bypass")
			next.ServeHTTP(w, r)
			return
		}
	}

	// 4. Generate Primary Key & Lookup Metadata
	primaryKey := GeneratePrimaryKey(r, &t.cfg.KeyConfig)
	storeCtx, storeCancel := context.WithTimeout(r.Context(), t.cfg.StorageTimeout)
	meta, err := t.storage.GetMeta(storeCtx, primaryKey)
	storeCancel()
	if err != nil {
		t.metrics.RecordRequest(StatusError)
		t.logger.Error("titip: storage error fetching metadata, bypassing to origin", "error", err, "key", primaryKey)
		t.emitCacheStatus(w, "BYPASS", "fwd=bypass; detail=storage-fallback")
		next.ServeHTTP(w, r)
		return
	}

	// 5. Cold Cache Miss
	if meta == nil {
		t.fetchOriginAndServe(w, r, next, primaryKey, nil, DefaultVariantKey, false, nil)
		return
	}

	// 6. Soft-Purged Entry: Synchronous refresh with stale fallback on error
	variantKey := GenerateVariantKey(r, meta.VaryHeaderNames)
	if variantKey == "" {
		variantKey = DefaultVariantKey
	}
	if meta.IsSoftPurged {
		varCtx, varCancel := context.WithTimeout(r.Context(), t.cfg.StorageTimeout)
		staleVar, staleBody, _ := t.storage.GetVariant(varCtx, primaryKey, variantKey)
		varCancel()
		var fb *staleFallback
		if staleVar != nil && len(staleBody) > 0 {
			fb = &staleFallback{varInfo: staleVar, body: staleBody, meta: meta}
		}
		t.fetchOriginAndServe(w, r, next, primaryKey, meta, variantKey, true, fb)
		return
	}

	// 7. Match Variant
	varInfo, exists := meta.Variants[variantKey]
	if !exists || varInfo == nil {
		t.fetchOriginAndServe(w, r, next, primaryKey, meta, variantKey, false, nil)
		return
	}

	// 8. Fetch Variant Body Payload
	varCtx, varCancel := context.WithTimeout(r.Context(), t.cfg.StorageTimeout)
	varInfo, compBody, err := t.storage.GetVariant(varCtx, primaryKey, variantKey)
	varCancel()
	if err != nil || varInfo == nil || len(compBody) == 0 {
		t.fetchOriginAndServe(w, r, next, primaryKey, meta, variantKey, false, nil)
		return
	}

	now := time.Now()
	nowNano := now.UnixNano()
	isFresh := nowNano <= meta.ExpiresAtUnixNano

	// 9. Fresh Cache Hit
	if isFresh {
		// Conditional Request Validation (If-None-Match / If-Modified-Since)
		if t.checkConditionalMatch(r, varInfo) {
			t.metrics.RecordRequest(StatusHit)
			t.emitCacheStatus(w, "HIT", "hit")
			w.WriteHeader(http.StatusNotModified)
			return
		}

		// HEAD Request Handling (0 body I/O)
		if r.Method == http.MethodHead {
			t.metrics.RecordRequest(StatusHit)
			t.emitCacheStatus(w, "HIT", fmt.Sprintf("hit; ttl=%d", t.calcTTL(meta.ExpiresAtUnixNano, nowNano)))
			t.copyProtoHeaders(w, varInfo.ResponseHeaders)
			w.WriteHeader(int(varInfo.StatusCode))
			return
		}

		// Serve Fresh Cache Hit
		dstBuf := GetBuffer()
		defer PutBuffer(dstBuf)

		if err := DecompressLZ4(compBody, dstBuf); err != nil {
			t.logger.Error("titip: decompression error, failing open to origin", "error", err)
			t.fetchOriginAndServe(w, r, next, primaryKey, meta, variantKey, false, nil)
			return
		}

		currentAge := (nowNano - meta.CreatedAtUnixNano) / int64(time.Second)
		if currentAge < 0 {
			currentAge = 0
		}
		w.Header().Set("Age", fmt.Sprintf("%d", currentAge))
		t.metrics.RecordRequest(StatusHit)
		t.emitCacheStatus(w, "HIT", fmt.Sprintf("hit; ttl=%d", t.calcTTL(meta.ExpiresAtUnixNano, nowNano)))
		t.copyProtoHeaders(w, varInfo.ResponseHeaders)
		w.WriteHeader(int(varInfo.StatusCode))
		_, _ = w.Write(dstBuf.Bytes())
		return
	}

	// 10. Stale While Revalidate
	if meta.StaleUntilUnixNano > meta.ExpiresAtUnixNano && nowNano <= meta.StaleUntilUnixNano {
		dstBuf := GetBuffer()
		defer PutBuffer(dstBuf)

		if err := DecompressLZ4(compBody, dstBuf); err == nil {
			t.metrics.RecordRequest(StatusStaleHit)
			t.emitCacheStatus(w, "STALE", "hit; stale; detail=swr")
			t.copyProtoHeaders(w, varInfo.ResponseHeaders)
			w.WriteHeader(int(varInfo.StatusCode))
			_, _ = w.Write(dstBuf.Bytes())

			// Background revalidation
			if !t.closed.Load() {
				t.swrWG.Add(1)
				go func() {
					defer t.swrWG.Done()
					t.revalidateOrigin(r, next, primaryKey, meta, variantKey)
				}()
			}
			return
		}
	}

	// 11. Expired Cache Miss (with fallback on 5xx if stale-if-error configured)
	fb := &staleFallback{varInfo: varInfo, body: compBody, meta: meta}
	t.fetchOriginAndServe(w, r, next, primaryKey, meta, variantKey, false, fb)
}

func (t *Titip) fetchOriginAndServe(
	w http.ResponseWriter,
	r *http.Request,
	next http.Handler,
	primaryKey string,
	meta *pb.CacheMetadata,
	variantKey string,
	isSoftPurgeRefresh bool,
	fallback *staleFallback,
) {
	sfKey := primaryKey + "|" + variantKey

	// Singleflight Coalescing
	val, err, _ := t.sf.Do(sfKey, func() (any, error) {
		// Context detachment per AC-1
		detachedCtx := context.WithoutCancel(r.Context())
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
					t.logger.Error("titip: origin handler panic inside singleflight", "panic", p)
				}
			}()
			next.ServeHTTP(rec, r.WithContext(originCtx))
		}()

		// Check if fallback to stale cache should be triggered
		if (panicked || rec.Code >= 500) && fallback != nil {
			return &fetchResult{
				isFallback: true,
				fallback:   fallback,
				statusCode: rec.Code,
				panicked:   panicked,
			}, nil
		}

		respTime := time.Now()
		bodyBytes := bytes.Clone(rec.Body.Bytes())
		headersClone := rec.Header().Clone()

		result := &fetchResult{
			statusCode: rec.Code,
			headers:    headersClone,
			body:       bodyBytes,
		}

		// Calculate freshness and evaluate cacheability
		freshness := CalculateFreshness(rec.Code, headersClone, reqTime, respTime, respTime, t.cacheableStatusCodes)
		result.ttl = freshness.EffectiveTTL

		if freshness.IsCacheable && !t.closed.Load() {
			// Extract tags
			tags := t.extractTags(headersClone)

			// Compress body payload
			compBuf := GetBuffer()
			_ = CompressLZ4(bodyBytes, compBuf)
			compBytes := bytes.Clone(compBuf.Bytes())
			PutBuffer(compBuf)

			// Build / update metadata
			varNames := t.extractVaryHeaderNames(headersClone)
			varKey := variantKey
			if varKey == "" || varKey == DefaultVariantKey {
				varKey = GenerateVariantKey(r, varNames)
			}
			if varKey == "" {
				varKey = DefaultVariantKey
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
				StatusCode:         int32(rec.Code),
				ResponseHeaders:    protoHeadersFromHTTP(headersClone),
				Etag:               headersClone.Get("ETag"),
				RawBodySize:        uint32(len(bodyBytes)),
				CompressedBodySize: uint32(len(compBytes)),
			}
			if lm, err := ParseDate(headersClone.Get("Last-Modified")); err == nil && !lm.IsZero() {
				newVariant.LastModifiedUnixNano = lm.UnixNano()
			}

			// Store in backend
			if storeErr := t.storage.SetVariant(originCtx, primaryKey, newMeta, newVariant, compBytes, freshness.EffectiveTTL); storeErr != nil {
				t.logger.Error("titip: storage error saving variant", "error", storeErr, "key", primaryKey)
			}
		}

		return result, nil
	})

	if err != nil {
		t.logger.Error("titip: singleflight execution error", "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	res, ok := val.(*fetchResult)
	if !ok || res == nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Serve stale fallback if origin returned 5xx/panic
	if res.isFallback && res.fallback != nil {
		dstBuf := GetBuffer()
		defer PutBuffer(dstBuf)

		if err := DecompressLZ4(res.fallback.body, dstBuf); err == nil {
			fwdStatus := res.statusCode
			if fwdStatus == 0 {
				fwdStatus = 500
			}
			t.metrics.RecordRequest(StatusStaleHit)
			t.emitCacheStatus(w, "STALE", fmt.Sprintf("hit; stale; fwd=stale; fwd-status=%d; detail=stale-if-error", fwdStatus))
			t.copyProtoHeaders(w, res.fallback.varInfo.ResponseHeaders)
			w.WriteHeader(int(res.fallback.varInfo.StatusCode))
			_, _ = w.Write(dstBuf.Bytes())
			return
		}
	}

	// Serve fresh response from singleflight origin call
	for k, vv := range res.headers {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}

	if isSoftPurgeRefresh {
		t.metrics.RecordRequest(StatusMiss)
		t.emitCacheStatus(w, "MISS", fmt.Sprintf("fwd=uri-miss; fwd-status=%d; stored; detail=soft-refreshed", res.statusCode))
	} else if res.ttl > 0 {
		t.metrics.RecordRequest(StatusMiss)
		t.emitCacheStatus(w, "MISS", fmt.Sprintf("fwd=uri-miss; fwd-status=%d; stored; ttl=%d", res.statusCode, int(res.ttl.Seconds())))
	} else {
		t.metrics.RecordRequest(StatusBypass)
		t.emitCacheStatus(w, "BYPASS", fmt.Sprintf("fwd=bypass; fwd-status=%d", res.statusCode))
	}

	w.WriteHeader(res.statusCode)
	if r.Method != http.MethodHead {
		_, _ = w.Write(res.body)
	}
}

func (t *Titip) revalidateOrigin(r *http.Request, next http.Handler, primaryKey string, meta *pb.CacheMetadata, variantKey string) {
	defer func() {
		if p := recover(); p != nil {
			t.logger.Error("titip: background revalidation panic", "panic", p)
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
	if freshness.IsCacheable {
		compBuf := GetBuffer()
		_ = CompressLZ4(bodyBytes, compBuf)
		compBytes := bytes.Clone(compBuf.Bytes())
		PutBuffer(compBuf)

		newMeta := &pb.CacheMetadata{
			PrimaryKey:        primaryKey,
			VaryHeaderNames:   meta.VaryHeaderNames,
			CreatedAtUnixNano: respTime.UnixNano(),
			ExpiresAtUnixNano: respTime.Add(freshness.EffectiveTTL).UnixNano(),
			Tags:              t.extractTags(headers),
			IsSoftPurged:      false,
		}
		if freshness.StaleWhileRevalidateTTL > 0 {
			newMeta.StaleUntilUnixNano = newMeta.ExpiresAtUnixNano + int64(freshness.StaleWhileRevalidateTTL)
		}

		newVariant := &pb.VariantInfo{
			VariantKey:         variantKey,
			StatusCode:         int32(rec.Code),
			ResponseHeaders:    protoHeadersFromHTTP(headers),
			Etag:               headers.Get("ETag"),
			RawBodySize:        uint32(len(bodyBytes)),
			CompressedBodySize: uint32(len(compBytes)),
		}

		if err := t.storage.SetVariant(bgCtx, primaryKey, newMeta, newVariant, compBytes, freshness.EffectiveTTL); err == nil {
			t.metrics.RecordRequest(StatusRevalidated)
		}
	}
}

func (t *Titip) handleMutatingRequest(w http.ResponseWriter, r *http.Request, next http.Handler) {
	rec := GetResponseRecorder()
	defer PutResponseRecorder(rec)

	next.ServeHTTP(rec, r)

	// If enabled and successful (200..399), invalidate request URI and target locations
	if t.cfg.AutoInvalidateMutatingMethods && rec.Code >= 200 && rec.Code < 400 {
		pk := GeneratePrimaryKey(r, &t.cfg.KeyConfig)
		_ = t.storage.Delete(r.Context(), pk)

		if loc := rec.Header().Get("Location"); loc != "" {
			_ = t.PurgeURL(r.Context(), loc)
		}
		if cloc := rec.Header().Get("Content-Location"); cloc != "" {
			_ = t.PurgeURL(r.Context(), cloc)
		}
	}

	for k, vv := range rec.Header() {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	t.emitCacheStatus(w, "BYPASS", "fwd=bypass")
	w.WriteHeader(rec.Code)
	_, _ = w.Write(rec.Body.Bytes())
}

func (t *Titip) checkConditionalMatch(r *http.Request, varInfo *pb.VariantInfo) bool {
	if etag := r.Header.Get("If-None-Match"); etag != "" && varInfo.Etag != "" {
		if etag == varInfo.Etag || etag == "*" {
			return true
		}
	}

	if ims := r.Header.Get("If-Modified-Since"); ims != "" && varInfo.LastModifiedUnixNano > 0 {
		if imsDate, err := ParseDate(ims); err == nil && !imsDate.IsZero() {
			lastMod := time.Unix(0, varInfo.LastModifiedUnixNano)
			if !lastMod.After(imsDate) {
				return true
			}
		}
	}
	return false
}

func (t *Titip) emitCacheStatus(w http.ResponseWriter, simpleToken, rfc9211 string) {
	switch t.cfg.CacheStatusMode {
	case CacheStatusRFC9211:
		w.Header().Set("Cache-Status", "titip; "+rfc9211)
	case CacheStatusSimpleToken:
		w.Header().Set("Cache-Status", simpleToken)
	case CacheStatusNone:
		// Do not set header
	}
}

func (t *Titip) extractTags(header http.Header) []string {
	var tags []string
	for _, hName := range t.cfg.TagHeaderNames {
		vals := header.Values(hName)
		for _, v := range vals {
			// Support comma-separated and space-separated tags
			parts := strings.FieldsFunc(v, func(r rune) bool {
				return r == ',' || r == ' ' || r == '\t'
			})
			for _, p := range parts {
				trimmed := strings.TrimSpace(p)
				if trimmed != "" && !slices.Contains(tags, trimmed) {
					tags = append(tags, trimmed)
				}
			}
		}
	}
	return tags
}

func (t *Titip) extractVaryHeaderNames(header http.Header) []string {
	var names []string
	varyVals := header.Values("Vary")
	for _, v := range varyVals {
		parts := strings.Split(v, ",")
		for _, p := range parts {
			trimmed := strings.TrimSpace(p)
			if trimmed != "" && !slices.Contains(names, trimmed) {
				names = append(names, trimmed)
			}
		}
	}
	return names
}

func (t *Titip) copyProtoHeaders(w http.ResponseWriter, headers map[string]*pb.HeaderValues) {
	for k, v := range headers {
		for _, val := range v.Values {
			w.Header().Add(k, val)
		}
	}
}

func protoHeadersFromHTTP(header http.Header) map[string]*pb.HeaderValues {
	m := make(map[string]*pb.HeaderValues, len(header))
	for k, vv := range header {
		m[k] = &pb.HeaderValues{Values: vv}
	}
	return m
}

func (t *Titip) calcTTL(expiresAtNano, nowNano int64) int64 {
	ttl := (expiresAtNano - nowNano) / int64(time.Second)
	if ttl < 0 {
		return 0
	}
	return ttl
}

func isMutatingMethod(method string) bool {
	return method == http.MethodPost ||
		method == http.MethodPut ||
		method == http.MethodDelete ||
		method == http.MethodPatch
}
