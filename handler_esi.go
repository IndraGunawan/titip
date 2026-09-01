package titip

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/indragunawan/titip/esi"
	pb "github.com/indragunawan/titip/proto"
)

var (
	// errESICircularInclude is returned when an ESI fragment causes a circular include loop.
	errESICircularInclude = errors.New("titip: esi: circular include loop detected")
	// errESIMaxDepthExceeded is returned when max recursion depth is reached.
	errESIMaxDepthExceeded = errors.New("titip: esi: max recursion depth exceeded")
)

type esiContextKey struct{}

type esiExecutionState struct {
	depth       uint32
	maxDepth    uint32
	visitedURLs []string
}

type fragmentResult struct {
	spec       *pb.EsiFragment
	body       []byte
	err        error
	setCookies []string
	duration   time.Duration
	mode       string
}

type fetchTarget struct {
	src       string
	alt       string
	timeoutMs uint32
	maxDepth  uint32
	onError   string
	fallback  []byte
}

// ponytail: shared helpers to avoid duplicating ESI collect/fetch/assemble across processESI and processNestedESI
func collectTargets(fragments []*pb.EsiFragment) map[string]fetchTarget {
	m := make(map[string]fetchTarget, len(fragments))
	for _, frag := range fragments {
		if frag.Src != "" {
			if _, exists := m[frag.Src]; !exists {
				m[frag.Src] = fetchTarget{src: frag.Src, alt: frag.Alt, timeoutMs: frag.TimeoutMs, maxDepth: frag.MaxDepth, onError: frag.OnError, fallback: frag.FallbackBody}
			}
		}
	}
	return m
}

func assembleResults(fragments []*pb.EsiFragment, fetched map[string]*fragmentResult) []*fragmentResult {
	results := make([]*fragmentResult, len(fragments))
	for i, frag := range fragments {
		if frag.Src == "" {
			results[i] = &fragmentResult{spec: frag}
			continue
		}
		if res, ok := fetched[frag.Src]; ok {
			results[i] = &fragmentResult{spec: frag, body: res.body, err: res.err, setCookies: res.setCookies, duration: res.duration, mode: res.mode}
		} else {
			results[i] = &fragmentResult{spec: frag, body: frag.FallbackBody}
		}
	}
	return results
}

func (t *Titip) fetchAllTargets(ctx *requestContext, targets map[string]fetchTarget, state esiExecutionState) (map[string]*fragmentResult, []string) {
	maxWorkers := t.cfg.esi.MaxConcurrentRequests
	if maxWorkers <= 0 {
		maxWorkers = 8
	}
	sem := make(chan struct{}, maxWorkers)
	var wg sync.WaitGroup
	var mu sync.Mutex
	fetched := make(map[string]*fragmentResult, len(targets))
	var allCookies []string
	for src, tgt := range targets {
		wg.Add(1)
		go func(s string, tg fetchTarget) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			res := t.executeInclude(ctx, s, tg, state)
			mu.Lock()
			fetched[s] = res
			if len(res.setCookies) > 0 {
				allCookies = append(allCookies, res.setCookies...)
			}
			mu.Unlock()
		}(src, tgt)
	}
	wg.Wait()
	return fetched, allCookies
}

func (t *Titip) fetchAllTargetsUnbounded(ctx *requestContext, targets map[string]fetchTarget, state esiExecutionState) (map[string]*fragmentResult, []string) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	fetched := make(map[string]*fragmentResult, len(targets))
	var allCookies []string
	for src, tgt := range targets {
		wg.Add(1)
		go func(s string, tg fetchTarget) {
			defer wg.Done()
			res := t.executeInclude(ctx, s, tg, state)
			mu.Lock()
			fetched[s] = res
			if len(res.setCookies) > 0 {
				allCookies = append(allCookies, res.setCookies...)
			}
			mu.Unlock()
		}(src, tgt)
	}
	wg.Wait()
	return fetched, allCookies
}

func (t *Titip) expandNestedESI(ctx *requestContext, body []byte, cookies []string, state esiExecutionState) ([]byte, []string) {
	hasESI, frags := esi.Scan(body)
	if !hasESI || len(frags) == 0 {
		return body, cookies
	}
	processed, nestedCookies := t.processNestedESI(ctx, body, frags, state)
	if len(nestedCookies) > 0 {
		cookies = append(cookies, nestedCookies...)
	}
	return processed, cookies
}

// processESI processes all ESI fragments in parentBody, executes includes concurrently,
// splices the results, and writes the assembled response to ctx.w.
func (t *Titip) processESI(
	ctx *requestContext,
	parentBody []byte,
	fragments []*pb.EsiFragment,
	statusCode int,
	headers http.Header,
	statusToken string,
	rfc9211Detail string,
) {
	startTime := time.Now()
	parentReqCtx := ctx.r.Context()

	// Retrieve or initialize ESI execution state
	execState, ok := parentReqCtx.Value(esiContextKey{}).(esiExecutionState)
	if !ok {
		execState = esiExecutionState{
			depth:       0,
			maxDepth:    t.cfg.esi.MaxDepth,
			visitedURLs: make([]string, 0, 8),
		}
	}

	if ctx.r.URL != nil && ctx.r.URL.Path != "" {
		execState.visitedURLs = append(execState.visitedURLs, ctx.r.URL.Path)
	}

	uniqueTargets := collectTargets(fragments)

	if t.logger.Enabled(parentReqCtx, slog.LevelDebug) {
		t.logger.DebugContext(parentReqCtx, "esi: processing document",
			slog.String("path", ctx.r.URL.Path),
			slog.Int("total_fragments", len(fragments)),
			slog.Int("unique_targets", len(uniqueTargets)),
			slog.Int("parent_bytes", len(parentBody)),
		)
	}

	fetchedBodies, allCookies := t.fetchAllTargets(ctx, uniqueTargets, execState)
	results := assembleResults(fragments, fetchedBodies)

	// 4. Pre-sized output buffer splicing
	outBuf := getBuffer()
	defer putBuffer(outBuf)

	t.spliceFragments(parentBody, results, outBuf)
	splicedBytes := outBuf.Bytes()

	// 5. Header reconciliation
	reconciledHeaders := headers.Clone()
	if reconciledHeaders == nil {
		reconciledHeaders = make(http.Header)
	}

	// Strip ESI/edge internal headers
	reconciledHeaders.Del(headerSurrogateControl)

	// Weaken or recalculate ETag
	if etag := reconciledHeaders.Get(headerETag); etag != "" {
		if !strings.HasPrefix(etag, "W/") && !strings.HasPrefix(etag, "w/") {
			reconciledHeaders.Set(headerETag, "W/"+etag)
		}
	}

	// Update Content-Length only if the origin explicitly provided one
	if reconciledHeaders.Get(headerContentLength) != "" {
		reconciledHeaders.Set(headerContentLength, strconv.Itoa(len(splicedBytes)))
	}

	// Copy reconciled headers to client writer
	for k, vv := range reconciledHeaders {
		if !isHopByHopHeader(k) {
			for _, v := range vv {
				ctx.w.Header().Add(k, v)
			}
		}
	}

	// Forward dynamic subrequest Set-Cookie headers to live client
	if !t.cfg.esi.DisableForwardCookies && len(allCookies) > 0 {
		for _, cookie := range allCookies {
			ctx.w.Header().Add("Set-Cookie", cookie)
		}
	}

	// Emit Cache-Status
	totalDur := time.Since(startTime)
	if t.logger.Enabled(parentReqCtx, slog.LevelDebug) {
		t.logger.DebugContext(parentReqCtx, "esi: completed splicing",
			slog.String("path", ctx.r.URL.Path),
			slog.Int("final_bytes", len(splicedBytes)),
			slog.Duration("duration", totalDur),
			slog.Int("cookies_forwarded", len(allCookies)),
		)
	}
	detailWithESI := fmt.Sprintf("%s; detail=\"esi-includes=%d;time=%s\"", rfc9211Detail, len(fragments), totalDur.String())
	t.emitCacheStatus(ctx.w, statusToken, detailWithESI)

	// Write status and response body
	ctx.w.WriteHeader(statusCode)
	if ctx.r.Method != http.MethodHead {
		_, _ = ctx.w.Write(splicedBytes)
	}
}

// executeInclude fetches a single include target (or alt) with fail-open fallback and panic safety.
func (t *Titip) executeInclude(
	parentCtx *requestContext,
	src string,
	target struct {
		src       string
		alt       string
		timeoutMs uint32
		maxDepth  uint32
		onError   string
		fallback  []byte
	},
	state esiExecutionState,
) (res *fragmentResult) {
	res = &fragmentResult{
		spec: &pb.EsiFragment{
			Src:          target.src,
			Alt:          target.alt,
			TimeoutMs:    target.timeoutMs,
			MaxDepth:     target.maxDepth,
			OnError:      target.onError,
			FallbackBody: target.fallback,
		},
		body: target.fallback,
	}

	defer func() {
		if r := recover(); r != nil {
			if t.logger.Enabled(parentCtx.r.Context(), slog.LevelError) {
				t.logger.ErrorContext(parentCtx.r.Context(), "esi: worker panic recovered",
					slog.Any("panic", r),
					slog.String("src", src),
					slog.String("stack", string(debug.Stack())),
				)
			}
			t.metrics.recordESIFragment("error")
			res.err = fmt.Errorf("titip: esi: panic: %v", r)
			res.body = t.resolveFallback(target.fallback, target.onError)
		}
	}()

	// Check recursion depth limit
	effectiveMaxDepth := state.maxDepth
	if target.maxDepth > 0 && target.maxDepth < effectiveMaxDepth {
		effectiveMaxDepth = target.maxDepth
	}

	if state.depth >= effectiveMaxDepth {
		t.metrics.recordESIFragment("fallback")
		res.err = errESIMaxDepthExceeded
		res.body = t.resolveFallback(target.fallback, target.onError)
		return res
	}

	// Check circular include
	if slices.Contains(state.visitedURLs, src) {
		t.metrics.recordESIFragment("fallback")
		if t.logger.Enabled(parentCtx.r.Context(), slog.LevelWarn) {
			t.logger.WarnContext(parentCtx.r.Context(), "esi: circular include loop detected",
				slog.String("src", src),
				slog.Any("visited", state.visitedURLs),
			)
		}
		res.err = errESICircularInclude
		res.body = t.resolveFallback(target.fallback, target.onError)
		return res
	}

	// Determine timeout budget
	tagTimeout := time.Duration(target.timeoutMs) * time.Millisecond
	effectiveTimeout := t.cfg.esi.MaxTimeout
	if tagTimeout > 0 && tagTimeout < effectiveTimeout {
		effectiveTimeout = tagTimeout
	}

	fetchStart := time.Now()
	childState := esiExecutionState{
		depth:       state.depth + 1,
		maxDepth:    effectiveMaxDepth,
		visitedURLs: append(slices.Clone(state.visitedURLs), src),
	}

	// Attempt primary src fetch
	body, cookies, mode, err := t.fetchFragment(parentCtx, src, effectiveTimeout, childState)
	if err == nil {
		body, cookies = t.expandNestedESI(parentCtx, body, cookies, childState)
		t.metrics.recordESIFragment("success")
		t.metrics.recordESIDuration(mode, time.Since(fetchStart))
		if t.logger.Enabled(parentCtx.r.Context(), slog.LevelDebug) {
			t.logger.DebugContext(parentCtx.r.Context(), "esi: fragment resolved",
				slog.String("src", src),
				slog.String("mode", mode),
				slog.Duration("duration", time.Since(fetchStart)),
				slog.Int("bytes", len(body)),
			)
		}
		res.body = body
		res.setCookies = cookies
		res.mode = mode
		res.duration = time.Since(fetchStart)
		return res
	}

	// Calculate remaining time budget for alt
	elapsed := time.Since(fetchStart)
	remainingBudget := effectiveTimeout - elapsed

	if target.alt != "" && remainingBudget > 0 {
		altStart := time.Now()
		altChildState := esiExecutionState{
			depth:       state.depth + 1,
			maxDepth:    effectiveMaxDepth,
			visitedURLs: append(slices.Clone(state.visitedURLs), target.alt),
		}

		altBody, altCookies, altMode, altErr := t.fetchFragment(parentCtx, target.alt, remainingBudget, altChildState)
		if altErr == nil {
			altBody, altCookies = t.expandNestedESI(parentCtx, altBody, altCookies, altChildState)
			t.metrics.recordESIFragment("fallback")
			t.metrics.recordESIDuration(altMode, time.Since(altStart))
			if t.logger.Enabled(parentCtx.r.Context(), slog.LevelDebug) {
				t.logger.DebugContext(parentCtx.r.Context(), "esi: alt fragment resolved",
					slog.String("src", src),
					slog.String("alt", target.alt),
					slog.String("mode", altMode),
					slog.Duration("duration", time.Since(altStart)),
					slog.Int("bytes", len(altBody)),
				)
			}
			res.body = altBody
			res.setCookies = altCookies
			res.mode = altMode
			res.duration = time.Since(altStart)
			return res
		}
		if t.logger.Enabled(parentCtx.r.Context(), slog.LevelWarn) {
			t.logger.WarnContext(parentCtx.r.Context(), "esi: alt fragment fetch failed",
				slog.String("src", src),
				slog.String("alt", target.alt),
				slog.Any("error", altErr),
			)
		}
	}

	// 3. Both primary and alt failed: Resolve fallback body or onError=continue
	t.metrics.recordESIFragment("fallback")
	res.err = err
	res.body = t.resolveFallback(target.fallback, target.onError)
	res.duration = time.Since(fetchStart)

	if t.logger.Enabled(parentCtx.r.Context(), slog.LevelDebug) {
		t.logger.DebugContext(parentCtx.r.Context(), "esi: fragment resolved via fallback",
			slog.String("src", src),
			slog.Any("error", err),
			slog.Duration("duration", res.duration),
			slog.Int("bytes", len(res.body)),
		)
	}
	return res
}

// processNestedESI executes nested ESI fragment includes recursively.
func (t *Titip) processNestedESI(
	parentCtx *requestContext,
	body []byte,
	fragments []*pb.EsiFragment,
	state esiExecutionState,
) ([]byte, []string) {
	uniqueTargets := collectTargets(fragments)
	fetchedBodies, allCookies := t.fetchAllTargetsUnbounded(parentCtx, uniqueTargets, state)
	results := assembleResults(fragments, fetchedBodies)
	outBuf := getBuffer()
	defer putBuffer(outBuf)
	t.spliceFragments(body, results, outBuf)
	return bytes.Clone(outBuf.Bytes()), allCookies
}

// fetchFragment routes the include request to either a custom in-process fetcher or outbound HTTP client.
func (t *Titip) fetchFragment(
	parentCtx *requestContext,
	targetURL string,
	timeout time.Duration,
	state esiExecutionState,
) ([]byte, []string, string, error) {
	parsed, err := esi.ValidateURLScheme(targetURL)
	if err != nil {
		t.metrics.recordESIFragment("ssrf_blocked")
		return nil, nil, "", err
	}

	// Helper to resolve the outbound fetch URL
	getOutboundURL := func() string {
		if parsed.Host == "" {
			scheme := "http"
			if parentCtx.r.TLS != nil || strings.EqualFold(parentCtx.r.Header.Get("X-Forwarded-Proto"), "https") {
				scheme = "https"
			}
			return scheme + "://" + parentCtx.r.Host + parsed.RequestURI()
		}
		return parsed.String()
	}

	// 1. If custom InternalFetcher is configured and target URL is relative or same host
	isSameHost := parsed.Host == "" || strings.EqualFold(parsed.Host, parentCtx.r.Host)
	if isSameHost && t.cfg.esi.InternalFetcher != nil {
		targetPath := parsed.RequestURI()
		if targetPath == "" {
			targetPath = "/"
		}
		body, cookies, mode, err := t.fetchViaCustomFetcher(parentCtx, targetPath, timeout, state)
		if err == nil {
			return body, cookies, mode, nil
		}
		if errors.Is(err, esi.ErrFallbackToHTTP) {
			if t.logger.Enabled(parentCtx.r.Context(), slog.LevelDebug) {
				t.logger.DebugContext(parentCtx.r.Context(), "esi: internal fetcher requested outbound http fallback",
					slog.String("target_path", targetPath),
				)
			}
			fetchURL := getOutboundURL()
			return t.fetchOutboundHTTP(parentCtx, fetchURL, timeout, state)
		}
		return nil, nil, mode, err
	}

	// 2. Otherwise, fetch directly via outbound HTTP
	fetchURL := getOutboundURL()
	return t.fetchOutboundHTTP(parentCtx, fetchURL, timeout, state)
}

// fetchViaCustomFetcher executes a custom in-process fragment fetcher hook.
func (t *Titip) fetchViaCustomFetcher(
	parentCtx *requestContext,
	targetPath string,
	timeout time.Duration,
	state esiExecutionState,
) ([]byte, []string, string, error) {
	ctx, cancel := context.WithTimeout(parentCtx.r.Context(), timeout)
	defer cancel()

	ctx = context.WithValue(ctx, esiContextKey{}, state)
	parentReq := parentCtx.r.Clone(ctx)

	type customFetchResult struct {
		body    []byte
		headers http.Header
		err     error
	}
	done := make(chan customFetchResult, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- customFetchResult{err: fmt.Errorf("panic: %v", r)}
			}
		}()
		b, h, err := t.cfg.esi.InternalFetcher(ctx, targetPath, parentReq)
		done <- customFetchResult{body: b, headers: h, err: err}
	}()

	var res customFetchResult
	select {
	case res = <-done:
		if res.err != nil {
			return nil, nil, "in_process", res.err
		}
		if ctx.Err() != nil {
			return nil, nil, "in_process", ctx.Err()
		}
	case <-ctx.Done():
		return nil, nil, "in_process", ctx.Err()
	}

	body := res.body
	headers := res.headers

	if t.cfg.esi.MaxResponseSize > 0 && int64(len(body)) > t.cfg.esi.MaxResponseSize {
		return nil, nil, "in_process", fmt.Errorf("fragment body size %d exceeds max %d", len(body), t.cfg.esi.MaxResponseSize)
	}

	var cookies []string
	if !t.cfg.esi.DisableForwardCookies && headers != nil {
		cookies = headers["Set-Cookie"]
	}

	return body, cookies, "in_process", nil
}

// fetchOutboundHTTP fetches an external or loopback fragment using the SSRF-safe HTTP client.
func (t *Titip) fetchOutboundHTTP(
	parentCtx *requestContext,
	targetURL string,
	timeout time.Duration,
	state esiExecutionState,
) ([]byte, []string, string, error) {
	ctx, cancel := context.WithTimeout(parentCtx.r.Context(), timeout)
	defer cancel()

	ctx = context.WithValue(ctx, esiContextKey{}, state)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, nil, "http", err
	}

	// Copy safe general request headers and enforce uncompressed fragments
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set(headerUserAgent, parentCtx.r.Header.Get(headerUserAgent))
	req.Header.Set(headerAcceptLanguage, parentCtx.r.Header.Get(headerAcceptLanguage))

	// Forward sensitive credentials (Cookie, Authorization) ONLY if destination is the same host/domain
	isSameHost := req.URL.Host == "" || strings.EqualFold(req.URL.Host, parentCtx.r.Host)
	if !isSameHost {
		tHost, tPort, err1 := net.SplitHostPort(req.URL.Host)
		if err1 != nil {
			tHost = req.URL.Host
			tPort = ""
		}
		pHost, pPort, err2 := net.SplitHostPort(parentCtx.r.Host)
		if err2 != nil {
			pHost = parentCtx.r.Host
			pPort = ""
		}
		if strings.EqualFold(tHost, pHost) {
			if tPort == pPort || ((tPort == "" || tPort == "80" || tPort == "443") && (pPort == "" || pPort == "80" || pPort == "443")) {
				isSameHost = true
			}
		}
	}

	if isSameHost {
		for _, c := range parentCtx.r.Header.Values(headerCookie) {
			req.Header.Add(headerCookie, c)
		}
		if auth := parentCtx.r.Header.Get(headerAuthorization); auth != "" {
			req.Header.Set(headerAuthorization, auth)
		}
	}

	resp, err := t.esiHTTPClient.Do(req)
	if err != nil {
		return nil, nil, "http", err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, nil, "http", fmt.Errorf("http fragment returned status %d", resp.StatusCode)
	}

	maxSize := t.cfg.esi.MaxResponseSize
	if maxSize <= 0 {
		maxSize = 10 * 1024 * 1024
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxSize+1))
	if err != nil {
		return nil, nil, "http", err
	}
	if int64(len(body)) > maxSize {
		return nil, nil, "http", fmt.Errorf("fragment body size %d exceeds max %d", len(body), maxSize)
	}

	var cookies []string
	if !t.cfg.esi.DisableForwardCookies {
		cookies = resp.Header["Set-Cookie"]
	}

	return body, cookies, "http", nil
}

// spliceFragments splices fragment bodies into the pre-indexed positions in parent using out.Grow.
func (t *Titip) spliceFragments(parent []byte, results []*fragmentResult, out *bytes.Buffer) {
	if len(results) == 0 {
		out.Write(parent)
		return
	}

	// Calculate exact final buffer size to eliminate heap reallocations
	exactSize := len(parent)
	for _, res := range results {
		tagLen := int(res.spec.EndPos - res.spec.StartPos)
		exactSize += len(res.body) - tagLen
	}

	if exactSize > 0 {
		out.Grow(exactSize)
	}

	lastPos := 0
	parentLen := len(parent)

	for _, res := range results {
		start := int(res.spec.StartPos)
		end := int(res.spec.EndPos)

		if start < lastPos || start > parentLen || end > parentLen || start > end {
			continue
		}

		out.Write(parent[lastPos:start])
		if len(res.body) > 0 {
			out.Write(res.body)
		}
		lastPos = end
	}

	if lastPos < parentLen {
		out.Write(parent[lastPos:])
	}
}

// resolveFallback returns the appropriate fallback HTML based on inner body, onerror, and configured error marker.
func (t *Titip) resolveFallback(fallbackBody []byte, onError string) []byte {
	if len(fallbackBody) > 0 {
		return fallbackBody
	}
	if strings.EqualFold(onError, "continue") {
		return nil
	}
	if t.cfg.esi.IncludeErrorMarker != "" {
		return []byte(t.cfg.esi.IncludeErrorMarker)
	}
	return nil
}
