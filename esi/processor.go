package esi

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

	"golang.org/x/sync/singleflight"

	proto "github.com/indragunawan/titip/proto"
)

// HTTP Header names used in ESI protocol processing.
const (
	headerSurrogateCapability = "Surrogate-Capability"
	headerSurrogateControl    = "Surrogate-Control"

	headerETag          = "ETag"
	headerLastModified  = "Last-Modified"
	headerContentLength = "Content-Length"
	headerSetCookie     = "Set-Cookie"
)

var (
	errCircularInclude  = errors.New("esi: circular include loop detected")
	errMaxDepthExceeded = errors.New("esi: max recursion depth exceeded")
)

type esiContextKey struct{}

type esiExecutionState struct {
	depth       uint32
	maxDepth    uint32
	visitedURLs []string
}

type fragmentResult struct {
	spec       *proto.EsiFragment
	body       []byte
	err        error
	setCookies []string
	duration   time.Duration
	mode       string
}

type rawFetchResult struct {
	body    []byte
	cookies []string
	mode    string
}

type flightTracker struct {
	cancel    context.CancelFunc
	listeners int
}

type flightGroup struct {
	mu       sync.Mutex
	trackers map[string]*flightTracker
	sf       singleflight.Group
}

func newFlightGroup() *flightGroup {
	return &flightGroup{
		trackers: make(map[string]*flightTracker),
	}
}

func (fg *flightGroup) do(
	parentCtx context.Context,
	key string,
	fn func(ctx context.Context) (*rawFetchResult, error),
) (*flightTracker, <-chan singleflight.Result) {
	fg.mu.Lock()
	defer fg.mu.Unlock()

	tracker, ok := fg.trackers[key]
	if !ok {
		flightCtx, cancel := context.WithCancel(parentCtx)
		tracker = &flightTracker{
			cancel:    cancel,
			listeners: 1,
		}
		fg.trackers[key] = tracker

		ch := fg.sf.DoChan(key, func() (any, error) {
			defer func() {
				fg.mu.Lock()
				delete(fg.trackers, key)
				fg.mu.Unlock()
			}()
			return fn(flightCtx)
		})
		return tracker, ch
	}

	tracker.listeners++
	ch := fg.sf.DoChan(key, func() (any, error) {
		return nil, nil
	})
	return tracker, ch
}

func (fg *flightGroup) release(key string, tracker *flightTracker) {
	fg.mu.Lock()
	defer fg.mu.Unlock()

	tracker.listeners--
	if tracker.listeners <= 0 {
		tracker.cancel()
		fg.sf.Forget(key)
		if fg.trackers[key] == tracker {
			delete(fg.trackers, key)
		}
	}
}


// Result contains the assembled document and metadata resulting from ESI processing.
// Callers must invoke Release when finished to return the underlying buffer to the memory pool.
type Result struct {
	buf        *bytes.Buffer
	raw        []byte
	SetCookies []string
	Duration   time.Duration
}

// Body returns the assembled document byte slice.
// The slice is valid only until Release is called.
func (r *Result) Body() []byte {
	if r == nil {
		return nil
	}
	if r.buf != nil {
		return r.buf.Bytes()
	}
	return r.raw
}

// Release returns the internal buffer back to the memory pool.
func (r *Result) Release() {
	if r != nil && r.buf != nil {
		putBuffer(r.buf)
		r.buf = nil
	}
}

// Processor orchestrates ESI fragment fetching, recursion handling, error fallbacks, and document splicing.
type Processor struct {
	config     config
	httpClient *http.Client
	logger     *slog.Logger
	metrics    *metrics
}

// NewProcessor constructs a new ESI Processor from the provided options.
func NewProcessor(opts ...Option) *Processor {
	var config config
	for _, opt := range opts {
		opt(&config)
	}

	if config.maxDepth == 0 {
		config.maxDepth = 3
	}
	if config.maxTimeout <= 0 {
		config.maxTimeout = 30 * time.Second
	}
	if config.maxConcurrentRequests <= 0 {
		config.maxConcurrentRequests = 8
	}
	if config.maxResponseSize <= 0 {
		config.maxResponseSize = 10 * 1024 * 1024
	}
	if config.logger == nil {
		config.logger = slog.Default()
	}
	if config.httpClient == nil {
		sc := ssrfConfig{
			BlockPrivateIPs:                !config.allowPrivateIPs,
			AllowedHosts:                   config.allowedHosts,
			AllowPrivateIPsForAllowedHosts: config.allowPrivateIPsForAllowedHosts,
		}
		config.httpClient = &http.Client{
			Transport: newSSRFSafeTransport(sc, 10*time.Second),
		}
	}

	return &Processor{
		config:     config,
		httpClient: config.httpClient,
		logger:     config.logger,
		metrics:    newMetrics(config.metrics),
	}
}

// HeaderRequired returns true if ESI processing requires the Surrogate-Control header.
func (p *Processor) HeaderRequired() bool {
	return p.config.headerRequired
}

// PreserveETag returns true if downstream ETag and Last-Modified headers are preserved.
func (p *Processor) PreserveETag() bool {
	return p.config.preserveETag
}

// AddSurrogateCapability appends an ESI/1.0 capability token for the specified deviceID
// to the request's Surrogate-Capability header (e.g. `deviceID="ESI/1.0"`).
// If deviceID is empty, "esi" is used.
func AddSurrogateCapability(h http.Header, deviceID string) {
	if h == nil {
		return
	}
	if deviceID == "" {
		deviceID = "esi"
	}
	token := fmt.Sprintf(`%s="ESI/1.0"`, deviceID)
	existing := h.Get(headerSurrogateCapability)
	if existing == "" {
		h.Set(headerSurrogateCapability, token)
	} else if !strings.Contains(existing, token) {
		h.Set(headerSurrogateCapability, existing+", "+token)
	}
}

// HasSurrogateCapability reports whether the request's Surrogate-Capability header
// contains an ESI/1.0 capability token for the specified deviceID (or any ESI/1.0 token if deviceID is empty).
func HasSurrogateCapability(h http.Header, deviceID string) bool {
	if h == nil {
		return false
	}
	val := h.Get(headerSurrogateCapability)
	if val == "" {
		return false
	}
	if deviceID == "" {
		return strings.Contains(val, "ESI/1.0")
	}
	return strings.Contains(val, fmt.Sprintf(`%s="ESI/1.0"`, deviceID))
}

// HasSurrogateControl returns true if the header contains an ESI/1.0 directive in Surrogate-Control.
func HasSurrogateControl(h http.Header) bool {
	if h == nil {
		return false
	}
	return strings.Contains(h.Get(headerSurrogateControl), "ESI/1.0")
}

// IsEligible checks if the response headers satisfy ESI processing criteria.
// When HeaderRequired is false, it returns true; otherwise it verifies Surrogate-Control contains "ESI/1.0".
func (p *Processor) IsEligible(h http.Header) bool {
	if !p.HeaderRequired() {
		return true
	}
	return HasSurrogateControl(h)
}

// ReconcileHeaders updates h in-place per ESI 1.0 (§3.2) and RFC 9110 specifications:
// - Removes Surrogate-Control header
// - Weakens or removes ETag and Last-Modified according to PreserveETag
// - Updates Content-Length if present to match the spliced body length
// - Appends fragment Set-Cookie headers from res
//
// If the original headers must be preserved, call h.Clone() before passing.
func (p *Processor) ReconcileHeaders(h http.Header, res *Result) {
	if h == nil {
		return
	}

	h.Del(headerSurrogateControl)

	if p.PreserveETag() {
		if etag := h.Get(headerETag); etag != "" {
			if !strings.HasPrefix(etag, "W/") && !strings.HasPrefix(etag, "w/") {
				h.Set(headerETag, "W/"+etag)
			}
		}
	} else {
		h.Del(headerETag)
		h.Del(headerLastModified)
	}

	if res != nil {
		if h.Get(headerContentLength) != "" {
			h.Set(headerContentLength, strconv.Itoa(len(res.Body())))
		}
		for _, cookie := range res.SetCookies {
			h.Add(headerSetCookie, cookie)
		}
	}
}

// Process scans body for ESI tags and executes includes in a single pass.
// If no ESI tags are detected, it returns the body untouched with zero allocations.
func (p *Processor) Process(ctx context.Context, req *http.Request, body []byte) (*Result, error) {
	hasESI, fragments := Scan(body)
	if !hasESI || len(fragments) == 0 {
		return &Result{
			raw: body,
		}, nil
	}
	return p.ProcessFragments(ctx, req, body, fragments)
}

// ProcessFragments resolves all ESI fragments in parentBody, fetches includes concurrently,
// splices the results, and returns an assembled Result.
func (p *Processor) ProcessFragments(
	ctx context.Context,
	parentReq *http.Request,
	parentBody []byte,
	fragments []*proto.EsiFragment,
) (*Result, error) {
	startTime := time.Now()

	if ctx == nil {
		if parentReq != nil {
			ctx = parentReq.Context()
		} else {
			ctx = context.Background()
		}
	}

	// Retrieve or initialize ESI execution state
	execState, ok := ctx.Value(esiContextKey{}).(esiExecutionState)
	if !ok {
		execState = esiExecutionState{
			depth:       0,
			maxDepth:    p.config.maxDepth,
			visitedURLs: make([]string, 0, 8),
		}
	}

	if parentReq != nil && parentReq.URL != nil && parentReq.URL.Path != "" {
		execState.visitedURLs = append(execState.visitedURLs, parentReq.URL.Path)
	}

	if p.logger.Enabled(ctx, slog.LevelDebug) {
		path := ""
		if parentReq != nil && parentReq.URL != nil {
			path = parentReq.URL.Path
		}
		p.logger.DebugContext(ctx, "esi: processing document",
			slog.String("path", path),
			slog.Int("total_fragments", len(fragments)),
			slog.Int("parent_bytes", len(parentBody)),
		)
	}

	results, allCookies := p.executeFragments(ctx, parentReq, parentBody, fragments, execState)

	outBuf := getBuffer()
	p.spliceFragments(parentBody, results, outBuf)

	totalDur := time.Since(startTime)
	if p.logger.Enabled(ctx, slog.LevelDebug) {
		path := ""
		if parentReq != nil && parentReq.URL != nil {
			path = parentReq.URL.Path
		}
		p.logger.DebugContext(ctx, "esi: completed splicing",
			slog.String("path", path),
			slog.Int("total_fragments", len(fragments)),
			slog.Int("final_bytes", outBuf.Len()),
			slog.Duration("duration", totalDur),
			slog.Int("cookies_forwarded", len(allCookies)),
		)
	}

	return &Result{
		buf:        outBuf,
		SetCookies: allCookies,
		Duration:   totalDur,
	}, nil
}

// safeSlice slices body from start to end with defensive bounds checking against corrupt offsets.
func safeSlice(body []byte, start, end int64) []byte {
	bodyLen := int64(len(body))
	if start <= 0 || end <= start || end > bodyLen {
		return nil
	}
	return body[start:end]
}

func (p *Processor) executeFragments(
	ctx context.Context,
	parentReq *http.Request,
	parentBody []byte,
	fragments []*proto.EsiFragment,
	state esiExecutionState,
) ([]*fragmentResult, []string) {
	results := make([]*fragmentResult, len(fragments))
	fg := newFlightGroup()

	maxWorkers := p.config.maxConcurrentRequests
	if maxWorkers <= 0 {
		maxWorkers = 8
	}
	sem := make(chan struct{}, maxWorkers)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var allCookies []string

	for i, frag := range fragments {
		if frag.Src == "" {
			inner := safeSlice(parentBody, frag.InnerStartPos, frag.InnerEndPos)
			body := p.resolveFallback(inner, frag.OnError)
			results[i] = &fragmentResult{spec: frag, body: body}
			continue
		}

		wg.Add(1)
		go func(idx int, f *proto.EsiFragment) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			res := p.executeFragment(ctx, parentReq, parentBody, f, state, fg)
			results[idx] = res

			if len(res.setCookies) > 0 {
				mu.Lock()
				allCookies = append(allCookies, res.setCookies...)
				mu.Unlock()
			}
		}(i, frag)
	}

	wg.Wait()
	return results, allCookies
}

func (p *Processor) expandNestedESI(
	ctx context.Context,
	parentReq *http.Request,
	body []byte,
	cookies []string,
	state esiExecutionState,
) ([]byte, []string) {
	hasESI, frags := Scan(body)
	if !hasESI || len(frags) == 0 {
		return body, cookies
	}
	processed, nestedCookies := p.processNestedESI(ctx, parentReq, body, frags, state)
	if len(nestedCookies) > 0 {
		cookies = append(cookies, nestedCookies...)
	}
	return processed, cookies
}

func (p *Processor) processNestedESI(
	ctx context.Context,
	parentReq *http.Request,
	body []byte,
	fragments []*proto.EsiFragment,
	state esiExecutionState,
) ([]byte, []string) {
	results, allCookies := p.executeFragments(ctx, parentReq, body, fragments, state)
	outBuf := getBuffer()
	defer putBuffer(outBuf)
	p.spliceFragments(body, results, outBuf)
	return bytes.Clone(outBuf.Bytes()), allCookies
}

func (p *Processor) executeFragment(
	parentCtx context.Context,
	parentReq *http.Request,
	parentBody []byte,
	frag *proto.EsiFragment,
	state esiExecutionState,
	fg *flightGroup,
) *fragmentResult {
	res := &fragmentResult{spec: frag}

	defer func() {
		if r := recover(); r != nil {
			if p.logger.Enabled(parentCtx, slog.LevelError) {
				p.logger.ErrorContext(parentCtx, "esi: worker panic recovered",
					slog.Any("panic", r),
					slog.String("src", frag.Src),
					slog.String("stack", string(debug.Stack())),
				)
			}
			p.metrics.recordFragment("error")
			res.err = fmt.Errorf("esi: panic: %v", r)
			inner := safeSlice(parentBody, frag.InnerStartPos, frag.InnerEndPos)
			res.body = p.resolveFallback(inner, frag.OnError)
		}
	}()

	// Check recursion depth limit
	effectiveMaxDepth := state.maxDepth
	if frag.MaxDepth > 0 && frag.MaxDepth < effectiveMaxDepth {
		effectiveMaxDepth = frag.MaxDepth
	}

	if state.depth >= effectiveMaxDepth {
		p.metrics.recordFragment("fallback")
		res.err = errMaxDepthExceeded
		inner := safeSlice(parentBody, frag.InnerStartPos, frag.InnerEndPos)
		res.body = p.resolveFallback(inner, frag.OnError)
		return res
	}

	// Check circular include
	if slices.Contains(state.visitedURLs, frag.Src) {
		p.metrics.recordFragment("fallback")
		if p.logger.Enabled(parentCtx, slog.LevelWarn) {
			p.logger.WarnContext(parentCtx, "esi: circular include loop detected",
				slog.String("src", frag.Src),
				slog.Any("visited", state.visitedURLs),
			)
		}
		res.err = errCircularInclude
		inner := safeSlice(parentBody, frag.InnerStartPos, frag.InnerEndPos)
		res.body = p.resolveFallback(inner, frag.OnError)
		return res
	}

	// Determine timeout budget
	tagTimeout := time.Duration(frag.TimeoutMs) * time.Millisecond
	effectiveTimeout := p.config.maxTimeout
	if tagTimeout > 0 && tagTimeout < effectiveTimeout {
		effectiveTimeout = tagTimeout
	}

	// fragCtx enforces the tree budget: min(parentCtx deadline, now + effectiveTimeout)
	fragCtx, cancel := context.WithTimeout(parentCtx, effectiveTimeout)
	defer cancel()

	fetchStart := time.Now()
	childState := esiExecutionState{
		depth:       state.depth + 1,
		maxDepth:    effectiveMaxDepth,
		visitedURLs: append(slices.Clone(state.visitedURLs), frag.Src),
	}

	// 1. Attempt primary src fetch via singleflight
	tracker, ch := fg.do(parentCtx, frag.Src, func(fCtx context.Context) (*rawFetchResult, error) {
		body, cookies, mode, err := p.fetchFragment(fCtx, parentReq, frag.Src, p.config.maxTimeout, childState)
		if err != nil {
			return nil, err
		}
		return &rawFetchResult{body: body, cookies: cookies, mode: mode}, nil
	})

	var (
		rawRes *rawFetchResult
		err    error
	)

	select {
	case sfRes, ok := <-ch:
		fg.release(frag.Src, tracker)
		if !ok {
			err = errors.New("esi: flight channel closed")
		} else if sfRes.Err != nil {
			err = sfRes.Err
		} else if r, ok := sfRes.Val.(*rawFetchResult); ok {
			rawRes = r
		} else {
			err = errors.New("esi: invalid flight result type")
		}
	case <-fragCtx.Done():
		fg.release(frag.Src, tracker)
		err = fragCtx.Err()
	}

	if err == nil && rawRes != nil {
		body, cookies := p.expandNestedESI(fragCtx, parentReq, rawRes.body, rawRes.cookies, childState)
		dur := time.Since(fetchStart)
		p.metrics.recordFragment("success")
		p.metrics.recordDuration(rawRes.mode, dur)
		if p.logger.Enabled(parentCtx, slog.LevelDebug) {
			p.logger.DebugContext(parentCtx, "esi: fragment resolved",
				slog.String("src", frag.Src),
				slog.String("mode", rawRes.mode),
				slog.Duration("duration", dur),
				slog.Int("bytes", len(body)),
			)
		}
		res.body = body
		res.setCookies = cookies
		res.mode = rawRes.mode
		res.duration = dur
		return res
	}

	// Calculate remaining time budget for alt
	elapsed := time.Since(fetchStart)
	remainingBudget := effectiveTimeout - elapsed

	if frag.Alt != "" && fragCtx.Err() == nil && remainingBudget > 0 {
		altStart := time.Now()
		altChildState := esiExecutionState{
			depth:       state.depth + 1,
			maxDepth:    effectiveMaxDepth,
			visitedURLs: append(slices.Clone(state.visitedURLs), frag.Alt),
		}

		altTracker, altCh := fg.do(parentCtx, frag.Alt, func(fCtx context.Context) (*rawFetchResult, error) {
			body, cookies, mode, err := p.fetchFragment(fCtx, parentReq, frag.Alt, p.config.maxTimeout, altChildState)
			if err != nil {
				return nil, err
			}
			return &rawFetchResult{body: body, cookies: cookies, mode: mode}, nil
		})

		var (
			altRaw *rawFetchResult
			altErr error
		)

		select {
		case sfRes, ok := <-altCh:
			fg.release(frag.Alt, altTracker)
			if !ok {
				altErr = errors.New("esi: alt flight channel closed")
			} else if sfRes.Err != nil {
				altErr = sfRes.Err
			} else if r, ok := sfRes.Val.(*rawFetchResult); ok {
				altRaw = r
			} else {
				altErr = errors.New("esi: invalid alt flight result type")
			}
		case <-fragCtx.Done():
			fg.release(frag.Alt, altTracker)
			altErr = fragCtx.Err()
		}

		if altErr == nil && altRaw != nil {
			altBody, altCookies := p.expandNestedESI(fragCtx, parentReq, altRaw.body, altRaw.cookies, altChildState)
			altDur := time.Since(altStart)
			p.metrics.recordFragment("fallback")
			p.metrics.recordDuration(altRaw.mode, altDur)
			if p.logger.Enabled(parentCtx, slog.LevelDebug) {
				p.logger.DebugContext(parentCtx, "esi: alt fragment resolved",
					slog.String("src", frag.Src),
					slog.String("alt", frag.Alt),
					slog.String("mode", altRaw.mode),
					slog.Duration("duration", altDur),
					slog.Int("bytes", len(altBody)),
				)
			}
			res.body = altBody
			res.setCookies = altCookies
			res.mode = altRaw.mode
			res.duration = altDur
			return res
		}

		if p.logger.Enabled(parentCtx, slog.LevelWarn) {
			p.logger.WarnContext(parentCtx, "esi: alt fragment fetch failed",
				slog.String("src", frag.Src),
				slog.String("alt", frag.Alt),
				slog.Any("error", altErr),
			)
		}
	}

	// 3. Both primary and alt failed or timed out
	p.metrics.recordFragment("fallback")
	res.err = err
	res.duration = time.Since(fetchStart)
	inner := safeSlice(parentBody, frag.InnerStartPos, frag.InnerEndPos)
	res.body = p.resolveFallback(inner, frag.OnError)

	if p.logger.Enabled(parentCtx, slog.LevelDebug) {
		p.logger.DebugContext(parentCtx, "esi: fragment fetch failed",
			slog.String("src", frag.Src),
			slog.Any("error", err),
			slog.Duration("duration", res.duration),
		)
	}
	return res
}

func (p *Processor) fetchFragment(
	ctx context.Context,
	parentReq *http.Request,
	targetURL string,
	timeout time.Duration,
	state esiExecutionState,
) ([]byte, []string, string, error) {
	parsed, err := validateURLScheme(targetURL)
	if err != nil {
		p.metrics.recordFragment("ssrf_blocked")
		return nil, nil, "", err
	}

	// Helper to resolve the outbound fetch URL
	getOutboundURL := func() string {
		if parsed.Host == "" {
			scheme := "http"
			if parentReq != nil {
				if parentReq.TLS != nil || strings.EqualFold(parentReq.Header.Get("X-Forwarded-Proto"), "https") {
					scheme = "https"
				}
				return scheme + "://" + parentReq.Host + parsed.RequestURI()
			}
			return scheme + "://localhost" + parsed.RequestURI()
		}
		return parsed.String()
	}

	parentHost := ""
	if parentReq != nil {
		parentHost = parentReq.Host
	}

	// 1. If custom InternalFetcher is configured and target URL is relative or same host
	isSameHost := parsed.Host == "" || (parentHost != "" && strings.EqualFold(parsed.Host, parentHost))
	if isSameHost && p.config.internalFetcher != nil {
		targetPath := parsed.RequestURI()
		if targetPath == "" {
			targetPath = "/"
		}
		body, cookies, mode, err := p.fetchViaCustomFetcher(ctx, parentReq, targetPath, timeout, state)
		if err == nil {
			return body, cookies, mode, nil
		}
		if errors.Is(err, ErrFallbackToHTTP) {
			if p.logger.Enabled(ctx, slog.LevelDebug) {
				p.logger.DebugContext(ctx, "esi: internal fetcher requested outbound http fallback",
					slog.String("target_path", targetPath),
				)
			}
			fetchURL := getOutboundURL()
			return p.fetchOutboundHTTP(ctx, parentReq, fetchURL, timeout, state)
		}
		return nil, nil, mode, err
	}

	// 2. Otherwise, fetch directly via outbound HTTP
	fetchURL := getOutboundURL()
	return p.fetchOutboundHTTP(ctx, parentReq, fetchURL, timeout, state)
}

func (p *Processor) fetchViaCustomFetcher(
	parentCtx context.Context,
	parentReq *http.Request,
	targetPath string,
	timeout time.Duration,
	state esiExecutionState,
) ([]byte, []string, string, error) {
	ctx, cancel := context.WithTimeout(parentCtx, timeout)
	defer cancel()

	ctx = context.WithValue(ctx, esiContextKey{}, state)
	var req *http.Request
	if parentReq != nil {
		req = parentReq.Clone(ctx)
	} else {
		var err error
		req, err = http.NewRequestWithContext(ctx, http.MethodGet, targetPath, nil)
		if err != nil {
			return nil, nil, "in_process", err
		}
	}

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
		b, h, err := p.config.internalFetcher(ctx, targetPath, req)
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

	if p.config.maxResponseSize > 0 && int64(len(body)) > p.config.maxResponseSize {
		return nil, nil, "in_process", fmt.Errorf("fragment body size %d exceeds max %d", len(body), p.config.maxResponseSize)
	}

	var cookies []string
	if !p.config.disableForwardCookies && headers != nil {
		cookies = headers["Set-Cookie"]
	}

	return body, cookies, "in_process", nil
}

func (p *Processor) fetchOutboundHTTP(
	parentCtx context.Context,
	parentReq *http.Request,
	targetURL string,
	timeout time.Duration,
	state esiExecutionState,
) ([]byte, []string, string, error) {
	ctx, cancel := context.WithTimeout(parentCtx, timeout)
	defer cancel()

	ctx = context.WithValue(ctx, esiContextKey{}, state)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, nil, "http", err
	}

	req.Header.Set("Accept-Encoding", "identity")
	AddSurrogateCapability(req.Header, "esi")
	if parentReq != nil {
		if ua := parentReq.Header.Get("User-Agent"); ua != "" {
			req.Header.Set("User-Agent", ua)
		}
		if al := parentReq.Header.Get("Accept-Language"); al != "" {
			req.Header.Set("Accept-Language", al)
		}

		isSameHost := req.URL.Host == "" || strings.EqualFold(req.URL.Host, parentReq.Host)
		if !isSameHost {
			tHost, tPort, err1 := net.SplitHostPort(req.URL.Host)
			if err1 != nil {
				tHost = req.URL.Host
				tPort = ""
			}
			pHost, pPort, err2 := net.SplitHostPort(parentReq.Host)
			if err2 != nil {
				pHost = parentReq.Host
				pPort = ""
			}
			if strings.EqualFold(tHost, pHost) {
				if tPort == pPort || ((tPort == "" || tPort == "80" || tPort == "443") && (pPort == "" || pPort == "80" || pPort == "443")) {
					isSameHost = true
				}
			}
		}

		if isSameHost {
			for _, c := range parentReq.Header.Values("Cookie") {
				req.Header.Add("Cookie", c)
			}
			if auth := parentReq.Header.Get("Authorization"); auth != "" {
				req.Header.Set("Authorization", auth)
			}
		}
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, nil, "http", err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		return nil, nil, "http", fmt.Errorf("http fragment returned status %d", resp.StatusCode)
	}

	maxSize := p.config.maxResponseSize
	var r io.Reader = resp.Body
	if maxSize > 0 {
		r = io.LimitReader(resp.Body, maxSize+1)
	}

	body, err := io.ReadAll(r)
	if err != nil {
		return nil, nil, "http", err
	}
	if maxSize > 0 && int64(len(body)) > maxSize {
		return nil, nil, "http", fmt.Errorf("fragment body size %d exceeds max %d", len(body), maxSize)
	}

	var cookies []string
	if !p.config.disableForwardCookies {
		cookies = resp.Header["Set-Cookie"]
	}

	return body, cookies, "http", nil
}

func (p *Processor) spliceFragments(parent []byte, results []*fragmentResult, out *bytes.Buffer) {
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

func (p *Processor) resolveFallback(innerBody []byte, onError string) []byte {
	if len(innerBody) > 0 {
		return innerBody
	}
	if strings.EqualFold(onError, "continue") {
		return nil
	}
	if p.config.includeErrorMarker != "" {
		return []byte(p.config.includeErrorMarker)
	}
	return nil
}
