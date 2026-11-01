package esi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
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

type visitedNode struct {
	url  string
	prev *visitedNode
}

func (n *visitedNode) contains(target string) bool {
	for curr := n; curr != nil; curr = curr.prev {
		if curr.url == target {
			return true
		}
	}
	return false
}

func (n *visitedNode) toSlice() []string {
	var s []string
	for curr := n; curr != nil; curr = curr.prev {
		s = append(s, curr.url)
	}
	slices.Reverse(s)
	return s
}

type esiExecutionState struct {
	depth    uint32
	maxDepth uint32
	visited  *visitedNode
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
	results  map[string]*rawFetchResult
	errors   map[string]error
	sf       singleflight.Group
}

func newFlightGroup() *flightGroup {
	return &flightGroup{}
}

func (fg *flightGroup) do(
	parentCtx context.Context,
	key string,
	fn func(ctx context.Context) (*rawFetchResult, error),
) (*flightTracker, <-chan singleflight.Result) {
	fg.mu.Lock()
	defer fg.mu.Unlock()

	if res, ok := fg.results[key]; ok {
		ch := make(chan singleflight.Result, 1)
		ch <- singleflight.Result{Val: res, Err: fg.errors[key], Shared: true}
		return nil, ch
	}

	tracker := fg.trackers[key]
	if tracker == nil {
		flightCtx, cancel := context.WithCancel(parentCtx)
		tracker = &flightTracker{
			cancel:    cancel,
			listeners: 1,
		}
		if fg.trackers == nil {
			fg.trackers = make(map[string]*flightTracker)
		}
		fg.trackers[key] = tracker

		ch := fg.sf.DoChan(key, func() (any, error) {
			res, err := fn(flightCtx)
			fg.mu.Lock()
			if fg.results == nil {
				fg.results = make(map[string]*rawFetchResult)
			}
			if fg.errors == nil {
				fg.errors = make(map[string]error)
			}
			fg.results[key] = res
			fg.errors[key] = err
			delete(fg.trackers, key)
			fg.mu.Unlock()
			return res, err
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
	if tracker == nil {
		return
	}
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
	setCookies []string
	duration   time.Duration
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

// Duration returns the total wall-clock time spent resolving and splicing all ESI includes.
func (r *Result) Duration() time.Duration {
	if r == nil {
		return 0
	}
	return r.duration
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
func NewProcessor(opts ...Option) (*Processor, error) {
	config := config{
		requireSurrogateControl: true,
		maxDepth:                3,
		maxTimeout:              30 * time.Second,
		maxConcurrentRequests:   8,
		maxResponseSize:         10 * 1024 * 1024,
		logger:                  slog.Default(),
	}
	for _, opt := range opts {
		if opt == nil {
			return nil, fmt.Errorf("%w: option cannot be nil", ErrInvalidOption)
		}
		if err := opt(&config); err != nil {
			if errors.Is(err, ErrInvalidOption) {
				return nil, err
			}
			return nil, fmt.Errorf("%w: %w", ErrInvalidOption, err)
		}
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
	}, nil
}

// ShouldPreserveETag reports whether downstream ETag (weakened) and Last-Modified headers are preserved.
func (p *Processor) ShouldPreserveETag() bool {
	return p.config.preserveETag
}

const surrogateCapabilityTitip = `titip="ESI/1.0"`

var surrogateHeaderTitip = []string{surrogateCapabilityTitip}

func addSurrogateCapability(h http.Header, deviceToken string) {
	if h == nil {
		return
	}
	var capability string
	if deviceToken == "" || deviceToken == "titip" {
		capability = surrogateCapabilityTitip
	} else {
		capability = deviceToken + `="ESI/1.0"`
	}
	existing := h.Get(headerSurrogateCapability)
	if existing == "" {
		if capability == surrogateCapabilityTitip {
			h[headerSurrogateCapability] = surrogateHeaderTitip
		} else {
			h.Set(headerSurrogateCapability, capability)
		}
	} else if !strings.Contains(existing, capability) {
		h.Set(headerSurrogateCapability, existing+", "+capability)
	}
}

// AddSurrogateCapability appends an ESI/1.0 capability token for the specified deviceToken
// to the request's Surrogate-Capability header (e.g. `deviceToken="ESI/1.0"`).
// If p is nil or h is nil, this is a no-op.
// If deviceToken is empty, "titip" is used.
func (p *Processor) AddSurrogateCapability(h http.Header, deviceToken string) {
	if p == nil {
		return
	}
	addSurrogateCapability(h, deviceToken)
}

func cloneRequestHeader(src http.Header) http.Header {
	if src == nil {
		h := make(http.Header, 2)
		h[headerSurrogateCapability] = surrogateHeaderTitip
		return h
	}
	h := make(http.Header, len(src)+2)
	for k, vv := range src {
		vv2 := make([]string, len(vv))
		copy(vv2, vv)
		h[k] = vv2
	}
	existing := h[headerSurrogateCapability]
	if len(existing) == 0 {
		h[headerSurrogateCapability] = surrogateHeaderTitip
	} else if !strings.Contains(existing[0], surrogateCapabilityTitip) {
		h[headerSurrogateCapability] = []string{existing[0] + ", " + surrogateCapabilityTitip}
	}
	return h
}

// containsESI reports whether s contains "ESI/1.0" or "esi/1.0".
func containsESI(s string) bool {
	return strings.Contains(s, "ESI/1.0") || strings.Contains(s, "esi/1.0")
}

// CanProcess reports whether the response headers satisfy ESI processing criteria.
// By default, it verifies that Surrogate-Control contains "ESI/1.0" (case-insensitive).
// It inspects all Surrogate-Control header lines and values without heap allocations.
// When WithoutSurrogateControlRequirement() is configured, it returns true unconditionally.
func (p *Processor) CanProcess(h http.Header) bool {
	if !p.config.requireSurrogateControl {
		return true
	}
	if len(h) == 0 {
		return false
	}
	// Direct canonical map lookup (fast path, 0 allocs)
	if vals, ok := h[headerSurrogateControl]; ok {
		return slices.ContainsFunc(vals, containsESI)
	}
	// Fallback for non-canonical keys
	for k, vv := range h {
		if strings.EqualFold(k, headerSurrogateControl) {
			return slices.ContainsFunc(vv, containsESI)
		}
	}
	return false
}

// CleanSurrogateControl strips all directives containing "ESI/1.0" (case-insensitive)
// from the Surrogate-Control header per W3C Edge Architecture Specification §3.4.
// If remaining directives exist (e.g. "max-age=3600"), Surrogate-Control is updated with them.
// If no directives remain, the Surrogate-Control header is completely removed from h.
// Fast-paths return immediately with zero allocations when no ESI directives exist.
func CleanSurrogateControl(h http.Header) {
	if len(h) == 0 {
		return
	}

	vals, ok := h[headerSurrogateControl]
	key := headerSurrogateControl
	if !ok {
		for k, vv := range h {
			if strings.EqualFold(k, headerSurrogateControl) {
				vals = vv
				key = k
				ok = true
				break
			}
		}
	}
	if !ok || len(vals) == 0 {
		return
	}

	if !slices.ContainsFunc(vals, containsESI) {
		return
	}

	var remaining []string
	for _, val := range vals {
		if !containsESI(val) {
			remaining = append(remaining, val)
			continue
		}
		for part := range strings.SplitSeq(val, ",") {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" && !containsESI(trimmed) {
				remaining = append(remaining, trimmed)
			}
		}
	}

	if key != headerSurrogateControl {
		delete(h, key)
	}
	if len(remaining) == 0 {
		delete(h, headerSurrogateControl)
	} else {
		h[headerSurrogateControl] = []string{strings.Join(remaining, ", ")}
	}
}

// ReconcileHeaders updates h in-place per ESI (§3.2) and RFC 9110 specifications:
// - Strips ESI directives from Surrogate-Control (or removes header if empty)
// - Weakens or removes ETag and Last-Modified according to PreserveETag
// - Updates Content-Length if present to match the spliced body length
// - Appends fragment Set-Cookie headers from res
//
// If the original headers must be preserved, call h.Clone() before passing.
func (p *Processor) ReconcileHeaders(h http.Header, res *Result) {
	if h == nil {
		return
	}

	CleanSurrogateControl(h)

	if p.ShouldPreserveETag() {
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
		for _, cookie := range res.setCookies {
			h.Add(headerSetCookie, cookie)
		}
	}
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

	var cancel context.CancelFunc
	if _, ok := ctx.Deadline(); !ok && p.config.maxTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, p.config.maxTimeout)
		defer cancel()
	}

	// Retrieve or initialize ESI execution state
	execState, ok := ctx.Value(esiContextKey{}).(*esiExecutionState)
	if !ok {
		execState = &esiExecutionState{
			depth:    0,
			maxDepth: p.config.maxDepth,
		}
		if parentReq != nil && parentReq.URL != nil && parentReq.URL.Path != "" {
			execState.visited = &visitedNode{url: parentReq.URL.Path}
		}
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
		setCookies: allCookies,
		duration:   totalDur,
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

func hasDuplicateURLs(fragments []*proto.EsiFragment) bool {
	for i, f := range fragments {
		if f.Src != "" {
			for _, other := range fragments[i+1:] {
				if f.Src == other.Src {
					return true
				}
			}
		}
	}
	return false
}

type fragmentRunner struct {
	p          *Processor
	ctx        context.Context
	parentReq  *http.Request
	parentBody []byte
	state      *esiExecutionState
	fg         *flightGroup
	results    []fragmentResult
	sem        chan struct{}
	wg         sync.WaitGroup
}

var fragmentRunnerPool = sync.Pool{
	New: func() any {
		return new(fragmentRunner)
	},
}

func (r *fragmentRunner) exec(idx int, f *proto.EsiFragment) {
	if r.sem != nil {
		r.sem <- struct{}{}
		defer func() { <-r.sem }()
	}
	r.p.executeFragment(r.ctx, r.parentReq, r.parentBody, f, r.state, r.fg, &r.results[idx])
}

func (p *Processor) executeFragments(
	ctx context.Context,
	parentReq *http.Request,
	parentBody []byte,
	fragments []*proto.EsiFragment,
	state *esiExecutionState,
) ([]fragmentResult, []string) {
	results := make([]fragmentResult, len(fragments))
	if len(fragments) == 0 {
		return results, nil
	}

	if len(fragments) == 1 {
		frag := fragments[0]
		if frag.Src == "" {
			inner := safeSlice(parentBody, frag.InnerStartPos, frag.InnerEndPos)
			body := p.resolveFallback(inner, frag.OnError)
			results[0] = fragmentResult{spec: frag, body: body}
			return results, nil
		}
		p.executeFragment(ctx, parentReq, parentBody, frag, state, nil, &results[0])
		return results, results[0].setCookies
	}

	var fg *flightGroup
	if hasDuplicateURLs(fragments) {
		fg = newFlightGroup()
	}

	maxWorkers := p.config.maxConcurrentRequests
	if maxWorkers <= 0 {
		maxWorkers = 8
	}
	var sem chan struct{}
	if len(fragments) > maxWorkers {
		sem = make(chan struct{}, maxWorkers)
	}

	runner := fragmentRunnerPool.Get().(*fragmentRunner)
	runner.p = p
	runner.ctx = ctx
	runner.parentReq = parentReq
	runner.parentBody = parentBody
	runner.state = state
	runner.fg = fg
	runner.results = results
	runner.sem = sem

	lastIdx := len(fragments) - 1
	for i := 0; i < lastIdx; i++ {
		frag := fragments[i]
		if frag.Src == "" {
			inner := safeSlice(parentBody, frag.InnerStartPos, frag.InnerEndPos)
			body := p.resolveFallback(inner, frag.OnError)
			results[i] = fragmentResult{spec: frag, body: body}
			continue
		}

		runner.wg.Add(1)
		go func(idx int, f *proto.EsiFragment) {
			defer runner.wg.Done()
			runner.exec(idx, f)
		}(i, frag)
	}

	lastFrag := fragments[lastIdx]
	if lastFrag.Src == "" {
		inner := safeSlice(parentBody, lastFrag.InnerStartPos, lastFrag.InnerEndPos)
		body := p.resolveFallback(inner, lastFrag.OnError)
		results[lastIdx] = fragmentResult{spec: lastFrag, body: body}
	} else {
		runner.exec(lastIdx, lastFrag)
	}

	runner.wg.Wait()
	*runner = fragmentRunner{}
	fragmentRunnerPool.Put(runner)

	var allCookies []string
	for i := range results {
		if len(results[i].setCookies) > 0 {
			allCookies = append(allCookies, results[i].setCookies...)
		}
	}

	return results, allCookies
}

func (p *Processor) expandNestedESI(
	ctx context.Context,
	parentReq *http.Request,
	body []byte,
	cookies []string,
	state *esiExecutionState,
) ([]byte, []string) {
	frags := Scan(body)
	if len(frags) == 0 {
		return body, cookies
	}
	processed, nestedCookies := p.executeAndSpliceNestedESI(ctx, parentReq, body, frags, state)
	if len(nestedCookies) > 0 {
		cookies = append(slices.Clone(cookies), nestedCookies...)
	}
	return processed, cookies
}

func (p *Processor) executeAndSpliceNestedESI(
	ctx context.Context,
	parentReq *http.Request,
	body []byte,
	fragments []*proto.EsiFragment,
	state *esiExecutionState,
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
	state *esiExecutionState,
	fg *flightGroup,
	res *fragmentResult,
) {
	res.spec = frag

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
		return
	}

	// Check circular include
	if state.visited.contains(frag.Src) {
		p.metrics.recordFragment("fallback")
		if p.logger.Enabled(parentCtx, slog.LevelWarn) {
			p.logger.WarnContext(parentCtx, "esi: circular include loop detected",
				slog.String("src", frag.Src),
				slog.Any("visited", state.visited.toSlice()),
			)
		}
		res.err = errCircularInclude
		inner := safeSlice(parentBody, frag.InnerStartPos, frag.InnerEndPos)
		res.body = p.resolveFallback(inner, frag.OnError)
		return
	}

	// Determine timeout budget
	tagTimeout := time.Duration(frag.TimeoutMs) * time.Millisecond
	effectiveTimeout := p.config.maxTimeout
	if tagTimeout > 0 && tagTimeout < effectiveTimeout {
		effectiveTimeout = tagTimeout
	}

	var (
		fragCtx = parentCtx
		cancel  context.CancelFunc
	)
	if tagTimeout > 0 {
		if parentDeadline, ok := parentCtx.Deadline(); !ok || time.Now().Add(tagTimeout).Before(parentDeadline) {
			fragCtx, cancel = context.WithTimeout(parentCtx, tagTimeout)
			defer cancel()
		}
	} else if _, ok := parentCtx.Deadline(); !ok && effectiveTimeout > 0 {
		fragCtx, cancel = context.WithTimeout(parentCtx, effectiveTimeout)
		defer cancel()
	}

	fetchStart := time.Now()
	childState := &esiExecutionState{
		depth:    state.depth + 1,
		maxDepth: effectiveMaxDepth,
		visited:  &visitedNode{url: frag.Src, prev: state.visited},
	}

	rawRes, err := p.fetchWithFlight(fragCtx, parentCtx, parentReq, frag.Src, effectiveTimeout, childState, fg)
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
		return
	}

	// Calculate remaining time budget for alt
	elapsed := time.Since(fetchStart)
	remainingBudget := effectiveTimeout - elapsed

	if frag.Alt != "" && fragCtx.Err() == nil && remainingBudget > 0 {
		altStart := time.Now()
		altChildState := &esiExecutionState{
			depth:    state.depth + 1,
			maxDepth: effectiveMaxDepth,
			visited:  &visitedNode{url: frag.Alt, prev: state.visited},
		}

		altRaw, altErr := p.fetchWithFlight(fragCtx, parentCtx, parentReq, frag.Alt, remainingBudget, altChildState, fg)
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
			return
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
}

func (p *Processor) fetchWithFlight(
	fragCtx, parentCtx context.Context,
	parentReq *http.Request,
	targetURL string,
	timeout time.Duration,
	state *esiExecutionState,
	fg *flightGroup,
) (*rawFetchResult, error) {
	if fg == nil {
		body, cookies, mode, err := p.fetchFragment(fragCtx, parentReq, targetURL, timeout, state)
		if err != nil {
			return nil, err
		}
		return &rawFetchResult{body: body, cookies: cookies, mode: mode}, nil
	}

	tracker, ch := fg.do(parentCtx, targetURL, func(fCtx context.Context) (*rawFetchResult, error) {
		body, cookies, mode, err := p.fetchFragment(fCtx, parentReq, targetURL, p.config.maxTimeout, state)
		if err != nil {
			return nil, err
		}
		return &rawFetchResult{body: body, cookies: cookies, mode: mode}, nil
	})

	select {
	case sfRes, ok := <-ch:
		fg.release(targetURL, tracker)
		if !ok {
			return nil, errors.New("esi: flight channel closed")
		}
		if sfRes.Err != nil {
			return nil, sfRes.Err
		}
		if r, ok := sfRes.Val.(*rawFetchResult); ok {
			return r, nil
		}
		return nil, errors.New("esi: invalid flight result type")
	case <-fragCtx.Done():
		fg.release(targetURL, tracker)
		return nil, fragCtx.Err()
	}
}

func (p *Processor) fetchFragment(
	ctx context.Context,
	parentReq *http.Request,
	targetURL string,
	timeout time.Duration,
	state *esiExecutionState,
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
		body, cookies, mode, err := p.fetchViaCustomFetcher(ctx, parentReq, targetPath, state)
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
	state *esiExecutionState,
) ([]byte, []string, string, error) {
	ctx := context.WithValue(parentCtx, esiContextKey{}, state)
	var req *http.Request
	if parentReq != nil {
		req = parentReq.WithContext(ctx)
		req.Header = cloneRequestHeader(parentReq.Header)
		if parentReq.Trailer != nil {
			req.Trailer = parentReq.Trailer.Clone()
		}
	} else {
		var err error
		req, err = http.NewRequestWithContext(ctx, http.MethodGet, targetPath, nil)
		if err != nil {
			return nil, nil, "in_process", err
		}
		p.AddSurrogateCapability(req.Header, "titip")
	}

	var (
		body    []byte
		headers http.Header
		err     error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic: %v", r)
			}
		}()
		body, headers, err = p.config.internalFetcher(ctx, targetPath, req)
	}()

	if err != nil {
		return nil, nil, "in_process", err
	}
	if ctx.Err() != nil {
		return nil, nil, "in_process", ctx.Err()
	}

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
	state *esiExecutionState,
) ([]byte, []string, string, error) {
	ctx, cancel := context.WithTimeout(parentCtx, timeout)
	defer cancel()

	ctx = context.WithValue(ctx, esiContextKey{}, state)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, nil, "http", err
	}

	req.Header.Set("Accept-Encoding", "identity")
	p.AddSurrogateCapability(req.Header, "titip")
	if parentReq != nil {
		if ua := parentReq.Header.Get("User-Agent"); ua != "" {
			req.Header.Set("User-Agent", ua)
		}
		if al := parentReq.Header.Get("Accept-Language"); al != "" {
			req.Header.Set("Accept-Language", al)
		}

		isSameHost := req.URL.Host == "" || strings.EqualFold(stripDefaultPort(req.URL.Host), stripDefaultPort(parentReq.Host))

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

// spliceFragments splices resolved fragment bodies into the parent HTML buffer out.
//
// INVARIANT: res.body may be shared across multiple duplicate tags via document-level
// singleflight memoization. NEVER mutate res.body in-place. Always write transformations
// directly into the destination buffer 'out'.
func (p *Processor) spliceFragments(parent []byte, results []fragmentResult, out *bytes.Buffer) {
	if len(results) == 0 {
		out.Write(parent)
		return
	}

	// Calculate exact final buffer size to eliminate heap reallocations
	exactSize := len(parent)
	for i := range results {
		res := &results[i]
		tagLen := int(res.spec.EndPos - res.spec.StartPos)
		exactSize += len(res.body) - tagLen
	}

	if exactSize > 0 {
		out.Grow(exactSize)
	}

	lastPos := 0
	parentLen := len(parent)

	for i := range results {
		res := &results[i]
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

func stripDefaultPort(h string) string {
	h = strings.TrimSuffix(h, ":80")
	return strings.TrimSuffix(h, ":443")
}
