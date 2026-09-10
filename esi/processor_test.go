package esi

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func processResult(t *testing.T, proc *Processor, html string, reqPath ...string) (*Result, time.Duration) {
	t.Helper()
	path := "http://localhost/test"
	if len(reqPath) > 0 && reqPath[0] != "" {
		path = reqPath[0]
	}
	req := httptest.NewRequest(http.MethodGet, path, nil)
	start := time.Now()
	res, err := proc.Process(context.Background(), req, []byte(html))
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("unexpected process error: %v", err)
	}
	return res, elapsed
}

func processHTML(t *testing.T, proc *Processor, html string, reqPath ...string) (string, time.Duration) {
	t.Helper()
	res, elapsed := processResult(t, proc, html, reqPath...)
	defer res.Release()
	return string(res.Body()), elapsed
}

func runDualFetcher(t *testing.T, mux *http.ServeMux, fn func(t *testing.T, proc *Processor, base string)) {
	t.Helper()
	t.Run("internal", func(t *testing.T) {
		proc := NewProcessor(
			WithMaxTimeout(5*time.Second),
			WithInternalFetcher(HandlerFetcher(mux)),
		)
		fn(t, proc, "")
	})
	t.Run("outbound_http", func(t *testing.T) {
		ts := httptest.NewServer(mux)
		defer ts.Close()
		proc := NewProcessor(
			WithMaxTimeout(5*time.Second),
			WithAllowPrivateIPs(true),
		)
		fn(t, proc, ts.URL)
	})
}

func TestProcessor_InProcessFetcher(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/cart", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Set-Cookie", "cart_id=12345; Path=/")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<span>3 Items</span>`))
	})
	mux.HandleFunc("/api/user", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<span>Alice</span>`))
	})

	reg := prometheus.NewRegistry()
	proc := NewProcessor(
		WithMaxTimeout(5*time.Second),
		WithMaxConcurrentRequests(4),
		WithInternalFetcher(HandlerFetcher(mux)),
		WithMetrics(reg),
	)

	html := []byte(`<html><body><div id="cart"><esi:include src="/api/cart" /></div><div id="user"><esi:include src="/api/user" /></div></body></html>`)
	req := httptest.NewRequest(http.MethodGet, "http://localhost/dashboard", nil)
	res, err := proc.Process(context.Background(), req, html)
	if err != nil {
		t.Fatalf("process error: %v", err)
	}
	defer res.Release()

	expectedBody := `<html><body><div id="cart"><span>3 Items</span></div><div id="user"><span>Alice</span></div></body></html>`
	if string(res.Body()) != expectedBody {
		t.Errorf("got %q, want %q", string(res.Body()), expectedBody)
	}

	if len(res.SetCookies) != 1 || !strings.Contains(res.SetCookies[0], "cart_id=12345") {
		t.Errorf("expected cart_id cookie, got: %v", res.SetCookies)
	}

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather error: %v", err)
	}
	var foundFragments bool
	for _, mf := range mfs {
		if mf.GetName() == "esi_fragments_total" {
			foundFragments = true
			if len(mf.GetMetric()) == 0 || mf.GetMetric()[0].GetCounter().GetValue() != 2 {
				t.Errorf("expected 2 success fragments, got %v", mf.GetMetric())
			}
		}
	}
	if !foundFragments {
		t.Errorf("expected esi_fragments_total metric to be gathered")
	}
}

func TestProcessor_OutboundHTTP(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("outbound fragment"))
	}))
	defer ts.Close()

	proc := NewProcessor(
		WithMaxTimeout(5*time.Second),
		WithAllowPrivateIPs(true), // test server runs on loopback
	)

	req := httptest.NewRequest(http.MethodGet, "http://example.com/page", nil)
	res, err := proc.Process(context.Background(), req, []byte(`<div><esi:include src="`+ts.URL+`/fragment" /></div>`))
	if err != nil {
		t.Fatalf("process error: %v", err)
	}
	defer res.Release()

	expected := `<div>outbound fragment</div>`
	if string(res.Body()) != expected {
		t.Errorf("got %q, want %q", string(res.Body()), expected)
	}
}

func TestProcessor_OutboundHTTP_RelativePath(t *testing.T) {
	t.Run("HTTP relative path", func(t *testing.T) {
		var receivedCookie, receivedUA, receivedCapability string
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/relative-fragment" {
				receivedCookie = r.Header.Get("Cookie")
				receivedUA = r.Header.Get("User-Agent")
				receivedCapability = r.Header.Get("Surrogate-Capability")
				w.Header().Set("Content-Type", "text/plain")
				_, _ = w.Write([]byte("relative fragment resolved via outbound HTTP"))
				return
			}
			http.NotFound(w, r)
		}))
		defer ts.Close()

		u, err := url.Parse(ts.URL)
		if err != nil {
			t.Fatalf("failed to parse test server URL: %v", err)
		}

		proc := NewProcessor(
			WithAllowPrivateIPs(true), // test server runs on loopback
			WithMaxTimeout(5*time.Second),
		)

		req := httptest.NewRequest(http.MethodGet, "http://"+u.Host+"/page", nil)
		req.Host = u.Host
		req.Header.Set("User-Agent", "TestClient/1.0")
		req.Header.Set("Cookie", "session=secret123")

		res, err := proc.Process(context.Background(), req, []byte(`<div><esi:include src="/relative-fragment" /></div>`))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer res.Release()

		expected := `<div>relative fragment resolved via outbound HTTP</div>`
		if string(res.Body()) != expected {
			t.Errorf("got %q, want %q", string(res.Body()), expected)
		}
		if receivedCookie != "session=secret123" {
			t.Errorf("expected Cookie to be forwarded to same-host outbound subrequest, got %q", receivedCookie)
		}
		if receivedUA != "TestClient/1.0" {
			t.Errorf("expected User-Agent to be forwarded, got %q", receivedUA)
		}
		if !strings.Contains(receivedCapability, "ESI/1.0") {
			t.Errorf("expected Surrogate-Capability to be sent, got %q", receivedCapability)
		}
	})

	t.Run("HTTPS relative path via X-Forwarded-Proto", func(t *testing.T) {
		var receivedHost string
		tsTLS := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/secure-frag" {
				receivedHost = r.Host
				w.Header().Set("Content-Type", "text/plain")
				_, _ = w.Write([]byte("secure content"))
				return
			}
			http.NotFound(w, r)
		}))
		defer tsTLS.Close()

		uTLS, err := url.Parse(tsTLS.URL)
		if err != nil {
			t.Fatalf("failed to parse test server URL: %v", err)
		}

		procTLS := NewProcessor(
			WithHTTPClient(tsTLS.Client()),
			WithAllowPrivateIPs(true),
			WithMaxTimeout(5*time.Second),
		)

		req := httptest.NewRequest(http.MethodGet, "http://"+uTLS.Host+"/page", nil)
		req.Host = uTLS.Host
		req.Header.Set("X-Forwarded-Proto", "https")

		res, err := procTLS.Process(context.Background(), req, []byte(`<div><esi:include src="/secure-frag" /></div>`))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer res.Release()

		expected := `<div>secure content</div>`
		if string(res.Body()) != expected {
			t.Errorf("got %q, want %q", string(res.Body()), expected)
		}
		if receivedHost != uTLS.Host {
			t.Errorf("expected host %q, got %q", uTLS.Host, receivedHost)
		}
	})
}

func TestProcessor_FallbackOnErrorContinue(t *testing.T) {
	proc := NewProcessor(WithMaxTimeout(1 * time.Second))
	got, _ := processHTML(t, proc, `<div>Before <esi:include src="http://192.0.2.1/fail" onerror="continue" timeout="50" /> After</div>`, "http://example.com/")
	if got != `<div>Before  After</div>` {
		t.Errorf("got %q, want %q", got, `<div>Before  After</div>`)
	}
}

func TestProcessor_FallbackErrorMarker(t *testing.T) {
	proc := NewProcessor(
		WithMaxTimeout(1*time.Second),
		WithIncludeErrorMarker("<!-- ESI ERROR -->"),
	)
	got, _ := processHTML(t, proc, `<div>Before <esi:include src="http://192.0.2.1/fail" timeout="50" /> After</div>`, "http://example.com/")
	if got != `<div>Before <!-- ESI ERROR --> After</div>` {
		t.Errorf("got %q, want %q", got, `<div>Before <!-- ESI ERROR --> After</div>`)
	}
}

func TestProcessor_CircularInclude(t *testing.T) {
	proc := NewProcessor(WithMaxTimeout(2 * time.Second))
	got, _ := processHTML(t, proc, `<div><esi:include src="/circular" onerror="continue" /></div>`, "http://example.com/circular")
	if got != `<div></div>` {
		t.Errorf("got %q, want %q", got, `<div></div>`)
	}
}

func TestProcessor_MaxRecursionDepth(t *testing.T) {
	var callCount atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/nested", func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<div>nested <esi:include src="/nested" onerror="continue" /></div>`))
	})

	proc := NewProcessor(
		WithMaxDepth(2),
		WithMaxTimeout(2*time.Second),
		WithInternalFetcher(HandlerFetcher(mux)),
	)

	_, _ = processHTML(t, proc, `<div><esi:include src="/nested" onerror="continue" /></div>`, "http://localhost/page")
	if callCount.Load() > 3 {
		t.Errorf("expected recursion depth <= 2, got callCount=%d", callCount.Load())
	}
}

func TestProcessor_SSRFBlocked(t *testing.T) {
	proc := NewProcessor(
		WithAllowPrivateIPs(false), // SSRF protection active
		WithMaxTimeout(1*time.Second),
	)

	got, _ := processHTML(t, proc, `<div><esi:include src="http://127.0.0.1:9999/secret" onerror="continue" /></div>`, "http://example.com/")
	if got != `<div></div>` {
		t.Errorf("got %q, want %q", got, `<div></div>`)
	}
}

func TestProcessor_WorkerPanicRecovery(t *testing.T) {
	panicFetcher := func(ctx context.Context, targetPath string, r *http.Request) ([]byte, http.Header, error) {
		panic("simulated worker failure")
	}

	proc := NewProcessor(
		WithInternalFetcher(panicFetcher),
		WithIncludeErrorMarker("PANIC_RECOVERED"),
	)

	got, _ := processHTML(t, proc, `<div><esi:include src="/panic-test" /></div>`, "http://localhost/")
	if got != `<div>PANIC_RECOVERED</div>` {
		t.Errorf("got %q, want %q", got, `<div>PANIC_RECOVERED</div>`)
	}
}

func TestProcessor_FallbackToOutboundHTTPOn404(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("from outbound server"))
	}))
	defer ts.Close()

	// In-process router returns 404
	emptyMux := http.NewServeMux()

	proc := NewProcessor(
		WithAllowPrivateIPs(true),
		WithInternalFetcher(HandlerFetcher(emptyMux)),
	)

	// Since emptyMux returns 404 for this path, HandlerFetcher returns ErrFallbackToHTTP,
	// causing Processor to fall back to outbound HTTP
	parentBody := []byte(`<div><esi:include src="` + ts.URL + `/fallback-path" /></div>`)
	req := httptest.NewRequest(http.MethodGet, "http://localhost/", nil)
	res, err := proc.Process(context.Background(), req, parentBody)
	if err != nil {
		t.Fatalf("process error: %v", err)
	}
	defer res.Release()

	expected := `<div>from outbound server</div>`
	if string(res.Body()) != expected {
		t.Errorf("got %q, want %q", string(res.Body()), expected)
	}
}

func TestProcessor_BufferReleaseSafety(t *testing.T) {
	proc := NewProcessor()

	parentBody := []byte(`Hello World`)
	res, err := proc.ProcessFragments(context.Background(), nil, parentBody, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	body := res.Body()
	if string(body) != "Hello World" {
		t.Errorf("got %q, want Hello World", string(body))
	}

	res.Release()
	if res.Body() != nil {
		t.Errorf("expected body to be nil after Release, got %v", res.Body())
	}
	// Calling Release multiple times should be safe (no panic)
	res.Release()
}

func TestSafeSlice_BoundsAndSafety(t *testing.T) {
	b := []byte("<div><esi:include src=\"/api\">Fallback Text</esi:include></div>")

	tests := []struct {
		name     string
		body     []byte
		start    int64
		end      int64
		expected string
	}{
		{"valid_inner_content", b, 29, 42, "Fallback Text"},
		{"zero_start_pos", b, 0, 42, ""},
		{"negative_start_pos", b, -5, 42, ""},
		{"negative_end_pos", b, 29, -1, ""},
		{"end_equals_start", b, 29, 29, ""},
		{"end_less_than_start", b, 42, 29, ""},
		{"end_exceeds_body_len", b, 29, int64(len(b) + 100), ""},
		{"math_max_int64", b, 0, math.MaxInt64, ""},
		{"nil_body", nil, 10, 20, ""},
		{"empty_body", []byte{}, 10, 20, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := safeSlice(tt.body, tt.start, tt.end)
			if string(got) != tt.expected {
				t.Errorf("safeSlice() = %q, want %q", string(got), tt.expected)
			}
		})
	}
}

func TestProcessor_CorruptedInnerOffsets(t *testing.T) {
	parent := []byte(`<div><esi:include src="/failing-api">fallback</esi:include></div>`)
	hasESI, frags := Scan(parent)
	if !hasESI || len(frags) == 0 {
		t.Fatalf("expected fragment from scan")
	}
	// Corrupt inner offsets
	frags[0].InnerStartPos = -99
	frags[0].InnerEndPos = 9999999

	proc := NewProcessor()
	res, err := proc.ProcessFragments(context.Background(), nil, parent, frags)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer res.Release()

	// safeSlice returns nil for corrupt bounds, so fallback resolves to nil and tag is removed
	expected := `<div></div>`
	if string(res.Body()) != expected {
		t.Errorf("got %q, want %q", string(res.Body()), expected)
	}
}

func TestProcessor_Process(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/user", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("<b>Alice</b>"))
	})

	proc := NewProcessor(
		WithInternalFetcher(HandlerFetcher(mux)),
	)

	// 1. Document with ESI tags: should scan and splice in one step
	docWithESI := []byte(`<div>User: <esi:include src="/user" /></div>`)
	req := httptest.NewRequest(http.MethodGet, "http://localhost/page", nil)
	res1, err := proc.Process(context.Background(), req, docWithESI)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer res1.Release()

	expected := `<div>User: <b>Alice</b></div>`
	if string(res1.Body()) != expected {
		t.Errorf("got %q, want %q", string(res1.Body()), expected)
	}

	// 2. Document without ESI tags: returns body untouched with zero buffer allocation
	docWithoutESI := []byte(`<div>Plain HTML content without any ESI tags</div>`)
	res2, err := proc.Process(context.Background(), req, docWithoutESI)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer res2.Release()

	if string(res2.Body()) != string(docWithoutESI) {
		t.Errorf("got %q, want %q", string(res2.Body()), string(docWithoutESI))
	}
	// Buffer should be nil (raw passthrough)
	if res2.buf != nil {
		t.Errorf("expected nil buf for document without ESI")
	}
}

func TestProcessor_CanProcess(t *testing.T) {
	tests := []struct {
		header   http.Header
		required bool
	}{
		{nil, false},
		{http.Header{}, false},
		{http.Header{"Surrogate-Control": []string{"max-age=3600"}}, false},
		{http.Header{"Surrogate-Control": []string{"content=\"ESI/1.0\""}}, true},
		{http.Header{"Surrogate-Control": []string{"abc, content=\"ESI/1.0\", max-age=60"}}, true},
	}

	procDefault := NewProcessor()
	procRequired := NewProcessor(WithHeaderRequired(true))

	for _, tt := range tests {
		if !procDefault.CanProcess(tt.header) {
			t.Errorf("procDefault.CanProcess(%v) = false, want true", tt.header)
		}
		if got := procRequired.CanProcess(tt.header); got != tt.required {
			t.Errorf("procRequired.CanProcess(%v) = %v, want %v", tt.header, got, tt.required)
		}
	}
}

func TestProcessor_SurrogateCapability(t *testing.T) {
	// 1. Nil header safe
	AddSurrogateCapability(nil, "titip")

	// 2. Empty header sets capability
	h := make(http.Header)
	AddSurrogateCapability(h, "titip")
	if got := h.Get(headerSurrogateCapability); got != `titip="ESI/1.0"` {
		t.Errorf("expected %q, got %q", `titip="ESI/1.0"`, got)
	}

	// 3. Already present token is not duplicated
	AddSurrogateCapability(h, "titip")
	if got := h.Get(headerSurrogateCapability); got != `titip="ESI/1.0"` {
		t.Errorf("expected no duplicate, got %q", got)
	}

	// 4. Appends to existing capability from downstream
	h2 := make(http.Header)
	h2.Set(headerSurrogateCapability, `cdn="ESI/1.0"`)
	AddSurrogateCapability(h2, "titip")
	expected := `cdn="ESI/1.0", titip="ESI/1.0"`
	if got := h2.Get(headerSurrogateCapability); got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}

	// 5. Default deviceID when empty is "esi"
	h3 := make(http.Header)
	AddSurrogateCapability(h3, "")
	if got := h3.Get(headerSurrogateCapability); got != `esi="ESI/1.0"` {
		t.Errorf("expected default deviceID esi, got %q", got)
	}
}

func TestProcessor_ReconcileHeaders(t *testing.T) {
	newHeader := func(sc, etag, lm string) http.Header {
		h := make(http.Header)
		if sc != "" {
			h.Set("Surrogate-Control", sc)
		}
		if etag != "" {
			h.Set("ETag", etag)
		}
		if lm != "" {
			h.Set("Last-Modified", lm)
		}
		return h
	}

	t.Run("default PreserveETag false strips ETag and LastModified", func(t *testing.T) {
		p := NewProcessor()
		h := newHeader("ESI/1.0", `"strong-123"`, "Wed, 21 Oct 2015 07:28:00 GMT")
		h.Set("Content-Length", "100")
		h.Set("Content-Type", "text/html")

		res := &Result{
			raw:        []byte("spliced content"),
			SetCookies: []string{"session=xyz; Path=/", "csrf=123; Secure"},
		}

		p.ReconcileHeaders(h, res)

		if h.Get("Surrogate-Control") != "" || h.Get("ETag") != "" || h.Get("Last-Modified") != "" {
			t.Errorf("expected Surrogate-Control, ETag, Last-Modified to be deleted, got: %v", h)
		}
		if got := h.Get("Content-Length"); got != "15" {
			t.Errorf("expected Content-Length to be 15, got %q", got)
		}
		if got := h.Values("Set-Cookie"); len(got) != 2 || got[0] != "session=xyz; Path=/" || got[1] != "csrf=123; Secure" {
			t.Errorf("expected 2 Set-Cookie headers, got %v", got)
		}
		if h.Get("Content-Type") != "text/html" {
			t.Errorf("expected Content-Type to remain intact, got %q", h.Get("Content-Type"))
		}
	})

	t.Run("PreserveETag true weakens strong ETag and keeps weak and LastModified", func(t *testing.T) {
		p := NewProcessor(WithPreserveETag(true))

		h1 := newHeader("ESI/1.0", `"strong-456"`, "Wed, 21 Oct 2015 07:28:00 GMT")
		p.ReconcileHeaders(h1, nil)
		if got := h1.Get("ETag"); got != `W/"strong-456"` {
			t.Errorf("expected weakened ETag W/\"strong-456\", got %q", got)
		}
		if got := h1.Get("Last-Modified"); got != "Wed, 21 Oct 2015 07:28:00 GMT" {
			t.Errorf("expected Last-Modified to be preserved, got %q", got)
		}
		if h1.Get("Surrogate-Control") != "" {
			t.Errorf("expected Surrogate-Control to be deleted")
		}

		h2 := newHeader("", `W/"weak-789"`, "")
		p.ReconcileHeaders(h2, nil)
		if got := h2.Get("ETag"); got != `W/"weak-789"` {
			t.Errorf("expected already-weak ETag unchanged, got %q", got)
		}
	})

	t.Run("omits Content-Length when origin omitted it", func(t *testing.T) {
		p := NewProcessor()
		h := make(http.Header)
		h.Set("Content-Type", "text/html")

		res := &Result{raw: []byte("spliced content")}
		p.ReconcileHeaders(h, res)
		if h.Get("Content-Length") != "" {
			t.Errorf("expected no Content-Length added when originally omitted")
		}
	})

	t.Run("nil resilience", func(t *testing.T) {
		p := NewProcessor()
		p.ReconcileHeaders(nil, nil)

		h := make(http.Header)
		h.Set("ETag", `"test"`)
		h.Set("Content-Length", "50")

		p.ReconcileHeaders(h, nil)
		if h.Get("ETag") != "" {
			t.Errorf("expected ETag stripped")
		}
		if got := h.Get("Content-Length"); got != "50" {
			t.Errorf("expected Content-Length unchanged when res is nil, got %q", got)
		}
	})

	t.Run("origin returns lowercase wire headers parsed via ReadResponse", func(t *testing.T) {
		rawWire := "HTTP/1.1 200 OK\r\n" +
			"surrogate-control: content=\"ESI/1.0\"\r\n" +
			"etag: \"origin-wire-etag\"\r\n" +
			"last-modified: Wed, 21 Oct 2015 07:28:00 GMT\r\n" +
			"content-length: 100\r\n" +
			"content-type: text/html\r\n" +
			"\r\n" +
			"raw body"

		parseWire := func() *http.Response {
			resp, err := http.ReadResponse(bufio.NewReader(strings.NewReader(rawWire)), nil)
			if err != nil {
				t.Fatalf("failed to parse raw wire response: %v", err)
			}
			return resp
		}

		res := &Result{
			raw:        []byte("spliced content"),
			SetCookies: []string{"session=wire123; Path=/"},
		}

		// 1. Default PreserveETag = false
		resp1 := parseWire()
		NewProcessor().ReconcileHeaders(resp1.Header, res)
		if resp1.Header.Get("Surrogate-Control") != "" || resp1.Header.Get("ETag") != "" || resp1.Header.Get("Last-Modified") != "" {
			t.Errorf("expected headers stripped, got %v", resp1.Header)
		}
		if got := resp1.Header.Get("Content-Length"); got != "15" {
			t.Errorf("expected Content-Length updated to 15, got %q", got)
		}
		if got := resp1.Header.Get("Set-Cookie"); got != "session=wire123; Path=/" {
			t.Errorf("expected Set-Cookie appended, got %q", got)
		}

		// 2. PreserveETag = true
		resp2 := parseWire()
		NewProcessor(WithPreserveETag(true)).ReconcileHeaders(resp2.Header, res)
		if got := resp2.Header.Get("ETag"); got != `W/"origin-wire-etag"` {
			t.Errorf("expected weakened ETag, got %q", got)
		}
		if got := resp2.Header.Get("Last-Modified"); got != "Wed, 21 Oct 2015 07:28:00 GMT" {
			t.Errorf("expected Last-Modified preserved, got %q", got)
		}
		if resp2.Header.Get("Surrogate-Control") != "" {
			t.Errorf("expected Surrogate-Control stripped, got %q", resp2.Header.Get("Surrogate-Control"))
		}
	})
}

// --- Category 1: Shared Timeout (src + alt) Tests ---

func TestProcessor_SharedTimeout_FastFail_AltSuccess(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/fast-fail", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Millisecond)
		w.WriteHeader(http.StatusInternalServerError)
	})
	mux.HandleFunc("/alt-success", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(20 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<span>Alt Success Content</span>`))
	})

	runDualFetcher(t, mux, func(t *testing.T, proc *Processor, base string) {
		html := `<div><esi:include src="` + base + `/fast-fail" alt="` + base + `/alt-success" timeout="150ms"><p>Fallback</p></esi:include></div>`
		got, elapsed := processHTML(t, proc, html)
		expected := `<div><span>Alt Success Content</span></div>`
		if got != expected {
			t.Errorf("got %q, want %q", got, expected)
		}
		if elapsed > 150*time.Millisecond {
			t.Errorf("elapsed %v exceeded 150ms budget", elapsed)
		}
	})
}

func TestProcessor_SharedTimeout_SrcTimeout_AltSkipped(t *testing.T) {
	var altCalls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/slow-src", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(200 * time.Millisecond):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<span>Slow Live</span>`))
		case <-r.Context().Done():
		}
	})
	mux.HandleFunc("/alt-should-not-run", func(w http.ResponseWriter, r *http.Request) {
		altCalls.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<span>Alt Run</span>`))
	})

	runDualFetcher(t, mux, func(t *testing.T, proc *Processor, base string) {
		altCalls.Store(0)
		html := `<div><esi:include src="` + base + `/slow-src" alt="` + base + `/alt-should-not-run" timeout="80ms"><p>Inline Fallback</p></esi:include></div>`
		got, elapsed := processHTML(t, proc, html)
		expected := `<div><p>Inline Fallback</p></div>`
		if got != expected {
			t.Errorf("got %q, want %q", got, expected)
		}
		if altCalls.Load() != 0 {
			t.Errorf("expected alt to be skipped, but was called %d times", altCalls.Load())
		}
		if elapsed > 130*time.Millisecond {
			t.Errorf("elapsed %v exceeded reasonable margin over 80ms timeout", elapsed)
		}
	})
}

func TestProcessor_SharedTimeout_AltTimeoutOnRemainingBudget(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/partial-fail", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(40 * time.Millisecond)
		w.WriteHeader(http.StatusInternalServerError)
	})
	mux.HandleFunc("/slow-alt", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(150 * time.Millisecond):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<span>Slow Alt Live</span>`))
		case <-r.Context().Done():
		}
	})

	runDualFetcher(t, mux, func(t *testing.T, proc *Processor, base string) {
		html := `<div><esi:include src="` + base + `/partial-fail" alt="` + base + `/slow-alt" timeout="100ms"><p>Inline Fallback</p></esi:include></div>`
		got, elapsed := processHTML(t, proc, html)
		expected := `<div><p>Inline Fallback</p></div>`
		if got != expected {
			t.Errorf("got %q, want %q", got, expected)
		}
		if elapsed > 150*time.Millisecond {
			t.Errorf("elapsed %v exceeded 100ms budget significantly", elapsed)
		}
	})
}

// --- Category 2: Tree Budget (Nested ESI) Tests ---

func TestProcessor_NestedESI_TimeoutInheritsParentRemainingBudget(t *testing.T) {
	mux := http.NewServeMux()
	var basePrefix string
	var mu sync.Mutex

	mux.HandleFunc("/parent", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(60 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		mu.Lock()
		prefix := basePrefix
		mu.Unlock()
		_, _ = w.Write([]byte(`<div>Parent Header <esi:include src="` + prefix + `/child" timeout="1s"><p>Child Fallback</p></esi:include></div>`))
	})

	mux.HandleFunc("/child", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(70 * time.Millisecond):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<span>Child Live Content</span>`))
		case <-r.Context().Done():
		}
	})

	runDualFetcher(t, mux, func(t *testing.T, proc *Processor, base string) {
		mu.Lock()
		basePrefix = base
		mu.Unlock()

		html := `<div><esi:include src="` + base + `/parent" timeout="100ms"><p>Parent Fallback</p></esi:include></div>`
		got, _ := processHTML(t, proc, html, base+"/index")
		expected := `<div><div>Parent Header <p>Child Fallback</p></div></div>`
		if got != expected {
			t.Errorf("nested timeout not enforced:\ngot:  %s\nwant: %s", got, expected)
		}
	})
}

func TestProcessor_NestedESI_ChildWithAlt_ClampedToParentBudget(t *testing.T) {
	mux := http.NewServeMux()
	var basePrefix string
	var mu sync.Mutex

	mux.HandleFunc("/parent", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(40 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		mu.Lock()
		prefix := basePrefix
		mu.Unlock()
		_, _ = w.Write([]byte(`<div>Parent Header <esi:include src="` + prefix + `/child-fail" alt="` + prefix + `/child-slow-alt" timeout="1s"><p>Child Nested Fallback</p></esi:include></div>`))
	})

	mux.HandleFunc("/child-fail", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Millisecond)
		w.WriteHeader(http.StatusInternalServerError)
	})

	mux.HandleFunc("/child-slow-alt", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(80 * time.Millisecond):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<span>Child Alt Live</span>`))
		case <-r.Context().Done():
		}
	})

	runDualFetcher(t, mux, func(t *testing.T, proc *Processor, base string) {
		mu.Lock()
		basePrefix = base
		mu.Unlock()

		html := `<div><esi:include src="` + base + `/parent" timeout="100ms"><p>Parent Fallback</p></esi:include></div>`
		got, _ := processHTML(t, proc, html, base+"/index")
		expected := `<div><div>Parent Header <p>Child Nested Fallback</p></div></div>`
		if got != expected {
			t.Errorf("nested alt timeout not clamped:\ngot:  %s\nwant: %s", got, expected)
		}
	})
}

// --- Category 3: Singleflight & Cancellation Tests ---

func TestProcessor_SharedSrc_DifferentTimeouts(t *testing.T) {
	var apiCalls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/slow-api", func(w http.ResponseWriter, r *http.Request) {
		apiCalls.Add(1)
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<span>Live API Response</span>`))
	})

	proc := NewProcessor(
		WithMaxTimeout(5*time.Second),
		WithInternalFetcher(HandlerFetcher(mux)),
	)

	html := `<div><esi:include src="/slow-api" timeout="50ms"><p>Fallback 1</p></esi:include>|<esi:include src="/slow-api" timeout="250ms"><p>Fallback 2</p></esi:include></div>`
	got, _ := processHTML(t, proc, html)

	expected := `<div><p>Fallback 1</p>|<span>Live API Response</span></div>`
	if got != expected {
		t.Errorf("got %q, want %q", got, expected)
	}
	if calls := apiCalls.Load(); calls != 1 {
		t.Errorf("expected exactly 1 upstream fetch to /slow-api, got %d", calls)
	}
}

func TestProcessor_Singleflight_AbortsWhenAllListenersTimeout(t *testing.T) {
	var handlerCanceled atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("/hang", func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		handlerCanceled.Store(true)
	})

	proc := NewProcessor(
		WithMaxTimeout(5*time.Second),
		WithInternalFetcher(HandlerFetcher(mux)),
	)

	html := `<div><esi:include src="/hang" timeout="50ms"><p>F1</p></esi:include>|<esi:include src="/hang" timeout="100ms"><p>F2</p></esi:include></div>`
	got, elapsed := processHTML(t, proc, html)

	expected := `<div><p>F1</p>|<p>F2</p></div>`
	if got != expected {
		t.Errorf("got %q, want %q", got, expected)
	}
	if elapsed > 300*time.Millisecond {
		t.Errorf("elapsed %v exceeded 300ms, flight did not abort early", elapsed)
	}
	time.Sleep(20 * time.Millisecond)
	if !handlerCanceled.Load() {
		t.Errorf("expected upstream handler context to be canceled on listener drop")
	}
}

func TestProcessor_SharedSrc_DifferentAltURLs(t *testing.T) {
	var failCalls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/fail", func(w http.ResponseWriter, r *http.Request) {
		failCalls.Add(1)
		time.Sleep(5 * time.Millisecond)
		w.WriteHeader(http.StatusInternalServerError)
	})
	mux.HandleFunc("/alt-alpha", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<span>Alpha Alt</span>`))
	})
	mux.HandleFunc("/alt-beta", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<span>Beta Alt</span>`))
	})

	proc := NewProcessor(
		WithMaxTimeout(5*time.Second),
		WithInternalFetcher(HandlerFetcher(mux)),
	)

	html := `<div><esi:include src="/fail" alt="/alt-alpha"><p>F1</p></esi:include>|<esi:include src="/fail" alt="/alt-beta"><p>F2</p></esi:include></div>`
	got, _ := processHTML(t, proc, html)

	expected := `<div><span>Alpha Alt</span>|<span>Beta Alt</span></div>`
	if got != expected {
		t.Errorf("got %q, want %q", got, expected)
	}
	if calls := failCalls.Load(); calls != 1 {
		t.Errorf("expected exactly 1 upstream fetch to /fail, got %d", calls)
	}
}

func TestProcessor_SharedSrc_DifferentMaxDepths(t *testing.T) {
	var nestedCalls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/nested-root", func(w http.ResponseWriter, r *http.Request) {
		nestedCalls.Add(1)
		time.Sleep(10 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<div>Level 1 <esi:include src="/level-2" /></div>`))
	})
	mux.HandleFunc("/level-2", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<span>Level 2 Content</span>`))
	})

	proc := NewProcessor(
		WithMaxDepth(5),
		WithInternalFetcher(HandlerFetcher(mux)),
	)

	html := `<div><esi:include src="/nested-root" max-depth="1" />|<esi:include src="/nested-root" max-depth="3" /></div>`
	got, _ := processHTML(t, proc, html)

	if !strings.Contains(got, "Level 2 Content") {
		t.Errorf("expected body to contain Level 2 Content for Tag 2, got %q", got)
	}
	if calls := nestedCalls.Load(); calls != 1 {
		t.Errorf("expected exactly 1 fetch to /nested-root via singleflight, got %d", calls)
	}
}

func TestProcessor_ConcurrentUsers_NoDataLeak(t *testing.T) {
	t.Parallel()

	var meCalls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/me", func(w http.ResponseWriter, r *http.Request) {
		meCalls.Add(1)
		cookie, _ := r.Cookie("session")
		user := "anonymous"
		if cookie != nil {
			user = cookie.Value
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = fmt.Fprintf(w, "Profile of %s", user)
	})

	proc := NewProcessor(
		WithInternalFetcher(HandlerFetcher(mux)),
	)

	const numUsers = 20
	var wg sync.WaitGroup
	wg.Add(numUsers)

	results := make([]string, numUsers)

	for i := range numUsers {
		idx := i
		username := fmt.Sprintf("user-%d", idx)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "http://example.com/page", nil)
			req.AddCookie(&http.Cookie{Name: "session", Value: username})

			html := `<div>Welcome: <esi:include src="/me" /></div>`
			res, err := proc.Process(context.Background(), req, []byte(html))
			if err != nil {
				t.Errorf("Process error: %v", err)
				return
			}
			defer res.Release()
			results[idx] = string(res.Body())
		}()
	}

	wg.Wait()

	if calls := meCalls.Load(); calls != numUsers {
		t.Fatalf("expected %d calls to /me (one per user), got %d", numUsers, calls)
	}

	for i, body := range results {
		expected := fmt.Sprintf("<div>Welcome: Profile of user-%d</div>", i)
		if body != expected {
			t.Errorf("user %d data leak or mismatch! Expected %q, got %q", i, expected, body)
		}
	}
}

func TestProcessor_MemoizedBufferNeverMutated(t *testing.T) {
	t.Parallel()

	// 1. Setup a sentinel canary byte slice
	canaryOriginal := []byte("<div><span id='canary'>CANARY_PROTECTED_DATA_12345</span></div>")
	baselineCRC := crc32.ChecksumIEEE(canaryOriginal)
	pristineCopy := bytes.Clone(canaryOriginal)

	var calls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/canary", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(canaryOriginal)
	})

	proc := NewProcessor(
		WithMaxTimeout(5*time.Second),
		WithInternalFetcher(HandlerFetcher(mux)),
	)

	// 2. Document with 5 duplicate tags on the same page
	const count = 5
	var sb strings.Builder
	sb.WriteString("<html><body>")
	for range count {
		sb.WriteString(`<div><esi:include src="/canary" /></div>`)
	}
	sb.WriteString("</body></html>")

	req := httptest.NewRequest(http.MethodGet, "http://localhost/page", nil)
	res, err := proc.Process(context.Background(), req, []byte(sb.String()))
	if err != nil {
		t.Fatalf("Process error: %v", err)
	}
	defer res.Release()

	// 3. CANARY ASSERTION: Verify the source byte slice was not mutated in-place
	currentCRC := crc32.ChecksumIEEE(canaryOriginal)
	if currentCRC != baselineCRC || !bytes.Equal(canaryOriginal, pristineCopy) {
		t.Fatalf("CRITICAL REGRESSION: A function in the ESI pipeline (e.g. spliceFragments) " +
			"mutated the memoized fragment in-place! Memory corruption detected.")
	}

	// 4. Verify all duplicate tags rendered pristine output and exactly 1 fetch occurred
	if calls.Load() != 1 {
		t.Errorf("expected exactly 1 upstream fetch, got %d", calls.Load())
	}
	occurrences := strings.Count(string(res.Body()), "CANARY_PROTECTED_DATA_12345")
	if occurrences != count {
		t.Errorf("expected %d intact occurrences, got %d", count, occurrences)
	}
}
