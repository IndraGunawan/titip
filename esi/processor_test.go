package esi

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

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

	parentBody := []byte(`<html><body><div id="cart"><esi:include src="/api/cart" /></div><div id="user"><esi:include src="/api/user" /></div></body></html>`)
	hasESI, frags := Scan(parentBody)
	if !hasESI || len(frags) != 2 {
		t.Fatalf("expected 2 fragments, got %d", len(frags))
	}

	req := httptest.NewRequest(http.MethodGet, "http://localhost/dashboard", nil)
	res, err := proc.Process(context.Background(), req, parentBody, frags)
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

	parentBody := []byte(`<div><esi:include src="` + ts.URL + `/fragment" /></div>`)
	hasESI, frags := Scan(parentBody)
	if !hasESI || len(frags) != 1 {
		t.Fatalf("expected 1 fragment, got %d", len(frags))
	}

	req := httptest.NewRequest(http.MethodGet, "http://example.com/page", nil)
	res, err := proc.Process(context.Background(), req, parentBody, frags)
	if err != nil {
		t.Fatalf("process error: %v", err)
	}
	defer res.Release()

	expected := `<div>outbound fragment</div>`
	if string(res.Body()) != expected {
		t.Errorf("got %q, want %q", string(res.Body()), expected)
	}
}

func TestProcessor_FallbackOnErrorContinue(t *testing.T) {
	proc := NewProcessor(WithMaxTimeout(1 * time.Second))

	// An invalid/non-routable URL with onerror="continue" should be deleted silently
	parentBody := []byte(`<div>Before <esi:include src="http://192.0.2.1/fail" onerror="continue" timeout="50" /> After</div>`)
	hasESI, frags := Scan(parentBody)
	if !hasESI || len(frags) != 1 {
		t.Fatalf("expected 1 fragment, got %d", len(frags))
	}

	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	res, err := proc.Process(context.Background(), req, parentBody, frags)
	if err != nil {
		t.Fatalf("process error: %v", err)
	}
	defer res.Release()

	expected := `<div>Before  After</div>`
	if string(res.Body()) != expected {
		t.Errorf("got %q, want %q", string(res.Body()), expected)
	}
}

func TestProcessor_FallbackErrorMarker(t *testing.T) {
	proc := NewProcessor(
		WithMaxTimeout(1*time.Second),
		WithIncludeErrorMarker("<!-- ESI ERROR -->"),
	)

	parentBody := []byte(`<div>Before <esi:include src="http://192.0.2.1/fail" timeout="50" /> After</div>`)
	hasESI, frags := Scan(parentBody)
	if !hasESI || len(frags) != 1 {
		t.Fatalf("expected 1 fragment, got %d", len(frags))
	}

	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	res, err := proc.Process(context.Background(), req, parentBody, frags)
	if err != nil {
		t.Fatalf("process error: %v", err)
	}
	defer res.Release()

	expected := `<div>Before <!-- ESI ERROR --> After</div>`
	if string(res.Body()) != expected {
		t.Errorf("got %q, want %q", string(res.Body()), expected)
	}
}

func TestProcessor_CircularInclude(t *testing.T) {
	proc := NewProcessor(WithMaxTimeout(2 * time.Second))

	// URL matches parent path /circular
	parentBody := []byte(`<div><esi:include src="/circular" onerror="continue" /></div>`)
	hasESI, frags := Scan(parentBody)
	if !hasESI || len(frags) != 1 {
		t.Fatalf("expected 1 fragment")
	}

	req := httptest.NewRequest(http.MethodGet, "http://example.com/circular", nil)
	res, err := proc.Process(context.Background(), req, parentBody, frags)
	if err != nil {
		t.Fatalf("process error: %v", err)
	}
	defer res.Release()

	expected := `<div></div>`
	if string(res.Body()) != expected {
		t.Errorf("got %q, want %q", string(res.Body()), expected)
	}
}

func TestProcessor_MaxRecursionDepth(t *testing.T) {
	var callCount atomic.Int32
	var handler http.HandlerFunc
	handler = func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<div>nested <esi:include src="/nested" onerror="continue" /></div>`))
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/nested", handler)

	proc := NewProcessor(
		WithMaxDepth(2),
		WithMaxTimeout(2*time.Second),
		WithInternalFetcher(HandlerFetcher(mux)),
	)

	parentBody := []byte(`<div><esi:include src="/nested" onerror="continue" /></div>`)
	hasESI, frags := Scan(parentBody)
	if !hasESI {
		t.Fatalf("expected fragments")
	}

	req := httptest.NewRequest(http.MethodGet, "http://localhost/page", nil)
	res, err := proc.Process(context.Background(), req, parentBody, frags)
	if err != nil {
		t.Fatalf("process error: %v", err)
	}
	defer res.Release()

	// Recursion depth is capped at 2
	if callCount.Load() > 3 {
		t.Errorf("expected recursion depth <= 2, got callCount=%d", callCount.Load())
	}
}

func TestProcessor_SSRFBlocked(t *testing.T) {
	proc := NewProcessor(
		WithAllowPrivateIPs(false), // SSRF protection active
		WithMaxTimeout(1*time.Second),
	)

	// Dialing private IP (127.0.0.1) should be blocked by SSRF
	parentBody := []byte(`<div><esi:include src="http://127.0.0.1:9999/secret" onerror="continue" /></div>`)
	hasESI, frags := Scan(parentBody)
	if !hasESI {
		t.Fatalf("expected fragments")
	}

	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	res, err := proc.Process(context.Background(), req, parentBody, frags)
	if err != nil {
		t.Fatalf("process error: %v", err)
	}
	defer res.Release()

	// Since onerror="continue", tag is removed cleanly
	expected := `<div></div>`
	if string(res.Body()) != expected {
		t.Errorf("got %q, want %q", string(res.Body()), expected)
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

	parentBody := []byte(`<div><esi:include src="/panic-test" /></div>`)
	hasESI, frags := Scan(parentBody)
	if !hasESI {
		t.Fatalf("expected fragments")
	}

	req := httptest.NewRequest(http.MethodGet, "http://localhost/", nil)
	res, err := proc.Process(context.Background(), req, parentBody, frags)
	if err != nil {
		t.Fatalf("process error: %v", err)
	}
	defer res.Release()

	expected := `<div>PANIC_RECOVERED</div>`
	if string(res.Body()) != expected {
		t.Errorf("got %q, want %q", string(res.Body()), expected)
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
	hasESI, frags := Scan(parentBody)
	if !hasESI {
		t.Fatalf("expected fragments")
	}

	req := httptest.NewRequest(http.MethodGet, "http://localhost/", nil)
	res, err := proc.Process(context.Background(), req, parentBody, frags)
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
	res, err := proc.Process(context.Background(), nil, parentBody, nil)
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
	res, err := proc.Process(context.Background(), nil, parent, frags)
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

func TestProcessor_ProcessDocument(t *testing.T) {
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
	res1, err := proc.ProcessDocument(context.Background(), req, docWithESI)
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
	res2, err := proc.ProcessDocument(context.Background(), req, docWithoutESI)
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

func TestProcessor_IsEligible_And_HasSurrogateControl(t *testing.T) {
	// Test HasSurrogateControl
	if HasSurrogateControl(nil) {
		t.Errorf("expected false for nil header")
	}

	h := make(http.Header)
	if HasSurrogateControl(h) {
		t.Errorf("expected false for empty header")
	}

	h.Set("Surrogate-Control", "content=\"ESI/1.0\"")
	if !HasSurrogateControl(h) {
		t.Errorf("expected true for Surrogate-Control with ESI/1.0")
	}

	h.Set("Surrogate-Control", "max-age=3600")
	if HasSurrogateControl(h) {
		t.Errorf("expected false for Surrogate-Control without ESI/1.0")
	}

	// Test p.IsEligible with HeaderRequired(false) - default
	procDefault := NewProcessor()
	if !procDefault.IsEligible(nil) {
		t.Errorf("expected true for default processor with nil header")
	}
	if !procDefault.IsEligible(h) {
		t.Errorf("expected true for default processor when header_required=false")
	}

	// Test p.IsEligible with HeaderRequired(true)
	procRequired := NewProcessor(WithHeaderRequired(true))
	if procRequired.IsEligible(nil) {
		t.Errorf("expected false for header_required=true with nil header")
	}
	if procRequired.IsEligible(h) {
		t.Errorf("expected false for header_required=true without ESI/1.0 in Surrogate-Control")
	}
	h.Set("Surrogate-Control", "abc, content=\"ESI/1.0\", max-age=60")
	if !procRequired.IsEligible(h) {
		t.Errorf("expected true for header_required=true with ESI/1.0 in Surrogate-Control")
	}
}

func TestProcessor_SurrogateCapability(t *testing.T) {
	// 1. Nil header safe
	AddSurrogateCapability(nil, "titip")
	if HasSurrogateCapability(nil, "titip") {
		t.Errorf("expected false for nil header")
	}

	// 2. Empty header sets capability
	h := make(http.Header)
	AddSurrogateCapability(h, "titip")
	if got := h.Get(HeaderSurrogateCapability); got != `titip="ESI/1.0"` {
		t.Errorf("expected %q, got %q", `titip="ESI/1.0"`, got)
	}
	if !HasSurrogateCapability(h, "titip") {
		t.Errorf("expected HasSurrogateCapability(h, \"titip\") to be true")
	}
	if HasSurrogateCapability(h, "akamai") {
		t.Errorf("expected HasSurrogateCapability(h, \"akamai\") to be false")
	}
	if !HasSurrogateCapability(h, "") {
		t.Errorf("expected generic HasSurrogateCapability(h, \"\") to be true")
	}

	// 3. Already present token is not duplicated
	AddSurrogateCapability(h, "titip")
	if got := h.Get(HeaderSurrogateCapability); got != `titip="ESI/1.0"` {
		t.Errorf("expected no duplicate, got %q", got)
	}

	// 4. Appends to existing capability from downstream
	h2 := make(http.Header)
	h2.Set(HeaderSurrogateCapability, `cdn="ESI/1.0"`)
	AddSurrogateCapability(h2, "titip")
	expected := `cdn="ESI/1.0", titip="ESI/1.0"`
	if got := h2.Get(HeaderSurrogateCapability); got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}

	// 5. Default deviceID when empty is "esi"
	h3 := make(http.Header)
	proc := NewProcessor()
	proc.AddSurrogateCapability(h3, "")
	if got := h3.Get(HeaderSurrogateCapability); got != `esi="ESI/1.0"` {
		t.Errorf("expected default deviceID esi, got %q", got)
	}
	if !proc.HasSurrogateCapability(h3, "esi") {
		t.Errorf("expected proc.HasSurrogateCapability to be true")
	}
}


