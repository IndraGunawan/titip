package caddy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	caddymain "github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

func TestCaddyHandler_ESI_MaxResponseSize_Provision(t *testing.T) {
	t.Parallel()

	t.Run("valid size", func(t *testing.T) {
		caddyfile := `titip {
			storage test
			esi {
				enabled true
				max_response_size 5MB
			}
		}`
		h, cleanup := parseAndProvisionHandler(t, caddyfile)
		defer cleanup()
		if h == nil || h.engine == nil {
			t.Fatal("expected engine to be provisioned with valid max_response_size")
		}
	})

	t.Run("invalid size returns error", func(t *testing.T) {
		d := caddyfile.NewTestDispenser(`titip {
			storage test
			esi {
				enabled true
				max_response_size not-a-size
			}
		}`)
		h := &Handler{}
		if err := h.UnmarshalCaddyfile(d); err != nil {
			t.Fatalf("unmarshal failed: %v", err)
		}
		ctx, cancel := caddymain.NewContext(caddymain.Context{Context: context.Background()})
		defer cancel()
		if err := h.Provision(ctx); err == nil {
			t.Fatal("expected Provision to fail with invalid max_response_size")
		}
	})

	t.Run("zero size provisions successfully as unlimited", func(t *testing.T) {
		caddyfile := `titip {
			storage test
			esi {
				enabled true
				max_response_size 0
			}
		}`
		h, cleanup := parseAndProvisionHandler(t, caddyfile)
		defer cleanup()
		if h == nil || h.engine == nil {
			t.Fatal("expected engine to be provisioned with max_response_size 0")
		}
	})
}

func TestCaddyHandler_ESI_MultiRouteResolution(t *testing.T) {
	t.Parallel()
	caddyfileInput := `titip {
		storage test
		esi {
			enabled true
		}
	}`

	h, cleanup := parseAndProvisionHandler(t, caddyfileInput)
	defer cleanup()

	// 1. Define a root Caddy server handler that routes both /page and /api/fragment
	rootServer := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		switch r.URL.Path {
		case "/page":
			w.Header().Set("Content-Type", "text/html")
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `<div>Page Content: <esi:include src="/api/fragment" /></div>`)
		case "/api/fragment":
			w.Header().Set("Content-Type", "text/html")
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `<span>Dynamic Spliced Fragment</span>`)
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = fmt.Fprint(w, `404 Not Found`)
		}
		return nil
	})

	// 2. Next handler represents only Route A's downstream (which ONLY knows /page)
	routeANext := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		if r.URL.Path == "/page" {
			w.Header().Set("Content-Type", "text/html")
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `<div>Page Content: <esi:include src="/api/fragment" /></div>`)
			return nil
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprint(w, `Route A does not know this path`)
		return nil
	})

	req := httptest.NewRequest(http.MethodGet, "http://example.com/page", nil)
	ctx := context.WithValue(req.Context(), caddyhttp.ServerCtxKey, rootServer)
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	if err := h.ServeHTTP(rec, req, routeANext); err != nil {
		t.Fatalf("serve failed: %v", err)
	}

	expected := `<div>Page Content: <span>Dynamic Spliced Fragment</span></div>`
	if rec.Body.String() != expected {
		t.Fatalf("ESI multi-route splicing failed.\nExpected: %s\nGot:      %s", expected, rec.Body.String())
	}
}

func TestCaddyHandler_ESI_ConcurrentReplacerSafety(t *testing.T) {
	t.Parallel()
	caddyfileInput := `titip {
		storage test
		esi {
			enabled true
		}
	}`

	h, cleanup := parseAndProvisionHandler(t, caddyfileInput)
	defer cleanup()

	rootServer := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		switch r.URL.Path {
		case "/multi-frag":
			w.Header().Set("Content-Type", "text/html")
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `<doc><esi:include src="/f1" /><esi:include src="/f2" /><esi:include src="/f3" /><esi:include src="/f4" /></doc>`)
		case "/f1":
			w.Header().Set("Content-Type", "text/html")
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `[F1]`)
		case "/f2":
			w.Header().Set("Content-Type", "text/html")
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `[F2]`)
		case "/f3":
			w.Header().Set("Content-Type", "text/html")
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `[F3]`)
		case "/f4":
			w.Header().Set("Content-Type", "text/html")
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `[F4]`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
		return nil
	})

	var wg sync.WaitGroup
	concurrentRequests := 20
	for range concurrentRequests {
		wg.Go(func() {
			req := httptest.NewRequest(http.MethodGet, "http://example.com/multi-frag", nil)
			repl := caddymain.NewReplacer()
			ctx := context.WithValue(req.Context(), caddymain.ReplacerCtxKey, repl)
			ctx = context.WithValue(ctx, caddyhttp.ServerCtxKey, rootServer)
			req = req.WithContext(ctx)

			rec := httptest.NewRecorder()
			if err := h.ServeHTTP(rec, req, rootServer); err != nil {
				t.Errorf("serve failed: %v", err)
				return
			}

			expected := `<doc>[F1][F2][F3][F4]</doc>`
			if rec.Body.String() != expected {
				t.Errorf("unexpected body: got %q, want %q", rec.Body.String(), expected)
			}
		})
	}
	wg.Wait()
}

func TestCaddyHandler_ESI_Subrequest404_FallbackToOutbound(t *testing.T) {
	t.Parallel()

	var outboundCalls atomic.Int32
	extServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/remote-alb-part" {
			outboundCalls.Add(1)
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `<span>From ALB Upstream</span>`)
			return
		}
		http.NotFound(w, r)
	}))
	defer extServer.Close()

	caddyfileInput := `titip {
		storage test
		esi {
			enabled true
			block_private_ips false
		}
	}`

	h, cleanup := parseAndProvisionHandler(t, caddyfileInput)
	defer cleanup()

	// Root server only knows /local-part, but returns 404 for /remote-alb-part
	rootServer := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		switch r.URL.Path {
		case "/local-part":
			w.Header().Set("Content-Type", "text/html")
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `<span>Local In-Process</span>`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
		return nil
	})

	routeNext := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		if r.URL.Path == "/hybrid-page" {
			w.Header().Set("Content-Type", "text/html")
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `<div><esi:include src="/local-part" /> + <esi:include src="/remote-alb-part" /></div>`)
			return nil
		}
		w.WriteHeader(http.StatusNotFound)
		return nil
	})

	extURL := strings.TrimPrefix(extServer.URL, "http://")
	req := httptest.NewRequest(http.MethodGet, "http://"+extURL+"/hybrid-page", nil)
	ctx := context.WithValue(req.Context(), caddyhttp.ServerCtxKey, rootServer)
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	if err := h.ServeHTTP(rec, req, routeNext); err != nil {
		t.Fatalf("serve failed: %v", err)
	}

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	expected := `<div><span>Local In-Process</span> + <span>From ALB Upstream</span></div>`
	if rec.Body.String() != expected {
		t.Fatalf("unexpected body.\nExpected: %s\nGot:      %s", expected, rec.Body.String())
	}

	if outboundCalls.Load() != 1 {
		t.Errorf("expected 1 outbound call to ALB for 404 subrequest, got %d", outboundCalls.Load())
	}
}
