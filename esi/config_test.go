package esi

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestConfig_Options(t *testing.T) {
	cfg := Config{
		MaxDepth:              3,
		MaxTimeout:            30 * time.Second,
		MaxConcurrentRequests: 8,
		MaxResponseSize:       10 * 1024 * 1024,
	}

	dummyFetcher := func(ctx context.Context, targetPath string, r *http.Request) ([]byte, http.Header, error) {
		return nil, nil, nil
	}

	opts := []Option{
		WithHeaderRequired(true),
		WithInternalFetcher(dummyFetcher),
		WithMaxDepth(5),
		WithMaxTimeout(10 * time.Second),
		WithMaxConcurrentRequests(16),
		WithAllowPrivateIPs(true),
		WithAllowedHosts("cdn.example.com", "*.partner.com"),
		WithAllowPrivateIPsForAllowedHosts(true),
		WithMaxResponseSize(2048),
		WithDisableForwardCookies(true),
		WithIncludeErrorMarker("<!-- error placeholder -->"),
		WithPreserveETag(true),
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	if !cfg.HeaderRequired {
		t.Errorf("expected HeaderRequired to be true")
	}
	if cfg.InternalFetcher == nil {
		t.Errorf("expected InternalFetcher to be non-nil")
	}
	if cfg.MaxDepth != 5 {
		t.Errorf("expected MaxDepth 5, got %d", cfg.MaxDepth)
	}
	if cfg.MaxTimeout != 10*time.Second {
		t.Errorf("expected MaxTimeout 10s, got %v", cfg.MaxTimeout)
	}
	if cfg.MaxConcurrentRequests != 16 {
		t.Errorf("expected MaxConcurrentRequests 16, got %d", cfg.MaxConcurrentRequests)
	}
	if !cfg.AllowPrivateIPs {
		t.Errorf("expected AllowPrivateIPs to be true")
	}
	if len(cfg.AllowedHosts) != 2 || cfg.AllowedHosts[0] != "cdn.example.com" || cfg.AllowedHosts[1] != "*.partner.com" {
		t.Errorf("unexpected AllowedHosts: %v", cfg.AllowedHosts)
	}
	if !cfg.AllowPrivateIPsForAllowedHosts {
		t.Errorf("expected AllowPrivateIPsForAllowedHosts to be true")
	}
	if cfg.MaxResponseSize != 2048 {
		t.Errorf("expected MaxResponseSize 2048, got %d", cfg.MaxResponseSize)
	}
	if !cfg.DisableForwardCookies {
		t.Errorf("expected DisableForwardCookies to be true")
	}
	if cfg.IncludeErrorMarker != "<!-- error placeholder -->" {
		t.Errorf("unexpected IncludeErrorMarker: %s", cfg.IncludeErrorMarker)
	}
	if !cfg.PreserveETag {
		t.Errorf("expected PreserveETag to be true")
	}
}

func TestConfig_Options_BoundaryGuards(t *testing.T) {
	cfg := Config{
		MaxDepth:              3,
		MaxTimeout:            30 * time.Second,
		MaxConcurrentRequests: 8,
		MaxResponseSize:       10 * 1024 * 1024,
	}

	// Applying invalid / zero values should preserve existing configuration
	WithMaxDepth(0)(&cfg)
	if cfg.MaxDepth != 3 {
		t.Errorf("expected MaxDepth preserved at 3, got %d", cfg.MaxDepth)
	}

	WithMaxTimeout(0)(&cfg)
	if cfg.MaxTimeout != 30*time.Second {
		t.Errorf("expected MaxTimeout preserved at 30s, got %v", cfg.MaxTimeout)
	}

	WithMaxTimeout(-5 * time.Second)(&cfg)
	if cfg.MaxTimeout != 30*time.Second {
		t.Errorf("expected MaxTimeout preserved at 30s, got %v", cfg.MaxTimeout)
	}

	WithMaxConcurrentRequests(0)(&cfg)
	if cfg.MaxConcurrentRequests != 8 {
		t.Errorf("expected MaxConcurrentRequests preserved at 8, got %d", cfg.MaxConcurrentRequests)
	}

	WithMaxConcurrentRequests(-1)(&cfg)
	if cfg.MaxConcurrentRequests != 8 {
		t.Errorf("expected MaxConcurrentRequests preserved at 8, got %d", cfg.MaxConcurrentRequests)
	}

	WithMaxResponseSize(-10)(&cfg)
	if cfg.MaxResponseSize != 10*1024*1024 {
		t.Errorf("expected MaxResponseSize preserved, got %d", cfg.MaxResponseSize)
	}

	// 0 is valid for MaxResponseSize (unlimited)
	WithMaxResponseSize(0)(&cfg)
	if cfg.MaxResponseSize != 0 {
		t.Errorf("expected MaxResponseSize 0 (unlimited), got %d", cfg.MaxResponseSize)
	}
}

func TestHandlerFetcher_Success(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/fragment", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept-Encoding") != "identity" {
			t.Errorf("expected Accept-Encoding: identity, got %q", r.Header.Get("Accept-Encoding"))
		}
		if r.Header.Get("X-Custom-Req") != "hello" {
			t.Errorf("expected X-Custom-Req: hello, got %q", r.Header.Get("X-Custom-Req"))
		}
		w.Header().Set("X-Fragment-Header", "success")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("<p>Included Fragment</p>"))
	})

	fetcher := HandlerFetcher(mux)

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://localhost/page", nil)
	req.Header.Set("X-Custom-Req", "hello")
	req.Trailer = http.Header{"X-Trailer": []string{"val"}}

	body, header, err := fetcher(context.Background(), "/fragment", req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(body) != "<p>Included Fragment</p>" {
		t.Errorf("unexpected body: %s", string(body))
	}
	if header.Get("X-Fragment-Header") != "success" {
		t.Errorf("expected X-Fragment-Header: success, got %q", header.Get("X-Fragment-Header"))
	}
}

func TestHandlerFetcher_Errors(t *testing.T) {
	t.Run("nil router", func(t *testing.T) {
		fetcher := HandlerFetcher(nil)
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/page", nil)
		_, _, err := fetcher(context.Background(), "/frag", req)
		if err == nil || err.Error() != "titip: esi: router is nil" {
			t.Fatalf("expected router is nil error, got: %v", err)
		}
	})

	t.Run("invalid url parse", func(t *testing.T) {
		mux := http.NewServeMux()
		fetcher := HandlerFetcher(mux)
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/page", nil)
		_, _, err := fetcher(context.Background(), "http://[::1]:namedport/frag", req)
		if err == nil {
			t.Fatalf("expected URL parse error, got nil")
		}
	})

	t.Run("404 fallback to http", func(t *testing.T) {
		mux := http.NewServeMux() // empty, returns 404
		fetcher := HandlerFetcher(mux)
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/page", nil)
		_, _, err := fetcher(context.Background(), "/not-found", req)
		if !errors.Is(err, ErrFallbackToHTTP) {
			t.Fatalf("expected ErrFallbackToHTTP, got: %v", err)
		}
	})

	t.Run("status 500 error", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("/error", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Error-Reason", "database-down")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("internal error"))
		})
		fetcher := HandlerFetcher(mux)
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/page", nil)
		_, header, err := fetcher(context.Background(), "/error", req)
		if err == nil || err.Error() != "subrequest returned status 500" {
			t.Fatalf("expected status 500 error, got: %v", err)
		}
		if header.Get("X-Error-Reason") != "database-down" {
			t.Errorf("expected X-Error-Reason preserved, got %q", header.Get("X-Error-Reason"))
		}
	})
}
