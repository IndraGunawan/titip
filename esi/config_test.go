package esi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestConfig_Options(t *testing.T) {
	cfg := config{
		maxDepth:              3,
		maxTimeout:            30 * time.Second,
		maxConcurrentRequests: 8,
		maxResponseSize:       10 * 1024 * 1024,
	}

	dummyFetcher := func(ctx context.Context, targetPath string, r *http.Request) ([]byte, http.Header, error) {
		return nil, nil, nil
	}

	logger := slog.Default()
	httpClient := &http.Client{}
	reg := prometheus.NewRegistry()
	opts := []Option{
		WithHeaderRequired(),
		WithInternalFetcher(dummyFetcher),
		WithMaxDepth(5),
		WithMaxTimeout(10 * time.Second),
		WithMaxConcurrentRequests(16),
		WithAllowPrivateIPs(),
		WithAllowedHosts("cdn.example.com", "*.partner.com"),
		WithAllowPrivateIPsForAllowedHosts(),
		WithMaxResponseSize(2048),
		WithoutForwardCookies(),
		WithIncludeErrorMarker("<!-- error placeholder -->"),
		WithPreserveETag(),
		WithMetrics(reg),
		WithLogger(logger),
		WithHTTPClient(httpClient),
	}

	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			t.Fatalf("unexpected option error: %v", err)
		}
	}

	if !cfg.headerRequired {
		t.Errorf("expected headerRequired to be true")
	}
	if cfg.internalFetcher == nil {
		t.Errorf("expected internalFetcher to be non-nil")
	}
	if cfg.maxDepth != 5 {
		t.Errorf("expected maxDepth 5, got %d", cfg.maxDepth)
	}
	if cfg.maxTimeout != 10*time.Second {
		t.Errorf("expected maxTimeout 10s, got %v", cfg.maxTimeout)
	}
	if cfg.maxConcurrentRequests != 16 {
		t.Errorf("expected maxConcurrentRequests 16, got %d", cfg.maxConcurrentRequests)
	}
	if !cfg.allowPrivateIPs {
		t.Errorf("expected allowPrivateIPs to be true")
	}
	if len(cfg.allowedHosts) != 2 || cfg.allowedHosts[0] != "cdn.example.com" || cfg.allowedHosts[1] != "*.partner.com" {
		t.Errorf("unexpected allowedHosts: %v", cfg.allowedHosts)
	}
	if !cfg.allowPrivateIPsForAllowedHosts {
		t.Errorf("expected allowPrivateIPsForAllowedHosts to be true")
	}
	if cfg.maxResponseSize != 2048 {
		t.Errorf("expected maxResponseSize 2048, got %d", cfg.maxResponseSize)
	}
	if !cfg.disableForwardCookies {
		t.Errorf("expected disableForwardCookies to be true")
	}
	if cfg.includeErrorMarker != "<!-- error placeholder -->" {
		t.Errorf("unexpected includeErrorMarker: %s", cfg.includeErrorMarker)
	}
	if !cfg.preserveETag {
		t.Errorf("expected preserveETag to be true")
	}
	if cfg.metrics != reg {
		t.Errorf("expected metrics registerer to be set")
	}
	if cfg.logger != logger {
		t.Errorf("expected logger to be set")
	}
	if cfg.httpClient != httpClient {
		t.Errorf("expected httpClient to be set")
	}
}

func TestNewProcessor_InvalidOption(t *testing.T) {
	tests := []struct {
		name        string
		opt         Option
		errContains string
	}{
		{
			name:        "MaxDepth zero",
			opt:         WithMaxDepth(0),
			errContains: "max depth must be greater than 0",
		},
		{
			name:        "MaxTimeout zero",
			opt:         WithMaxTimeout(0),
			errContains: "max timeout must be positive",
		},
		{
			name:        "MaxTimeout negative",
			opt:         WithMaxTimeout(-5 * time.Second),
			errContains: "max timeout must be positive",
		},
		{
			name:        "MaxConcurrentRequests zero",
			opt:         WithMaxConcurrentRequests(0),
			errContains: "max concurrent requests must be positive",
		},
		{
			name:        "MaxConcurrentRequests negative",
			opt:         WithMaxConcurrentRequests(-1),
			errContains: "max concurrent requests must be positive",
		},
		{
			name:        "MaxResponseSize negative",
			opt:         WithMaxResponseSize(-10),
			errContains: "max response size cannot be negative",
		},
		{
			name:        "AllowedHosts empty string",
			opt:         WithAllowedHosts(""),
			errContains: "allowed host cannot be empty",
		},
		{
			name:        "AllowedHosts whitespace only",
			opt:         WithAllowedHosts("   "),
			errContains: "allowed host cannot be empty",
		},
		{
			name:        "InternalFetcher nil",
			opt:         WithInternalFetcher(nil),
			errContains: "internal fetcher cannot be nil",
		},
		{
			name:        "Logger nil",
			opt:         WithLogger(nil),
			errContains: "logger cannot be nil",
		},
		{
			name:        "Metrics nil",
			opt:         WithMetrics(nil),
			errContains: "metrics registerer cannot be nil",
		},
		{
			name:        "HTTPClient nil",
			opt:         WithHTTPClient(nil),
			errContains: "http client cannot be nil",
		},
		{
			name:        "Nil option",
			opt:         nil,
			errContains: "option cannot be nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewProcessor(tt.opt)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.errContains)
			}
			if !errors.Is(err, ErrInvalidOption) {
				t.Errorf("expected error to wrap ErrInvalidOption, got: %v", err)
			}
			if !strings.Contains(err.Error(), tt.errContains) {
				t.Errorf("expected error message to contain %q, got: %v", tt.errContains, err)
			}
		})
	}
}

func TestNewProcessor_ValidOptions(t *testing.T) {
	proc, err := NewProcessor(WithMaxResponseSize(0))
	if err != nil {
		t.Fatalf("expected WithMaxResponseSize(0) to succeed (unlimited), got: %v", err)
	}
	if proc.config.maxResponseSize != 0 {
		t.Errorf("expected maxResponseSize 0, got %d", proc.config.maxResponseSize)
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
		if err == nil || err.Error() != "esi: router is nil" {
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
