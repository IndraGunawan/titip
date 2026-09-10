package esi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// ErrFallbackToHTTP is returned by an InternalFetcher to signal that the processor should resolve this include via outbound HTTP.
	ErrFallbackToHTTP = errors.New("esi: fallback to outbound http")
)

// InternalFetcherFunc defines the signature for resolving in-process ESI includes.
type InternalFetcherFunc func(ctx context.Context, targetPath string, r *http.Request) ([]byte, http.Header, error)

// config defines the configuration options for Edge Side Includes (ESI) processing.
type config struct {
	headerRequired                 bool
	internalFetcher                InternalFetcherFunc
	maxDepth                       uint32
	maxTimeout                     time.Duration
	maxConcurrentRequests          int
	allowPrivateIPs                bool
	allowedHosts                   []string
	allowPrivateIPsForAllowedHosts bool
	maxResponseSize                int64
	disableForwardCookies          bool
	preserveETag                   bool
	includeErrorMarker             string

	logger     *slog.Logger
	metrics    prometheus.Registerer
	httpClient *http.Client
}

// Option configures ESI processor parameters.
type Option func(*config)

// WithHeaderRequired configures whether ESI is processed only when Surrogate-Control is present.
func WithHeaderRequired(required bool) Option {
	return func(c *config) {
		c.headerRequired = required
	}
}

// WithInternalFetcher configures a custom in-process handler for resolving local fragment subrequests.
func WithInternalFetcher(fetcher InternalFetcherFunc) Option {
	return func(c *config) {
		c.internalFetcher = fetcher
	}
}

// WithMaxDepth configures the maximum global recursion depth for nested includes (default: 3).
func WithMaxDepth(depth uint32) Option {
	return func(c *config) {
		if depth > 0 {
			c.maxDepth = depth
		}
	}
}

// WithMaxTimeout configures the maximum fetch timeout for resolving an include fragment (default: 30s).
func WithMaxTimeout(timeout time.Duration) Option {
	return func(c *config) {
		if timeout > 0 {
			c.maxTimeout = timeout
		}
	}
}

// WithMaxConcurrentRequests caps concurrent fragment fetch goroutines per document (default: 8).
func WithMaxConcurrentRequests(limit int) Option {
	return func(c *config) {
		if limit > 0 {
			c.maxConcurrentRequests = limit
		}
	}
}

// WithAllowPrivateIPs configures whether SSRF blocking permits dials to private/loopback CIDRs (default: false = blocked).
func WithAllowPrivateIPs(allow bool) Option {
	return func(c *config) {
		c.allowPrivateIPs = allow
	}
}

// WithAllowedHosts restricts external HTTP includes to matching domain patterns (default: empty = all public).
func WithAllowedHosts(hosts ...string) Option {
	return func(c *config) {
		c.allowedHosts = hosts
	}
}

// WithAllowPrivateIPsForAllowedHosts permits private IPs specifically for explicitly allowed hosts.
func WithAllowPrivateIPsForAllowedHosts(allow bool) Option {
	return func(c *config) {
		c.allowPrivateIPsForAllowedHosts = allow
	}
}

// WithMaxResponseSize caps the maximum allowed fragment body size in bytes (default: 10MB, 0 = unlimited).
func WithMaxResponseSize(size int64) Option {
	return func(c *config) {
		if size >= 0 {
			c.maxResponseSize = size
		}
	}
}

// WithDisableForwardCookies configures whether Set-Cookie headers from subrequests are forwarded to the client (default: false = forwarded).
func WithDisableForwardCookies(disable bool) Option {
	return func(c *config) {
		c.disableForwardCookies = disable
	}
}

// WithIncludeErrorMarker configures an HTML placeholder rendered on unhandled fetch errors.
func WithIncludeErrorMarker(marker string) Option {
	return func(c *config) {
		c.includeErrorMarker = marker
	}
}

// WithPreserveETag configures whether downstream ETag (weakened) and Last-Modified are preserved on ESI documents
// (default: false = headers stripped, downstream 304 bypassed).
func WithPreserveETag(preserve bool) Option {
	return func(c *config) {
		c.preserveETag = preserve
	}
}

// WithLogger configures a custom slog.Logger for ESI processing.
func WithLogger(logger *slog.Logger) Option {
	return func(c *config) {
		if logger != nil {
			c.logger = logger
		}
	}
}

// WithMetrics registers ESI telemetry collectors with the provided Prometheus Registerer.
func WithMetrics(reg prometheus.Registerer) Option {
	return func(c *config) {
		if reg != nil {
			c.metrics = reg
		}
	}
}

// WithHTTPClient configures a custom http.Client for outbound fragment fetching.
func WithHTTPClient(client *http.Client) Option {
	return func(c *config) {
		if client != nil {
			c.httpClient = client
		}
	}
}

// HandlerFetcher adapts any standard http.Handler into an InternalFetcherFunc for in-process subrequests.
func HandlerFetcher(router http.Handler) InternalFetcherFunc {
	return func(ctx context.Context, targetPath string, r *http.Request) ([]byte, http.Header, error) {
		if router == nil {
			return nil, nil, errors.New("esi: router is nil")
		}

		parsedURL, err := url.Parse(targetPath)
		if err != nil {
			return nil, nil, fmt.Errorf("esi: parse url: %w", err)
		}

		subReq := &http.Request{
			Method:     http.MethodGet,
			URL:        parsedURL,
			RequestURI: targetPath,
			Header:     r.Header.Clone(),
			Host:       r.Host,
			RemoteAddr: "127.0.0.1:10000",
			Proto:      r.Proto,
			ProtoMajor: r.ProtoMajor,
			ProtoMinor: r.ProtoMinor,
			Body:       http.NoBody,
		}
		subReq.Header.Set("Accept-Encoding", "identity")
		if r.Trailer != nil {
			subReq.Trailer = r.Trailer.Clone()
		}
		subReq = subReq.WithContext(ctx)

		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, subReq)

		if rec.Code == http.StatusNotFound {
			return nil, nil, ErrFallbackToHTTP
		}
		if rec.Code >= 400 {
			return nil, rec.Header().Clone(), fmt.Errorf("subrequest returned status %d", rec.Code)
		}

		return bytes.Clone(rec.Body.Bytes()), rec.Header().Clone(), nil
	}
}
