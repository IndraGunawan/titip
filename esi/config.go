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
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// ErrInvalidOption is returned when an invalid configuration option is provided to NewProcessor.
	ErrInvalidOption = errors.New("esi: invalid option")

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
type Option func(*config) error

// WithHeaderRequired requires origin responses to explicitly advertise ESI capability
// via Surrogate-Control: content="ESI/1.0" before fragment tags are processed.
// By default, ESI tags are processed unconditionally.
func WithHeaderRequired() Option {
	return func(c *config) error {
		c.headerRequired = true
		return nil
	}
}

// WithInternalFetcher configures a custom in-process handler for resolving local fragment subrequests.
func WithInternalFetcher(fetcher InternalFetcherFunc) Option {
	return func(c *config) error {
		if fetcher == nil {
			return fmt.Errorf("%w: internal fetcher cannot be nil", ErrInvalidOption)
		}
		c.internalFetcher = fetcher
		return nil
	}
}

// WithMaxDepth configures the maximum global recursion depth for nested includes (default: 3).
func WithMaxDepth(depth uint32) Option {
	return func(c *config) error {
		if depth == 0 {
			return fmt.Errorf("%w: max depth must be greater than 0, got %d", ErrInvalidOption, depth)
		}
		c.maxDepth = depth
		return nil
	}
}

// WithMaxTimeout configures the maximum fetch timeout for resolving an include fragment (default: 30s).
func WithMaxTimeout(timeout time.Duration) Option {
	return func(c *config) error {
		if timeout <= 0 {
			return fmt.Errorf("%w: max timeout must be positive, got %v", ErrInvalidOption, timeout)
		}
		c.maxTimeout = timeout
		return nil
	}
}

// WithMaxConcurrentRequests caps concurrent fragment fetch goroutines per document (default: 8).
func WithMaxConcurrentRequests(limit int) Option {
	return func(c *config) error {
		if limit <= 0 {
			return fmt.Errorf("%w: max concurrent requests must be positive, got %d", ErrInvalidOption, limit)
		}
		c.maxConcurrentRequests = limit
		return nil
	}
}

// WithAllowPrivateIPs permits external include fetches to dial private, loopback, and link-local CIDRs.
// By default, SSRF guards block all private network addresses.
func WithAllowPrivateIPs() Option {
	return func(c *config) error {
		c.allowPrivateIPs = true
		return nil
	}
}

// WithAllowedHosts restricts external HTTP includes to matching domain patterns (default: empty = all public).
func WithAllowedHosts(hosts ...string) Option {
	return func(c *config) error {
		for _, h := range hosts {
			if strings.TrimSpace(h) == "" {
				return fmt.Errorf("%w: allowed host cannot be empty", ErrInvalidOption)
			}
		}
		c.allowedHosts = hosts
		return nil
	}
}

// WithAllowPrivateIPsForAllowedHosts permits private IPs specifically for explicitly allowed hosts.
func WithAllowPrivateIPsForAllowedHosts() Option {
	return func(c *config) error {
		c.allowPrivateIPsForAllowedHosts = true
		return nil
	}
}

// WithMaxResponseSize caps the maximum allowed fragment body size in bytes (default: 10MB, 0 = unlimited).
func WithMaxResponseSize(size int64) Option {
	return func(c *config) error {
		if size < 0 {
			return fmt.Errorf("%w: max response size cannot be negative, got %d", ErrInvalidOption, size)
		}
		c.maxResponseSize = size
		return nil
	}
}

// WithoutForwardCookies disables forwarding Set-Cookie headers from fragment subrequests to downstream clients.
// By default, fragment cookies are forwarded.
func WithoutForwardCookies() Option {
	return func(c *config) error {
		c.disableForwardCookies = true
		return nil
	}
}

// WithIncludeErrorMarker configures an HTML placeholder rendered on unhandled fetch errors.
func WithIncludeErrorMarker(marker string) Option {
	return func(c *config) error {
		c.includeErrorMarker = marker
		return nil
	}
}

// WithPreserveETag preserves downstream ETag (weakened to W/"...") and Last-Modified on ESI documents.
// By default, ETag and Last-Modified are stripped to guarantee fresh dynamic fragment evaluation.
func WithPreserveETag() Option {
	return func(c *config) error {
		c.preserveETag = true
		return nil
	}
}

// WithLogger configures a custom slog.Logger for ESI processing.
func WithLogger(logger *slog.Logger) Option {
	return func(c *config) error {
		if logger == nil {
			return fmt.Errorf("%w: logger cannot be nil", ErrInvalidOption)
		}
		c.logger = logger
		return nil
	}
}

// WithMetrics registers ESI telemetry collectors with the provided Prometheus Registerer.
func WithMetrics(reg prometheus.Registerer) Option {
	return func(c *config) error {
		if reg == nil {
			return fmt.Errorf("%w: metrics registerer cannot be nil", ErrInvalidOption)
		}
		c.metrics = reg
		return nil
	}
}

// WithHTTPClient configures a custom http.Client for outbound fragment fetching.
func WithHTTPClient(client *http.Client) Option {
	return func(c *config) error {
		if client == nil {
			return fmt.Errorf("%w: http client cannot be nil", ErrInvalidOption)
		}
		c.httpClient = client
		return nil
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
