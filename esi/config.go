package esi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"time"
)

var (
	// ErrFallbackToHTTP is returned by an InternalFetcher to signal that Titip should resolve this include via outbound HTTP.
	ErrFallbackToHTTP = errors.New("titip: esi: fallback to outbound http")
)

// InternalFetcherFunc defines the signature for resolving in-process ESI includes.
type InternalFetcherFunc func(ctx context.Context, targetPath string, r *http.Request) ([]byte, http.Header, error)

// Config defines the configuration options for Edge Side Includes (ESI) processing.
type Config struct {
	// Enabled is the master switch for ESI parsing and fragment splicing (default: false).
	Enabled bool

	// HeaderRequired processes ESI only when the origin returns Surrogate-Control (default: false).
	HeaderRequired bool

	// InternalFetcher provides a custom hook to resolve internal/same-host ESI includes in-process.
	InternalFetcher InternalFetcherFunc

	// MaxDepth defines the maximum global recursion depth for nested includes (default: 3).
	MaxDepth uint32

	// MaxTimeout specifies the global maximum time budget for fetching an include fragment (default: 30s).
	MaxTimeout time.Duration

	// MaxConcurrentRequests caps concurrent fragment fetch goroutines per document (default: 8).
	MaxConcurrentRequests int

	// AllowPrivateIPs disables SSRF IP blocking and permits dials to private/loopback CIDRs (default: false = blocked).
	AllowPrivateIPs bool

	// AllowedHosts restricts external HTTP includes to matching domain patterns (default: empty = all public).
	AllowedHosts []string

	// AllowPrivateIPsForAllowedHosts allows internal IPs for explicitly whitelisted hosts (default: false).
	AllowPrivateIPsForAllowedHosts bool

	// MaxResponseSize caps the maximum allowed fragment body size in bytes (default: 10MB, 0 = unlimited).
	MaxResponseSize int64

	// DisableForwardCookies disables forwarding Set-Cookie headers from subrequests to the client (default: false = forwarded).
	DisableForwardCookies bool

	// IncludeErrorMarker is the HTML placeholder rendered on unhandled fetch errors (default: "").
	IncludeErrorMarker string
}

// Option configures ESI engine parameters.
type Option func(*Config)

// WithHeaderRequired configures whether ESI is processed only when Surrogate-Control is present.
func WithHeaderRequired(required bool) Option {
	return func(c *Config) {
		c.HeaderRequired = required
	}
}

// WithInternalFetcher configures a custom in-process handler for resolving local fragment subrequests.
func WithInternalFetcher(fetcher InternalFetcherFunc) Option {
	return func(c *Config) {
		c.InternalFetcher = fetcher
	}
}

// WithMaxDepth configures the maximum global recursion depth for nested includes (default: 3).
func WithMaxDepth(depth uint32) Option {
	return func(c *Config) {
		if depth > 0 {
			c.MaxDepth = depth
		}
	}
}

// WithMaxTimeout configures the maximum fetch timeout for resolving an include fragment (default: 30s).
func WithMaxTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		if timeout > 0 {
			c.MaxTimeout = timeout
		}
	}
}

// WithMaxConcurrentRequests caps concurrent fragment fetch goroutines per document (default: 8).
func WithMaxConcurrentRequests(limit int) Option {
	return func(c *Config) {
		if limit > 0 {
			c.MaxConcurrentRequests = limit
		}
	}
}

// WithAllowPrivateIPs configures whether SSRF blocking permits dials to private/loopback CIDRs (default: false = blocked).
func WithAllowPrivateIPs(allow bool) Option {
	return func(c *Config) {
		c.AllowPrivateIPs = allow
	}
}

// WithAllowedHosts restricts external HTTP includes to matching domain patterns (default: empty = all public).
func WithAllowedHosts(hosts ...string) Option {
	return func(c *Config) {
		c.AllowedHosts = hosts
	}
}

// WithAllowPrivateIPsForAllowedHosts permits private IPs specifically for explicitly allowed hosts.
func WithAllowPrivateIPsForAllowedHosts(allow bool) Option {
	return func(c *Config) {
		c.AllowPrivateIPsForAllowedHosts = allow
	}
}

// WithMaxResponseSize caps the maximum allowed fragment body size in bytes (default: 10MB).
func WithMaxResponseSize(size int64) Option {
	return func(c *Config) {
		if size > 0 {
			c.MaxResponseSize = size
		}
	}
}

// WithDisableForwardCookies configures whether Set-Cookie headers from subrequests are forwarded to the client (default: false = forwarded).
func WithDisableForwardCookies(disable bool) Option {
	return func(c *Config) {
		c.DisableForwardCookies = disable
	}
}

// WithIncludeErrorMarker configures an HTML placeholder rendered on unhandled fetch errors.
func WithIncludeErrorMarker(marker string) Option {
	return func(c *Config) {
		c.IncludeErrorMarker = marker
	}
}

// HandlerFetcher adapts any standard http.Handler into an InternalFetcherFunc for in-process subrequests.
func HandlerFetcher(router http.Handler) InternalFetcherFunc {
	return func(ctx context.Context, targetPath string, r *http.Request) ([]byte, http.Header, error) {
		if router == nil {
			return nil, nil, errors.New("titip: esi: router is nil")
		}

		parsedURL, err := url.Parse(targetPath)
		if err != nil {
			return nil, nil, fmt.Errorf("titip: esi: parse url: %w", err)
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
