package titip

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/indragunawan/titip/storage"
)

// CacheStatusMode specifies the format of the emitted Cache-Status header.
type CacheStatusMode int

const (
	// CacheStatusSimpleToken outputs single-token status header (e.g. HIT, MISS, BYPASS, STALE) by default.
	CacheStatusSimpleToken CacheStatusMode = iota
	// CacheStatusRFC9211 outputs structured RFC-9211 Cache-Status header (e.g. Cache-Status: titip; hit; ttl=240).
	CacheStatusRFC9211
	// CacheStatusNone disables cache status header generation.
	CacheStatusNone
)

// InternalFetcherFunc defines the signature for resolving in-process ESI includes.
type InternalFetcherFunc func(ctx context.Context, targetPath string, r *http.Request) ([]byte, http.Header, error)

// ESIConfig defines the configuration options for Edge Side Includes (ESI) processing.
type ESIConfig struct {
	// Enabled is the master switch for ESI parsing and fragment splicing (default: false).
	Enabled bool

	// HeaderRequired processes ESI only when the origin returns Surrogate-Control or Edge-Control (default: false).
	HeaderRequired bool

	// InternalFetcher provides a custom hook to resolve internal/same-host ESI includes in-process.
	InternalFetcher InternalFetcherFunc

	// MaxDepth defines the maximum global recursion depth for nested includes (default: 3).
	MaxDepth uint32

	// MaxTimeout specifies the global maximum time budget for fetching an include fragment (default: 30s).
	MaxTimeout time.Duration

	// MaxConcurrentRequests caps concurrent fragment fetch goroutines per document (default: 8).
	MaxConcurrentRequests int

	// BlockPrivateIPs blocks private, loopback, and cloud metadata CIDRs at dial time (default: true).
	BlockPrivateIPs bool

	// AllowedHosts restricts external HTTP includes to matching domain patterns (default: empty = all public).
	AllowedHosts []string

	// AllowPrivateIPsForAllowedHosts allows internal IPs for explicitly whitelisted hosts (default: false).
	AllowPrivateIPsForAllowedHosts bool

	// MaxResponseSize caps the maximum allowed fragment body size in bytes (default: 10MB, 0 = unlimited).
	MaxResponseSize int64

	// ForwardFragmentCookies forwards Set-Cookie headers from subrequests to the client (default: true).
	ForwardFragmentCookies bool

	// IncludeErrorMarker is the HTML placeholder rendered on unhandled fetch errors (default: "").
	IncludeErrorMarker string
}

// Config defines the configuration parameters for the Titip middleware.
type Config struct {
	Storage                       storage.Storage
	Logger                        *slog.Logger
	Metrics                       prometheus.Registerer
	CacheStatusMode               CacheStatusMode
	IgnoreClientCacheControl      bool
	AutoInvalidateMutatingMethods bool
	KeyConfig                     KeyConfig
	CacheableStatusCodes          map[int]struct{}
	TagHeaderName                 string
	OriginTimeout                 time.Duration
	StorageTimeout                time.Duration
	ESI                           ESIConfig
}

// Option configures Titip middleware options.
type Option func(*Config)

// WithAutoInvalidateMutatingMethods configures whether successful mutating requests (POST, PUT, DELETE, PATCH)
// automatically invalidate the cached entry for the request URI and response Location/Content-Location headers (defaults to false).
func WithAutoInvalidateMutatingMethods(enable bool) Option {
	return func(c *Config) {
		c.AutoInvalidateMutatingMethods = enable
	}
}

// WithMetrics configures the Prometheus metrics registerer.
func WithMetrics(reg prometheus.Registerer) Option {
	return func(c *Config) {
		c.Metrics = reg
	}
}

// WithStorageTimeout configures maximum timeout for storage operations (defaults to 1s).
func WithStorageTimeout(d time.Duration) Option {
	return func(c *Config) {
		c.StorageTimeout = d
	}
}

// WithStorage configures the backend cache storage engine.
func WithStorage(s storage.Storage) Option {
	return func(c *Config) {
		c.Storage = s
	}
}

// WithLogger configures the structured slog.Logger.
func WithLogger(l *slog.Logger) Option {
	return func(c *Config) {
		c.Logger = l
	}
}

// WithCacheStatusMode configures Cache-Status header mode.
func WithCacheStatusMode(mode CacheStatusMode) Option {
	return func(c *Config) {
		c.CacheStatusMode = mode
	}
}

// WithIgnoreClientCacheControl configures whether to ignore client Cache-Control directives (defaults to true).
func WithIgnoreClientCacheControl(ignore bool) Option {
	return func(c *Config) {
		c.IgnoreClientCacheControl = ignore
	}
}

// WithKeyConfig configures cache key generation rules.
func WithKeyConfig(cfg KeyConfig) Option {
	return func(c *Config) {
		c.KeyConfig = cfg
	}
}

// WithCacheableStatusCodes configures the set of HTTP status codes eligible for caching.
func WithCacheableStatusCodes(codes ...int) Option {
	return func(c *Config) {
		c.CacheableStatusCodes = make(map[int]struct{}, len(codes))
		for _, code := range codes {
			c.CacheableStatusCodes[code] = struct{}{}
		}
	}
}

// WithTagHeaderName configures the response header inspected for cache tags (defaults to "Cache-Tag").
func WithTagHeaderName(name string) Option {
	return func(c *Config) {
		c.TagHeaderName = name
	}
}

// WithOriginTimeout configures maximum origin fetch timeout (defaults to 30s).
func WithOriginTimeout(d time.Duration) Option {
	return func(c *Config) {
		c.OriginTimeout = d
	}
}

// WithESI configures full ESI options.
func WithESI(cfg ESIConfig) Option {
	return func(c *Config) {
		c.ESI = cfg
	}
}

// ESIHandlerFetcher adapts any standard http.Handler into an InternalFetcherFunc for in-process subrequests.
func ESIHandlerFetcher(router http.Handler) InternalFetcherFunc {
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

		rec := GetResponseRecorder()
		defer PutResponseRecorder(rec)

		router.ServeHTTP(rec, subReq)

		if rec.Code >= 400 {
			return nil, rec.Header().Clone(), fmt.Errorf("subrequest returned status %d", rec.Code)
		}

		return bytes.Clone(rec.Body.Bytes()), rec.Header().Clone(), nil
	}
}

// WithESIEnabled enables or disables ESI processing.
func WithESIEnabled(enabled bool) Option {
	return func(c *Config) {
		c.ESI.Enabled = enabled
	}
}

// WithESIInternalFetcher configures a custom hook to resolve internal/same-host ESI includes in-process.
func WithESIInternalFetcher(fetcher InternalFetcherFunc) Option {
	return func(c *Config) {
		c.ESI.InternalFetcher = fetcher
	}
}

// WithESIMaxTimeout configures the global maximum time budget for fetching an include fragment.
func WithESIMaxTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		c.ESI.MaxTimeout = timeout
	}
}

// WithESIMaxDepth configures the global maximum recursion depth for nested includes.
func WithESIMaxDepth(depth uint32) Option {
	return func(c *Config) {
		c.ESI.MaxDepth = depth
	}
}

// WithESIMaxConcurrentRequests configures maximum concurrent fragment fetch goroutines per document.
func WithESIMaxConcurrentRequests(maxConcurrent int) Option {
	return func(c *Config) {
		c.ESI.MaxConcurrentRequests = maxConcurrent
	}
}

// WithESISSRFProtection configures dial-time SSRF blocking and allowed host patterns.
func WithESISSRFProtection(blockPrivateIPs bool, allowedHosts ...string) Option {
	return func(c *Config) {
		c.ESI.BlockPrivateIPs = blockPrivateIPs
		c.ESI.AllowedHosts = allowedHosts
	}
}

// WithESIForwardFragmentCookies configures whether Set-Cookie headers from subrequests are forwarded to client.
func WithESIForwardFragmentCookies(forward bool) Option {
	return func(c *Config) {
		c.ESI.ForwardFragmentCookies = forward
	}
}

// WithESIMaxResponseSize configures maximum allowed fragment body size in bytes.
func WithESIMaxResponseSize(maxBytes int64) Option {
	return func(c *Config) {
		c.ESI.MaxResponseSize = maxBytes
	}
}

// WithESIErrorMarker configures the HTML placeholder rendered on unhandled fetch errors.
func WithESIErrorMarker(marker string) Option {
	return func(c *Config) {
		c.ESI.IncludeErrorMarker = marker
	}
}

// PurgeConfig defines options for cache invalidations.
type PurgeConfig struct {
	Soft bool
}

// PurgeOption configures PurgeURL or PurgeTag operations.
type PurgeOption func(*PurgeConfig)

// WithSoftPurge marks entries as stale rather than evicting immediately.
func WithSoftPurge() PurgeOption {
	return func(c *PurgeConfig) {
		c.Soft = true
	}
}
