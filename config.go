package titip

import (
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/indragunawan/titip/storage"
)

// CacheStatusMode specifies the format of the emitted Cache-Status header.
type CacheStatusMode int

const (
	// CacheStatusRFC9211 outputs structured RFC-9211 Cache-Status header (e.g. Cache-Status: titip; hit; ttl=240).
	CacheStatusRFC9211 CacheStatusMode = iota
	// CacheStatusSimpleToken outputs single-token status header (e.g. HIT, MISS, BYPASS, STALE).
	CacheStatusSimpleToken
	// CacheStatusNone disables cache status header generation.
	CacheStatusNone
)

// Config defines the configuration parameters for the Titip middleware.
type Config struct {
	Storage                  storage.Storage
	Logger                   *slog.Logger
	Metrics                  prometheus.Registerer
	CacheStatusMode          CacheStatusMode
	IgnoreClientCacheControl bool
	KeyConfig                KeyConfig
	CacheableStatusCodes     map[int]struct{}
	TagHeaderNames           []string
	OriginTimeout            time.Duration
	StorageTimeout           time.Duration
}

// Option configures Titip middleware options.
type Option func(*Config)

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

// WithTagHeaderNames configures headers inspected for cache tags (defaults to ["Cache-Tag", "Surrogate-Key"]).
func WithTagHeaderNames(headers ...string) Option {
	return func(c *Config) {
		c.TagHeaderNames = headers
	}
}

// WithOriginTimeout configures maximum origin fetch timeout (defaults to 30s).
func WithOriginTimeout(d time.Duration) Option {
	return func(c *Config) {
		c.OriginTimeout = d
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
