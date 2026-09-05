package titip

import (
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/indragunawan/titip/esi"
	"github.com/indragunawan/titip/storage"
)

// CacheStatusMode specifies the format of the emitted Cache-Status header.
type CacheStatusMode int

const (
	// CacheStatusSimpleToken outputs single-token status header (e.g. HIT, MISS, EXPIRED, REVALIDATED, UPDATING, STALE, BYPASS, DYNAMIC) by default.
	CacheStatusSimpleToken CacheStatusMode = iota
	// CacheStatusRFC9211 outputs structured RFC-9211 Cache-Status header (e.g. Cache-Status: titip; hit; ttl=240).
	CacheStatusRFC9211
	// CacheStatusNone disables cache status header generation.
	CacheStatusNone
)

// Standard Simple-Token Cache-Status values emitted when CacheStatusSimpleToken is active.
const (
	// tokenHit indicates the request was served fresh directly from the cache (or matched downstream client conditional 304).
	tokenHit = "HIT"

	// tokenMiss indicates the resource was not found in the cache, was fetched from origin, and was saved to cache.
	tokenMiss = "MISS"

	// tokenExpired indicates the cached entry was expired or soft-purged, was synchronously revalidated with origin, and was refreshed with a new 200 OK.
	tokenExpired = "EXPIRED"

	// tokenRevalidated indicates the cached entry was expired, was revalidated with origin via conditional headers, and was refreshed with a 304 Not Modified.
	tokenRevalidated = "REVALIDATED"

	// tokenUpdating indicates a stale cached entry was served immediately to the client while an asynchronous background goroutine revalidates it with origin (stale-while-revalidate).
	tokenUpdating = "UPDATING"

	// tokenStale indicates a stale cached entry was served as a failover fallback because the origin returned a 5xx server error or panicked (stale-if-error).
	tokenStale = "STALE"

	// tokenBypass indicates caching was explicitly bypassed due to request characteristics (mutating HTTP methods like POST/PUT/DELETE, Range requests, client Cache-Control: no-store, or WebSocket upgrades).
	tokenBypass = "BYPASS"

	// tokenDynamic indicates the request was evaluated for caching, but the origin returned an uncacheable response (e.g. Set-Cookie, Cache-Control: private/no-store, or SSE event-stream).
	tokenDynamic = "DYNAMIC"
)

// config defines the configuration parameters for the Titip middleware.
type config struct {
	storage                       storage.Storage
	logger                        *slog.Logger
	metrics                       prometheus.Registerer
	cacheStatusMode               CacheStatusMode
	respectClientCacheControl     bool
	convertHeadToGet              bool
	autoInvalidateMutatingMethods bool
	keyConfig                     KeyConfig
	tagHeaderName                 string
	backgroundFetchTimeout        time.Duration
	storageTimeout                time.Duration
	esi                           esi.Config
}

// Option configures Titip middleware options.
type Option func(*config)

// WithConvertHeadToGet configures whether HEAD cache misses and revalidations are converted to GET
// when fetching from the upstream origin to prime the cache (defaults to true).
// When false, HEAD misses query the origin as HEAD and are not saved to cache.
func WithConvertHeadToGet(enable bool) Option {
	return func(c *config) {
		c.convertHeadToGet = enable
	}
}

// WithAutoInvalidateMutatingMethods enables automatic invalidation of cached GET entries
// when successful mutating requests (POST, PUT, DELETE, PATCH) are received for the URI,
// matching the mandatory invalidation behavior defined in RFC 9111 Section 4.4.
// By default, this is disabled so applications can rely on explicit tag-based (Cache-Tag) or URL invalidation.
func WithAutoInvalidateMutatingMethods() Option {
	return func(c *config) {
		c.autoInvalidateMutatingMethods = true
	}
}

// WithMetrics configures the Prometheus metrics registerer.
func WithMetrics(reg prometheus.Registerer) Option {
	return func(c *config) {
		c.metrics = reg
	}
}

// WithStorageTimeout configures maximum timeout for storage operations (defaults to 1s).
func WithStorageTimeout(d time.Duration) Option {
	return func(c *config) {
		c.storageTimeout = d
	}
}

// WithStorage configures the backend cache storage engine.
func WithStorage(s storage.Storage) Option {
	return func(c *config) {
		c.storage = s
	}
}

// WithLogger configures the structured slog.Logger.
func WithLogger(l *slog.Logger) Option {
	return func(c *config) {
		c.logger = l
	}
}

// WithCacheStatusMode configures Cache-Status header mode.
func WithCacheStatusMode(mode CacheStatusMode) Option {
	return func(c *config) {
		c.cacheStatusMode = mode
	}
}

// WithRespectClientCacheControl enables respecting client request Cache-Control directives (e.g. no-cache, no-store).
// By default, client cache directives are ignored to protect origin servers.
func WithRespectClientCacheControl() Option {
	return func(c *config) {
		c.respectClientCacheControl = true
	}
}

// WithKeyConfig configures cache key generation rules.
func WithKeyConfig(cfg KeyConfig) Option {
	return func(c *config) {
		c.keyConfig = cfg
	}
}

// WithTagHeaderName configures the response header inspected for cache tags (defaults to "Cache-Tag").
func WithTagHeaderName(name string) Option {
	return func(c *config) {
		c.tagHeaderName = name
	}
}

// WithBackgroundFetchTimeout configures the maximum timeout for asynchronous background revalidation
// (stale-while-revalidate) origin fetches (defaults to 125s).
// Set to 0 or negative to disable background timeout enforcement.
func WithBackgroundFetchTimeout(d time.Duration) Option {
	return func(c *config) {
		c.backgroundFetchTimeout = d
	}
}

// WithESI enables ESI processing with the provided ESI options.
// If no options are provided, ESI is enabled with safe production defaults.
func WithESI(opts ...esi.Option) Option {
	return func(c *config) {
		c.esi.Enabled = true
		for _, opt := range opts {
			if opt != nil {
				opt(&c.esi)
			}
		}
	}
}

// purgeConfig defines options for cache invalidations.
type purgeConfig struct {
	soft bool // soft marks entries as stale rather than evicting immediately (default: false = hard delete).
}

// PurgeOption configures Purge, PurgeTag, or PurgeAll operations.
type PurgeOption func(*purgeConfig)

// WithSoftPurge marks entries as stale rather than evicting immediately (safe thundering-herd mode).
// The stale copy is preserved for stale-if-error fallback if the origin subsequently fails.
func WithSoftPurge() PurgeOption {
	return func(c *purgeConfig) {
		c.soft = true
	}
}
