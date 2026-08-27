package titip

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/indragunawan/titip/esi"
	"github.com/indragunawan/titip/storage"
)

// Titip represents the HTTP caching middleware instance.
type Titip struct {
	cfg                  Config
	storage              storage.Storage
	sf                   singleflight.Group
	logger               *slog.Logger
	cacheableStatusCodes map[int]struct{}
	metrics              *metrics
	swrWG                sync.WaitGroup
	closed               atomic.Bool
	esiHTTPClient        *http.Client
}

// New creates a new Titip caching middleware instance.
func New(opts ...Option) (*Titip, error) {
	cfg := Config{
		CacheStatusMode:           CacheStatusSimpleToken,
		RespectClientCacheControl: false,
		ConvertHeadToGet:          true,
		KeyConfig:                KeyConfig{},
		CacheableStatusCodes:     defaultCacheableStatusCodes,
		TagHeaderName:            headerCacheTag,
		OriginTimeout:            30 * time.Second,
		StorageTimeout:           1 * time.Second,
		Logger:                   slog.Default(),
		ESI: ESIConfig{
			Enabled:                false,
			HeaderRequired:         false,
			MaxDepth:               3,
			MaxTimeout:             30 * time.Second,
			MaxConcurrentRequests:  8,
			BlockPrivateIPs:        true,
			MaxResponseSize:        10 * 1024 * 1024,
			ForwardFragmentCookies: true,
		},
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.Storage == nil {
		return nil, fmt.Errorf("titip: storage is required")
	}

	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	if len(cfg.CacheableStatusCodes) == 0 {
		cfg.CacheableStatusCodes = defaultCacheableStatusCodes
	}

	if cfg.ESI.MaxDepth == 0 {
		cfg.ESI.MaxDepth = 3
	}
	if cfg.ESI.MaxTimeout <= 0 {
		cfg.ESI.MaxTimeout = 30 * time.Second
	}
	if cfg.ESI.MaxConcurrentRequests <= 0 {
		cfg.ESI.MaxConcurrentRequests = 8
	}
	if cfg.ESI.MaxResponseSize <= 0 {
		cfg.ESI.MaxResponseSize = 10 * 1024 * 1024 // 10MB
	}

	ssrfCfg := esi.SSRFConfig{
		BlockPrivateIPs:                cfg.ESI.BlockPrivateIPs,
		AllowedHosts:                   cfg.ESI.AllowedHosts,
		AllowPrivateIPsForAllowedHosts: cfg.ESI.AllowPrivateIPsForAllowedHosts,
	}

	esiTransport := esi.NewSSRFSafeTransport(ssrfCfg, 10*time.Second)

	return &Titip{
		cfg:                  cfg,
		storage:              cfg.Storage,
		logger:               cfg.Logger,
		cacheableStatusCodes: cfg.CacheableStatusCodes,
		metrics:              newMetrics(cfg.Metrics, cfg.ESI.Enabled),
		esiHTTPClient: &http.Client{
			Transport: esiTransport,
		},
	}, nil
}

// Purge invalidates cache entries matching the specified path, URL, exact query variant, or wildcard.
//
// The target supports four formats:
//   - "/api/products"           — sweeps the path and ALL query string variations
//   - "/api/products?id=42"     — purges only this exact query variant
//   - "/assets/*"               — wipes all cached paths under /assets/ (wildcard)
//   - "https://example.com/api" — host-scoped sweep
//
// By default, purge is a hard-delete (immediate physical eviction). Use WithSoftPurge()
// to mark entries as stale instead for safe thundering-herd protection.
//
// Use WithPurgeHost(host) to scope a path-only target to a specific domain.
func (t *Titip) Purge(ctx context.Context, target string, opts ...PurgeOption) error {
	cfg := &PurgeConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	// Override host from option if provided (takes precedence over parsed host).
	pt, err := parsePurgeTarget(target, &t.cfg.KeyConfig)
	if err != nil {
		return fmt.Errorf("titip: purge parse error: %w", err)
	}
	if pt == nil {
		return nil
	}
	if cfg.Host != "" {
		pt.host = cfg.Host
	}

	patterns := buildPurgePatterns(pt, &t.cfg.KeyConfig)

	if t.logger != nil && t.logger.Enabled(ctx, slog.LevelDebug) {
		t.logger.DebugContext(ctx, "titip: purge path",
			slog.String("target", target),
			slog.Bool("soft", cfg.Soft),
			slog.Any("patterns", patterns),
		)
	}

	for _, pattern := range patterns {
		if err := t.executePurge(ctx, pt, pattern, cfg.Soft); err != nil {
			return err
		}
	}
	return nil
}

// executePurge dispatches a single pattern to the appropriate storage operation.
func (t *Titip) executePurge(ctx context.Context, pt *purgeTarget, pattern string, soft bool) error {
	switch pt.mode {
	case purgeModeExact:
		// pattern is a full primary key — use direct Delete or SoftPurge.
		if soft {
			if err := t.storage.SoftPurge(ctx, pattern); err != nil {
				return fmt.Errorf("titip: purge path soft: %w", err)
			}
		} else {
			if err := t.storage.Delete(ctx, pattern); err != nil {
				return fmt.Errorf("titip: purge path delete: %w", err)
			}
		}

	default:
		if soft {
			// Soft-purge pattern: scan matching keys and mark each stale individually.
			if err := t.softPurgeByPattern(ctx, pattern); err != nil {
				return fmt.Errorf("titip: purge path soft pattern: %w", err)
			}
		} else {
			// Hard-purge pattern — requires PatternDeleter.
			pd, ok := t.storage.(storage.PatternDeleter)
			if !ok {
				return fmt.Errorf("titip: purge path: storage does not implement PatternDeleter for pattern-based purges")
			}
			if err := pd.DeletePattern(ctx, pattern); err != nil {
				return fmt.Errorf("titip: purge path pattern delete: %w", err)
			}
		}
	}
	return nil
}

// softPurgeByPattern scans keys matching the given pattern and calls SoftPurge on each match.
// Requires the storage to implement storage.PatternScanner (an internal capability).
func (t *Titip) softPurgeByPattern(ctx context.Context, pattern string) error {
	// Use the SoftPurgeScanner interface if available (e.g. RedisStorage).
	type softPurgeScanner interface {
		SoftPurgePattern(ctx context.Context, pattern string) error
	}
	if sps, ok := t.storage.(softPurgeScanner); ok {
		return sps.SoftPurgePattern(ctx, pattern)
	}
	if t.logger != nil && t.logger.Enabled(ctx, slog.LevelWarn) {
		t.logger.WarnContext(ctx, "titip: soft purge pattern: storage does not implement SoftPurgePattern; skipping",
			slog.String("pattern", pattern),
		)
	}
	return nil
}

// PurgeTag invalidates all cache entries tagged with the specified tag.
//
// The tag is treated as a literal string. To wipe the entire cache namespace, use PurgeAll.
func (t *Titip) PurgeTag(ctx context.Context, tag string, opts ...PurgeOption) error {
	cfg := &PurgeConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	if t.logger != nil && t.logger.Enabled(ctx, slog.LevelDebug) {
		t.logger.DebugContext(ctx, "titip: purge tag",
			slog.String("tag", tag),
			slog.Bool("soft", cfg.Soft),
		)
	}

	if err := t.storage.PurgeByTag(ctx, tag, cfg.Soft); err != nil {
		return fmt.Errorf("titip: purge tag: %w", err)
	}
	return nil
}

// PurgeAll deletes every cache entry in the configured storage namespace.
// Only entries belonging to Titip's key prefix are affected — other Redis keys are preserved.
//
// Requires the storage backend to implement storage.AllPurger or storage.PatternDeleter.
func (t *Titip) PurgeAll(ctx context.Context) error {
	if t.logger != nil && t.logger.Enabled(ctx, slog.LevelDebug) {
		t.logger.DebugContext(ctx, "titip: purge all")
	}

	// Prefer AllPurger (most efficient — single SCAN * loop).
	if ap, ok := t.storage.(storage.AllPurger); ok {
		if err := ap.PurgeAll(ctx); err != nil {
			return fmt.Errorf("titip: purge all: %w", err)
		}
		return nil
	}

	// Fallback: use PatternDeleter with wildcard.
	if pd, ok := t.storage.(storage.PatternDeleter); ok {
		if err := pd.DeletePattern(ctx, "*"); err != nil {
			return fmt.Errorf("titip: purge all via pattern: %w", err)
		}
		return nil
	}

	return fmt.Errorf("titip: purge all: storage does not implement AllPurger or PatternDeleter")
}

// Close cleanly shuts down the middleware, awaiting background SWR revalidations.
func (t *Titip) Close(ctx context.Context) error {
	t.closed.Store(true)

	// Await background SWR goroutines with context cancellation
	done := make(chan struct{})
	go func() {
		t.swrWG.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		if t.logger.Enabled(ctx, slog.LevelWarn) {
			t.logger.WarnContext(ctx, "titip: close timeout waiting for background swr tasks", slog.Any("error", ctx.Err()))
		}
	}

	if err := t.storage.Close(); err != nil {
		return fmt.Errorf("titip: storage close error: %w", err)
	}
	return nil
}
