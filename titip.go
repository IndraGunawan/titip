package titip

import (
	"context"
	"errors"
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
	cfg           config
	storage       storage.Storage
	sf            singleflight.Group
	logger        *slog.Logger
	metrics       *metrics
	swrWG         sync.WaitGroup
	closed        atomic.Bool
	esiHTTPClient *http.Client
}

// New creates a new Titip caching middleware instance.
func New(opts ...Option) (*Titip, error) {
	cfg := config{
		cacheStatusMode:           CacheStatusSimpleToken,
		respectClientCacheControl: false,
		convertHeadToGet:          true,
		keyConfig:                 KeyConfig{},
		tagHeaderName:             headerCacheTag,
		backgroundFetchTimeout:    125 * time.Second,
		storageTimeout:            1 * time.Second,
		logger:                    slog.Default(),
		esi: esi.Config{
			Enabled:               false,
			HeaderRequired:        false,
			MaxDepth:              3,
			MaxTimeout:            30 * time.Second,
			MaxConcurrentRequests: 8,
			MaxResponseSize:       10 * 1024 * 1024,
		},
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.storage == nil {
		return nil, fmt.Errorf("titip: storage is required")
	}

	if cfg.logger == nil {
		cfg.logger = slog.Default()
	}

	ssrfCfg := esi.SSRFConfig{
		BlockPrivateIPs:                !cfg.esi.AllowPrivateIPs,
		AllowedHosts:                   cfg.esi.AllowedHosts,
		AllowPrivateIPsForAllowedHosts: cfg.esi.AllowPrivateIPsForAllowedHosts,
	}

	esiTransport := esi.NewSSRFSafeTransport(ssrfCfg, 10*time.Second)

	return &Titip{
		cfg:           cfg,
		storage:       cfg.storage,
		logger:        cfg.logger,
		metrics:       newMetrics(cfg.metrics, cfg.esi.Enabled),
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
//   - "https://example.com/api" — host-scoped sweep (include domain in target to scope by host)
//
// By default, purge is a hard-delete (immediate physical eviction). Use WithSoftPurge()
// to mark entries as stale instead for safe thundering-herd protection.
//
// Returns the total number of logical cache entries invalidated.
func (t *Titip) Purge(ctx context.Context, target string, opts ...PurgeOption) (int64, error) {
	cfg := &purgeConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	mode := purgeModeString(cfg.soft)

	pt, err := parsePurgeTarget(target, &t.cfg.keyConfig)
	if err != nil {
		t.metrics.recordPurge("url", mode, "error", 0)
		return 0, fmt.Errorf("titip: purge parse error: %w", err)
	}
	if pt == nil {
		return 0, nil
	}

	patterns := buildPurgePatterns(pt, &t.cfg.keyConfig)

	if t.logger != nil && t.logger.Enabled(ctx, slog.LevelDebug) {
		t.logger.DebugContext(ctx, "purge path",
			slog.String("target", target),
			slog.Bool("soft", cfg.soft),
			slog.Any("patterns", patterns),
		)
	}

	var totalCount int64
	for _, pattern := range patterns {
		count, err := t.executePurge(ctx, pt, pattern, cfg.soft)
		if err != nil {
			t.metrics.recordPurge("url", mode, "error", totalCount)
			return totalCount, err
		}
		totalCount += count
	}

	t.metrics.recordPurge("url", mode, "success", totalCount)
	return totalCount, nil
}

// executePurge dispatches a single pattern to the appropriate storage operation.
func (t *Titip) executePurge(ctx context.Context, pt *purgeTarget, pattern string, soft bool) (int64, error) {
	if pt.mode == purgeModeExact && (t.cfg.keyConfig.ExcludeHost || pt.host != "") {
		// pattern is a full exact primary key — use direct Purge.
		n, err := t.storage.Purge(ctx, pattern, soft)
		if err != nil {
			return 0, fmt.Errorf("titip: purge path: %w", err)
		}
		return n, nil
	}

	// Pattern-based purge requires PatternPurger capability.
	pp, ok := t.storage.(storage.PatternPurger)
	if !ok {
		return 0, fmt.Errorf("titip: purge path: storage does not implement PatternPurger for pattern-based purges")
	}
	n, err := pp.PurgeByPattern(ctx, pattern, soft)
	if err != nil {
		return 0, fmt.Errorf("titip: purge path pattern: %w", err)
	}
	return n, nil
}

// PurgeTag invalidates all cache entries tagged with the specified tag.
// The tag is treated as a literal string. To wipe the entire cache namespace, use PurgeAll.
func (t *Titip) PurgeTag(ctx context.Context, tag string, opts ...PurgeOption) (int64, error) {
	cfg := &purgeConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	mode := purgeModeString(cfg.soft)
	if t.logger != nil && t.logger.Enabled(ctx, slog.LevelDebug) {
		t.logger.DebugContext(ctx, "purge tag", slog.String("tag", tag), slog.Bool("soft", cfg.soft))
	}
	n, err := t.storage.PurgeByTag(ctx, tag, cfg.soft)
	if err != nil {
		t.metrics.recordPurge("tag", mode, "error", 0)
		return 0, fmt.Errorf("titip: purge tag: %w", err)
	}
	t.metrics.recordPurge("tag", mode, "success", n)
	return n, nil
}

// PurgeAll deletes every cache entry in the configured storage namespace.
func (t *Titip) PurgeAll(ctx context.Context) (int64, error) {
	if t.logger != nil && t.logger.Enabled(ctx, slog.LevelDebug) {
		t.logger.DebugContext(ctx, "purge all")
	}

	// Prefer AllPurger (most efficient — single SCAN * loop).
	if ap, ok := t.storage.(storage.AllPurger); ok {
		n, err := ap.PurgeAll(ctx)
		if err != nil {
			t.metrics.recordPurge("all", "hard", "error", 0)
			return 0, fmt.Errorf("titip: purge all: %w", err)
		}
		t.metrics.recordPurge("all", "hard", "success", n)
		return n, nil
	}

	// Fallback: use PatternPurger with wildcard.
	if pp, ok := t.storage.(storage.PatternPurger); ok {
		n, err := pp.PurgeByPattern(ctx, "*", false)
		if err != nil {
			t.metrics.recordPurge("all", "hard", "error", 0)
			return 0, fmt.Errorf("titip: purge all via pattern: %w", err)
		}
		t.metrics.recordPurge("all", "hard", "success", n)
		return n, nil
	}
	t.metrics.recordPurge("all", "hard", "error", 0)
	return 0, fmt.Errorf("titip: purge all: storage does not implement AllPurger or PatternDeleter")
}

func purgeModeString(soft bool) string {
	if soft {
		return "soft"
	}
	return "hard"
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

	var waitErr error
	select {
	case <-done:
	case <-ctx.Done():
		select {
		case <-done:
		default:
			waitErr = fmt.Errorf("titip: close timeout waiting for background swr tasks: %w", ctx.Err())
			if t.logger.Enabled(ctx, slog.LevelWarn) {
				t.logger.WarnContext(ctx, "close timeout waiting for background swr tasks", slog.Any("error", ctx.Err()))
			}
		}
	}

	if err := t.storage.Close(); err != nil {
		if waitErr != nil {
			return errors.Join(waitErr, fmt.Errorf("titip: storage close error: %w", err))
		}
		return fmt.Errorf("titip: storage close error: %w", err)
	}
	return waitErr
}
