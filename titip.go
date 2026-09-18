package titip

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/indragunawan/titip/esi"
	"github.com/indragunawan/titip/storage"
)

var (
	// ErrStorageRequired is returned by New when the mandatory storage parameter is nil.
	ErrStorageRequired = errors.New("titip: storage is required")
)

// Titip represents the HTTP caching middleware instance.
type Titip struct {
	config       config
	storage      storage.Storage
	singleflight singleflight.Group
	logger       *slog.Logger
	metrics      *metrics
	swrWG        sync.WaitGroup
	closed       atomic.Bool
	esiProcessor *esi.Processor
}

// New creates a new Titip caching middleware instance.
// The store parameter is mandatory. If store is nil, ErrStorageRequired is returned.
func New(store storage.Storage, opts ...Option) (*Titip, error) {
	if store == nil {
		return nil, ErrStorageRequired
	}

	cfg := config{
		storage:                   store,
		cacheStatusMode:           CacheStatusSimpleToken,
		respectClientCacheControl: false,
		convertHeadToGet:          true,
		cacheKey:                  CacheKey{},
		tagHeaderName:             headerCacheTag,
		// Default backgroundFetchTimeout is 125s, aligned with Cloudflare's default 125-second Proxy Read Timeout connection limit.
		backgroundFetchTimeout: 125 * time.Second,
		// Default storageTimeout is 5s, providing buffer for remote storage latency and TLS connection handshakes to prevent premature fail-open.
		storageTimeout: 5 * time.Second,
		logger:         slog.Default(),
	}

	for _, opt := range opts {
		if opt == nil {
			return nil, fmt.Errorf("%w: option cannot be nil", ErrInvalidOption)
		}
		if err := opt(&cfg); err != nil {
			if errors.Is(err, ErrInvalidOption) {
				return nil, err
			}
			return nil, fmt.Errorf("%w: %w", ErrInvalidOption, err)
		}
	}

	if cfg.logger == nil {
		cfg.logger = slog.Default()
	}

	t := &Titip{
		config:  cfg,
		storage: cfg.storage,
		logger:  cfg.logger,
		metrics: newMetrics(cfg.metrics),
	}

	if cfg.esiOptions != nil {
		processorOpts := make([]esi.Option, 0, len(cfg.esiOptions)+2)
		processorOpts = append(processorOpts,
			esi.WithLogger(cfg.logger),
		)
		if cfg.metrics != nil {
			processorOpts = append(processorOpts,
				esi.WithMetrics(prometheus.WrapRegistererWithPrefix("titip_", cfg.metrics)),
			)
		}
		processorOpts = append(processorOpts, cfg.esiOptions...)
		proc, err := esi.NewProcessor(processorOpts...)
		if err != nil {
			return nil, fmt.Errorf("%w: ESI configuration error: %w", ErrInvalidOption, err)
		}
		t.esiProcessor = proc
	}

	return t, nil
}

// Purge invalidates cache entries matching the specified path or URL (and its query variations).
//
// The target supports the following formats:
//   - "/api/products"                    — purges the path and ALL query string variations
//   - "https://example.com/api/products" — host-scoped path purge (include domain in target to scope by host)
//   - "https://example.com/api?id=42"    — exact query variant (O(1) exact delete)
//   - "/api/products?id=42"              — exact query variant (exact delete if ExcludeHost=true, or across all hosts)
//   - "/"                                — purges the homepage only
//
// Note: Purge treats any asterisks in the path literally (not as a wildcard).
// To purge a path hierarchy or directory prefix, use PurgePrefix().
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

	isExact, keys, err := buildPurgeOperation(target, &t.config.cacheKey)
	if err != nil {
		t.metrics.recordPurge("url", mode, "error", 0)
		return 0, fmt.Errorf("titip: purge parse error: %w", err)
	}
	if len(keys) == 0 {
		return 0, nil
	}

	if t.logger != nil && t.logger.Enabled(ctx, slog.LevelDebug) {
		t.logger.DebugContext(ctx, "purge path",
			slog.String("target", target),
			slog.Bool("soft", cfg.soft),
			slog.Bool("exact", isExact),
			slog.Any("keys", keys),
		)
	}

	if isExact {
		n, err := t.storage.Purge(ctx, keys[0], cfg.soft)
		if err != nil {
			t.metrics.recordPurge("url", mode, "error", 0)
			return 0, fmt.Errorf("titip: purge path: %w", err)
		}
		t.metrics.recordPurge("url", mode, "success", n)
		return n, nil
	}

	var totalCount int64
	for _, pattern := range keys {
		n, err := t.storage.PurgeByPattern(ctx, pattern, cfg.soft)
		if err != nil {
			t.metrics.recordPurge("url", mode, "error", totalCount)
			return totalCount, fmt.Errorf("titip: purge path pattern: %w", err)
		}
		totalCount += n
	}

	t.metrics.recordPurge("url", mode, "success", totalCount)
	return totalCount, nil
}

// PurgePrefix invalidates cache entries matching the specified path or URL prefix (Cloudflare-style).
//
// Behavior:
//   - "/assets/" (with trailing slash)    — directory prefix: purges all child paths under /assets/ (does not touch /assets-v2)
//   - "/assets"  (without trailing slash) — raw string prefix: purges /assets, /assets/*, AND /assets-v2
//   - "/"                                 — purges the entire cache namespace (supports WithSoftPurge())
//   - "https://example.com/assets/"       — host-scoped prefix purge
//
// By default, purge is a hard-delete (immediate physical eviction). Use WithSoftPurge()
// to mark entries as stale instead for safe thundering-herd protection.
//
// Returns the total number of logical cache entries invalidated.
func (t *Titip) PurgePrefix(ctx context.Context, prefix string, opts ...PurgeOption) (int64, error) {
	cfg := &purgeConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	mode := purgeModeString(cfg.soft)

	keys, err := buildPurgePrefixOperation(prefix, &t.config.cacheKey)
	if err != nil {
		t.metrics.recordPurge("prefix", mode, "error", 0)
		return 0, fmt.Errorf("titip: purge prefix error: %w", err)
	}
	if len(keys) == 0 {
		return 0, nil
	}

	if t.logger != nil && t.logger.Enabled(ctx, slog.LevelDebug) {
		t.logger.DebugContext(ctx, "purge prefix",
			slog.String("prefix", prefix),
			slog.Bool("soft", cfg.soft),
			slog.Any("keys", keys),
		)
	}

	var totalCount int64
	for _, pattern := range keys {
		n, err := t.storage.PurgeByPattern(ctx, pattern, cfg.soft)
		if err != nil {
			t.metrics.recordPurge("prefix", mode, "error", totalCount)
			return totalCount, fmt.Errorf("titip: purge prefix pattern: %w", err)
		}
		totalCount += n
	}

	t.metrics.recordPurge("prefix", mode, "success", totalCount)
	return totalCount, nil
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

	n, err := t.storage.PurgeAll(ctx)
	if err != nil {
		t.metrics.recordPurge("all", "hard", "error", 0)
		return 0, fmt.Errorf("titip: purge all: %w", err)
	}
	t.metrics.recordPurge("all", "hard", "success", n)
	return n, nil
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

	var closeErr error
	if closer, ok := t.storage.(storage.Closer); ok {
		closeErr = closer.Close(ctx)
	} else if closer, ok := t.storage.(io.Closer); ok {
		closeErr = closer.Close()
	}

	if closeErr != nil {
		closeErr = fmt.Errorf("titip: storage close error: %w", closeErr)
	}
	return errors.Join(waitErr, closeErr)
}
