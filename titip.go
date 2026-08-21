package titip

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/indragunawan/titip/storage"
)

// Titip represents the HTTP caching middleware instance.
type Titip struct {
	cfg                  Config
	storage              storage.Storage
	sf                   singleflight.Group
	logger               *slog.Logger
	cacheableStatusCodes map[int]struct{}
	metrics              *Metrics
	swrWG                sync.WaitGroup
	closed               atomic.Bool
}

// New creates a new Titip caching middleware instance.
func New(opts ...Option) (*Titip, error) {
	cfg := Config{
		CacheStatusMode:          CacheStatusRFC9211,
		IgnoreClientCacheControl: true,
		KeyConfig:                *DefaultKeyConfig(),
		CacheableStatusCodes:     DefaultCacheableStatusCodes,
		TagHeaderNames:           []string{"Cache-Tag", "Surrogate-Key"},
		OriginTimeout:            30 * time.Second,
		StorageTimeout:           1 * time.Second,
		Logger:                   slog.Default(),
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.Storage == nil {
		return nil, fmt.Errorf("titip: storage is required")
	}

	if cfg.CacheableStatusCodes == nil {
		cfg.CacheableStatusCodes = DefaultCacheableStatusCodes
	}

	return &Titip{
		cfg:                  cfg,
		storage:              cfg.Storage,
		logger:               cfg.Logger,
		cacheableStatusCodes: cfg.CacheableStatusCodes,
		metrics:              newMetrics(cfg.Metrics),
	}, nil
}

// Handler returns standard http.Handler middleware.
func (t *Titip) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.serveHTTP(w, r, next)
	})
}

// Wrap is a convenience helper for http.HandlerFunc.
func (t *Titip) Wrap(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		t.serveHTTP(w, r, h)
	}
}

// PurgeURL invalidates cache entries matching the specified URL or path.
func (t *Titip) PurgeURL(ctx context.Context, rawURL string, opts ...PurgeOption) error {
	cfg := &PurgeConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("titip: purge url parse error: %w", err)
	}

	req := &http.Request{
		Host: parsed.Host,
		URL:  parsed,
	}
	primaryKey := GeneratePrimaryKey(req, &t.cfg.KeyConfig)

	if cfg.Soft {
		if err := t.storage.SoftPurge(ctx, primaryKey); err != nil {
			return fmt.Errorf("titip: soft purge url: %w", err)
		}
		return nil
	}

	if err := t.storage.Delete(ctx, primaryKey); err != nil {
		return fmt.Errorf("titip: purge url delete: %w", err)
	}
	return nil
}

// PurgeTag invalidates all cache entries tagged with the specified tag.
func (t *Titip) PurgeTag(ctx context.Context, tag string, opts ...PurgeOption) error {
	cfg := &PurgeConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	if err := t.storage.PurgeByTag(ctx, tag, cfg.Soft); err != nil {
		return fmt.Errorf("titip: purge tag: %w", err)
	}
	return nil
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
		t.logger.Warn("titip: close timeout waiting for background swr tasks", "error", ctx.Err())
	}

	if err := t.storage.Close(); err != nil {
		return fmt.Errorf("titip: storage close error: %w", err)
	}
	return nil
}
