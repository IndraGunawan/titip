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
	metrics              *Metrics
	swrWG                sync.WaitGroup
	closed               atomic.Bool
	esiHTTPClient        *http.Client
}

// New creates a new Titip caching middleware instance.
func New(opts ...Option) (*Titip, error) {
	cfg := Config{
		CacheStatusMode:          CacheStatusSimpleToken,
		IgnoreClientCacheControl: true,
		KeyConfig:                *DefaultKeyConfig(),
		CacheableStatusCodes:     DefaultCacheableStatusCodes,
		TagHeaderName:            HeaderCacheTag,
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

	if cfg.CacheableStatusCodes == nil {
		cfg.CacheableStatusCodes = DefaultCacheableStatusCodes
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
		cfg.ESI.MaxResponseSize = 10 * 1024 * 1024
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
		metrics:              newMetrics(cfg.Metrics),
		esiHTTPClient: &http.Client{
			Transport: esiTransport,
		},
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
		if t.logger != nil && t.logger.Enabled(ctx, slog.LevelDebug) {
			t.logger.DebugContext(ctx, "titip: purge url (soft)",
				slog.String("url", rawURL),
				slog.String("primary_key", primaryKey),
			)
		}
		if err := t.storage.SoftPurge(ctx, primaryKey); err != nil {
			return fmt.Errorf("titip: soft purge url: %w", err)
		}
		return nil
	}

	if t.logger != nil && t.logger.Enabled(ctx, slog.LevelDebug) {
		t.logger.DebugContext(ctx, "titip: purge url (hard)",
			slog.String("url", rawURL),
			slog.String("primary_key", primaryKey),
		)
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
