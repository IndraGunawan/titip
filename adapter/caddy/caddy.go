package caddy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"

	"github.com/indragunawan/titip"
	"github.com/indragunawan/titip/esi"
	"github.com/indragunawan/titip/storage"
)

func init() {
	caddy.RegisterModule(Handler{})
	httpcaddyfile.RegisterDirectiveOrder("titip", httpcaddyfile.After, "encode")
	httpcaddyfile.RegisterHandlerDirective("titip", parseCaddyfile)
}

// StorageModule defines the interface that Caddy guest storage modules must implement.
type StorageModule interface {
	Storage() storage.Storage
}

// Multi-Engine Support & Admin Purge Routing:
//
// In Caddy, multiple `titip` middleware directives can exist simultaneously across
// different site blocks (virtual hosts) or route segments within the same Caddyfile.
// For example:
//
//	api.example.com {
//	    titip {
//	        storage redis { address localhost:6379, key_prefix api: }
//	    }
//	    reverse_proxy localhost:9000
//	}
//
//	static.example.com {
//	    titip {
//	        storage redis { address localhost:6379, key_prefix static: }
//	    }
//	    reverse_proxy localhost:9001
//	}
//
// Each `titip` directive block in the Caddyfile provisions its own `Handler`
// and an independent `*titip.Titip` engine instance.
//
// Furthermore, during zero-downtime dynamic reloads (`caddy reload`), Caddy provisions
// new handler instances before calling `Cleanup()` on the superseded instances.
//
// Because the Caddy Admin Purge API (`POST /titip/purge`) is mounted once as a global
// singleton endpoint on Caddy's private admin port (default `:2019`), it uses a
// thread-safe global registry (`engines` map protected by `enginesMu`) to:
//  1. Track all active Titip engine instances across all virtual hosts and routes.
//  2. Broadcast purge operations (URL, Tag, Purge All) across all active storage backends.
//  3. Safely manage registrations during concurrent configuration reloads and admin requests.
var (
	enginesMu sync.RWMutex
	engines   = make(map[string]*titip.Titip)
)

func registerEngine(id string, t *titip.Titip) {
	enginesMu.Lock()
	defer enginesMu.Unlock()
	engines[id] = t
}

func unregisterEngine(id string) {
	enginesMu.Lock()
	defer enginesMu.Unlock()
	delete(engines, id)
}

func getEngines() []*titip.Titip {
	enginesMu.RLock()
	defer enginesMu.RUnlock()
	list := make([]*titip.Titip, 0, len(engines))
	for _, e := range engines {
		list = append(list, e)
	}
	return list
}

// KeyConfig defines the cache key generation parameters in Caddy.
type KeyConfig struct {
	IncludeProtocol          *bool               `json:"include_protocol,omitempty"`
	ExcludeHost              *bool               `json:"exclude_host,omitempty"`
	ExcludeQueryString       *bool               `json:"exclude_query_string,omitempty"`
	DisableQueryStringSort   *bool               `json:"disable_query_string_sort,omitempty"`
	IncludedQueryParams      []string            `json:"included_query_params,omitempty"`
	ExcludedQueryParams      []string            `json:"excluded_query_params,omitempty"`
	ExcludeMarketingParams   *bool               `json:"exclude_marketing_params,omitempty"`
	IncludedHeaderNames      []string            `json:"included_header_names,omitempty"`
	IncludedCookieNames      []string            `json:"included_cookie_names,omitempty"`
	CaseInsensitivePath      *bool               `json:"case_insensitive_path,omitempty"`
	IncludedQueryParamValues map[string][]string `json:"included_query_param_values,omitempty"`
}

// ESIConfig defines ESI parameters in Caddy.
type ESIConfig struct {
	Enabled                        *bool    `json:"enabled,omitempty"`
	HeaderRequired                 *bool    `json:"header_required,omitempty"`
	MaxDepth                       *uint32  `json:"max_depth,omitempty"`
	MaxTimeout                     string   `json:"max_timeout,omitempty"`
	MaxConcurrentRequests          *int     `json:"max_concurrent_requests,omitempty"`
	BlockPrivateIPs                *bool    `json:"block_private_ips,omitempty"`
	AllowedHosts                   []string `json:"allowed_hosts,omitempty"`
	AllowPrivateIPsForAllowedHosts *bool    `json:"allow_private_ips_for_allowed_hosts,omitempty"`
	MaxResponseSize                string   `json:"max_response_size,omitempty"`
	ForwardFragmentCookies         *bool    `json:"forward_fragment_cookies,omitempty"`
	ErrorMarker                    string   `json:"error_marker,omitempty"`
}

// Handler implements the Caddy HTTP middleware for Titip caching.
type Handler struct {
	StorageRaw                    json.RawMessage `json:"storage,omitempty" caddy:"namespace=titip.storage inline_key=name"`
	CacheStatus                   string          `json:"cache_status,omitempty"`
	RespectClientCacheControl     *bool           `json:"respect_client_cache_control,omitempty"`
	AutoInvalidateMutatingMethods *bool           `json:"auto_invalidate_mutating_methods,omitempty"`
	ConvertHeadToGet              *bool           `json:"convert_head_to_get,omitempty"`
	BackgroundFetchTimeout        string          `json:"background_fetch_timeout,omitempty"`
	StorageTimeout                string          `json:"storage_timeout,omitempty"`
	TagHeader                     string          `json:"tag_header,omitempty"`
	Key                           *KeyConfig      `json:"key,omitempty"`
	ESI                           *ESIConfig      `json:"esi,omitempty"`
	UseRewrittenURL               *bool           `json:"use_rewritten_url,omitempty"`

	storageMod      StorageModule
	engine          *titip.Titip
	id              string
	useRewrittenURL bool
	logger          *slog.Logger
}

// CaddyModule returns the Caddy module information.
func (Handler) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.titip",
		New: func() caddy.Module { return new(Handler) },
	}
}

// Provision sets up the Titip caching engine and guest storage module.
func (h *Handler) Provision(ctx caddy.Context) error {
	h.id = fmt.Sprintf("titip-%p", h)
	if h.logger == nil {
		h.logger = ctx.Slogger()
	}

	// Check for global App defaults
	var app *App
	func() {
		defer func() { _ = recover() }()
		if appIface, err := ctx.App("titip"); err == nil && appIface != nil {
			if a, ok := appIface.(*App); ok && a != nil {
				app = a
			}
		}
	}()

	// 1. Provision dynamic storage guest module (or inherit from global App)
	var store storage.Storage
	if len(h.StorageRaw) > 0 {
		val, err := ctx.LoadModule(h, "StorageRaw")
		if err != nil {
			return fmt.Errorf("titip: storage module not installed or invalid: %w", err)
		}
		sMod, ok := val.(StorageModule)
		if !ok {
			return fmt.Errorf("titip: module does not implement StorageModule")
		}
		h.storageMod = sMod
		store = sMod.Storage()
	} else if app != nil && app.storageMod != nil {
		store = app.storageMod.Storage()
	} else {
		return fmt.Errorf("titip: storage configuration is required (neither route nor global storage provided)")
	}

	if store == nil {
		return fmt.Errorf("titip: storage backend failed to initialize")
	}

	// 2. Configure Titip options
	opts := []titip.Option{
		titip.WithStorage(store),
		titip.WithMetrics(ctx.GetMetricsRegistry()),
	}

	if h.logger != nil {
		opts = append(opts, titip.WithLogger(h.logger))
	}

	// Cache-Status header mode (inherit from app if not set)
	cacheStatus := h.CacheStatus
	if cacheStatus == "" && app != nil {
		cacheStatus = app.CacheStatus
	}
	switch strings.ToLower(cacheStatus) {
	case "simple", "":
		opts = append(opts, titip.WithCacheStatusMode(titip.CacheStatusSimpleToken))
	case "rfc9211":
		opts = append(opts, titip.WithCacheStatusMode(titip.CacheStatusRFC9211))
	case "none", "disabled":
		opts = append(opts, titip.WithCacheStatusMode(titip.CacheStatusNone))
	default:
		return fmt.Errorf("titip: unknown cache_status mode %q (allowed: rfc9211, simple, none)", cacheStatus)
	}

	// RespectClientCacheControl (inherit from app if not set)
	respectClient := h.RespectClientCacheControl
	if respectClient == nil && app != nil {
		respectClient = app.RespectClientCacheControl
	}
	if respectClient != nil && *respectClient {
		opts = append(opts, titip.WithRespectClientCacheControl())
	}

	// AutoInvalidateMutatingMethods (inherit from app if not set)
	autoInvalidate := h.AutoInvalidateMutatingMethods
	if autoInvalidate == nil && app != nil {
		autoInvalidate = app.AutoInvalidateMutatingMethods
	}
	if autoInvalidate != nil && *autoInvalidate {
		opts = append(opts, titip.WithAutoInvalidateMutatingMethods())
	}

	// ConvertHeadToGet (inherit from app if not set)
	convertHead := h.ConvertHeadToGet
	if convertHead == nil && app != nil {
		convertHead = app.ConvertHeadToGet
	}
	if convertHead != nil {
		opts = append(opts, titip.WithConvertHeadToGet(*convertHead))
	}

	// BackgroundFetchTimeout (inherit from app if not set)
	bgFetchTimeout := h.BackgroundFetchTimeout
	if bgFetchTimeout == "" && app != nil {
		bgFetchTimeout = app.BackgroundFetchTimeout
	}
	if bgFetchTimeout != "" {
		d, err := caddy.ParseDuration(bgFetchTimeout)
		if err != nil {
			return fmt.Errorf("titip: invalid background_fetch_timeout duration %q: %w", bgFetchTimeout, err)
		}
		opts = append(opts, titip.WithBackgroundFetchTimeout(d))
	}

	// StorageTimeout (inherit from app if not set)
	storageTimeout := h.StorageTimeout
	if storageTimeout == "" && app != nil {
		storageTimeout = app.StorageTimeout
	}
	if storageTimeout != "" {
		d, err := caddy.ParseDuration(storageTimeout)
		if err != nil {
			return fmt.Errorf("titip: invalid storage_timeout duration %q: %w", storageTimeout, err)
		}
		opts = append(opts, titip.WithStorageTimeout(d))
	}

	if h.TagHeader != "" {
		opts = append(opts, titip.WithTagHeaderName(h.TagHeader))
	}

	// UseRewrittenURL (inherit from app if not set)
	useRewritten := false
	if app != nil && app.UseRewrittenURL != nil {
		useRewritten = *app.UseRewrittenURL
	}
	if h.UseRewrittenURL != nil {
		useRewritten = *h.UseRewrittenURL
	}
	h.useRewrittenURL = useRewritten

	// Key configuration: default -> global App defaults -> route overrides
	if (app != nil && app.KeyConfig != nil) || h.Key != nil {
		keyCfg := titip.KeyConfig{}
		if app != nil && app.KeyConfig != nil {
			if err := applyKeyConfig(&keyCfg, app.KeyConfig); err != nil {
				return err
			}
		}
		if h.Key != nil {
			if err := applyKeyConfig(&keyCfg, h.Key); err != nil {
				return err
			}
		}
		opts = append(opts, titip.WithKeyConfig(keyCfg))
	}

	// ESI configuration: default -> global App defaults -> route overrides
	if (app != nil && app.ESI != nil) || h.ESI != nil {
		var esiOpts []esi.Option
		if app != nil && app.ESI != nil {
			_ = applyESIConfig(&esiOpts, app.ESI)
		}
		if h.ESI != nil {
			_ = applyESIConfig(&esiOpts, h.ESI)
		}

		// In-process virtual subrequest fetcher adapted from Caddy funcHTTPInclude:
		// https://github.com/caddyserver/caddy/blob/e2eee6a/modules/caddyhttp/templates/tplcontext.go#L169-L217
		internalFetcher := func(ctx context.Context, targetPath string, r *http.Request) ([]byte, http.Header, error) {
			parsedURL, err := url.Parse(targetPath)
			if err != nil {
				return nil, nil, err
			}

			virtReq := &http.Request{
				Method:     http.MethodGet,
				URL:        parsedURL,
				RequestURI: targetPath,
				Header:     r.Header.Clone(),
				Host:       r.Host,
				RemoteAddr: "127.0.0.1:10000", // https://github.com/caddyserver/caddy/issues/5835
				Proto:      r.Proto,
				ProtoMajor: r.ProtoMajor,
				ProtoMinor: r.ProtoMinor,
				Body:       http.NoBody,
			}
			virtReq.Header.Set("Accept-Encoding", "identity") // https://github.com/caddyserver/caddy/issues/4352
			if r.Trailer != nil {
				virtReq.Trailer = r.Trailer.Clone()
			}
			virtReq = virtReq.WithContext(ctx)

			rec := httptest.NewRecorder()

			if srv, ok := r.Context().Value(caddyhttp.ServerCtxKey).(http.Handler); ok && srv != nil {
				srv.ServeHTTP(rec, virtReq)
			} else if srv, ok := r.Context().Value(caddyhttp.ServerCtxKey).(caddyhttp.Handler); ok && srv != nil {
				_ = srv.ServeHTTP(rec, virtReq)
			} else {
				return nil, nil, errors.New("titip: caddy: server context missing")
			}

			if rec.Code == http.StatusNotFound {
				return nil, nil, esi.ErrFallbackToHTTP
			}
			if rec.Code >= 400 {
				return nil, rec.Header().Clone(), fmt.Errorf("subrequest returned status %d", rec.Code)
			}

			return bytes.Clone(rec.Body.Bytes()), rec.Header().Clone(), nil
		}
		esiOpts = append(esiOpts, esi.WithInternalFetcher(internalFetcher))

		opts = append(opts, titip.WithESI(esiOpts...))
	}

	engine, err := titip.New(opts...)
	if err != nil {
		return fmt.Errorf("titip: failed to create engine: %w", err)
	}
	h.engine = engine
	registerEngine(h.id, engine)

	storageName := "unknown"
	if h.storageMod != nil {
		if mod, ok := h.storageMod.(caddy.Module); ok {
			storageName = strings.TrimPrefix(string(mod.CaddyModule().ID), "titip.storage.")
		}
	} else if app != nil && app.storageMod != nil {
		if mod, ok := app.storageMod.(caddy.Module); ok {
			storageName = strings.TrimPrefix(string(mod.CaddyModule().ID), "titip.storage.")
		}
	}
	if storageName == "unknown" {
		tName := strings.TrimPrefix(fmt.Sprintf("%T", store), "*")
		if before, _, ok := strings.Cut(tName, "."); ok {
			storageName = strings.ToLower(before)
		} else {
			storageName = strings.ToLower(tName)
		}
	}

	if h.logger != nil {
		if h.logger.Enabled(ctx, slog.LevelInfo) {
			h.logger.InfoContext(ctx, "module initialized",
				slog.String("storage", storageName),
			)
		}
		if h.logger.Enabled(ctx, slog.LevelDebug) {
			h.logger.DebugContext(ctx, "module registered",
				slog.String("id", h.id),
			)
		}
	}

	return nil
}

// Validate ensures the handler is configured properly.
func (h *Handler) Validate() error {
	if h.engine == nil {
		return fmt.Errorf("titip: engine was not provisioned")
	}
	return nil
}

// Cleanup gracefully shuts down the caching engine.
func (h *Handler) Cleanup() error {
	unregisterEngine(h.id)
	if h.logger != nil && h.logger.Enabled(context.Background(), slog.LevelDebug) {
		h.logger.DebugContext(context.Background(), "module cleaned up",
			slog.String("id", h.id),
		)
	}
	if h.engine != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = h.engine.Close(ctx)
	}
	return nil
}

// ServeHTTP implements the caddyhttp.MiddlewareHandler interface.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	// In Caddy, directives like `rewrite` or `try_files` can rewrite the request URI
	// (for example, routing multiple URL paths to a single entrypoint or front controller)
	// before middleware handlers like Titip execute. Caddy preserves the client's original,
	// un-rewritten request in the context under caddyhttp.OriginalRequestCtxKey.
	//
	// We extract the original request URL for Titip's cache key generation so that distinct
	// client-facing paths (e.g. "/about", "/products") do not collapse into the same cache key,
	// while preserving the current request's headers, context, and body.
	engineReq := r
	if !h.useRewrittenURL {
		if origReq, ok := r.Context().Value(caddyhttp.OriginalRequestCtxKey).(http.Request); ok && origReq.URL != nil {
			rCopy := *r
			rCopy.URL = origReq.URL
			rCopy.RequestURI = origReq.RequestURI
			engineReq = &rCopy
		}
	}

	// Bridge caddyhttp.Handler to standard http.Handler.
	// Downstream handlers must receive the rewritten request so downstream route matchers
	// and backend handlers see their expected target path.
	downstream := http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		downstreamReq := req
		if req.URL != r.URL {
			rCopy := *req
			rCopy.URL = r.URL
			rCopy.RequestURI = r.RequestURI
			downstreamReq = &rCopy
		}
		_ = next.ServeHTTP(rw, downstreamReq)
	})

	h.engine.ServeHTTP(w, engineReq, downstream)
	return nil
}

// UnmarshalCaddyfile sets up the handler from Caddyfile tokens.
func (h *Handler) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		for d.NextBlock(0) {
			switch d.Val() {
			case "storage":
				if !d.NextArg() {
					return d.ArgErr()
				}
				storageName := d.Val()
				unm, err := caddyfile.UnmarshalModule(d, "titip.storage."+storageName)
				if err != nil {
					return err
				}
				h.StorageRaw = caddyconfig.JSONModuleObject(unm, "name", storageName, nil)
			case "cache_status":
				if !d.NextArg() {
					return d.ArgErr()
				}
				h.CacheStatus = d.Val()
			case "respect_client_cache_control":
				if !d.NextArg() {
					return d.ArgErr()
				}
				val, err := strconv.ParseBool(d.Val())
				if err != nil {
					return d.Errf("invalid boolean value %q: %v", d.Val(), err)
				}
				h.RespectClientCacheControl = &val
			case "auto_invalidate_mutating_methods":
				if !d.NextArg() {
					return d.ArgErr()
				}
				val, err := strconv.ParseBool(d.Val())
				if err != nil {
					return d.Errf("invalid boolean value %q: %v", d.Val(), err)
				}
				h.AutoInvalidateMutatingMethods = &val
			case "convert_head_to_get":
				if !d.NextArg() {
					return d.ArgErr()
				}
				val, err := strconv.ParseBool(d.Val())
				if err != nil {
					return d.Errf("invalid boolean value %q: %v", d.Val(), err)
				}
				h.ConvertHeadToGet = &val
			case "background_fetch_timeout":
				if !d.NextArg() {
					return d.ArgErr()
				}
				h.BackgroundFetchTimeout = d.Val()
			case "storage_timeout":
				if !d.NextArg() {
					return d.ArgErr()
				}
				h.StorageTimeout = d.Val()
			case "tag_header":
				if !d.NextArg() {
					return d.ArgErr()
				}
				h.TagHeader = d.Val()
			case "key":
				if h.Key == nil {
					h.Key = new(KeyConfig)
				}
				if err := h.Key.unmarshalCaddyfile(d); err != nil {
					return err
				}
			case "esi":
				if h.ESI == nil {
					h.ESI = new(ESIConfig)
				}
				if err := h.ESI.unmarshalCaddyfile(d); err != nil {
					return err
				}
			case "use_rewritten_url":
				val := true
				if d.NextArg() {
					var err error
					val, err = strconv.ParseBool(d.Val())
					if err != nil {
						return d.Errf("invalid boolean value %q: %v", d.Val(), err)
					}
				}
				h.UseRewrittenURL = &val
			default:
				return d.Errf("unknown titip directive %q", d.Val())
			}
		}
	}
	return nil
}

func (kc *KeyConfig) unmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.NextBlock(1) {
		switch d.Val() {
		case "include_protocol":
			if !d.NextArg() {
				return d.ArgErr()
			}
			val, err := strconv.ParseBool(d.Val())
			if err != nil {
				return d.Errf("invalid boolean value for include_protocol: %v", err)
			}
			kc.IncludeProtocol = &val
		case "exclude_host":
			if !d.NextArg() {
				return d.ArgErr()
			}
			val, err := strconv.ParseBool(d.Val())
			if err != nil {
				return d.Errf("invalid boolean value for exclude_host: %v", err)
			}
			kc.ExcludeHost = &val
		case "include_host": // backward compatibility
			if !d.NextArg() {
				return d.ArgErr()
			}
			val, err := strconv.ParseBool(d.Val())
			if err != nil {
				return d.Errf("invalid boolean value for include_host: %v", err)
			}
			inv := !val
			kc.ExcludeHost = &inv
		case "exclude_query_string":
			if !d.NextArg() {
				return d.ArgErr()
			}
			val, err := strconv.ParseBool(d.Val())
			if err != nil {
				return d.Errf("invalid boolean value for exclude_query_string: %v", err)
			}
			kc.ExcludeQueryString = &val
		case "disable_query_string_sort":
			if !d.NextArg() {
				return d.ArgErr()
			}
			val, err := strconv.ParseBool(d.Val())
			if err != nil {
				return d.Errf("invalid boolean value for disable_query_string_sort: %v", err)
			}
			kc.DisableQueryStringSort = &val
		case "included_query_params", "query_whitelist":
			kc.IncludedQueryParams = append(kc.IncludedQueryParams, d.RemainingArgs()...)
		case "excluded_query_params", "query_blacklist":
			kc.ExcludedQueryParams = append(kc.ExcludedQueryParams, d.RemainingArgs()...)
		case "exclude_marketing_params", "ignore_marketing_params":
			val := true
			if d.NextArg() {
				var err error
				val, err = strconv.ParseBool(d.Val())
				if err != nil {
					return d.Errf("invalid boolean value for marketing params: %v", err)
				}
			}
			kc.ExcludeMarketingParams = &val
		case "included_header_names", "include_headers":
			kc.IncludedHeaderNames = append(kc.IncludedHeaderNames, d.RemainingArgs()...)
		case "included_cookie_names", "include_cookies":
			kc.IncludedCookieNames = append(kc.IncludedCookieNames, d.RemainingArgs()...)
		case "case_insensitive_path", "lowercase_path":
			val := true
			if d.NextArg() {
				var err error
				val, err = strconv.ParseBool(d.Val())
				if err != nil {
					return d.Errf("invalid boolean value for case_insensitive_path: %v", err)
				}
			}
			kc.CaseInsensitivePath = &val
		case "included_query_param_values", "query_enum":
			if !d.NextArg() {
				return d.ArgErr()
			}
			param := d.Val()
			vals := d.RemainingArgs()
			if len(vals) == 0 {
				return d.Errf("included_query_param_values requires at least one allowed value for parameter %q", param)
			}
			if kc.IncludedQueryParamValues == nil {
				kc.IncludedQueryParamValues = make(map[string][]string)
			}
			kc.IncludedQueryParamValues[param] = append(kc.IncludedQueryParamValues[param], vals...)
		case "query":
			if !d.NextArg() {
				return d.ArgErr()
			}
			mode := d.Val()
			switch strings.ToLower(mode) {
			case "all":
				f := false
				kc.ExcludeQueryString = &f
			case "none", "exclude_all":
				t := true
				kc.ExcludeQueryString = &t
			case "whitelist":
				kc.IncludedQueryParams = append(kc.IncludedQueryParams, d.RemainingArgs()...)
			case "blacklist":
				kc.ExcludedQueryParams = append(kc.ExcludedQueryParams, d.RemainingArgs()...)
			default:
				return d.Errf("unknown query mode %q (allowed: all, none, whitelist <params...>, blacklist <params...>)", mode)
			}
		default:
			return d.Errf("unknown key subdirective %q", d.Val())
		}
	}
	return nil
}

func (e *ESIConfig) unmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.NextBlock(1) {
		switch d.Val() {
		case "enabled":
			val := true
			if d.NextArg() {
				var err error
				val, err = strconv.ParseBool(d.Val())
				if err != nil {
					return d.Errf("invalid boolean value for esi enabled: %v", err)
				}
			}
			e.Enabled = &val
		case "header_required":
			val := true
			if d.NextArg() {
				var err error
				val, err = strconv.ParseBool(d.Val())
				if err != nil {
					return d.Errf("invalid boolean value for esi header_required: %v", err)
				}
			}
			e.HeaderRequired = &val
		case "max_depth":
			if !d.NextArg() {
				return d.ArgErr()
			}
			val, err := strconv.ParseUint(d.Val(), 10, 32)
			if err != nil {
				return d.Errf("invalid integer for max_depth: %v", err)
			}
			uVal := uint32(val)
			e.MaxDepth = &uVal
		case "max_timeout":
			if !d.NextArg() {
				return d.ArgErr()
			}
			e.MaxTimeout = d.Val()
		case "max_concurrent_requests":
			if !d.NextArg() {
				return d.ArgErr()
			}
			val, err := strconv.Atoi(d.Val())
			if err != nil {
				return d.Errf("invalid integer for max_concurrent_requests: %v", err)
			}
			e.MaxConcurrentRequests = &val
		case "block_private_ips":
			val := true
			if d.NextArg() {
				var err error
				val, err = strconv.ParseBool(d.Val())
				if err != nil {
					return d.Errf("invalid boolean value for block_private_ips: %v", err)
				}
			}
			e.BlockPrivateIPs = &val
		case "allowed_hosts":
			e.AllowedHosts = append(e.AllowedHosts, d.RemainingArgs()...)
		case "allow_private_ips_for_allowed_hosts":
			val := true
			if d.NextArg() {
				var err error
				val, err = strconv.ParseBool(d.Val())
				if err != nil {
					return d.Errf("invalid boolean value for allow_private_ips_for_allowed_hosts: %v", err)
				}
			}
			e.AllowPrivateIPsForAllowedHosts = &val
		case "max_response_size":
			if !d.NextArg() {
				return d.ArgErr()
			}
			e.MaxResponseSize = d.Val()
		case "forward_fragment_cookies":
			val := true
			if d.NextArg() {
				var err error
				val, err = strconv.ParseBool(d.Val())
				if err != nil {
					return d.Errf("invalid boolean value for forward_fragment_cookies: %v", err)
				}
			}
			e.ForwardFragmentCookies = &val
		case "error_marker":
			if !d.NextArg() {
				return d.ArgErr()
			}
			e.ErrorMarker = d.Val()
		default:
			return d.Errf("unknown esi subdirective %q", d.Val())
		}
	}
	return nil
}

func parseCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	var handler Handler
	err := handler.UnmarshalCaddyfile(h.Dispenser)
	return &handler, err
}

func parseByteSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	multi := int64(1)
	upper := strings.ToUpper(s)
	if strings.HasSuffix(upper, "GB") || strings.HasSuffix(upper, "G") {
		multi = 1024 * 1024 * 1024
		s = strings.TrimRight(s, "gGbB ")
	} else if strings.HasSuffix(upper, "MB") || strings.HasSuffix(upper, "M") {
		multi = 1024 * 1024
		s = strings.TrimRight(s, "mMbB ")
	} else if strings.HasSuffix(upper, "KB") || strings.HasSuffix(upper, "K") {
		multi = 1024
		s = strings.TrimRight(s, "kKbB ")
	} else if strings.HasSuffix(upper, "B") {
		s = strings.TrimRight(s, "bB ")
	}
	val, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, err
	}
	return val * multi, nil
}

func applyKeyConfig(target *titip.KeyConfig, src *KeyConfig) error {
	if src == nil {
		return nil
	}
	if src.IncludeProtocol != nil {
		target.IncludeProtocol = *src.IncludeProtocol
	}
	if src.ExcludeHost != nil {
		target.ExcludeHost = *src.ExcludeHost
	}
	if src.ExcludeQueryString != nil {
		target.ExcludeQueryString = *src.ExcludeQueryString
	} else if len(src.IncludedQueryParams) > 0 || len(src.IncludedQueryParamValues) > 0 {
		target.ExcludeQueryString = false
	}
	if src.DisableQueryStringSort != nil {
		target.DisableQueryStringSort = *src.DisableQueryStringSort
	}
	if len(src.IncludedQueryParams) > 0 {
		target.IncludedQueryParams = src.IncludedQueryParams
	}
	if len(src.ExcludedQueryParams) > 0 {
		target.ExcludedQueryParams = src.ExcludedQueryParams
	}
	if src.ExcludeMarketingParams != nil {
		target.ExcludeMarketingParams = *src.ExcludeMarketingParams
	}
	if len(src.IncludedHeaderNames) > 0 {
		target.IncludedHeaderNames = src.IncludedHeaderNames
	}
	if len(src.IncludedCookieNames) > 0 {
		target.IncludedCookieNames = src.IncludedCookieNames
	}
	if src.CaseInsensitivePath != nil {
		target.CaseInsensitivePath = *src.CaseInsensitivePath
	}
	if len(src.IncludedQueryParamValues) > 0 {
		if target.IncludedQueryParamValues == nil {
			target.IncludedQueryParamValues = make(map[string][]string, len(src.IncludedQueryParamValues))
		}
		for k, v := range src.IncludedQueryParamValues {
			target.IncludedQueryParamValues[k] = slices.Clone(v)
		}
	}
	return nil
}

func applyESIConfig(opts *[]esi.Option, src *ESIConfig) error {
	if src == nil {
		return nil
	}
	if src.HeaderRequired != nil {
		*opts = append(*opts, esi.WithHeaderRequired(*src.HeaderRequired))
	}
	if src.MaxDepth != nil {
		*opts = append(*opts, esi.WithMaxDepth(*src.MaxDepth))
	}
	if src.MaxTimeout != "" {
		d, err := caddy.ParseDuration(src.MaxTimeout)
		if err != nil {
			return fmt.Errorf("titip: invalid esi max_timeout duration %q: %w", src.MaxTimeout, err)
		}
		*opts = append(*opts, esi.WithMaxTimeout(d))
	}
	if src.MaxConcurrentRequests != nil {
		*opts = append(*opts, esi.WithMaxConcurrentRequests(*src.MaxConcurrentRequests))
	}
	if src.BlockPrivateIPs != nil {
		*opts = append(*opts, esi.WithAllowPrivateIPs(!*src.BlockPrivateIPs))
	}
	if len(src.AllowedHosts) > 0 {
		*opts = append(*opts, esi.WithAllowedHosts(src.AllowedHosts...))
	}
	if src.AllowPrivateIPsForAllowedHosts != nil {
		*opts = append(*opts, esi.WithAllowPrivateIPsForAllowedHosts(*src.AllowPrivateIPsForAllowedHosts))
	}
	if src.MaxResponseSize != "" {
		size, err := parseByteSize(src.MaxResponseSize)
		if err != nil {
			return fmt.Errorf("titip: invalid esi max_response_size %q: %w", src.MaxResponseSize, err)
		}
		*opts = append(*opts, esi.WithMaxResponseSize(size))
	}
	if src.ForwardFragmentCookies != nil {
		*opts = append(*opts, esi.WithDisableForwardCookies(!*src.ForwardFragmentCookies))
	}
	if src.ErrorMarker != "" {
		*opts = append(*opts, esi.WithIncludeErrorMarker(src.ErrorMarker))
	}
	return nil
}

// Interface guards
var (
	_ caddy.Module                = (*Handler)(nil)
	_ caddy.Provisioner           = (*Handler)(nil)
	_ caddy.Validator             = (*Handler)(nil)
	_ caddy.CleanerUpper          = (*Handler)(nil)
	_ caddyhttp.MiddlewareHandler = (*Handler)(nil)
	_ caddyfile.Unmarshaler       = (*Handler)(nil)
)
