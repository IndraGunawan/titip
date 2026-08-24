package caddy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
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
	"github.com/indragunawan/titip/storage"
)

func init() {
	caddy.RegisterModule(Handler{})
	httpcaddyfile.RegisterDirectiveOrder("titip", httpcaddyfile.Before, "reverse_proxy")
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
	IncludeProtocol       *bool    `json:"include_protocol,omitempty"`
	IncludeHost           *bool    `json:"include_host,omitempty"`
	IncludePath           *bool    `json:"include_path,omitempty"`
	QueryMode             string   `json:"query_mode,omitempty"`
	QueryWhitelist        []string `json:"query_whitelist,omitempty"`
	QueryBlacklist        []string `json:"query_blacklist,omitempty"`
	IgnoreMarketingParams *bool    `json:"ignore_marketing_params,omitempty"`
	IncludeHeaders        []string `json:"include_headers,omitempty"`
	IncludeCookies        []string `json:"include_cookies,omitempty"`
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
	IgnoreClientCacheControl      *bool           `json:"ignore_client_cache_control,omitempty"`
	AutoInvalidateMutatingMethods *bool           `json:"auto_invalidate_mutating_methods,omitempty"`
	OriginTimeout                 string          `json:"origin_timeout,omitempty"`
	TagHeader                     string          `json:"tag_header,omitempty"`
	Key                           *KeyConfig      `json:"key,omitempty"`
	ESI                           *ESIConfig      `json:"esi,omitempty"`

	storageMod StorageModule
	engine     *titip.Titip
	id         string
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

	// 1. Provision dynamic storage guest module
	if len(h.StorageRaw) == 0 {
		return fmt.Errorf("titip: storage configuration is required")
	}

	val, err := ctx.LoadModule(h, "StorageRaw")
	if err != nil {
		return fmt.Errorf("titip: storage module not installed or invalid: %w", err)
	}

	sMod, ok := val.(StorageModule)
	if !ok {
		return fmt.Errorf("titip: module does not implement StorageModule")
	}
	h.storageMod = sMod

	store := sMod.Storage()
	if store == nil {
		return fmt.Errorf("titip: storage backend failed to initialize")
	}

	// 2. Configure Titip options
	opts := []titip.Option{
		titip.WithStorage(store),
		titip.WithMetrics(ctx.GetMetricsRegistry()),
	}

	func() {
		defer func() {
			_ = recover()
		}()
		if l := ctx.Slogger(); l != nil {
			opts = append(opts, titip.WithLogger(l))
		}
	}()

	// Cache-Status header mode
	switch strings.ToLower(h.CacheStatus) {
	case "rfc9211":
		opts = append(opts, titip.WithCacheStatusMode(titip.CacheStatusRFC9211))
	case "simple", "":
		opts = append(opts, titip.WithCacheStatusMode(titip.CacheStatusSimpleToken))
	case "none", "disabled":
		opts = append(opts, titip.WithCacheStatusMode(titip.CacheStatusNone))
	default:
		return fmt.Errorf("titip: unknown cache_status mode %q (allowed: rfc9211, simple, none)", h.CacheStatus)
	}

	if h.IgnoreClientCacheControl != nil {
		opts = append(opts, titip.WithIgnoreClientCacheControl(*h.IgnoreClientCacheControl))
	}
	if h.AutoInvalidateMutatingMethods != nil {
		opts = append(opts, titip.WithAutoInvalidateMutatingMethods(*h.AutoInvalidateMutatingMethods))
	}

	if h.OriginTimeout != "" {
		d, err := caddy.ParseDuration(h.OriginTimeout)
		if err != nil {
			return fmt.Errorf("titip: invalid origin_timeout duration %q: %w", h.OriginTimeout, err)
		}
		opts = append(opts, titip.WithOriginTimeout(d))
	}

	if h.TagHeader != "" {
		opts = append(opts, titip.WithTagHeaderName(h.TagHeader))
	}

	// Key configuration
	if h.Key != nil {
		keyCfg := *titip.DefaultKeyConfig()
		if h.Key.IncludeProtocol != nil {
			keyCfg.IncludeProtocol = *h.Key.IncludeProtocol
		}
		if h.Key.IncludeHost != nil {
			keyCfg.IncludeHost = *h.Key.IncludeHost
		}
		if h.Key.IncludePath != nil {
			keyCfg.IncludePath = *h.Key.IncludePath
		}
		if h.Key.QueryMode != "" {
			switch strings.ToLower(h.Key.QueryMode) {
			case "all":
				keyCfg.QueryMode = titip.QueryParamsAll
			case "none", "exclude_all":
				keyCfg.QueryMode = titip.QueryParamsNone
			case "whitelist":
				keyCfg.QueryMode = titip.QueryParamsWhitelist
			case "blacklist":
				keyCfg.QueryMode = titip.QueryParamsBlacklist
			default:
				return fmt.Errorf("titip: unknown query_mode %q (allowed: all, none, whitelist, blacklist)", h.Key.QueryMode)
			}
		}
		if len(h.Key.QueryWhitelist) > 0 {
			keyCfg.QueryMode = titip.QueryParamsWhitelist
			keyCfg.QueryWhitelist = h.Key.QueryWhitelist
		}
		if len(h.Key.QueryBlacklist) > 0 {
			if keyCfg.QueryMode != titip.QueryParamsWhitelist && keyCfg.QueryMode != titip.QueryParamsNone {
				keyCfg.QueryMode = titip.QueryParamsBlacklist
			}
			keyCfg.QueryBlacklist = h.Key.QueryBlacklist
		}
		if h.Key.IgnoreMarketingParams != nil && *h.Key.IgnoreMarketingParams {
			keyCfg.WithIgnoredMarketingParams()
		}
		if len(h.Key.IncludeHeaders) > 0 {
			keyCfg.IncludeHeaders = h.Key.IncludeHeaders
		}
		if len(h.Key.IncludeCookies) > 0 {
			keyCfg.IncludeCookies = h.Key.IncludeCookies
		}
		opts = append(opts, titip.WithKeyConfig(keyCfg))
	}

	// ESI configuration
	if h.ESI != nil {
		esiCfg := titip.ESIConfig{
			Enabled:                true,
			HeaderRequired:         false,
			MaxDepth:               3,
			MaxTimeout:             30 * time.Second,
			MaxConcurrentRequests:  8,
			BlockPrivateIPs:        true,
			MaxResponseSize:        10 * 1024 * 1024,
			ForwardFragmentCookies: true,
		}
		if h.ESI.Enabled != nil {
			esiCfg.Enabled = *h.ESI.Enabled
		}
		if h.ESI.HeaderRequired != nil {
			esiCfg.HeaderRequired = *h.ESI.HeaderRequired
		}
		if h.ESI.MaxDepth != nil {
			esiCfg.MaxDepth = *h.ESI.MaxDepth
		}
		if h.ESI.MaxTimeout != "" {
			d, err := caddy.ParseDuration(h.ESI.MaxTimeout)
			if err != nil {
				return fmt.Errorf("titip: invalid esi max_timeout duration %q: %w", h.ESI.MaxTimeout, err)
			}
			esiCfg.MaxTimeout = d
		}
		if h.ESI.MaxConcurrentRequests != nil {
			esiCfg.MaxConcurrentRequests = *h.ESI.MaxConcurrentRequests
		}
		if h.ESI.BlockPrivateIPs != nil {
			esiCfg.BlockPrivateIPs = *h.ESI.BlockPrivateIPs
		}
		if len(h.ESI.AllowedHosts) > 0 {
			esiCfg.AllowedHosts = h.ESI.AllowedHosts
		}
		if h.ESI.AllowPrivateIPsForAllowedHosts != nil {
			esiCfg.AllowPrivateIPsForAllowedHosts = *h.ESI.AllowPrivateIPsForAllowedHosts
		}
		if h.ESI.MaxResponseSize != "" {
			size, err := parseByteSize(h.ESI.MaxResponseSize)
			if err != nil {
				return fmt.Errorf("titip: invalid esi max_response_size %q: %w", h.ESI.MaxResponseSize, err)
			}
			esiCfg.MaxResponseSize = size
		}
		if h.ESI.ForwardFragmentCookies != nil {
			esiCfg.ForwardFragmentCookies = *h.ESI.ForwardFragmentCookies
		}
		if h.ESI.ErrorMarker != "" {
			esiCfg.IncludeErrorMarker = h.ESI.ErrorMarker
		}
		opts = append(opts, titip.WithESI(esiCfg))
	}

	engine, err := titip.New(opts...)
	if err != nil {
		return fmt.Errorf("titip: failed to create engine: %w", err)
	}
	h.engine = engine
	registerEngine(h.id, engine)

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
	if h.engine != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = h.engine.Close(ctx)
	}
	return nil
}

// ServeHTTP implements the caddyhttp.MiddlewareHandler interface.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	if r.Body == nil {
		r.Body = http.NoBody
	}
	// Bridge caddyhttp.Handler to standard http.Handler
	downstream := http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if req.Body == nil {
			req.Body = http.NoBody
		}
		if repl, ok := req.Context().Value(caddy.ReplacerCtxKey).(*caddy.Replacer); ok && repl != nil && req.URL != nil {
			repl.Set("http.request.uri.path", req.URL.Path)
			repl.Set("http.request.orig_uri.path", req.URL.Path)
			repl.Set("http.request.uri", req.URL.RequestURI())
			repl.Set("http.request.orig_uri", req.URL.RequestURI())
		}
		_ = next.ServeHTTP(rw, req)
	})

	h.engine.Handler(downstream).ServeHTTP(w, r)
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
			case "ignore_client_cache_control":
				if !d.NextArg() {
					return d.ArgErr()
				}
				val, err := strconv.ParseBool(d.Val())
				if err != nil {
					return d.Errf("invalid boolean value %q: %v", d.Val(), err)
				}
				h.IgnoreClientCacheControl = &val
			case "auto_invalidate_mutating_methods":
				if !d.NextArg() {
					return d.ArgErr()
				}
				val, err := strconv.ParseBool(d.Val())
				if err != nil {
					return d.Errf("invalid boolean value %q: %v", d.Val(), err)
				}
				h.AutoInvalidateMutatingMethods = &val
			case "origin_timeout":
				if !d.NextArg() {
					return d.ArgErr()
				}
				h.OriginTimeout = d.Val()
			case "tag_header":
				if !d.NextArg() {
					return d.ArgErr()
				}
				h.TagHeader = d.Val()
			case "key":
				if h.Key == nil {
					h.Key = new(KeyConfig)
				}
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
						h.Key.IncludeProtocol = &val
					case "include_host":
						if !d.NextArg() {
							return d.ArgErr()
						}
						val, err := strconv.ParseBool(d.Val())
						if err != nil {
							return d.Errf("invalid boolean value for include_host: %v", err)
						}
						h.Key.IncludeHost = &val
					case "include_path":
						if !d.NextArg() {
							return d.ArgErr()
						}
						val, err := strconv.ParseBool(d.Val())
						if err != nil {
							return d.Errf("invalid boolean value for include_path: %v", err)
						}
						h.Key.IncludePath = &val
					case "query":
						if !d.NextArg() {
							return d.ArgErr()
						}
						mode := d.Val()
						switch strings.ToLower(mode) {
						case "all":
							h.Key.QueryMode = "all"
						case "none", "exclude_all":
							h.Key.QueryMode = "none"
						case "whitelist":
							h.Key.QueryMode = "whitelist"
							h.Key.QueryWhitelist = append(h.Key.QueryWhitelist, d.RemainingArgs()...)
						case "blacklist":
							h.Key.QueryMode = "blacklist"
							h.Key.QueryBlacklist = append(h.Key.QueryBlacklist, d.RemainingArgs()...)
						default:
							return d.Errf("unknown query mode %q (allowed: all, none, whitelist <params...>, blacklist <params...>)", mode)
						}
					case "query_whitelist":
						h.Key.QueryMode = "whitelist"
						h.Key.QueryWhitelist = append(h.Key.QueryWhitelist, d.RemainingArgs()...)
					case "query_blacklist":
						h.Key.QueryMode = "blacklist"
						h.Key.QueryBlacklist = append(h.Key.QueryBlacklist, d.RemainingArgs()...)
					case "ignore_marketing_params":
						val := true
						if d.NextArg() {
							var err error
							val, err = strconv.ParseBool(d.Val())
							if err != nil {
								return d.Errf("invalid boolean value for ignore_marketing_params: %v", err)
							}
						}
						h.Key.IgnoreMarketingParams = &val
					case "include_headers":
						h.Key.IncludeHeaders = append(h.Key.IncludeHeaders, d.RemainingArgs()...)
					case "include_cookies":
						h.Key.IncludeCookies = append(h.Key.IncludeCookies, d.RemainingArgs()...)
					default:
						return d.Errf("unknown key subdirective %q", d.Val())
					}
				}
			case "esi":
				if h.ESI == nil {
					h.ESI = new(ESIConfig)
				}
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
						h.ESI.Enabled = &val
					case "header_required":
						val := true
						if d.NextArg() {
							var err error
							val, err = strconv.ParseBool(d.Val())
							if err != nil {
								return d.Errf("invalid boolean value for esi header_required: %v", err)
							}
						}
						h.ESI.HeaderRequired = &val
					case "max_depth":
						if !d.NextArg() {
							return d.ArgErr()
						}
						val, err := strconv.ParseUint(d.Val(), 10, 32)
						if err != nil {
							return d.Errf("invalid integer for max_depth: %v", err)
						}
						uVal := uint32(val)
						h.ESI.MaxDepth = &uVal
					case "max_timeout":
						if !d.NextArg() {
							return d.ArgErr()
						}
						h.ESI.MaxTimeout = d.Val()
					case "max_concurrent_requests":
						if !d.NextArg() {
							return d.ArgErr()
						}
						val, err := strconv.Atoi(d.Val())
						if err != nil {
							return d.Errf("invalid integer for max_concurrent_requests: %v", err)
						}
						h.ESI.MaxConcurrentRequests = &val
					case "block_private_ips":
						val := true
						if d.NextArg() {
							var err error
							val, err = strconv.ParseBool(d.Val())
							if err != nil {
								return d.Errf("invalid boolean value for block_private_ips: %v", err)
							}
						}
						h.ESI.BlockPrivateIPs = &val
					case "allowed_hosts":
						h.ESI.AllowedHosts = append(h.ESI.AllowedHosts, d.RemainingArgs()...)
					case "allow_private_ips_for_allowed_hosts":
						val := true
						if d.NextArg() {
							var err error
							val, err = strconv.ParseBool(d.Val())
							if err != nil {
								return d.Errf("invalid boolean value for allow_private_ips_for_allowed_hosts: %v", err)
							}
						}
						h.ESI.AllowPrivateIPsForAllowedHosts = &val
					case "max_response_size":
						if !d.NextArg() {
							return d.ArgErr()
						}
						h.ESI.MaxResponseSize = d.Val()
					case "forward_fragment_cookies":
						val := true
						if d.NextArg() {
							var err error
							val, err = strconv.ParseBool(d.Val())
							if err != nil {
								return d.Errf("invalid boolean value for forward_fragment_cookies: %v", err)
							}
						}
						h.ESI.ForwardFragmentCookies = &val
					case "error_marker":
						if !d.NextArg() {
							return d.ArgErr()
						}
						h.ESI.ErrorMarker = d.Val()
					default:
						return d.Errf("unknown esi subdirective %q", d.Val())
					}
				}
			default:
				return d.Errf("unknown titip directive %q", d.Val())
			}
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

// Interface guards
var (
	_ caddy.Module                = (*Handler)(nil)
	_ caddy.Provisioner           = (*Handler)(nil)
	_ caddy.Validator             = (*Handler)(nil)
	_ caddy.CleanerUpper          = (*Handler)(nil)
	_ caddyhttp.MiddlewareHandler = (*Handler)(nil)
	_ caddyfile.Unmarshaler       = (*Handler)(nil)
)
