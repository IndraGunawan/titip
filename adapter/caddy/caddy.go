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
//   api.example.com {
//       titip {
//           storage redis { address localhost:6379, key_prefix api: }
//       }
//       reverse_proxy localhost:9000
//   }
//
//   static.example.com {
//       titip {
//           storage redis { address localhost:6379, key_prefix static: }
//       }
//       reverse_proxy localhost:9001
//   }
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

// Handler implements the Caddy HTTP middleware for Titip caching.
type Handler struct {
	StorageRaw                    json.RawMessage `json:"storage,omitempty" caddy:"namespace=titip.storage inline_key=name"`
	CacheStatus                   string          `json:"cache_status,omitempty"`
	IgnoreClientCacheControl      *bool           `json:"ignore_client_cache_control,omitempty"`
	AutoInvalidateMutatingMethods *bool           `json:"auto_invalidate_mutating_methods,omitempty"`
	OriginTimeout                 string          `json:"origin_timeout,omitempty"`

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
		return fmt.Errorf("titip: storage module not installed or invalid (build caddy with xcaddy): %w", err)
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

	opts = append(opts, titip.WithLogger(ctx.Slogger()))

	// Cache-Status header mode
	switch strings.ToLower(h.CacheStatus) {
	case "rfc9211", "":
		opts = append(opts, titip.WithCacheStatusMode(titip.CacheStatusRFC9211))
	case "simple":
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
	// Bridge caddyhttp.Handler to standard http.Handler
	downstream := http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
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
				h.StorageRaw = caddyconfig.JSON(unm, nil)
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

// Interface guards
var (
	_ caddy.Module                = (*Handler)(nil)
	_ caddy.Provisioner           = (*Handler)(nil)
	_ caddy.Validator             = (*Handler)(nil)
	_ caddy.CleanerUpper          = (*Handler)(nil)
	_ caddyhttp.MiddlewareHandler = (*Handler)(nil)
	_ caddyfile.Unmarshaler       = (*Handler)(nil)
)
