package caddy

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
)

func init() {
	caddy.RegisterModule(App{})
	httpcaddyfile.RegisterGlobalOption("titip", parseGlobalOption)
}

// App is the global Caddy application for Titip caching.
// It manages shared storage and global configuration defaults across all sites.
type App struct {
	// StorageRaw is the raw JSON configuration for global storage.
	StorageRaw json.RawMessage `json:"storage,omitempty" caddy:"namespace=titip.storage inline_key=name"`

	// CacheStatus specifies the default Cache-Status header format ("rfc9211", "simple", or "none").
	CacheStatus string `json:"cache_status,omitempty"`

	// RespectClientCacheControl obeys client no-cache/no-store requests when true (defaults to false).
	RespectClientCacheControl *bool `json:"respect_client_cache_control,omitempty"`

	// AutoInvalidateMutatingMethods purges cached GET entries when receiving POST/PUT/DELETE.
	AutoInvalidateMutatingMethods *bool `json:"auto_invalidate_mutating_methods,omitempty"`

	// ConvertHeadToGet converts cold HEAD requests to GET when querying origin.
	ConvertHeadToGet *bool `json:"convert_head_to_get,omitempty"`

	// BackgroundFetchTimeout specifies the default timeout for background revalidations.
	BackgroundFetchTimeout string `json:"background_fetch_timeout,omitempty"`

	// StorageTimeout specifies the default timeout for storage operations.
	StorageTimeout string `json:"storage_timeout,omitempty"`

	// KeyConfig defines default cache key generation rules.
	KeyConfig *KeyConfig `json:"key_config,omitempty"`

	// ESI defines default Edge Side Includes parameters.
	ESI *ESIConfig `json:"esi,omitempty"`

	// UseRewrittenURL uses the rewritten request URL rather than client original request URL.
	UseRewrittenURL *bool `json:"use_rewritten_url,omitempty"`

	storageMod StorageModule
}

// CaddyModule returns the Caddy module information.
func (App) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "titip",
		New: func() caddy.Module { return new(App) },
	}
}

// Provision sets up the global Titip application module.
func (a *App) Provision(ctx caddy.Context) error {
	if len(a.StorageRaw) > 0 {
		mod, err := ctx.LoadModule(a, "StorageRaw")
		if err != nil {
			return fmt.Errorf("titip: loading global storage module: %w", err)
		}
		sm, ok := mod.(StorageModule)
		if !ok {
			return fmt.Errorf("titip: global storage module does not implement StorageModule")
		}
		a.storageMod = sm
	}
	return nil
}

// Start implements caddy.App.
func (a *App) Start() error {
	return nil
}

// Stop implements caddy.App.
func (a *App) Stop() error {
	return nil
}

// Cleanup implements caddy.CleanerUpper.
func (a *App) Cleanup() error {
	if a.storageMod != nil {
		if s := a.storageMod.Storage(); s != nil {
			_ = s.Close()
		}
	}
	return nil
}

// parseGlobalOption parses the global `titip { ... }` block inside Caddyfile global options.
func parseGlobalOption(d *caddyfile.Dispenser, prev any) (any, error) {
	app := new(App)
	for d.Next() {
		for d.NextBlock(0) {
			switch d.Val() {
			case "storage":
				if !d.NextArg() {
					return nil, d.ArgErr()
				}
				name := d.Val()
				modID := "titip.storage." + name
				unm, err := caddyfile.UnmarshalModule(d, modID)
				if err != nil {
					return nil, err
				}
				app.StorageRaw = caddyconfig.JSONModuleObject(unm, "name", name, nil)

			case "cache_status":
				if !d.NextArg() {
					return nil, d.ArgErr()
				}
				app.CacheStatus = d.Val()

			case "respect_client_cache_control":
				if !d.NextArg() {
					return nil, d.ArgErr()
				}
				b, err := strconv.ParseBool(d.Val())
				if err != nil {
					return nil, d.Errf("invalid boolean for respect_client_cache_control: %v", err)
				}
				app.RespectClientCacheControl = &b

			case "auto_invalidate_mutating_methods":
				if !d.NextArg() {
					return nil, d.ArgErr()
				}
				b, err := strconv.ParseBool(d.Val())
				if err != nil {
					return nil, d.Errf("invalid boolean for auto_invalidate_mutating_methods: %v", err)
				}
				app.AutoInvalidateMutatingMethods = &b

			case "convert_head_to_get":
				if !d.NextArg() {
					return nil, d.ArgErr()
				}
				b, err := strconv.ParseBool(d.Val())
				if err != nil {
					return nil, d.Errf("invalid boolean for convert_head_to_get: %v", err)
				}
				app.ConvertHeadToGet = &b

			case "background_fetch_timeout":
				if !d.NextArg() {
					return nil, d.ArgErr()
				}
				app.BackgroundFetchTimeout = d.Val()

			case "storage_timeout":
				if !d.NextArg() {
					return nil, d.ArgErr()
				}
				app.StorageTimeout = d.Val()

			case "key", "key_config":
				kc := new(KeyConfig)
				if err := kc.unmarshalCaddyfile(d); err != nil {
					return nil, err
				}
				app.KeyConfig = kc

			case "esi":
				esi := new(ESIConfig)
				if err := esi.unmarshalCaddyfile(d); err != nil {
					return nil, err
				}
				app.ESI = esi

			case "use_rewritten_url":
				val := true
				if d.NextArg() {
					var err error
					val, err = strconv.ParseBool(d.Val())
					if err != nil {
						return nil, d.Errf("invalid boolean for use_rewritten_url: %v", err)
					}
				}
				app.UseRewrittenURL = &val

			default:
				return nil, d.Errf("unknown global titip directive: %s", d.Val())
			}
		}
	}

	return httpcaddyfile.App{
		Name:  "titip",
		Value: caddyconfig.JSON(app, nil),
	}, nil
}
