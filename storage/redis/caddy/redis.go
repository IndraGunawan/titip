package caddy

import (
	"fmt"
	"strconv"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/redis/rueidis"

	"github.com/indragunawan/titip/storage"
	storageRedis "github.com/indragunawan/titip/storage/redis"
)

func init() {
	caddy.RegisterModule(RedisStorage{})
}

// RedisStorage implements a Caddy storage guest module under the "titip.storage.redis" namespace.
type RedisStorage struct {
	Address         string `json:"address,omitempty"`
	KeyPrefix       string `json:"key_prefix,omitempty"`
	Username        string `json:"username,omitempty"`
	Password        string `json:"password,omitempty"`
	DB              int    `json:"db,omitempty"`
	ClientSideCache bool   `json:"client_side_cache,omitempty"`

	store  storage.Storage
	client rueidis.Client
}

// CaddyModule returns the Caddy module information.
func (RedisStorage) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "titip.storage.redis",
		New: func() caddy.Module { return new(RedisStorage) },
	}
}

// Provision sets up the Redis client and storage backend.
func (r *RedisStorage) Provision(ctx caddy.Context) error {
	repl := caddy.NewReplacer()
	addr := repl.ReplaceKnown(r.Address, "")
	if addr == "" {
		addr = "localhost:6380"
	}
	prefix := repl.ReplaceKnown(r.KeyPrefix, "")
	if prefix == "" {
		prefix = "titip:"
	}
	username := repl.ReplaceKnown(r.Username, "")
	password := repl.ReplaceKnown(r.Password, "")

	opt := rueidis.ClientOption{
		InitAddress:  []string{addr},
		Username:     username,
		Password:     password,
		SelectDB:     r.DB,
		DisableCache: !r.ClientSideCache,
	}

	client, err := rueidis.NewClient(opt)
	if err != nil {
		return fmt.Errorf("titip.storage.redis: connect error: %w", err)
	}
	r.client = client

	store, err := storageRedis.New(
		storageRedis.WithClient(client),
		storageRedis.WithKeyPrefix(prefix),
	)
	if err != nil {
		client.Close()
		return fmt.Errorf("titip.storage.redis: init storage error: %w", err)
	}
	r.store = store

	return nil
}

// Cleanup closes the Redis client and storage connection.
func (r *RedisStorage) Cleanup() error {
	if r.store != nil {
		_ = r.store.Close()
	}
	if r.client != nil {
		r.client.Close()
	}
	return nil
}

// Storage returns the initialized storage.Storage interface.
func (r *RedisStorage) Storage() storage.Storage {
	return r.store
}

// UnmarshalCaddyfile sets up the storage module from Caddyfile tokens.
func (r *RedisStorage) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		for d.NextBlock(0) {
			switch d.Val() {
			case "address":
				if !d.NextArg() {
					return d.ArgErr()
				}
				r.Address = d.Val()
			case "key_prefix":
				if !d.NextArg() {
					return d.ArgErr()
				}
				r.KeyPrefix = d.Val()
			case "username":
				if !d.NextArg() {
					return d.ArgErr()
				}
				r.Username = d.Val()
			case "password":
				if !d.NextArg() {
					return d.ArgErr()
				}
				r.Password = d.Val()
			case "db":
				if !d.NextArg() {
					return d.ArgErr()
				}
				db, err := strconv.Atoi(d.Val())
				if err != nil {
					return d.Errf("invalid db number %q: %v", d.Val(), err)
				}
				r.DB = db
			case "client_side_cache":
				if !d.NextArg() {
					return d.ArgErr()
				}
				csc, err := strconv.ParseBool(d.Val())
				if err != nil {
					return d.Errf("invalid client_side_cache value %q: %v", d.Val(), err)
				}
				r.ClientSideCache = csc
			default:
				return d.Errf("unknown subdirective %q", d.Val())
			}
		}
	}
	return nil
}

// Interface guards
var (
	_ caddy.Module          = (*RedisStorage)(nil)
	_ caddy.Provisioner     = (*RedisStorage)(nil)
	_ caddy.CleanerUpper    = (*RedisStorage)(nil)
	_ caddyfile.Unmarshaler = (*RedisStorage)(nil)
)
