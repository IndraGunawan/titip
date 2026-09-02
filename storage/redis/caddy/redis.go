package caddy

import (
	"fmt"
	"strconv"
	"strings"

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
	Addresses         []string `json:"addresses,omitempty"`
	KeyPrefix         string   `json:"key_prefix,omitempty"`
	Username          string   `json:"username,omitempty"`
	Password          string   `json:"password,omitempty"`
	DB                int      `json:"db,omitempty"`
	PipelineMultiplex int      `json:"pipeline_multiplex,omitempty"`

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
	var addrs []string
	for _, raw := range r.Addresses {
		replaced := repl.ReplaceKnown(raw, "")
		for a := range strings.SplitSeq(replaced, ",") {
			trimmed := strings.TrimSpace(a)
			if trimmed != "" {
				addrs = append(addrs, trimmed)
			}
		}
	}
	if len(addrs) == 0 {
		addrs = []string{"127.0.0.1:6379"}
	}

	prefix := repl.ReplaceKnown(r.KeyPrefix, "")
	if prefix == "" {
		prefix = "titip:"
	}
	username := repl.ReplaceKnown(r.Username, "")
	password := repl.ReplaceKnown(r.Password, "")

	opt := rueidis.ClientOption{
		InitAddress:       addrs,
		Username:          username,
		Password:          password,
		SelectDB:          r.DB,
		PipelineMultiplex: r.PipelineMultiplex,
	}

	client, err := rueidis.NewClient(opt)
	if err != nil {
		return fmt.Errorf("titip.storage.redis: connect error: %w", err)
	}
	r.client = client

	store, err := storageRedis.New(
		client,
		storageRedis.WithKeyPrefix(prefix),
		storageRedis.WithLogger(ctx.Slogger()),
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
			case "address", "addresses":
				args := d.RemainingArgs()
				if len(args) == 0 {
					return d.ArgErr()
				}
				for _, arg := range args {
					for a := range strings.SplitSeq(arg, ",") {
						trimmed := strings.TrimSpace(a)
						if trimmed != "" {
							r.Addresses = append(r.Addresses, trimmed)
						}
					}
				}
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
			case "pipeline_multiplex":
				if !d.NextArg() {
					return d.ArgErr()
				}
				pm, err := strconv.Atoi(d.Val())
				if err != nil {
					return d.Errf("invalid pipeline_multiplex number %q: %v", d.Val(), err)
				}
				r.PipelineMultiplex = pm
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
