package caddy

import (
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/indragunawan/titip/internal/teststore"
	"github.com/indragunawan/titip/storage"
)

func init() {
	caddy.RegisterModule(TestStorage{})
}

// TestStorage implements a test-only Caddy storage guest module under "titip.storage.test".
type TestStorage struct {
	store *teststore.Store
}

// CaddyModule returns the Caddy module information.
func (TestStorage) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID: "titip.storage.test",
		New: func() caddy.Module {
			return &TestStorage{store: teststore.New()}
		},
	}
}

// Provision initializes the test store.
func (t *TestStorage) Provision(ctx caddy.Context) error {
	if t.store == nil {
		t.store = teststore.New()
	}
	return nil
}

// Cleanup closes the test store.
func (t *TestStorage) Cleanup() error {
	if t.store != nil {
		_ = t.store.Close()
	}
	return nil
}

// Storage returns the underlying storage.Storage implementation.
func (t *TestStorage) Storage() storage.Storage {
	return t.store
}

// UnmarshalCaddyfile consumes optional tokens for "storage test".
func (t *TestStorage) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		for d.NextBlock(0) {
			// consume any optional test config block if given
		}
	}
	return nil
}

// Interface guards
var (
	_ caddy.Module          = (*TestStorage)(nil)
	_ caddy.Provisioner     = (*TestStorage)(nil)
	_ caddy.CleanerUpper    = (*TestStorage)(nil)
	_ caddyfile.Unmarshaler = (*TestStorage)(nil)
	_ StorageModule         = (*TestStorage)(nil)
)
