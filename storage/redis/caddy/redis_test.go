package caddy

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	caddymain "github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

func TestRedisStorage_UnmarshalCaddyfile(t *testing.T) {
	config := `redis {
		address 127.0.0.1:6380
		key_prefix custom:
		username testuser
		password testpass
		db 2
		client_side_cache false
	}`

	d := caddyfile.NewTestDispenser(config)
	var r RedisStorage
	if err := r.UnmarshalCaddyfile(d); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if r.Address != "127.0.0.1:6380" {
		t.Errorf("expected address 127.0.0.1:6380, got %s", r.Address)
	}
	if r.KeyPrefix != "custom:" {
		t.Errorf("expected key_prefix custom:, got %s", r.KeyPrefix)
	}
	if r.Username != "testuser" {
		t.Errorf("expected username testuser, got %s", r.Username)
	}
	if r.Password != "testpass" {
		t.Errorf("expected password testpass, got %s", r.Password)
	}
	if r.DB != 2 {
		t.Errorf("expected db 2, got %d", r.DB)
	}
	if r.ClientSideCache != false {
		t.Errorf("expected client_side_cache false, got %v", r.ClientSideCache)
	}
}

func TestRedisStorage_ProvisionAndCleanup(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	r := &RedisStorage{
		Address:   mr.Addr(),
		KeyPrefix: "caddy_test:",
	}

	ctx, cancel := caddymain.NewContext(caddymain.Context{Context: t.Context()})
	defer cancel()

	if err := r.Provision(ctx); err != nil {
		t.Fatalf("provision failed: %v", err)
	}

	if r.Storage() == nil {
		t.Fatalf("expected initialized storage")
	}

	if err := r.Cleanup(); err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}
}
