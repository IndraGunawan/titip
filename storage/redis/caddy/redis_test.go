package caddy

import (
	"os"
	"testing"

	caddymain "github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

func getTestRedisAddr() string {
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		return addr
	}
	return "127.0.0.1:6379"
}

func TestRedisStorage_UnmarshalCaddyfile(t *testing.T) {
	config := `redis {
		address 127.0.0.1:6379 127.0.0.1:6380,127.0.0.1:6381
		key_prefix custom:
		username testuser
		password testpass
		db 2
	}`

	d := caddyfile.NewTestDispenser(config)
	var r RedisStorage
	if err := r.UnmarshalCaddyfile(d); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	expectedAddrs := []string{"127.0.0.1:6379", "127.0.0.1:6380", "127.0.0.1:6381"}
	if len(r.Addresses) != len(expectedAddrs) {
		t.Fatalf("expected %d addresses, got %v", len(expectedAddrs), r.Addresses)
	}
	for i, addr := range expectedAddrs {
		if r.Addresses[i] != addr {
			t.Errorf("address[%d] expected %q, got %q", i, addr, r.Addresses[i])
		}
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
}

func TestRedisStorage_ProvisionAndCleanup(t *testing.T) {
	addr := getTestRedisAddr()
	r := &RedisStorage{
		Addresses: []string{addr},
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
