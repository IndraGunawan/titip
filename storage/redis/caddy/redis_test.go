package caddy

import (
	"encoding/json"
	"os"
	"strings"
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
		pipeline_multiplex 4
	}`

	d := caddyfile.NewTestDispenser(config)
	var r RedisStorage
	if err := r.UnmarshalCaddyfile(d); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	expectedAddrs := []string{"127.0.0.1:6379", "127.0.0.1:6380", "127.0.0.1:6381"}
	if len(r.Address) != len(expectedAddrs) {
		t.Fatalf("expected %d addresses, got %v", len(expectedAddrs), r.Address)
	}
	for i, addr := range expectedAddrs {
		if r.Address[i] != addr {
			t.Errorf("address[%d] expected %q, got %q", i, addr, r.Address[i])
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
	if r.PipelineMultiplex != 4 {
		t.Errorf("expected pipeline_multiplex 4, got %d", r.PipelineMultiplex)
	}
}

func TestRedisStorage_UnmarshalCaddyfile_URL(t *testing.T) {
	//nolint:gosec // false positive: dummy test credentials in test configuration
	config := `redis {
		url redis://testuser:testpass@127.0.0.1:6379/3
		key_prefix url_test:
		pipeline_multiplex 2
	}`

	d := caddyfile.NewTestDispenser(config)
	var r RedisStorage
	if err := r.UnmarshalCaddyfile(d); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if r.URL != "redis://testuser:testpass@127.0.0.1:6379/3" {
		t.Errorf("expected url redis://testuser:testpass@127.0.0.1:6379/3, got %s", r.URL)
	}
	if r.KeyPrefix != "url_test:" {
		t.Errorf("expected key_prefix url_test:, got %s", r.KeyPrefix)
	}
	if r.PipelineMultiplex != 2 {
		t.Errorf("expected pipeline_multiplex 2, got %d", r.PipelineMultiplex)
	}
}

func TestRedisStorage_Provision_URL_MutualExclusivity(t *testing.T) {
	ctx, cancel := caddymain.NewContext(caddymain.Context{Context: t.Context()})
	defer cancel()

	// 1. URL + address -> error
	r1 := &RedisStorage{
		URL:     "redis://127.0.0.1:6379/0",
		Address: []string{"127.0.0.1:6380"},
	}
	if err := r1.Provision(ctx); err == nil {
		t.Error("expected error for URL + address, got nil")
	}

	// 2. URL + db (non-zero) -> error
	r2 := &RedisStorage{
		URL: "redis://127.0.0.1:6379/0",
		DB:  1,
	}
	if err := r2.Provision(ctx); err == nil {
		t.Error("expected error for URL + db, got nil")
	}

	// 2b. URL + db (0 via Caddyfile) -> error
	dConflict := caddyfile.NewTestDispenser(`redis {
		url redis://127.0.0.1:6379/4
		db 0
	}`)
	var r2b RedisStorage
	if err := r2b.UnmarshalCaddyfile(dConflict); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if err := r2b.Provision(ctx); err == nil {
		t.Error("expected error for Caddyfile with URL + db 0, got nil")
	}

	// 3. URL + username -> error
	r3 := &RedisStorage{
		URL:      "redis://127.0.0.1:6379/0",
		Username: "user",
	}
	if err := r3.Provision(ctx); err == nil {
		t.Error("expected error for URL + username, got nil")
	}

	// 4. URL + password -> error
	r4 := &RedisStorage{
		URL:      "redis://127.0.0.1:6379/0",
		Password: "pass",
	}
	if err := r4.Provision(ctx); err == nil {
		t.Error("expected error for URL + password, got nil")
	}
}

func TestRedisStorage_Provision_URL_Replacer(t *testing.T) {
	addr := getTestRedisAddr()
	t.Setenv("TEST_REDIS_URL", "redis://"+addr+"/0?client_name=test-worker&dial_timeout=2s")

	r := &RedisStorage{
		URL:               "{env.TEST_REDIS_URL}",
		KeyPrefix:         "url_repl_test:",
		PipelineMultiplex: 2,
	}

	ctx, cancel := caddymain.NewContext(caddymain.Context{Context: t.Context()})
	defer cancel()

	if err := r.Provision(ctx); err != nil {
		t.Fatalf("provision with URL failed: %v", err)
	}
	defer func() { _ = r.Cleanup() }()

	if r.Storage() == nil {
		t.Fatalf("expected initialized storage")
	}
}

func TestRedisStorage_Provision_URL_Invalid(t *testing.T) {
	ctx, cancel := caddymain.NewContext(caddymain.Context{Context: t.Context()})
	defer cancel()

	// Invalid scheme
	r1 := &RedisStorage{
		URL: "http://127.0.0.1:6379",
	}
	if err := r1.Provision(ctx); err == nil {
		t.Error("expected error for invalid scheme http, got nil")
	}

	// Empty resolved URL
	r2 := &RedisStorage{
		URL: "{env.NON_EXISTENT_REDIS_URL_VAR}",
	}
	if err := r2.Provision(ctx); err == nil {
		t.Error("expected error for empty resolved url, got nil")
	}
}

func TestRedisStorage_JSON_URL(t *testing.T) {
	data := `{"url":"redis://127.0.0.1:6379/1","key_prefix":"json_test:","pipeline_multiplex":3}`
	var r RedisStorage
	if err := json.Unmarshal([]byte(data), &r); err != nil {
		t.Fatalf("json unmarshal error: %v", err)
	}
	if r.URL != "redis://127.0.0.1:6379/1" {
		t.Errorf("expected url redis://127.0.0.1:6379/1, got %s", r.URL)
	}
	if r.KeyPrefix != "json_test:" {
		t.Errorf("expected key_prefix json_test:, got %s", r.KeyPrefix)
	}
	if r.PipelineMultiplex != 3 {
		t.Errorf("expected pipeline_multiplex 3, got %d", r.PipelineMultiplex)
	}
}

func TestRedisStorage_UnmarshalCaddyfile_DuplicateURL(t *testing.T) {
	config := `redis {
		url redis://127.0.0.1:6379/1
		url redis://127.0.0.1:6379/2
	}`
	d := caddyfile.NewTestDispenser(config)
	var r RedisStorage
	if err := r.UnmarshalCaddyfile(d); err == nil {
		t.Error("expected error for duplicate url, got nil")
	}
}

func TestRedisStorage_ProvisionAndCleanup(t *testing.T) {
	addr := getTestRedisAddr()
	r := &RedisStorage{
		Address:   []string{addr},
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

func TestRedisStorage_Provision_MissingConnection(t *testing.T) {
	ctx, cancel := caddymain.NewContext(caddymain.Context{Context: t.Context()})
	defer cancel()

	// Missing both url and address
	r1 := &RedisStorage{}
	if err := r1.Provision(ctx); err == nil || !strings.Contains(err.Error(), "connection configuration required") {
		t.Errorf("expected connection configuration required error, got %v", err)
	}

	// Address resolves to empty string
	r2 := &RedisStorage{Address: []string{"{env.UNSET_ADDR_VAR}"}}
	if err := r2.Provision(ctx); err == nil || !strings.Contains(err.Error(), "connection configuration required") {
		t.Errorf("expected connection configuration required error, got %v", err)
	}
}

func TestRedisStorage_Provision_InvalidPipelineMultiplex(t *testing.T) {
	ctx, cancel := caddymain.NewContext(caddymain.Context{Context: t.Context()})
	defer cancel()

	r := &RedisStorage{
		Address:           []string{"127.0.0.1:6379"},
		PipelineMultiplex: -1,
	}
	if err := r.Provision(ctx); err == nil || !strings.Contains(err.Error(), "pipeline_multiplex must be >= 0") {
		t.Errorf("expected pipeline_multiplex must be >= 0 error, got %v", err)
	}
}

func TestRedisStorage_UnmarshalCaddyfile_ValidationErrors(t *testing.T) {
	tests := []struct {
		name   string
		config string
	}{
		{"missing url arg", "redis {\n url\n}"},
		{"missing address arg", "redis {\n address\n}"},
		{"missing key_prefix arg", "redis {\n key_prefix\n}"},
		{"missing username arg", "redis {\n username\n}"},
		{"missing password arg", "redis {\n password\n}"},
		{"missing db arg", "redis {\n db\n}"},
		{"invalid db", "redis {\n db notanumber\n}"},
		{"negative db", "redis {\n db -1\n}"},
		{"missing pipeline_multiplex arg", "redis {\n pipeline_multiplex\n}"},
		{"invalid pipeline_multiplex", "redis {\n pipeline_multiplex notanumber\n}"},
		{"negative pipeline_multiplex", "redis {\n pipeline_multiplex -1\n}"},
		{"unknown subdirective", "redis {\n unknown_foo bar\n}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := caddyfile.NewTestDispenser(tt.config)
			var r RedisStorage
			if err := r.UnmarshalCaddyfile(d); err == nil {
				t.Errorf("expected unmarshal error for %s, got nil", tt.name)
			}
		})
	}
}
