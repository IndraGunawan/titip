package caddy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	caddymain "github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/redis/rueidis"

	"github.com/indragunawan/titip"
	"github.com/indragunawan/titip/storage"
	storageRedis "github.com/indragunawan/titip/storage/redis"
	_ "github.com/indragunawan/titip/storage/redis/caddy"
)

func getTestRedisAddr() string {
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		return addr
	}
	return "127.0.0.1:6379"
}

func setupTestCaddyEngine(t testing.TB) (rueidis.Client, storage.Storage, *titip.Titip) {
	addr := getTestRedisAddr()
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{addr},
		DisableCache: true,
	})
	if err != nil {
		t.Fatalf("failed to connect to test Redis at %s: %v", addr, err)
	}

	prefix := fmt.Sprintf("test_caddy:%d:%d:", time.Now().UnixNano(), rand.Int63())
	store, err := storageRedis.New(client, storageRedis.WithKeyPrefix(prefix))
	if err != nil {
		client.Close()
		t.Fatalf("failed to create storage: %v", err)
	}

	engine, err := titip.New(
		titip.WithStorage(store),
		titip.WithOriginTimeout(5*time.Second),
	)
	if err != nil {
		client.Close()
		t.Fatalf("failed to create engine: %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp := client.Do(ctx, client.B().Keys().Pattern(prefix+"*").Build())
		if keys, err := resp.AsStrSlice(); err == nil && len(keys) > 0 {
			delCmds := make([]rueidis.Completed, len(keys))
			for i, k := range keys {
				delCmds[i] = client.B().Del().Key(k).Build()
			}
			client.DoMulti(ctx, delCmds...)
		}
		_ = engine.Close(context.Background())
		client.Close()
	})

	return client, store, engine
}

type mockStorageModule struct {
	store storage.Storage
}

func (m *mockStorageModule) Storage() storage.Storage {
	return m.store
}

func (m *mockStorageModule) Destruct() error {
	return nil
}

func TestCaddyHandler_UnmarshalCaddyfile(t *testing.T) {
	config := `titip {
		cache_status RFC9211
		origin_timeout 20s
		storage redis {
			address 127.0.0.1:6379
		}
	}`

	d := caddyfile.NewTestDispenser(config)
	var h Handler
	if err := h.UnmarshalCaddyfile(d); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if h.CacheStatus != "RFC9211" {
		t.Errorf("expected cache_status RFC9211, got %s", h.CacheStatus)
	}
	if h.OriginTimeout != "20s" {
		t.Errorf("expected origin_timeout 20s, got %s", h.OriginTimeout)
	}
}

func TestCaddyHandler_MiddlewareExecution(t *testing.T) {
	_, store, engine := setupTestCaddyEngine(t)

	h := &Handler{
		engine:     engine,
		storageMod: &mockStorageModule{store: store},
	}

	var upstreamCalls atomic.Int32
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		upstreamCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, `{"message":"caddy proxied content"}`)
		return nil
	})

	// 1. Initial request (miss)
	req1 := httptest.NewRequest(http.MethodGet, "http://example.com/caddy/test", nil)
	rec1 := httptest.NewRecorder()
	if err := h.ServeHTTP(rec1, req1, next); err != nil {
		t.Fatalf("serveHTTP error: %v", err)
	}

	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec1.Code)
	}
	if upstreamCalls.Load() != 1 {
		t.Fatalf("expected 1 upstream call, got %d", upstreamCalls.Load())
	}

	// 2. Second request (hit)
	rec2 := httptest.NewRecorder()
	if err := h.ServeHTTP(rec2, req1, next); err != nil {
		t.Fatalf("serveHTTP error: %v", err)
	}

	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec2.Code)
	}
	if upstreamCalls.Load() != 1 {
		t.Fatalf("cache hit should not call upstream: %d", upstreamCalls.Load())
	}
}

func TestCaddyHandler_ProvisionMissingStorage(t *testing.T) {
	h := &Handler{}
	ctx, cancel := caddymain.NewContext(caddymain.Context{Context: t.Context()})
	defer cancel()

	err := h.Provision(ctx)
	if err == nil {
		t.Fatalf("expected error when storage is missing")
	}
}

// AC-3: Admin Purge API Single-Target Validation & Execution
func TestAdminPurge_ValidationAndMutualExclusivity(t *testing.T) {
	// 1. Mutual exclusivity violation (both urls and tags)
	body1 := `{"urls": ["http://example.com/api/item"], "tags": ["tag1"], "soft": true}`
	req1 := httptest.NewRequest(http.MethodPost, "/titip/purge", bytes.NewBufferString(body1))
	req1.Header.Set("Content-Type", "application/json")
	rec1 := httptest.NewRecorder()

	_ = handleAdminPurge(rec1, req1)
	if rec1.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for multiple targets, got %d", rec1.Code)
	}

	// 2. Missing target
	body2 := `{"soft": true}`
	req2 := httptest.NewRequest(http.MethodPost, "/titip/purge", bytes.NewBufferString(body2))
	req2.Header.Set("Content-Type", "application/json")
	rec2 := httptest.NewRecorder()

	_ = handleAdminPurge(rec2, req2)
	if rec2.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for missing target, got %d", rec2.Code)
	}

	// 3. Valid single-target URL purge
	body3 := `{"urls": ["http://example.com/api/item"], "soft": true}`
	req3 := httptest.NewRequest(http.MethodPost, "/titip/purge", bytes.NewBufferString(body3))
	req3.Header.Set("Content-Type", "application/json")
	rec3 := httptest.NewRecorder()

	_ = handleAdminPurge(rec3, req3)
	if rec3.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rec3.Code, rec3.Body.String())
	}

	var resp purgeAdminResponse
	if err := json.NewDecoder(rec3.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if !resp.Success || resp.Purged.Type != "urls" || resp.Purged.Count != 1 || !resp.Purged.Soft {
		t.Errorf("unexpected purge response: %+v", resp)
	}

	// 4. Valid single-target Tag purge
	body4 := `{"tags": ["users", "products"], "soft": false}`
	req4 := httptest.NewRequest(http.MethodPost, "/titip/purge", bytes.NewBufferString(body4))
	req4.Header.Set("Content-Type", "application/json")
	rec4 := httptest.NewRecorder()

	_ = handleAdminPurge(rec4, req4)
	if rec4.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for tags, got %d", rec4.Code)
	}

	// 5. Valid purge_everything
	body5 := `{"purge_everything": true, "soft": true}`
	req5 := httptest.NewRequest(http.MethodPost, "/titip/purge", bytes.NewBufferString(body5))
	req5.Header.Set("Content-Type", "application/json")
	rec5 := httptest.NewRecorder()

	_ = handleAdminPurge(rec5, req5)
	if rec5.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for purge_everything, got %d", rec5.Code)
	}
}

// AC-1 Edge Case: Undefined Storage in Caddyfile
func TestCaddyHandler_UndefinedStorage_FailsProvisioning(t *testing.T) {
	config := `titip {
		cache_status rfc9211
		origin_timeout 10s
	}`

	d := caddyfile.NewTestDispenser(config)
	var h Handler
	if err := h.UnmarshalCaddyfile(d); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	ctx, cancel := caddymain.NewContext(caddymain.Context{Context: t.Context()})
	defer cancel()

	err := h.Provision(ctx)
	if err == nil {
		t.Fatalf("expected error when storage directive is omitted")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("storage configuration is required")) {
		t.Errorf("expected 'storage configuration is required' error, got: %v", err)
	}
}

// AC-1 Edge Case: Unknown Storage Module in Caddyfile
func TestCaddyHandler_UnknownStorageModule_Fails(t *testing.T) {
	config := `titip {
		storage memcached {
			servers 127.0.0.1:11211
		}
	}`

	d := caddyfile.NewTestDispenser(config)
	var h Handler
	err := h.UnmarshalCaddyfile(d)
	if err == nil {
		// If unmarshal succeeded (raw json created), provisioning must fail to load unknown module
		ctx, cancel := caddymain.NewContext(caddymain.Context{Context: t.Context()})
		defer cancel()
		err = h.Provision(ctx)
	}

	if err == nil {
		t.Fatalf("expected failure when unknown storage module 'memcached' is configured")
	}
}

// AC-3 / AC-4: End-to-End Live Admin Purge Invalidation
func TestAdminPurge_EndToEndLiveInvalidation(t *testing.T) {
	_, store, engine := setupTestCaddyEngine(t)

	h := &Handler{
		engine:     engine,
		storageMod: &mockStorageModule{store: store},
		id:         "test-admin-e2e",
	}
	registerEngine(h.id, engine)
	defer unregisterEngine(h.id)

	var originCalls atomic.Int32
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		callNum := originCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("Cache-Tag", "catalog,items")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `{"call":%d,"data":"item-123"}`, callNum)
		return nil
	})

	testURL := "http://example.com/api/item-123"
	req := httptest.NewRequest(http.MethodGet, testURL, nil)

	// 1. Prime cache (Miss -> call #1)
	rec1 := httptest.NewRecorder()
	_ = h.ServeHTTP(rec1, req, next)
	if rec1.Body.String() != `{"call":1,"data":"item-123"}` {
		t.Fatalf("expected call 1, got %s", rec1.Body.String())
	}
	if originCalls.Load() != 1 {
		t.Fatalf("expected 1 origin call, got %d", originCalls.Load())
	}

	// 2. Cache Hit (Hit -> 0 origin calls)
	rec2 := httptest.NewRecorder()
	_ = h.ServeHTTP(rec2, req, next)
	if rec2.Body.String() != `{"call":1,"data":"item-123"}` {
		t.Fatalf("expected cached call 1, got %s", rec2.Body.String())
	}
	if originCalls.Load() != 1 {
		t.Fatalf("cache hit should not increment origin calls: %d", originCalls.Load())
	}

	// 3. Trigger Soft Purge via Admin API
	purgeBody := fmt.Sprintf(`{"urls": [%q], "soft": true}`, testURL)
	purgeReq := httptest.NewRequest(http.MethodPost, "/titip/purge", bytes.NewBufferString(purgeBody))
	purgeReq.Header.Set("Content-Type", "application/json")
	purgeRec := httptest.NewRecorder()

	_ = handleAdminPurge(purgeRec, purgeReq)
	if purgeRec.Code != http.StatusOK {
		t.Fatalf("admin purge failed with status %d: %s", purgeRec.Code, purgeRec.Body.String())
	}

	// 4. Subsequent request must synchronously fetch fresh data (call #2)
	rec3 := httptest.NewRecorder()
	_ = h.ServeHTTP(rec3, req, next)
	if rec3.Body.String() != `{"call":2,"data":"item-123"}` {
		t.Fatalf("expected fresh call 2 after purge, got %s", rec3.Body.String())
	}
	if originCalls.Load() != 2 {
		t.Fatalf("expected 2 origin calls after purge, got %d", originCalls.Load())
	}

	// 5. Test Tag Purge
	tagPurgeBody := `{"tags": ["catalog"], "soft": true}`
	tagPurgeReq := httptest.NewRequest(http.MethodPost, "/titip/purge", bytes.NewBufferString(tagPurgeBody))
	tagPurgeReq.Header.Set("Content-Type", "application/json")
	tagPurgeRec := httptest.NewRecorder()

	_ = handleAdminPurge(tagPurgeRec, tagPurgeReq)
	if tagPurgeRec.Code != http.StatusOK {
		t.Fatalf("tag purge failed with status %d", tagPurgeRec.Code)
	}

	// 6. Request after tag purge fetches fresh data (call #3)
	rec4 := httptest.NewRecorder()
	_ = h.ServeHTTP(rec4, req, next)
	if rec4.Body.String() != `{"call":3,"data":"item-123"}` {
		t.Fatalf("expected fresh call 3 after tag purge, got %s", rec4.Body.String())
	}
	if originCalls.Load() != 3 {
		t.Fatalf("expected 3 origin calls after tag purge, got %d", originCalls.Load())
	}
}

// TestCaddy_StandaloneStorageDirective_Fails verifies that storage modules cannot be configured standalone
func TestCaddy_StandaloneStorageDirective_Fails(t *testing.T) {
	// Attempting to configure "storage redis" directly in Caddyfile without titip block
	config := `:8080 {
		storage redis {
			address localhost:6379
		}
	}`

	adapter := caddyconfig.GetAdapter("caddyfile")
	if adapter != nil {
		_, _, err := adapter.Adapt([]byte(config), nil)
		if err == nil {
			t.Fatalf("expected failure when configuring standalone storage directive without titip block")
		}
	}
}


