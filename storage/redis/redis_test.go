package redis

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/rueidis"

	pb "github.com/indragunawan/titip/proto"
)

func getTestRedisAddr() string {
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		return addr
	}
	return "127.0.0.1:6379"
}

func setupTestRedis(t testing.TB) (rueidis.Client, *RedisStorage, string) {
	addr := getTestRedisAddr()
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{addr},
		DisableCache: true,
	})
	if err != nil {
		t.Fatalf("failed to connect to test Redis at %s: %v. Make sure Redis 7+ is running (e.g. docker compose up -d)", addr, err)
	}

	prefix := fmt.Sprintf("test:%d:%d:", time.Now().UnixNano(), rand.Int63())
	store, err := New(client, WithKeyPrefix(prefix))
	if err != nil {
		client.Close()
		t.Fatalf("failed to create RedisStorage: %v", err)
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
		_ = store.Close()
		client.Close()
	})

	return client, store, prefix
}

func keyExists(ctx context.Context, client rueidis.Client, key string) bool {
	res, err := client.Do(ctx, client.B().Exists().Key(key).Build()).AsInt64()
	return err == nil && res > 0
}

func getKeyTTL(ctx context.Context, client rueidis.Client, key string) int64 {
	res, _ := client.Do(ctx, client.B().Ttl().Key(key).Build()).AsInt64()
	return res
}

func TestSetAndGetVariant_MultipleVariants(t *testing.T) {
	ctx := context.Background()
	_, store, _ := setupTestRedis(t)

	primaryKey := "https://example.com/api/v1/products"
	meta := &pb.CacheMetadata{
		PrimaryKey:        primaryKey,
		VaryHeaderNames:   []string{"Accept-Encoding", "User-Agent"},
		CreatedAtUnixNano: time.Now().UnixNano(),
		ExpiresAtUnixNano: time.Now().Add(10 * time.Minute).UnixNano(),
		Tags:              []string{"products", "api"},
		IsSoftPurged:      false,
	}

	// 1. Variant 1: gzip
	vGzip := &pb.VariantInfo{
		VariantKey:      "gzip",
		StatusCode:      200,
		ResponseHeaders: map[string]*pb.HeaderValues{"Content-Encoding": {Values: []string{"gzip"}}},
		Etag:            `"etag-gzip"`,
	}
	bodyGzip := []byte("gzip_compressed_body")
	if err := store.SetVariant(ctx, primaryKey, meta, vGzip, bodyGzip, 60*time.Second); err != nil {
		t.Fatalf("failed to set gzip variant: %v", err)
	}

	// 2. Variant 2: brotli
	vBr := &pb.VariantInfo{
		VariantKey:      "br",
		StatusCode:      200,
		ResponseHeaders: map[string]*pb.HeaderValues{"Content-Encoding": {Values: []string{"br"}}},
		Etag:            `"etag-br"`,
	}
	bodyBr := []byte("br_compressed_body")
	if err := store.SetVariant(ctx, primaryKey, meta, vBr, bodyBr, 60*time.Second); err != nil {
		t.Fatalf("failed to set br variant: %v", err)
	}

	// 3. Stage 1: GetMeta
	m, err := store.GetMeta(ctx, primaryKey)
	if err != nil {
		t.Fatalf("GetMeta failed: %v", err)
	}
	if m == nil {
		t.Fatal("expected metadata, got nil")
	}
	if m.PrimaryKey != primaryKey {
		t.Errorf("expected primary key %s, got %s", primaryKey, m.PrimaryKey)
	}

	// 4. Stage 2: GetVariant
	varInfo, body, err := store.GetVariant(ctx, primaryKey, "gzip")
	if err != nil {
		t.Fatalf("GetVariant gzip failed: %v", err)
	}
	if varInfo == nil || string(body) != "gzip_compressed_body" {
		t.Fatalf("unexpected gzip payload: %v, %s", varInfo, string(body))
	}

	varInfoBr, bodyBrotli, err := store.GetVariant(ctx, primaryKey, "br")
	if err != nil {
		t.Fatalf("GetVariant br failed: %v", err)
	}
	if varInfoBr == nil || string(bodyBrotli) != "br_compressed_body" {
		t.Fatalf("unexpected br payload: %v, %s", varInfoBr, string(bodyBrotli))
	}

	// 5. Non-existent variant
	vMissing, bMissing, err := store.GetVariant(ctx, primaryKey, "non-existent")
	if err != nil {
		t.Fatalf("unexpected error on missing variant: %v", err)
	}
	if vMissing != nil || bMissing != nil {
		t.Fatal("expected nil for missing variant")
	}
}

func TestDelete_CompletePurge_ZeroOrphanedKeys(t *testing.T) {
	ctx := context.Background()
	client, store, prefix := setupTestRedis(t)

	primaryKey := "https://example.com/item/42"
	meta := &pb.CacheMetadata{
		PrimaryKey: primaryKey,
		Tags:       []string{"items"},
	}

	for _, varKey := range []string{"gzip", "br", "identity"} {
		v := &pb.VariantInfo{VariantKey: varKey, StatusCode: 200}
		body := []byte("body_" + varKey)
		if err := store.SetVariant(ctx, primaryKey, meta, v, body, 60*time.Second); err != nil {
			t.Fatalf("set variant %s failed: %v", varKey, err)
		}
	}

	// Verify keys exist
	if !keyExists(ctx, client, prefix+"meta:"+primaryKey) {
		t.Fatal("expected meta key to exist")
	}
	for _, varKey := range []string{"gzip", "br", "identity"} {
		if !keyExists(ctx, client, prefix+"body:"+primaryKey+":"+varKey) {
			t.Fatalf("expected body key for %s to exist", varKey)
		}
	}

	// Delete
	if err := store.Delete(ctx, primaryKey); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	// Verify zero orphaned keys
	if keyExists(ctx, client, prefix+"meta:"+primaryKey) {
		t.Fatal("expected meta key to be deleted")
	}
	for _, varKey := range []string{"gzip", "br", "identity"} {
		if keyExists(ctx, client, prefix+"body:"+primaryKey+":"+varKey) {
			t.Fatalf("orphaned body key detected for %s!", varKey)
		}
	}
}

func TestSoftPurge(t *testing.T) {
	ctx := context.Background()
	_, store, _ := setupTestRedis(t)

	primaryKey := "https://example.com/profile"
	meta := &pb.CacheMetadata{
		PrimaryKey:   primaryKey,
		IsSoftPurged: false,
	}
	v := &pb.VariantInfo{VariantKey: "default", StatusCode: 200}
	body := []byte("profile_body")

	if err := store.SetVariant(ctx, primaryKey, meta, v, body, 60*time.Second); err != nil {
		t.Fatalf("set variant failed: %v", err)
	}

	// Verify initial state
	m, err := store.GetMeta(ctx, primaryKey)
	if err != nil || m.IsSoftPurged {
		t.Fatalf("expected IsSoftPurged=false, got %v", m.IsSoftPurged)
	}

	// Perform Soft Purge
	if err := store.SoftPurge(ctx, primaryKey); err != nil {
		t.Fatalf("soft purge failed: %v", err)
	}

	// Verify IsSoftPurged is now true
	m2, err := store.GetMeta(ctx, primaryKey)
	if err != nil {
		t.Fatalf("get meta after soft purge failed: %v", err)
	}
	if !m2.IsSoftPurged {
		t.Fatal("expected IsSoftPurged=true after soft purge")
	}

	// Body is still intact
	_, b, err := store.GetVariant(ctx, primaryKey, "default")
	if err != nil || string(b) != "profile_body" {
		t.Fatalf("body should remain intact after soft purge, got %s", string(b))
	}
}

func TestPurgeByTag_HardAndSoft(t *testing.T) {
	ctx := context.Background()
	client, store, prefix := setupTestRedis(t)

	// Setup 3 URLs under tag "category:tech"
	for i := 1; i <= 3; i++ {
		pk := fmt.Sprintf("https://example.com/tech/%d", i)
		meta := &pb.CacheMetadata{
			PrimaryKey: pk,
			Tags:       []string{"category:tech"},
		}
		v := &pb.VariantInfo{VariantKey: "gzip", StatusCode: 200}
		body := []byte(fmt.Sprintf("tech_body_%d", i))
		if err := store.SetVariant(ctx, pk, meta, v, body, 60*time.Second); err != nil {
			t.Fatalf("set variant failed: %v", err)
		}
	}

	// Hard Purge
	if err := store.PurgeByTag(ctx, "category:tech", false); err != nil {
		t.Fatalf("purge tag failed: %v", err)
	}

	// Verify all deleted
	for i := 1; i <= 3; i++ {
		pk := fmt.Sprintf("https://example.com/tech/%d", i)
		if keyExists(ctx, client, prefix+"meta:"+pk) {
			t.Fatalf("expected meta key %s to be deleted", pk)
		}
		if keyExists(ctx, client, prefix+"body:"+pk+":gzip") {
			t.Fatalf("expected body key %s:gzip to be deleted", pk)
		}
	}
}

func TestDynamicTTLExtension(t *testing.T) {
	ctx := context.Background()
	client, store, prefix := setupTestRedis(t)

	primaryKey := "https://example.com/dynamic-ttl"
	meta := &pb.CacheMetadata{PrimaryKey: primaryKey}

	// 1. Variant 1 with TTL = 60s
	v1 := &pb.VariantInfo{VariantKey: "v1", StatusCode: 200}
	if err := store.SetVariant(ctx, primaryKey, meta, v1, []byte("body1"), 60*time.Second); err != nil {
		t.Fatalf("failed to set v1: %v", err)
	}

	ttl1 := getKeyTTL(ctx, client, prefix+"meta:"+primaryKey)
	if ttl1 < 50 || ttl1 > 60 {
		t.Fatalf("unexpected meta TTL: %v", ttl1)
	}

	// 2. Variant 2 with TTL = 300s -> extends meta TTL
	v2 := &pb.VariantInfo{VariantKey: "v2", StatusCode: 200}
	if err := store.SetVariant(ctx, primaryKey, meta, v2, []byte("body2"), 300*time.Second); err != nil {
		t.Fatalf("failed to set v2: %v", err)
	}

	ttl2 := getKeyTTL(ctx, client, prefix+"meta:"+primaryKey)
	if ttl2 < 290 || ttl2 > 300 {
		t.Fatalf("expected meta TTL to expand to ~300s, got %v", ttl2)
	}
}

// TestDynamicTTLExtension_MultiVariantScenario replicates the real-world timeline on real Redis:
// 1. Store "en" variant with 2s TTL. Meta TTL = 2s.
// 2. After 1s: Store "es" variant with 4s TTL. Meta TTL extended to 4s.
// 3. After 1.5s more (total 2.5s): "en" body has expired, but Meta key and "es" body are still alive!
func TestDynamicTTLExtension_MultiVariantScenario(t *testing.T) {
	ctx := context.Background()
	client, store, prefix := setupTestRedis(t)

	primaryKey := "https://example.com/page/1"
	meta := &pb.CacheMetadata{
		PrimaryKey:      primaryKey,
		VaryHeaderNames: []string{"Accept-Language"},
	}

	// Step 1: Store "en" with 2s TTL
	vEN := &pb.VariantInfo{VariantKey: "en", StatusCode: 200}
	if err := store.SetVariant(ctx, primaryKey, meta, vEN, []byte("english_content"), 2*time.Second); err != nil {
		t.Fatalf("failed to set en variant: %v", err)
	}

	metaTTL1 := getKeyTTL(ctx, client, prefix+"meta:"+primaryKey)
	bodyENTTL1 := getKeyTTL(ctx, client, prefix+"body:"+primaryKey+":en")
	if metaTTL1 < 1 || metaTTL1 > 2 || bodyENTTL1 < 1 || bodyENTTL1 > 2 {
		t.Fatalf("expected ~2s TTL, got meta=%v body=%v", metaTTL1, bodyENTTL1)
	}

	// Step 2: Sleep 1s, then store "es" with 4s TTL
	time.Sleep(1050 * time.Millisecond)

	vES := &pb.VariantInfo{VariantKey: "es", StatusCode: 200}
	if err := store.SetVariant(ctx, primaryKey, meta, vES, []byte("spanish_content"), 4*time.Second); err != nil {
		t.Fatalf("failed to set es variant: %v", err)
	}

	// Meta Hash TTL must be dynamically extended to ~4s
	metaTTL3 := getKeyTTL(ctx, client, prefix+"meta:"+primaryKey)
	bodyESTTL := getKeyTTL(ctx, client, prefix+"body:"+primaryKey+":es")
	if metaTTL3 < 3 || metaTTL3 > 4 {
		t.Fatalf("expected meta TTL extended to ~4s, got %v", metaTTL3)
	}
	if bodyESTTL < 3 || bodyESTTL > 4 {
		t.Fatalf("expected es body TTL to be ~4s, got %v", bodyESTTL)
	}

	// Step 3: Sleep 1.2s more (total 2.25s) -> "en" variant (2s TTL) expires, but "es" and Meta are still alive!
	time.Sleep(1200 * time.Millisecond)

	if keyExists(ctx, client, prefix+"body:"+primaryKey+":en") {
		t.Fatalf("expected en body to have expired")
	}
	if !keyExists(ctx, client, prefix+"body:"+primaryKey+":es") {
		t.Fatalf("expected es body to still be alive")
	}
	if !keyExists(ctx, client, prefix+"meta:"+primaryKey) {
		t.Fatalf("expected meta hash to still be alive")
	}
}

func TestConcurrencyAndRaces(t *testing.T) {
	ctx := context.Background()
	_, store, _ := setupTestRedis(t)

	const goroutines = 20
	const iterations = 10

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := range goroutines {
		go func(id int) {
			defer wg.Done()

			pk := fmt.Sprintf("https://example.com/concurrent/%d", id%5)
			meta := &pb.CacheMetadata{
				PrimaryKey: pk,
				Tags:       []string{"concurrent"},
			}

			for j := range iterations {
				varKey := fmt.Sprintf("v_%d", j%3)
				v := &pb.VariantInfo{VariantKey: varKey, StatusCode: 200}
				body := []byte(fmt.Sprintf("body_%d_%d", id, j))

				_ = store.SetVariant(ctx, pk, meta, v, body, 30*time.Second)
				_, _, _ = store.GetVariant(ctx, pk, varKey)
				_, _ = store.GetMeta(ctx, pk)
			}
		}(i)
	}

	wg.Wait()
}

func TestTagSetDynamicTTLExtension(t *testing.T) {
	ctx := context.Background()
	client, store, prefix := setupTestRedis(t)

	tag := "electronics"
	pk1 := "https://example.com/item/1"
	meta1 := &pb.CacheMetadata{
		PrimaryKey: pk1,
		Tags:       []string{tag},
	}
	v1 := &pb.VariantInfo{VariantKey: "default", StatusCode: 200}

	// 1. Store item 1 with 30s TTL
	if err := store.SetVariant(ctx, pk1, meta1, v1, []byte("item1"), 30*time.Second); err != nil {
		t.Fatalf("failed to set item1: %v", err)
	}

	tagKey := prefix + "tag:" + tag
	ttl1 := getKeyTTL(ctx, client, tagKey)
	if ttl1 < 25 || ttl1 > 30 {
		t.Fatalf("expected tag TTL ~30s, got %v", ttl1)
	}

	// 2. Store item 2 under same tag with 120s TTL -> extends tag set TTL
	pk2 := "https://example.com/item/2"
	meta2 := &pb.CacheMetadata{
		PrimaryKey: pk2,
		Tags:       []string{tag},
	}
	v2 := &pb.VariantInfo{VariantKey: "default", StatusCode: 200}
	if err := store.SetVariant(ctx, pk2, meta2, v2, []byte("item2"), 120*time.Second); err != nil {
		t.Fatalf("failed to set item2: %v", err)
	}

	ttl2 := getKeyTTL(ctx, client, tagKey)
	if ttl2 < 115 || ttl2 > 120 {
		t.Fatalf("expected tag TTL to extend to ~120s, got %v", ttl2)
	}
}

func BenchmarkStorage_SetAndGetVariant(b *testing.B) {
	_, store, _ := setupTestRedis(b)

	ctx := context.Background()
	primaryKey := "https://example.com/bench"
	meta := &pb.CacheMetadata{PrimaryKey: primaryKey}
	v := &pb.VariantInfo{VariantKey: "gzip", StatusCode: 200}
	body := []byte("bench_body_payload")

	for b.Loop() {
		_ = store.SetVariant(ctx, primaryKey, meta, v, body, 60*time.Second)
		_, _, _ = store.GetVariant(ctx, primaryKey, "gzip")
	}
}
