package redis

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/rueidis"

	pb "github.com/indragunawan/titip/proto"
)

func setupTestRedis(t *testing.T) (*miniredis.Miniredis, *RedisStorage) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{mr.Addr()},
		DisableCache: true,
	})
	if err != nil {
		mr.Close()
		t.Fatalf("failed to create rueidis client: %v", err)
	}

	store, err := New(client, WithKeyPrefix("titip_test:"))
	if err != nil {
		client.Close()
		mr.Close()
		t.Fatalf("failed to create RedisStorage: %v", err)
	}

	t.Cleanup(func() {
		_ = store.Close()
		mr.Close()
	})

	return mr, store
}

func TestSetAndGetVariant_MultipleVariants(t *testing.T) {
	ctx := context.Background()
	_, store := setupTestRedis(t)

	primaryKey := "https://example.com/api/v1/products"
	meta := &pb.CacheMetadata{
		PrimaryKey:        primaryKey,
		VaryHeaderNames:   []string{"Accept-Encoding"},
		CreatedAtUnixNano: time.Now().UnixNano(),
		ExpiresAtUnixNano: time.Now().Add(60 * time.Second).UnixNano(),
		Tags:              []string{"products", "api"},
	}

	// 1. Store gzip variant
	vGzip := &pb.VariantInfo{
		VariantKey:         "gzip",
		StatusCode:         200,
		Etag:               `"etag-gzip"`,
		RawBodySize:        1024,
		CompressedBodySize: 200,
	}
	bodyGzip := []byte("compressed_gzip_body_bytes")
	if err := store.SetVariant(ctx, primaryKey, meta, vGzip, bodyGzip, 60*time.Second); err != nil {
		t.Fatalf("failed to set gzip variant: %v", err)
	}

	// 2. Store br variant
	vBr := &pb.VariantInfo{
		VariantKey:         "br",
		StatusCode:         200,
		Etag:               `"etag-br"`,
		RawBodySize:        1024,
		CompressedBodySize: 180,
	}
	bodyBr := []byte("compressed_br_body_bytes")
	if err := store.SetVariant(ctx, primaryKey, meta, vBr, bodyBr, 60*time.Second); err != nil {
		t.Fatalf("failed to set br variant: %v", err)
	}

	// 3. GetMeta verifies both variants exist
	metaRetrieved, err := store.GetMeta(ctx, primaryKey)
	if err != nil {
		t.Fatalf("failed to get meta: %v", err)
	}
	if metaRetrieved == nil {
		t.Fatal("expected non-nil metadata")
	}
	if len(metaRetrieved.Variants) != 2 {
		t.Fatalf("expected 2 variants, got %d", len(metaRetrieved.Variants))
	}
	if metaRetrieved.Variants["gzip"].Etag != `"etag-gzip"` || metaRetrieved.Variants["br"].Etag != `"etag-br"` {
		t.Fatalf("variant etags mismatch: %+v", metaRetrieved.Variants)
	}

	// 4. GetVariant for gzip
	vInfoGzip, bGzip, err := store.GetVariant(ctx, primaryKey, "gzip")
	if err != nil {
		t.Fatalf("failed to get gzip variant: %v", err)
	}
	if vInfoGzip == nil || string(bGzip) != string(bodyGzip) {
		t.Fatalf("gzip variant mismatch: %v, %s", vInfoGzip, string(bGzip))
	}

	// 5. GetVariant for br
	vInfoBr, bBr, err := store.GetVariant(ctx, primaryKey, "br")
	if err != nil {
		t.Fatalf("failed to get br variant: %v", err)
	}
	if vInfoBr == nil || string(bBr) != string(bodyBr) {
		t.Fatalf("br variant mismatch: %v, %s", vInfoBr, string(bBr))
	}

	// 6. Get non-existent variant returns nil
	vMissing, bMissing, err := store.GetVariant(ctx, primaryKey, "zstd")
	if err != nil {
		t.Fatalf("unexpected error for missing variant: %v", err)
	}
	if vMissing != nil || bMissing != nil {
		t.Fatalf("expected nil for missing variant, got %v, %v", vMissing, bMissing)
	}
}

func TestDelete_CompletePurge_ZeroOrphanedKeys(t *testing.T) {
	ctx := context.Background()
	mr, store := setupTestRedis(t)

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

	// Verify keys exist in miniredis
	if !mr.Exists("titip_test:meta:" + primaryKey) {
		t.Fatal("expected meta key to exist")
	}
	for _, varKey := range []string{"gzip", "br", "identity"} {
		if !mr.Exists("titip_test:body:" + primaryKey + ":" + varKey) {
			t.Fatalf("expected body key for %s to exist", varKey)
		}
	}

	// Delete
	if err := store.Delete(ctx, primaryKey); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	// Verify zero orphaned keys
	if mr.Exists("titip_test:meta:" + primaryKey) {
		t.Fatal("expected meta key to be deleted")
	}
	for _, varKey := range []string{"gzip", "br", "identity"} {
		if mr.Exists("titip_test:body:" + primaryKey + ":" + varKey) {
			t.Fatalf("orphaned body key detected for %s!", varKey)
		}
	}
}

func TestSoftPurge(t *testing.T) {
	ctx := context.Background()
	_, store := setupTestRedis(t)

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

	// Soft purge
	if err := store.SoftPurge(ctx, primaryKey); err != nil {
		t.Fatalf("soft purge failed: %v", err)
	}

	// Metadata is marked is_soft_purged = true
	m, err := store.GetMeta(ctx, primaryKey)
	if err != nil || m == nil {
		t.Fatalf("failed to get meta: %v", err)
	}
	if !m.IsSoftPurged {
		t.Fatal("expected is_soft_purged to be true")
	}

	// Body is still intact
	_, b, err := store.GetVariant(ctx, primaryKey, "default")
	if err != nil || string(b) != "profile_body" {
		t.Fatalf("body should remain intact after soft purge, got %s", string(b))
	}
}

func TestPurgeByTag_HardAndSoft(t *testing.T) {
	ctx := context.Background()
	mr, store := setupTestRedis(t)

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
		if mr.Exists("titip_test:meta:" + pk) {
			t.Fatalf("expected meta key %s to be deleted", pk)
		}
		if mr.Exists("titip_test:body:" + pk + ":gzip") {
			t.Fatalf("expected body key %s:gzip to be deleted", pk)
		}
	}
}

func TestDynamicTTLExtension(t *testing.T) {
	ctx := context.Background()
	mr, store := setupTestRedis(t)

	primaryKey := "https://example.com/dynamic-ttl"
	meta := &pb.CacheMetadata{PrimaryKey: primaryKey}

	// 1. Variant 1 with TTL = 60s
	v1 := &pb.VariantInfo{VariantKey: "v1", StatusCode: 200}
	if err := store.SetVariant(ctx, primaryKey, meta, v1, []byte("body1"), 60*time.Second); err != nil {
		t.Fatalf("failed to set v1: %v", err)
	}

	ttl1 := mr.TTL("titip_test:meta:" + primaryKey)
	if ttl1 < 50*time.Second || ttl1 > 60*time.Second {
		t.Fatalf("unexpected meta TTL: %v", ttl1)
	}

	// 2. Variant 2 with TTL = 300s -> extends meta TTL
	v2 := &pb.VariantInfo{VariantKey: "v2", StatusCode: 200}
	if err := store.SetVariant(ctx, primaryKey, meta, v2, []byte("body2"), 300*time.Second); err != nil {
		t.Fatalf("failed to set v2: %v", err)
	}

	ttl2 := mr.TTL("titip_test:meta:" + primaryKey)
	if ttl2 < 290*time.Second || ttl2 > 300*time.Second {
		t.Fatalf("expected meta TTL to expand to ~300s, got %v", ttl2)
	}
}

// TestDynamicTTLExtension_MultiVariantScenario replicates the real-world timeline:
// 1. At 00:00: Store "en" variant with 10h TTL (36000s). Meta TTL = 10h.
// 2. At 05:00 (5h later): Store "es" variant with 10h TTL (36000s). Meta TTL extended to 10h (expires at 15:00).
// 3. At 10:00 (10h after en stored): "en" body expires, but Meta key and "es" body are still alive for another 5h!
func TestDynamicTTLExtension_MultiVariantScenario(t *testing.T) {
	ctx := context.Background()
	mr, store := setupTestRedis(t)

	primaryKey := "https://example.com/page/1"
	meta := &pb.CacheMetadata{
		PrimaryKey:      primaryKey,
		VaryHeaderNames: []string{"Accept-Language"},
	}

	const tenHours = 36000 * time.Second

	// Step 1: At 00:00 -> store "en" with 10h TTL
	vEN := &pb.VariantInfo{VariantKey: "en", StatusCode: 200}
	if err := store.SetVariant(ctx, primaryKey, meta, vEN, []byte("english_content"), tenHours); err != nil {
		t.Fatalf("failed to set en variant: %v", err)
	}

	metaTTL1 := mr.TTL("titip_test:meta:" + primaryKey)
	bodyENTTL1 := mr.TTL("titip_test:body:" + primaryKey + ":en")
	if metaTTL1 != tenHours || bodyENTTL1 != tenHours {
		t.Fatalf("expected 10h TTL at 00:00, got meta=%v body=%v", metaTTL1, bodyENTTL1)
	}

	// Step 2: 5 hours elapse (05:00)
	mr.FastForward(5 * time.Hour)

	metaTTL2 := mr.TTL("titip_test:meta:" + primaryKey)
	bodyENTTL2 := mr.TTL("titip_test:body:" + primaryKey + ":en")
	if metaTTL2 != 5*time.Hour || bodyENTTL2 != 5*time.Hour {
		t.Fatalf("expected 5h remaining at 05:00, got meta=%v body=%v", metaTTL2, bodyENTTL2)
	}

	// Step 3: At 05:00 -> store "es" variant with 10h TTL
	vES := &pb.VariantInfo{VariantKey: "es", StatusCode: 200}
	if err := store.SetVariant(ctx, primaryKey, meta, vES, []byte("spanish_content"), tenHours); err != nil {
		t.Fatalf("failed to set es variant: %v", err)
	}

	// Meta Hash TTL must be dynamically extended back to 10h (expires at 15:00)
	metaTTL3 := mr.TTL("titip_test:meta:" + primaryKey)
	bodyESTTL := mr.TTL("titip_test:body:" + primaryKey + ":es")
	bodyENTTL3 := mr.TTL("titip_test:body:" + primaryKey + ":en")

	if metaTTL3 != tenHours {
		t.Fatalf("expected meta TTL to be extended to 10h (15:00 expiry), got %v", metaTTL3)
	}
	if bodyESTTL != tenHours {
		t.Fatalf("expected es body TTL to be 10h (15:00 expiry), got %v", bodyESTTL)
	}
	if bodyENTTL3 != 5*time.Hour {
		t.Fatalf("expected en body TTL to remain at 5h (10:00 expiry), got %v", bodyENTTL3)
	}

	// Step 4: Another 5 hours elapse (10:00) -> "en" variant expires, but "es" and Meta are still alive!
	mr.FastForward(5 * time.Hour)

	if mr.Exists("titip_test:body:" + primaryKey + ":en") {
		t.Fatalf("expected en body to have expired at 10:00")
	}
	if !mr.Exists("titip_test:body:" + primaryKey + ":es") {
		t.Fatalf("expected es body to still be alive at 10:00")
	}
	if !mr.Exists("titip_test:meta:" + primaryKey) {
		t.Fatalf("expected meta hash to still be alive at 10:00")
	}

	metaTTL4 := mr.TTL("titip_test:meta:" + primaryKey)
	bodyESTTL4 := mr.TTL("titip_test:body:" + primaryKey + ":es")
	if metaTTL4 != 5*time.Hour || bodyESTTL4 != 5*time.Hour {
		t.Fatalf("expected 5h remaining on meta and es body at 10:00, got meta=%v es=%v", metaTTL4, bodyESTTL4)
	}
}

func TestConcurrencyAndRaces(t *testing.T) {
	ctx := context.Background()
	_, store := setupTestRedis(t)

	const goroutines = 50
	const iterations = 20

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()

			pk := fmt.Sprintf("https://example.com/concurrent/%d", id%5)
			meta := &pb.CacheMetadata{
				PrimaryKey: pk,
				Tags:       []string{"concurrent"},
			}

			for j := 0; j < iterations; j++ {
				varKey := fmt.Sprintf("v_%d", j%3)
				v := &pb.VariantInfo{VariantKey: varKey, StatusCode: 200}
				body := []byte(fmt.Sprintf("body_%d_%d", id, j))

				_ = store.SetVariant(ctx, pk, meta, v, body, 30*time.Second)
				_, _, _ = store.GetVariant(ctx, pk, varKey)
				_, _ = store.GetMeta(ctx, pk)

				if j%10 == 0 {
					_ = store.SoftPurge(ctx, pk)
				}
			}
		}(i)
	}

	wg.Wait()
}

func BenchmarkRedisSetAndGetVariant(b *testing.B) {
	mr, err := miniredis.Run()
	if err != nil {
		b.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{mr.Addr()},
		DisableCache: true,
	})
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	store, err := New(client, WithKeyPrefix("bench:"))
	if err != nil {
		b.Fatalf("failed to create storage: %v", err)
	}

	ctx := context.Background()
	primaryKey := "https://example.com/bench"
	meta := &pb.CacheMetadata{PrimaryKey: primaryKey}
	v := &pb.VariantInfo{VariantKey: "gzip", StatusCode: 200}
	body := []byte("bench_body_payload")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = store.SetVariant(ctx, primaryKey, meta, v, body, 60*time.Second)
		_, _, _ = store.GetVariant(ctx, primaryKey, "gzip")
	}
}

