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

func getFieldTTL(ctx context.Context, client rueidis.Client, key, field string) int64 {
	resp := client.Do(ctx, client.B().Httl().Key(key).Fields().Numfields(1).Field(field).Build())
	slice, err := resp.AsIntSlice()
	if err == nil && len(slice) > 0 {
		return slice[0]
	}
	return -2
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
	m, isSoftPurged, err := store.GetMeta(ctx, primaryKey)
	if err != nil {
		t.Fatalf("GetMeta failed: %v", err)
	}
	if m == nil {
		t.Fatal("expected metadata, got nil")
	}
	if isSoftPurged {
		t.Fatal("expected isSoftPurged=false")
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

	// Hard Purge
	n, err := store.Purge(ctx, primaryKey, false)
	if err != nil {
		t.Fatalf("failed to delete: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 entry deleted, got %d", n)
	}

	// Hard Purge non-existent key returns 0
	n2, err := store.Purge(ctx, primaryKey, false)
	if err != nil {
		t.Fatalf("failed to delete non-existent key: %v", err)
	}
	if n2 != 0 {
		t.Fatalf("expected 0 entries deleted for non-existent key, got %d", n2)
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
	client, store, prefix := setupTestRedis(t)

	primaryKey := "https://example.com/profile"
	metaKey := prefix + "meta:" + primaryKey
	meta := &pb.CacheMetadata{
		PrimaryKey: primaryKey,
	}
	v := &pb.VariantInfo{VariantKey: "default", StatusCode: 200}
	body := []byte("profile_body")

	// 1. Initial SetVariant: verify _soft_purged does NOT exist in Redis hash
	if err := store.SetVariant(ctx, primaryKey, meta, v, body, 60*time.Second); err != nil {
		t.Fatalf("set variant failed: %v", err)
	}

	existsResp := client.Do(ctx, client.B().Hexists().Key(metaKey).Field("_soft_purged").Build())
	exists, err := existsResp.AsBool()
	if err != nil || exists {
		t.Fatalf("expected _soft_purged to NOT exist initially in Redis hash, got exists=%v (err=%v)", exists, err)
	}

	m, isSoftPurged, err := store.GetMeta(ctx, primaryKey)
	if err != nil || isSoftPurged {
		t.Fatalf("expected isSoftPurged=false initially from GetMeta, got %v", isSoftPurged)
	}
	if m == nil {
		t.Fatal("expected meta, got nil")
	}

	// 2. Perform Soft Purge: verify _soft_purged is set to "1" in Redis hash
	n, err := store.Purge(ctx, primaryKey, true)
	if err != nil {
		t.Fatalf("soft purge failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 entry soft-purged, got %d", n)
	}

	getResp := client.Do(ctx, client.B().Hget().Key(metaKey).Field("_soft_purged").Build())
	val, err := getResp.ToString()
	if err != nil || val != "1" {
		t.Fatalf("expected _soft_purged='1' in Redis hash after SoftPurge, got val=%q (err=%v)", val, err)
	}

	m2, isSoftPurged2, err := store.GetMeta(ctx, primaryKey)
	if err != nil {
		t.Fatalf("get meta after soft purge failed: %v", err)
	}
	if m2 == nil || !isSoftPurged2 {
		t.Fatal("expected isSoftPurged=true from GetMeta after soft purge")
	}

	// Body is still intact
	_, b, err := store.GetVariant(ctx, primaryKey, "default")
	if err != nil || string(b) != "profile_body" {
		t.Fatalf("body should remain intact after soft purge, got %s", string(b))
	}

	// 3. Re-save/Refresh via SetVariant: verify _soft_purged is removed/deleted from Redis hash
	vFresh := &pb.VariantInfo{VariantKey: "default", StatusCode: 200}
	bodyFresh := []byte("profile_body_fresh")
	if err := store.SetVariant(ctx, primaryKey, meta, vFresh, bodyFresh, 60*time.Second); err != nil {
		t.Fatalf("second set variant failed: %v", err)
	}

	existsResp2 := client.Do(ctx, client.B().Hexists().Key(metaKey).Field("_soft_purged").Build())
	exists2, err := existsResp2.AsBool()
	if err != nil || exists2 {
		t.Fatalf("expected _soft_purged to be cleared from Redis hash on SetVariant, got exists=%v (err=%v)", exists2, err)
	}

	m3, isSoftPurged3, err := store.GetMeta(ctx, primaryKey)
	if err != nil || isSoftPurged3 {
		t.Fatalf("expected isSoftPurged=false from GetMeta after re-saving variant, got %v", isSoftPurged3)
	}
	if m3 == nil {
		t.Fatal("expected meta after re-saving variant, got nil")
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

	// Setup 2 URLs under tag "category:news" for soft purge
	for i := 1; i <= 2; i++ {
		pk := fmt.Sprintf("https://example.com/news/%d", i)
		meta := &pb.CacheMetadata{
			PrimaryKey: pk,
			Tags:       []string{"category:news"},
		}
		v := &pb.VariantInfo{VariantKey: "gzip", StatusCode: 200}
		body := []byte(fmt.Sprintf("news_body_%d", i))
		if err := store.SetVariant(ctx, pk, meta, v, body, 60*time.Second); err != nil {
			t.Fatalf("set variant failed: %v", err)
		}
	}

	// 1. Soft Purge on "category:news"
	nSoft, err := store.PurgeByTag(ctx, "category:news", true)
	if err != nil {
		t.Fatalf("soft purge tag failed: %v", err)
	}
	if nSoft != 2 {
		t.Fatalf("expected 2 entries soft-purged, got %d", nSoft)
	}
	for i := 1; i <= 2; i++ {
		pk := fmt.Sprintf("https://example.com/news/%d", i)
		m, softPurged, err := store.GetMeta(ctx, pk)
		if err != nil || m == nil {
			t.Fatalf("expected meta to exist for %s, err: %v", pk, err)
		}
		if !softPurged {
			t.Fatalf("expected entry %s to be marked soft-purged", pk)
		}
	}

	// 2. Hard Purge on "category:tech"
	n, err := store.PurgeByTag(ctx, "category:tech", false)
	if err != nil {
		t.Fatalf("purge tag failed: %v", err)
	}
	if n != 3 {
		t.Fatalf("expected 3 entries purged, got %d", n)
	}

	// Verify all deleted for tech
	for i := 1; i <= 3; i++ {
		pk := fmt.Sprintf("https://example.com/tech/%d", i)
		if keyExists(ctx, client, prefix+"meta:"+pk) {
			t.Fatalf("expected meta key %s to be deleted", pk)
		}
		if keyExists(ctx, client, prefix+"body:"+pk+":gzip") {
			t.Fatalf("expected body key %s:gzip to be deleted", pk)
		}
	}
	if keyExists(ctx, client, prefix+"tag:category:tech") {
		t.Fatalf("expected tag hash key to be unlinked after hard purge")
	}
}

func TestPurgeByPattern_HardAndSoft(t *testing.T) {
	ctx := context.Background()
	client, store, prefix := setupTestRedis(t)

	// 1. Setup 3 entries under /blog/*
	for i := 1; i <= 3; i++ {
		pk := fmt.Sprintf("p=/blog/post-%d:h=example.com:m=GET", i)
		meta := &pb.CacheMetadata{PrimaryKey: pk}
		v := &pb.VariantInfo{VariantKey: "default", StatusCode: 200}
		if err := store.SetVariant(ctx, pk, meta, v, []byte("blog_content"), 60*time.Second); err != nil {
			t.Fatalf("set variant failed: %v", err)
		}
	}

	// 2. Soft Purge via pattern
	nSoft, err := store.PurgeByPattern(ctx, "p=/blog/*", true)
	if err != nil {
		t.Fatalf("soft purge pattern failed: %v", err)
	}
	if nSoft != 3 {
		t.Fatalf("expected 3 entries soft-purged, got %d", nSoft)
	}

	for i := 1; i <= 3; i++ {
		pk := fmt.Sprintf("p=/blog/post-%d:h=example.com:m=GET", i)
		m, isSoftPurged, err := store.GetMeta(ctx, pk)
		if err != nil || m == nil || !isSoftPurged {
			t.Fatalf("expected entry %s to be soft purged (isSoftPurged=true), got %v", pk, isSoftPurged)
		}
		_, b, err := store.GetVariant(ctx, pk, "default")
		if err != nil || len(b) == 0 {
			t.Fatalf("expected body for %s to remain intact after soft purge", pk)
		}
	}

	// 3. Hard Purge via pattern
	nHard, err := store.PurgeByPattern(ctx, "p=/blog/*", false)
	if err != nil {
		t.Fatalf("hard purge pattern failed: %v", err)
	}
	if nHard != 3 {
		t.Fatalf("expected 3 entries hard-purged, got %d", nHard)
	}

	for i := 1; i <= 3; i++ {
		pk := fmt.Sprintf("p=/blog/post-%d:h=example.com:m=GET", i)
		if keyExists(ctx, client, prefix+"meta:"+pk) {
			t.Fatalf("expected meta key %s to be hard deleted", pk)
		}
		if keyExists(ctx, client, prefix+"body:"+pk+":default") {
			t.Fatalf("expected body key %s to be hard deleted", pk)
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
				_, _, _ = store.GetMeta(ctx, pk)
			}
		}(i)
	}

	wg.Wait()
}

func TestTagHashDynamicTTLExtension(t *testing.T) {
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
		t.Fatalf("expected tag key TTL ~30s, got %v", ttl1)
	}
	fieldTTL1 := getFieldTTL(ctx, client, tagKey, pk1)
	if fieldTTL1 < 25 || fieldTTL1 > 30 {
		t.Fatalf("expected field pk1 TTL ~30s, got %v", fieldTTL1)
	}

	// 2. Store item 2 under same tag with 120s TTL -> extends tag hash key TTL to 120s
	pk2 := "https://example.com/item/2"
	meta2 := &pb.CacheMetadata{
		PrimaryKey: pk2,
		Tags:       []string{tag},
	}
	v2 := &pb.VariantInfo{VariantKey: "default", StatusCode: 200}
	if err := store.SetVariant(ctx, pk2, meta2, v2, []byte("item2"), 120*time.Second); err != nil {
		t.Fatalf("failed to set item2: %v", err)
	}

	// Top-level key TTL must extend to longest item TTL (~120s)
	ttl2 := getKeyTTL(ctx, client, tagKey)
	if ttl2 < 115 || ttl2 > 120 {
		t.Fatalf("expected tag key TTL to extend to ~120s, got %v", ttl2)
	}

	// pk1 field TTL should still be ~30s, while pk2 field TTL is ~120s
	f1 := getFieldTTL(ctx, client, tagKey, pk1)
	if f1 < 25 || f1 > 30 {
		t.Fatalf("expected pk1 field TTL ~30s, got %v", f1)
	}
	f2 := getFieldTTL(ctx, client, tagKey, pk2)
	if f2 < 115 || f2 > 120 {
		t.Fatalf("expected pk2 field TTL ~120s, got %v", f2)
	}
}

func TestTagHashFieldAutoEviction(t *testing.T) {
	ctx := context.Background()
	client, store, prefix := setupTestRedis(t)

	tag := "flash-sale"
	tagKey := prefix + "tag:" + tag

	pkShort := "https://example.com/short"
	metaShort := &pb.CacheMetadata{
		PrimaryKey: pkShort,
		Tags:       []string{tag},
	}
	vShort := &pb.VariantInfo{VariantKey: "default", StatusCode: 200}

	pkLong := "https://example.com/long"
	metaLong := &pb.CacheMetadata{
		PrimaryKey: pkLong,
		Tags:       []string{tag},
	}
	vLong := &pb.VariantInfo{VariantKey: "default", StatusCode: 200}

	// Store short item with 1s TTL and long item with 60s TTL
	if err := store.SetVariant(ctx, pkShort, metaShort, vShort, []byte("short"), 1*time.Second); err != nil {
		t.Fatalf("failed to set short item: %v", err)
	}
	if err := store.SetVariant(ctx, pkLong, metaLong, vLong, []byte("long"), 60*time.Second); err != nil {
		t.Fatalf("failed to set long item: %v", err)
	}

	// Verify both exist initially in the tag Hash
	shortVal, err := client.Do(ctx, client.B().Hget().Key(tagKey).Field(pkShort).Build()).ToString()
	if err != nil || shortVal != "1" {
		t.Fatalf("expected short item in tag hash, got err=%v, val=%s", err, shortVal)
	}
	longVal, err := client.Do(ctx, client.B().Hget().Key(tagKey).Field(pkLong).Build()).ToString()
	if err != nil || longVal != "1" {
		t.Fatalf("expected long item in tag hash, got err=%v, val=%s", err, longVal)
	}

	// Wait for short item field TTL to elapse
	time.Sleep(1500 * time.Millisecond)

	// Verify short item field was automatically evicted by Redis HEXPIRE
	shortResp := client.Do(ctx, client.B().Hget().Key(tagKey).Field(pkShort).Build())
	if !rueidis.IsRedisNil(shortResp.Error()) {
		t.Fatalf("expected short item field to be auto-evicted from tag hash, got %v", shortResp.Error())
	}

	// Long item must still be present
	longValAfter, err := client.Do(ctx, client.B().Hget().Key(tagKey).Field(pkLong).Build()).ToString()
	if err != nil || longValAfter != "1" {
		t.Fatalf("expected long item to still exist, got err=%v, val=%s", err, longValAfter)
	}

	// Tag key TTL should still be active (~58s)
	keyTTL := getKeyTTL(ctx, client, tagKey)
	if keyTTL < 50 || keyTTL > 60 {
		t.Fatalf("expected tag key TTL ~58s, got %v", keyTTL)
	}

	// Purge by tag should only process the 1 remaining active item
	n, err := store.PurgeByTag(ctx, tag, false)
	if err != nil {
		t.Fatalf("purge tag failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected exactly 1 entry purged, got %d", n)
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
