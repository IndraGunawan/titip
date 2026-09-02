package teststore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	pb "github.com/indragunawan/titip/proto"
)

func TestStore_BasicGetSet(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	primaryKey := "http://example.com/api/test"
	variantKey := "text/html"

	meta := &pb.CacheMetadata{
		VaryHeaderNames: []string{"Accept"},
		Tags:            []string{"tag-a", "tag-b"},
	}
	variant := &pb.VariantInfo{
		VariantKey: variantKey,
		StatusCode: 200,
	}
	body := []byte("hello world payload")

	// Initial Get should be empty
	m, soft, err := s.GetMeta(ctx, primaryKey)
	if err != nil || m != nil || soft {
		t.Fatalf("expected nil meta on empty store, got m=%v, soft=%v, err=%v", m, soft, err)
	}

	v, b, err := s.GetVariant(ctx, primaryKey, variantKey)
	if err != nil || v != nil || b != nil {
		t.Fatalf("expected nil variant on empty store, got v=%v, b=%v, err=%v", v, b, err)
	}

	// Set variant
	err = s.SetVariant(ctx, primaryKey, meta, variant, body, time.Minute)
	if err != nil {
		t.Fatalf("failed to set variant: %v", err)
	}

	// GetMeta
	m, soft, err = s.GetMeta(ctx, primaryKey)
	if err != nil || m == nil || soft {
		t.Fatalf("expected valid meta, got m=%v, soft=%v, err=%v", m, soft, err)
	}
	if len(m.VaryHeaderNames) != 1 || m.VaryHeaderNames[0] != "Accept" {
		t.Fatalf("unexpected VaryHeaderNames: %v", m.VaryHeaderNames)
	}

	// GetVariant
	v, b, err = s.GetVariant(ctx, primaryKey, variantKey)
	if err != nil || v == nil || !bytes.Equal(b, body) {
		t.Fatalf("expected valid variant and matching body, got v=%v, b=%q, err=%v", v, string(b), err)
	}
}

func TestStore_TTLExpiration(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	primaryKey := "http://example.com/expired"
	variantKey := "v1"

	meta := &pb.CacheMetadata{}
	variant := &pb.VariantInfo{VariantKey: variantKey}
	body := []byte("short lived")

	err := s.SetVariant(ctx, primaryKey, meta, variant, body, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to set variant: %v", err)
	}

	time.Sleep(70 * time.Millisecond)

	m, _, err := s.GetMeta(ctx, primaryKey)
	if err != nil || m != nil {
		t.Fatalf("expected expired meta to return nil, got %v, err=%v", m, err)
	}

	v, b, err := s.GetVariant(ctx, primaryKey, variantKey)
	if err != nil || v != nil || b != nil {
		t.Fatalf("expected expired variant to return nil, got %v, %v, err=%v", v, b, err)
	}
}

func TestStore_Purge(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	primaryKey := "http://example.com/purge-test"
	variantKey := "v1"

	meta := &pb.CacheMetadata{Tags: []string{"tag1"}}
	variant := &pb.VariantInfo{VariantKey: variantKey}
	body := []byte("body")

	_ = s.SetVariant(ctx, primaryKey, meta, variant, body, time.Minute)

	// Soft Purge
	count, err := s.Purge(ctx, primaryKey, true)
	if err != nil || count != 1 {
		t.Fatalf("expected soft purge count=1, got %d, err=%v", count, err)
	}

	m, soft, err := s.GetMeta(ctx, primaryKey)
	if err != nil || m == nil || !soft {
		t.Fatalf("expected softPurged=true, got m=%v, soft=%v, err=%v", m, soft, err)
	}

	// Variant body should still be retained for stale fallback
	_, b, err := s.GetVariant(ctx, primaryKey, variantKey)
	if err != nil || !bytes.Equal(b, body) {
		t.Fatalf("expected body retained after soft purge, got %q, err=%v", string(b), err)
	}

	// Hard Purge
	count, err = s.Purge(ctx, primaryKey, false)
	if err != nil || count != 1 {
		t.Fatalf("expected hard purge count=1, got %d, err=%v", count, err)
	}

	m, _, err = s.GetMeta(ctx, primaryKey)
	if err != nil || m != nil {
		t.Fatalf("expected deleted meta after hard purge, got %v, err=%v", m, err)
	}
	_, b, err = s.GetVariant(ctx, primaryKey, variantKey)
	if err != nil || b != nil {
		t.Fatalf("expected deleted body after hard purge, got %v, err=%v", b, err)
	}
}

func TestStore_PurgeByTag(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		pk := fmt.Sprintf("http://example.com/tag-test/%d", i)
		meta := &pb.CacheMetadata{Tags: []string{"user:123"}}
		variant := &pb.VariantInfo{VariantKey: "default"}
		_ = s.SetVariant(ctx, pk, meta, variant, []byte("data"), time.Minute)
	}

	// Other untagged key
	_ = s.SetVariant(ctx, "http://example.com/other", &pb.CacheMetadata{}, &pb.VariantInfo{VariantKey: "default"}, []byte("data"), time.Minute)

	// Purge by tag
	count, err := s.PurgeByTag(ctx, "user:123", false)
	if err != nil || count != 3 {
		t.Fatalf("expected 3 purged items, got %d, err=%v", count, err)
	}

	m, _, _ := s.GetMeta(ctx, "http://example.com/tag-test/1")
	if m != nil {
		t.Fatalf("expected tag-test/1 to be deleted")
	}

	mOther, _, _ := s.GetMeta(ctx, "http://example.com/other")
	if mOther == nil {
		t.Fatalf("expected other key to remain intact")
	}
}

func TestStore_PurgeByPattern_And_PurgeAll(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	_ = s.SetVariant(ctx, "test:key:1", &pb.CacheMetadata{}, &pb.VariantInfo{}, []byte("1"), time.Minute)
	_ = s.SetVariant(ctx, "test:key:2", &pb.CacheMetadata{}, &pb.VariantInfo{}, []byte("2"), time.Minute)
	_ = s.SetVariant(ctx, "other:key:1", &pb.CacheMetadata{}, &pb.VariantInfo{}, []byte("3"), time.Minute)

	count, err := s.PurgeByPattern(ctx, "test:key:*", false)
	if err != nil || count != 2 {
		t.Fatalf("expected 2 purged by pattern, got %d, err=%v", count, err)
	}

	m, _, _ := s.GetMeta(ctx, "other:key:1")
	if m == nil {
		t.Fatalf("expected other key to remain")
	}

	countAll, err := s.PurgeAll(ctx)
	if err != nil || countAll != 1 {
		t.Fatalf("expected 1 purged by PurgeAll, got %d, err=%v", countAll, err)
	}
}

func TestStore_FaultInjection_And_Closed(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	s.SetClosed(true)
	_, _, err := s.GetMeta(ctx, "k")
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed on GetMeta when closed, got %v", err)
	}

	s.SetClosed(false)
	customErr := errors.New("custom injected storage error")
	s.SetGetMetaHook(func(ctx context.Context, primaryKey string) (*pb.CacheMetadata, bool, error) {
		return nil, false, customErr
	})

	_, _, err = s.GetMeta(ctx, "k")
	if !errors.Is(err, customErr) {
		t.Fatalf("expected injected custom error, got %v", err)
	}
}

func TestStore_ConcurrentAccess_Race(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	const workers = 20
	const iterations = 100
	var wg sync.WaitGroup

	for i := range workers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := range iterations {
				pk := fmt.Sprintf("http://example.com/resource/%d", (workerID+j)%10)
				vk := fmt.Sprintf("var-%d", j%3)
				meta := &pb.CacheMetadata{
					VaryHeaderNames: []string{"Accept-Encoding"},
					Tags:            []string{fmt.Sprintf("tag-%d", workerID%5)},
				}
				variant := &pb.VariantInfo{VariantKey: vk, StatusCode: 200}
				body := fmt.Appendf(nil, "worker-%d-iter-%d", workerID, j)

				_ = s.SetVariant(ctx, pk, meta, variant, body, 5*time.Second)
				_, _, _ = s.GetMeta(ctx, pk)
				_, _, _ = s.GetVariant(ctx, pk, vk)

				if j%20 == 0 {
					_, _ = s.Purge(ctx, pk, j%40 == 0)
				}
				if j%30 == 0 {
					_, _ = s.PurgeByTag(ctx, fmt.Sprintf("tag-%d", workerID%5), false)
				}
			}
		}(i)
	}

	wg.Wait()
}

func BenchmarkStore_GetMeta(b *testing.B) {
	s := New()
	ctx := context.Background()
	pk := "http://example.com/api/bench"
	meta := &pb.CacheMetadata{
		PrimaryKey:      pk,
		VaryHeaderNames: []string{"Accept-Encoding"},
		Tags:            []string{"api", "v1"},
	}
	_ = s.SetVariant(ctx, pk, meta, &pb.VariantInfo{VariantKey: "default"}, []byte("bench body"), time.Hour)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		m, soft, err := s.GetMeta(ctx, pk)
		if err != nil || m == nil || soft {
			b.Fatalf("GetMeta failed: m=%v, soft=%v, err=%v", m, soft, err)
		}
	}
}

func BenchmarkStore_GetVariant(b *testing.B) {
	s := New()
	ctx := context.Background()
	pk := "http://example.com/api/bench"
	meta := &pb.CacheMetadata{PrimaryKey: pk}
	body := []byte("bench payload data")
	_ = s.SetVariant(ctx, pk, meta, &pb.VariantInfo{VariantKey: "default"}, body, time.Hour)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		v, payload, err := s.GetVariant(ctx, pk, "default")
		if err != nil || v == nil || len(payload) == 0 {
			b.Fatalf("GetVariant failed: v=%v, err=%v", v, err)
		}
	}
}

func BenchmarkStore_SetVariant(b *testing.B) {
	s := New()
	ctx := context.Background()
	pk := "http://example.com/api/bench"
	meta := &pb.CacheMetadata{
		PrimaryKey:      pk,
		VaryHeaderNames: []string{"Accept-Encoding"},
		Tags:            []string{"api"},
	}
	variant := &pb.VariantInfo{VariantKey: "default", StatusCode: 200}
	body := []byte("bench payload data")

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		if err := s.SetVariant(ctx, pk, meta, variant, body, time.Hour); err != nil {
			b.Fatalf("SetVariant failed: %v", err)
		}
	}
}

func BenchmarkStore_ParallelReads(b *testing.B) {
	s := New()
	ctx := context.Background()
	pk := "http://example.com/api/bench"
	meta := &pb.CacheMetadata{
		PrimaryKey:      pk,
		VaryHeaderNames: []string{"Accept-Encoding"},
	}
	_ = s.SetVariant(ctx, pk, meta, &pb.VariantInfo{VariantKey: "default"}, []byte("bench payload data"), time.Hour)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, _ = s.GetMeta(ctx, pk)
			_, _, _ = s.GetVariant(ctx, pk, "default")
		}
	})
}
