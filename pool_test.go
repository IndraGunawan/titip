package titip

import (
	"bytes"
	"crypto/rand"
	"net/http"
	"strings"
	"sync"
	"testing"

	googleproto "google.golang.org/protobuf/proto"

	pb "github.com/indragunawan/titip/proto"
)

func TestBufferPool(t *testing.T) {
	buf := GetBuffer()
	if buf == nil {
		t.Fatal("expected non-nil buffer")
	}
	buf.WriteString("hello world")
	if buf.String() != "hello world" {
		t.Fatalf("unexpected buffer content: %s", buf.String())
	}
	PutBuffer(buf)

	// Get buffer again, should be reset (empty)
	buf2 := GetBuffer()
	if buf2.Len() != 0 {
		t.Fatalf("expected reset buffer with len 0, got %d", buf2.Len())
	}
	PutBuffer(buf2)
}

func TestBufferPoolGrowthProtection(t *testing.T) {
	buf := GetBuffer()
	// Grow buffer beyond 2MB
	largeData := make([]byte, 3*1024*1024)
	buf.Write(largeData)
	if buf.Cap() <= maxBufferSize {
		t.Fatalf("expected buffer cap > %d, got %d", maxBufferSize, buf.Cap())
	}

	// Putting large buffer should discard it without panic
	PutBuffer(buf)

	// Getting a buffer should work cleanly
	buf2 := GetBuffer()
	if buf2.Cap() > maxBufferSize {
		t.Fatalf("expected fresh pooled buffer, got cap %d", buf2.Cap())
	}
	PutBuffer(buf2)
}

func TestResponseRecorder(t *testing.T) {
	rec := GetResponseRecorder()
	defer PutResponseRecorder(rec)

	rec.Header().Set("Content-Type", "application/json")
	rec.Header().Add("X-Custom", "value1")
	rec.Header().Add("X-Custom", "value2")
	rec.WriteHeader(http.StatusCreated)

	payload := []byte(`{"status":"ok"}`)
	n, err := rec.Write(payload)
	if err != nil {
		t.Fatalf("unexpected write error: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("expected %d bytes written, got %d", len(payload), n)
	}

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d", rec.Code)
	}
	if rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("expected application/json header, got %s", rec.Header().Get("Content-Type"))
	}
	if rec.Body.String() != string(payload) {
		t.Fatalf("expected body %s, got %s", payload, rec.Body.String())
	}
	if !rec.WroteHeader() {
		t.Fatal("expected WroteHeader() to be true")
	}

	rec.Flush()
	if !rec.flushed {
		t.Fatal("expected flushed to be true")
	}
}

func TestResponseRecorderImplicitStatus200(t *testing.T) {
	rec := GetResponseRecorder()
	defer PutResponseRecorder(rec)

	_, err := rec.Write([]byte("default 200 ok"))
	if err != nil {
		t.Fatalf("write error: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected implicit status 200, got %d", rec.Code)
	}
	if !rec.WroteHeader() {
		t.Fatal("expected wroteHeader true")
	}
}

func TestResponseRecorderResetAndReuse(t *testing.T) {
	rec := GetResponseRecorder()
	rec.Header().Set("X-Test", "123")
	rec.WriteHeader(http.StatusNotFound)
	rec.Write([]byte("not found"))
	PutResponseRecorder(rec)

	rec2 := GetResponseRecorder()
	defer PutResponseRecorder(rec2)

	if len(rec2.Header()) != 0 {
		t.Fatalf("expected empty headers, got %v", rec2.Header())
	}
	if rec2.Body.Len() != 0 {
		t.Fatalf("expected empty body, got len %d", rec2.Body.Len())
	}
	if rec2.Code != http.StatusOK {
		t.Fatalf("expected reset code 200, got %d", rec2.Code)
	}
	if rec2.WroteHeader() {
		t.Fatal("expected wroteHeader false")
	}
}

func TestProtobufPools(t *testing.T) {
	meta := GetCacheMetadata()
	meta.PrimaryKey = "https://example.com/api/users"
	meta.VaryHeaderNames = []string{"Accept-Encoding", "Accept-Language"}
	meta.CreatedAtUnixNano = 1700000000000
	meta.ExpiresAtUnixNano = 1700000060000
	meta.Tags = []string{"users", "api"}
	meta.Variants = make(map[string]*pb.VariantInfo)

	v := GetVariantInfo()
	v.VariantKey = "gzip_en"
	v.StatusCode = 200
	v.Etag = `"etag-123"`
	v.RawBodySize = 1024
	v.CompressedBodySize = 256
	meta.Variants["gzip_en"] = v

	// Marshal and test roundtrip
	data, err := googleproto.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	decodedMeta := GetCacheMetadata()
	defer PutCacheMetadata(decodedMeta)
	if err := googleproto.Unmarshal(data, decodedMeta); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if decodedMeta.PrimaryKey != meta.PrimaryKey {
		t.Fatalf("expected primary key %s, got %s", meta.PrimaryKey, decodedMeta.PrimaryKey)
	}
	if len(decodedMeta.Variants) != 1 || decodedMeta.Variants["gzip_en"].Etag != `"etag-123"` {
		t.Fatalf("variants mismatch: %v", decodedMeta.Variants)
	}

	// Return to pool and verify reset
	PutVariantInfo(v)
	PutCacheMetadata(meta)

	metaFresh := GetCacheMetadata()
	defer PutCacheMetadata(metaFresh)
	if metaFresh.PrimaryKey != "" || len(metaFresh.VaryHeaderNames) != 0 || len(metaFresh.Tags) != 0 {
		t.Fatalf("expected reset metadata, got %+v", metaFresh)
	}
}

func TestLZ4CompressionRoundtrip(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte("")},
		{"small", []byte("Hello, World!")},
		{"json", []byte(`{"id": 1, "name": "John Doe", "email": "john@example.com", "roles": ["admin", "user"]}`)},
		{"repeated_html", []byte(strings.Repeat("<div><span>Content payload block</span></div>\n", 200))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compBuf := GetBuffer()
			defer PutBuffer(compBuf)

			if err := CompressLZ4(tt.data, compBuf); err != nil {
				t.Fatalf("compression failed: %v", err)
			}

			decompBuf := GetBuffer()
			defer PutBuffer(decompBuf)

			if err := DecompressLZ4(compBuf.Bytes(), decompBuf); err != nil {
				t.Fatalf("decompression failed: %v", err)
			}

			if !bytes.Equal(decompBuf.Bytes(), tt.data) {
				t.Fatalf("data mismatch: expected %d bytes, got %d bytes", len(tt.data), decompBuf.Len())
			}
		})
	}
}

func TestLZ4CompressionRatio(t *testing.T) {
	// Sample JSON dataset
	htmlDoc := strings.Repeat(`
<!DOCTYPE html>
<html>
<head><title>Test Page</title></head>
<body>
  <h1>Welcome to Titip High Performance Caching Middleware</h1>
  <p>Titip provides zero-allocation RFC-7234 compliant caching with Redis storage backend.</p>
</body>
</html>
`, 50)

	rawBytes := []byte(htmlDoc)
	compBuf := GetBuffer()
	defer PutBuffer(compBuf)

	if err := CompressLZ4(rawBytes, compBuf); err != nil {
		t.Fatalf("compression failed: %v", err)
	}

	rawSize := len(rawBytes)
	compSize := compBuf.Len()
	ratio := float64(rawSize-compSize) / float64(rawSize)

	t.Logf("Raw size: %d, Compressed size: %d, Compression ratio: %.2f%%", rawSize, compSize, ratio*100)

	if ratio < 0.60 {
		t.Fatalf("expected >= 60%% compression ratio, got %.2f%%", ratio*100)
	}
}

func TestLZ4CorruptData(t *testing.T) {
	corruptBytes := []byte("this is not valid lz4 compressed data frame")
	dst := GetBuffer()
	defer PutBuffer(dst)

	err := DecompressLZ4(corruptBytes, dst)
	if err == nil {
		t.Fatal("expected error decompressing corrupt data, got nil")
	}
}

func TestPoolConcurrencyAndRaces(t *testing.T) {
	const goroutines = 100
	const iterations = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()

			for j := 0; j < iterations; j++ {
				// Buffer pool test
				buf := GetBuffer()
				buf.WriteString("concurrency test string")
				PutBuffer(buf)

				// ResponseRecorder pool test
				rec := GetResponseRecorder()
				rec.Header().Set("X-Goroutine", "test")
				rec.WriteHeader(http.StatusOK)
				rec.Write([]byte("response body test"))
				PutResponseRecorder(rec)

				// Protobuf pool test
				meta := GetCacheMetadata()
				meta.PrimaryKey = "https://test.local"
				PutCacheMetadata(meta)

				v := GetVariantInfo()
				v.VariantKey = "v1"
				PutVariantInfo(v)

				// LZ4 compress/decompress test
				payload := []byte(strings.Repeat("concurrent lz4 test payload ", 10))
				cBuf := GetBuffer()
				if err := CompressLZ4(payload, cBuf); err != nil {
					t.Errorf("concurrent compression failed: %v", err)
				}
				dBuf := GetBuffer()
				if err := DecompressLZ4(cBuf.Bytes(), dBuf); err != nil {
					t.Errorf("concurrent decompression failed: %v", err)
				}
				if !bytes.Equal(dBuf.Bytes(), payload) {
					t.Errorf("concurrent payload mismatch")
				}
				PutBuffer(cBuf)
				PutBuffer(dBuf)
			}
		}(i)
	}

	wg.Wait()
}

// --- Benchmarks ---

func BenchmarkBufferPool(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf := GetBuffer()
		buf.WriteString("benchmark buffer data payload")
		PutBuffer(buf)
	}
}

func BenchmarkResponseRecorderPool(b *testing.B) {
	payload := []byte(`{"status":"cached"}`)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rec := GetResponseRecorder()
		rec.WriteHeader(http.StatusOK)
		rec.Write(payload)
		PutResponseRecorder(rec)
	}
}

func BenchmarkProtobufPool(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m := GetCacheMetadata()
		m.PrimaryKey = "https://example.com/api/v1"
		PutCacheMetadata(m)

		v := GetVariantInfo()
		v.VariantKey = "gzip"
		PutVariantInfo(v)
	}
}

func BenchmarkLZ4CompressDecompress(b *testing.B) {
	payload := make([]byte, 4096)
	_, _ = rand.Read(payload)

	compBuf := GetBuffer()
	_ = CompressLZ4(payload, compBuf)
	compBytes := bytes.Clone(compBuf.Bytes())
	PutBuffer(compBuf)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		dst := GetBuffer()
		_ = DecompressLZ4(compBytes, dst)
		PutBuffer(dst)
	}
}

