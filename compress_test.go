package titip

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/indragunawan/titip/internal/teststore"
	pb "github.com/indragunawan/titip/proto"
)

// mockCustomCompressor implements Compressor with a simple reversible ROT13-like byte transformation.
type mockCustomCompressor struct {
	name string
}

func (c *mockCustomCompressor) Name() string {
	return c.name
}

func (c *mockCustomCompressor) Compress(src []byte, dst *bytes.Buffer) error {
	if dst == nil {
		return errors.New("dst buffer cannot be nil")
	}
	// Prefix with a 4-byte magic header, then inverted bytes
	dst.WriteString("CUST")
	for _, b := range src {
		dst.WriteByte(b ^ 0x55)
	}
	return nil
}

func (c *mockCustomCompressor) Decompress(src []byte, dst *bytes.Buffer) error {
	if dst == nil {
		return errors.New("dst buffer cannot be nil")
	}
	if len(src) < 4 || string(src[:4]) != "CUST" {
		return errors.New("invalid custom magic header")
	}
	payload := src[4:]
	for _, b := range payload {
		dst.WriteByte(b ^ 0x55)
	}
	return nil
}

// failingCompressor simulates a compressor that fails on Compress.
type failingCompressor struct{}

func (f *failingCompressor) Name() string {
	return "failing"
}

func (f *failingCompressor) Compress(src []byte, dst *bytes.Buffer) error {
	return errors.New("simulated compression failure")
}

func (f *failingCompressor) Decompress(src []byte, dst *bytes.Buffer) error {
	return errors.New("simulated decompression failure")
}

func TestCompressionRoundtrip(t *testing.T) {
	t.Parallel()

	compressors := []struct {
		name string
		comp Compressor
	}{
		{"lz4", NewLZ4Compressor()},
		{"zstd", NewZstdCompressor()},
		{"none", NewNoneCompressor()},
		{"custom", &mockCustomCompressor{name: "my-rot"}},
	}

	payloads := []struct {
		name string
		data []byte
	}{
		{"empty", []byte("")},
		{"small", []byte("Hello, World!")},
		{"json_2kb", []byte(strings.Repeat(`{"id": 12345, "name": "Titip Cache Engine", "valid": true},`, 40))},
		{"html_100kb", []byte(strings.Repeat("<div><h1>Titip High Performance HTTP Caching</h1><p>Zero-allocation RFC-compliant.</p></div>\n", 1200))},
		{"binary_16kb", bytes.Repeat([]byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xAA, 0x55}, 2500)},
	}

	for _, c := range compressors {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			for _, p := range payloads {
				t.Run(p.name, func(t *testing.T) {
					compBuf := getBuffer()
					defer putBuffer(compBuf)

					if err := c.comp.Compress(p.data, compBuf); err != nil {
						t.Fatalf("compression failed: %v", err)
					}

					decompBuf := getBuffer()
					defer putBuffer(decompBuf)

					if err := c.comp.Decompress(compBuf.Bytes(), decompBuf); err != nil {
						t.Fatalf("decompression failed: %v", err)
					}

					if !bytes.Equal(decompBuf.Bytes(), p.data) {
						t.Fatalf("roundtrip mismatch: got %d bytes, want %d bytes", decompBuf.Len(), len(p.data))
					}
				})
			}
		})
	}
}

func TestCompressorNilDst(t *testing.T) {
	t.Parallel()

	compressors := []Compressor{
		NewLZ4Compressor(),
		NewZstdCompressor(),
		NewNoneCompressor(),
	}

	data := []byte("some payload")

	for _, comp := range compressors {
		t.Run(comp.Name(), func(t *testing.T) {
			if err := comp.Compress(data, nil); err == nil {
				t.Fatalf("%s: expected error compressing into nil dst, got nil", comp.Name())
			}
			if err := comp.Decompress(data, nil); err == nil {
				t.Fatalf("%s: expected error decompressing into nil dst, got nil", comp.Name())
			}
		})
	}
}

func TestCorruptedPayloadHandling(t *testing.T) {
	t.Parallel()

	compressors := []Compressor{
		NewLZ4Compressor(),
		NewZstdCompressor(),
		&mockCustomCompressor{name: "cust"},
	}

	corruptBytes := []byte("random non-compressed or damaged bytes string that should fail")

	for _, comp := range compressors {
		t.Run(comp.Name(), func(t *testing.T) {
			dst := getBuffer()
			defer putBuffer(dst)

			err := comp.Decompress(corruptBytes, dst)
			if err == nil {
				t.Fatalf("%s: expected decompression error on corrupt data, got nil", comp.Name())
			}
		})
	}
}

func TestStorageCompressionOptionsValidation(t *testing.T) {
	t.Parallel()

	store := teststore.New()

	t.Run("valid builtin codecs", func(t *testing.T) {
		for _, codec := range []string{"lz4", "zstd", "none", "LZ4", "Zstd", "NONE"} {
			mw, err := New(store, WithStorageCompression(codec))
			if err != nil {
				t.Fatalf("expected codec %q to be accepted, got: %v", codec, err)
			}
			_ = mw.Close(context.Background())
		}
	})

	t.Run("empty string rejected", func(t *testing.T) {
		_, err := New(store, WithStorageCompression(""))
		if !errors.Is(err, ErrInvalidOption) {
			t.Fatalf("expected ErrInvalidOption for empty storage compression, got: %v", err)
		}
	})

	t.Run("whitespace string rejected", func(t *testing.T) {
		_, err := New(store, WithStorageCompression("   "))
		if !errors.Is(err, ErrInvalidOption) {
			t.Fatalf("expected ErrInvalidOption for whitespace storage compression, got: %v", err)
		}
	})

	t.Run("unregistered codec rejected", func(t *testing.T) {
		_, err := New(store, WithStorageCompression("brotli"))
		if !errors.Is(err, ErrInvalidOption) {
			t.Fatalf("expected ErrInvalidOption for unregistered storage compression, got: %v", err)
		}
	})

	t.Run("nil custom compressor rejected", func(t *testing.T) {
		_, err := New(store, WithStorageCompressor(nil))
		if !errors.Is(err, ErrInvalidOption) {
			t.Fatalf("expected ErrInvalidOption for nil custom compressor, got: %v", err)
		}
	})

	t.Run("custom compressor with empty name rejected", func(t *testing.T) {
		_, err := New(store, WithStorageCompressor(&mockCustomCompressor{name: ""}))
		if !errors.Is(err, ErrInvalidOption) {
			t.Fatalf("expected ErrInvalidOption for empty custom compressor name, got: %v", err)
		}
	})

	t.Run("cannot overwrite protected built-in names", func(t *testing.T) {
		for _, name := range []string{"lz4", "zstd", "none", "LZ4", "ZSTD", "None"} {
			_, err := New(store, WithStorageCompressor(&mockCustomCompressor{name: name}))
			if !errors.Is(err, ErrInvalidOption) {
				t.Fatalf("expected ErrInvalidOption when registering protected name %q, got: %v", name, err)
			}
		}
	})

	t.Run("duplicate custom compressor name rejected", func(t *testing.T) {
		custom1 := &mockCustomCompressor{name: "my-codec"}
		custom2 := &mockCustomCompressor{name: "my-codec"}
		_, err := New(store, WithStorageCompressor(custom1), WithStorageCompressor(custom2))
		if !errors.Is(err, ErrInvalidOption) {
			t.Fatalf("expected ErrInvalidOption for duplicate compressor name, got: %v", err)
		}
	})

	t.Run("duplicate custom compressor case-insensitive rejected", func(t *testing.T) {
		custom1 := &mockCustomCompressor{name: "my-codec"}
		custom2 := &mockCustomCompressor{name: "MY-CODEC"}
		_, err := New(store, WithStorageCompressor(custom1), WithStorageCompressor(custom2))
		if !errors.Is(err, ErrInvalidOption) {
			t.Fatalf("expected ErrInvalidOption for case-insensitive duplicate compressor name, got: %v", err)
		}
	})

	t.Run("registering compressor does not change default storage compression lz4", func(t *testing.T) {
		custom := &mockCustomCompressor{name: "my-codec"}
		mw, err := New(store, WithStorageCompressor(custom))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer func() { _ = mw.Close(context.Background()) }()

		if mw.config.storageCompression != StorageCompressionLZ4 {
			t.Fatalf("expected storage compression %q, got %q", StorageCompressionLZ4, mw.config.storageCompression)
		}
	})

	t.Run("register and activate custom compressor", func(t *testing.T) {
		custom := &mockCustomCompressor{name: "my-codec"}
		mw, err := New(store, WithStorageCompressor(custom), WithStorageCompression("my-codec"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer func() { _ = mw.Close(context.Background()) }()

		if mw.config.storageCompression != "my-codec" {
			t.Fatalf("expected storage compression %q, got %q", "my-codec", mw.config.storageCompression)
		}
	})
}

func TestSmartAutoBypassSelection(t *testing.T) {
	t.Parallel()

	configured := NewZstdCompressor()

	tests := []struct {
		name       string
		bodyLen    int
		headers    map[string]string
		wantActive bool
	}{
		{
			name:       "under 256 bytes bypassed",
			bodyLen:    255,
			headers:    map[string]string{"Content-Type": "application/json"},
			wantActive: false,
		},
		{
			name:       "exact 256 bytes compressed",
			bodyLen:    256,
			headers:    map[string]string{"Content-Type": "application/json"},
			wantActive: true,
		},
		{
			name:       "large payload compressed",
			bodyLen:    1024,
			headers:    map[string]string{"Content-Type": "text/html; charset=utf-8"},
			wantActive: true,
		},
		{
			name:       "content-encoding gzip bypassed",
			bodyLen:    1024,
			headers:    map[string]string{"Content-Encoding": "gzip", "Content-Type": "application/json"},
			wantActive: false,
		},
		{
			name:       "content-encoding br bypassed",
			bodyLen:    1024,
			headers:    map[string]string{"Content-Encoding": "br", "Content-Type": "text/html"},
			wantActive: false,
		},
		{
			name:       "image/jpeg bypassed",
			bodyLen:    5000,
			headers:    map[string]string{"Content-Type": "image/jpeg"},
			wantActive: false,
		},
		{
			name:       "image/png bypassed",
			bodyLen:    5000,
			headers:    map[string]string{"Content-Type": "image/png"},
			wantActive: false,
		},
		{
			name:       "image/webp bypassed",
			bodyLen:    5000,
			headers:    map[string]string{"Content-Type": "image/webp"},
			wantActive: false,
		},
		{
			name:       "video/mp4 bypassed",
			bodyLen:    50000,
			headers:    map[string]string{"Content-Type": "video/mp4"},
			wantActive: false,
		},
		{
			name:       "application/zip bypassed",
			bodyLen:    10000,
			headers:    map[string]string{"Content-Type": "application/zip"},
			wantActive: false,
		},
		{
			name:       "application/pdf bypassed",
			bodyLen:    20000,
			headers:    map[string]string{"Content-Type": "application/pdf"},
			wantActive: false,
		},
		{
			name:       "image/svg+xml NOT bypassed (xml is compressible)",
			bodyLen:    1024,
			headers:    map[string]string{"Content-Type": "image/svg+xml"},
			wantActive: true,
		},
		{
			name:       "application/x-tar bypassed",
			bodyLen:    1024,
			headers:    map[string]string{"Content-Type": "application/x-tar"},
			wantActive: false,
		},
		{
			name:       "application/avatar+json NOT bypassed",
			bodyLen:    1024,
			headers:    map[string]string{"Content-Type": "application/avatar+json"},
			wantActive: true,
		},
		{
			name:       "application/vnd.target+json NOT bypassed",
			bodyLen:    1024,
			headers:    map[string]string{"Content-Type": "application/vnd.target+json"},
			wantActive: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := make(http.Header)
			for k, v := range tt.headers {
				h.Set(k, v)
			}
			chosen := resolveCompressor(configured, h, tt.bodyLen)
			if tt.wantActive && chosen.Name() != configured.Name() {
				t.Fatalf("expected active compressor %s, got %s", configured.Name(), chosen.Name())
			}
			if !tt.wantActive && chosen.Name() != StorageCompressionNone {
				t.Fatalf("expected noneCompressor, got %s", chosen.Name())
			}
		})
	}
}

func TestEndToEnd_StorageCompressionBuiltin(t *testing.T) {
	t.Parallel()

	for _, codec := range []string{StorageCompressionLZ4, StorageCompressionZstd, StorageCompressionNone} {
		t.Run(codec, func(t *testing.T) {
			store, _, mw := setupTestTitip(t, WithStorageCompression(codec))

			payload := strings.Repeat("Caching with Titip high performance engine! ", 30) // ~1300 bytes

			originCalls := 0
			origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				originCalls++
				w.Header().Set("Content-Type", "text/plain")
				w.Header().Set("Cache-Control", "public, max-age=60")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(payload))
			})

			handler := mw.testHandler(origin)

			req := httptest.NewRequest(http.MethodGet, "http://example.com/test-codec-"+codec, nil)

			// 1. Cold miss -> stores payload with specified codec
			res1 := doReq(t, handler, req)
			if res1.Code != http.StatusOK {
				t.Fatalf("expected status 200, got %d", res1.Code)
			}
			if res1.Body.String() != payload {
				t.Fatalf("body mismatch on cold miss")
			}
			if originCalls != 1 {
				t.Fatalf("expected 1 origin call, got %d", originCalls)
			}

			// Verify in storage
			pk := generatePrimaryKey(req, &CacheKey{})
			meta, _, err := store.GetMeta(context.Background(), pk)
			if err != nil || meta == nil {
				t.Fatalf("failed to retrieve stored metadata: %v", err)
			}
			varInfo, rawCompBody, err := store.GetVariant(context.Background(), pk, defaultVariantKey)
			if err != nil || varInfo == nil {
				t.Fatalf("failed to retrieve variant: %v", err)
			}

			if varInfo.StorageCompression != codec {
				t.Fatalf("expected variant StorageCompression %q, got %q", codec, varInfo.StorageCompression)
			}

			if codec == StorageCompressionNone {
				if !bytes.Equal(rawCompBody, []byte(payload)) {
					t.Fatalf("expected raw bytes in storage for none codec")
				}
			} else {
				if bytes.Equal(rawCompBody, []byte(payload)) {
					t.Fatalf("expected compressed bytes in storage, got raw uncompressed")
				}
			}

			// 2. Cache hit -> decompresses and returns identical payload
			res2 := doReq(t, handler, req)
			if res2.Code != http.StatusOK {
				t.Fatalf("expected status 200, got %d", res2.Code)
			}
			if res2.Body.String() != payload {
				t.Fatalf("body mismatch on cache hit: got %q", res2.Body.String())
			}
			if originCalls != 1 {
				t.Fatalf("expected cache hit with 0 additional origin calls, got %d", originCalls)
			}
			if !strings.Contains(res2.Header().Get(headerCacheStatus), "hit") {
				t.Fatalf("expected hit in Cache-Status header, got %s", res2.Header().Get(headerCacheStatus))
			}
		})
	}
}

func TestEndToEnd_CustomCompressor(t *testing.T) {
	t.Parallel()

	custom := &mockCustomCompressor{name: "super-rot"}
	store, _, mw := setupTestTitip(t,
		WithStorageCompressor(custom),
		WithStorageCompression("super-rot"),
	)

	payload := strings.Repeat("Custom compressor payload verification string! ", 20)

	originCalls := 0
	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls++
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(payload))
	})

	handler := mw.testHandler(origin)
	req := httptest.NewRequest(http.MethodGet, "http://example.com/test-custom-codec", nil)

	// Cold miss
	res1 := doReq(t, handler, req)
	if res1.Body.String() != payload {
		t.Fatalf("cold miss body mismatch")
	}

	pk := generatePrimaryKey(req, &CacheKey{})
	// Verify variant in store has custom codec and magic header
	varInfo, rawBody, err := store.GetVariant(context.Background(), pk, defaultVariantKey)
	if err != nil || varInfo == nil {
		t.Fatalf("failed to retrieve stored variant: %v", err)
	}
	if varInfo.StorageCompression != "super-rot" {
		t.Fatalf("expected variant StorageCompression %q, got %q", "super-rot", varInfo.StorageCompression)
	}
	if !strings.HasPrefix(string(rawBody), "CUST") {
		t.Fatalf("expected custom header CUST in stored body")
	}

	// Cache hit
	res2 := doReq(t, handler, req)
	if res2.Body.String() != payload {
		t.Fatalf("cache hit body mismatch: got %q, want %q", res2.Body.String(), payload)
	}
	if originCalls != 1 {
		t.Fatalf("expected 1 origin call, got %d", originCalls)
	}
}

func TestBackwardCompatibleLZ4(t *testing.T) {
	t.Parallel()

	// Simulates an existing key in Redis stored prior to the introduction of storage_compression.
	// In proto3, unset string fields default to "".
	store := teststore.New()
	mw, err := New(store, WithStorageCompression(StorageCompressionZstd)) // Active writer is zstd!
	if err != nil {
		t.Fatalf("failed to create Titip: %v", err)
	}
	defer func() { _ = mw.Close(context.Background()) }()

	payload := []byte(strings.Repeat("Existing legacy cache entry compressed with LZ4.", 20))

	// Compress payload with LZ4
	lz4Comp := NewLZ4Compressor()
	compBuf := getBuffer()
	defer putBuffer(compBuf)
	if err := lz4Comp.Compress(payload, compBuf); err != nil {
		t.Fatalf("failed to compress with LZ4: %v", err)
	}

	rawURL := "http://example.com/legacy-key"
	req := httptest.NewRequest(http.MethodGet, rawURL, nil)
	primaryKey := generatePrimaryKey(req, &CacheKey{})
	now := time.Now()

	meta := &pb.CacheMetadata{
		PrimaryKey:        primaryKey,
		CreatedAtUnixNano: now.UnixNano(),
		ExpiresAtUnixNano: now.Add(1 * time.Hour).UnixNano(),
	}

	varInfo := &pb.VariantInfo{
		VariantKey: defaultVariantKey,
		StatusCode: http.StatusOK,
		ResponseHeaders: map[string]*pb.HeaderValues{
			"Content-Type": {Values: []string{"text/plain"}},
		},
		RawBodySize:        int64(len(payload)),
		StorageCompression: "", // Unset / legacy empty string!
	}

	if err := store.SetVariant(context.Background(), primaryKey, meta, varInfo, compBuf.Bytes(), 1*time.Hour); err != nil {
		t.Fatalf("failed to store legacy variant: %v", err)
	}

	originCalls := 0
	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls++
		w.WriteHeader(http.StatusInternalServerError)
	})

	handler := mw.testHandler(origin)

	res := doReq(t, handler, req)
	if res.Code != http.StatusOK {
		t.Fatalf("expected status 200 from legacy cache hit, got %d", res.Code)
	}
	if res.Body.String() != string(payload) {
		t.Fatalf("decompression mismatch: got %q, want %q", res.Body.String(), string(payload))
	}
	if originCalls != 0 {
		t.Fatalf("expected 0 origin calls, got %d", originCalls)
	}
	if !strings.Contains(strings.ToLower(res.Header().Get(headerCacheStatus)), "hit") {
		t.Fatalf("expected cache hit, got %s", res.Header().Get(headerCacheStatus))
	}
}

func TestMultiCodecMigration_ConcurrentDecompress(t *testing.T) {
	t.Parallel()

	customA := &mockCustomCompressor{name: "cust-a"}
	store := teststore.New()

	mw, err := New(store,
		WithStorageCompressor(customA),
		WithStorageCompression(StorageCompressionZstd), // Active writer is zstd
	)
	if err != nil {
		t.Fatalf("failed to create Titip: %v", err)
	}
	defer func() { _ = mw.Close(context.Background()) }()

	type keyFixture struct {
		url     string
		codec   string
		comp    Compressor
		payload string
	}

	fixtures := []keyFixture{
		{
			url:     "http://example.com/item-lz4",
			codec:   StorageCompressionLZ4,
			comp:    NewLZ4Compressor(),
			payload: strings.Repeat("LZ4 compressed representation data. ", 15),
		},
		{
			url:     "http://example.com/item-zstd",
			codec:   StorageCompressionZstd,
			comp:    NewZstdCompressor(),
			payload: strings.Repeat("Zstandard compressed representation data. ", 15),
		},
		{
			url:     "http://example.com/item-none",
			codec:   StorageCompressionNone,
			comp:    NewNoneCompressor(),
			payload: strings.Repeat("Uncompressed raw representation data. ", 15),
		},
		{
			url:     "http://example.com/item-custom",
			codec:   "cust-a",
			comp:    customA,
			payload: strings.Repeat("Custom algorithm representation data. ", 15),
		},
	}

	// Preload all 4 keys with their respective codecs
	now := time.Now()
	for _, f := range fixtures {
		cBuf := getBuffer()
		if err := f.comp.Compress([]byte(f.payload), cBuf); err != nil {
			t.Fatalf("failed to compress fixture %s: %v", f.url, err)
		}
		req := httptest.NewRequest(http.MethodGet, f.url, nil)
		pk := generatePrimaryKey(req, &CacheKey{})
		meta := &pb.CacheMetadata{
			PrimaryKey:        pk,
			CreatedAtUnixNano: now.UnixNano(),
			ExpiresAtUnixNano: now.Add(1 * time.Hour).UnixNano(),
		}
		varInfo := &pb.VariantInfo{
			VariantKey: defaultVariantKey,
			StatusCode: http.StatusOK,
			ResponseHeaders: map[string]*pb.HeaderValues{
				"Content-Type": {Values: []string{"text/plain"}},
			},
			RawBodySize:        int64(len(f.payload)),
			StorageCompression: f.codec,
		}
		if err := store.SetVariant(context.Background(), pk, meta, varInfo, cBuf.Bytes(), 1*time.Hour); err != nil {
			t.Fatalf("failed to store variant %s: %v", f.url, err)
		}
		putBuffer(cBuf)
	}

	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("origin should not be called for warm cache entries: %s", r.URL.String())
	})
	handler := mw.testHandler(origin)

	// Concurrently query all 4 keys across 100 goroutines
	var wg sync.WaitGroup
	const concurrency = 25
	for i := range concurrency {
		for _, f := range fixtures {
			wg.Add(1)
			go func(iter int, fix keyFixture) {
				defer wg.Done()
				req := httptest.NewRequest(http.MethodGet, fix.url, nil)
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)

				if rec.Code != http.StatusOK {
					t.Errorf("expected 200, got %d for %s", rec.Code, fix.url)
				}
				if rec.Body.String() != fix.payload {
					t.Errorf("payload mismatch for %s: got %s, want %s", fix.url, rec.Body.String(), fix.payload)
				}
			}(i, f)
		}
	}
	wg.Wait()
}

func TestUnknownCodec_FailOpenAndSelfHeal(t *testing.T) {
	t.Parallel()

	store := teststore.New()
	mw, err := New(store, WithStorageCompression(StorageCompressionZstd))
	if err != nil {
		t.Fatalf("failed to create Titip: %v", err)
	}
	defer func() { _ = mw.Close(context.Background()) }()

	rawURL := "http://example.com/unknown-codec-item"
	req := httptest.NewRequest(http.MethodGet, rawURL, nil)
	primaryKey := generatePrimaryKey(req, &CacheKey{})
	now := time.Now()

	// Seed store with an unknown codec
	meta := &pb.CacheMetadata{
		PrimaryKey:        primaryKey,
		CreatedAtUnixNano: now.UnixNano(),
		ExpiresAtUnixNano: now.Add(1 * time.Hour).UnixNano(),
	}
	varInfo := &pb.VariantInfo{
		VariantKey: defaultVariantKey,
		StatusCode: http.StatusOK,
		ResponseHeaders: map[string]*pb.HeaderValues{
			"Content-Type": {Values: []string{"text/plain"}},
		},
		RawBodySize:        100,
		StorageCompression: "obsolete-gzip-v1", // Unknown codec!
	}

	if err := store.SetVariant(context.Background(), primaryKey, meta, varInfo, []byte("junk body"), 1*time.Hour); err != nil {
		t.Fatalf("failed to store variant: %v", err)
	}

	freshPayload := strings.Repeat("Fresh origin content after self-healing! ", 20)
	originCalls := 0
	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls++
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(freshPayload))
	})

	handler := mw.testHandler(origin)

	// 1. First request fails-open to origin because codec is unknown
	res1 := doReq(t, handler, req)
	if res1.Code != http.StatusOK {
		t.Fatalf("expected 200 from fail-open, got %d", res1.Code)
	}
	if res1.Body.String() != freshPayload {
		t.Fatalf("expected fresh origin payload, got %q", res1.Body.String())
	}
	if originCalls != 1 {
		t.Fatalf("expected 1 origin call on fail-open, got %d", originCalls)
	}

	// Verify storage was self-healed with the active codec ("zstd")
	updatedVar, _, err := store.GetVariant(context.Background(), primaryKey, defaultVariantKey)
	if err != nil || updatedVar == nil {
		t.Fatalf("failed to get healed variant: %v", err)
	}
	if updatedVar.StorageCompression != StorageCompressionZstd {
		t.Fatalf("expected healed variant StorageCompression to be %q, got %q", StorageCompressionZstd, updatedVar.StorageCompression)
	}

	// 2. Second request hits the healed cache
	res2 := doReq(t, handler, req)
	if res2.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res2.Code)
	}
	if res2.Body.String() != freshPayload {
		t.Fatalf("expected fresh payload on healed cache hit")
	}
	if originCalls != 1 {
		t.Fatalf("expected 0 additional origin calls, got %d", originCalls)
	}
}

func TestCorruptBody_FailOpenAndSelfHeal(t *testing.T) {
	t.Parallel()

	store := teststore.New()
	mw, err := New(store, WithStorageCompression(StorageCompressionLZ4))
	if err != nil {
		t.Fatalf("failed to create Titip: %v", err)
	}
	defer func() { _ = mw.Close(context.Background()) }()

	rawURL := "http://example.com/corrupt-body-item"
	req := httptest.NewRequest(http.MethodGet, rawURL, nil)
	primaryKey := generatePrimaryKey(req, &CacheKey{})
	now := time.Now()

	// Seed store with valid codec name but corrupted compressed bytes
	meta := &pb.CacheMetadata{
		PrimaryKey:        primaryKey,
		CreatedAtUnixNano: now.UnixNano(),
		ExpiresAtUnixNano: now.Add(1 * time.Hour).UnixNano(),
	}
	varInfo := &pb.VariantInfo{
		VariantKey: defaultVariantKey,
		StatusCode: http.StatusOK,
		ResponseHeaders: map[string]*pb.HeaderValues{
			"Content-Type": {Values: []string{"text/plain"}},
		},
		RawBodySize:        500,
		StorageCompression: StorageCompressionLZ4,
	}

	if err := store.SetVariant(context.Background(), primaryKey, meta, varInfo, []byte("totally corrupt non-lz4 bytes"), 1*time.Hour); err != nil {
		t.Fatalf("failed to store variant: %v", err)
	}

	freshPayload := strings.Repeat("Fresh payload after recovering from corruption! ", 20)
	originCalls := 0
	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls++
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(freshPayload))
	})

	handler := mw.testHandler(origin)

	// Fails open to origin
	res1 := doReq(t, handler, req)
	if res1.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res1.Code)
	}
	if res1.Body.String() != freshPayload {
		t.Fatalf("mismatch in origin payload")
	}
	if originCalls != 1 {
		t.Fatalf("expected 1 origin call, got %d", originCalls)
	}

	// Cache hit on re-request
	res2 := doReq(t, handler, req)
	if res2.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res2.Code)
	}
	if res2.Body.String() != freshPayload {
		t.Fatalf("mismatch on cache hit")
	}
	if originCalls != 1 {
		t.Fatalf("expected 0 extra origin calls, got %d", originCalls)
	}
}

func TestFailingCompressor_FallbackToUncompressed(t *testing.T) {
	t.Parallel()

	failing := &failingCompressor{}
	store, _, mw := setupTestTitip(t,
		WithStorageCompressor(failing),
		WithStorageCompression("failing"),
	)

	payload := strings.Repeat("Test failing compressor fallback string! ", 25)

	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(payload))
	})

	handler := mw.testHandler(origin)
	req := httptest.NewRequest(http.MethodGet, "http://example.com/test-failing-compressor", nil)
	pk := generatePrimaryKey(req, &CacheKey{})

	// Cold miss -> failing compressor fails, Titip logs warning and stores uncompressed as "none"
	res1 := doReq(t, handler, req)
	if res1.Body.String() != payload {
		t.Fatalf("cold miss body mismatch")
	}

	// Verify variant in store has StorageCompression = "none" and raw payload
	varInfo, rawBody, err := store.GetVariant(context.Background(), pk, defaultVariantKey)
	if err != nil || varInfo == nil {
		t.Fatalf("failed to retrieve variant: %v", err)
	}
	if varInfo.StorageCompression != StorageCompressionNone {
		t.Fatalf("expected variant StorageCompression %q on fallback, got %q", StorageCompressionNone, varInfo.StorageCompression)
	}
	if !bytes.Equal(rawBody, []byte(payload)) {
		t.Fatalf("expected uncompressed raw bytes in storage")
	}

	// Subsequent request succeeds cleanly via "none" decompressor
	res2 := doReq(t, handler, req)
	if res2.Body.String() != payload {
		t.Fatalf("cache hit body mismatch: got %q, want %q", res2.Body.String(), payload)
	}
}

func BenchmarkCompressorPoolRecycling(b *testing.B) {
	payload := make([]byte, 4096)
	_, _ = rand.Read(payload)

	compressors := []struct {
		name string
		comp Compressor
	}{
		{"lz4", NewLZ4Compressor()},
		{"zstd", NewZstdCompressor()},
	}

	for _, tc := range compressors {
		compBuf := getBuffer()
		_ = tc.comp.Compress(payload, compBuf)
		compBytes := bytes.Clone(compBuf.Bytes())
		putBuffer(compBuf)

		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				dst := getBuffer()
				_ = tc.comp.Decompress(compBytes, dst)
				putBuffer(dst)
			}
		})
	}
}
