package titip

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// Storage compression codec identifiers.
const (
	// StorageCompressionLZ4 indicates LZ4 compression (default).
	StorageCompressionLZ4 = "lz4"

	// StorageCompressionZstd indicates Zstandard compression.
	StorageCompressionZstd = "zstd"

	// StorageCompressionNone indicates identity / uncompressed raw bytes.
	StorageCompressionNone = "none"

	// minCompressionSize defines the minimum body length (256 bytes) eligible for storage compression.
	// Payloads smaller than this threshold suffer from compression framing overhead and are stored raw as "none".
	minCompressionSize = 256
)

// Compressor defines the pluggable storage compression engine interface.
type Compressor interface {
	// Name returns the unique, human-readable name of the codec recorded in storage metadata.
	Name() string

	// Compress compresses src bytes into dst buffer.
	Compress(src []byte, dst *bytes.Buffer) error

	// Decompress decompresses compressed src bytes into dst buffer.
	Decompress(src []byte, dst *bytes.Buffer) error
}

// bytesReaderPool pools *bytes.Reader instances to wrap source byte slices without allocation during decompression.
var bytesReaderPool = sync.Pool{
	New: func() any {
		return bytes.NewReader(nil)
	},
}

// --- LZ4 Compressor ---

type lz4Compressor struct{}

var lz4WriterPool = sync.Pool{
	New: func() any {
		return lz4.NewWriter(nil)
	},
}

var lz4ReaderPool = sync.Pool{
	New: func() any {
		return lz4.NewReader(nil)
	},
}

func (c *lz4Compressor) Name() string {
	return StorageCompressionLZ4
}

func (c *lz4Compressor) Compress(src []byte, dst *bytes.Buffer) error {
	if dst == nil {
		return fmt.Errorf("titip: compress: destination buffer is nil")
	}
	zw := lz4WriterPool.Get().(*lz4.Writer)
	defer lz4WriterPool.Put(zw)

	zw.Reset(dst)
	if _, err := zw.Write(src); err != nil {
		return fmt.Errorf("titip: compress: lz4 write error: %w", err)
	}
	if err := zw.Close(); err != nil {
		return fmt.Errorf("titip: compress: lz4 close error: %w", err)
	}
	return nil
}

func (c *lz4Compressor) Decompress(src []byte, dst *bytes.Buffer) error {
	if dst == nil {
		return fmt.Errorf("titip: decompress: destination buffer is nil")
	}
	zr := lz4ReaderPool.Get().(*lz4.Reader)
	defer lz4ReaderPool.Put(zr)

	br := bytesReaderPool.Get().(*bytes.Reader)
	br.Reset(src)
	defer func() {
		br.Reset(nil)
		bytesReaderPool.Put(br)
	}()

	zr.Reset(br)
	if _, err := dst.ReadFrom(zr); err != nil {
		return fmt.Errorf("titip: decompress: lz4 read error: %w", err)
	}
	return nil
}

// NewLZ4Compressor creates a new Compressor instance backed by LZ4.
func NewLZ4Compressor() Compressor {
	return &lz4Compressor{}
}

// --- None (Identity) Compressor ---

type noneCompressor struct{}

func (c *noneCompressor) Name() string {
	return StorageCompressionNone
}

func (c *noneCompressor) Compress(src []byte, dst *bytes.Buffer) error {
	if dst == nil {
		return fmt.Errorf("titip: compress: destination buffer is nil")
	}
	_, err := dst.Write(src)
	if err != nil {
		return fmt.Errorf("titip: compress: write error: %w", err)
	}
	return nil
}

func (c *noneCompressor) Decompress(src []byte, dst *bytes.Buffer) error {
	if dst == nil {
		return fmt.Errorf("titip: decompress: destination buffer is nil")
	}
	_, err := dst.Write(src)
	if err != nil {
		return fmt.Errorf("titip: decompress: write error: %w", err)
	}
	return nil
}

var noneInstance = &noneCompressor{}

// NewNoneCompressor creates a new Compressor instance for raw, uncompressed payload storage.
func NewNoneCompressor() Compressor {
	return noneInstance
}

// --- Zstandard Compressor ---

type zstdCompressor struct{}

var zstdWriterPool = sync.Pool{
	New: func() any {
		w, _ := zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.SpeedDefault),
			zstd.WithWindowSize(512*1024),
			zstd.WithEncoderConcurrency(1),
		)
		return w
	},
}

var zstdReaderPool = sync.Pool{
	New: func() any {
		r, _ := zstd.NewReader(nil,
			zstd.WithDecoderConcurrency(1),
		)
		return r
	},
}

func (c *zstdCompressor) Name() string {
	return StorageCompressionZstd
}

func (c *zstdCompressor) Compress(src []byte, dst *bytes.Buffer) error {
	if dst == nil {
		return fmt.Errorf("titip: compress: destination buffer is nil")
	}
	zw := zstdWriterPool.Get().(*zstd.Encoder)
	defer zstdWriterPool.Put(zw)

	zw.Reset(dst)
	if _, err := zw.Write(src); err != nil {
		return fmt.Errorf("titip: compress: zstd write error: %w", err)
	}
	if err := zw.Close(); err != nil {
		return fmt.Errorf("titip: compress: zstd close error: %w", err)
	}
	return nil
}

func (c *zstdCompressor) Decompress(src []byte, dst *bytes.Buffer) error {
	if dst == nil {
		return fmt.Errorf("titip: decompress: destination buffer is nil")
	}
	zr := zstdReaderPool.Get().(*zstd.Decoder)
	defer zstdReaderPool.Put(zr)

	br := bytesReaderPool.Get().(*bytes.Reader)
	br.Reset(src)
	defer func() {
		br.Reset(nil)
		bytesReaderPool.Put(br)
	}()

	if err := zr.Reset(br); err != nil {
		return fmt.Errorf("titip: decompress: zstd reset error: %w", err)
	}
	if _, err := dst.ReadFrom(zr); err != nil {
		return fmt.Errorf("titip: decompress: zstd read error: %w", err)
	}
	return nil
}

// NewZstdCompressor creates a new Compressor instance backed by Zstandard.
func NewZstdCompressor() Compressor {
	return &zstdCompressor{}
}

// --- Smart Auto-Bypass Helpers ---

// isPrecompressedContentType checks if Content-Type indicates inherently compressed media
// (e.g. images/*, audio/*, video/*, or archive formats like application/zip, application/gzip, application/zstd, and application/pdf).
func isPrecompressedContentType(ct string) bool {
	if ct == "" {
		return false
	}
	base, _, _ := strings.Cut(ct, ";")
	ct = strings.TrimSpace(strings.ToLower(base))
	if strings.HasPrefix(ct, "text/") ||
		ct == "application/json" ||
		ct == "application/javascript" ||
		ct == "application/xml" {
		return false
	}
	switch {
	case strings.HasPrefix(ct, "image/"),
		strings.HasPrefix(ct, "video/"),
		strings.HasPrefix(ct, "audio/"):
		// Text-based or uncompressed image formats that benefit from compression
		return ct != "image/svg+xml" && ct != "image/x-icon" && ct != "image/vnd.microsoft.icon"
	case strings.Contains(ct, "zip"),
		strings.Contains(ct, "gzip"),
		strings.Contains(ct, "compressed"),
		strings.Contains(ct, "zstd"),
		strings.Contains(ct, "brotli"),
		strings.Contains(ct, "pdf"),
		strings.Contains(ct, "/tar"),
		strings.Contains(ct, "/x-tar"):
		return true
	default:
		return false
	}
}

// resolveCompressor resolves the storage compressor for a variant payload.
// Automatically bypasses compression (<256B, wire-encoded Content-Encoding, or pre-compressed Content-Type)
// to prevent double compression and CPU waste.
func resolveCompressor(configured Compressor, headers http.Header, bodyLen int) Compressor {
	if bodyLen < minCompressionSize {
		return noneInstance
	}
	if headers.Get(headerContentEncoding) != "" {
		return noneInstance
	}
	if isPrecompressedContentType(headers.Get(headerContentType)) {
		return noneInstance
	}
	if configured == nil {
		return noneInstance
	}
	return configured
}
