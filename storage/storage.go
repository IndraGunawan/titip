package storage

import (
	"context"
	"time"

	pb "github.com/indragunawan/titip/proto"
)

// Storage defines the decoupled interface for Titip cache storage backends.
type Storage interface {
	// GetMeta retrieves the primary index and Vary headers for the primary URL key.
	// Returns (nil, nil) if the entry does not exist.
	GetMeta(ctx context.Context, primaryKey string) (*pb.CacheMetadata, error)

	// GetVariant retrieves a specific variant metadata and its compressed body payload.
	// Returns (nil, nil, nil) if the variant or body does not exist.
	GetVariant(ctx context.Context, primaryKey, variantKey string) (*pb.VariantInfo, []byte, error)

	// SetVariant atomically records a new or updated variant and its body payload without rewriting the full metadata index.
	SetVariant(ctx context.Context, primaryKey string, meta *pb.CacheMetadata, variant *pb.VariantInfo, body []byte, ttl time.Duration) error

	// Delete physically evicts the primary metadata Hash AND all associated variant body keys from storage.
	Delete(ctx context.Context, primaryKey string) error

	// SoftPurge marks primary metadata as stale in storage while preserving entries for fallback.
	SoftPurge(ctx context.Context, primaryKey string) error

	// PurgeByTag invalidates all primary metadata and all associated body keys matching a given tag.
	PurgeByTag(ctx context.Context, tag string, soft bool) error

	// Close cleanly terminates storage connections.
	Close() error
}
