package storage

import (
	"context"
	"time"

	pb "github.com/indragunawan/titip/proto"
)

// Storage defines the decoupled interface for Titip cache storage backends.
type Storage interface {
	// GetMeta retrieves the primary index, Vary headers, and soft-purged operational status for the primary URL key.
	// Returns (nil, false, nil) if the entry does not exist.
	GetMeta(ctx context.Context, primaryKey string) (*pb.CacheMetadata, bool, error)

	// GetVariant retrieves a specific variant metadata and its compressed body payload.
	// Returns (nil, nil, nil) if the variant or body does not exist.
	GetVariant(ctx context.Context, primaryKey, variantKey string) (*pb.VariantInfo, []byte, error)

	// SetVariant atomically records a new or updated variant and its body payload without rewriting the full metadata index.
	SetVariant(ctx context.Context, primaryKey string, meta *pb.CacheMetadata, variant *pb.VariantInfo, body []byte, ttl time.Duration) error

	// Purge invalidates the primary metadata and its associated variant body keys.
	// If soft is true, marks the entry as stale for fallback while preserving payload data.
	// If soft is false (hard purge), physically deletes the metadata and all variant body keys.
	// Returns the number of primary entries purged (1 if found, 0 if not found).
	Purge(ctx context.Context, primaryKey string, soft bool) (int64, error)

	// PurgeByPattern invalidates all keys matching the given glob pattern within the storage namespace.
	// If soft is true, marks all matched entries as stale.
	// If soft is false (hard purge), physically deletes all matched metadata and associated body keys.
	// Returns the number of matching primary metadata entries invalidated.
	PurgeByPattern(ctx context.Context, pattern string, soft bool) (int64, error)

	// PurgeByTag invalidates all primary metadata and all associated body keys matching a given tag.
	// Returns the total number of primary entries invalidated.
	PurgeByTag(ctx context.Context, tag string, soft bool) (int64, error)

	// PurgeAll deletes every key in the storage engine's configured namespace prefix.
	// Returns the number of primary metadata entries deleted.
	PurgeAll(ctx context.Context) (int64, error)
}

// Closer is an optional interface that storage backends may implement
// to receive graceful shutdown notifications with a context deadline.
// Alternatively, storage backends may implement the standard library io.Closer interface.
type Closer interface {
	Close(ctx context.Context) error
}
