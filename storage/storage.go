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
	// Returns the number of primary entries deleted (1 if found and deleted, 0 if not found).
	Delete(ctx context.Context, primaryKey string) (int64, error)

	// SoftPurge marks primary metadata as stale in storage while preserving entries for fallback.
	// Returns the number of primary entries marked stale (1 if found and updated, 0 if not found).
	SoftPurge(ctx context.Context, primaryKey string) (int64, error)

	// PurgeByTag invalidates all primary metadata and all associated body keys matching a given tag.
	// Returns the total number of primary entries invalidated.
	PurgeByTag(ctx context.Context, tag string, soft bool) (int64, error)

	// Close cleanly terminates storage connections.
	Close() error
}

// PatternDeleter is an optional capability interface implemented by storage engines that
// support glob/pattern-based key deletion (e.g. Redis SCAN + UNLINK).
//
// The pattern syntax follows the backend's native glob syntax:
//   - Redis: https://redis.io/docs/manual/patterns/
//   - "*" matches any sequence of characters
//   - "?" matches any single character
//
// Implementations must guarantee that only keys belonging to the engine's configured
// namespace prefix are matched — never keys outside the prefix.
type PatternDeleter interface {
	// DeletePattern deletes all keys matching the given glob pattern within the storage namespace.
	// The pattern is automatically scoped to the storage engine's configured key prefix.
	// Returns the number of matching primary metadata entries deleted.
	DeletePattern(ctx context.Context, pattern string) (int64, error)
}

// AllPurger is an optional capability interface implemented by storage engines that
// support a total namespace wipeout (e.g. Redis SCAN prefix* + UNLINK).
//
// Only keys within the engine's configured namespace prefix are affected.
// All other keys in the same backend instance are preserved.
type AllPurger interface {
	// PurgeAll deletes every key in the storage engine's configured namespace prefix.
	// Returns the number of primary metadata entries deleted.
	PurgeAll(ctx context.Context) (int64, error)
}
