package teststore

import (
	"bytes"
	"context"
	"errors"
	"regexp"
	"strings"
	"sync"
	"time"

	pb "github.com/indragunawan/titip/proto"
	"github.com/indragunawan/titip/storage"
	"google.golang.org/protobuf/proto"
)

var (
	_ storage.Storage       = (*Store)(nil)
	_ storage.PatternPurger = (*Store)(nil)
	_ storage.AllPurger     = (*Store)(nil)

	// ErrClosed is returned when operations are attempted on a closed Store.
	ErrClosed = errors.New("teststore: store is closed")
)

type variantEntry struct {
	variant   *pb.VariantInfo
	body      []byte
	expiresAt time.Time
}

type metaEntry struct {
	meta       *pb.CacheMetadata
	softPurged bool
	expiresAt  time.Time
}

// Store is a thread-safe, in-memory implementation of storage.Storage designed for unit testing.
type Store struct {
	mu       sync.RWMutex
	closed   bool
	meta     map[string]*metaEntry
	variants map[string]map[string]*variantEntry
	tagIndex map[string]map[string]struct{} // tag -> map[primaryKey]struct{}

	// Fault injection hooks (optional)
	getMetaHook    func(ctx context.Context, primaryKey string) (*pb.CacheMetadata, bool, error)
	getVariantHook func(ctx context.Context, primaryKey, variantKey string) (*pb.VariantInfo, []byte, error)
	setVariantHook func(ctx context.Context, primaryKey string, meta *pb.CacheMetadata, variant *pb.VariantInfo, body []byte, ttl time.Duration) error
	purgeHook      func(ctx context.Context, primaryKey string, soft bool) (int64, error)
	purgeTagHook   func(ctx context.Context, tag string, soft bool) (int64, error)
}

// New creates a new instance of Store.
func New() *Store {
	return &Store{
		meta:     make(map[string]*metaEntry),
		variants: make(map[string]map[string]*variantEntry),
		tagIndex: make(map[string]map[string]struct{}),
	}
}

// SetClosed simulates a storage outage / network disconnection.
func (s *Store) SetClosed(closed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = closed
}

// SetGetMetaHook sets a custom hook for GetMeta operations.
func (s *Store) SetGetMetaHook(fn func(ctx context.Context, primaryKey string) (*pb.CacheMetadata, bool, error)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getMetaHook = fn
}

// SetGetVariantHook sets a custom hook for GetVariant operations.
func (s *Store) SetGetVariantHook(fn func(ctx context.Context, primaryKey, variantKey string) (*pb.VariantInfo, []byte, error)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getVariantHook = fn
}

// SetSetVariantHook sets a custom hook for SetVariant operations.
func (s *Store) SetSetVariantHook(fn func(ctx context.Context, primaryKey string, meta *pb.CacheMetadata, variant *pb.VariantInfo, body []byte, ttl time.Duration) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setVariantHook = fn
}

// SetPurgeHook sets a custom hook for Purge operations.
func (s *Store) SetPurgeHook(fn func(ctx context.Context, primaryKey string, soft bool) (int64, error)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeHook = fn
}

// SetPurgeByTagHook sets a custom hook for PurgeByTag operations.
func (s *Store) SetPurgeByTagHook(fn func(ctx context.Context, tag string, soft bool) (int64, error)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeTagHook = fn
}

// Reset clears all data and resets hooks.
func (s *Store) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.meta = make(map[string]*metaEntry)
	s.variants = make(map[string]map[string]*variantEntry)
	s.tagIndex = make(map[string]map[string]struct{})
	s.closed = false
	s.getMetaHook = nil
	s.getVariantHook = nil
	s.setVariantHook = nil
	s.purgeHook = nil
	s.purgeTagHook = nil
}

// GetMeta retrieves the primary index, Vary headers, and soft-purged operational status.
func (s *Store) GetMeta(ctx context.Context, primaryKey string) (*pb.CacheMetadata, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, false, ErrClosed
	}
	if s.getMetaHook != nil {
		return s.getMetaHook(ctx, primaryKey)
	}

	entry, ok := s.meta[primaryKey]
	if !ok {
		return nil, false, nil
	}

	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		return nil, false, nil
	}

	clonedMeta := proto.Clone(entry.meta).(*pb.CacheMetadata)
	if vMap, hasVariants := s.variants[primaryKey]; hasVariants {
		clonedMeta.Variants = make(map[string]*pb.VariantInfo, len(vMap))
		now := time.Now()
		for vk, vEntry := range vMap {
			if vEntry.expiresAt.IsZero() || now.Before(vEntry.expiresAt) {
				clonedMeta.Variants[vk] = proto.Clone(vEntry.variant).(*pb.VariantInfo)
			}
		}
	} else {
		clonedMeta.Variants = make(map[string]*pb.VariantInfo)
	}

	return clonedMeta, entry.softPurged, nil
}

// GetVariant retrieves a specific variant metadata and its body payload.
func (s *Store) GetVariant(ctx context.Context, primaryKey, variantKey string) (*pb.VariantInfo, []byte, error) {
	if variantKey == "" {
		variantKey = "default"
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, nil, ErrClosed
	}
	if s.getVariantHook != nil {
		return s.getVariantHook(ctx, primaryKey, variantKey)
	}

	vMap, ok := s.variants[primaryKey]
	if !ok {
		return nil, nil, nil
	}

	entry, ok := vMap[variantKey]
	if !ok {
		return nil, nil, nil
	}

	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		return nil, nil, nil
	}

	return proto.Clone(entry.variant).(*pb.VariantInfo), bytes.Clone(entry.body), nil
}

// SetVariant atomically records a new or updated variant and its body payload.
func (s *Store) SetVariant(ctx context.Context, primaryKey string, meta *pb.CacheMetadata, variant *pb.VariantInfo, body []byte, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}
	if s.setVariantHook != nil {
		return s.setVariantHook(ctx, primaryKey, meta, variant, body, ttl)
	}

	expiresAt := time.Time{}
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	// Update meta entry and reset soft-purged flag on fresh write (matching Redis HDEL _soft_purged)
	existingMeta, exists := s.meta[primaryKey]
	if exists {
		if expiresAt.After(existingMeta.expiresAt) {
			existingMeta.expiresAt = expiresAt
		}
		if meta != nil {
			existingMeta.meta = proto.Clone(meta).(*pb.CacheMetadata)
		}
		existingMeta.softPurged = false
	} else {
		s.meta[primaryKey] = &metaEntry{
			meta:       proto.Clone(meta).(*pb.CacheMetadata),
			softPurged: false,
			expiresAt:  expiresAt,
		}
	}

	// Index tags
	if meta != nil && len(meta.Tags) > 0 {
		for _, tag := range meta.Tags {
			if s.tagIndex[tag] == nil {
				s.tagIndex[tag] = make(map[string]struct{})
			}
			s.tagIndex[tag][primaryKey] = struct{}{}
		}
	}

	// Update variant entry
	if s.variants[primaryKey] == nil {
		s.variants[primaryKey] = make(map[string]*variantEntry)
	}

	variantKey := "default"
	if variant != nil && variant.VariantKey != "" {
		variantKey = variant.VariantKey
	} else if variant != nil {
		variant.VariantKey = "default"
	}

	s.variants[primaryKey][variantKey] = &variantEntry{
		variant:   proto.Clone(variant).(*pb.VariantInfo),
		body:      bytes.Clone(body),
		expiresAt: expiresAt,
	}

	return nil
}

// Purge invalidates the primary metadata and its associated variant body keys.
func (s *Store) Purge(ctx context.Context, primaryKey string, soft bool) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, ErrClosed
	}
	if s.purgeHook != nil {
		return s.purgeHook(ctx, primaryKey, soft)
	}

	entry, ok := s.meta[primaryKey]
	if !ok {
		return 0, nil
	}

	if soft {
		entry.softPurged = true
		return 1, nil
	}

	// Hard purge: delete meta, variants, and tag references
	if entry.meta != nil {
		for _, tag := range entry.meta.Tags {
			if keys, found := s.tagIndex[tag]; found {
				delete(keys, primaryKey)
				if len(keys) == 0 {
					delete(s.tagIndex, tag)
				}
			}
		}
	}

	delete(s.meta, primaryKey)
	delete(s.variants, primaryKey)
	return 1, nil
}

// PurgeByTag invalidates all primary metadata matching a given tag.
func (s *Store) PurgeByTag(ctx context.Context, tag string, soft bool) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, ErrClosed
	}
	if s.purgeTagHook != nil {
		return s.purgeTagHook(ctx, tag, soft)
	}

	keys, ok := s.tagIndex[tag]
	if !ok || len(keys) == 0 {
		return 0, nil
	}

	targetKeys := make([]string, 0, len(keys))
	for k := range keys {
		targetKeys = append(targetKeys, k)
	}

	var count int64
	for _, k := range targetKeys {
		entry, exists := s.meta[k]
		if !exists {
			continue
		}
		if soft {
			entry.softPurged = true
			count++
		} else {
			if entry.meta != nil {
				for _, t := range entry.meta.Tags {
					if tKeys, found := s.tagIndex[t]; found {
						delete(tKeys, k)
						if len(tKeys) == 0 {
							delete(s.tagIndex, t)
						}
					}
				}
			}
			delete(s.meta, k)
			delete(s.variants, k)
			count++
		}
	}

	if !soft {
		delete(s.tagIndex, tag)
	}

	return count, nil
}

// PurgeByPattern invalidates all keys matching the given glob pattern within the storage.
func (s *Store) PurgeByPattern(ctx context.Context, pattern string, soft bool) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, ErrClosed
	}

	var matchingKeys []string
	for k := range s.meta {
		if matchGlob(pattern, k) {
			matchingKeys = append(matchingKeys, k)
		}
	}

	var count int64
	for _, k := range matchingKeys {
		entry, ok := s.meta[k]
		if !ok {
			continue
		}
		if soft {
			entry.softPurged = true
			count++
		} else {
			if entry.meta != nil {
				for _, tag := range entry.meta.Tags {
					if tKeys, found := s.tagIndex[tag]; found {
						delete(tKeys, k)
						if len(tKeys) == 0 {
							delete(s.tagIndex, tag)
						}
					}
				}
			}
			delete(s.meta, k)
			delete(s.variants, k)
			count++
		}
	}

	return count, nil
}

// PurgeAll wipes every entry in the store.
func (s *Store) PurgeAll(ctx context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, ErrClosed
	}

	count := int64(len(s.meta))
	s.meta = make(map[string]*metaEntry)
	s.variants = make(map[string]map[string]*variantEntry)
	s.tagIndex = make(map[string]map[string]struct{})
	return count, nil
}

// Close closes the store.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// matchGlob performs Redis-style glob pattern matching (* matches any sequence including /, ? matches single char).
func matchGlob(pattern, s string) bool {
	var sb strings.Builder
	sb.WriteString("^")
	for i := 0; i < len(pattern); i++ {
		c := pattern[i]
		switch c {
		case '*':
			sb.WriteString(".*")
		case '?':
			sb.WriteString(".")
		case '.', '+', '(', ')', '{', '}', '^', '$', '|', '\\', '[', ']':
			sb.WriteString(`\`)
			sb.WriteByte(c)
		default:
			sb.WriteByte(c)
		}
	}
	sb.WriteString("$")
	re, err := regexp.Compile(sb.String())
	if err != nil {
		return false
	}
	return re.MatchString(s)
}
