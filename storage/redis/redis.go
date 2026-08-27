package redis

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/rueidis"
	googleproto "google.golang.org/protobuf/proto"

	pb "github.com/indragunawan/titip/proto"
	"github.com/indragunawan/titip/storage"
)

const (
	defaultPrefix = "titip:"
	indexField    = "_index"

	// scanBatchSize controls how many keys are requested per SCAN iteration.
	scanBatchSize = 100
)

// Config holds configuration options for the Redis storage engine.
type Config struct {
	Prefix string
	Logger *slog.Logger
}

// Option is a function to configure RedisStorage.
type Option func(*Config)

// WithKeyPrefix sets the prefix for all Redis keys (defaults to "titip:").
func WithKeyPrefix(prefix string) Option {
	return func(c *Config) {
		c.Prefix = prefix
	}
}

// WithLogger sets the structured logger for storage operations.
func WithLogger(logger *slog.Logger) Option {
	return func(c *Config) {
		c.Logger = logger
	}
}

// RedisStorage implements storage.Storage with atomic Redis Hashes and pipelined operations.
type RedisStorage struct {
	client rueidis.Client
	prefix string
	logger *slog.Logger
}

// Ensure RedisStorage implements the core Storage interface and optional capability interfaces.
var (
	_ storage.Storage        = (*RedisStorage)(nil)
	_ storage.PatternDeleter = (*RedisStorage)(nil)
	_ storage.AllPurger      = (*RedisStorage)(nil)
)

// New creates a new RedisStorage instance backed by the provided rueidis.Client.
func New(client rueidis.Client, opts ...Option) (*RedisStorage, error) {
	if client == nil {
		return nil, fmt.Errorf("titip: redis: client is required")
	}

	cfg := &Config{
		Prefix: defaultPrefix,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	return &RedisStorage{
		client: client,
		prefix: cfg.Prefix,
		logger: cfg.Logger,
	}, nil
}

// metaKey returns the Redis key for a primary cache entry's metadata Hash.
func (s *RedisStorage) metaKey(primaryKey string) string {
	return s.prefix + "meta:" + primaryKey
}

// bodyKey returns the Redis key for a variant body payload.
func (s *RedisStorage) bodyKey(primaryKey, variantKey string) string {
	return s.prefix + "body:" + primaryKey + ":" + variantKey
}

// tagKey returns the Redis key for a tag's Set of primary keys.
func (s *RedisStorage) tagKey(tagName string) string {
	return s.prefix + "tag:" + tagName
}

// GetMeta retrieves the primary index and all active variants for a primary URL key.
func (s *RedisStorage) GetMeta(ctx context.Context, primaryKey string) (*pb.CacheMetadata, error) {
	cmd := s.client.B().Hgetall().Key(s.metaKey(primaryKey)).Build()
	resp := s.client.Do(ctx, cmd)
	if rueidis.IsRedisNil(resp.Error()) {
		return nil, nil
	}
	if err := resp.Error(); err != nil {
		return nil, fmt.Errorf("titip: redis: get meta: %w", err)
	}

	m, err := resp.AsStrMap()
	if err != nil || len(m) == 0 {
		return nil, nil
	}

	indexStr, ok := m[indexField]
	if !ok {
		return nil, nil
	}

	meta := &pb.CacheMetadata{}
	if err := googleproto.Unmarshal([]byte(indexStr), meta); err != nil {
		return nil, fmt.Errorf("titip: redis: unmarshal meta index: %w", err)
	}

	meta.Variants = make(map[string]*pb.VariantInfo)
	for k, v := range m {
		if k == indexField {
			continue
		}
		varInfo := &pb.VariantInfo{}
		if err := googleproto.Unmarshal([]byte(v), varInfo); err == nil {
			meta.Variants[k] = varInfo
		}
	}

	return meta, nil
}

// GetVariant retrieves a specific variant metadata and its compressed body payload in a single pipelined roundtrip.
func (s *RedisStorage) GetVariant(ctx context.Context, primaryKey, variantKey string) (*pb.VariantInfo, []byte, error) {
	if variantKey == "" {
		variantKey = "default"
	}
	cmd1 := s.client.B().Hget().Key(s.metaKey(primaryKey)).Field(variantKey).Build()
	cmd2 := s.client.B().Get().Key(s.bodyKey(primaryKey, variantKey)).Build()

	resps := s.client.DoMulti(ctx, cmd1, cmd2)

	if rueidis.IsRedisNil(resps[0].Error()) || rueidis.IsRedisNil(resps[1].Error()) {
		return nil, nil, nil
	}

	if err := resps[0].Error(); err != nil {
		return nil, nil, fmt.Errorf("titip: redis: get variant meta: %w", err)
	}
	if err := resps[1].Error(); err != nil {
		return nil, nil, fmt.Errorf("titip: redis: get variant body: %w", err)
	}

	varBytes, err := resps[0].AsBytes()
	if err != nil {
		return nil, nil, fmt.Errorf("titip: redis: parse variant meta bytes: %w", err)
	}

	varInfo := &pb.VariantInfo{}
	if err := googleproto.Unmarshal(varBytes, varInfo); err != nil {
		return nil, nil, fmt.Errorf("titip: redis: unmarshal variant info: %w", err)
	}

	bodyBytes, err := resps[1].AsBytes()
	if err != nil {
		return nil, nil, fmt.Errorf("titip: redis: parse variant body bytes: %w", err)
	}

	return varInfo, bodyBytes, nil
}

// SetVariant atomically saves the variant metadata, compressed body, tags, and updates the dynamic metadata TTL.
func (s *RedisStorage) SetVariant(ctx context.Context, primaryKey string, meta *pb.CacheMetadata, variant *pb.VariantInfo, body []byte, ttl time.Duration) error {
	if variant.VariantKey == "" {
		variant.VariantKey = "default"
	}
	// Create lean index without copying the entire variants map into the _index field
	leanMeta := &pb.CacheMetadata{
		PrimaryKey:         meta.PrimaryKey,
		VaryHeaderNames:    meta.VaryHeaderNames,
		CreatedAtUnixNano:  meta.CreatedAtUnixNano,
		ExpiresAtUnixNano:  meta.ExpiresAtUnixNano,
		StaleUntilUnixNano: meta.StaleUntilUnixNano,
		Tags:               meta.Tags,
		IsSoftPurged:       meta.IsSoftPurged,
	}

	indexBytes, err := googleproto.Marshal(leanMeta)
	if err != nil {
		return fmt.Errorf("titip: redis: marshal meta index: %w", err)
	}

	varBytes, err := googleproto.Marshal(variant)
	if err != nil {
		return fmt.Errorf("titip: redis: marshal variant: %w", err)
	}

	ttlSeconds := int64(ttl.Seconds())
	if ttlSeconds < 1 {
		ttlSeconds = 1
	}

	cmds := make([]rueidis.Completed, 0, 5+len(meta.Tags))

	// 1. HSET metaKey _index <metaBytes> <variantKey> <varBytes>
	hsetCmd := s.client.B().Hset().
		Key(s.metaKey(primaryKey)).
		FieldValue().
		FieldValue(indexField, rueidis.BinaryString(indexBytes)).
		FieldValue(variant.VariantKey, rueidis.BinaryString(varBytes)).
		Build()
	cmds = append(cmds, hsetCmd)

	// 2. SET bodyKey <body> EX <ttl>
	setBodyCmd := s.client.B().Set().
		Key(s.bodyKey(primaryKey, variant.VariantKey)).
		Value(rueidis.BinaryString(body)).
		ExSeconds(ttlSeconds).
		Build()
	cmds = append(cmds, setBodyCmd)

	// 3. Dynamic TTL: Set initial TTL if none exists (NX), or extend if new TTL is greater (GT)
	expireNXCmd := s.client.B().Expire().Key(s.metaKey(primaryKey)).Seconds(ttlSeconds).Nx().Build()
	expireGTCmd := s.client.B().Expire().Key(s.metaKey(primaryKey)).Seconds(ttlSeconds).Gt().Build()
	cmds = append(cmds, expireNXCmd, expireGTCmd)

	// 4. Index tags in Redis Sets with dynamic TTL extension
	for _, tag := range meta.Tags {
		if tag != "" {
			tk := s.tagKey(tag)
			saddCmd := s.client.B().Sadd().
				Key(tk).
				Member(primaryKey).
				Build()
			tagExpireNXCmd := s.client.B().Expire().Key(tk).Seconds(ttlSeconds).Nx().Build()
			tagExpireGTCmd := s.client.B().Expire().Key(tk).Seconds(ttlSeconds).Gt().Build()
			cmds = append(cmds, saddCmd, tagExpireNXCmd, tagExpireGTCmd)
		}
	}

	// Execute all commands in a single pipelined roundtrip
	resps := s.client.DoMulti(ctx, cmds...)
	for i, r := range resps {
		if err := r.Error(); err != nil && !rueidis.IsRedisNil(err) {
			return fmt.Errorf("titip: redis: set variant pipeline error at op %d: %w", i, err)
		}
	}

	return nil
}

// Delete physically deletes the primary metadata Hash and all associated variant body keys.
// Returns 1 if the primary entry existed and was deleted, or 0 if not found.
func (s *RedisStorage) Delete(ctx context.Context, primaryKey string) (int64, error) {
	metaKey := s.metaKey(primaryKey)

	// Fetch all field names to know all variant body keys
	hkeysCmd := s.client.B().Hkeys().Key(metaKey).Build()
	resp := s.client.Do(ctx, hkeysCmd)
	if rueidis.IsRedisNil(resp.Error()) {
		return 0, nil
	}
	if err := resp.Error(); err != nil {
		return 0, fmt.Errorf("titip: redis: delete fetch keys: %w", err)
	}

	keys, err := resp.AsStrSlice()
	if err != nil || len(keys) == 0 {
		return 0, nil
	}

	delKeys := make([]string, 0, len(keys))
	delKeys = append(delKeys, metaKey)
	for _, k := range keys {
		if k != indexField {
			delKeys = append(delKeys, s.bodyKey(primaryKey, k))
		}
	}

	delCmds := make([]rueidis.Completed, 0, len(delKeys))
	for _, k := range delKeys {
		delCmds = append(delCmds, s.client.B().Del().Key(k).Build())
	}
	resps := s.client.DoMulti(ctx, delCmds...)
	for _, r := range resps {
		if err := r.Error(); err != nil && !rueidis.IsRedisNil(err) {
			return 0, fmt.Errorf("titip: redis: delete execute: %w", err)
		}
	}

	if s.logger != nil && s.logger.Enabled(ctx, slog.LevelDebug) {
		s.logger.DebugContext(ctx, "titip: redis: purged cache keys",
			slog.String("primary_key", primaryKey),
			slog.Int("keys_count", len(delKeys)),
			slog.Any("keys", delKeys),
		)
	}

	return 1, nil
}

// SoftPurge marks the primary metadata index as soft-purged while retaining body payloads.
// Returns 1 if the entry existed and was updated, or 0 if not found.
func (s *RedisStorage) SoftPurge(ctx context.Context, primaryKey string) (int64, error) {
	metaKey := s.metaKey(primaryKey)

	hgetCmd := s.client.B().Hget().Key(metaKey).Field(indexField).Build()
	resp := s.client.Do(ctx, hgetCmd)
	if rueidis.IsRedisNil(resp.Error()) {
		return 0, nil
	}
	if err := resp.Error(); err != nil {
		return 0, fmt.Errorf("titip: redis: soft purge get index: %w", err)
	}

	indexBytes, err := resp.AsBytes()
	if err != nil {
		return 0, nil
	}

	meta := &pb.CacheMetadata{}
	if err := googleproto.Unmarshal(indexBytes, meta); err != nil {
		return 0, fmt.Errorf("titip: redis: soft purge unmarshal: %w", err)
	}

	meta.IsSoftPurged = true
	updatedBytes, err := googleproto.Marshal(meta)
	if err != nil {
		return 0, fmt.Errorf("titip: redis: soft purge marshal: %w", err)
	}

	hsetCmd := s.client.B().Hset().
		Key(metaKey).
		FieldValue().
		FieldValue(indexField, rueidis.BinaryString(updatedBytes)).
		Build()
	if err := s.client.Do(ctx, hsetCmd).Error(); err != nil {
		return 0, fmt.Errorf("titip: redis: soft purge save: %w", err)
	}

	if s.logger != nil && s.logger.Enabled(ctx, slog.LevelDebug) {
		s.logger.DebugContext(ctx, "titip: redis: soft-purged cache entry",
			slog.String("primary_key", primaryKey),
			slog.String("meta_key", metaKey),
		)
	}

	return 1, nil
}

// PurgeByTag invalidates all primary metadata hashes and variant body keys matching a given tag.
// Uses non-blocking SSCAN streaming to handle sets of arbitrary size without blocking Redis.
// The tag is treated as a literal string — use PurgeAll for namespace wipeout.
// Returns the number of primary entries invalidated.
func (s *RedisStorage) PurgeByTag(ctx context.Context, tag string, soft bool) (int64, error) {
	tagKey := s.tagKey(tag)
	cursor := uint64(0)
	totalProcessed := 0

	for {
		sscanCmd := s.client.B().Sscan().
			Key(tagKey).
			Cursor(cursor).
			Count(scanBatchSize).
			Build()
		resp := s.client.Do(ctx, sscanCmd)
		if rueidis.IsRedisNil(resp.Error()) {
			break
		}
		if err := resp.Error(); err != nil {
			return 0, fmt.Errorf("titip: redis: purge tag sscan: %w", err)
		}

		scanEntry, err := resp.AsScanEntry()
		if err != nil {
			return 0, fmt.Errorf("titip: redis: purge tag scan entry: %w", err)
		}

		primaryKeys := scanEntry.Elements
		if len(primaryKeys) > 0 {
			if soft {
				for _, pk := range primaryKeys {
					if _, err := s.SoftPurge(ctx, pk); err != nil {
						return 0, err
					}
				}
			} else {
				// Hard purge: pipeline fetching variant keys for this batch
				hkeysCmds := make([]rueidis.Completed, len(primaryKeys))
				for i, pk := range primaryKeys {
					hkeysCmds[i] = s.client.B().Hkeys().Key(s.metaKey(pk)).Build()
				}
				hkeysResps := s.client.DoMulti(ctx, hkeysCmds...)

				delKeys := make([]string, 0, len(primaryKeys)*2)
				for i, r := range hkeysResps {
					pk := primaryKeys[i]
					delKeys = append(delKeys, s.metaKey(pk))
					if keys, err := r.AsStrSlice(); err == nil {
						for _, k := range keys {
							if k != indexField {
								delKeys = append(delKeys, s.bodyKey(pk, k))
							}
						}
					}
				}

				if len(delKeys) > 0 {
					unlinkCmd := s.client.B().Unlink().Key(delKeys...).Build()
					if err := s.client.Do(ctx, unlinkCmd).Error(); err != nil && !rueidis.IsRedisNil(err) {
						return 0, fmt.Errorf("titip: redis: purge tag unlink: %w", err)
					}
				}
			}
			totalProcessed += len(primaryKeys)
		}

		cursor = scanEntry.Cursor
		if cursor == 0 {
			break
		}
	}

	// For hard purge, remove the tag set itself
	if !soft {
		unlinkTagCmd := s.client.B().Unlink().Key(tagKey).Build()
		_ = s.client.Do(ctx, unlinkTagCmd)
	}

	if s.logger != nil && s.logger.Enabled(ctx, slog.LevelDebug) {
		s.logger.DebugContext(ctx, "titip: redis: purge tag complete",
			slog.String("tag", tag),
			slog.Bool("soft", soft),
			slog.Int("processed_count", totalProcessed),
		)
	}

	return int64(totalProcessed), nil
}

// DeletePattern deletes all keys matching the given glob pattern within the storage namespace.
// If the pattern does not explicitly specify a keyspace prefix ("meta:", "body:", "tag:"),
// it automatically deletes both matching metadata Hashes and variant body payload keys.
// Uses SCAN + UNLINK for non-blocking, incremental deletion.
// Returns the count of matching primary metadata entries deleted.
func (s *RedisStorage) DeletePattern(ctx context.Context, pattern string) (int64, error) {
	var patterns []string
	isExplicitPrefix := strings.HasPrefix(pattern, "meta:") || strings.HasPrefix(pattern, "body:") || strings.HasPrefix(pattern, "tag:") || pattern == "*"
	if isExplicitPrefix {
		patterns = []string{s.prefix + pattern}
	} else {
		patterns = []string{
			s.prefix + "meta:" + pattern,
			s.prefix + "body:" + pattern + "*",
		}
	}

	deleted := 0
	primaryDeleted := 0
	for _, fullPattern := range patterns {
		isMetaPattern := strings.HasPrefix(fullPattern, s.prefix+"meta:") || fullPattern == s.prefix+"*"
		cursor := uint64(0)
		for {
			scanCmd := s.client.B().Scan().
				Cursor(cursor).
				Match(fullPattern).
				Count(scanBatchSize).
				Build()
			scanResp := s.client.Do(ctx, scanCmd)
			if err := scanResp.Error(); err != nil {
				return 0, fmt.Errorf("titip: redis: delete pattern scan: %w", err)
			}

			scanEntry, err := scanResp.AsScanEntry()
			if err != nil {
				return 0, fmt.Errorf("titip: redis: delete pattern scan entry: %w", err)
			}

			if len(scanEntry.Elements) > 0 {
				unlinkCmd := s.client.B().Unlink().Key(scanEntry.Elements...).Build()
				if err := s.client.Do(ctx, unlinkCmd).Error(); err != nil && !rueidis.IsRedisNil(err) {
					return 0, fmt.Errorf("titip: redis: delete pattern unlink: %w", err)
				}
				deleted += len(scanEntry.Elements)
				if isMetaPattern {
					for _, k := range scanEntry.Elements {
						if strings.HasPrefix(k, s.prefix+"meta:") {
							primaryDeleted++
						}
					}
				}
			}

			cursor = scanEntry.Cursor
			if cursor == 0 {
				break
			}
		}
	}

	if s.logger != nil && s.logger.Enabled(ctx, slog.LevelDebug) {
		s.logger.DebugContext(ctx, "titip: redis: delete pattern complete",
			slog.String("pattern", pattern),
			slog.Int("deleted_count", deleted),
			slog.Int("primary_deleted_count", primaryDeleted),
		)
	}

	return int64(primaryDeleted), nil
}

// PurgeAll deletes every key in the configured storage namespace prefix.
// Only keys matching the prefix are affected; all other Redis keys are preserved.
// Uses SCAN + UNLINK for non-blocking, incremental deletion.
// Returns the number of primary metadata entries deleted.
func (s *RedisStorage) PurgeAll(ctx context.Context) (int64, error) {
	fullPattern := s.prefix + "*"

	deleted := 0
	primaryDeleted := 0
	cursor := uint64(0)
	for {
		scanCmd := s.client.B().Scan().
			Cursor(cursor).
			Match(fullPattern).
			Count(scanBatchSize).
			Build()
		scanResp := s.client.Do(ctx, scanCmd)
		if err := scanResp.Error(); err != nil {
			return 0, fmt.Errorf("titip: redis: purge all scan: %w", err)
		}

		scanEntry, err := scanResp.AsScanEntry()
		if err != nil {
			return 0, fmt.Errorf("titip: redis: purge all scan entry: %w", err)
		}

		if len(scanEntry.Elements) > 0 {
			unlinkCmd := s.client.B().Unlink().Key(scanEntry.Elements...).Build()
			if err := s.client.Do(ctx, unlinkCmd).Error(); err != nil && !rueidis.IsRedisNil(err) {
				return 0, fmt.Errorf("titip: redis: purge all unlink: %w", err)
			}
			deleted += len(scanEntry.Elements)
			for _, k := range scanEntry.Elements {
				if strings.HasPrefix(k, s.prefix+"meta:") {
					primaryDeleted++
				}
			}
		}

		cursor = scanEntry.Cursor
		if cursor == 0 {
			break
		}
	}

	if s.logger != nil && s.logger.Enabled(ctx, slog.LevelDebug) {
		s.logger.DebugContext(ctx, "titip: redis: purge all complete",
			slog.String("prefix", s.prefix),
			slog.Int("deleted_count", deleted),
			slog.Int("primary_deleted_count", primaryDeleted),
		)
	}

	return int64(primaryDeleted), nil
}

// SoftPurgePattern scans all metadata keys matching the given glob pattern (scoped to the storage prefix)
// and marks each matching primary metadata entry as soft-purged (IsSoftPurged = true).
// Stale body payloads are preserved for stale-if-error fallback.
// Returns the number of primary metadata entries soft-purged.
func (s *RedisStorage) SoftPurgePattern(ctx context.Context, pattern string) (int64, error) {
	fullPattern := s.prefix + "meta:" + strings.TrimPrefix(pattern, "meta:")

	count := 0
	cursor := uint64(0)
	for {
		scanCmd := s.client.B().Scan().
			Cursor(cursor).
			Match(fullPattern).
			Count(scanBatchSize).
			Build()
		scanResp := s.client.Do(ctx, scanCmd)
		if err := scanResp.Error(); err != nil {
			return 0, fmt.Errorf("titip: redis: soft purge pattern scan: %w", err)
		}

		scanEntry, err := scanResp.AsScanEntry()
		if err != nil {
			return 0, fmt.Errorf("titip: redis: soft purge pattern scan entry: %w", err)
		}

		for _, key := range scanEntry.Elements {
			// Strip the storage prefix and "meta:" to recover the primary key.
			primaryKey := strings.TrimPrefix(key, s.prefix+"meta:")
			if _, err := s.SoftPurge(ctx, primaryKey); err != nil {
				// Non-fatal: log and continue to avoid partial failures.
				if s.logger != nil {
					s.logger.ErrorContext(ctx, "titip: redis: soft purge pattern: failed to soft-purge key",
						slog.String("key", key),
						slog.Any("error", err),
					)
				}
			}
			count++
		}

		cursor = scanEntry.Cursor
		if cursor == 0 {
			break
		}
	}

	if s.logger != nil && s.logger.Enabled(ctx, slog.LevelDebug) {
		s.logger.DebugContext(ctx, "titip: redis: soft purge pattern complete",
			slog.String("pattern", fullPattern),
			slog.Int("soft_purged_count", count),
		)
	}

	return int64(count), nil
}

// Close terminates Redis connections cleanly.
func (s *RedisStorage) Close() error {
	s.client.Close()
	return nil
}
