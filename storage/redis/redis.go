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
	defaultPrefix   = "titip:"
	indexField      = "_index"
	softPurgedField = "_soft_purged"
	variantPrefix   = "v:"

	// scanBatchSize controls how many keys are requested per SCAN iteration.
	scanBatchSize = 100
)

var softPurgeScript = rueidis.NewLuaScript(`
if redis.call('HEXISTS', KEYS[1], '_index') == 1 then
    redis.call('HSET', KEYS[1], '_soft_purged', '1')
    return 1
else
    return 0
end
`)

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
	_ storage.Storage       = (*RedisStorage)(nil)
	_ storage.PatternPurger = (*RedisStorage)(nil)
	_ storage.AllPurger     = (*RedisStorage)(nil)
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

// variantField returns the Redis Hash field name for a variant key.
func (s *RedisStorage) variantField(variantKey string) string {
	return variantPrefix + variantKey
}

// GetMeta retrieves the primary index, Vary headers, and soft-purged status for a primary URL key.
func (s *RedisStorage) GetMeta(ctx context.Context, primaryKey string) (*pb.CacheMetadata, bool, error) {
	cmd := s.client.B().Hgetall().Key(s.metaKey(primaryKey)).Build()
	resp := s.client.Do(ctx, cmd)
	if rueidis.IsRedisNil(resp.Error()) {
		return nil, false, nil
	}
	if err := resp.Error(); err != nil {
		return nil, false, fmt.Errorf("titip: redis: get meta: %w", err)
	}

	m, err := resp.AsStrMap()
	if err != nil || len(m) == 0 {
		return nil, false, nil
	}

	indexStr, ok := m[indexField]
	if !ok {
		return nil, false, nil
	}

	meta := &pb.CacheMetadata{}
	if err := googleproto.Unmarshal([]byte(indexStr), meta); err != nil {
		return nil, false, fmt.Errorf("titip: redis: unmarshal meta index: %w", err)
	}

	isSoftPurged := (m[softPurgedField] == "1")

	meta.Variants = make(map[string]*pb.VariantInfo)
	for k, v := range m {
		if strings.HasPrefix(k, variantPrefix) {
			rawVarKey := strings.TrimPrefix(k, variantPrefix)
			varInfo := &pb.VariantInfo{}
			if err := googleproto.Unmarshal([]byte(v), varInfo); err == nil {
				meta.Variants[rawVarKey] = varInfo
			}
		}
	}

	return meta, isSoftPurged, nil
}

// GetVariant retrieves a specific variant metadata and its compressed body payload in a single pipelined roundtrip.
func (s *RedisStorage) GetVariant(ctx context.Context, primaryKey, variantKey string) (*pb.VariantInfo, []byte, error) {
	if variantKey == "" {
		variantKey = "default"
	}
	cmd1 := s.client.B().Hget().Key(s.metaKey(primaryKey)).Field(s.variantField(variantKey)).Build()
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
		PrimaryKey:                 meta.PrimaryKey,
		VaryHeaderNames:            meta.VaryHeaderNames,
		CreatedAtUnixNano:          meta.CreatedAtUnixNano,
		ExpiresAtUnixNano:          meta.ExpiresAtUnixNano,
		StaleUntilUnixNano:         meta.StaleUntilUnixNano,
		CorrectedInitialAgeSeconds: meta.CorrectedInitialAgeSeconds,
		Tags:                       meta.Tags,
	}

	indexBytes, err := googleproto.Marshal(leanMeta)
	if err != nil {
		return fmt.Errorf("titip: redis: marshal meta index: %w", err)
	}

	varBytes, err := googleproto.Marshal(variant)
	if err != nil {
		return fmt.Errorf("titip: redis: marshal variant: %w", err)
	}

	ttlSeconds := max(int64(ttl.Seconds()), 1)

	cmds := make([]rueidis.Completed, 0, 6+len(meta.Tags))

	// 1. HSET metaKey _index <metaBytes> v:<variantKey> <varBytes>
	hsetCmd := s.client.B().Hset().
		Key(s.metaKey(primaryKey)).
		FieldValue().
		FieldValue(indexField, rueidis.BinaryString(indexBytes)).
		FieldValue(s.variantField(variant.VariantKey), rueidis.BinaryString(varBytes)).
		Build()
	cmds = append(cmds, hsetCmd)

	// 2. HDEL metaKey _soft_purged (resets soft purge on fresh content)
	hdelSoftPurgeCmd := s.client.B().Hdel().
		Key(s.metaKey(primaryKey)).
		Field(softPurgedField).
		Build()
	cmds = append(cmds, hdelSoftPurgeCmd)

	// 3. SET bodyKey <body> EX <ttl>
	setBodyCmd := s.client.B().Set().
		Key(s.bodyKey(primaryKey, variant.VariantKey)).
		Value(rueidis.BinaryString(body)).
		ExSeconds(ttlSeconds).
		Build()
	cmds = append(cmds, setBodyCmd)

	// 4. Dynamic TTL: Set initial TTL if none exists (NX), or extend if new TTL is greater (GT)
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

// Purge invalidates the primary metadata and its associated variant body keys.
// If soft is true, marks the entry as stale for fallback while preserving payload data.
// If soft is false (hard purge), physically deletes the metadata and all variant body keys.
// Returns 1 if the primary entry existed and was purged, or 0 if not found.
func (s *RedisStorage) Purge(ctx context.Context, primaryKey string, soft bool) (int64, error) {
	if soft {
		return s.softPurge(ctx, primaryKey)
	}
	return s.hardPurge(ctx, primaryKey)
}

func (s *RedisStorage) hardPurge(ctx context.Context, primaryKey string) (int64, error) {
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

	delKeys := make([]string, 0, len(keys)+1)
	delKeys = append(delKeys, metaKey)
	for _, k := range keys {
		if strings.HasPrefix(k, variantPrefix) {
			varKey := strings.TrimPrefix(k, variantPrefix)
			delKeys = append(delKeys, s.bodyKey(primaryKey, varKey))
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

func (s *RedisStorage) softPurge(ctx context.Context, primaryKey string) (int64, error) {
	metaKey := s.metaKey(primaryKey)
	res, err := softPurgeScript.Exec(ctx, s.client, []string{metaKey}, nil).AsInt64()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("titip: redis: soft purge: %w", err)
	}

	if s.logger != nil && s.logger.Enabled(ctx, slog.LevelDebug) && res > 0 {
		s.logger.DebugContext(ctx, "titip: redis: soft-purged cache entry",
			slog.String("primary_key", primaryKey),
			slog.String("meta_key", metaKey),
		)
	}

	return res, nil
}

// PurgeByTag invalidates all primary metadata hashes and variant body keys matching a given tag.
// Uses non-blocking SSCAN streaming to handle sets of arbitrary size without blocking Redis.
// The tag is treated as a literal string — use PurgeAll for namespace wipeout.
// Returns the number of primary entries invalidated.
func (s *RedisStorage) PurgeByTag(ctx context.Context, tag string, soft bool) (int64, error) {
	tagKey := s.tagKey(tag)
	cursor := uint64(0)
	processed := 0

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
					if _, err := s.softPurge(ctx, pk); err != nil {
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
					if rueidis.IsRedisNil(r.Error()) {
						continue
					}
					if err := r.Error(); err != nil {
						return 0, fmt.Errorf("titip: redis: purge tag fetch keys: %w", err)
					}
					varKeys, err := r.AsStrSlice()
					if err != nil {
						return 0, fmt.Errorf("titip: redis: purge tag keys parse: %w", err)
					}
					pk := primaryKeys[i]
					delKeys = append(delKeys, s.metaKey(pk))
					for _, k := range varKeys {
						if strings.HasPrefix(k, variantPrefix) {
							varKey := strings.TrimPrefix(k, variantPrefix)
							delKeys = append(delKeys, s.bodyKey(pk, varKey))
						}
					}
				}

				if len(delKeys) > 0 {
					delCmds := make([]rueidis.Completed, len(delKeys))
					for i, k := range delKeys {
						delCmds[i] = s.client.B().Del().Key(k).Build()
					}
					delResps := s.client.DoMulti(ctx, delCmds...)
					for _, r := range delResps {
						if err := r.Error(); err != nil && !rueidis.IsRedisNil(err) {
							return 0, fmt.Errorf("titip: redis: purge tag execute: %w", err)
						}
					}
				}
			}
			processed += len(primaryKeys)
		}

		cursor = scanEntry.Cursor
		if cursor == 0 {
			break
		}
	}

	// For hard purge, remove the tag set itself
	if !soft {
		_ = s.client.Do(ctx, s.client.B().Unlink().Key(tagKey).Build())
	}

	if s.logger != nil && s.logger.Enabled(ctx, slog.LevelDebug) {
		s.logger.DebugContext(ctx, "titip: redis: purge tag complete",
			slog.String("tag", tag),
			slog.Bool("soft", soft),
			slog.Int("processed_count", processed),
		)
	}

	return int64(processed), nil
}

func (s *RedisStorage) scanAndUnlink(ctx context.Context, matchPattern string) (deleted int, primaryDeleted int, err error) {
	cursor := uint64(0)
	for {
		scanCmd := s.client.B().Scan().Cursor(cursor).Match(matchPattern).Count(scanBatchSize).Build()
		resp := s.client.Do(ctx, scanCmd)
		if err := resp.Error(); err != nil {
			return deleted, primaryDeleted, fmt.Errorf("titip: redis: scan: %w", err)
		}
		entry, err := resp.AsScanEntry()
		if err != nil {
			return deleted, primaryDeleted, fmt.Errorf("titip: redis: scan entry: %w", err)
		}
		if len(entry.Elements) > 0 {
			unlinkCmd := s.client.B().Unlink().Key(entry.Elements...).Build()
			if err := s.client.Do(ctx, unlinkCmd).Error(); err != nil && !rueidis.IsRedisNil(err) {
				return deleted, primaryDeleted, fmt.Errorf("titip: redis: unlink: %w", err)
			}
			deleted += len(entry.Elements)
			for _, key := range entry.Elements {
				if strings.Contains(key, ":meta:") {
					primaryDeleted++
				}
			}
		}
		cursor = entry.Cursor
		if cursor == 0 {
			break
		}
	}
	return
}

// PurgeByPattern invalidates all keys matching pattern. If soft is true, marks each soft-purged. If false, physically deletes.
func (s *RedisStorage) PurgeByPattern(ctx context.Context, pattern string, soft bool) (int64, error) {
	if soft {
		return s.softPurgePattern(ctx, pattern)
	}
	return s.hardPurgePattern(ctx, pattern)
}

func (s *RedisStorage) hardPurgePattern(ctx context.Context, pattern string) (int64, error) {
	var patterns []string
	isExplicitPrefix := strings.HasPrefix(pattern, "meta:") || strings.HasPrefix(pattern, "body:") || strings.HasPrefix(pattern, "tag:") || pattern == "*"
	if isExplicitPrefix {
		patterns = []string{s.prefix + pattern}
	} else {
		patterns = []string{s.prefix + "meta:" + pattern, s.prefix + "body:" + pattern + "*"}
	}
	deleted, primaryDeleted := 0, 0
	for _, p := range patterns {
		d, pd, err := s.scanAndUnlink(ctx, p)
		if err != nil {
			return 0, err
		}
		deleted += d
		primaryDeleted += pd
	}
	if s.logger != nil && s.logger.Enabled(ctx, slog.LevelDebug) {
		s.logger.DebugContext(ctx, "titip: redis: delete pattern complete", slog.String("pattern", pattern), slog.Int("deleted_count", deleted), slog.Int("primary_deleted_count", primaryDeleted))
	}
	return int64(primaryDeleted), nil
}

func (s *RedisStorage) softPurgePattern(ctx context.Context, pattern string) (int64, error) {
	fullPattern := s.prefix + "meta:" + strings.TrimPrefix(pattern, "meta:")
	count := 0
	cursor := uint64(0)
	for {
		scanCmd := s.client.B().Scan().Cursor(cursor).Match(fullPattern).Count(scanBatchSize).Build()
		resp := s.client.Do(ctx, scanCmd)
		if err := resp.Error(); err != nil {
			return 0, fmt.Errorf("titip: redis: scan: %w", err)
		}
		entry, err := resp.AsScanEntry()
		if err != nil {
			return 0, fmt.Errorf("titip: redis: scan entry: %w", err)
		}
		for _, key := range entry.Elements {
			primaryKey := strings.TrimPrefix(key, s.prefix+"meta:")
			if _, e := s.softPurge(ctx, primaryKey); e != nil && s.logger != nil {
				s.logger.ErrorContext(ctx, "titip: redis: soft purge pattern: failed to soft-purge key", slog.String("key", key), slog.Any("error", e))
			}
			count++
		}
		cursor = entry.Cursor
		if cursor == 0 {
			break
		}
	}
	if s.logger != nil && s.logger.Enabled(ctx, slog.LevelDebug) {
		s.logger.DebugContext(ctx, "titip: redis: soft purge pattern complete", slog.String("pattern", fullPattern), slog.Int("soft_purged_count", count))
	}
	return int64(count), nil
}

// PurgeAll deletes every key in the configured storage namespace prefix.
func (s *RedisStorage) PurgeAll(ctx context.Context) (int64, error) {
	deleted, primaryDeleted, err := s.scanAndUnlink(ctx, s.prefix+"*")
	if err != nil {
		return 0, err
	}
	if s.logger != nil && s.logger.Enabled(ctx, slog.LevelDebug) {
		s.logger.DebugContext(ctx, "titip: redis: purge all complete", slog.String("prefix", s.prefix), slog.Int("deleted_count", deleted), slog.Int("primary_deleted_count", primaryDeleted))
	}
	return int64(primaryDeleted), nil
}

// Close terminates Redis connections cleanly.
func (s *RedisStorage) Close() error {
	s.client.Close()
	return nil
}
