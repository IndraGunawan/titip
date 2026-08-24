package redis

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/rueidis"
	googleproto "google.golang.org/protobuf/proto"

	pb "github.com/indragunawan/titip/proto"
	"github.com/indragunawan/titip/storage"
)

const (
	defaultPrefix = "titip:"
	indexField    = "_index"
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

// Ensure RedisStorage implements storage.Storage.
var _ storage.Storage = (*RedisStorage)(nil)

func (s *RedisStorage) metaKey(primaryKey string) string {
	return s.prefix + "meta:" + primaryKey
}

func (s *RedisStorage) bodyKey(primaryKey, variantKey string) string {
	return s.prefix + "body:" + primaryKey + ":" + variantKey
}

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
		PrimaryKey:        meta.PrimaryKey,
		VaryHeaderNames:   meta.VaryHeaderNames,
		CreatedAtUnixNano: meta.CreatedAtUnixNano,
		ExpiresAtUnixNano: meta.ExpiresAtUnixNano,
		StaleUntilUnixNano: meta.StaleUntilUnixNano,
		Tags:              meta.Tags,
		IsSoftPurged:      meta.IsSoftPurged,
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

	// 4. Index tags in Redis Sets
	for _, tag := range meta.Tags {
		if tag != "" {
			saddCmd := s.client.B().Sadd().
				Key(s.tagKey(tag)).
				Member(primaryKey).
				Build()
			cmds = append(cmds, saddCmd)
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
func (s *RedisStorage) Delete(ctx context.Context, primaryKey string) error {
	metaKey := s.metaKey(primaryKey)

	// Fetch all field names to know all variant body keys
	hkeysCmd := s.client.B().Hkeys().Key(metaKey).Build()
	resp := s.client.Do(ctx, hkeysCmd)
	if rueidis.IsRedisNil(resp.Error()) {
		return nil
	}
	if err := resp.Error(); err != nil {
		return fmt.Errorf("titip: redis: delete fetch keys: %w", err)
	}

	keys, err := resp.AsStrSlice()
	if err != nil || len(keys) == 0 {
		return nil
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
			return fmt.Errorf("titip: redis: delete execute: %w", err)
		}
	}

	if s.logger != nil && s.logger.Enabled(ctx, slog.LevelDebug) {
		s.logger.DebugContext(ctx, "titip: redis: purged cache keys",
			slog.String("primary_key", primaryKey),
			slog.Int("keys_count", len(delKeys)),
			slog.Any("keys", delKeys),
		)
	}

	return nil
}

// SoftPurge marks the primary metadata index as soft-purged while retaining body payloads.
func (s *RedisStorage) SoftPurge(ctx context.Context, primaryKey string) error {
	metaKey := s.metaKey(primaryKey)

	hgetCmd := s.client.B().Hget().Key(metaKey).Field(indexField).Build()
	resp := s.client.Do(ctx, hgetCmd)
	if rueidis.IsRedisNil(resp.Error()) {
		return nil
	}
	if err := resp.Error(); err != nil {
		return fmt.Errorf("titip: redis: soft purge get index: %w", err)
	}

	indexBytes, err := resp.AsBytes()
	if err != nil {
		return nil
	}

	meta := &pb.CacheMetadata{}
	if err := googleproto.Unmarshal(indexBytes, meta); err != nil {
		return fmt.Errorf("titip: redis: soft purge unmarshal: %w", err)
	}

	meta.IsSoftPurged = true
	updatedBytes, err := googleproto.Marshal(meta)
	if err != nil {
		return fmt.Errorf("titip: redis: soft purge marshal: %w", err)
	}

	hsetCmd := s.client.B().Hset().
		Key(metaKey).
		FieldValue().
		FieldValue(indexField, rueidis.BinaryString(updatedBytes)).
		Build()
	if err := s.client.Do(ctx, hsetCmd).Error(); err != nil {
		return fmt.Errorf("titip: redis: soft purge save: %w", err)
	}

	if s.logger != nil && s.logger.Enabled(ctx, slog.LevelDebug) {
		s.logger.DebugContext(ctx, "titip: redis: soft-purged cache entry",
			slog.String("primary_key", primaryKey),
			slog.String("meta_key", metaKey),
		)
	}

	return nil
}

// PurgeByTag invalidates all primary metadata hashes and variant body keys matching a given tag.
func (s *RedisStorage) PurgeByTag(ctx context.Context, tag string, soft bool) error {
	tagKey := s.tagKey(tag)

	smembersCmd := s.client.B().Smembers().Key(tagKey).Build()
	resp := s.client.Do(ctx, smembersCmd)
	if rueidis.IsRedisNil(resp.Error()) {
		return nil
	}
	if err := resp.Error(); err != nil {
		return fmt.Errorf("titip: redis: purge tag get members: %w", err)
	}

	primaryKeys, err := resp.AsStrSlice()
	if err != nil || len(primaryKeys) == 0 {
		if s.logger != nil && s.logger.Enabled(ctx, slog.LevelDebug) {
			s.logger.DebugContext(ctx, "titip: redis: purge tag found 0 keys", slog.String("tag", tag))
		}
		return nil
	}

	if soft {
		for _, pk := range primaryKeys {
			if err := s.SoftPurge(ctx, pk); err != nil {
				return err
			}
		}
		if s.logger != nil && s.logger.Enabled(ctx, slog.LevelDebug) {
			s.logger.DebugContext(ctx, "titip: redis: soft-purged entries by tag",
				slog.String("tag", tag),
				slog.Int("primary_keys_count", len(primaryKeys)),
				slog.Any("primary_keys", primaryKeys),
			)
		}
		return nil
	}

	// Hard purge: pipeline fetching variant keys for each primary key
	hkeysCmds := make([]rueidis.Completed, len(primaryKeys))
	for i, pk := range primaryKeys {
		hkeysCmds[i] = s.client.B().Hkeys().Key(s.metaKey(pk)).Build()
	}
	hkeysResps := s.client.DoMulti(ctx, hkeysCmds...)

	var delKeys []string
	delKeys = append(delKeys, tagKey)

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

	// Batch delete all metadata, body keys, and tag set in single pipelined DoMulti roundtrip
	delCmds := make([]rueidis.Completed, 0, len(delKeys))
	for _, k := range delKeys {
		delCmds = append(delCmds, s.client.B().Del().Key(k).Build())
	}
	delResps := s.client.DoMulti(ctx, delCmds...)
	for _, r := range delResps {
		if err := r.Error(); err != nil && !rueidis.IsRedisNil(err) {
			return fmt.Errorf("titip: redis: purge tag delete: %w", err)
		}
	}

	if s.logger != nil && s.logger.Enabled(ctx, slog.LevelDebug) {
		s.logger.DebugContext(ctx, "titip: redis: hard-purged entries by tag",
			slog.String("tag", tag),
			slog.Int("primary_keys_count", len(primaryKeys)),
			slog.Any("primary_keys", primaryKeys),
			slog.Int("deleted_keys_count", len(delKeys)),
			slog.Any("deleted_keys", delKeys),
		)
	}

	return nil
}

// Close terminates Redis connections cleanly.
func (s *RedisStorage) Close() error {
	s.client.Close()
	return nil
}
