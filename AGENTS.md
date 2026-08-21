# AGENTS.md — AI Agent Guidelines & Operating Manual

> **Repository**: `github.com/indragunawan/titip`  
> **Mission**: Build a high-performance, low-allocation, RFC-7234 & RFC-9211 compliant HTTP caching middleware in Go.

---

## 1. Core Operating Principles

### What AI Agent MUST ALWAYS Do (Required Behaviors)
1. **Adhere to the Master PRD and Feature PRDs**:
   - Always refer to [PRD.md](file:///Users/indra/code/project/titip/PRD.md) as the single source of truth for architecture and specifications.
   - Follow the phase-by-phase execution roadmap in [`docs/ways-of-work/plan/titip-v1/`](file:///Users/indra/code/project/titip/docs/ways-of-work/plan/titip-v1/).
2. **Practice Test-Driven Development (TDD) & Zero-Race Concurrency**:
   - Write automated unit, concurrency, and race condition tests for every feature before declaring it complete.
   - Run tests with continuous race detection: `go test -race -count=100 ./...`.
3. **Enforce Zero-Allocation Standards on Hot Paths**:
   - Use `sync.Pool` for all byte buffers, response recorders, and LZ4 compressor/decompressor instances.
   - Verify hot hit paths with `testing.B` benchmarks (`0 allocs/op`).
4. **Implement Fail-Open Architecture**:
   - Storage outages (Redis down/timeout), Protobuf deserialization errors, or decompression failures **must never crash or return 500 errors to end users**.
   - Transparently forward requests directly to the origin handler (`fwd=bypass`).
5. **Enforce Context Detachment in Singleflight**:
   - Wrap singleflight origin fetches in `context.WithoutCancel(r.Context())` so client cancellations do not abort in-flight origin calls for waiting concurrent requests.
6. **Maintain Complete Key Cleanup on Hard Purges**:
   - Hard purges (URL and Tag) must atomically delete both the metadata Hash AND all associated variant body keys. Zero orphaned keys may remain in Redis.
7. **Maintain Multi-Module Workspace (`go.work`) Integration**:
   - Whenever a new module is added to the repository (e.g. framework adapters under `adapter/*`, storage drivers under `storage/*`, or applications under `examples/*`), it **MUST** declare its own `go.mod` and be registered in the root `go.work` file.
   - Always run `go work sync` to maintain clean workspace dependency resolution.

---

### What AI Agent MUST NEVER Do (Strict Prohibitions)
1. **NEVER Run Per-Request Hashing on URLs**:
   - Do **NOT** use SHA-256, MD5, Murmur3, or xxHash for cache keys. Assemble normalized strings directly via pooled builders via pooled string builders.
2. **NEVER Perform Read-Modify-Write in Redis for Variants**:
   - Do **NOT** fetch the entire metadata record, unmarshal, append a variant in Go, and re-write to Redis. Use atomic Redis Hash commands (`HSET`, `HMGET`) to eliminate race conditions.
3. **NEVER Cache Heuristically Without `Cache-Control`**:
   - Responses lacking explicit cacheable `Cache-Control` directives (`max-age`, `s-maxage`, `public`) must **never** be cached.
4. **NEVER Leak Sensitive Sessions (`Set-Cookie` / `private`)**:
   - Any origin response containing `Set-Cookie` or `Cache-Control: private` must bypass caching unconditionally.
5. **NEVER Bleed Dependencies Across Subpackages**:
   - Do **NOT** import `github.com/go-chi/chi/v5` into core `titip` or `storage/`.
   - Do **NOT** import `github.com/redis/rueidis` into core `titip` or `adapter/`.
   - Keep core `titip` dependency-free (except Protobuf and LZ4).
6. **NEVER Leave Goroutine Leaks on Revalidation or Shutdown**:
   - All asynchronous `stale-while-revalidate` goroutines must be tracked via `sync.WaitGroup` and awaited cleanly during `titip.Close(ctx)`.

---

## 2. Architecture & Design Constraints

| Component | Strict Rules |
|---|---|
| **Storage Engine** | Decoupled via `storage.Storage` interface. Redis is the sole first-class v1.0 storage (`github.com/redis/rueidis`). |
| **Serialization** | Compact Protobuf schema (`CacheMetadata`, `VariantInfo`) + LZ4 compression (`github.com/pierrec/lz4/v4`). |
| **Cache Key** | Configurable via `KeyConfig` (All, Whitelist, Blacklist, Exclude All query parameters). Zero-hash direct assembly. |
| **Origin Age Handling** | RFC-7234 Section 4.2.3 algorithm (apparent age, corrected initial age, resident time, effective TTL). Max TTL clamped to 1 year. |
| **Cache Status Headers** | RFC-9211 structured field (`Cache-Status: titip; hit; ...`), Simple Token (`HIT`, `MISS`), or Disabled. |
| **Status Codes** | Standard cacheable status codes set (`200, 203, 204, 206, 300, 301, 302, 307, 308, 400, 403, 404, 405, 410, 451, 500, 501, 502, 503, 504`) when origin has `Cache-Control`. |
| **Tag Headers** | Auto-detect `Cache-Tag` and `Surrogate-Key`, customizable via `WithTagHeaderName()`. |

---

## 3. Definition of Done ("What Complete Means")

A feature or task is **COMPLETE** if and only if all of the following conditions are met:

- [ ] **100% Functional Compliance**: All acceptance criteria defined in the specific Feature PRD are satisfied.
- [ ] **Automated Test Suite**:
  - Unit tests covering all branches, error paths, and edge cases.
  - Concurrency & stampede tests validating singleflight coalescing and soft-purge freshness.
- [ ] **Race Detector Cleanliness**:
  ```bash
  go test -race -count=100 -parallel=8 ./...
  ```
  Must pass with **0 data races** and **0 goroutine leaks**.
- [ ] **Zero-Allocation Benchmarks**:
  ```bash
  go test -benchmem -bench=BenchmarkCacheHit ./...
  ```
  Confirms `0 B/op` and `0 allocs/op` on cached hit responses.
- [ ] **Go Idioms & Code Quality**:
  - `go vet ./...` and `golangci-lint` pass with zero warnings.
  - Clean error wrapping (`fmt.Errorf("titip: ...: %w", err)`).
  - All public types, functions, and options have clear GoDoc comments.

---

## 4. Execution Workflow

1. **Pick the Next Phase**: Read the target feature PRD in `docs/ways-of-work/plan/titip-v1/`.
2. **Create Implementation Plan**: Scaffold interfaces and plan changes.
3. **Implement Incrementally (TDD)**:
   - Scaffold structs and interfaces.
   - Implement logic with memory pool reuse.
   - Write tests and run with `-race`.
4. **Benchmark**: Run `testing.B` to verify allocation constraints.
5. **Verify & Document**: Run race detector and verify zero regressions.

---

## 5. Memory Pool & Zero-Allocation Safety Rules

1. **Strict Pool Return Discipline**:
   - Always pair `GetBuffer()` / `GetResponseRecorder()` with an immediate `defer PutBuffer(buf)` / `defer PutResponseRecorder(rec)`.
2. **Zero Slice Retention After Put**:
   - **NEVER** hold references or slice pointers to a pooled buffer's underlying byte array after `PutBuffer` has been called. If bytes must outlive the request, allocate an explicit copy (`bytes.Clone(b)`).
3. **Buffer Growth Protection**:
   - In `PutBuffer(b)`, if `b.Cap() > 2*1024*1024` ($2\text{ MB}$), discard the buffer rather than returning it to the pool to protect against permanent heap retention from abnormally large single responses.
4. **Protobuf Instance Reuse**:
   - Always call `proto.Reset(msg)` when recycling Protobuf structs back into pools.

---

## 6. Error Handling, Logging & Panic Recovery Standards

1. **Structured Error Wrapping**:
   - Prefix all internal errors consistently: `fmt.Errorf("titip: <subsystem>: <operation>: %w", err)` (e.g. `fmt.Errorf("titip: storage: get metadata: %w", err)`).
2. **Zero `fmt.Print` / Standard `log` in Library Code**:
   - Never use `fmt.Println` or the standard `log` package in core code.
   - Always log through the configured `*slog.Logger` with structured key-value attributes (`slog.String("key", key)`, `slog.Duration("dur", elapsed)`).
3. **Fail-Open Fallback Logging**:
   - When catching storage or unmarshal errors, log at `slog.LevelError` or `slog.LevelWarn` and transparently forward to the origin handler (`fwd=bypass`).
4. **Panic Recovery Protocol**:
   - Every middleware handler MUST include panic recovery:
     ```go
     defer func() {
         if r := recover(); r != nil {
             // 1. Log stack trace with slog.Error
             // 2. Fallback to stale cache if stale-if-error is active
             // 3. Otherwise write 500 Internal Server Error without crashing process
         }
     }()
     ```

---

## 7. Tooling & Development Environment

- **Go Compiler**: Go `1.22+` (utilizes `context.WithoutCancel`, `net/http` enhanced routing).
- **Go Multi-Module Workspace**: Managed via root `go.work`. All submodules (`adapter/*`, `storage/*`, `examples/*`) must be registered in `go.work` so cross-module imports resolve locally without manual `replace` directives.
- **Protobuf Generation**: `protoc-gen-go` / `buf` targeting `google.golang.org/protobuf`.
- **Unit Testing Redis Mock**: Use `github.com/alicebob/miniredis/v2` for self-contained, high-speed unit tests without external Redis dependencies.
- **Integration Redis Testing**: Real Redis 7+ instance using `github.com/redis/rueidis`.

---

## 8. Git Commit & Documentation Conventions

- **Conventional Commits**:
  - `feat(pool)`: New feature or pool enhancement.
  - `fix(fsm)`: Bug fix in state machine logic.
  - `test(redis)`: Concurrency, race, or unit tests.
  - `bench(keygen)`: Performance and allocation benchmarks.
  - `docs(prd)`: Documentation or PRD updates.
- **No Binary / Temporary Artifacts**:
  - Never commit `.DS_Store`, generated test binaries, or scratch files.

