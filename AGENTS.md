# AGENTS.md — AI Agent Guidelines & Operating Manual

> **Repository**: `github.com/indragunawan/titip`
> **Mission**: Build a high-performance, low-allocation, RFC-7234 & RFC-9211 compliant HTTP caching middleware in Go.

---

## Critical Workflow Rules

**MANDATORY — Always Follow This Order:**

1. ✅ **Read Before Edit** — Always inspect target files, surrounding context, and interfaces before modifying code.
2. ✅ **Use Correct Build & Test Commands** — See [Quick Command Reference](#quick-command-reference) below.
3. ✅ **Test After Changes** — Run targeted tests with race detection immediately after editing:
   - Specific package: `go test -v -race ./<pkg>/...`
   - Workspace-wide: `make test` or `make race`
4. ✅ **Format & Lint Code** — Run `make fmt`, `make vet`, and `make lint` before finishing any task.
5. ✅ **Follow Architecture & RFCs** — Strict zero-allocation pools, fail-open resilience, atomic Redis Hash operations, and RFC compliance. See [Core Operating Principles](#1-core-operating-principles).
6. ✅ **Never Push to Main** — Always create a dedicated feature or bugfix branch and open a PR. Never run `git push origin main`.

---

## Quick Command Reference

| Action | Command | Scope |
| :--- | :--- | :--- |
| **Format Code** | `make fmt` | Formats all workspace modules (`gofmt -s -w .`) and syncs workspace |
| **Static Analysis** | `make vet` | Static analysis across workspace (`go vet`) and syncs workspace |
| **Linter** | `make lint` | `golangci-lint run` across all workspace modules |
| **Code Cleanup** | `make fix` | `go fix` across all workspace modules |
| **Unit Tests** | `make test` | `go test -v` across all workspace modules |
| **Race Detector** | `make race` | `go test -race -count=100 -parallel=8` across all modules |
| **Memory Benchmarks** | `make bench` | Memory allocation benchmarks (`go test -benchmem -bench=.`) |
| **Targeted Test** | `go test -v -race ./<pkg>/...` | Fast feedback for the specific module under test |
| **Targeted Bench** | `go test -benchmem -bench=. ./<pkg>/...` | Alloc/op validation for specific module |
| **Workspace Sync** | `go work sync` | Re-sync multi-module dependency resolution |

---

## 1. Core Operating Principles

### What AI Agent MUST ALWAYS Do (Required Behaviors)

1. **Adhere to Master Specifications & Architecture**:
   - Always refer to [README.md](README.md) and standard RFC specifications (RFC 7234, RFC 9111, RFC 9211) as the source of truth for architecture and specifications.
2. **Practice Test-Driven Development (TDD) & Zero-Race Concurrency**:
   - Write automated unit, concurrency, and race condition tests for every feature before declaring it complete.
   - Run tests with continuous race detection: `go test -race -count=100 ./...`.
3. **Enforce Low-Allocation Standards via Memory Pools**:
   - Use `sync.Pool` for all byte buffers, response recorders, LZ4 compressor/decompressor instances, and request contexts to eliminate payload heap churn.
   - Verify buffer and reader recycling with `testing.B` benchmarks.
4. **Implement Fail-Open Architecture**:
   - Storage outages (Redis down/timeout), Protobuf deserialization errors, or decompression failures **must never crash or return 500 errors to end users**.
   - Transparently forward requests directly to the origin handler (`fwd=bypass`).
5. **Enforce Context Detachment in Singleflight**:
   - Wrap singleflight origin fetches in `context.WithoutCancel(r.Context())` so client cancellations do not abort in-flight origin calls for waiting concurrent requests.
6. **Maintain Complete Key Cleanup on Hard Purges**:
   - Hard purges (URL and Tag) must atomically delete both the metadata Hash AND all associated variant body keys. Zero orphaned keys may remain in Redis.
7. **Maintain Multi-Module Workspace (`go.work`) Integration**:
   - Whenever a new distributed module is added to the repository (e.g. framework adapters under `adapter/*` or storage drivers under `storage/*`), it **MUST** declare its own `go.mod` and be registered in the root `go.work` file.
   - Always run `go work sync` to maintain clean workspace dependency resolution.

---

### What AI Agent MUST NEVER Do (Strict Prohibitions)

1. **NEVER Run Per-Request Hashing on URLs**:
   - Do **NOT** use SHA-256, MD5, Murmur3, or xxHash for cache keys. Assemble normalized strings directly via pooled string builders.
2. **NEVER Perform Read-Modify-Write in Redis for Variants**:
   - Do **NOT** fetch the entire metadata record, unmarshal, append a variant in Go, and re-write to Redis. Use atomic Redis Hash commands (`HSET`, `HMGET`) to eliminate race conditions.
3. **NEVER Cache Heuristically Without `Cache-Control`**:
   - Responses lacking explicit cacheable `Cache-Control` directives (`max-age`, `s-maxage`, `public`) must **never** be cached.
4. **NEVER Leak Sensitive Sessions (`Set-Cookie` / `private`)**:
   - Any origin response containing `Set-Cookie` or `Cache-Control: private` must bypass caching unconditionally.
5. **NEVER Use Singleflight on Cold Misses (Data Leak Protection)**:
   - Do **NOT** coalesce concurrent requests on cold/unverified URLs with `singleflight`. Singleflight across concurrent cold requests can broadcast private headers (`Set-Cookie`) to unauthenticated callers.
   - Restrict `singleflight` exclusively to revalidating known cacheable entries (stale-while-revalidate and expired refresh).
6. **NEVER Bleed Dependencies Across Subpackages**:
   - Do **NOT** import third-party framework routers into core `titip` or `storage/` (keep framework adapters strictly isolated under `adapter/*`).
   - Do **NOT** import concrete storage clients (e.g. `github.com/redis/rueidis`) into core `titip` or `adapter/` (keep storage implementations strictly isolated under `storage/*`).
   - **Core Dependency Policy**: Prefer Go standard library and official `golang.org/x/*` packages. Proven, lightweight, low-transitive-dependency libraries (e.g. Protobuf, LZ4, Prometheus) are permitted when they prevent reinventing complex or error-prone wheels, but avoid heavy transitive dependency trees or importing libraries for trivial functions.
7. **NEVER Leave Goroutine Leaks on Revalidation or Shutdown**:
   - All asynchronous `stale-while-revalidate` goroutines must be tracked via `sync.WaitGroup` and awaited cleanly during `titip.Close(ctx)`.

---

## 2. Architecture & Design Constraints

| Component | Strict Rules |
| --- | --- |
| **Storage Engine** | Decoupled via `storage.Storage` interface. Redis is the sole first-class v1.0 storage (`github.com/redis/rueidis`). |
| **Serialization** | Compact Protobuf schema (`CacheMetadata`, `VariantInfo`) + LZ4 compression (`github.com/pierrec/lz4/v4`). |
| **Cache Key** | Configurable via `CacheKey` (All, Allowlist, Denylist, Exclude All query parameters). Zero-hash direct assembly. |
| **Origin Age Handling** | RFC 9111 / RFC-7234 Section 4.2.3 algorithm (apparent age, corrected initial age, resident time, effective TTL). Max TTL clamped to 1 year. |
| **Cache Status Headers** | RFC-9211 structured field (`Cache-Status: titip; hit; ...`), Simple Token (`HIT`, `MISS`), or Disabled. |
| **Status Codes** | Standard cacheable status codes (`200, 203, 204, 206, 300, 301, 302, 307, 308, 400, 403, 404, 405, 410, 451, 500, 501, 502, 503, 504`) when origin has `Cache-Control`. |
| **Tag Headers** | Default `Cache-Tag`, customizable via `WithTagHeader(name)`. |

---

## 3. Definition of Done ("What Complete Means")

A feature or task is **COMPLETE** if and only if all of the following conditions are met:

- [ ] **100% Functional Compliance**: All acceptance criteria and RFC specifications are satisfied.
- [ ] **Automated Test Suite**:
  - Unit tests covering all branches, error paths, and edge cases.
  - Concurrency & stampede tests validating singleflight coalescing and soft-purge freshness.
- [ ] **Race Detector Cleanliness**:

  ```bash
  go test -race -count=100 -parallel=8 ./...
  ```

  Must pass with **0 data races** and **0 goroutine leaks**.
- [ ] **Low-Allocation Benchmarks**:

  ```bash
  go test -benchmem -bench=. ./...
  ```

  Confirms memory pool buffer reuse and minimal allocations on critical paths.
- [ ] **Go Idioms & Code Quality**:
  - `go vet ./...` and `golangci-lint` pass with zero warnings.
  - Clean error wrapping (`fmt.Errorf("titip: ...: %w", err)`).
  - All public types, functions, and options have clear GoDoc comments.

---

## 4. Execution Workflow

1. **Understand Specifications**: Review targeted component specifications in `README.md` and relevant RFCs.
2. **Implement Incrementally (TDD)**:
   - Scaffold structs and interfaces.
   - Implement logic with memory pool reuse.
   - Write automated unit, concurrency, and race tests.
3. **Benchmark**: Run `testing.B` benchmarks to verify zero-allocation and performance constraints.
4. **Verify**: Run race detection (`go test -race ./...`) and lint checks (`go vet ./...`).

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

- **Go Compiler**: Compatible Go version as declared in [`go.mod`](go.mod) (utilizes `context.WithoutCancel`, `b.Loop()`, `net/http` enhanced routing).
- **Go Multi-Module Workspace**: Managed via root `go.work`. All submodules (`adapter/*`, `storage/*`) declare relative `replace` directives in `go.mod` and are registered in `go.work` so cross-module imports and `go mod tidy` resolve locally. Demo apps in `examples/*` are included in `go.work` solely for local integration testing.
- **Protobuf Generation**: `protoc-gen-go` / `buf` targeting `google.golang.org/protobuf`.
- **Redis Testing Environment**: Real Redis 7+ instance using `github.com/redis/rueidis` (`docker compose up -d` with `redis:8-alpine` or `redis:7-alpine`). Employs native `EXPIRE ... GT` and atomic hash operations with isolated test key prefixes.
- **Makefile Scoping Policy**:
  - The root `Makefile` is strictly reserved for core library workflows: `test`, `race`, `bench`, `vet`, `fmt`, `lint`, and `fix`.
  - **NEVER** add demo-specific or application-specific run commands to the root `Makefile`. Keep all demo and example lifecycle commands self-contained in their own subdirectories (e.g. `examples/caddy-demo/Makefile`).

---

## 8. Git Commit & Documentation Conventions

- **Conventional Commits**:
  - `feat`: New features (e.g. `feat: add Server-Timing diagnostics`).
  - `fix`: Bug fixes (e.g. `fix: resolve ESI quote parsing edge case`).
  - `test`: Concurrency, race, or unit tests (e.g. `test: add race condition test for dynamic TTL`).
  - `bench`: Performance and allocation benchmarks (e.g. `bench: add cache key benchmark`).
  - `chore`: Maintenance, dependencies, or tooling (e.g. `chore: update dependencies`).
  - `docs`: Documentation updates (e.g. `docs: update contributing guide`).
  - Do not use scopes (use plain `feat:`, `fix:`, etc.).
- **No Binary / Temporary Artifacts**:
  - Never commit `.DS_Store`, generated test binaries, or scratch files.
