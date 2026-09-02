.PHONY: test bench race vet lint redis-up redis-down flamegraph flamegraph-mem help

BENCH ?= BenchmarkMiddleware_ParallelThroughput
BENCHTIME ?= 5s
PORT ?= 8080

# Automatically discover all Go modules in workspace (excluding examples and hidden directories)
MODULE_PATHS := $(shell find . -name "go.mod" -not -path "*/examples/*" -not -path "*/.*" -exec dirname {} \; | sort)
MODULE_PKGS  := $(foreach dir,$(MODULE_PATHS),$(dir)/...)

help: ## Show help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

test: ## Run unit tests across all workspace modules
	go test -v $(MODULE_PKGS)

race: ## Run race detection and concurrency tests across all workspace modules
	go test -race -count=100 -parallel=8 $(MODULE_PKGS)

bench: ## Run modernized b.Loop() memory allocation benchmarks across workspace
	go test -benchmem -bench=. -run=^$$ $(MODULE_PKGS)

flamegraph: ## Profile CPU and launch interactive flamegraph (Usage: make flamegraph BENCH=BenchmarkCacheHit BENCHTIME=5s PORT=8080)
	go test -bench=$(BENCH) -run=^$$ -benchtime=$(BENCHTIME) -cpuprofile=cpu.prof .
	go tool pprof -http=:$(PORT) cpu.prof

flamegraph-mem: ## Profile memory allocations and launch interactive flamegraph (Usage: make flamegraph-mem BENCH=BenchmarkCacheHit PORT=8080)
	go test -bench=$(BENCH) -run=^$$ -benchtime=$(BENCHTIME) -memprofile=mem.prof .
	go tool pprof -alloc_space -http=:$(PORT) mem.prof

redis-up: ## Start Redis container
	docker compose up -d

redis-down: ## Stop Redis container
	docker compose down

vet: ## Run Go static analysis across workspace
	go vet $(MODULE_PKGS)
	go work sync
