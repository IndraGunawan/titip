.PHONY: test bench race vet redis-up redis-down flamegraph flamegraph-mem help

BENCH ?= BenchmarkMiddleware_ParallelThroughput
BENCHTIME ?= 5s
PORT ?= 8080

help: ## Show help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

test: ## Run unit tests across workspace
	go test -v ./... ./adapter/caddy/... ./storage/redis/...

race: ## Run race detection and concurrency tests
	go test -race -count=100 -parallel=8 ./...

bench: ## Run modernized b.Loop() memory allocation benchmarks
	go test -benchmem -bench=. -run=^$$ ./... ./storage/redis/...

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
	go vet ./... ./adapter/caddy/... ./storage/redis/...
	go work sync

