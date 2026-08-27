.PHONY: test bench race vet redis-up redis-down help

help: ## Show help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

test: ## Run unit tests across workspace
	go test -v ./... ./adapter/caddy/... ./storage/redis/...

race: ## Run race detection and concurrency tests
	go test -race -count=100 -parallel=8 ./...

bench: ## Run modernized b.Loop() memory allocation benchmarks
	go test -benchmem -bench=. -run=^$$ ./... ./storage/redis/...

vet: ## Run Go static analysis across workspace
	go vet ./... ./adapter/caddy/... ./storage/redis/...
	go work sync
