.PHONY: test bench race vet fmt lint fix help

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

vet: ## Run Go static analysis across workspace
	go vet $(MODULE_PKGS)
	go work sync

fmt: ## Format and simplify all Go code across workspace
	gofmt -s -w .
	go work sync

lint: ## Run golangci-lint across workspace
	golangci-lint run $(MODULE_PKGS)

fix: ## Run go fix across all workspace modules
	go fix $(MODULE_PKGS)
