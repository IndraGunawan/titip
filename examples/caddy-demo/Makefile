.PHONY: run origin build-caddy redis-up test-curl clean help

help: ## Show help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}'

redis-up: ## Start local Redis 8 Docker container
	@echo "Checking Redis status..."
	@docker compose -f ../../docker-compose.yml up -d redis
	@until docker exec titip-redis8 redis-cli ping > /dev/null 2>&1; do \
		echo "Waiting for Redis to be ready..."; \
		sleep 0.5; \
	done
	@echo "Redis 8 is ready on localhost:6379"

build-caddy: ## Build custom Caddy binary with Titip and Redis modules
	@echo "Building Caddy binary with Titip plugins..."
	@go build -o ./caddy ./cmd/caddy
	@echo "Built custom Caddy binary at ./caddy"

origin: ## Run the mock upstream origin server on :9000
	@echo "Starting mock upstream origin on http://localhost:9000 ..."
	@go run main.go

run: redis-up build-caddy ## Start Redis, mock origin (:9000), and Caddy (:8080) in one command
	@echo "Ensuring port 9000 is clean..."
	@lsof -ti:9000 | xargs kill -9 2>/dev/null || true
	@echo "Starting mock upstream origin on :9000 in background..."; \
	go run main.go & ORIGIN_PID=$$!; \
	trap "kill -9 $$ORIGIN_PID 2>/dev/null; lsof -ti:9000 | xargs kill -9 2>/dev/null || true" EXIT INT TERM; \
	sleep 1; \
	echo "Starting Caddy on http://localhost:8080 (Admin on :2019)..."; \
	./caddy run --config Caddyfile

test-curl: ## Run automated smoke test curl against ESI endpoint
	@echo "Fetching Caddy ESI demo endpoint..."
	@curl -i http://localhost:8080/esi-demo

clean: ## Remove compiled local Caddy binary and clean ports
	@lsof -ti:9000 | xargs kill -9 2>/dev/null || true
	@rm -f ./caddy .origin.pid

