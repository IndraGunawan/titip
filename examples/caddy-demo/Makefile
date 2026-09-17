.PHONY: run origin build-caddy services-up services-down redis-up loadtest test-curl clean help

help: ## Show help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}'

services-up: ## Start Redis 8, Prometheus, and Grafana via Docker Compose
	@echo "Starting observability stack (Redis 8, Prometheus, Grafana)..."
	@docker compose up -d
	@until docker exec titip-redis8 redis-cli ping > /dev/null 2>&1; do \
		echo "Waiting for Redis to be ready..."; \
		sleep 0.5; \
	done
	@echo "Observability stack is ready:"
	@echo "  • Redis 8:    localhost:6379"
	@echo "  • Grafana:    http://localhost:3000 (Titip Dashboard preloaded)"

services-down: ## Stop Redis, Prometheus, and Grafana containers
	@docker compose down

redis-up: services-up ## Alias for services-up

build-caddy: ## Build custom Caddy binary with Titip and Redis modules
	@echo "Building Caddy binary with Titip plugins..."
	@go build -o ./caddy ./cmd/caddy
	@echo "Built custom Caddy binary at ./caddy"

origin: ## Run the mock upstream origin server on :9000
	@echo "Starting mock upstream origin on http://localhost:9000 ..."
	@go run main.go

run: services-up build-caddy ## Start Redis, Prometheus, Grafana, mock origin (:9000), and Caddy (:8080)
	@echo "Ensuring port 9000 is clean..."
	@lsof -ti:9000 | xargs kill -9 2>/dev/null || true
	@echo "Starting mock upstream origin on :9000 in background..."; \
	go run main.go & ORIGIN_PID=$$!; \
	trap "kill -9 $$ORIGIN_PID 2>/dev/null; lsof -ti:9000 | xargs kill -9 2>/dev/null || true" EXIT INT TERM; \
	sleep 1; \
	echo "Starting Caddy on http://localhost:8080 (Admin on :2019)..."; \
	./caddy run --config Caddyfile

loadtest: ## Run k6 load test (configure settings directly in loadtest.js)
	@k6 run loadtest.js

test-curl: ## Run automated smoke test curl against ESI endpoint
	@echo "Fetching Caddy ESI demo endpoint..."
	@curl -i http://localhost:8080/esi-demo

clean: ## Remove compiled local Caddy binary, clean ports, and stop containers
	@lsof -ti:9000 | xargs kill -9 2>/dev/null || true
	@rm -f ./caddy .origin.pid
	@docker compose down
