module github.com/indragunawan/titip/examples/caddy-demo

go 1.26.1

replace (
	github.com/indragunawan/titip => ../..
	github.com/indragunawan/titip/adapter/caddy => ../../adapter/caddy
	github.com/indragunawan/titip/storage/redis => ../../storage/redis
	github.com/indragunawan/titip/storage/redis/caddy => ../../storage/redis/caddy
)
