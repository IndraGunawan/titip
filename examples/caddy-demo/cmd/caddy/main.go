package main

import (
	caddycmd "github.com/caddyserver/caddy/v2/cmd"

	// Plug in Caddy standard modules
	_ "github.com/caddyserver/caddy/v2/modules/standard"

	// Plug in Titip Caddy HTTP middleware and Redis storage adapter
	_ "github.com/indragunawan/titip/adapter/caddy"
	_ "github.com/indragunawan/titip/storage/redis/caddy"
)

func main() {
	caddycmd.Main()
}
