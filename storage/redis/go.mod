module github.com/indragunawan/titip/storage/redis

go 1.26.1

require (
	github.com/alicebob/miniredis/v2 v2.38.0
	github.com/indragunawan/titip v0.0.0
	github.com/redis/rueidis v1.0.77
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/yuin/gopher-lua v1.1.1 // indirect
	golang.org/x/sys v0.47.0 // indirect
)

replace github.com/indragunawan/titip => ../..
