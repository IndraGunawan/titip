module github.com/indragunawan/titip/storage/redis

go 1.26.1

require (
	github.com/indragunawan/titip v0.1.0
	github.com/redis/rueidis v1.0.77
	google.golang.org/protobuf v1.36.12
)

replace github.com/indragunawan/titip => ../..

require (
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
)
