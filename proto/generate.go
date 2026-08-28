//go:build generate

package proto

//go:generate buf generate --template ../buf.gen.yaml --output .. titip.proto
