//go:build !fdb

package main

import "errors"

// newStore without the fdb build tag: the collector needs the FoundationDB binding (cgo),
// built into the image with `go build -tags fdb` (docker/Dockerfile).
func newStore(string) (Store, error) {
	return nil, errors.New("built without FoundationDB support (build with -tags fdb)")
}
