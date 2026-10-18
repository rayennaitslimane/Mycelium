# botan-2.pc emits link flags that cgo rejects by default.
export CGO_LDFLAGS_ALLOW := -fstack-protector|-m64

.PHONY: build test cluster clean

build:
	go build -o bin/ ./cmd/...

# Unit, component and cluster-scenario tests, with the race detector (about a minute).
# For only the fast ones: go test -short ./...
test:
	go test -race ./...

# Local cluster: 1 monitor, 4 nodes, 1 gateway. See scripts/cluster.sh.
cluster: build
	./scripts/cluster.sh

clean:
	rm -rf bin run
	go clean -testcache
