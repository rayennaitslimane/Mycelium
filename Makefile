# botan-3.pc emits link flags that cgo rejects by default.
export CGO_LDFLAGS_ALLOW := -fstack-protector|-m64
# Botan 3.12 is built into .botan/ by `make botan` (scripts/build-botan.sh).
export PKG_CONFIG_PATH := $(CURDIR)/.botan/lib/pkgconfig$(if $(PKG_CONFIG_PATH),:$(PKG_CONFIG_PATH))

.PHONY: build test cluster clean botan

build:
	go build -o bin/ ./cmd/...

botan:
	./scripts/build-botan.sh

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
