#!/usr/bin/env bash
# Builds and installs a static, minimal Botan 3.12.0 under .botan/ (see ADR 0011).
# The Makefile points pkg-config at it; for plain `go build`/`go test` run:
#   export PKG_CONFIG_PATH=$PWD/.botan/lib/pkgconfig
set -euo pipefail
cd "$(dirname "$0")/.."

VERSION=3.12.0
PREFIX=${PREFIX:-$PWD/.botan}
TARBALL=Botan-$VERSION.tar.xz
# SHA-256 of the release tarball; fill in when pinning (the script prints the computed value).
SHA256=${BOTAN_SHA256:-}

if PKG_CONFIG_PATH="$PREFIX/lib/pkgconfig" pkg-config --exact-version="$VERSION" botan-3 2>/dev/null; then
  echo "botan $VERSION already installed in $PREFIX"
  exit 0
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
curl -fsSL -o "$work/$TARBALL" "https://botan.randombit.net/releases/$TARBALL"
sum=$(sha256sum "$work/$TARBALL" | cut -d' ' -f1)
echo "$TARBALL sha256 $sum"
if [ -n "$SHA256" ] && [ "$sum" != "$SHA256" ]; then
  echo "checksum mismatch" >&2
  exit 1
fi

tar -C "$work" -xf "$work/$TARBALL"
cd "$work/Botan-$VERSION"
./configure.py --prefix="$PREFIX" --disable-shared-library --minimized-build \
  --enable-modules=ml_kem,aes,gcm,hkdf,sha2_32,system_rng
make -j"$(nproc)" libs
make install
