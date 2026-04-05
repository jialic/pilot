#!/bin/sh
set -eu

# Build release binary and upload to R2.
# Usage: ./release.sh

ARCH=$(uname -m)
VERSION=$(grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)".*/\1/')
BINARY="pilot-darwin-${ARCH}-${VERSION}"

echo "Building pilot ${VERSION} for darwin-${ARCH}..."
cargo build --release

echo "Uploading ${BINARY}..."
rclone copyto target/release/pilot "r2-pilot:pilot/${BINARY}" --progress

echo "Uploading version.txt..."
echo "${VERSION}" | rclone rcat "r2-pilot:pilot/version.txt"

echo "Uploading install.sh..."
rclone copyto install.sh "r2-pilot:pilot/install.sh"

echo "Done. Install with:"
echo "  curl -fsSL https://pub-cd5a485df5854584bcd09a2fa723f40a.r2.dev/install.sh | sh"
