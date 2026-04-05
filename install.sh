#!/bin/sh
set -eu

BASE_URL="https://pub-cd5a485df5854584bcd09a2fa723f40a.r2.dev"
INSTALL_DIR="$HOME/.local/bin"
ARCH=$(uname -m)

# Get latest version
LATEST=$(curl -fsSL "$BASE_URL/version.txt")

# Check if already up to date
CURRENT=$("$INSTALL_DIR/pilot" --version 2>/dev/null | awk '{print $2}' || echo "none")
if [ "$CURRENT" = "$LATEST" ]; then
  echo "pilot ${LATEST} is already installed."
  exit 0
fi

# Download and install
BINARY="pilot-darwin-${ARCH}-${LATEST}"
echo "Installing pilot ${LATEST} (current: ${CURRENT})..."
mkdir -p "$INSTALL_DIR"
curl -fsSL "$BASE_URL/${BINARY}" -o "$INSTALL_DIR/pilot"
chmod +x "$INSTALL_DIR/pilot"
echo "Done."

# Check PATH
case ":$PATH:" in
  *":$INSTALL_DIR:"*) ;;
  *) echo "Add to your PATH: export PATH=\"$INSTALL_DIR:\$PATH\"" ;;
esac
