#!/bin/sh
# Builds this checkout and (re)starts it as the COS user service on this host.
set -eu
cd "$(dirname "$0")/.."
mkdir -p "$HOME/.local/bin"
nice -n 19 go build -trimpath -o "$HOME/.local/bin/.cos.new" .
mv "$HOME/.local/bin/.cos.new" "$HOME/.local/bin/cos"
install -Dm644 deploy/cos.service "$HOME/.config/systemd/user/cos.service"
systemctl --user daemon-reload
systemctl --user enable cos.service
systemctl --user restart cos.service
