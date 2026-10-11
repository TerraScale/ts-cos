#!/bin/sh
# Bootstraps this clean checkout as the last-known-good COS build on this host and
# (re)starts the user service. Rollouts replace builds from then on.
set -eu
cd "$(dirname "$0")/.."
if [ -n "$(git status --porcelain)" ]; then
	echo "install.sh: commit or stash local changes first" >&2
	exit 1
fi
revision=$(git rev-parse HEAD)
dir="$HOME/.local/share/cos"
mkdir -p "$dir/builds/$revision" "$HOME/.local/bin"
CGO_ENABLED=0 nice -n 19 go build -trimpath -o "$dir/builds/$revision/cos.new" .
mv "$dir/builds/$revision/cos.new" "$dir/builds/$revision/cos"
rm -f "$dir/trial" "$dir/trial.next" "$dir/reverted"
ln -sfn "builds/$revision" "$dir/good"
ln -sfn "builds/$revision" "$dir/current"
ln -sfn "$dir/current/cos" "$HOME/.local/bin/cos"
install -Dm644 deploy/cos.service "$HOME/.config/systemd/user/cos.service"
systemctl --user daemon-reload
systemctl --user enable cos.service
systemctl --user restart cos.service
