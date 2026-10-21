#!/bin/bash
set -e

cd "$(dirname "$0")"

# Skip (rather than fail) when the branch isn't tracking a remote branch yet
if ! git rev-parse --abbrev-ref --symbolic-full-name '@{u}' >/dev/null 2>&1; then
  echo "Branch '$(git branch --show-current)' has no upstream, skipping git pull."
  exit 0
fi

echo "Pulling $(basename "$PWD") ($(git branch --show-current))..."
git pull
