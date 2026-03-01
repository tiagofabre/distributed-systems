#!/usr/bin/env bash
# Setup async-profiler and FlameGraph via Homebrew for CPU flamegraphs on macOS/Linux.
# Usage: from project root, run: ./tools/setup-flamegraph.sh

set -e

if ! command -v brew >/dev/null 2>&1; then
  echo "Homebrew is required. Install from https://brew.sh" >&2
  exit 1
fi

echo "Installing FlameGraph..."
brew install flamegraph

echo "Installing async-profiler (tap qwwdfsad/tap)..."
brew tap qwwdfsad/tap 2>/dev/null || true
brew install async-profiler

if ! command -v asprof >/dev/null 2>&1; then
  echo "asprof not found in PATH after install. Check your Homebrew setup." >&2
  exit 1
fi

if ! command -v flamegraph.pl >/dev/null 2>&1; then
  echo "flamegraph.pl not found in PATH after install. Check your Homebrew setup." >&2
  exit 1
fi

echo "Done. Use: make flamegraph (after starting a node with make run-flamegraph-node1)"
