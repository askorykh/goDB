#!/usr/bin/env bash
set -euo pipefail

# Render docs/repl-demo.gif from docs/repl-demo.cast using agg.
# Usage: tools/replgif/render_gif.sh

if ! command -v agg >/dev/null 2>&1; then
  echo "agg not found. Install with: go install github.com/asciinema/agg@latest" >&2
  exit 1
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

agg -w 80 -h 24 -t "Solarized Dark" docs/repl-demo.cast docs/repl-demo.gif
