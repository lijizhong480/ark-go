#!/usr/bin/env bash
set -euo pipefail

workspace="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
found=0
for module_file in "$workspace"/*/go.mod; do
  [[ -f "$module_file" ]] || continue
  module_dir="${module_file%/go.mod}"
  module_name="${module_dir##*/}"
  printf '\n== %s ==\n' "$module_name"
  (cd "$module_dir" && go test ./...)
  found=$((found + 1))
done
if [[ -f "$workspace/examples/service/go.mod" ]]; then
  printf '\n== examples/service ==\n'
  (cd "$workspace/examples/service" && go test ./...)
  found=$((found + 1))
fi
printf '\nTested %d independent modules.\n' "$found"
