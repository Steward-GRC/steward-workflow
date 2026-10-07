#!/usr/bin/env bash
# Compares internal/workloadauth byte for byte with steward-core's origin copy
# at the commit STEWARD_CORE_REF pins in proto-refs.env. The copy is never
# edited here: a change goes to steward-core first, then the pin moves and the
# files are copied again.
#
# STEWARD_CORE_DIR points at a local steward-core checkout instead, for trying
# an unmerged change.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "$root/proto-refs.env"

pkg="internal/workloadauth"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

if [[ -n "${STEWARD_CORE_DIR:-}" ]]; then
  echo "workloadauth: steward-core from $STEWARD_CORE_DIR"
  mkdir -p "$work/$pkg"
  cp -R "$STEWARD_CORE_DIR/$pkg/." "$work/$pkg/"
else
  echo "workloadauth: steward-core at $STEWARD_CORE_REF"
  curl -sSfL "https://codeload.github.com/Steward-GRC/steward-core/tar.gz/$STEWARD_CORE_REF" |
    tar -xz -C "$work" --strip-components=1 --wildcards "*/$pkg/*"
fi

if ! diff -r "$work/$pkg" "$root/$pkg"; then
  echo "workloadauth: $pkg differs from steward-core at the pinned commit; copy it again from there" >&2
  exit 1
fi
echo "workloadauth: $pkg matches steward-core"
