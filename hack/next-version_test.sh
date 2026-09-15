#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPT="$ROOT/hack/next-version.sh"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
git -C "$tmp" init -q
git -C "$tmp" config user.email "t@t"
git -C "$tmp" config user.name "t"

commit() {
  echo "$1" >>"$tmp/f"
  git -C "$tmp" add f
  git -C "$tmp" commit -qm "$1"
}

expect() {
  local want=$1
  local got
  got=$(bash "$SCRIPT")
  [[ "$got" == "$want" ]] || { echo "want $want got $got"; exit 1; }
}

expect_skip() {
  local code=0
  bash "$SCRIPT" >/dev/null 2>&1 || code=$?
  [[ "$code" == 2 ]] || { echo "want skip exit 2 got $code"; exit 1; }
}

cd "$tmp"
commit "chore: init"
expect "v0.1.0"

git tag v0.1.0
expect_skip

commit "docs: readme"
expect_skip

commit "fix: leak"
expect "v0.1.1"

commit "feat: plugins"
expect "v0.2.0"

commit "feat!: drop v1alpha1"
expect "v1.0.0"

echo "ok"
