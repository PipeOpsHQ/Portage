#!/usr/bin/env bash
# Print the next vMAJOR.MINOR.PATCH from conventional commits since the
# latest v* tag. Exit 2 if HEAD is already tagged or nothing is releasable
# (docs/chore/ci/test only).
set -euo pipefail

if git describe --exact-match --tags --match 'v[0-9]*' HEAD >/dev/null 2>&1; then
  echo "skip: HEAD already tagged $(git describe --exact-match --tags --match 'v[0-9]*' HEAD)" >&2
  exit 2
fi

latest=$(git describe --tags --abbrev=0 --match 'v[0-9]*' 2>/dev/null || true)
if [[ -z "$latest" ]]; then
  echo "v0.1.0"
  exit 0
fi

ver=${latest#v}
ver=${ver%%-*}
IFS=. read -r major minor patch <<<"$ver"
major=${major:-0}
minor=${minor:-0}
patch=${patch:-0}

bump=none
while IFS= read -r subject; do
  [[ -z "$subject" ]] && continue
  if [[ "$subject" =~ ^(feat|fix|perf|refactor)(\(.+\))?!: ]] || [[ "$subject" == *"BREAKING CHANGE"* ]]; then
    bump=major
    break
  fi
  if [[ "$subject" =~ ^feat(\(.+\))?: ]]; then
    bump=minor
    continue
  fi
  if [[ "$subject" =~ ^(fix|perf|e2e)(\(.+\))?: ]] || [[ "$subject" =~ ^[Rr]evert ]]; then
    if [[ "$bump" != minor && "$bump" != major ]]; then
      bump=patch
    fi
  fi
done < <(git log "${latest}..HEAD" --pretty=%s)

if git log "${latest}..HEAD" --pretty=%b | grep -q 'BREAKING CHANGE:'; then
  bump=major
fi

case "$bump" in
  major)
    major=$((major + 1))
    minor=0
    patch=0
    ;;
  minor)
    minor=$((minor + 1))
    patch=0
    ;;
  patch)
    patch=$((patch + 1))
    ;;
  *)
    echo "skip: no feat/fix since ${latest}" >&2
    exit 2
    ;;
esac

echo "v${major}.${minor}.${patch}"
