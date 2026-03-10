#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/release-version.sh next <cli|extensions>
  scripts/release-version.sh apply <cli|extensions> <0.1.x>
EOF
}

require_supported_kind() {
  case "${1}" in
    cli|extensions) ;;
    *)
      echo "unsupported release kind: ${1}" >&2
      exit 1
      ;;
  esac
}

tag_prefix_for_kind() {
  case "${1}" in
    cli) echo "cli-v" ;;
    extensions) echo "ext-v" ;;
  esac
}

validate_version() {
  if [[ ! "${1}" =~ ^0\.1\.[0-9]+$ ]]; then
    echo "release version must match 0.1.x: ${1}" >&2
    exit 1
  fi
}

manifest_files_for_kind() {
  case "${1}" in
    cli)
      printf '%s\n' "crates/stopgap-cli/Cargo.toml"
      ;;
    extensions)
      printf '%s\n' \
        "crates/common/Cargo.toml" \
        "crates/plts/Cargo.toml" \
        "crates/stopgap/Cargo.toml" \
        "packages/runtime/package.json"
      ;;
  esac
}

cargo_lock_packages_for_kind() {
  case "${1}" in
    cli)
      printf '%s\n' "stopgap-cli"
      ;;
    extensions)
      printf '%s\n' "common" "plts" "stopgap"
      ;;
  esac
}

next_version() {
  local kind="$1"
  local prefix latest_patch tag patch

  prefix="$(tag_prefix_for_kind "${kind}")"
  latest_patch=-1

  while IFS= read -r tag; do
    if [[ "${tag}" =~ ^${prefix}0\.1\.([0-9]+)$ ]]; then
      patch="${BASH_REMATCH[1]}"
      if (( patch > latest_patch )); then
        latest_patch="${patch}"
      fi
    fi
  done < <(git tag --list "${prefix}0.1.*")

  echo "0.1.$((latest_patch + 1))"
}

apply_version_to_manifest() {
  local file="$1"
  local version="$2"

  case "${file}" in
    *.toml)
      VERSION="${version}" perl -0pi -e 's/^version = "[^"]+"$/version = "$ENV{VERSION}"/m' "${file}"
      ;;
    *.json)
      VERSION="${version}" perl -0pi -e 's/"version":\s*"[^"]+"/"version": "$ENV{VERSION}"/' "${file}"
      ;;
    *)
      echo "unsupported manifest file: ${file}" >&2
      exit 1
      ;;
  esac
}

apply_version_to_cargo_lock() {
  local package_name="$1"
  local version="$2"

  PACKAGE_NAME="${package_name}" VERSION="${version}" perl -0pi -e '
    my $package = $ENV{PACKAGE_NAME};
    my $version = $ENV{VERSION};
    s/(name = "\Q$package\E"\nversion = ")[^"]+(")/$1.$version.$2/egs;
  ' Cargo.lock
}

apply_version() {
  local kind="$1"
  local version="$2"
  local file package_name

  while IFS= read -r file; do
    apply_version_to_manifest "${file}" "${version}"
  done < <(manifest_files_for_kind "${kind}")

  while IFS= read -r package_name; do
    apply_version_to_cargo_lock "${package_name}" "${version}"
  done < <(cargo_lock_packages_for_kind "${kind}")
}

main() {
  if [[ $# -lt 2 ]]; then
    usage >&2
    exit 1
  fi

  local command="$1"
  local kind="$2"

  require_supported_kind "${kind}"

  case "${command}" in
    next)
      if [[ $# -ne 2 ]]; then
        usage >&2
        exit 1
      fi
      next_version "${kind}"
      ;;
    apply)
      if [[ $# -ne 3 ]]; then
        usage >&2
        exit 1
      fi
      validate_version "${3}"
      apply_version "${kind}" "${3}"
      ;;
    *)
      usage >&2
      exit 1
      ;;
  esac
}

main "$@"
