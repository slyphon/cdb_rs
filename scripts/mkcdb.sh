#!/bin/bash

set -euo pipefail
IFS=$'\n\t'

die() { echo "fatal: $*" >&2; exit 1; }

cd "$(git rev-parse --show-toplevel)" || die "could not cd to top level"

TEMP="$(mktemp -d -t TEMP.XXXXXXX)" || die "failed to make tmpdir"
cleanup() { [[ -n "${TEMP:-}" ]] && rm -rf "${TEMP}"; }
trap cleanup EXIT


INPUT="$TEMP/cdbin"

cargo run --release --bin generatecdb |head -n 5000|pv > "$INPUT"
echo '' >> $INPUT

cdb -c dict.cdb "$INPUT"
