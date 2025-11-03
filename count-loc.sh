#!/usr/bin/env bash
set -euo pipefail

# count-loc.sh — zero-config LOC counter for Java & Groovy
# Usage: ./count-loc.sh
# Outputs to reports/loc-YYYYMMDD-HHMMSS/{loc.txt,loc.json,loc.csv}

DEFAULT_EXCLUDES=(target build dist out node_modules .gradle .idea .vscode gen docs tmp .git)
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
OUT_DIR="reports/loc-$TIMESTAMP"
RUN_VIA_DOCKER=${RUN_VIA_DOCKER:-auto} # auto|docker|podman|no
CLOC_BIN="cloc"
PATH_ARG="."
DEFAULT_INCLUDE_EXT="java,groovy"

have_cmd() { command -v "$1" >/dev/null 2>&1; }

run_cloc() {
  if [[ "$RUN_VIA_DOCKER" == "no" ]] && ! have_cmd "$CLOC_BIN"; then
    echo "Error: cloc not found and container fallback disabled." >&2
    exit 3
  fi
  if have_cmd "$CLOC_BIN" && [[ "$RUN_VIA_DOCKER" != "docker" && "$RUN_VIA_DOCKER" != "podman" ]]; then
    $CLOC_BIN "$@"
  else
    engine="docker"
    if [[ "$RUN_VIA_DOCKER" == "podman" ]] || ( [[ "$RUN_VIA_DOCKER" == "auto" ]] && have_cmd podman ); then
      engine="podman"
    elif ! have_cmd docker && [[ "$RUN_VIA_DOCKER" == "auto" ]]; then
      echo "No cloc, docker, or podman available." >&2; exit 3
    fi
    $engine run --rm -u "$(id -u):$(id -g)" \
      -v "$(pwd)":/work -w /work aldanial/cloc:latest "$@"
  fi
}

mkdir -p "$OUT_DIR"

# NOTE: removed --sum-reports (that was causing "No counts found in .")
CLOC_ARGS=(--quiet --hide-rate --read-binary-files)
CLOC_ARGS+=(--include-ext="$DEFAULT_INCLUDE_EXT")
CLOC_ARGS+=(--match-f='.*\.(java|groovy)$')

# Exclude build/vendor dirs
EXCLUDES=("${DEFAULT_EXCLUDES[@]}")
CLEAN_EXCLUDES=()
for d in "${EXCLUDES[@]}"; do [[ -n "$d" ]] && CLEAN_EXCLUDES+=("$d"); done
if [[ ${#CLEAN_EXCLUDES[@]} -gt 0 ]]; then
  CLOC_ARGS+=(--exclude-dir="$(IFS=, ; echo "${CLEAN_EXCLUDES[*]}")")
fi

TEXT_OUT="$OUT_DIR/loc.txt"
JSON_OUT="$OUT_DIR/loc.json"
CSV_OUT="$OUT_DIR/loc.csv"

echo ">> Counting Java & Groovy lines of code in current directory"
version=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
echo $version >> "$TEXT_OUT"
run_cloc "$PATH_ARG" "${CLOC_ARGS[@]}" | tee --append "$TEXT_OUT" >/dev/null
run_cloc "$PATH_ARG" "${CLOC_ARGS[@]}" --json > "$JSON_OUT"
run_cloc "$PATH_ARG" "${CLOC_ARGS[@]}" --csv --report-file="$CSV_OUT" >/dev/null

cat $TEXT_OUT
echo
echo "Version $version"
echo "✅ LOC reports created:"
echo "  - Text : $TEXT_OUT"
echo "  - JSON : $JSON_OUT"
echo "  - CSV  : $CSV_OUT"
