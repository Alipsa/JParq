#!/usr/bin/env bash
# count-loc.sh — zero-config LOC counter for Java & Groovy
# Usage: ./count-loc.sh
# Outputs to reports/loc-YYYYMMDD-HHMMSS/{loc.txt,loc.json,loc.csv}

# Set strict mode: exit on error (-e), exit on unset vars (-u),
# fail pipe chain if any command fails (-o pipefail)
set -euo pipefail

DEFAULT_EXCLUDES=(target build dist out node_modules .gradle .idea .vscode gen docs tmp .git)
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
OUT_DIR="reports/loc-$TIMESTAMP"
RUN_VIA_DOCKER=${RUN_VIA_DOCKER:-auto} # auto|docker|podman|no
CLOC_BIN="cloc"
PATH_ARG="."
DEFAULT_INCLUDE_EXT="java,groovy"

# Function to check if a command exists
have_cmd() { command -v "$1" >/dev/null 2>&1; }

# Function to run cloc, potentially via a container
run_cloc() {
  if [[ "$RUN_VIA_DOCKER" == "no" ]] && ! have_cmd "$CLOC_BIN"; then
    echo "Error: cloc not found and container fallback disabled." >&2
    exit 3
  fi
  if have_cmd "$CLOC_BIN" && [[ "$RUN_VIA_DOCKER" != "docker" && "$RUN_VIA_DOCKER" != "podman" ]]; then
    "$CLOC_BIN" "$@"
  else
    engine="docker"
    if [[ "$RUN_VIA_DOCKER" == "podman" ]] || ( [[ "$RUN_VIA_DOCKER" == "auto" ]] && have_cmd podman ); then
      engine="podman"
    elif ! have_cmd docker && [[ "$RUN_VIA_DOCKER" == "auto" ]]; then
      echo "No cloc, docker, or podman available." >&2; exit 3
    fi
    # Use 'id -u' and 'id -g' which are portable
    "$engine" run --rm -u "$(id -u):$(id -g)" \
      -v "$(pwd)":/work -w /work aldanial/cloc:latest "$@"
  fi
}

mkdir -p "$OUT_DIR"

# --- CLOC ARGUMENT SETUP ---
CLOC_ARGS=(--quiet --hide-rate --read-binary-files)
CLOC_ARGS+=(--include-ext="$DEFAULT_INCLUDE_EXT")
CLOC_ARGS+=(--match-f='.*\.(java|groovy)$')

# Exclude build/vendor dirs using a robust array join
EXCLUDES=("${DEFAULT_EXCLUDES[@]}")
CLEAN_EXCLUDES=()
for d in "${EXCLUDES[@]}"; do [[ -n "$d" ]] && CLEAN_EXCLUDES+=("$d"); done
if [[ ${#CLEAN_EXCLUDES[@]} -gt 0 ]]; then
  # Use command substitution to join array elements with comma
  CLOC_ARGS+=(--exclude-dir="$(IFS=, ; echo "${CLEAN_EXCLUDES[*]}")")
fi

TEXT_OUT="$OUT_DIR/loc.txt"
JSON_OUT="$OUT_DIR/loc.json"
CSV_OUT="$OUT_DIR/loc.csv"

echo ">> Counting Java & Groovy lines of code in current directory"

set +e
version=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout 2>/dev/null)
mvn_exit_code=$?
set -e

if [[ $mvn_exit_code -ne 0 ]] || [[ -z "$version" ]]; then
  version="N/A"
  echo "Warning: Could not determine project version (Maven failed). Using 'N/A'." >&2
fi

CLOC_REPORT=$(run_cloc "$PATH_ARG" "${CLOC_ARGS[@]}")

echo "$version" > "$TEXT_OUT"

echo "$CLOC_REPORT" >> "$TEXT_OUT"
run_cloc "$PATH_ARG" "${CLOC_ARGS[@]}" --json > "$JSON_OUT"
run_cloc "$PATH_ARG" "${CLOC_ARGS[@]}" --csv --report-file="$CSV_OUT" >/dev/null

cat "$TEXT_OUT"
echo
echo "Version $version"
echo "✅ LOC reports created:"
echo "  - Text : $TEXT_OUT"
echo "  - JSON : $JSON_OUT"
echo "  - CSV  : $CSV_OUT"