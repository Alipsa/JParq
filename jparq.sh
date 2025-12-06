#!/usr/bin/env bash
set -euo pipefail

mkdir -p target
JARFILE=$(find target -name "jparq*fat.jar" -print -quit)
if [[ ! -f "$JARFILE" ]]; then
  echo "No fat jar found, building project..."
  mvn -B package
fi
java -jar "$JARFILE" target/test-classes/acme