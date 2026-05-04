#!/bin/bash
# Cross-reference dork query results with ABN DB
set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DORK_FILE="${1:-$PROJECT_ROOT/data/sydney_plumber_queries.txt}"
OUTPUT="${2:-$PROJECT_ROOT/data/dork_results_$(date +%Y%m%d_%H%M).jsonl}"

echo "=== Dork → ABN Cross-Reference ==="
echo "  Dork file : $DORK_FILE"
echo "  Output    : $OUTPUT"

# Extract capitalised business names from dork file (heuristic)
# Adjust this if your dork output format differs
grep -E '^[A-Za-z][A-Za-z &]+' "$DORK_FILE" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | grep -v '^$' | sort -u > /tmp/dork_names.txt
COUNT=$(wc -l < /tmp/dork_names.txt)
echo "[INFO] Extracted $COUNT unique business names"

python3 "$PROJECT_ROOT/scripts/crossref_abn_business_names.py" \
    --input /tmp/dork_names.txt \
    --output "$OUTPUT" \
    --limit 500

echo "✅ Done: $OUTPUT"
