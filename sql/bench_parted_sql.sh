#!/usr/bin/env bash

#   Copyright (c) 2024-2026 Anton Kundenko <singaraiona@gmail.com>
#   All rights reserved.
#
#   Permission is hereby granted, free of charge, to any person obtaining a copy
#   of this software and associated documentation files (the "Software"), to deal
#   in the Software without restriction, including without limitation the rights
#   to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#   copies of the Software, and to permit persons to whom the Software is
#   furnished to do so, subject to the following conditions:
#
#   The above copyright notice and this permission notice shall be included in all
#   copies or substantial portions of the Software.
#
#   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#   SOFTWARE.

# Benchmark H2OAI groupby queries on a partitioned table via SQL.
#
# Runs all 7 queries in a single session (table opened once),
# using --timer for per-query execution timing.
#
# Usage:
#   sql/bench_parted_sql.sh [--db /tmp/teide_db] [--iters N]
#
# Prerequisites:
#   - Parted table created by: sql/create_parted_bench.sh
#   - CLI binary: cargo build --release --features cli

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

DB_ROOT="/tmp/teide_db"
N_ITER=7
TABLE_NAME="quotes"

while [[ $# -gt 0 ]]; do
    case $1 in
        --db)    DB_ROOT="$2"; shift 2 ;;
        --iters) N_ITER="$2"; shift 2 ;;
        *)       echo "Usage: $0 [--db /path] [--iters N]"; exit 1 ;;
    esac
done

TEIDE="$PROJECT_DIR/target/release/teide"
if [[ ! -x "$TEIDE" ]]; then
    echo "Building CLI binary..."
    cargo build --release --features cli --manifest-path "$PROJECT_DIR/rs/Cargo.toml"
fi

TABLE_PATH="$DB_ROOT/$TABLE_NAME"
if [[ ! -d "$DB_ROOT" ]]; then
    echo "Error: database not found at $DB_ROOT"
    echo "Run sql/create_parted_bench.sh first."
    exit 1
fi

QUERY_LABELS=(q1 q2 q3 q4 q5 q6 q7)
QUERY_DESCS=(
    "1 low-card key, SUM"
    "2 low-card keys, SUM"
    "1 high-card key, SUM+AVG"
    "1 med-card key, 3xAVG"
    "1 high-card key, 3xSUM"
    "1 high-card key, MAX+MIN"
    "6 keys, SUM+COUNT"
)

# Generate SQL file with all 7 queries
gen_sql() {
    cat <<EOF
SELECT id1, SUM(v1) FROM '${TABLE_PATH}' GROUP BY id1;
SELECT id1, id2, SUM(v1) FROM '${TABLE_PATH}' GROUP BY id1, id2;
SELECT id3, SUM(v1), AVG(v3) FROM '${TABLE_PATH}' GROUP BY id3;
SELECT id4, AVG(v1), AVG(v2), AVG(v3) FROM '${TABLE_PATH}' GROUP BY id4;
SELECT id6, SUM(v1), SUM(v2), SUM(v3) FROM '${TABLE_PATH}' GROUP BY id6;
SELECT id3, MAX(v1), MIN(v2) FROM '${TABLE_PATH}' GROUP BY id3;
SELECT id1, id2, id3, id4, id5, id6, SUM(v3), COUNT(v1) FROM '${TABLE_PATH}' GROUP BY id1, id2, id3, id4, id5, id6;
EOF
}

TMPFILE=$(mktemp /tmp/bench_parted_XXXXXX.sql)
gen_sql > "$TMPFILE"
trap "rm -f $TMPFILE" EXIT

echo "Teide SQL — H2OAI Groupby Benchmark (parted table)"
echo "  Table: $TABLE_PATH"
echo "  Iterations: $N_ITER (median)"
echo ""

# Collect times: run N_ITER iterations, each producing 7 "Run Time" lines
declare -a ALL_TIMES   # flat array: iter0_q0 iter0_q1 ... iter0_q6 iter1_q0 ...
for ((iter = 0; iter < N_ITER; iter++)); do
    mapfile -t run_times < <("$TEIDE" -t -f "$TMPFILE" 2>&1 | sed 's/\x1b\[[0-9;]*m//g' | grep -oP 'Run Time: \K[\d.]+' || true)
    for t in "${run_times[@]}"; do
        ALL_TIMES+=("$t")
    done
done

printf "  %-5s  %-28s  %10s\n" "Query" "Description" "Median"
printf "  %-5s  %-28s  %10s\n" "-----" "----------------------------" "----------"

for ((q = 0; q < 7; q++)); do
    # Extract times for query q across all iterations
    times_for_q=()
    for ((iter = 0; iter < N_ITER; iter++)); do
        idx=$(( iter * 7 + q ))
        if [[ $idx -lt ${#ALL_TIMES[@]} ]]; then
            times_for_q+=("${ALL_TIMES[$idx]}")
        fi
    done
    # Sort and pick median
    IFS=$'\n' sorted=($(sort -n <<<"${times_for_q[*]}")); unset IFS
    median="${sorted[$(( ${#sorted[@]} / 2 ))]}"
    printf "  %-5s  %-28s  %8s ms\n" "${QUERY_LABELS[$q]}" "${QUERY_DESCS[$q]}" "$median"
done

echo ""
echo "Done."
