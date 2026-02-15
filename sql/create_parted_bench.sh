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

# Create a partitioned table from the H2OAI 10M-row benchmark CSV.
#
# Usage:
#   sql/create_parted_bench.sh [--parts N] [--db /tmp/teide_db]
#
# Prerequisites:
#   - Release build: cmake --build build_release
#   - Bench CSV: ../rayforce-bench/datasets/G1_1e7_1e2_0_0/G1_1e7_1e2_0_0.csv

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Defaults
PARTS=10
DB_ROOT="/tmp/teide_db"

while [[ $# -gt 0 ]]; do
    case $1 in
        --parts) PARTS="$2"; shift 2 ;;
        --db)    DB_ROOT="$2"; shift 2 ;;
        *)       echo "Usage: $0 [--parts N] [--db /path]"; exit 1 ;;
    esac
done

LIB="$PROJECT_DIR/build_release/libteide.so"
if [[ ! -f "$LIB" ]]; then
    echo "Building release library..."
    cmake --build "$PROJECT_DIR/build_release" --parallel
fi

echo "Creating parted table: $PARTS partitions → $DB_ROOT"
TEIDE_LIB="$LIB" python3 "$SCRIPT_DIR/create_parted.py" --parts "$PARTS" --db "$DB_ROOT"
