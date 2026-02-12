#!/usr/bin/env python3
"""Window function benchmark runner for Teide via CLI on H2OAI 10M dataset."""

import subprocess
import time
import os

CSV_PATH = os.path.join(os.path.dirname(__file__),
                        "..", "rayforce-bench", "datasets",
                        "G1_1e7_1e2_0_0", "G1_1e7_1e2_0_0.csv")

CLI = os.path.join(os.path.dirname(__file__), "target", "release", "teide")
LIB = os.path.join(os.path.dirname(__file__), "build_release", "libteide.so")

N_ITER = 5

QUERIES = {
    "w1": f"SELECT id1, v1, ROW_NUMBER() OVER (PARTITION BY id1 ORDER BY v1) as rn FROM '{CSV_PATH}'",
    "w2": f"SELECT id1, id4, RANK() OVER (PARTITION BY id1 ORDER BY id4) as rnk FROM '{CSV_PATH}'",
    "w3": f"SELECT id3, v1, SUM(v1) OVER (PARTITION BY id3 ORDER BY v1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_sum FROM '{CSV_PATH}'",
    "w4": f"SELECT id1, v1, LAG(v1, 1) OVER (PARTITION BY id1 ORDER BY v1) as lag_v1 FROM '{CSV_PATH}'",
    "w5": f"SELECT id1, v1, AVG(v1) OVER (PARTITION BY id1) as avg_v1 FROM '{CSV_PATH}'",
    "w6": f"SELECT id1, id2, v1, ROW_NUMBER() OVER (PARTITION BY id1, id2 ORDER BY v1) as rn FROM '{CSV_PATH}'",
}


def main():
    csv_path = os.path.abspath(CSV_PATH)
    if not os.path.exists(csv_path):
        print(f"CSV not found: {csv_path}")
        return
    if not os.path.exists(CLI):
        print(f"CLI not found: {CLI} — run: cargo build --release -p teide-cli")
        return

    env = os.environ.copy()
    env["TEIDE_LIB"] = os.path.abspath(LIB)

    print(f"Teide window benchmarks (includes CSV load ~300ms):")
    print(f"  {'Query':12s}  {'Time':>8s}")
    print(f"  {'-'*12}  {'-'*8}")

    for label, sql in QUERIES.items():
        # Warmup
        for _ in range(2):
            subprocess.run([CLI, sql], env=env, capture_output=True)

        times = []
        for _ in range(N_ITER):
            t0 = time.perf_counter()
            result = subprocess.run([CLI, sql], env=env, capture_output=True)
            times.append(time.perf_counter() - t0)
            if result.returncode != 0:
                print(f"  {label:12s}  FAILED: {result.stderr.decode()[:100]}")
                break

        if times:
            elapsed = sorted(times)[len(times) // 2]
            print(f"  {label:12s}  {elapsed*1000:8.0f} ms")

    print("\nDone.")


if __name__ == "__main__":
    main()
