# Teide

Fast columnar dataframe library for Python, powered by a zero-dependency C17 engine with lazy fusion execution.

## Install

```bash
pip install teide
```

> **Note:** v0.1.0 supports Linux and macOS only. Windows support is planned.

## Quick Start

```python
from teide import Context, col

with Context() as ctx:
    df = ctx.read_csv("data.csv")

    result = (
        df.filter(col("price") > 0)
          .group_by("category")
          .agg(col("price").sum(), col("price").mean())
          .sort("price_sum", descending=True)
          .collect()
    )

    print(result)
```

## Features

- **Lazy evaluation** with automatic query optimization (predicate pushdown, CSE, operator fusion)
- **Morsel-driven parallel execution** across all cores
- **Zero-copy** NumPy interop for numeric columns
- **Fast CSV reader** — parallel parsing, mmap I/O
- **Columnar storage** — splayed tables, date-partitioned datasets
- **Pure C17 engine** — no external dependencies, minimal memory overhead

## License

MIT
