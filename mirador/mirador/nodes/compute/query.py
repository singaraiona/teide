"""Query node — unified filter/groupby/sort/join via form or SQL."""

# Copyright (c) 2024-2026 Anton Kundenko
# SPDX-License-Identifier: MIT

from typing import Any
from mirador.nodes.base import BaseNode, NodeMeta, NodePort


class QueryNode(BaseNode):
    meta = NodeMeta(
        id="query",
        label="Query",
        category="compute",
        description="Filter, group, sort, or join data using a form or SQL",
        inputs=[NodePort(name="in", description="Input dataframe")],
        outputs=[NodePort(name="out", description="Transformed dataframe")],
        config_schema={
            "type": "object",
            "properties": {
                "mode": {"type": "string", "enum": ["form", "sql"], "default": "form"},
                "sql": {"type": "string", "title": "SQL Query"},
                "filter": {
                    "type": "object",
                    "properties": {
                        "column": {"type": "string"},
                        "operator": {"type": "string",
                                     "enum": ["eq", "ne", "gt", "lt", "ge", "le"]},
                        "value": {},
                    },
                },
                "groupby": {
                    "type": "object",
                    "properties": {
                        "keys": {"type": "array", "items": {"type": "string"}},
                        "aggs": {"type": "array", "items": {
                            "type": "object",
                            "properties": {
                                "column": {"type": "string"},
                                "op": {"type": "string",
                                       "enum": ["sum", "avg", "min", "max", "count"]},
                            },
                        }},
                    },
                },
                "sort": {
                    "type": "object",
                    "properties": {
                        "columns": {"type": "array", "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "descending": {"type": "boolean", "default": False},
                            },
                        }},
                    },
                },
                "join": {
                    "type": "object",
                    "properties": {
                        "right_file": {"type": "string"},
                        "keys": {"type": "array", "items": {"type": "string"}},
                        "how": {"type": "string", "enum": ["inner", "left"],
                                "default": "inner"},
                    },
                },
            },
        },
    )

    def execute(self, inputs: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        mode = config.get("mode", "form")
        table = inputs["df"]

        if mode == "sql":
            table = self._exec_sql(table, config.get("sql", ""))
        else:
            table = self._exec_form(table, config)

        return {
            "df": table,
            "rows": len(table),
            "columns": table.columns,
        }

    def _exec_form(self, table: Any, config: dict[str, Any]) -> Any:
        """Chain filter → join → groupby → sort from structured config."""
        from teide.api import col, lit

        # 1. Filter
        flt = config.get("filter")
        if flt and flt.get("column") and flt.get("operator"):
            value = flt["value"]
            try:
                value = int(value)
            except (ValueError, TypeError):
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    pass

            op_map = {
                "eq": lambda c, v: c == v,
                "ne": lambda c, v: c != v,
                "gt": lambda c, v: c > v,
                "lt": lambda c, v: c < v,
                "ge": lambda c, v: c >= v,
                "le": lambda c, v: c <= v,
            }
            expr = op_map[flt["operator"]](col(flt["column"]), lit(value))
            table = table.filter(expr).collect()

        # 2. Join
        jn = config.get("join")
        if jn and jn.get("right_file") and jn.get("keys"):
            from mirador.app import get_teide
            from teide.api import Table

            lib = get_teide()
            right_ptr = lib.read_csv(jn["right_file"])
            if not right_ptr or right_ptr < 32:
                raise RuntimeError(f"Failed to load right table: {jn['right_file']}")
            right = Table(lib, right_ptr)
            how = jn.get("how", "inner")
            table = table.join(right, on=jn["keys"], how=how)

        # 3. Group By
        gb = config.get("groupby")
        if gb and gb.get("keys") and gb.get("aggs"):
            agg_map = {"sum": "sum", "avg": "mean", "min": "min",
                       "max": "max", "count": "count"}
            agg_exprs = [getattr(col(a["column"]), agg_map[a["op"]])()
                         for a in gb["aggs"]]
            table = table.group_by(*gb["keys"]).agg(*agg_exprs).collect()

        # 4. Sort
        srt = config.get("sort")
        if srt and srt.get("columns"):
            col_names = [c["name"] for c in srt["columns"]]
            descs = [c.get("descending", False) for c in srt["columns"]]
            table = table.sort(*col_names, descending=descs).collect()

        return table

    def _exec_sql(self, table: Any, sql: str) -> Any:
        """Parse SQL with sqlglot and chain Teide operations.

        Accepts either full SQL or shorthand clauses:
          - "SELECT * FROM data WHERE val > 20"   (full)
          - "WHERE val > 20 ORDER BY val DESC"     (shorthand — auto-wrapped)
          - "GROUP BY id"                          (shorthand)
        The input table is always the upstream dataframe — 'data' in FROM
        is optional syntactic sugar.
        """
        sql = sql.strip()
        if not sql:
            raise ValueError("SQL query is empty")

        import sqlglot
        from sqlglot import exp
        from sqlglot.errors import ParseError

        # Auto-wrap shorthand: if it starts with a clause keyword, prepend SELECT * FROM data
        upper = sql.upper().lstrip()
        clause_starts = ("WHERE ", "GROUP ", "ORDER ", "JOIN ", "LEFT ", "INNER ")
        if any(upper.startswith(kw) for kw in clause_starts):
            sql = f"SELECT * FROM data {sql}"

        # 1. Parse — reject unparseable SQL immediately
        try:
            parsed = sqlglot.parse_one(sql)
        except ParseError as e:
            raise ValueError(f"SQL syntax error: {e}") from None

        # 2. Must be a SELECT statement
        if not isinstance(parsed, exp.Select):
            raise ValueError(
                f"Only SELECT statements are supported, got: "
                f"{type(parsed).__name__}. "
                f"Example: WHERE val > 20  or  SELECT * FROM data WHERE val > 20"
            )

        # 3. Validate FROM table — must be 'data' or absent
        from_clause = parsed.find(exp.From)
        if from_clause:
            table_expr = from_clause.find(exp.Table)
            if table_expr:
                tname = table_expr.name.lower()
                if tname != "data":
                    raise ValueError(
                        f"Unknown table '{table_expr.name}'. "
                        f"Use 'data' to refer to the input table, or omit FROM entirely. "
                        f"Example: WHERE val > 20"
                    )

        # 4. Validate referenced columns exist
        table_cols = set(table.columns)
        self._validate_columns(parsed, table_cols)

        # 5. Extract and apply clauses — track if anything was applied
        applied = False

        # WHERE
        where = parsed.find(exp.Where)
        if where:
            table = self._apply_where(table, where.this)
            applied = True

        # JOIN
        joins = list(parsed.find_all(exp.Join))
        for join_node in joins:
            table = self._apply_join(table, join_node)
            applied = True

        # GROUP BY
        group = parsed.find(exp.Group)
        if group:
            table = self._apply_group(table, parsed)
            applied = True

        # ORDER BY
        order = parsed.find(exp.Order)
        if order:
            table = self._apply_order(table, order)
            applied = True

        # 6. SELECT * with no clauses is a valid pass-through
        #    But if nothing parsed at all, that's an error
        if not applied and not from_clause:
            raise ValueError(
                "No actionable clauses found. Examples:\n"
                "  WHERE val > 20\n"
                "  GROUP BY id\n"
                "  ORDER BY val DESC\n"
                "  SELECT id, SUM(val) FROM data GROUP BY id"
            )

        return table

    @staticmethod
    def _validate_columns(parsed: Any, table_cols: set[str]) -> None:
        """Check that column references in the SQL exist in the input table."""
        from sqlglot import exp

        # Collect all column references from the AST
        refs: list[str] = []
        for col_node in parsed.find_all(exp.Column):
            name = col_node.name
            # Skip qualified refs like data.col (the table part is separate)
            if name:
                refs.append(name)

        # Also check ORDER BY column names
        for ordered in parsed.find_all(exp.Ordered):
            if isinstance(ordered.this, exp.Column):
                refs.append(ordered.this.name)

        # Filter: skip '*' and aggregate-generated names
        bad = [r for r in refs if r not in table_cols and r != "*"]
        if bad:
            unique_bad = sorted(set(bad))
            available = ", ".join(sorted(table_cols))
            raise ValueError(
                f"Unknown column(s): {', '.join(unique_bad)}. "
                f"Available columns: {available}"
            )

    def _apply_where(self, table: Any, condition: Any) -> Any:
        """Convert a sqlglot WHERE condition to a Teide filter."""
        from sqlglot import exp
        from teide.api import col, lit

        # Handle AND/OR chains
        if isinstance(condition, exp.And):
            table = self._apply_where(table, condition.left)
            return self._apply_where(table, condition.right)

        if isinstance(condition, (exp.EQ, exp.NEQ, exp.GT, exp.LT, exp.GTE, exp.LTE)):
            left_col = condition.left.name
            right_val = self._extract_value(condition.right)

            op_map = {
                exp.EQ: lambda c, v: c == v,
                exp.NEQ: lambda c, v: c != v,
                exp.GT: lambda c, v: c > v,
                exp.LT: lambda c, v: c < v,
                exp.GTE: lambda c, v: c >= v,
                exp.LTE: lambda c, v: c <= v,
            }
            expr = op_map[type(condition)](col(left_col), lit(right_val))
            return table.filter(expr).collect()

        raise ValueError(
            f"Unsupported WHERE condition: {type(condition).__name__}. "
            f"Supported: =, !=, >, <, >=, <=, AND"
        )

    def _apply_join(self, table: Any, join_node: Any) -> Any:
        """Apply a JOIN from parsed SQL."""
        from sqlglot import exp
        from mirador.app import get_teide
        from teide.api import Table

        right_name = join_node.this
        if isinstance(right_name, exp.Table):
            right_file = right_name.name
        else:
            right_file = str(right_name)

        lib = get_teide()
        right_ptr = lib.read_csv(right_file)
        if not right_ptr or right_ptr < 32:
            raise RuntimeError(f"Failed to load right table: {right_file}")
        right = Table(lib, right_ptr)

        on_clause = join_node.find(exp.EQ)
        if on_clause:
            key = on_clause.left.name
            side = join_node.args.get("side")
            how = "left" if isinstance(side, str) and "LEFT" in side.upper() else "inner"
            return table.join(right, on=[key], how=how)

        raise ValueError("JOIN without ON clause not supported")

    def _apply_group(self, table: Any, parsed: Any) -> Any:
        """Apply GROUP BY + aggregations from parsed SQL."""
        from sqlglot import exp
        from teide.api import col

        group = parsed.find(exp.Group)
        keys = [e.name for e in group.expressions]

        agg_map = {"sum": "sum", "avg": "mean", "min": "min",
                   "max": "max", "count": "count"}

        agg_exprs = []
        select = parsed.find(exp.Select)
        if select:
            for expr in select.expressions:
                if isinstance(expr, (exp.Sum, exp.Avg, exp.Min, exp.Max, exp.Count)):
                    func_name = type(expr).__name__.lower()
                    col_name = expr.this.name if hasattr(expr.this, 'name') else str(expr.this)
                    method = agg_map.get(func_name, func_name)
                    agg_exprs.append(getattr(col(col_name), method)())

        if not agg_exprs:
            raise ValueError(
                "GROUP BY requires aggregate functions in SELECT. "
                "Example: SELECT id, SUM(val) FROM data GROUP BY id"
            )

        return table.group_by(*keys).agg(*agg_exprs).collect()

    def _apply_order(self, table: Any, order: Any) -> Any:
        """Apply ORDER BY from parsed SQL."""
        from sqlglot import exp

        col_names = []
        descs = []
        for ordered in order.expressions:
            if isinstance(ordered, exp.Ordered):
                col_names.append(ordered.this.name)
                descs.append(ordered.args.get("desc", False))
            else:
                col_names.append(ordered.name)
                descs.append(False)

        return table.sort(*col_names, descending=descs).collect()

    @staticmethod
    def _extract_value(node: Any) -> Any:
        """Extract a literal value from a sqlglot expression node."""
        from sqlglot import exp

        if isinstance(node, exp.Literal):
            if node.is_number:
                text = node.this
                try:
                    return int(text)
                except ValueError:
                    return float(text)
            return node.this
        return str(node)
