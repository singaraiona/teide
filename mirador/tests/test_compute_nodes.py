import os
import tempfile
from teide.api import Table
from mirador.nodes.compute.query import QueryNode


def _make_table(lib):
    data = "id,val\na,10\na,20\nb,30\nb,40\n"
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(data)
        path = f.name
    tbl_ptr = lib.read_csv(path)
    table = Table(lib, tbl_ptr)
    os.unlink(path)
    return table


def test_query_filter_eq(init_teide):
    table = _make_table(init_teide)
    node = QueryNode()
    result = node.execute(
        {"df": table, "columns": table.columns},
        {"mode": "form", "filter": {"column": "id", "operator": "eq", "value": "a"}}
    )
    assert result["rows"] == 2


def test_query_filter_gt(init_teide):
    table = _make_table(init_teide)
    node = QueryNode()
    result = node.execute(
        {"df": table, "columns": table.columns},
        {"mode": "form", "filter": {"column": "val", "operator": "gt", "value": "20"}}
    )
    assert result["rows"] == 2  # 30, 40


def test_query_groupby_sum(init_teide):
    table = _make_table(init_teide)
    node = QueryNode()
    result = node.execute(
        {"df": table, "columns": table.columns},
        {"mode": "form", "groupby": {"keys": ["id"],
                                      "aggs": [{"column": "val", "op": "sum"}]}}
    )
    assert result["rows"] == 2  # a, b groups
    assert len(result["columns"]) >= 2  # at least id + sum


def test_sql_rejects_garbage(init_teide):
    """Garbage SQL must raise ValueError, not silently pass through."""
    table = _make_table(init_teide)
    node = QueryNode()
    import pytest
    with pytest.raises(ValueError):
        node.execute({"df": table}, {"mode": "sql", "sql": "hello world nonsense"})


def test_sql_rejects_wrong_table(init_teide):
    """SQL referencing a table other than 'data' must raise ValueError."""
    table = _make_table(init_teide)
    node = QueryNode()
    import pytest
    with pytest.raises(ValueError, match="Unknown table"):
        node.execute({"df": table}, {"mode": "sql", "sql": "SELECT * FROM users WHERE id = 1"})


def test_sql_rejects_unknown_column(init_teide):
    """SQL referencing a column that doesn't exist must raise ValueError."""
    table = _make_table(init_teide)
    node = QueryNode()
    import pytest
    with pytest.raises(ValueError, match="Unknown column"):
        node.execute({"df": table}, {"mode": "sql", "sql": "SELECT * FROM data WHERE bogus > 5"})


def test_sql_rejects_empty(init_teide):
    """Empty SQL must raise ValueError."""
    table = _make_table(init_teide)
    node = QueryNode()
    import pytest
    with pytest.raises(ValueError, match="empty"):
        node.execute({"df": table}, {"mode": "sql", "sql": ""})


def test_sql_valid_filter(init_teide):
    """Valid SQL filter should work correctly."""
    table = _make_table(init_teide)
    node = QueryNode()
    result = node.execute(
        {"df": table},
        {"mode": "sql", "sql": "SELECT * FROM data WHERE val > 20"}
    )
    assert result["rows"] == 2  # 30, 40


def test_sql_shorthand_filter(init_teide):
    """Shorthand SQL (just WHERE) auto-wraps and works."""
    table = _make_table(init_teide)
    node = QueryNode()
    result = node.execute(
        {"df": table},
        {"mode": "sql", "sql": "WHERE val > 20"}
    )
    assert result["rows"] == 2  # 30, 40


def test_sql_shorthand_order(init_teide):
    """Shorthand ORDER BY auto-wraps and works."""
    table = _make_table(init_teide)
    node = QueryNode()
    result = node.execute(
        {"df": table},
        {"mode": "sql", "sql": "ORDER BY val DESC"}
    )
    assert result["rows"] == 4
    data = result["df"].to_dict()
    assert data["val"] == [40, 30, 20, 10]


def test_sql_shorthand_combined(init_teide):
    """Shorthand WHERE + ORDER BY combined."""
    table = _make_table(init_teide)
    node = QueryNode()
    result = node.execute(
        {"df": table},
        {"mode": "sql", "sql": "WHERE val > 10 ORDER BY val DESC"}
    )
    assert result["rows"] == 3  # 40, 30, 20
    data = result["df"].to_dict()
    assert data["val"] == [40, 30, 20]


def test_sql_valid_order(init_teide):
    """Valid SQL ORDER BY should work correctly."""
    table = _make_table(init_teide)
    node = QueryNode()
    result = node.execute(
        {"df": table},
        {"mode": "sql", "sql": "SELECT * FROM data ORDER BY val DESC"}
    )
    assert result["rows"] == 4
    data = result["df"].to_dict()
    assert data["val"] == [40, 30, 20, 10]


def test_sql_valid_group(init_teide):
    """Valid SQL GROUP BY should work correctly."""
    table = _make_table(init_teide)
    node = QueryNode()
    result = node.execute(
        {"df": table},
        {"mode": "sql", "sql": "SELECT id, SUM(val) FROM data GROUP BY id"}
    )
    assert result["rows"] == 2  # a, b
