import os
import tempfile
from teide.api import Table
from mirador.nodes.compute.filter import FilterNode
from mirador.nodes.compute.groupby import GroupByNode


def _make_table(lib):
    data = "id,val\na,10\na,20\nb,30\nb,40\n"
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(data)
        path = f.name
    tbl_ptr = lib.read_csv(path)
    table = Table(lib, tbl_ptr)
    os.unlink(path)
    return table


def test_filter_eq(init_teide):
    table = _make_table(init_teide)
    node = FilterNode()
    result = node.execute(
        {"df": table, "columns": table.columns},
        {"column": "id", "operator": "eq", "value": "a"}
    )
    assert result["rows"] == 2


def test_filter_gt(init_teide):
    table = _make_table(init_teide)
    node = FilterNode()
    result = node.execute(
        {"df": table, "columns": table.columns},
        {"column": "val", "operator": "gt", "value": "20"}
    )
    assert result["rows"] == 2  # 30, 40


def test_groupby_sum(init_teide):
    table = _make_table(init_teide)
    node = GroupByNode()
    result = node.execute(
        {"df": table, "columns": table.columns},
        {"keys": ["id"], "aggs": [{"column": "val", "op": "sum"}]}
    )
    assert result["rows"] == 2  # a, b groups
    assert len(result["columns"]) >= 2  # at least id + sum
