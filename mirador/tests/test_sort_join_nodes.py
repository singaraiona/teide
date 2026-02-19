import os
import tempfile
from teide.api import Table
from mirador.nodes.compute.query import QueryNode


def _make_table(lib, csv_str):
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(csv_str)
        path = f.name
    tbl_ptr = lib.read_csv(path)
    table = Table(lib, tbl_ptr)
    return table, path


def test_query_sort_asc(init_teide):
    table, path = _make_table(init_teide, "x,y\n3,c\n1,a\n2,b\n")
    try:
        node = QueryNode()
        result = node.execute(
            {"df": table, "columns": table.columns},
            {"mode": "form", "sort": {"columns": [{"name": "x"}]}}
        )
        assert result["rows"] == 3
        data = result["df"].to_dict()
        assert data["x"] == [1, 2, 3]
    finally:
        os.unlink(path)


def test_query_sort_desc(init_teide):
    table, path = _make_table(init_teide, "x\n1\n3\n2\n")
    try:
        node = QueryNode()
        result = node.execute(
            {"df": table, "columns": table.columns},
            {"mode": "form", "sort": {"columns": [{"name": "x", "descending": True}]}}
        )
        data = result["df"].to_dict()
        assert data["x"] == [3, 2, 1]
    finally:
        os.unlink(path)


def test_query_join_inner(init_teide):
    left, lpath = _make_table(init_teide, "id,val\na,10\nb,20\nc,30\n")
    _right_csv = "id,score\na,100\nb,200\n"
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(_right_csv)
        rpath = f.name
    try:
        node = QueryNode()
        result = node.execute(
            {"df": left, "columns": left.columns},
            {"mode": "form", "join": {"right_file": rpath, "keys": ["id"], "how": "inner"}}
        )
        assert result["rows"] == 2  # a, b (c dropped in inner join)
    finally:
        os.unlink(lpath)
        os.unlink(rpath)
