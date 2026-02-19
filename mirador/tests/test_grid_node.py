import os
import tempfile
from mirador.nodes.outputs.grid import GridNode
from teide.api import Table


def test_grid_returns_rows(init_teide):
    lib = init_teide
    data = "x,y\n1,10.5\n2,20.0\n3,30.5\n"
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(data)
        path = f.name
    try:
        tbl_ptr = lib.read_csv(path)
        table = Table(lib, tbl_ptr)

        node = GridNode()
        result = node.execute(
            {"df": table, "columns": table.columns},
            {"page_size": 10}
        )

        assert "rows" in result
        assert "columns" in result
        assert result["total"] == 3
        assert len(result["rows"]) == 3
        # Verify data is JSON-serializable (no Table/Series objects)
        import json
        json.dumps(result)
    finally:
        os.unlink(path)


def test_grid_empty_input():
    node = GridNode()
    result = node.execute({}, {})
    assert result["rows"] == []
    assert result["total"] == 0


def test_grid_page_size(init_teide):
    lib = init_teide
    data = "x\n1\n2\n3\n4\n5\n"
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(data)
        path = f.name
    try:
        tbl_ptr = lib.read_csv(path)
        table = Table(lib, tbl_ptr)

        node = GridNode()
        result = node.execute(
            {"df": table, "columns": ["x"]},
            {"page_size": 2}
        )
        assert len(result["rows"]) == 2
        assert result["total"] == 5
    finally:
        os.unlink(path)
