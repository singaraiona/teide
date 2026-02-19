import os
import tempfile
from mirador.nodes.inputs.csv_source import CsvSourceNode


def test_csv_source_loads(init_teide):
    data = "id,name,value\n1,a,10.5\n2,b,20.0\n3,c,30.5\n"
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(data)
        path = f.name
    try:
        node = CsvSourceNode()
        result = node.execute({}, {"file_path": path})
        assert "df" in result
        assert result["rows"] == 3
        assert "id" in result["columns"]
        assert "value" in result["columns"]
    finally:
        os.unlink(path)


def test_csv_source_missing_file():
    node = CsvSourceNode()
    try:
        node.execute({}, {"file_path": "/nonexistent/file.csv"})
        assert False, "Should raise RuntimeError"
    except RuntimeError:
        pass
