"""Tests for Chart and Export output nodes."""

import csv
import json
import os
import tempfile

import pytest

from mirador.nodes.outputs.chart import ChartNode
from mirador.nodes.outputs.export import ExportNode
from teide.api import Table


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_table(init_teide, csv_text):
    """Write csv_text to a temp file, load via Teide, return (Table, path)."""
    lib = init_teide
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    f.write(csv_text)
    f.close()
    tbl_ptr = lib.read_csv(f.name)
    table = Table(lib, tbl_ptr)
    return table, f.name


# ---------------------------------------------------------------------------
# ChartNode tests
# ---------------------------------------------------------------------------

class TestChartNode:
    def test_bar_chart(self, init_teide):
        csv_data = "region,sales\nEast,100\nWest,200\nNorth,150\n"
        table, path = _make_table(init_teide, csv_data)
        try:
            node = ChartNode()
            result = node.execute(
                {"df": table, "columns": table.columns},
                {"chart_type": "bar", "x_column": "region", "y_column": "sales"},
            )
            assert result["chart_type"] == "bar"
            opts = result["options"]
            assert "xAxis" in opts
            assert "yAxis" in opts
            assert opts["xAxis"]["type"] == "category"
            assert opts["yAxis"]["type"] == "value"
            assert len(opts["series"]) == 1
            assert opts["series"][0]["type"] == "bar"
            assert opts["series"][0]["data"] == [100, 200, 150]
            assert opts["xAxis"]["data"] == ["East", "West", "North"]
            assert result["rows"] == 3
        finally:
            os.unlink(path)

    def test_line_chart(self, init_teide):
        csv_data = "x,y\n1,10\n2,20\n3,30\n"
        table, path = _make_table(init_teide, csv_data)
        try:
            node = ChartNode()
            result = node.execute(
                {"df": table, "columns": table.columns},
                {"chart_type": "line", "x_column": "x", "y_column": "y"},
            )
            assert result["chart_type"] == "line"
            opts = result["options"]
            assert opts["series"][0]["type"] == "line"
            assert "xAxis" in opts
            assert "yAxis" in opts
        finally:
            os.unlink(path)

    def test_pie_chart(self, init_teide):
        csv_data = "category,value\nA,10\nB,20\nC,30\n"
        table, path = _make_table(init_teide, csv_data)
        try:
            node = ChartNode()
            result = node.execute(
                {"df": table, "columns": table.columns},
                {"chart_type": "pie", "x_column": "category", "y_column": "value"},
            )
            assert result["chart_type"] == "pie"
            opts = result["options"]
            series_data = opts["series"][0]["data"]
            assert len(series_data) == 3
            # Each entry should have name/value
            for entry in series_data:
                assert "name" in entry
                assert "value" in entry
            assert series_data[0]["name"] == "A"
            assert series_data[0]["value"] == 10
            assert opts["tooltip"]["trigger"] == "item"
        finally:
            os.unlink(path)

    def test_scatter_chart(self, init_teide):
        csv_data = "x,y\n1,10\n2,20\n3,30\n"
        table, path = _make_table(init_teide, csv_data)
        try:
            node = ChartNode()
            result = node.execute(
                {"df": table, "columns": table.columns},
                {"chart_type": "scatter", "x_column": "x", "y_column": "y"},
            )
            assert result["chart_type"] == "scatter"
            opts = result["options"]
            assert opts["xAxis"]["type"] == "value"
            assert opts["yAxis"]["type"] == "value"
            series_data = opts["series"][0]["data"]
            assert len(series_data) == 3
            # Each entry should be [x, y]
            assert series_data[0] == [1, 10]
            assert series_data[1] == [2, 20]
            assert series_data[2] == [3, 30]
        finally:
            os.unlink(path)

    def test_chart_with_title(self, init_teide):
        csv_data = "x,y\n1,10\n"
        table, path = _make_table(init_teide, csv_data)
        try:
            node = ChartNode()
            result = node.execute(
                {"df": table, "columns": table.columns},
                {
                    "chart_type": "bar",
                    "x_column": "x",
                    "y_column": "y",
                    "title": "My Chart",
                },
            )
            opts = result["options"]
            assert "title" in opts
            assert opts["title"]["text"] == "My Chart"
        finally:
            os.unlink(path)

    def test_missing_column(self, init_teide):
        csv_data = "x,y\n1,10\n"
        table, path = _make_table(init_teide, csv_data)
        try:
            node = ChartNode()
            with pytest.raises(ValueError, match="not found"):
                node.execute(
                    {"df": table, "columns": table.columns},
                    {"chart_type": "bar", "x_column": "nonexistent", "y_column": "y"},
                )
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# ExportNode tests
# ---------------------------------------------------------------------------

class TestExportNode:
    def test_csv_export(self, init_teide):
        csv_data = "x,y\n1,10.5\n2,20.0\n3,30.5\n"
        table, src_path = _make_table(init_teide, csv_data)
        out_fd, out_path = tempfile.mkstemp(suffix=".csv")
        os.close(out_fd)
        try:
            node = ExportNode()
            result = node.execute(
                {"df": table, "columns": table.columns},
                {"format": "csv", "output_path": out_path},
            )
            assert result["format"] == "csv"
            assert result["path"] == out_path
            assert result["rows"] == 3
            assert result["size"] > 0

            # Read back and verify
            with open(out_path, "r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            assert len(rows) == 3
            assert set(rows[0].keys()) == {"x", "y"}
        finally:
            os.unlink(src_path)
            if os.path.exists(out_path):
                os.unlink(out_path)

    def test_json_export(self, init_teide):
        csv_data = "a,b\nhello,1\nworld,2\n"
        table, src_path = _make_table(init_teide, csv_data)
        out_fd, out_path = tempfile.mkstemp(suffix=".json")
        os.close(out_fd)
        try:
            node = ExportNode()
            result = node.execute(
                {"df": table, "columns": table.columns},
                {"format": "json", "output_path": out_path},
            )
            assert result["format"] == "json"
            assert result["path"] == out_path
            assert result["rows"] == 2
            assert result["size"] > 0

            # Read back and verify
            with open(out_path, "r") as f:
                rows = json.load(f)
            assert len(rows) == 2
            assert rows[0]["a"] == "hello"
            assert rows[1]["a"] == "world"
        finally:
            os.unlink(src_path)
            if os.path.exists(out_path):
                os.unlink(out_path)

    def test_export_missing_df(self):
        node = ExportNode()
        with pytest.raises(ValueError, match="No input dataframe"):
            node.execute({}, {"format": "csv", "output_path": "/tmp/test.csv"})
