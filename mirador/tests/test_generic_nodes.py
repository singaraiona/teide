"""Tests for Formula, Script, DictTransform, and Conditional nodes."""

import os
import tempfile
import pytest
from teide.api import Table

from mirador.nodes.compute.formula import FormulaNode
from mirador.nodes.generic.script import ScriptNode
from mirador.nodes.generic.dict_transform import DictTransformNode
from mirador.nodes.generic.conditional import ConditionalNode


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_table(lib, csv_text):
    """Write CSV text to a temp file, read it into a Teide Table."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(csv_text)
        path = f.name
    tbl_ptr = lib.read_csv(path)
    table = Table(lib, tbl_ptr)
    os.unlink(path)
    return table


# ---------------------------------------------------------------------------
# FormulaNode
# ---------------------------------------------------------------------------

class TestFormulaNode:
    def test_simple_addition(self, init_teide):
        table = _make_table(init_teide, "x,y\n1,10\n2,20\n3,30\n")
        node = FormulaNode()
        result = node.execute(
            {"df": table, "columns": table.columns},
            {"expression": "x + y", "output_name": "total"},
        )
        assert result["rows"] == 3
        assert "total" in result["columns"]
        assert result["extra_columns"]["total"] == [11, 22, 33]

    def test_subtraction(self, init_teide):
        table = _make_table(init_teide, "revenue,cost\n100,40\n200,80\n300,50\n")
        node = FormulaNode()
        result = node.execute(
            {"df": table, "columns": table.columns},
            {"expression": "revenue - cost", "output_name": "profit"},
        )
        assert result["extra_columns"]["profit"] == [60, 120, 250]

    def test_multiplication(self, init_teide):
        table = _make_table(init_teide, "a,b\n2,3\n4,5\n")
        node = FormulaNode()
        result = node.execute(
            {"df": table, "columns": table.columns},
            {"expression": "a * b", "output_name": "product"},
        )
        assert result["extra_columns"]["product"] == [6, 20]

    def test_complex_expression(self, init_teide):
        table = _make_table(init_teide, "x,y\n10,3\n20,7\n")
        node = FormulaNode()
        result = node.execute(
            {"df": table, "columns": table.columns},
            {"expression": "(x + y) * 2", "output_name": "doubled"},
        )
        assert result["extra_columns"]["doubled"] == [26, 54]

    def test_unary_negation(self, init_teide):
        table = _make_table(init_teide, "x\n5\n-3\n")
        node = FormulaNode()
        result = node.execute(
            {"df": table, "columns": table.columns},
            {"expression": "-x", "output_name": "neg"},
        )
        assert result["extra_columns"]["neg"] == [-5, 3]

    def test_default_output_name(self, init_teide):
        table = _make_table(init_teide, "x\n1\n2\n")
        node = FormulaNode()
        result = node.execute(
            {"df": table, "columns": table.columns},
            {"expression": "x + 1"},
        )
        assert "result" in result["columns"]
        assert result["extra_columns"]["result"] == [2, 3]

    def test_rejects_function_call(self, init_teide):
        table = _make_table(init_teide, "x\n1\n")
        node = FormulaNode()
        with pytest.raises(ValueError, match="Unsupported"):
            node.execute(
                {"df": table, "columns": table.columns},
                {"expression": "abs(x)"},
            )

    def test_rejects_string_literal(self, init_teide):
        table = _make_table(init_teide, "x\n1\n")
        node = FormulaNode()
        with pytest.raises(ValueError, match="Only numeric"):
            node.execute(
                {"df": table, "columns": table.columns},
                {"expression": "x + 'bad'"},
            )

    def test_passes_through_table(self, init_teide):
        table = _make_table(init_teide, "x\n1\n")
        node = FormulaNode()
        result = node.execute(
            {"df": table, "columns": table.columns},
            {"expression": "x * 2", "output_name": "doubled"},
        )
        # Original table reference is preserved
        assert result["df"] is table


# ---------------------------------------------------------------------------
# ScriptNode
# ---------------------------------------------------------------------------

class TestScriptNode:
    def test_simple_output(self):
        node = ScriptNode()
        result = node.execute(
            {"value": 42},
            {"code": "output = {'doubled': input['value'] * 2}"},
        )
        assert result == {"doubled": 84}

    def test_list_processing(self):
        node = ScriptNode()
        result = node.execute(
            {"data": [1, 2, 3, 4, 5]},
            {"code": "output = {'total': sum(input['data']), 'count': len(input['data'])}"},
        )
        assert result == {"total": 15, "count": 5}

    def test_loop_in_script(self):
        node = ScriptNode()
        result = node.execute(
            {"items": [10, 20, 30]},
            {"code": "s = 0\nfor x in input['items']:\n    s += x\noutput = {'sum': s}"},
        )
        assert result == {"sum": 60}

    def test_builtins_available(self):
        node = ScriptNode()
        result = node.execute(
            {"data": [3, 1, 4, 1, 5]},
            {"code": "output = {'sorted': sorted(input['data']), 'max': max(input['data'])}"},
        )
        assert result == {"sorted": [1, 1, 3, 4, 5], "max": 5}

    def test_output_must_be_dict(self):
        node = ScriptNode()
        with pytest.raises(TypeError, match="must be a dict"):
            node.execute({}, {"code": "output = 42"})

    def test_restricted_builtins(self):
        node = ScriptNode()
        # __import__ is not available
        with pytest.raises(Exception):
            node.execute({}, {"code": "import os\noutput = {}"})

    def test_empty_input(self):
        node = ScriptNode()
        result = node.execute({}, {"code": "output = {'empty': True}"})
        assert result == {"empty": True}


# ---------------------------------------------------------------------------
# DictTransformNode
# ---------------------------------------------------------------------------

class TestDictTransformNode:
    def test_rename(self):
        node = DictTransformNode()
        result = node.execute(
            {"old_name": 1, "keep": 2},
            {"rename": {"old_name": "new_name"}},
        )
        assert result == {"new_name": 1, "keep": 2}

    def test_pick(self):
        node = DictTransformNode()
        result = node.execute(
            {"a": 1, "b": 2, "c": 3},
            {"pick": ["a", "c"]},
        )
        assert result == {"a": 1, "c": 3}

    def test_drop(self):
        node = DictTransformNode()
        result = node.execute(
            {"a": 1, "b": 2, "c": 3},
            {"drop": ["b"]},
        )
        assert result == {"a": 1, "c": 3}

    def test_drop_then_rename(self):
        node = DictTransformNode()
        result = node.execute(
            {"x": 10, "y": 20, "z": 30},
            {"drop": ["z"], "rename": {"x": "alpha"}},
        )
        assert result == {"alpha": 10, "y": 20}

    def test_pick_then_rename(self):
        node = DictTransformNode()
        result = node.execute(
            {"a": 1, "b": 2, "c": 3},
            {"pick": ["a", "b"], "rename": {"a": "x"}},
        )
        assert result == {"x": 1, "b": 2}

    def test_empty_config(self):
        node = DictTransformNode()
        result = node.execute({"a": 1, "b": 2}, {})
        assert result == {"a": 1, "b": 2}

    def test_drop_nonexistent_key(self):
        node = DictTransformNode()
        result = node.execute(
            {"a": 1},
            {"drop": ["nonexistent"]},
        )
        assert result == {"a": 1}

    def test_rename_nonexistent_key(self):
        node = DictTransformNode()
        result = node.execute(
            {"a": 1},
            {"rename": {"nonexistent": "new"}},
        )
        assert result == {"a": 1}


# ---------------------------------------------------------------------------
# ConditionalNode
# ---------------------------------------------------------------------------

class TestConditionalNode:
    def test_gt_true(self):
        node = ConditionalNode()
        result = node.execute(
            {"rows": 200, "data": "stuff"},
            {"field": "rows", "operator": "gt", "value": 100},
        )
        assert result["branch"] == "true"
        assert result["condition_met"] is True
        # Original data is preserved
        assert result["data"] == "stuff"

    def test_gt_false(self):
        node = ConditionalNode()
        result = node.execute(
            {"rows": 50},
            {"field": "rows", "operator": "gt", "value": 100},
        )
        assert result["branch"] == "false"
        assert result["condition_met"] is False

    def test_eq(self):
        node = ConditionalNode()
        result = node.execute(
            {"status": "ok"},
            {"field": "status", "operator": "eq", "value": "ok"},
        )
        assert result["condition_met"] is True

    def test_ne(self):
        node = ConditionalNode()
        result = node.execute(
            {"status": "error"},
            {"field": "status", "operator": "ne", "value": "ok"},
        )
        assert result["condition_met"] is True

    def test_lt(self):
        node = ConditionalNode()
        result = node.execute(
            {"count": 5},
            {"field": "count", "operator": "lt", "value": 10},
        )
        assert result["condition_met"] is True

    def test_ge(self):
        node = ConditionalNode()
        result = node.execute(
            {"count": 10},
            {"field": "count", "operator": "ge", "value": 10},
        )
        assert result["condition_met"] is True

    def test_le(self):
        node = ConditionalNode()
        result = node.execute(
            {"count": 10},
            {"field": "count", "operator": "le", "value": 10},
        )
        assert result["condition_met"] is True

    def test_missing_field_raises(self):
        node = ConditionalNode()
        with pytest.raises(KeyError, match="not found"):
            node.execute(
                {"other": 1},
                {"field": "missing", "operator": "eq", "value": 0},
            )

    def test_unknown_operator_raises(self):
        node = ConditionalNode()
        with pytest.raises(ValueError, match="Unknown operator"):
            node.execute(
                {"x": 1},
                {"field": "x", "operator": "invalid", "value": 0},
            )

    def test_string_value_coercion(self):
        """When actual is int and value is string, coerce value to int."""
        node = ConditionalNode()
        result = node.execute(
            {"count": 200},
            {"field": "count", "operator": "gt", "value": "100"},
        )
        assert result["condition_met"] is True
