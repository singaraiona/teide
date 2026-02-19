"""Formula node — adds a computed column via expression parsing."""

import ast
from typing import Any
from mirador.nodes.base import BaseNode, NodeMeta, NodePort


def _parse_formula(expression: str, columns: list[str]) -> ast.Expression:
    """Parse a formula string into an AST, validating safety."""
    tree = ast.parse(expression, mode='eval')
    _validate_ast(tree.body, columns)
    return tree


def _validate_ast(node: ast.AST, columns: list[str]) -> None:
    """Walk the AST and reject anything beyond arithmetic on columns/literals."""
    if isinstance(node, ast.Name):
        # Allow any identifier — will resolve against column namespace
        return
    elif isinstance(node, ast.Constant):
        if not isinstance(node.value, (int, float)):
            raise ValueError(f"Only numeric literals are allowed, got {type(node.value).__name__}")
        return
    elif isinstance(node, ast.BinOp):
        allowed_ops = (ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Mod)
        if not isinstance(node.op, allowed_ops):
            raise ValueError(f"Unsupported operator: {type(node.op).__name__}")
        _validate_ast(node.left, columns)
        _validate_ast(node.right, columns)
        return
    elif isinstance(node, ast.UnaryOp):
        if not isinstance(node.op, (ast.USub, ast.UAdd)):
            raise ValueError(f"Unsupported unary operator: {type(node.op).__name__}")
        _validate_ast(node.operand, columns)
        return
    else:
        raise ValueError(f"Unsupported expression element: {type(node).__name__}")


class FormulaNode(BaseNode):
    meta = NodeMeta(
        id="formula",
        label="Formula",
        category="compute",
        description="Add a computed column using a math expression",
        inputs=[NodePort(name="in", description="Input dataframe")],
        outputs=[NodePort(name="out", description="Dataframe with computed column")],
        config_schema={
            "type": "object",
            "properties": {
                "expression": {"type": "string", "title": "Expression",
                               "description": "e.g. revenue - cost"},
                "output_name": {"type": "string", "title": "Output Column Name",
                                "default": "result"},
            },
            "required": ["expression"],
        },
    )

    def execute(self, inputs: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        table = inputs["df"]
        expression = config["expression"]
        output_name = config.get("output_name", "result")

        columns = inputs.get("columns", table.columns if hasattr(table, 'columns') else [])
        data = table.to_dict()
        n = len(table)

        # Parse and validate the expression
        tree = _parse_formula(expression, columns)

        # Compile once, evaluate per row
        code = compile(tree, '<formula>', 'eval')
        result_col = []
        for i in range(n):
            row_ns = {k: v[i] for k, v in data.items()}
            row_ns["__builtins__"] = {}
            result_col.append(eval(code, row_ns))

        return {
            "df": table,
            "rows": n,
            "columns": columns + [output_name],
            "extra_columns": {output_name: result_col},
        }
