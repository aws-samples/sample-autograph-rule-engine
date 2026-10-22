"""
Shared handling of `compute` action expressions.

Two things live here because both the model layer and the executor need them:

  - ALLOWED_FUNCTIONS: the only functions callable from a rule expression. simpleeval's
    defaults include rand() and randint(), which make a rule non-deterministic — the same
    payload can produce a different decision on each run. For a decision engine that is an
    auditability problem, so the function table is declared explicitly instead.

  - expression_field_names(): the field names an expression reads, so a rule can be checked
    at parse time against its declared `inputs`. A field read without being declared creates
    no graph edge, which means no ordering guarantee relative to the rule that produces it.
"""

from __future__ import annotations
import ast
import math
from typing import Any, Callable, Dict, Set

ALLOWED_FUNCTIONS: Dict[str, Callable[..., Any]] = {
    "abs": abs,
    "round": round,
    "min": min,
    "max": max,
    "floor": math.floor,
    "ceil": math.ceil,
    "sqrt": math.sqrt,
    "int": int,
    "float": float,
    "str": str,
}


def expression_field_names(expression: str) -> Set[str]:
    """
    Field names an expression reads, excluding allowed function names.

    Raises ValueError if the expression cannot be parsed, so a malformed expression is
    rejected at parse time rather than at execution time.
    """
    try:
        tree = ast.parse(expression, mode="eval")
    except SyntaxError as exc:
        raise ValueError(f"expression is not parseable: {exc}") from exc

    return {
        node.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Name) and node.id not in ALLOWED_FUNCTIONS
    }
