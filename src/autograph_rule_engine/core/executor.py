"""
Executor: evaluates rules in topological order against an input payload.

Supports 4 action types:
  - COMPUTE   : arithmetic expression evaluated against the current context
  - LOOKUP    : threshold/range classification (ordered entries, first match wins)
  - AGGREGATE : sum / avg / min / max over a list of fields
  - DECISION  : multi-condition branches → categorical output

The executor maintains a "context" dict that grows as rules fire.
Starting context = raw payload fields.
Each rule adds its output(s) to the context.
Downstream rules read from this growing context.
"""

from __future__ import annotations
import logging
import math
import operator
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from simpleeval import SimpleEval

from .expressions import ALLOWED_FUNCTIONS
from .models import ActionType, Condition, OperatorType, Rule, RuleGraph

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
#  Operator map for condition evaluation                              #
# ------------------------------------------------------------------ #
_OPS = {
    OperatorType.GT:      operator.gt,
    OperatorType.GTE:     operator.ge,
    OperatorType.LT:      operator.lt,
    OperatorType.LTE:     operator.le,
    OperatorType.EQ:      operator.eq,
    OperatorType.NEQ:     operator.ne,
    # GT/GTE/LT/LTE string variants (since use_enum_values = True gives strings)
    "gt":     operator.gt,
    "gte":    operator.ge,
    "lt":     operator.lt,
    "lte":    operator.le,
    "eq":     operator.eq,
    "neq":    operator.ne,
}


@dataclass
class RuleResult:
    """Result of executing a single rule."""
    rule_id: str
    rule_name: str
    fired: bool                    # True if conditions passed and action ran
    skipped: bool = False          # True if conditions failed
    outputs: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    duration_ms: float = 0.0


@dataclass
class ExecutionResult:
    """Result of executing the full rule graph against a payload."""
    success: bool
    context: Dict[str, Any]        # full context after execution (payload + all rule outputs)
    rule_results: List[RuleResult] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    total_duration_ms: float = 0.0

    def get_output(self, field_name: str) -> Any:
        """Convenience: get a specific output field from the context."""
        return self.context.get(field_name)

    def get_leaf_outputs(self, graph: RuleGraph) -> Dict[str, Any]:
        """Return only the terminal (leaf rule) outputs."""
        leaf_outputs = {}
        for rule_id in graph.get_leaf_rules():
            rule = graph.rules[rule_id]
            for out_field in rule.outputs:
                if out_field in self.context:
                    leaf_outputs[out_field] = self.context[out_field]
        return leaf_outputs

    def summary(self) -> str:
        lines = [
            f"Execution {'SUCCESS' if self.success else 'FAILED'}",
            f"  Total duration : {self.total_duration_ms:.2f}ms",
            f"  Rules fired    : {sum(1 for r in self.rule_results if r.fired)}",
            f"  Rules skipped  : {sum(1 for r in self.rule_results if r.skipped)}",
            f"  Rules errored  : {sum(1 for r in self.rule_results if r.error)}",
        ]
        if self.errors:
            lines.append("  Errors:")
            for e in self.errors:
                lines.append(f"    ✗ {e}")
        lines.append("  Rule trace:")
        for rr in self.rule_results:
            status = "✓ FIRED" if rr.fired else ("⚡ SKIP" if rr.skipped else "✗ ERROR")
            outputs_str = str(rr.outputs) if rr.outputs else ""
            lines.append(f"    [{status}] {rr.rule_id} ({rr.rule_name}) {outputs_str}")
        return "\n".join(lines)


class RuleExecutor:
    """
    Executes a validated RuleGraph against an input payload.

    Usage:
        executor = RuleExecutor()
        result = executor.execute(graph, payload={"loan_amount": 250000, ...})
        print(result.summary())
        print(result.get_leaf_outputs(graph))
    """

    def __init__(self, fail_fast: bool = False):
        """
        Args:
            fail_fast: If True, stop execution on first rule error.
                       If False (default), log errors and continue.
        """
        self.fail_fast = fail_fast

    def execute(self, graph: RuleGraph, payload: Dict[str, Any]) -> ExecutionResult:
        """
        Execute all rules in topological order.

        Args:
            graph:   A built and validated RuleGraph.
            payload: Input data dict (the "raw" fields).

        Returns:
            ExecutionResult with full context and per-rule results.
        """
        start_total = time.perf_counter()

        # Context starts as a copy of the payload
        context: Dict[str, Any] = dict(payload)

        rule_results: List[RuleResult] = []
        errors: List[str] = []
        success = True

        for rule_id in graph.execution_order:
            rule = graph.rules[rule_id]

            if not rule.enabled:
                logger.debug(f"Skipping disabled rule: {rule_id}")
                continue

            start_rule = time.perf_counter()
            rr = self._execute_rule(rule, context)
            rr.duration_ms = (time.perf_counter() - start_rule) * 1000

            rule_results.append(rr)

            if rr.error:
                errors.append(f"Rule {rule_id}: {rr.error}")
                if self.fail_fast:
                    success = False
                    break
            elif rr.fired:
                # Merge rule outputs into context
                context.update(rr.outputs)

        total_duration_ms = (time.perf_counter() - start_total) * 1000

        if errors:
            success = False

        return ExecutionResult(
            success=success,
            context=context,
            rule_results=rule_results,
            errors=errors,
            total_duration_ms=total_duration_ms,
        )

    def _execute_rule(self, rule: Rule, context: Dict[str, Any]) -> RuleResult:
        """Execute a single rule against the current context."""
        # Check pre-conditions
        try:
            for condition in rule.conditions:
                if not self._evaluate_condition(condition, context):
                    logger.debug(
                        f"Rule '{rule.rule_id}' skipped: condition not met — "
                        f"{condition.field} {condition.op} {condition.value}"
                    )
                    return RuleResult(
                        rule_id=rule.rule_id,
                        rule_name=rule.name,
                        fired=False,
                        skipped=True,
                    )
        except Exception as e:
            return RuleResult(
                rule_id=rule.rule_id,
                rule_name=rule.name,
                fired=False,
                error=f"Condition evaluation error: {e}",
            )

        # Execute action
        try:
            outputs = self._execute_action(rule, context)
            logger.debug(f"Rule '{rule.rule_id}' fired → outputs: {outputs}")
            return RuleResult(
                rule_id=rule.rule_id,
                rule_name=rule.name,
                fired=True,
                outputs=outputs,
            )
        except Exception as e:
            logger.error(f"Rule '{rule.rule_id}' action error: {e}")
            return RuleResult(
                rule_id=rule.rule_id,
                rule_name=rule.name,
                fired=False,
                error=f"Action execution error: {e}",
            )

    def _evaluate_condition(self, condition: Condition, context: Dict[str, Any]) -> bool:
        """Evaluate a single condition against the context."""
        op_str = condition.op if isinstance(condition.op, str) else condition.op.value
        field_val = context.get(condition.field)

        if field_val is None:
            raise ValueError(
                f"Condition references field '{condition.field}' which is not in context."
            )

        if op_str == "in":
            return field_val in condition.value
        elif op_str == "not_in":
            return field_val not in condition.value
        elif op_str == "between":
            lo, hi = condition.value[0], condition.value[1]
            return lo <= field_val <= hi
        else:
            op_fn = _OPS.get(op_str)
            if op_fn is None:
                raise ValueError(f"Unknown operator: '{op_str}'")
            return op_fn(field_val, condition.value)

    def _execute_action(self, rule: Rule, context: Dict[str, Any]) -> Dict[str, Any]:
        """Dispatch to the appropriate action handler."""
        atype = rule.action.type if isinstance(rule.action.type, str) else rule.action.type.value

        if atype == "compute":
            return self._action_compute(rule, context)
        elif atype == "lookup":
            return self._action_lookup(rule, context)
        elif atype == "aggregate":
            return self._action_aggregate(rule, context)
        elif atype == "decision":
            return self._action_decision(rule, context)
        else:
            raise ValueError(f"Unknown action type: '{atype}'")

    # ------------------------------------------------------------------ #
    #  Action handlers                                                     #
    # ------------------------------------------------------------------ #

    def _action_compute(self, rule: Rule, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate an arithmetic expression using the context as variables.
        Uses simpleeval for safe expression evaluation (no exec/eval).

        Example expression: "loan_amount / property_value"
        """
        expression = rule.action.expression
        try:
            # An explicit function table, rather than simpleeval's defaults, which include
            # rand() and randint() — a rule able to call those produces a different result
            # for the same payload on each run, which a decision engine cannot audit.
            # SimpleEval rather than EvalWithCompoundTypes: no rule uses list/dict/set
            # literals, so this also drops those constructors from the callable surface.
            evaluator = SimpleEval(names=context, functions=ALLOWED_FUNCTIONS)
            result = evaluator.eval(expression)
        except Exception as e:
            raise ValueError(f"Expression '{expression}' failed: {e}")

        # Map result to all output fields (typically one for compute)
        outputs = {}
        for out_field in rule.outputs:
            outputs[out_field] = result
        return outputs

    def _action_lookup(self, rule: Rule, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Lookup / classification: evaluate ordered entries, first match wins.
        Falls back to default_result if no entry matches.

        Example: ltv_ratio → "LOW" / "MEDIUM" / "HIGH"
        """
        entries = rule.action.entries
        default = rule.action.default_result

        # Evaluation errors are NOT caught here. A missing field, an unknown operator or an
        # incomparable type used to be indistinguishable from an entry that simply did not
        # match, so the rule fell through to default_result and reported fired=True — a
        # wrong answer presented as a successful one. Letting the error propagate turns it
        # into a rule error, which _execute_rule records and which flips `success`.
        result = default  # fallback when no entry matches
        for entry in entries:
            if self._evaluate_condition(entry.condition, context):
                result = entry.result
                break

        if result is None:
            raise ValueError(
                f"Lookup rule '{rule.rule_id}' found no matching entry and has no default_result."
            )

        outputs = {}
        for out_field in rule.outputs:
            outputs[out_field] = result
        return outputs

    def _action_aggregate(self, rule: Rule, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Aggregate: apply agg_func over a list of field values.

        Example: sum of [ltv_rate_adj, credit_rate_adj, dti_rate_adj] + base_rate
        """
        agg_func = rule.action.agg_func
        fields = rule.action.fields

        values = []
        for f in fields:
            val = context.get(f)
            if val is None:
                raise ValueError(f"Aggregate field '{f}' not found in context.")
            # bool is a subclass of int, so float(True) is 1.0. Aggregating a boolean is
            # almost always a modelling mistake, and silently turning it into a number
            # hides that; a decision rule is the right tool for a boolean.
            if isinstance(val, bool):
                raise ValueError(
                    f"Aggregate field '{f}' is a boolean ({val!r}); "
                    "use a decision or lookup rule instead of aggregating it."
                )
            try:
                values.append(float(val))
            except (TypeError, ValueError):
                raise ValueError(
                    f"Aggregate field '{f}' is not numeric: {val!r} ({type(val).__name__})."
                )

        if agg_func == "sum":
            result = sum(values)
        elif agg_func == "avg":
            result = sum(values) / len(values) if values else 0.0
        elif agg_func == "min":
            result = min(values)
        elif agg_func == "max":
            result = max(values)
        elif agg_func == "count":
            result = len(values)
        else:
            raise ValueError(f"Unknown agg_func: '{agg_func}'")

        outputs = {}
        for out_field in rule.outputs:
            outputs[out_field] = result
        return outputs

    def _action_decision(self, rule: Rule, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Decision: evaluate branches in order; first branch where ALL conditions
        hold wins. Falls back to default_result.

        Example:
          if is_eligible == True AND risk_score <= 30 → "APPROVED"
          if is_eligible == False OR risk_score > 70  → "DECLINED"
          default → "REVIEW"
        """
        branches = rule.action.branches
        default = rule.action.default_result

        # Every condition in a branch is evaluated, and errors are not caught.
        #
        # Previously the whole `all(...)` generator sat inside one try/except, so whether a
        # broken condition was noticed depended on whether an earlier condition happened to
        # be False and short-circuited past it — the same rule could silently drop a branch
        # for one payload and evaluate it for another. Building the list first removes that
        # order dependence; letting errors propagate turns a broken condition into a rule
        # error rather than a silent fall-through to default_result.
        result = default
        for branch in branches:
            outcomes = [
                self._evaluate_condition(cond, context)
                for cond in branch.conditions
            ]
            if all(outcomes):
                result = branch.result
                break

        if result is None:
            raise ValueError(
                f"Decision rule '{rule.rule_id}' matched no branch and has no default_result."
            )

        outputs = {}
        for out_field in rule.outputs:
            outputs[out_field] = result
        return outputs