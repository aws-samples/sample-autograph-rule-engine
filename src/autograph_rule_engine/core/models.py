"""
Core data models for the Rule Engine.
Rules are defined as JSON and parsed into these Pydantic models.
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Union
from enum import Enum
from pydantic import BaseModel, Field, model_validator, validator

from .expressions import expression_field_names

# `bool` must be a member: with a plain Union[str, float, int], JSON `false` is coerced to
# 0.0, so a boolean field silently became a float. The shipped rules only worked because
# `0.0 == False` happens to be true in Python. Pydantic's smart union matches exact types
# first, so including bool is sufficient — no Strict* types needed.
ResultValue = Union[bool, str, float, int]


class OperatorType(str, Enum):
    GT = "gt"           # greater than
    GTE = "gte"         # greater than or equal
    LT = "lt"           # less than
    LTE = "lte"         # less than or equal
    EQ = "eq"           # equal
    NEQ = "neq"         # not equal
    IN = "in"           # in list
    NOT_IN = "not_in"   # not in list
    BETWEEN = "between" # between two values


class ActionType(str, Enum):
    COMPUTE = "compute"     # arithmetic expression: loan_amount / property_value
    LOOKUP = "lookup"       # threshold/range based classification
    AGGREGATE = "aggregate" # sum/avg multiple fields
    DECISION = "decision"   # multi-condition → categorical output


class Condition(BaseModel):
    """A single condition that must be true for a rule to fire."""
    field: str
    op: OperatorType
    value: Union[bool, str, float, int, List[Any]]

    class Config:
        use_enum_values = True

    @model_validator(mode="after")
    def value_matches_operator(self):
        """
        Reject operator/value combinations that would fail at execution time.

        A malformed `between` used to raise IndexError inside the action handler, where it
        was swallowed and became an ordinary non-match.
        """
        op = self.op if isinstance(self.op, str) else self.op.value

        if op in ("in", "not_in"):
            if not isinstance(self.value, list):
                raise ValueError(f"operator '{op}' requires a list value, got {type(self.value).__name__}")
        elif op == "between":
            if not isinstance(self.value, list) or len(self.value) != 2:
                raise ValueError("operator 'between' requires a list of exactly two values")
        elif isinstance(self.value, list):
            raise ValueError(f"operator '{op}' does not accept a list value")

        return self


class LookupEntry(BaseModel):
    """One row in a lookup table: if field satisfies condition → assign label."""
    condition: Condition
    result: ResultValue


class DecisionBranch(BaseModel):
    """A branch in a decision rule: all conditions must hold → result."""
    conditions: List[Condition]
    result: ResultValue


class Action(BaseModel):
    """
    What the rule does when its conditions pass.

    action_type determines which fields are used:
      - COMPUTE:   expression (string formula using input field names)
      - LOOKUP:    entries (list of LookupEntry), default_result
      - AGGREGATE: agg_func ("sum" | "avg" | "min" | "max"), fields
      - DECISION:  branches (list of DecisionBranch), default_result
    """
    type: ActionType
    # COMPUTE
    expression: Optional[str] = None
    # LOOKUP
    entries: Optional[List[LookupEntry]] = None
    default_result: Optional[ResultValue] = None
    # AGGREGATE
    agg_func: Optional[str] = None   # sum, avg, min, max
    fields: Optional[List[str]] = None
    # DECISION
    branches: Optional[List[DecisionBranch]] = None

    class Config:
        use_enum_values = True


class Rule(BaseModel):
    """
    A single rule node in the graph.

    inputs  : fields this rule reads  (from payload OR other rules' outputs)
    outputs : fields this rule writes (available to downstream rules)
    The graph is built by matching outputs of one rule to inputs of another.
    """
    rule_id: str
    name: str
    description: Optional[str] = ""
    inputs: List[str] = Field(..., description="Input field names this rule consumes")
    outputs: List[str] = Field(..., description="Output field names this rule produces")
    conditions: List[Condition] = Field(default_factory=list,
                                        description="Pre-conditions; if any fails, rule is skipped")
    action: Action
    priority: int = Field(default=0, description="Higher priority rules run first at same topo level")
    tags: List[str] = Field(default_factory=list, description="Optional tags for filtering")
    enabled: bool = True

    class Config:
        use_enum_values = True

    @validator("outputs")
    def outputs_not_empty(cls, v):
        if not v:
            raise ValueError("A rule must declare at least one output field")
        return v

    @validator("inputs")
    def inputs_not_empty(cls, v):
        if not v:
            raise ValueError("A rule must declare at least one input field")
        return v

    @model_validator(mode="after")
    def reads_are_declared(self):
        """
        Every field this rule reads must be declared in `inputs`.

        An undeclared read creates no graph edge, so the rule has no ordering guarantee
        relative to whichever rule produces that field — it can evaluate before its
        producer has run. A typo also used to become a brand-new payload field, surfacing
        much later as a caller-facing "payload missing required fields" error rather than
        an authoring mistake.
        """
        action = self.action
        atype = action.type if isinstance(action.type, str) else action.type.value

        reads = {c.field for c in self.conditions}

        if atype == "compute" and action.expression:
            reads |= expression_field_names(action.expression)
        elif atype == "aggregate" and action.fields:
            reads |= set(action.fields)
        elif atype == "lookup" and action.entries:
            reads |= {entry.condition.field for entry in action.entries}
        elif atype == "decision" and action.branches:
            reads |= {c.field for branch in action.branches for c in branch.conditions}

        undeclared = sorted(reads - set(self.inputs))
        if undeclared:
            raise ValueError(
                f"reads undeclared field(s) {undeclared}; add them to `inputs` so the "
                "dependency graph can order this rule after the rules that produce them"
            )
        return self

    @model_validator(mode="after")
    def single_output_per_rule(self):
        """
        Every action type produces one value, so only one output may be declared.

        The action handlers copied that single result into *every* declared output, so a
        rule declaring two outputs silently received the same value twice — which is how
        `decision_reason` ended up duplicating `decision`.
        """
        if len(self.outputs) > 1:
            atype = self.action.type if isinstance(self.action.type, str) else self.action.type.value
            raise ValueError(
                f"declares {len(self.outputs)} outputs {self.outputs}, but a '{atype}' "
                "action produces a single value; declare one output per rule"
            )
        return self


class RuleGraph(BaseModel):
    """
    The complete graph of rules with their dependency edges.
    Built by GraphBuilder, validated by Validator, executed by Executor.
    """
    rules: Dict[str, Rule] = Field(default_factory=dict)      # rule_id → Rule
    edges: Dict[str, List[str]] = Field(default_factory=dict)  # rule_id → [dependent rule_ids]
    reverse_edges: Dict[str, List[str]] = Field(default_factory=dict)  # rule_id → [dependency rule_ids]
    payload_fields: List[str] = Field(default_factory=list)    # fields available from raw payload
    execution_order: List[str] = Field(default_factory=list)   # topological order

    def add_rule(self, rule: Rule):
        self.rules[rule.rule_id] = rule
        if rule.rule_id not in self.edges:
            self.edges[rule.rule_id] = []
        if rule.rule_id not in self.reverse_edges:
            self.reverse_edges[rule.rule_id] = []

    def add_edge(self, from_rule_id: str, to_rule_id: str):
        """from_rule produces output that to_rule consumes."""
        if to_rule_id not in self.edges[from_rule_id]:
            self.edges[from_rule_id].append(to_rule_id)
        if from_rule_id not in self.reverse_edges[to_rule_id]:
            self.reverse_edges[to_rule_id].append(from_rule_id)

    def get_root_rules(self) -> List[str]:
        """Rules with no upstream rule dependencies (inputs come purely from payload)."""
        return [rid for rid, deps in self.reverse_edges.items() if not deps]

    def get_leaf_rules(self) -> List[str]:
        """Rules that no other rule depends on (terminal outputs)."""
        return [rid for rid, deps in self.edges.items() if not deps]