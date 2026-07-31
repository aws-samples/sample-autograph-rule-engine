"""
Core data models for the Rule Engine.
Rules are defined as JSON and parsed into these Pydantic models.
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Union
from enum import Enum
from pydantic import BaseModel, Field, validator


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
    value: Union[float, int, str, List[Any]]

    class Config:
        use_enum_values = True


class LookupEntry(BaseModel):
    """One row in a lookup table: if field satisfies condition → assign label."""
    condition: Condition
    result: Union[str, float, int]


class DecisionBranch(BaseModel):
    """A branch in a decision rule: all conditions must hold → result."""
    conditions: List[Condition]
    result: Union[str, float, int]


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
    default_result: Optional[Union[str, float, int]] = None
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