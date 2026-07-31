"""
Rule Parser: loads and validates rule JSON files into Rule objects.
Supports loading from a single file, a directory, or a list of dicts.
"""

from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import Dict, List, Union

from pydantic import ValidationError

from .models import Rule

logger = logging.getLogger(__name__)


class RuleParseError(Exception):
    """Raised when a rule JSON fails to parse or validate."""
    pass


class RuleParser:
    """
    Loads rule definitions from JSON and validates them against the Rule schema.

    Usage:
        parser = RuleParser()
        rules = parser.load_directory("rules/loan_pricing/")
        rules = parser.load_file("rules/loan_pricing/R01_compute_ltv.json")
        rules = parser.load_dicts([{...}, {...}])
    """

    def load_file(self, path: Union[str, Path]) -> Rule:
        """Load a single rule from a JSON file."""
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Rule file not found: {path}")
        if not path.suffix == ".json":
            raise RuleParseError(f"Expected .json file, got: {path.suffix}")

        with open(path, "r") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError as e:
                raise RuleParseError(f"Invalid JSON in {path}: {e}")

        return self._parse_dict(data, source=str(path))

    def load_directory(self, directory: Union[str, Path]) -> Dict[str, Rule]:
        """
        Load all .json rule files from a directory.
        Returns a dict of rule_id → Rule.
        Skips disabled rules by default.
        """
        directory = Path(directory)
        if not directory.exists():
            raise FileNotFoundError(f"Rules directory not found: {directory}")

        rules: Dict[str, Rule] = {}
        json_files = sorted(directory.glob("*.json"))

        if not json_files:
            logger.warning(f"No .json rule files found in {directory}")
            return rules

        errors = []
        for fp in json_files:
            try:
                rule = self.load_file(fp)
                if not rule.enabled:
                    logger.info(f"Skipping disabled rule: {rule.rule_id} ({rule.name})")
                    continue
                if rule.rule_id in rules:
                    raise RuleParseError(
                        f"Duplicate rule_id '{rule.rule_id}' found in {fp} "
                        f"(already loaded from another file)"
                    )
                rules[rule.rule_id] = rule
                logger.debug(f"Loaded rule: {rule.rule_id} — {rule.name}")
            except (RuleParseError, FileNotFoundError) as e:
                errors.append(str(e))

        if errors:
            raise RuleParseError(
                f"Failed to load {len(errors)} rule(s):\n" + "\n".join(f"  - {e}" for e in errors)
            )

        logger.info(f"Loaded {len(rules)} rules from {directory}")
        return rules

    def load_dicts(self, rule_dicts: List[dict]) -> Dict[str, Rule]:
        """
        Load rules from a list of plain Python dicts (e.g., from an API or database).
        Returns a dict of rule_id → Rule.
        """
        rules: Dict[str, Rule] = {}
        errors = []

        for i, d in enumerate(rule_dicts):
            try:
                rule = self._parse_dict(d, source=f"dict[{i}]")
                if rule.rule_id in rules:
                    raise RuleParseError(f"Duplicate rule_id '{rule.rule_id}' at index {i}")
                rules[rule.rule_id] = rule
            except RuleParseError as e:
                errors.append(str(e))

        if errors:
            raise RuleParseError(
                f"Failed to parse {len(errors)} rule dict(s):\n" + "\n".join(f"  - {e}" for e in errors)
            )

        return rules

    def _parse_dict(self, data: dict, source: str = "unknown") -> Rule:
        """Parse and validate a single rule dict."""
        try:
            rule = Rule(**data)
        except ValidationError as e:
            # Format Pydantic errors into a clean message
            error_details = "; ".join(
                f"{' -> '.join(str(loc) for loc in err['loc'])}: {err['msg']}"
                for err in e.errors()
            )
            raise RuleParseError(f"Validation error in rule from {source}: {error_details}")
        except TypeError as e:
            raise RuleParseError(f"Type error parsing rule from {source}: {e}")
        return rule

    def to_json(self, rule: Rule, indent: int = 2) -> str:
        """Serialize a Rule back to a JSON string."""
        return rule.json(indent=indent)

    def to_dict(self, rule: Rule) -> dict:
        """Serialize a Rule to a plain dict."""
        return rule.dict()

    def save_file(self, rule: Rule, path: Union[str, Path]):
        """Save a Rule to a JSON file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            f.write(self.to_json(rule))
        logger.info(f"Saved rule {rule.rule_id} to {path}")