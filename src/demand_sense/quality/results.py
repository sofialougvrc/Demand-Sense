from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DataQualityResult:
    suite_name: str
    success: bool
    evaluated_expectations: int
    successful_expectations: int
    unsuccessful_expectations: int
    failure_messages: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "suite_name": self.suite_name,
            "success": self.success,
            "evaluated_expectations": self.evaluated_expectations,
            "successful_expectations": self.successful_expectations,
            "unsuccessful_expectations": self.unsuccessful_expectations,
            "failure_messages": list(self.failure_messages),
        }


class DataQualityError(RuntimeError):
    """Raised when a Great Expectations data quality gate fails."""
