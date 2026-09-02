from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import great_expectations as gx
import pandas as pd
from great_expectations.expectations import (
    ExpectColumnValuesToBeInSet,
    ExpectColumnValuesToBeJsonParseable,
    ExpectColumnValuesToNotBeNull,
    ExpectCompoundColumnsToBeUnique,
    ExpectTableColumnsToMatchSet,
    ExpectTableRowCountToBeBetween,
)

BRONZE_TO_SILVER_SUITE_NAME = "bronze_to_silver_cdc_events"
REQUIRED_BRONZE_COLUMNS = {
    "ingest_id",
    "ingested_at",
    "ingest_date",
    "topic",
    "kafka_partition",
    "kafka_offset",
    "source_schema",
    "source_table",
    "operation",
    "event_ts_ms",
    "source_lsn",
    "kafka_key",
    "debezium_value",
}
ALLOWED_SOURCE_TABLES = {"stores", "products", "promotions", "sales_transactions"}
ALLOWED_OPERATIONS = {"r", "c", "u", "d"}


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


def validate_bronze_events(df: pd.DataFrame) -> DataQualityResult:
    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_pandas("pandas")
    asset = datasource.add_dataframe_asset(name="bronze_cdc_events")
    batch_definition = asset.add_batch_definition_whole_dataframe("whole_dataframe")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    suite = build_bronze_to_silver_suite(context)
    validator = context.get_validator(
        batch=batch,
        expectation_suite=suite,
    )

    raw_result = validator.validate()
    result = quality_result_from_gx(raw_result)
    if not result.success:
        joined_failures = "; ".join(result.failure_messages)
        raise DataQualityError(
            f"Great Expectations suite {result.suite_name!r} failed: {joined_failures}"
        )
    return result


def build_bronze_to_silver_suite(context: Any | None = None) -> gx.ExpectationSuite:
    context = context or gx.get_context(mode="ephemeral")
    suite = gx.ExpectationSuite(name=BRONZE_TO_SILVER_SUITE_NAME)
    add_bronze_to_silver_expectations(suite)
    return suite


def add_bronze_to_silver_expectations(suite: gx.ExpectationSuite) -> None:
    suite.add_expectation(
        ExpectTableColumnsToMatchSet(
            column_set=REQUIRED_BRONZE_COLUMNS,
            exact_match=False,
        )
    )
    suite.add_expectation(ExpectTableRowCountToBeBetween(min_value=1))

    for column in REQUIRED_BRONZE_COLUMNS:
        if column == "source_lsn":
            continue
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column=column))

    suite.add_expectation(
        ExpectCompoundColumnsToBeUnique(
            column_list=["topic", "kafka_partition", "kafka_offset"],
            ignore_row_if="any_value_is_missing",
        )
    )
    suite.add_expectation(ExpectColumnValuesToBeInSet(column="source_schema", value_set=["retail"]))
    suite.add_expectation(
        ExpectColumnValuesToBeInSet(
            column="source_table",
            value_set=sorted(ALLOWED_SOURCE_TABLES),
        )
    )
    suite.add_expectation(
        ExpectColumnValuesToBeInSet(column="operation", value_set=sorted(ALLOWED_OPERATIONS))
    )
    suite.add_expectation(ExpectColumnValuesToBeJsonParseable(column="kafka_key"))
    suite.add_expectation(ExpectColumnValuesToBeJsonParseable(column="debezium_value"))


def quality_result_from_gx(raw_result: Any) -> DataQualityResult:
    statistics = raw_result.statistics
    failure_messages: list[str] = []
    for result in raw_result.results:
        if result.success:
            continue
        expectation_type = result.expectation_config.type
        failure_messages.append(expectation_type)

    return DataQualityResult(
        suite_name=BRONZE_TO_SILVER_SUITE_NAME,
        success=bool(raw_result.success),
        evaluated_expectations=statistics["evaluated_expectations"],
        successful_expectations=statistics["successful_expectations"],
        unsuccessful_expectations=statistics["unsuccessful_expectations"],
        failure_messages=tuple(failure_messages),
    )
