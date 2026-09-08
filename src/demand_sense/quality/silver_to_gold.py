from __future__ import annotations

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

from demand_sense.quality.results import DataQualityError, DataQualityResult

SILVER_TO_GOLD_SUITE_NAME = "silver_to_gold_retail_cdc_events"
REQUIRED_SILVER_COLUMNS = {
    "event_id",
    "record_key",
    "source_schema",
    "source_table",
    "operation",
    "operation_name",
    "is_snapshot",
    "is_deleted",
    "event_at",
    "event_date",
    "source_lsn",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "record_data",
    "ingested_at",
    "silver_loaded_at",
}
ALLOWED_SOURCE_TABLES = {"stores", "products", "promotions", "sales_transactions"}
ALLOWED_OPERATIONS = {"r", "c", "u", "d"}


def validate_silver_events(df: pd.DataFrame) -> DataQualityResult:
    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_pandas("pandas")
    asset = datasource.add_dataframe_asset(name="silver_cdc_events")
    batch_definition = asset.add_batch_definition_whole_dataframe("whole_dataframe")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    suite = build_silver_to_gold_suite(context)
    validator = context.get_validator(batch=batch, expectation_suite=suite)

    raw_result = validator.validate()
    result = quality_result_from_gx(raw_result)
    if not result.success:
        joined_failures = "; ".join(result.failure_messages)
        raise DataQualityError(
            f"Great Expectations suite {result.suite_name!r} failed: {joined_failures}"
        )
    return result


def build_silver_to_gold_suite(context: Any | None = None) -> gx.ExpectationSuite:
    context = context or gx.get_context(mode="ephemeral")
    suite = gx.ExpectationSuite(name=SILVER_TO_GOLD_SUITE_NAME)
    suite.add_expectation(
        ExpectTableColumnsToMatchSet(
            column_set=REQUIRED_SILVER_COLUMNS,
            exact_match=False,
        )
    )
    suite.add_expectation(ExpectTableRowCountToBeBetween(min_value=1))

    for column in REQUIRED_SILVER_COLUMNS:
        if column == "source_lsn":
            continue
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column=column))

    suite.add_expectation(
        ExpectCompoundColumnsToBeUnique(
            column_list=["kafka_topic", "kafka_partition", "kafka_offset"],
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
    suite.add_expectation(ExpectColumnValuesToBeJsonParseable(column="record_key"))
    suite.add_expectation(ExpectColumnValuesToBeJsonParseable(column="record_data"))
    return suite


def quality_result_from_gx(raw_result: Any) -> DataQualityResult:
    statistics = raw_result.statistics
    failure_messages: list[str] = []
    for result in raw_result.results:
        if result.success:
            continue
        failure_messages.append(result.expectation_config.type)

    return DataQualityResult(
        suite_name=SILVER_TO_GOLD_SUITE_NAME,
        success=bool(raw_result.success),
        evaluated_expectations=statistics["evaluated_expectations"],
        successful_expectations=statistics["successful_expectations"],
        unsuccessful_expectations=statistics["unsuccessful_expectations"],
        failure_messages=tuple(failure_messages),
    )
