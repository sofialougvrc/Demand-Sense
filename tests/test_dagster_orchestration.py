from __future__ import annotations

from dagster import Definitions, materialize

from demand_sense.orchestration import definitions
from demand_sense.orchestration.definitions import (
    DemandSenseLakehouseResource,
    bronze_cdc_events,
    daily_lakehouse_job,
    daily_lakehouse_schedule,
    defs,
    gold_store_sku_daily_demand,
    lakehouse_run_config_from_env,
    silver_retail_cdc_events,
)


def test_dagster_definitions_are_loadable() -> None:
    Definitions.validate_loadable(defs)

    assert daily_lakehouse_job.name == "daily_lakehouse_job"
    assert daily_lakehouse_schedule.cron_schedule == "0 6 * * *"


def test_lakehouse_resource_reads_environment(monkeypatch) -> None:
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    monkeypatch.setenv("BRONZE_TABLE_URI", "s3://bucket/bronze")
    monkeypatch.setenv("SILVER_TABLE_URI", "s3://bucket/silver")
    monkeypatch.setenv("GOLD_DAILY_DEMAND_TABLE_URI", "s3://bucket/gold")
    monkeypatch.setenv("BRONZE_MAX_MESSAGES", "42")

    resource = DemandSenseLakehouseResource.from_env()

    assert resource.bronze_settings().kafka_bootstrap_servers == "kafka:29092"
    assert resource.silver_settings().bronze_table_uri == "s3://bucket/bronze"
    assert resource.gold_settings().gold_daily_demand_table_uri == "s3://bucket/gold"
    assert resource.bronze_settings().max_messages == 42


def test_assets_materialize_in_dependency_order(monkeypatch) -> None:
    calls: list[str] = []

    def fake_land_bronze_events(settings):
        calls.append(f"bronze:{settings.table_uri}")
        return {"table_uri": settings.table_uri, "landed_rows": 10}

    def fake_build_silver_events(settings):
        calls.append(f"silver:{settings.silver_table_uri}")
        return {
            "bronze_table_uri": settings.bronze_table_uri,
            "silver_table_uri": settings.silver_table_uri,
            "bronze_rows": 10,
            "silver_rows": 10,
            "quality_gate": {"success": True},
        }

    def fake_build_gold_daily_demand(settings):
        calls.append(f"gold:{settings.gold_daily_demand_table_uri}")
        return {
            "silver_table_uri": settings.silver_table_uri,
            "gold_daily_demand_table_uri": settings.gold_daily_demand_table_uri,
            "silver_rows": 10,
            "gold_rows": 3,
            "quality_gate": {"success": True},
        }

    monkeypatch.setattr(definitions, "land_bronze_events", fake_land_bronze_events)
    monkeypatch.setattr(definitions, "build_silver_events", fake_build_silver_events)
    monkeypatch.setattr(definitions, "build_gold_daily_demand", fake_build_gold_daily_demand)

    result = materialize(
        [bronze_cdc_events, silver_retail_cdc_events, gold_store_sku_daily_demand],
        resources={
            "lakehouse": DemandSenseLakehouseResource(
                bronze_table_uri="s3://test/bronze",
                silver_table_uri="s3://test/silver",
                gold_daily_demand_table_uri="s3://test/gold",
            )
        },
    )

    assert result.success
    assert calls == ["bronze:s3://test/bronze", "silver:s3://test/silver", "gold:s3://test/gold"]


def test_run_config_from_env_is_launchpad_ready(monkeypatch) -> None:
    monkeypatch.setenv("SILVER_WRITE_MODE", "overwrite")

    run_config = lakehouse_run_config_from_env()

    assert run_config["resources"]["lakehouse"]["config"]["silver_write_mode"] == "overwrite"
