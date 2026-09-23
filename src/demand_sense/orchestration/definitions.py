from __future__ import annotations

import json
import os
from collections.abc import Mapping
from typing import Any

from dagster import (
    ConfigurableResource,
    Definitions,
    MaterializeResult,
    MetadataValue,
    ScheduleDefinition,
    asset,
    define_asset_job,
)
from dotenv import load_dotenv

from demand_sense.lakehouse.bronze import (
    DEFAULT_BRONZE_TABLE_URI,
    BronzeSettings,
    land_bronze_events,
)
from demand_sense.lakehouse.gold import (
    DEFAULT_GOLD_DAILY_DEMAND_TABLE_URI,
    GoldSettings,
    build_gold_daily_demand,
)
from demand_sense.lakehouse.silver import (
    DEFAULT_SILVER_TABLE_URI,
    SilverSettings,
    build_silver_events,
)

LAKEHOUSE_GROUP = "lakehouse"


class DemandSenseLakehouseResource(ConfigurableResource):
    kafka_bootstrap_servers: str = "localhost:9092"
    bronze_table_uri: str = DEFAULT_BRONZE_TABLE_URI
    silver_table_uri: str = DEFAULT_SILVER_TABLE_URI
    gold_daily_demand_table_uri: str = DEFAULT_GOLD_DAILY_DEMAND_TABLE_URI
    minio_endpoint_url: str = "http://localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    aws_region: str = "us-east-1"
    bronze_consumer_group: str = "demand-sense-dagster-bronze"
    bronze_batch_size: int = 5_000
    bronze_max_messages: int = 10_000
    bronze_idle_timeout_seconds: float = 10.0
    bronze_write_mode: str = "append"
    silver_write_mode: str = "overwrite"
    gold_write_mode: str = "overwrite"

    @classmethod
    def from_env(cls) -> DemandSenseLakehouseResource:
        load_dotenv()
        return cls(
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            bronze_table_uri=os.getenv("BRONZE_TABLE_URI", DEFAULT_BRONZE_TABLE_URI),
            silver_table_uri=os.getenv("SILVER_TABLE_URI", DEFAULT_SILVER_TABLE_URI),
            gold_daily_demand_table_uri=os.getenv(
                "GOLD_DAILY_DEMAND_TABLE_URI", DEFAULT_GOLD_DAILY_DEMAND_TABLE_URI
            ),
            minio_endpoint_url=os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9000"),
            minio_access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
            minio_secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
            aws_region=os.getenv("AWS_REGION", "us-east-1"),
            bronze_consumer_group=os.getenv("BRONZE_CONSUMER_GROUP", "demand-sense-dagster-bronze"),
            bronze_batch_size=int(os.getenv("BRONZE_BATCH_SIZE", "5000")),
            bronze_max_messages=int(os.getenv("BRONZE_MAX_MESSAGES", "10000")),
            bronze_idle_timeout_seconds=float(os.getenv("BRONZE_IDLE_TIMEOUT_SECONDS", "10")),
            bronze_write_mode=os.getenv("BRONZE_WRITE_MODE", "append"),
            silver_write_mode=os.getenv("SILVER_WRITE_MODE", "overwrite"),
            gold_write_mode=os.getenv("GOLD_WRITE_MODE", "overwrite"),
        )

    def bronze_settings(self) -> BronzeSettings:
        return BronzeSettings(
            kafka_bootstrap_servers=self.kafka_bootstrap_servers,
            table_uri=self.bronze_table_uri,
            minio_endpoint_url=self.minio_endpoint_url,
            minio_access_key=self.minio_access_key,
            minio_secret_key=self.minio_secret_key,
            aws_region=self.aws_region,
            consumer_group=self.bronze_consumer_group,
            batch_size=self.bronze_batch_size,
            max_messages=self.bronze_max_messages,
            idle_timeout_seconds=self.bronze_idle_timeout_seconds,
            mode=self.bronze_write_mode,
        )

    def silver_settings(self) -> SilverSettings:
        return SilverSettings(
            bronze_table_uri=self.bronze_table_uri,
            silver_table_uri=self.silver_table_uri,
            minio_endpoint_url=self.minio_endpoint_url,
            minio_access_key=self.minio_access_key,
            minio_secret_key=self.minio_secret_key,
            aws_region=self.aws_region,
            mode=self.silver_write_mode,
        )

    def gold_settings(self) -> GoldSettings:
        return GoldSettings(
            silver_table_uri=self.silver_table_uri,
            gold_daily_demand_table_uri=self.gold_daily_demand_table_uri,
            minio_endpoint_url=self.minio_endpoint_url,
            minio_access_key=self.minio_access_key,
            minio_secret_key=self.minio_secret_key,
            aws_region=self.aws_region,
            mode=self.gold_write_mode,
        )


@asset(
    group_name=LAKEHOUSE_GROUP,
    compute_kind="kafka_delta",
    description="Land Debezium Kafka CDC events into the bronze Delta table on MinIO.",
)
def bronze_cdc_events(
    context,
    lakehouse: DemandSenseLakehouseResource,
) -> MaterializeResult:
    summary = land_bronze_events(lakehouse.bronze_settings())
    context.log.info("Landed %s bronze CDC events", summary["landed_rows"])
    return MaterializeResult(metadata=metadata_from_summary(summary))


@asset(
    deps=[bronze_cdc_events],
    group_name=LAKEHOUSE_GROUP,
    compute_kind="great_expectations_delta",
    description="Validate bronze CDC events and write typed, deduplicated silver events.",
)
def silver_retail_cdc_events(
    context,
    lakehouse: DemandSenseLakehouseResource,
) -> MaterializeResult:
    summary = build_silver_events(lakehouse.silver_settings())
    context.log.info("Built %s silver CDC events", summary["silver_rows"])
    return MaterializeResult(metadata=metadata_from_summary(summary))


@asset(
    deps=[silver_retail_cdc_events],
    group_name=LAKEHOUSE_GROUP,
    compute_kind="great_expectations_delta",
    description="Validate silver CDC events and write store/SKU/day gold demand aggregates.",
)
def gold_store_sku_daily_demand(
    context,
    lakehouse: DemandSenseLakehouseResource,
) -> MaterializeResult:
    summary = build_gold_daily_demand(lakehouse.gold_settings())
    context.log.info("Built %s gold store/SKU/day demand rows", summary["gold_rows"])
    return MaterializeResult(metadata=metadata_from_summary(summary))


lakehouse_assets = [
    bronze_cdc_events,
    silver_retail_cdc_events,
    gold_store_sku_daily_demand,
]
daily_lakehouse_job = define_asset_job(
    name="daily_lakehouse_job",
    selection=lakehouse_assets,
    description="Run the bronze, silver, and gold lakehouse assets in dependency order.",
)
daily_lakehouse_schedule = ScheduleDefinition(
    name="daily_lakehouse_schedule",
    job=daily_lakehouse_job,
    cron_schedule="0 6 * * *",
    execution_timezone="UTC",
    description="Materialize the local lakehouse once per day at 06:00 UTC.",
)

defs = Definitions(
    assets=lakehouse_assets,
    jobs=[daily_lakehouse_job],
    schedules=[daily_lakehouse_schedule],
    resources={"lakehouse": DemandSenseLakehouseResource.from_env()},
)


def metadata_from_summary(summary: Mapping[str, Any]) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    for key, value in summary.items():
        if isinstance(value, bool | float | int | str):
            metadata[key] = value
        elif value is None:
            metadata[key] = "None"
        elif isinstance(value, Mapping | list | tuple):
            metadata[key] = MetadataValue.json(json.loads(json.dumps(value, default=str)))
        else:
            metadata[key] = str(value)
    return metadata


def lakehouse_run_config_from_env() -> dict[str, Any]:
    resource = DemandSenseLakehouseResource.from_env()
    return {
        "resources": {
            "lakehouse": {
                "config": resource.model_dump(),
            }
        }
    }


def main() -> None:
    print(json.dumps(lakehouse_run_config_from_env(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
