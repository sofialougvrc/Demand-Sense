from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from dotenv import load_dotenv

DEFAULT_CONNECTOR_NAME = "demand-sense-postgres"
RETAIL_TABLES = (
    "retail.stores",
    "retail.products",
    "retail.promotions",
    "retail.sales_transactions",
)


@dataclass(frozen=True)
class DebeziumSettings:
    connect_url: str
    connector_name: str
    postgres_host: str
    postgres_port: str
    postgres_user: str
    postgres_password: str
    postgres_db: str
    topic_prefix: str = "demand_sense"
    slot_name: str = "demand_sense_slot"
    publication_name: str = "demand_sense_publication"


def main() -> None:
    load_dotenv()
    args = parse_args()
    settings = settings_from_env()

    try:
        if args.command == "register":
            response = register_connector(settings)
        elif args.command == "status":
            response = get_connector_status(settings)
        elif args.command == "delete":
            response = delete_connector(settings)
        else:
            raise ValueError(f"Unsupported command: {args.command}")
    except ConnectorRequestError as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(1) from exc

    if response is not None:
        print(json.dumps(response, indent=2, sort_keys=True))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage the Demand-Sense Debezium connector.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("register", help="Register or update the Postgres CDC connector.")
    subparsers.add_parser("status", help="Show connector task status.")
    subparsers.add_parser("delete", help="Delete the connector.")
    return parser.parse_args()


def settings_from_env() -> DebeziumSettings:
    return DebeziumSettings(
        connect_url=os.getenv("DEBEZIUM_CONNECT_URL", "http://localhost:8083").rstrip("/"),
        connector_name=os.getenv("DEBEZIUM_CONNECTOR_NAME", DEFAULT_CONNECTOR_NAME),
        postgres_host=os.getenv("POSTGRES_HOST_FOR_DEBEZIUM", "postgres"),
        postgres_port=os.getenv("POSTGRES_PORT_FOR_DEBEZIUM", "5432"),
        postgres_user=os.getenv("POSTGRES_USER", "demand_sense"),
        postgres_password=os.getenv("POSTGRES_PASSWORD", "demand_sense"),
        postgres_db=os.getenv("POSTGRES_DB", "demand_sense"),
    )


def connector_config(settings: DebeziumSettings) -> dict[str, str]:
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": settings.postgres_host,
        "database.port": settings.postgres_port,
        "database.user": settings.postgres_user,
        "database.password": settings.postgres_password,
        "database.dbname": settings.postgres_db,
        "topic.prefix": settings.topic_prefix,
        "plugin.name": "pgoutput",
        "slot.name": settings.slot_name,
        "publication.name": settings.publication_name,
        "publication.autocreate.mode": "disabled",
        "table.include.list": ",".join(RETAIL_TABLES),
        "snapshot.mode": "initial",
        "snapshot.include.collection.list": ",".join(RETAIL_TABLES),
        "decimal.handling.mode": "string",
        "time.precision.mode": "adaptive_time_microseconds",
        "include.schema.changes": "false",
        "tombstones.on.delete": "false",
        "heartbeat.interval.ms": "10000",
    }


def connector_payload(settings: DebeziumSettings) -> dict[str, Any]:
    return {"name": settings.connector_name, "config": connector_config(settings)}


def register_connector(settings: DebeziumSettings) -> dict[str, Any]:
    return connect_request(
        settings,
        method="PUT",
        path=f"/connectors/{settings.connector_name}/config",
        payload=connector_config(settings),
    )


def get_connector_status(settings: DebeziumSettings) -> dict[str, Any]:
    return connect_request(
        settings, method="GET", path=f"/connectors/{settings.connector_name}/status"
    )


def delete_connector(settings: DebeziumSettings) -> dict[str, str] | None:
    try:
        connect_request(settings, method="DELETE", path=f"/connectors/{settings.connector_name}")
    except ConnectorRequestError as exc:
        if "HTTP 404" in str(exc):
            return {"message": f"Connector {settings.connector_name!r} was already absent."}
        raise
    return {"message": f"Connector {settings.connector_name!r} deleted."}


def connect_request(
    settings: DebeziumSettings,
    *,
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = Request(
        url=f"{settings.connect_url}{path}",
        data=data,
        method=method,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )

    try:
        with urlopen(request, timeout=20) as response:
            raw_body = response.read().decode("utf-8")
    except HTTPError as exc:
        body = exc.read().decode("utf-8")
        raise ConnectorRequestError(
            f"Debezium Connect request failed with HTTP {exc.code}: {body}"
        ) from exc
    except URLError as exc:
        raise ConnectorRequestError(
            f"Could not reach Debezium Connect at {settings.connect_url}"
        ) from exc

    if not raw_body:
        return {}
    return json.loads(raw_body)


class ConnectorRequestError(RuntimeError):
    """Raised when Debezium Connect cannot complete a connector request."""


if __name__ == "__main__":
    main()
