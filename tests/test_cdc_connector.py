from demand_sense.ingestion.cdc import (
    RETAIL_TABLES,
    DebeziumSettings,
    connector_config,
    connector_payload,
)


def test_connector_config_captures_all_retail_tables() -> None:
    settings = DebeziumSettings(
        connect_url="http://localhost:8083",
        connector_name="demand-sense-postgres",
        postgres_host="postgres",
        postgres_port="5432",
        postgres_user="demand_sense",
        postgres_password="demand_sense",
        postgres_db="demand_sense",
    )

    config = connector_config(settings)

    assert config["connector.class"] == "io.debezium.connector.postgresql.PostgresConnector"
    assert config["database.hostname"] == "postgres"
    assert config["plugin.name"] == "pgoutput"
    assert config["publication.name"] == "demand_sense_publication"
    assert config["publication.autocreate.mode"] == "disabled"
    assert config["table.include.list"].split(",") == list(RETAIL_TABLES)
    assert config["snapshot.mode"] == "initial"
    assert config["include.schema.changes"] == "false"


def test_connector_payload_uses_named_connector_and_config() -> None:
    settings = DebeziumSettings(
        connect_url="http://localhost:8083",
        connector_name="custom-connector",
        postgres_host="postgres",
        postgres_port="5432",
        postgres_user="user",
        postgres_password="password",
        postgres_db="database",
    )

    payload = connector_payload(settings)

    assert payload["name"] == "custom-connector"
    assert payload["config"]["database.user"] == "user"
    assert payload["config"]["database.dbname"] == "database"
