from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from deltalake import DeltaTable
from dotenv import load_dotenv

from demand_sense.lakehouse.gold import DEFAULT_GOLD_DAILY_DEMAND_TABLE_URI

SERIES_COLUMNS = ["store_id", "sku_id"]
DATE_COLUMN = "business_date"
TARGET_COLUMN = "units_sold"
FORECAST_MODEL_NAME = "seasonal_naive_weekly"


@dataclass(frozen=True)
class BaselineForecastSettings:
    gold_daily_demand_table_uri: str
    minio_endpoint_url: str
    minio_access_key: str
    minio_secret_key: str
    aws_region: str
    holdout_days: int = 28
    season_length_days: int = 7
    output_dir: Path = Path("artifacts/forecasting/baseline")
    write_outputs: bool = True


def main() -> None:
    load_dotenv()
    args = parse_args()
    settings = settings_from_env(args)

    try:
        if args.command == "evaluate":
            summary = evaluate_baseline_forecast(settings)
        else:
            raise ValueError(f"Unsupported command: {args.command}")
    except BaselineForecastError as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(1) from exc

    print(json.dumps(summary, indent=2, sort_keys=True, default=str))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate baseline demand forecasts from gold demand aggregates."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    evaluate = subparsers.add_parser(
        "evaluate", help="Run the seasonal-naive baseline over a chronological holdout."
    )
    evaluate.add_argument("--holdout-days", type=int, default=None)
    evaluate.add_argument("--season-length-days", type=int, default=None)
    evaluate.add_argument("--output-dir", type=Path, default=None)
    evaluate.add_argument(
        "--no-write-outputs",
        action="store_true",
        help="Skip writing forecast CSV and metrics JSON artifacts.",
    )
    return parser.parse_args()


def settings_from_env(args: argparse.Namespace) -> BaselineForecastSettings:
    holdout_days = getattr(args, "holdout_days", None)
    season_length_days = getattr(args, "season_length_days", None)
    output_dir = getattr(args, "output_dir", None)

    return BaselineForecastSettings(
        gold_daily_demand_table_uri=os.getenv(
            "GOLD_DAILY_DEMAND_TABLE_URI", DEFAULT_GOLD_DAILY_DEMAND_TABLE_URI
        ),
        minio_endpoint_url=os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9000"),
        minio_access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        minio_secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        aws_region=os.getenv("AWS_REGION", "us-east-1"),
        holdout_days=holdout_days
        if holdout_days is not None
        else int(os.getenv("BASELINE_HOLDOUT_DAYS", "28")),
        season_length_days=season_length_days
        if season_length_days is not None
        else int(os.getenv("BASELINE_SEASON_LENGTH_DAYS", "7")),
        output_dir=output_dir
        if output_dir is not None
        else Path(os.getenv("BASELINE_OUTPUT_DIR", "artifacts/forecasting/baseline")),
        write_outputs=not getattr(args, "no_write_outputs", False),
    )


def evaluate_baseline_forecast(settings: BaselineForecastSettings) -> dict[str, Any]:
    gold_df = read_gold_daily_demand(settings)
    panel = complete_daily_demand_panel(gold_df)
    validate_evaluation_window(panel, settings)

    max_date = panel[DATE_COLUMN].max()
    test_start_date = max_date - pd.Timedelta(days=settings.holdout_days - 1)
    forecasts = seasonal_naive_forecast(
        panel,
        test_start_date=test_start_date,
        test_end_date=max_date,
        season_length_days=settings.season_length_days,
    )
    metrics = evaluate_forecasts(forecasts)

    if settings.write_outputs:
        write_evaluation_outputs(forecasts=forecasts, metrics=metrics, settings=settings)

    return {
        "gold_daily_demand_table_uri": settings.gold_daily_demand_table_uri,
        "model": FORECAST_MODEL_NAME,
        "holdout_days": settings.holdout_days,
        "season_length_days": settings.season_length_days,
        "panel_rows": len(panel),
        "forecast_rows": len(forecasts),
        "date_range": {
            "train_start": panel[DATE_COLUMN].min().date(),
            "test_start": test_start_date.date(),
            "test_end": max_date.date(),
        },
        "metrics": metrics,
        "output_dir": settings.output_dir if settings.write_outputs else None,
    }


def read_gold_daily_demand(settings: BaselineForecastSettings) -> pd.DataFrame:
    try:
        table = DeltaTable(
            settings.gold_daily_demand_table_uri,
            storage_options=baseline_storage_options(settings),
        )
    except Exception as exc:
        raise BaselineForecastError(
            f"Could not open gold Delta table at {settings.gold_daily_demand_table_uri}"
        ) from exc

    return table.to_pyarrow_table().to_pandas()


def complete_daily_demand_panel(gold_df: pd.DataFrame) -> pd.DataFrame:
    required_columns = {*SERIES_COLUMNS, DATE_COLUMN, TARGET_COLUMN}
    missing_columns = required_columns.difference(gold_df.columns)
    if missing_columns:
        raise BaselineForecastError(f"Gold demand is missing columns: {sorted(missing_columns)}")

    if gold_df.empty:
        raise BaselineForecastError("Gold demand table is empty.")

    demand = gold_df[[*SERIES_COLUMNS, DATE_COLUMN, TARGET_COLUMN]].copy()
    demand[DATE_COLUMN] = pd.to_datetime(demand[DATE_COLUMN]).dt.normalize()
    demand[TARGET_COLUMN] = demand[TARGET_COLUMN].astype(float)
    demand = demand.groupby([*SERIES_COLUMNS, DATE_COLUMN], as_index=False)[TARGET_COLUMN].sum()

    series = demand[SERIES_COLUMNS].drop_duplicates().sort_values(SERIES_COLUMNS)
    calendar = pd.DataFrame(
        {DATE_COLUMN: pd.date_range(demand[DATE_COLUMN].min(), demand[DATE_COLUMN].max(), freq="D")}
    )
    panel = series.merge(calendar, how="cross").merge(
        demand,
        on=[*SERIES_COLUMNS, DATE_COLUMN],
        how="left",
    )
    panel[TARGET_COLUMN] = panel[TARGET_COLUMN].fillna(0.0)
    return panel.sort_values([*SERIES_COLUMNS, DATE_COLUMN], kind="stable").reset_index(drop=True)


def validate_evaluation_window(
    panel: pd.DataFrame,
    settings: BaselineForecastSettings,
) -> None:
    if settings.holdout_days <= 0:
        raise BaselineForecastError("holdout_days must be positive.")
    if settings.season_length_days <= 0:
        raise BaselineForecastError("season_length_days must be positive.")

    available_days = panel[DATE_COLUMN].nunique()
    minimum_days = settings.holdout_days + settings.season_length_days
    if available_days < minimum_days:
        raise BaselineForecastError(
            "Not enough history for baseline evaluation: "
            f"need at least {minimum_days} days, found {available_days}."
        )


def seasonal_naive_forecast(
    panel: pd.DataFrame,
    *,
    test_start_date: pd.Timestamp,
    test_end_date: pd.Timestamp,
    season_length_days: int = 7,
) -> pd.DataFrame:
    if season_length_days <= 0:
        raise BaselineForecastError("season_length_days must be positive.")

    panel = panel.copy()
    panel[DATE_COLUMN] = pd.to_datetime(panel[DATE_COLUMN]).dt.normalize()
    test_start_date = pd.Timestamp(test_start_date).normalize()
    test_end_date = pd.Timestamp(test_end_date).normalize()

    train = panel[panel[DATE_COLUMN] < test_start_date].sort_values(
        [*SERIES_COLUMNS, DATE_COLUMN], kind="stable"
    )
    test = panel[
        (panel[DATE_COLUMN] >= test_start_date) & (panel[DATE_COLUMN] <= test_end_date)
    ].sort_values([DATE_COLUMN, *SERIES_COLUMNS], kind="stable")

    if train.empty or test.empty:
        raise BaselineForecastError(
            "Seasonal-naive evaluation needs non-empty train and test data."
        )

    fallback_lookup = trailing_mean_lookup(train, season_length_days=season_length_days)
    history = {
        (row.store_id, row.sku_id, row.business_date): float(row.units_sold)
        for row in train.itertuples(index=False)
    }

    rows: list[dict[str, Any]] = []
    cutoff_date = test_start_date - pd.Timedelta(days=1)
    for row in test.itertuples(index=False):
        business_date = pd.Timestamp(row.business_date).normalize()
        key = (row.store_id, row.sku_id)
        lag_date = business_date - pd.Timedelta(days=season_length_days)
        forecast_units = history.get(
            (row.store_id, row.sku_id, lag_date),
            fallback_lookup.get(key, 0.0),
        )
        forecast_units = max(0.0, float(forecast_units))
        actual_units = float(row.units_sold)

        rows.append(
            {
                "store_id": row.store_id,
                "sku_id": row.sku_id,
                "business_date": business_date,
                "cutoff_date": cutoff_date,
                "model": FORECAST_MODEL_NAME,
                "actual_units": actual_units,
                "forecast_units": forecast_units,
                "error": actual_units - forecast_units,
                "absolute_error": abs(actual_units - forecast_units),
            }
        )
        history[(row.store_id, row.sku_id, business_date)] = forecast_units

    return pd.DataFrame(rows)


def trailing_mean_lookup(
    train: pd.DataFrame, *, season_length_days: int
) -> dict[tuple[str, str], float]:
    lookup: dict[tuple[str, str], float] = {}
    for key, group in train.groupby(SERIES_COLUMNS, sort=False):
        recent = group.sort_values(DATE_COLUMN, kind="stable").tail(season_length_days)
        lookup[key] = float(recent[TARGET_COLUMN].mean())
    return lookup


def evaluate_forecasts(forecasts: pd.DataFrame) -> dict[str, Any]:
    if forecasts.empty:
        raise BaselineForecastError("Cannot evaluate an empty forecast frame.")

    actual = forecasts["actual_units"].to_numpy(dtype=float)
    predicted = forecasts["forecast_units"].to_numpy(dtype=float)
    error = actual - predicted
    absolute_error = np.abs(error)
    squared_error = np.square(error)
    positive_actual = actual > 0

    actual_units = float(actual.sum())
    forecast_units = float(predicted.sum())
    mape_positive_actual = (
        float(np.mean(absolute_error[positive_actual] / actual[positive_actual]) * 100)
        if positive_actual.any()
        else None
    )
    wmape = float(absolute_error.sum() / actual_units * 100) if actual_units > 0 else None

    return {
        "rows": int(len(forecasts)),
        "series_count": int(forecasts[SERIES_COLUMNS].drop_duplicates().shape[0]),
        "actual_units": round(actual_units, 4),
        "forecast_units": round(forecast_units, 4),
        "mae": round(float(absolute_error.mean()), 4),
        "rmse": round(float(np.sqrt(squared_error.mean())), 4),
        "mape_positive_actual_pct": round(mape_positive_actual, 4)
        if mape_positive_actual is not None
        else None,
        "wmape_pct": round(wmape, 4) if wmape is not None else None,
        "bias_units": round(float(error.mean()), 4),
        "pinball_loss_p50": round(float(pinball_loss(actual, predicted, quantile=0.5)), 4),
    }


def pinball_loss(actual: np.ndarray, predicted: np.ndarray, *, quantile: float) -> float:
    residual = actual - predicted
    return np.mean(np.maximum(quantile * residual, (quantile - 1) * residual))


def write_evaluation_outputs(
    *,
    forecasts: pd.DataFrame,
    metrics: dict[str, Any],
    settings: BaselineForecastSettings,
) -> None:
    settings.output_dir.mkdir(parents=True, exist_ok=True)
    forecasts.to_csv(settings.output_dir / "seasonal_naive_forecasts.csv", index=False)
    with (settings.output_dir / "seasonal_naive_metrics.json").open("w", encoding="utf-8") as file:
        json.dump(metrics, file, indent=2, sort_keys=True)


def baseline_storage_options(settings: BaselineForecastSettings) -> dict[str, str]:
    return {
        "AWS_ACCESS_KEY_ID": settings.minio_access_key,
        "AWS_SECRET_ACCESS_KEY": settings.minio_secret_key,
        "AWS_ENDPOINT_URL": settings.minio_endpoint_url,
        "AWS_REGION": settings.aws_region,
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        "AWS_S3_ADDRESSING_STYLE": "path",
    }


class BaselineForecastError(RuntimeError):
    """Raised when baseline forecasting cannot be evaluated."""


if __name__ == "__main__":
    main()
