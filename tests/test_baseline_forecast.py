from __future__ import annotations

import pandas as pd
import pytest

from demand_sense.forecasting.baseline import (
    BaselineForecastError,
    complete_daily_demand_panel,
    evaluate_forecasts,
    seasonal_naive_forecast,
)


def test_complete_daily_demand_panel_fills_missing_store_sku_dates() -> None:
    gold_df = pd.DataFrame(
        [
            {
                "store_id": "STORE-001",
                "sku_id": "SKU-001",
                "business_date": "2025-01-01",
                "units_sold": 5,
            },
            {
                "store_id": "STORE-001",
                "sku_id": "SKU-001",
                "business_date": "2025-01-03",
                "units_sold": 7,
            },
        ]
    )

    panel = complete_daily_demand_panel(gold_df)

    assert panel["business_date"].dt.strftime("%Y-%m-%d").tolist() == [
        "2025-01-01",
        "2025-01-02",
        "2025-01-03",
    ]
    assert panel["units_sold"].tolist() == [5.0, 0.0, 7.0]


def test_seasonal_naive_forecast_uses_prior_week_without_future_leakage() -> None:
    dates = pd.date_range("2025-01-01", periods=24, freq="D")
    train_units = [1, 2, 3, 4, 5, 6, 7] * 2
    test_units = [99] * 10
    panel = pd.DataFrame(
        {
            "store_id": "STORE-001",
            "sku_id": "SKU-001",
            "business_date": dates,
            "units_sold": train_units + test_units,
        }
    )

    forecasts = seasonal_naive_forecast(
        panel,
        test_start_date=pd.Timestamp("2025-01-15"),
        test_end_date=pd.Timestamp("2025-01-24"),
        season_length_days=7,
    )

    assert forecasts["forecast_units"].tolist() == [1, 2, 3, 4, 5, 6, 7, 1, 2, 3]


def test_evaluate_forecasts_reports_core_metrics() -> None:
    forecasts = pd.DataFrame(
        {
            "store_id": ["STORE-001", "STORE-001", "STORE-002"],
            "sku_id": ["SKU-001", "SKU-001", "SKU-002"],
            "business_date": pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-01"]),
            "actual_units": [10.0, 0.0, 5.0],
            "forecast_units": [8.0, 1.0, 7.0],
        }
    )

    metrics = evaluate_forecasts(forecasts)

    assert metrics["rows"] == 3
    assert metrics["series_count"] == 2
    assert metrics["actual_units"] == 15.0
    assert metrics["forecast_units"] == 16.0
    assert metrics["mae"] == pytest.approx(1.6667)
    assert metrics["wmape_pct"] == pytest.approx(33.3333)
    assert metrics["mape_positive_actual_pct"] == pytest.approx(30.0)
    assert metrics["pinball_loss_p50"] == pytest.approx(0.8333)


def test_seasonal_naive_requires_non_empty_train_and_test() -> None:
    panel = pd.DataFrame(
        {
            "store_id": ["STORE-001"],
            "sku_id": ["SKU-001"],
            "business_date": pd.to_datetime(["2025-01-01"]),
            "units_sold": [1.0],
        }
    )

    with pytest.raises(BaselineForecastError, match="non-empty train and test"):
        seasonal_naive_forecast(
            panel,
            test_start_date=pd.Timestamp("2025-01-01"),
            test_end_date=pd.Timestamp("2025-01-01"),
            season_length_days=7,
        )
