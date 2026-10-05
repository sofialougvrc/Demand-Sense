# ADR 0007: Seasonal-Naive Baseline Forecast Evaluation

## Status

Accepted

## Context

The first forecasting milestone needs a credible benchmark before adding a learned model.
Gold demand is available at the store/SKU/day grain, but the table only stores observed
sales rows. A forecast evaluation should treat missing store/SKU/date combinations as zero
demand so the holdout is not biased toward active selling days only.

## Decision

Add a weekly seasonal-naive baseline that predicts each store/SKU/day from the same
store/SKU's demand seven days earlier. The evaluation command completes the daily panel,
uses a chronological holdout, and reports MAE, RMSE, MAPE on positive-actual rows, WMAPE,
bias, and p50 pinball loss.

## Rationale

- Weekly seasonality is a realistic first benchmark for retail daily demand.
- A fixed chronological holdout avoids random leakage across time.
- Completing the panel with zero-demand days makes the baseline suitable for later
  inventory-policy comparison, where no-sale days still matter.

## Consequences

- The baseline has no promotion, price, trend, or stockout-censoring awareness.
- MAPE is reported only where actual demand is positive; WMAPE is the preferred aggregate
  metric when zero-demand days are present.
- Later CQR and backtest milestones should compare against this same exported forecast
  frame rather than creating a second baseline definition.
