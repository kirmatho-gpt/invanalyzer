# Portfolio Analysis Project Structure

This document outlines a suggested Python project layout for analyzing transactions,
portfolio holdings, returns, and dividends across funds, ETFs, and equities. It
assumes transaction reports and holding snapshots arrive as separate data sources.
Raw and derived datasets should live outside the repository and be passed in via
explicit paths or environment configuration.

## High-level layout

```
src/
├── notebooks/                     # exploratory analysis
├── src/
│   ├── __init__.py
│   ├── config/                # configuration defaults, schema definitions
│   ├── ingestion/             # readers/parsers for broker formats
│   ├── normalization/         # clean + map raw inputs to canonical schema
│   ├── reference/             # security master + corporate actions helpers
│   ├── positions/             # position building from transactions
│   ├── performance/           # returns, PnL, attribution
│   ├── dividends/             # dividend accruals and cash flow analysis
│   ├── reporting/             # tables, charts, exports
│   └── cli.py                  # command-line entry points
├── tests/
│   ├── unit/
│   └── integration/
├── scripts/                       # one-off utilities
├── pyproject.toml                 # packaging, dependencies, tooling
└── README.md
```

## Core concepts and data flow

1. **Ingestion**
   - Parse transactions (trades, fees, taxes, dividends) and holdings snapshots
     into raw staging tables.
   - Persist parsed data with source file metadata for traceability.

2. **Normalization**
   - Map broker-specific fields into canonical schemas:
     - `transactions`: trade date/time, settlement date, action, quantity, price,
       fees, taxes, currency, instrument ID.
     - `holdings`: valuation date, quantity, market value, cost basis, currency,
       instrument ID.
   - De-duplicate, enforce data types, and normalize currencies.

3. **Reference data**
   - Maintain a security master (ticker, ISIN, asset class) and corporate actions
     (splits, mergers).
   - Store FX rates and benchmark data.

4. **Positions**
   - Build daily positions from transactions; reconcile with holdings snapshots
     to surface discrepancies.
   - Support lot-level tracking for cost basis and realized PnL.

5. **Performance and income**
   - Calculate time-weighted and money-weighted returns.
   - Track dividend accruals, payments, and reinvestments.

6. **Reporting**
   - Produce summaries by asset class, instrument, and account.
   - Export to CSV/Parquet and generate charts/dashboards.

## Recommended schemas

- **Transactions**
  - `transaction_id`, `account_id`, `trade_date`, `settle_date`, `action`,
    `quantity`, `price`, `fees`, `taxes`, `currency`, `instrument_id`,
    `source_file`.
- **Holdings**
  - `snapshot_id`, `account_id`, `valuation_date`, `quantity`, `market_value`,
    `cost_basis`, `currency`, `instrument_id`, `source_file`.
- **Instruments**
  - `instrument_id`, `ticker`, `isin`, `name`, `asset_class`, `currency`.

## Suggested tooling

- **Dataframes**: pandas or polars for ETL.
- **Storage**: parquet files or a lightweight DB (SQLite, DuckDB).
- **Validation**: pandera or pydantic for schema checks.
- **CLI**: typer or argparse for batch jobs.
- **Testing**: pytest with small fixtures for transactions/holdings.
