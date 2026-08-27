# Nord Pool Electricity Prices Lakehouse

> A Databricks Lakeflow Declarative Pipelines project that ingests Nord Pool
> electricity prices through the Elering API and publishes curated
> bronze, silver, quarantine, and gold datasets.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Databricks](https://img.shields.io/badge/Platform-Databricks-FF3621?logo=databricks&logoColor=white)](https://www.databricks.com/)
[![Lakeflow](https://img.shields.io/badge/Orchestration-Lakeflow-FF3621)](https://www.databricks.com/product/data-engineering)
[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)

> [!NOTE]
> Learning and portfolio project. It is not affiliated with Nord Pool or
> Elering. Validate source data, licensing, and output quality before using
> this project for operational or financial decisions.

## Overview

Data source Elering API - https://dashboard.elering.ee/assets/swagger-ui/index.html

The pipeline queries a configurable rolling price window—two days before and
two days after the execution date by default.

API windows overlap by design. Raw observations are retained in landing and
bronze for auditability, while silver keeps the latest valid observation for
each country and delivery timestamp.

## Architecture

```text
Elering API
    │
    ▼
Daily Databricks Job
    │
    ▼
landing_nordpool_api_batches
    │  Append-only Delta API batches
    ▼
bronze_nordpool_prices
    │  Incremental streaming ingestion
    ▼
stg_nordpool_silver_base
    ├──────────────────────────────► silver_nordpool_prices_quarantine
    │                                 Invalid records and reason codes
    ▼
silver_nordpool_prices
    │  Validated, enriched, latest-state prices
    ▼
gold_nordpool_daily_summary
       Daily reporting metrics
```

| Layer | Dataset | Purpose |
|---|---|---|
| Landing | `landing_nordpool_api_batches` | Durable append-only API batches written by the ingestion Job |
| Bronze | `bronze_nordpool_prices` | Incremental raw observations with batch and source metadata |
| Silver | `silver_nordpool_prices` | Validated, enriched, and deduplicated current-state prices |
| Quarantine | `silver_nordpool_prices_quarantine` | Invalid observations retained with reason |
| Gold | `gold_nordpool_daily_summary` | Daily country-level electricity-price metrics |

## Features

- Elering/Nord Pool electricity-price API ingestion
- Configurable historical and future request window
- UTC and `Europe/Vilnius` delivery timestamps
- EUR/MWh and EUR/kWh price measures
- Latest-record selection across overlapping API batches
- Daily average, minimum, maximum, and interval-count gold metrics
- Databricks Asset Bundles deployment targets for development and production

## Project Structure

```text
.
├── databricks.yml
├── resources/
│   ├── job.yml
│   ├── pipelines.yml
│   └── schemas.yml
├── src/
│   ├── nordpool_api.py
│   ├── ingest_nordpool_landing.py
│   └── pipelines/
│       ├── bronze_nordpool.py
│       ├── silver_nordpool.py
│       └── gold_nordpool.py
└── tests/
```

## Quick Start

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Databricks CLI authenticated to the workspace
- Permission to create schemas, Jobs, Lakeflow pipelines, and Delta tables
- Serverless or compatible Databricks compute
- Git and Python 3.10+

### Clone and authenticate

```bash
git clone https://github.com/Jonas121/nordpool-electricity-prices.git
cd nordpool-electricity-prices

databricks auth login --host https://<your-workspace-host>
```

### Validate and deploy

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run nordpool_daily_job -t dev
```

The scheduled workflow runs in this order:

1. `ingest_api_to_landing` fetches API data and appends one Delta batch.
2. `refresh_nordpool_pipeline` refreshes the Lakeflow dependency graph.

## Configuration

Environment-specific configuration is defined in
[`databricks.yml`](databricks.yml).

| Setting | Development example |
|---|---|
| Catalog | `workspace` |
| Schema | `nordpool_dev` |
| Landing table | `workspace.nordpool_dev.landing_nordpool_api_batches` |
| Historical window | 2 days |
| Forward window | 2 days |
| Schedule | Paused by default |

Expected development datasets:

```text
workspace.nordpool_dev.landing_nordpool_api_batches
workspace.nordpool_dev.bronze_nordpool_prices
workspace.nordpool_dev.silver_nordpool_prices
workspace.nordpool_dev.silver_nordpool_prices_quarantine
workspace.nordpool_dev.gold_nordpool_daily_summary
```

## Data Quality

Silver validates:

- Supported market areas: `EE`, `FI`, `LT`, and `LV`
- Positive price timestamps
- Prices within a configured valid range
- Present batch identifiers
- Present API-fetch timestamps

Invalid records are retained in
`silver_nordpool_prices_quarantine` with a `quarantine_reason`. Gold datasets
consume only curated silver data.


## Roadmap

- [ ] Delivery-interval completeness checks
- [ ] Price-correction monitoring
- [ ] Automated unit and integration tests
- [ ] CI/CD deployment through GitHub Actions

## License

Distributed under the [MIT License](LICENSE).