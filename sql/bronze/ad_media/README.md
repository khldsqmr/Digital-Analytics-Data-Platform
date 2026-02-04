# Ad Media | Bronze Layer

This directory contains the **Bronze-layer ingestion logic for paid media (Ad Media) data**, sourced from Improvado and stored in BigQuery as part of a **medallion (Bronze / Silver / Gold) architecture**.

The Bronze layer preserves **source-level fidelity** while enabling safe incremental loads, historical backfills, and downstream analytics.

---

## Source Overview

- **Source System:** Improvado – Paid Media
- **Upstream Table:** `ps_admedia_daily_tmo`
- **Update Pattern:** Daily, append-only with possible late-arriving corrections
- **Grain:** Campaign × Day

---

## Bronze Table

### Table Name
`sdi_bronze_admedia_daily`

### Grain
One row per:

account × campaign × date


### Core Metrics
- clicks
- impressions
- conversions
- spend

### Design Principles
- Raw source fields preserved
- Source date retained as `date_yyyymmdd` (STRING) for lineage
- Derived `date` (DATE) used for partitioning and joins
- No business logic or KPI definitions in Bronze
- Full ingestion metadata retained for auditability

---

## Partitioning & Clustering

- **Partitioned by:** `date`
- **Clustered by:** `account_name`, `campaign`, `campaign_id`

This ensures:
- Efficient incremental loads
- Cost-effective scans
- Scalable downstream joins

---

## Directory Structure

```text
ad_media/
├── README.md
├── bootstrap/
│   └── create_admedia_daily.sql
├── incremental/
│   └── admedia_daily_merge.sql
├── backfill/
│   └── admedia_daily_backfill_3y.sql
└── tests/
    ├── test_grain_uniqueness_admedia.sql
    ├── test_not_null_keys_admedia.sql
    ├── test_future_dates.sql
    └── test_negative_metrics.sql
```
## Bootstrap (One-Time)
- **bootstrap/create_admedia_daily.sql**
- Creates the physical Bronze table
- Defines schema, partitioning, clustering, and column descriptions
- Executed once per environment
- Must not be re-run after creation

## Incremental Ingestion
- **incremental/admedia_daily_merge.sql**
- MERGE-based incremental load
- Rolling lookback window for late-arriving data
- Idempotent and safe to re-run
- Updates metrics and metadata without duplicating rows

## Backfill Strategy

Backfills the last **3 years** of historical data.

**Intended for:**
- Initial platform setup
- Historical corrections
- Recovery scenarios

Uses `MERGE` logic to preserve **grain integrity** and ensure idempotent loads.

---

## Data Quality Tests

Tests are **read-only validation queries** designed to catch common data issues early.

| Test | Purpose |
|-----|--------|
| Grain uniqueness | Ensures one row per campaign per day |
| Not-null keys | Validates required business keys |
| Future dates | Prevents invalid future-dated records |
| Negative metrics | Flags invalid negative values |

Tests do **not mutate data** and can be run safely at any time.

---

## Downstream Usage

This Bronze dataset feeds:

- **Silver layer:** normalization, channel mapping, campaign taxonomy
- **Gold layer:** spend facts, performance KPIs, forecasting, attribution

Bronze remains **decoupled from reporting and visualization logic** by design.

---

# Ownership

- **Layer:** Bronze
- **Domain:** SDI
- **Source:** Ad Media
- **Maintained by:** Khalid Qamar

