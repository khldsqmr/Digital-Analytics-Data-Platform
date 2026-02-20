# Google Search Console — Bronze Layer

This directory contains the **Bronze-layer ingestion logic** for Google Search
Console data sourced via Improvado.

The Bronze layer preserves **full source fidelity**, applies only minimal
transformations, and serves as the immutable foundation for downstream Silver
and Gold models.

---

## Data Sources

- **Upstream system:** Google Search Console
- **Ingestion provider:** Improvado
- **Warehouse:** BigQuery
- **Update cadence:** Daily (incremental)

---

## Tables

### 1. Query-Level Daily Metrics

**Table**
`ds_dbi_digitalmedia_automation.sdi_bronze_search_console_query_daily`

**Grain**
- account
- site
- page
- query
- search_type
- date

**Description**
Stores the most granular Search Console performance data available, including
query, page, and search type dimensions.

This table is used for:
- SEO diagnostics
- Content performance analysis
- Query-level attribution and modeling

---

### 2. Site-Level Daily Totals

**Table**
`ds_dbi_digitalmedia_automation.sdi_bronze_search_console_site_totals_daily`

**Grain**
- account
- site
- date

**Description**
Stores site-level aggregated Search Console metrics without query or page
breakdowns.

This table is used for:
- Executive KPI reporting
- Trend analysis
- Topline SEO monitoring


---

## Bootstrap (One-Time)

**Purpose**
- Create physical Bronze tables
- Define schema, partitioning, clustering, and metadata
- Executed once per environment


⚠️ These scripts must **never** be scheduled.

---

## Incremental Loads (Daily)

**Strategy**
- MERGE-based ingestion
- Rolling lookback window (default: 7 days)
- Idempotent and safe for re-runs
- Handles late-arriving data

---

## Backfills (Manual)

**Purpose**
- Load historical data (e.g. last 3 years)
- Rebuild corrupted or missing partitions
- Controlled execution only


Backfill scripts:
- Do NOT use rolling windows
- Explicitly bound date ranges
- Can be safely re-run

---

## Tests & Data Quality

**Current checks**
- Grain uniqueness
- Non-null primary keys
- No future-dated records


Tests are designed to be:
- Executable manually
- Automatable later via CI, Airflow, or dbt

---

## Design Principles

- Source fidelity over convenience
- Explicit grains and contracts
- Separation of ingestion, logic, and reporting
- Partition-aware and cost-efficient
- Scalable across additional ad platforms

---

## Ownership

- **Layer:** Bronze
- **Domain:** SDI
- **Source:** Google Search Console
- **Maintained by:** Khalid Qamar
