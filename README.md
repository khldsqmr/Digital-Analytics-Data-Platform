# Digital Analytics Data Platform

An enterprise-grade digital analytics data platform built on a **Medallion Architecture (Bronze / Silver / Gold)** to support scalable, auditable, and reproducible analytics across paid media, web analytics, and SEO domains.

This repository is the **single source of truth** for:

- SQL transformations
- table/view definitions
- QA and reconciliation logic
- orchestration procedures
- execution documentation

It is designed for **production-grade analytics engineering** in BigQuery with clear separation of concerns across ingestion, transformation, curation, and validation.

---

## Platform Architecture

The platform follows a layered medallion design:

### Bronze Layer
**Purpose:** Source-faithful landing and standardization

- Ingests raw upstream data with minimal transformation
- Preserves source granularity and auditability
- Supports incremental, idempotent loads
- Enables replay/backfill safely

### Silver Layer
**Purpose:** Business logic and normalization

- Applies canonical transformations and standard business rules
- Cleans and standardizes key fields
- Enforces data quality validations
- Produces analytics-ready datasets (but not final KPI consumption layer)

### Gold Layer
**Purpose:** Curated, reporting-ready and analytics-ready datasets

- Produces final curated datasets (daily/weekly, wide/long)
- Serves as the **source of truth** for dashboards, reporting, and downstream modeling
- Includes robust QA and cross-layer reconciliation
- Supports multiple consumers (Tableau, APIs, forecasting, ad hoc analysis)

> Gold is intentionally decoupled from any single visualization tool so the same datasets can be reused consistently across dashboards, notebooks, and services.

---

## Tech Stack

- **Warehouse:** BigQuery
- **Language:** SQL (BigQuery Standard SQL)
- **Execution:** BigQuery Scheduled Queries / Stored Procedures
- **Architecture:** Medallion (Bronze / Silver / Gold)
- **QA Pattern:** Layer-level + cross-layer reconciliation test procedures

---

## Repository Structure (Current)

> Below is the current structure with **easy explanations embedded directly in the tree** so new contributors can quickly understand what each folder/script does.

```text
# SA360 (Search Ads 360)
# Root shows only the pieces relevant to the SA360 pipeline.

DIGITAL-ANALYTICS-DATA-PLATFORM/
├── orchestration/
│   └── bigquery/
│       └── Paid Search Dashboard Orchestration/
│           └── 00_sdi_sa360_paid_search_sp_call.sql
│               # Wrapper SQL used by BigQuery Scheduler to CALL the SA360 master procedures
│
├── refs/
│   └── sa360_Sanity_Check_1.sql
│       # Manual one-off sanity query for quick investigation/debugging
│
└── sql/
    ├── 01_common/
    │   ├── 00_fn_qgp_week.sql
    │   │   # Shared function: standard QGP week derivation used by weekly Gold + QA
    │   └── 01_vw_qgp_calendar.sql
    │       # Shared view: date ↔ qgp_week mapping (calendar spine)
    │
    └── 02_SDI_SA360/
        ├── 00_COMMON/
        │   ├── 00_fn_qgp_week.sql
        │   └── 01_vw_qgp_calendar.sql
        │       # (Optional) SA360-local copies; prefer sql/01_common where possible
        │
        ├── 01_BRONZE/                           # Raw → standardized landing (auditable, minimal logic)
        │   ├── DDL/
        │   │   ├── 00_create_sdi_bronze_sa360_campaign_daily.sql
        │   │   │   # Creates Bronze daily fact table (campaign × day) with partition/cluster
        │   │   └── 00_create_sdi_bronze_sa360_campaign_entity.sql
        │   │       # Creates Bronze entity snapshot table (campaign settings/metadata by day)
        │   │
        │   ├── Backfill/
        │   │   ├── 00_backfill_bronze_sa360_campaign_daily.sql
        │   │   │   # One-time historical load for Bronze daily (chunked by date)
        │   │   └── 01_backfill_bronze_sa360_campaign_entity.sql
        │   │       # One-time historical load for Bronze entity (chunked by date)
        │   │
        │   ├── MERGE/
        │   │   ├── 01_merge_sdi_bronze_sa360_campaign_daily.sql
        │   │   │   # Recurring incremental MERGE (RAW → Bronze daily) with lookback + dedupe
        │   │   └── 01_merge_sdi_bronze_sa360_campaign_entity.sql
        │   │       # Recurring incremental MERGE (RAW → Bronze entity) with lookback + dedupe
        │   │
        │   ├── Orchestration/
        │   │   └── 01_sp_bronze_sa360_master_orchestration.sql
        │   │       # Master Bronze procedure (runs daily + entity merges in correct order)
        │   │
        │   └── tests/
        │       ├── 00_create_sdi_bronze_sa360_test_results.sql
        │       │   # Creates Bronze test_results table (stores PASS/FAIL outputs)
        │       ├── 01_sp_bronze_campaign_daily_critical.sql
        │       │   # Bronze daily critical tests (null keys, duplicates, invalid values)
        │       ├── 02_sp_bronze_campaign_daily_reconciliation.sql
        │       │   # Bronze daily reconciliation (Bronze vs RAW-dedup metrics/row counts)
        │       ├── 03_sp_bronze_campaign_entity_critical.sql
        │       │   # Bronze entity critical tests (null keys, duplicates, freshness)
        │       ├── 04_sp_bronze_campaign_entity_reconciliation.sql
        │       │   # Bronze entity reconciliation (Bronze vs RAW-dedup; non-append-only aware)
        │       ├── 05_sp_bronze_weekly_deep_validation.sql
        │       │   # Weekly-level anomaly checks for key metrics (lightweight deep QA)
        │       ├── 06_sp_bronze_sa360_qa_master_orchestration.sql
        │       │   # Runs all Bronze QA procs and writes 1-row-per-test to test_results
        │       └── 07_view_bronze_test_dashboard.sql
        │           # Dashboard view for Bronze QA results (easy monitoring)
        │
        ├── 02_SILVER/                           # Business logic (clean/map/normalize/enrich)
        │   ├── DDL/
        │   │   └── 00_create_sdi_silver_sa360_campaign_daily.sql
        │   │       # Creates Silver daily table (enriched + normalized, still daily grain)
        │   │
        │   ├── Backfill/
        │   │   └── 00_backfill_silver_sa360_campaign_daily.sql
        │   │       # One-time historical build (Bronze daily + entity → Silver daily)
        │   │
        │   ├── MERGE/
        │   │   └── 01_merge_sdi_silver_sa360_campaign_daily.sql
        │   │       # Recurring incremental build (Bronze → Silver) incl. as-of entity join
        │   │
        │   ├── Orchestration/
        │   │   └── 01_sp_silver_sa360_master_orchestration.sql
        │   │       # Master Silver procedure (runs Silver merge in correct order)
        │   │
        │   └── tests/
        │       ├── 00_create_sdi_silver_sa360_test_results.sql
        │       │   # Creates Silver test_results table
        │       ├── 01_sp_silver_campaign_daily_critical.sql
        │       │   # Silver critical tests (null keys, duplicates, freshness)
        │       ├── 02_sp_silver_campaign_daily_reconciliation.sql
        │       │   # Reconciles Silver vs Bronze (ensures no metric drift)
        │       ├── 03_sp_silver_campaign_daily_business_logic.sql
        │       │   # Validates mappings/derivations (LOB, platform, campaign_type, etc.)
        │       ├── 06_sp_silver_sa360_qa_master_orchestration.sql
        │       │   # Runs all Silver QA procs and writes outcomes
        │       └── 07_view_silver_test_dashboard.sql
        │           # Dashboard view for Silver QA results
        │
        └── 03_GOLD/                             # Curated consumption layer (daily/weekly + wide/long)
            ├── DDL/
            │   ├── 00_create_sdi_gold_sa360_campaign_daily.sql
            │   │   # Gold daily WIDE (many metric columns; dashboard-friendly)
            │   ├── 00_create_sdi_gold_sa360_campaign_daily_long.sql
            │   │   # Gold daily LONG (metric_name, metric_value; flexible charting)
            │   ├── 00_create_sdi_gold_sa360_campaign_weekly.sql
            │   │   # Gold weekly WIDE (weekly rollups, uses QGP week logic)
            │   └── 00_create_sdi_gold_sa360_campaign_weekly_long.sql
            │       # Gold weekly LONG (metric_name, metric_value)
            │
            ├── Backfill/
            │   ├── 00_backfill_gold_sa360_campaign_daily.sql
            │   │   # One-time build Silver → Gold daily WIDE
            │   ├── 01_backfill_gold_sa360_campaign_daily_long.sql
            │   │   # One-time unpivot Gold daily WIDE → Gold daily LONG
            │   ├── 01_backfill_gold_sa360_campaign_weekly.sql
            │   │   # One-time rollup Gold daily → Gold weekly WIDE
            │   └── 01_backfill_gold_sa360_campaign_weekly_long.sql
            │       # One-time unpivot Gold weekly WIDE → Gold weekly LONG (+ future-week guard)
            │
            ├── MERGE/
            │   ├── 01_sp_merge_sdi_gold_sa360_campaign_daily.sql
            │   │   # Recurring incremental build/update Gold daily WIDE (from Silver)
            │   ├── 01_sp_merge_sdi_gold_sa360_campaign_daily_long.sql
            │   │   # Recurring incremental build/update Gold daily LONG (from Gold daily WIDE)
            │   ├── 01_sp_merge_sdi_gold_sa360_campaign_weekly.sql
            │   │   # Recurring incremental build/update Gold weekly WIDE (from Gold daily)
            │   └── 01_sp_merge_sdi_gold_sa360_campaign_weekly_long.sql
            │       # Recurring incremental build/update Gold weekly LONG (from Gold weekly WIDE)
            │
            ├── Orchestration/
            │   └── 02_sp_gold_sa360_master_orchestration.sql
            │       # Master Gold procedure: daily wide → daily long → weekly wide → weekly long
            │
            ├── Views/
            │   ├── vw_sdi_gold_sa360_ps_daily_wide.sql
            │   │   # Final daily WIDE consumption view (dashboards/analysts)
            │   ├── vw_sdi_gold_sa360_ps_daily_long.sql
            │   │   # Final daily LONG consumption view (metric-driven dashboards)
            │   ├── vw_sdi_gold_sa360_ps_weekly_wide.sql
            │   │   # Final weekly WIDE consumption view
            │   └── vw_sdi_gold_sa360_ps_weekly_long.sql
            │       # Final weekly LONG consumption view
            │
            └── tests/
                ├── 00_create_sdi_gold_sa360_test_results.sql
                │   # Creates Gold test_results table
                ├── 01_sp_gold_campaign_daily_critical.sql
                │   # Gold daily WIDE critical tests (keys, duplicates, freshness)
                ├── 02_sp_gold_campaign_daily_reconciliation.sql
                │   # Gold daily WIDE reconciliation (Gold vs Silver)
                ├── 03_sp_gold_campaign_weekly_critical.sql
                │   # Gold weekly WIDE critical tests (QGP week validity, keys, duplicates)
                ├── 04_sp_gold_campaign_weekly_reconciliation.sql
                │   # Gold weekly WIDE reconciliation (weekly == SUM(daily))
                ├── 05_sp_gold_campaign_long_daily_critical.sql
                │   # Gold daily LONG critical tests (keys, duplicates)
                ├── 06_sp_gold_campaign_long_daily_reconciliation.sql
                │   # Gold daily LONG reconciliation (long == wide for metric subsets)
                ├── 07_sp_gold_campaign_long_weekly_critical.sql
                │   # Gold weekly LONG critical tests (keys, duplicates, QGP week validity)
                ├── 08_sp_gold_campaign_long_weekly_reconciliation.sql
                │   # Gold weekly LONG reconciliation (long == wide)
                ├── 09_sp_gold_campaign_long_bronze_reconciliation.sql
                │   # End-to-end reconciliation (Gold LONG vs Bronze baseline metrics)
                ├── 10_sp_gold_sa360_qa_master_orchestration.sql
                │   # Runs all Gold QA procs and writes outcomes
                ├── 11_view_gold_test_dashboard.sql
                │   # Gold-only QA dashboard view
                ├── 99_view_sa360_test_dashboard_all_layers.sql
                │   # Combined QA dashboard across Bronze + Silver + Gold
                ├── 99_view_sa360_test_dashboard.sql
                │   # Unified/latest SA360 QA dashboard view (single-pane)
                └── 99_view_sa360_test_summary.sql
                    # QA summary (pass/fail counts by layer + severity)
```
## What Each Major Folder Does

- **`sql/01_common`**: Shared building blocks used across layers (common date logic, QGP week standardization, reusable calendar mappings).
- **`sql/02_bronze`**: Raw/source-faithful ingestion with minimal standardization; first clean landing layer for source data.
- **`sql/03_silver`**: Business logic layer for cleaning, mapping, normalization, and core transformation rules.
- **`sql/04_gold`**: Final curated reporting layer producing daily/weekly, wide/long, dashboard-ready and analyst-ready datasets.
- **`tests/` (inside each layer)**: Layer-specific QA (critical tests, reconciliation tests, QA master orchestration, dashboard views).
- **`Views/` (inside Gold)**: Final consumption views for dashboards, analysts, reporting tools, and downstream services/models.

## Current Data Domain (Implemented)

### SA360 (Search Ads 360) Pipeline
Implemented end-to-end medallion pipeline with:
- Bronze daily + entity ingestion
- Silver daily business transformation
- Gold daily/weekly datasets (wide + long)
- QA dashboards + cross-layer reconciliation
- Backfill + incremental orchestration

This is a reusable production pattern for other sources (e.g., Google Ads, Meta Ads, Search Console).

## Execution Model

- SQL runs in **BigQuery**
- Core pipelines are orchestrated via **stored procedures**
- Trigger options: **BigQuery Scheduled Queries**, **Composer/Airflow**, **CI/CD jobs**, or **manual runs** (backfill/debugging)

**Dependency rule:** `Bronze -> Silver -> Gold`  
Gold QA should pass before dashboards consume Gold Views.

## End-to-End Execution Order (Runbook)

### 1) First-Time Setup (One-Time)
- Run `sql/01_common` objects
- Create Bronze DDL + Bronze test-results table
- Create Silver DDL + Silver test-results table
- Create Gold DDL + Gold test-results table
- Create QA dashboard views (Bronze/Silver/Gold + unified)
- Create final Gold reporting views

### 2) Historical Load (One-Time Backfill)
- Bronze backfill -> Bronze QA -> review dashboard
- Silver backfill -> Silver QA -> review dashboard
- Gold backfill (daily wide, daily long, weekly wide, weekly long)
- Gold QA -> review unified QA dashboards

### 3) Daily / Recurring Production Run
- Bronze Master Orchestration
- Bronze QA Master
- Silver Master Orchestration
- Silver QA Master
- Gold Master Orchestration
- Gold QA Master
- Review unified QA dashboard + summary
- Dashboards/analysts consume Gold Views

## QA Strategy

- **Bronze QA**: Validates ingestion correctness (row counts, key validity, duplicates, reconciliation to raw)
- **Silver QA**: Validates transformation correctness (mappings, derivations, reconciliation to Bronze)
- **Gold QA**: Validates reporting correctness (daily vs weekly, wide vs long, end-to-end reconciliation to Bronze baseline)
- **Dashboard QA Views**: Operational monitoring with PASS/FAIL, failure reasons, severity, and next-step guidance

## Design Principles

- Idempotent (safe reruns)
- Auditable (traceable across layers)
- Layered separation of concerns
- Extensible for new sources/domains
- QA is first-class (not ad hoc)
- Gold is decoupled from any single dashboard tool

## Non-Goals

This platform intentionally does **not**:
- Embed dashboard-specific logic in Bronze
- Mix ingestion and reporting logic
- Skip reconciliation checks
- Hard-code a single downstream consumer

## How to Get Started

Clone the repository:

```bash
git clone https://github.com/khldsqmr/Digital-Analytics-Data-Platform.git
cd Digital-Analytics-Data-Platform