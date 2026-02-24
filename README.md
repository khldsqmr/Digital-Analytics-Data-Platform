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
DIGITAL-ANALYTICS-DATA-PLATFORM/
│
├── docs/
│   ├── architecture/
│   │   # Architecture diagrams, design decisions, and data flow notes
│   └── bootstrap/
│       # One-time setup notes (datasets, procedures, first-run instructions)
│
├── infra/
│   # Infrastructure setup (datasets, IAM, schedulers, IaC) [current/future]
│
├── orchestration/
│   # External orchestration wrappers (Airflow/Composer/CI schedulers) [current/future]
│
├── refs/
│   └── sa360_Sanity_Check_1.sql
│       # Manual ad-hoc sanity check query for quick validation/debugging
│
└── sql/
    │
    ├── 01_common/
    │   ├── 00_fn_qgp_week.sql
    │   │   # Shared function to derive/standardize QGP week
    │   └── 01_vw_qgp_calendar.sql
    │       # Calendar mapping view (date ↔ qgp_week) used across layers
    │
    ├── 02_bronze/
    │   ├── ad_media/
    │   │   # Bronze pipelines for ad media sources (domain-specific)
    │   ├── google_ads/
    │   │   # Bronze pipelines for Google Ads (domain-specific)
    │   ├── meta_ads/
    │   │   # Bronze pipelines for Meta Ads (domain-specific)
    │   ├── search_console/
    │   │   # Bronze pipelines for Search Console (domain-specific)
    │   │
    │   └── sa360 (via procedures)/
    │       │
    │       ├── DDL/
    │       │   ├── 00_create_sdi_bronze_sa360_campaign_daily.sql
    │       │   │   # Creates Bronze daily campaign landing table (source-faithful standardized raw)
    │       │   └── 00_create_sdi_bronze_sa360_campaign_entity.sql
    │       │       # Creates Bronze campaign entity table (campaign metadata/dimensions)
    │       │
    │       ├── Backfill/
    │       │   ├── 00_backfill_bronze_sa360_campaign_daily.sql
    │       │   │   # One-time historical backfill into Bronze daily
    │       │   └── 01_backfill_bronze_sa360_campaign_entity.sql
    │       │       # One-time historical backfill into Bronze entity
    │       │
    │       ├── MERGE/
    │       │   ├── 01_merge_sdi_bronze_sa360_campaign_daily.sql
    │       │   │   # Recurring incremental MERGE (raw → Bronze daily)
    │       │   └── 01_merge_sdi_bronze_sa360_campaign_entity.sql
    │       │       # Recurring incremental MERGE (raw → Bronze entity)
    │       │
    │       ├── Orchestration/
    │       │   └── 01_sp_bronze_sa360_master_orchestration.sql
    │       │       # Master Bronze stored procedure to run Bronze pipeline steps in order
    │       │
    │       └── tests/
    │           ├── 00_create_sdi_bronze_sa360_test_results.sql
    │           │   # Creates Bronze QA results table to store PASS/FAIL test outcomes
    │           ├── 01_sp_bronze_campaign_daily_critical.sql
    │           │   # Critical QA tests on Bronze daily (nulls, duplicates, invalid IDs/keys)
    │           ├── 02_sp_bronze_campaign_daily_reconciliation.sql
    │           │   # Reconciliation QA: Bronze daily vs raw source totals/counts
    │           ├── 03_sp_bronze_campaign_entity_critical.sql
    │           │   # Critical QA tests on Bronze entity metadata table
    │           ├── 04_sp_bronze_campaign_entity_reconciliation.sql
    │           │   # Reconciliation QA: Bronze entity vs raw entity source
    │           ├── 05_sp_bronze_weekly_deep_validation.sql
    │           │   # Additional deeper weekly checks on Bronze metrics/trends
    │           ├── 06_sp_bronze_sa360_qa_master_orchestration.sql
    │           │   # Runs all Bronze QA procedures and writes outcomes to test_results
    │           └── 07_view_bronze_test_dashboard.sql
    │               # Creates Bronze QA dashboard view for easy PASS/FAIL monitoring
    │
    ├── 03_silver/
    │   └── sa360 (via procedures)/
    │       │
    │       ├── DDL/
    │       │   └── 00_create_sdi_silver_sa360_campaign_daily.sql
    │       │       # Creates Silver daily table (cleaned + normalized + business logic applied)
    │       │
    │       ├── Backfill/
    │       │   └── 00_backfill_silver_sa360_campaign_daily.sql
    │       │       # One-time historical backfill Bronze → Silver
    │       │
    │       ├── MERGE/
    │       │   └── 01_merge_sdi_silver_sa360_campaign_daily.sql
    │       │       # Recurring incremental MERGE Bronze → Silver with business transformations
    │       │
    │       ├── Orchestration/
    │       │   └── 01_sp_silver_sa360_master_orchestration.sql
    │       │       # Master Silver stored procedure to run Silver transformation steps
    │       │
    │       └── tests/
    │           ├── 00_create_sdi_silver_sa360_test_results.sql
    │           │   # Creates Silver QA results table
    │           ├── 01_sp_silver_campaign_daily_critical.sql
    │           │   # Critical QA tests on Silver daily (keys/nulls/duplicates)
    │           ├── 02_sp_silver_campaign_daily_reconciliation.sql
    │           │   # Reconciliation QA: Silver vs Bronze totals/counts
    │           ├── 03_sp_silver_campaign_daily_business_logic.sql
    │           │   # Validates mappings, derivations, and business logic applied in Silver
    │           ├── 06_sp_silver_sa360_qa_master_orchestration.sql
    │           │   # Runs all Silver QA procedures and writes outcomes
    │           └── 07_view_silver_test_dashboard.sql
    │               # Creates Silver QA dashboard view
    │
    └── 04_gold/
        └── sa360 (via procedures)/
            │
            ├── DDL/
            │   ├── 00_create_sdi_gold_sa360_campaign_daily.sql
            │   │   # Creates Gold daily WIDE table (one row = many metric columns)
            │   ├── 00_create_sdi_gold_sa360_campaign_daily_long.sql
            │   │   # Creates Gold daily LONG table (metric_name + metric_value format)
            │   ├── 00_create_sdi_gold_sa360_campaign_weekly.sql
            │   │   # Creates Gold weekly WIDE table (weekly aggregated metrics)
            │   └── 00_create_sdi_gold_sa360_campaign_weekly_long.sql
            │       # Creates Gold weekly LONG table
            │
            ├── Backfill/
            │   ├── 00_backfill_gold_sa360_campaign_daily.sql
            │   │   # One-time historical backfill Silver → Gold daily WIDE
            │   ├── 01_backfill_gold_sa360_campaign_daily_long.sql
            │   │   # One-time historical unpivot Gold daily WIDE → Gold daily LONG
            │   ├── 01_backfill_gold_sa360_campaign_weekly.sql
            │   │   # One-time historical aggregation Gold daily → Gold weekly WIDE
            │   └── 01_backfill_gold_sa360_campaign_weekly_long.sql
            │       # One-time historical unpivot Gold weekly WIDE → Gold weekly LONG
            │       # Safety rule: excludes future qgp_week buckets
            │
            ├── MERGE/
            │   ├── 01_sp_merge_sdi_gold_sa360_campaign_daily.sql
            │   │   # Recurring incremental build/update for Gold daily WIDE
            │   ├── 01_sp_merge_sdi_gold_sa360_campaign_daily_long.sql
            │   │   # Recurring incremental build/update for Gold daily LONG from daily WIDE
            │   ├── 01_sp_merge_sdi_gold_sa360_campaign_weekly.sql
            │   │   # Recurring incremental build/update for Gold weekly WIDE
            │   └── 01_sp_merge_sdi_gold_sa360_campaign_weekly_long.sql
            │       # Recurring incremental build/update for Gold weekly LONG from weekly WIDE
            │
            ├── Orchestration/
            │   └── 02_sp_gold_sa360_master_orchestration.sql
            │       # Master Gold stored procedure:
            │       # runs daily wide → daily long → weekly wide → weekly long in sequence
            │
            ├── tests/
            │   ├── 00_create_sdi_gold_sa360_test_results.sql
            │   │   # Creates Gold QA results table
            │   ├── 01_sp_gold_campaign_daily_critical.sql
            │   │   # Critical QA tests on Gold daily WIDE
            │   ├── 02_sp_gold_campaign_daily_reconciliation.sql
            │   │   # Reconciliation QA: Gold daily WIDE vs expected upstream totals
            │   ├── 03_sp_gold_campaign_weekly_critical.sql
            │   │   # Critical QA tests on Gold weekly WIDE
            │   ├── 04_sp_gold_campaign_weekly_reconciliation.sql
            │   │   # Reconciliation QA: Gold weekly WIDE vs Gold daily rollups
            │   ├── 05_sp_gold_campaign_long_daily_critical.sql
            │   │   # Critical QA tests on Gold daily LONG
            │   ├── 06_sp_gold_campaign_long_daily_reconciliation.sql
            │   │   # Reconciliation QA: Gold daily LONG vs Gold daily WIDE
            │   ├── 07_sp_gold_campaign_long_weekly_critical.sql
            │   │   # Critical QA tests on Gold weekly LONG
            │   ├── 08_sp_gold_campaign_long_weekly_reconciliation.sql
            │   │   # Reconciliation QA: Gold weekly LONG vs Gold weekly WIDE
            │   ├── 09_sp_gold_campaign_long_bronze_reconciliation.sql
            │   │   # End-to-end reconciliation: Gold LONG vs Bronze baseline
            │   ├── 10_sp_gold_sa360_qa_master_orchestration.sql
            │   │   # Runs all Gold QA procedures and writes outcomes
            │   ├── 11_view_gold_test_dashboard.sql
            │   │   # Creates Gold-only QA dashboard view
            │   ├── 99_view_sa360_test_dashboard_all_layers.sql
            │   │   # Combined QA dashboard view across Bronze + Silver + Gold
            │   ├── 99_view_sa360_test_dashboard.sql
            │   │   # Unified/latest SA360 QA dashboard view (single pane)
            │   └── 99_view_sa360_test_summary.sql
            │       # Summary QA view (pass/fail counts by layer, severity, date)
            │
            └── Views/
                ├── vw_sdi_gold_sa360_ps_daily_long.sql
                │   # Final daily LONG reporting view (best for metric-driven dashboards)
                ├── vw_sdi_gold_sa360_ps_daily_wide.sql
                │   # Final daily WIDE reporting view (best for easy analyst consumption)
                ├── vw_sdi_gold_sa360_ps_weekly_long.sql
                │   # Final weekly LONG reporting view
                └── vw_sdi_gold_sa360_ps_weekly_wide.sql
                    # Final weekly WIDE reporting view

# What Each Major Folder Does?

sql/01_common
# Shared building blocks used across layers:
# - common date logic
# - qgp week standardization
# - reusable calendar mappings

sql/02_bronze
# Raw/source-faithful ingestion + minimal standardization
# First clean landing layer for source data

sql/03_silver
# Business logic layer
# Cleans, maps, normalizes, and applies transformation rules

sql/04_gold
# Final curated reporting layer
# Produces:
# - daily / weekly datasets
# - wide / long models
# - dashboard-ready / analyst-ready outputs

tests/ (inside each layer)
# Layer-specific QA:
# - critical tests
# - reconciliation tests
# - QA master orchestration
# - dashboard views for monitoring

Views/ (inside Gold)
# Final consumption views for:
# - dashboards
# - analysts
# - reporting tools
# - downstream models/services


# Current Data Domain (Implemented in Detail)

SA360 (Search Ads 360) Pipeline
# Implemented end-to-end medallion pipeline with:
# - Bronze daily + entity ingestion
# - Silver daily business transformation
# - Gold daily/weekly (wide + long)
# - QA dashboards + cross-layer reconciliation
# - backfill + incremental orchestration
# Reusable pattern for other sources (Google Ads, Meta, Search Console, etc.)


# Execution Model

# SQL runs in BigQuery
# Core pipelines run via stored procedures
# Trigger options:
# - BigQuery Scheduled Queries
# - Composer / Airflow
# - CI/CD jobs
# - manual runs (backfill/debugging)

# Dependency rule:
# Bronze -> Silver -> Gold
# Gold QA should pass before dashboards consume Gold Views


# End-to-End Execution Order (Runbook)

1) First-Time Setup (one-time)
# - Run sql/01_common objects
# - Create Bronze DDL + Bronze test results table
# - Create Silver DDL + Silver test results table
# - Create Gold DDL + Gold test results table
# - Create QA dashboard views (Bronze/Silver/Gold + unified)
# - Create final Gold reporting views

2) Historical Load (one-time backfill)
# - Bronze backfill -> Bronze QA -> review dashboard
# - Silver backfill -> Silver QA -> review dashboard
# - Gold backfill (daily wide, daily long, weekly wide, weekly long)
# - Gold QA -> review unified QA dashboards

3) Daily / Recurring Production Run
# - Bronze Master Orchestration
# - Bronze QA Master
# - Silver Master Orchestration
# - Silver QA Master
# - Gold Master Orchestration
# - Gold QA Master
# - Review unified QA dashboard + summary
# - Dashboards/analysts consume Gold Views


# QA Strategy (Why This Matters)

Bronze QA
# Validates ingestion correctness:
# - row counts
# - key validity
# - duplicates
# - reconciliation to raw

Silver QA
# Validates transformation correctness:
# - mappings
# - derivations
# - reconciliation to Bronze

Gold QA
# Validates reporting correctness:
# - daily vs weekly consistency
# - wide vs long consistency
# - end-to-end reconciliation to Bronze baseline

Dashboard QA Views
# Makes QA operational:
# - PASS/FAIL visibility
# - failure reasons
# - severity
# - next-step guidance


# Design Principles

# - Idempotent (safe reruns)
# - Auditable (traceable across layers)
# - Layered separation of concerns
# - Extensible for new sources/domains
# - QA is first-class (not ad hoc)
# - Gold is decoupled from any single dashboard tool


# Non-Goals (By Design)

# This platform intentionally does NOT:
# - embed dashboard-specific logic in Bronze
# - mix ingestion and reporting logic
# - skip reconciliation checks
# - hard-code one downstream consumer


# How to Get Started

# Clone the repo:
git clone https://github.com/khldsqmr/Digital-Analytics-Data-Platform.git
cd Digital-Analytics-Data-Platform

# Then start with:
# 1. docs/bootstrap/
# 2. sql/01_common/
# 3. Bronze DDL + Bronze test-results DDL
# 4. Silver DDL + Silver test-results DDL
# 5. Gold DDL + Gold test-results DDL
# 6. Backfill (if needed)
# 7. QA orchestration + dashboards
# 8. Schedule recurring runs


# Ownership

# Domain: Digital Analytics
# Architecture: Medallion (Bronze / Silver / Gold)
# Warehouse: BigQuery
# Language: SQL
# Pattern: Procedure-driven orchestration + QA dashboards


# Notes for Contributors

# For any new source/domain, follow the same pattern:
# - DDL/            -> table creation
# - Backfill/       -> historical load
# - MERGE/          -> recurring incremental logic
# - Orchestration/  -> master procedure
# - tests/          -> QA + reconciliation
# - Views/ (Gold)   -> final consumption layer