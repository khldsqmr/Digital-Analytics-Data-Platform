# Profound (AI Visibility) – Digital Analytics Data Platform

This folder implements a **production-ready Profound AI Visibility pipeline** on BigQuery using a **Medallion Architecture (Bronze → Silver → Gold)**.

It is the **single source of truth** for:

- AI Visibility (asset-level)
- AI Citations (domain-level)
- Topic breakdown
- Tag breakdown
- Topic + Tag breakdown

The pipeline ensures:

- Deterministic deduplication
- Strict grain enforcement
- Non-additive metric protection
- Idempotent incremental builds
- Layer-level QA + cross-layer reconciliation
- Orchestration-ready execution

---

# What This Pipeline Produces (Outputs)

## Bronze (Landing – Source Faithful)

Daily fact tables aligned to vendor grain:

- `sdi_profound_bronze_visibility_asset_daily`
- `sdi_profound_bronze_visibility_tag_daily`
- `sdi_profound_bronze_visibility_topic_daily`
- `sdi_profound_bronze_visibility_topic_tag_daily`
- `sdi_profound_bronze_citations_domain_daily`
- `sdi_profound_bronze_citations_tag_daily`
- `sdi_profound_bronze_citations_topic_daily`
- `sdi_profound_bronze_citations_topic_tag_daily`

---

## Silver (Clean + Grain Enforced)

- `sdi_profound_silver_visibility_asset_daily`
- `sdi_profound_silver_visibility_tag_daily`
- `sdi_profound_silver_visibility_topic_daily`
- `sdi_profound_silver_visibility_topic_tag_daily`
- `sdi_profound_silver_citations_domain_daily`
- `sdi_profound_silver_citations_tag_daily`
- `sdi_profound_silver_citations_topic_daily`
- `sdi_profound_silver_citations_topic_tag_daily`

---

## Gold (Dashboard-Ready)

- `sdi_profound_gold_visibility_asset_daily`
- `sdi_profound_gold_visibility_tag_daily`
- `sdi_profound_gold_visibility_topic_daily`
- `sdi_profound_gold_visibility_topic_tag_daily`
- `sdi_profound_gold_citations_domain_daily`
- `sdi_profound_gold_citations_tag_daily`
- `sdi_profound_gold_citations_topic_daily`
- `sdi_profound_gold_citations_topic_tag_daily`

Consumption Views:

- `vw_sdi_profound_visibility_asset_daily`
- `vw_sdi_profound_visibility_tag_daily`
- `vw_sdi_profound_visibility_topic_daily`
- `vw_sdi_profound_visibility_topic_tag_daily`
- `vw_sdi_profound_citations_domain_daily`
- `vw_sdi_profound_citations_tag_daily`
- `vw_sdi_profound_citations_topic_daily`
- `vw_sdi_profound_citations_topic_tag_daily`

---

# Architecture (Medallion Layers)

## Bronze – Source-Faithful Landing (Auditable)

**Purpose:** Land vendor data with minimal transformation.

Key Responsibilities:

- Standardize column names
- Canonical date parsing
- Preserve vendor metrics
- Deterministic dedupe using `File_Load_datetime`
- Idempotent incremental MERGE
- Maintain traceability (`Filename`, `File_Load_datetime`)

**No business logic applied here.**

---

## Silver – Business Logic + Grain Enforcement

**Purpose:** Enforce primary key grain and clean data.

Key Responsibilities:

- Enforce strict grain per table
- Convert `date` / `date_yyyymmdd` → `DATE`
- Drop redundant fields
- Normalize `tag` / `topic` casing
- Validate metric ranges
- Deduplicate using latest load timestamp
- Prevent cross-grain aggregation

**No aggregation across levels (domain vs topic vs tag).**

---

## Gold – Consumption Layer

**Purpose:** Dashboard-ready curated outputs.

Key Responsibilities:

- Expose clean fact tables
- Preserve non-additive metrics
- Ensure reconciliation to Silver
- Provide stable views for BI tools
- Maintain metric consistency

**Dashboards must consume Gold Views only.**

---

# Grain Definitions (Strict)

### Visibility Tables

| Table | Grain |
|-------|-------|
| visibility_asset_daily | account_name + asset_name + event_date |
| visibility_tag_daily | account_name + asset_name + event_date + tag |
| visibility_topic_daily | account_name + asset_name + event_date + topic |
| visibility_topic_tag_daily | account_name + asset_name + event_date + topic + tag |

### Citation Tables

| Table | Grain |
|-------|-------|
| citations_domain_daily | account_name + root_domain + event_date |
| citations_tag_daily | account_name + root_domain + event_date + tag |
| citations_topic_daily | account_name + root_domain + event_date + topic |
| citations_topic_tag_daily | account_name + root_domain + event_date + topic + tag |

**Operational Rule:** Grain must never change between layers.

---

# Non-Additive Metrics (Critical)

The following metrics are NON-ADDITIVE and must never be aggregated across granularities:

- `share_of_voice`
- `visibility_score`

These must only be viewed at their native grain.

Summable metrics include:

- `count`
- `mentions_count`
- `executions`

---

# Repository Structure (Profound)

```bash
sql/
└── 03_SDI_PROFOUND/
    ├── 00_COMMON/
    │   └── (shared utility functions if needed)
    │
    ├── 01_BRONZE/
    │   ├── DDL/
    │   │   ├── 00_create_sdi_profound_bronze_visibility_asset_daily.sql
    │   │   ├── 00_create_sdi_profound_bronze_visibility_tag_daily.sql
    │   │   ├── 00_create_sdi_profound_bronze_visibility_topic_daily.sql
    │   │   ├── 00_create_sdi_profound_bronze_visibility_topic_tag_daily.sql
    │   │   ├── 00_create_sdi_profound_bronze_citations_domain_daily.sql
    │   │   ├── 00_create_sdi_profound_bronze_citations_tag_daily.sql
    │   │   ├── 00_create_sdi_profound_bronze_citations_topic_daily.sql
    │   │   └── 00_create_sdi_profound_bronze_citations_topic_tag_daily.sql
    │   │
    │   ├── Backfill/
    │   │   └── 00_backfill_sdi_profound_bronze_*.sql
    │   │
    │   ├── MERGE/
    │   │   └── 01_merge_sdi_profound_bronze_*.sql
    │   │
    │   ├── Orchestration/
    │   │   └── sp_sdi_profound_bronze_master_orchestration.sql
    │   │
    │   └── tests/
    │       ├── 00_create_sdi_profound_bronze_test_results.sql
    │       ├── 01_sp_sdi_profound_bronze_critical.sql
    │       ├── 02_sp_sdi_profound_bronze_reconciliation.sql
    │       ├── 03_sp_sdi_profound_bronze_duplicate_validation.sql
    │       └── 04_sp_sdi_profound_bronze_qa_master_orchestration.sql
    │
    ├── 02_SILVER/
    │   ├── DDL/
    │   ├── Backfill/
    │   ├── MERGE/
    │   ├── Orchestration/
    │   │   └── sp_sdi_profound_silver_master_orchestration.sql
    │   │
    │   └── tests/
    │       ├── 00_create_sdi_profound_silver_test_results.sql
    │       ├── 01_sp_sdi_profound_silver_critical.sql
    │       ├── 02_sp_sdi_profound_silver_grain_validation.sql
    │       ├── 03_sp_sdi_profound_silver_range_validation.sql
    │       ├── 04_sp_sdi_profound_silver_reconciliation.sql
    │       └── 05_sp_sdi_profound_silver_qa_master_orchestration.sql
    │
    └── 03_GOLD/
        ├── DDL/
        │   └── 00_create_sdi_profound_gold_*.sql
        │
        ├── Backfill/
        │   └── 00_backfill_sdi_profound_gold_*.sql
        │
        ├── MERGE/
        │   └── 01_merge_sdi_profound_gold_*.sql
        │
        ├── Views/
        │   └── vw_sdi_profound_*.sql
        │
        ├── Orchestration/
        │   └── sp_sdi_profound_gold_master_orchestration.sql
        │
        └── tests/
            ├── 00_create_sdi_profound_gold_test_results.sql
            ├── 01_sp_sdi_profound_gold_critical.sql
            ├── 02_sp_sdi_profound_gold_consistency.sql
            ├── 03_sp_sdi_profound_gold_metric_validation.sql
            ├── 04_sp_sdi_profound_gold_cross_layer_reconciliation.sql
            └── 05_sp_sdi_profound_gold_qa_master_orchestration.sql
```
# QA Framework

Each layer writes **exactly one row per test** into its respective test results table:

- `sdi_profound_bronze_test_results`
- `sdi_profound_silver_test_results`
- `sdi_profound_gold_test_results`

Each test record stores:

- `status` (PASS / FAIL)
- `severity`
- `failure_reason`
- `next_step`
- `expected_value`
- `actual_value`
- `variance_value`
- `test_run_timestamp`

This ensures every validation is traceable, auditable, and repeatable.

---

# Daily Orchestration

```sql
CALL sp_sdi_profound_bronze_master_orchestration();
CALL sp_sdi_profound_bronze_qa_master_orchestration();

CALL sp_sdi_profound_silver_master_orchestration();
CALL sp_sdi_profound_silver_qa_master_orchestration();

CALL sp_sdi_profound_gold_master_orchestration();
CALL sp_sdi_profound_gold_qa_master_orchestration();

**Dashboards refresh only after Gold QA passes.**

---

# Incremental Strategy

- **Bronze:** `MERGE` with configurable lookback window  
- **Silver:** `ROW_NUMBER()` grain enforcement using latest `File_Load_datetime`  
- **Gold:** Idempotent rebuild from Silver  
- All layers are safe to rerun  

---

# Design Principles

- Idempotent
- Grain-first modeling
- QA-first validation
- Layer isolation
- Dashboard decoupling
- Vendor change resilience
- Production-ready orchestration

---

# Non-Goals

This pipeline does **NOT**:

- Aggregate non-additive metrics
- Blend granularities
- Skip QA for faster delivery
- Embed dashboard-specific logic in Bronze or Silver

---

# Operational Expectation

Gold tables are the **only approved reporting layer**.

If QA fails:

- Dashboards must not refresh
- The issue must be investigated at the failing layer
- No manual override without documented approval

---

# Summary

The Profound pipeline guarantees:

- Reliable AI visibility reporting
- Deterministic deduplication
- Strict grain enforcement
- Non-additive metric safety
- Production-grade QA monitoring
- Clean orchestration and governance

It is built to be stable, scalable, and audit-ready.