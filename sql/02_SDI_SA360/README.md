# SA360 (Search Ads 360) - Digital Analytics Data Platform

This folder implements a **production-ready SA360 pipeline on BigQuery** using a **Medallion Architecture (Bronze → Silver → Gold)**.

It is the **single source of truth for SA360**:
- table/view definitions (DDL)
- backfill + incremental MERGE logic
- stored-procedure orchestration
- QA + reconciliation tests (layer-level + cross-layer)
- monitoring views (QA dashboards + summaries)

The goal is simple: **reliable, auditable, and repeatable SA360 reporting** where every layer has a clear responsibility and QA is built-in.

---

## What This Pipeline Produces (Outputs)

### Bronze (Landing)
- **`sdi_bronze_sa360_campaign_daily`**: daily performance metrics at campaign-day grain  
- **`sdi_bronze_sa360_campaign_entity`**: daily “settings snapshot” at campaign-day grain

### Silver (Business Logic)
- **`sdi_silver_sa360_campaign_daily`**: cleaned + enriched daily dataset  
  (LOB + platform + campaign_type + as-of entity attributes)

### Gold (Consumption)
- **Daily WIDE**: `sdi_gold_sa360_campaign_daily`
- **Daily LONG**: `sdi_gold_sa360_campaign_daily_long`
- **Weekly WIDE**: `sdi_gold_sa360_campaign_weekly`
- **Weekly LONG**: `sdi_gold_sa360_campaign_weekly_long`
- **Final Views for dashboards** (daily/weekly, wide/long) under `Views/`

---

## Architecture (Medallion Layers)

### Bronze — Source-faithful landing (auditable)
**Purpose:** land raw SA360 data in a clean, standardized, replayable format.

Key points:
- minimal transformation (standardized naming + canonical date parsing)
- incremental MERGE with lookback window for late arriving files
- dedupe inside the merge window using deterministic ordering
- designed for audit + troubleshooting (keeps history)

### Silver — Business logic + normalization
**Purpose:** apply standard transformations once, consistently.

Key points:
- enrichment using **as-of join** to entity snapshot (e.date <= d.date)
- derived fields: LOB, ad_platform, campaign_type, etc.
- ensures downstream reporting does not re-implement logic per dashboard

### Gold — Curated consumption layer
**Purpose:** final reporting-ready outputs.

Key points:
- produces both **daily and weekly** datasets
- supports both **wide and long** models
- includes cross-check QA (daily vs weekly, wide vs long, Gold vs Bronze baselines)
- final views are decoupled from dashboards (Tableau/others are consumers)

---

## Tech Stack / Execution Model

- **Warehouse:** BigQuery  
- **SQL:** BigQuery Standard SQL  
- **Orchestration:** Stored Procedures called by:
  - BigQuery Scheduled Queries
  - Composer/Airflow
  - CI/CD jobs
  - manual backfill/debug runs

**Dependency rule (strict):**  
`Bronze → Bronze QA → Silver → Silver QA → Gold → Gold QA → Views/Dashboards`

---

## Repository Structure (SA360 Focus)

> This tree is scoped to the SA360 pipeline and includes short, manager-friendly explanations.

```text
DIGITAL-ANALYTICS-DATA-PLATFORM/
├── orchestration/
│   └── bigquery/
│       └── Paid Search Dashboard Orchestration/
│           └── 00_sdi_sa360_paid_search_sp_call.sql
│               # Scheduler wrapper that CALLs SA360 master procedures (production trigger)
│
├── refs/
│   └── sa360_Sanity_Check_1.sql
│       # Manual ad-hoc sanity query for quick debugging (not part of scheduled pipeline)
│
└── sql/
    ├── 01_common/
    │   ├── 00_fn_qgp_week.sql
    │   │   # Standard QGP week function (used by weekly Gold + QA)
    │   └── 01_vw_qgp_calendar.sql
    │       # Calendar spine (date ↔ qgp_week mapping)
    │
    └── 02_SDI_SA360/
        ├── 00_COMMON/
        │   ├── 00_fn_qgp_week.sql
        │   └── 01_vw_qgp_calendar.sql
        │       # Optional SA360-local copies (prefer sql/01_common when possible)
        │
        ├── 01_BRONZE/                         # Raw → Landing layer
        │   ├── DDL/
        │   │   ├── 00_create_sdi_bronze_sa360_campaign_daily.sql
        │   │   │   # Create Bronze daily fact table (campaign × day)
        │   │   └── 00_create_sdi_bronze_sa360_campaign_entity.sql
        │   │       # Create Bronze entity snapshot table (campaign settings by day)
        │   │
        │   ├── Backfill/
        │   │   ├── 00_backfill_bronze_sa360_campaign_daily.sql
        │   │   │   # One-time historical load for Bronze daily (chunked)
        │   │   └── 01_backfill_bronze_sa360_campaign_entity.sql
        │   │       # One-time historical load for Bronze entity (chunked)
        │   │
        │   ├── MERGE/
        │   │   ├── 01_merge_sdi_bronze_sa360_campaign_daily.sql
        │   │   │   # Daily incremental MERGE (RAW → Bronze daily) with lookback + dedupe
        │   │   └── 01_merge_sdi_bronze_sa360_campaign_entity.sql
        │   │       # Daily incremental MERGE (RAW → Bronze entity) with lookback + dedupe
        │   │
        │   ├── Orchestration/
        │   │   └── 01_sp_bronze_sa360_master_orchestration.sql
        │   │       # Runs Bronze merges (daily + entity) in correct order
        │   │
        │   └── tests/
        │       ├── 00_create_sdi_bronze_sa360_test_results.sql
        │       │   # Bronze QA results table (stores PASS/FAIL outcomes)
        │       ├── 01_sp_bronze_campaign_daily_critical.sql
        │       │   # Critical checks: null keys, duplicates, invalid values
        │       ├── 02_sp_bronze_campaign_daily_reconciliation.sql
        │       │   # Reconcile Bronze daily vs RAW-dedup (row counts + key metrics)
        │       ├── 03_sp_bronze_campaign_entity_critical.sql
        │       │   # Critical checks on entity snapshot table
        │       ├── 04_sp_bronze_campaign_entity_reconciliation.sql
        │       │   # Reconcile Bronze entity vs RAW-dedup (non-append-only aware)
        │       ├── 05_sp_bronze_weekly_deep_validation.sql
        │       │   # Weekly anomaly checks for focus metrics (lightweight)
        │       ├── 06_sp_bronze_sa360_qa_master_orchestration.sql
        │       │   # Runs all Bronze QA procs and writes results
        │       └── 07_view_bronze_test_dashboard.sql
        │           # Bronze QA dashboard view (operational monitoring)
        │
        ├── 02_SILVER/                         # Business logic layer
        │   ├── DDL/
        │   │   └── 00_create_sdi_silver_sa360_campaign_daily.sql
        │   │       # Create Silver daily (enriched + normalized)
        │   │
        │   ├── Backfill/
        │   │   └── 00_backfill_silver_sa360_campaign_daily.sql
        │   │       # One-time Bronze → Silver historical build
        │   │
        │   ├── MERGE/
        │   │   └── 01_merge_sdi_silver_sa360_campaign_daily.sql
        │   │       # Incremental Bronze → Silver build with as-of entity join
        │   │
        │   ├── Orchestration/
        │   │   └── 01_sp_silver_sa360_master_orchestration.sql
        │   │       # Runs Silver merge in correct order
        │   │
        │   └── tests/
        │       ├── 00_create_sdi_silver_sa360_test_results.sql
        │       │   # Silver QA results table
        │       ├── 01_sp_silver_campaign_daily_critical.sql
        │       │   # Critical checks on Silver daily
        │       ├── 02_sp_silver_campaign_daily_reconciliation.sql
        │       │   # Reconcile Silver vs Bronze (ensures no metric drift)
        │       ├── 03_sp_silver_campaign_daily_business_logic.sql
        │       │   # Validate business logic outputs (LOB/platform/type rules)
        │       ├── 06_sp_silver_sa360_qa_master_orchestration.sql
        │       │   # Runs all Silver QA tests and writes results
        │       └── 07_view_silver_test_dashboard.sql
        │           # Silver QA dashboard view
        │
        └── 03_GOLD/                           # Reporting + consumption layer
            ├── DDL/
            │   ├── 00_create_sdi_gold_sa360_campaign_daily.sql
            │   ├── 00_create_sdi_gold_sa360_campaign_daily_long.sql
            │   ├── 00_create_sdi_gold_sa360_campaign_weekly.sql
            │   └── 00_create_sdi_gold_sa360_campaign_weekly_long.sql
            │       # Create Gold reporting tables (daily/weekly × wide/long)
            │
            ├── Backfill/
            │   ├── 00_backfill_gold_sa360_campaign_daily.sql
            │   ├── 01_backfill_gold_sa360_campaign_daily_long.sql
            │   ├── 01_backfill_gold_sa360_campaign_weekly.sql
            │   └── 01_backfill_gold_sa360_campaign_weekly_long.sql
            │       # One-time historical builds (includes future-week guard for weekly long)
            │
            ├── MERGE/
            │   ├── 01_sp_merge_sdi_gold_sa360_campaign_daily.sql
            │   ├── 01_sp_merge_sdi_gold_sa360_campaign_daily_long.sql
            │   ├── 01_sp_merge_sdi_gold_sa360_campaign_weekly.sql
            │   └── 01_sp_merge_sdi_gold_sa360_campaign_weekly_long.sql
            │       # Daily incremental builds for Gold (wide → long → weekly wide → weekly long)
            │
            ├── Orchestration/
            │   └── 02_sp_gold_sa360_master_orchestration.sql
            │       # Runs Gold pipeline in sequence and ensures dependencies
            │
            ├── Views/
            │   ├── vw_sdi_gold_sa360_ps_daily_wide.sql
            │   ├── vw_sdi_gold_sa360_ps_daily_long.sql
            │   ├── vw_sdi_gold_sa360_ps_weekly_wide.sql
            │   └── vw_sdi_gold_sa360_ps_weekly_long.sql
            │       # Final consumption views used by dashboards + analysts
            │
            └── tests/
                ├── 00_create_sdi_gold_sa360_test_results.sql
                ├── 01_sp_gold_campaign_daily_critical.sql
                ├── 02_sp_gold_campaign_daily_reconciliation.sql
                ├── 03_sp_gold_campaign_weekly_critical.sql
                ├── 04_sp_gold_campaign_weekly_reconciliation.sql
                ├── 05_sp_gold_campaign_long_daily_critical.sql
                ├── 06_sp_gold_campaign_long_daily_reconciliation.sql
                ├── 07_sp_gold_campaign_long_weekly_critical.sql
                ├── 08_sp_gold_campaign_long_weekly_reconciliation.sql
                ├── 09_sp_gold_campaign_long_bronze_reconciliation.sql
                ├── 10_sp_gold_sa360_qa_master_orchestration.sql
                ├── 11_view_gold_test_dashboard.sql
                ├── 99_view_sa360_test_dashboard_all_layers.sql
                ├── 99_view_sa360_test_dashboard.sql
                └── 99_view_sa360_test_summary.sql
                    # Gold QA + cross-layer QA + unified monitoring views

```
## How QA Works ?

This SA360 pipeline is designed to answer four questions clearly and consistently:

1) **Did ingestion load correctly?**  
   **Bronze vs RAW reconciliation** confirms row-count + key-metric totals match the deduped RAW snapshot for the same window.

2) **Did business logic change totals unexpectedly?**  
   **Silver vs Bronze reconciliation** confirms transformations/enrichments did **not drift** core metrics.

3) **Are reporting tables internally consistent?**  
   **Gold consistency checks** confirm:
   - **Daily vs Weekly** (weekly rollups equal SUM(daily))
   - **Wide vs Long** (unpivoted long equals wide for the same metrics)

4) **Does Gold still match the baseline source metrics end-to-end?**  
   **Gold vs Bronze focused reconciliation** ensures selected “source-of-truth” metrics still reconcile after all transformations.

**Operational rule:** every QA test writes **exactly 1 row per test** into a `test_results` table and is exposed through dashboard views with:
- PASS/FAIL
- severity
- failure reason
- next-step guidance

---

## Stored Procedures (Major Ones vs Minor Ones)

### Major “Master” Procedures (the ones you schedule/call)

These are the entrypoints that orchestrate many steps in the correct order:

- **Bronze build**
  - `sp_bronze_sa360_master_orchestration`
    - runs Bronze MERGEs (daily + entity)

- **Bronze QA**
  - `sp_bronze_sa360_qa_master_orchestration`
    - runs all Bronze tests and writes results to `sdi_bronze_sa360_test_results`

- **Silver build**
  - `sp_silver_sa360_master_orchestration`
    - runs Silver MERGE (Bronze → Silver)

- **Silver QA**
  - `sp_silver_sa360_qa_master_orchestration`
    - runs all Silver tests and writes results to `sdi_silver_sa360_test_results`

- **Gold build**
  - `sp_gold_sa360_master_orchestration`
    - runs Gold builds in sequence:
      1) daily wide
      2) daily long
      3) weekly wide
      4) weekly long

- **Gold QA**
  - `sp_gold_sa360_qa_master_orchestration`
    - runs all Gold tests and writes results to `sdi_gold_sa360_test_results`
    - also feeds unified QA dashboards (`99_*` views)

### Minor “Step” Procedures (called by masters)

Examples by layer:

- Bronze tests:
  - `sp_bronze_campaign_daily_critical`
  - `sp_bronze_campaign_daily_reconciliation`
  - `sp_bronze_campaign_entity_critical`
  - `sp_bronze_campaign_entity_reconciliation`
  - `sp_bronze_weekly_deep_validation`

- Silver tests:
  - `sp_silver_campaign_daily_critical`
  - `sp_silver_campaign_daily_reconciliation`
  - `sp_silver_campaign_daily_business_logic`

- Gold tests:
  - `sp_gold_campaign_daily_critical`
  - `sp_gold_campaign_daily_reconciliation`
  - `sp_gold_campaign_weekly_critical`
  - `sp_gold_campaign_weekly_reconciliation`
  - `sp_gold_campaign_long_daily_critical`
  - `sp_gold_campaign_long_daily_reconciliation`
  - `sp_gold_campaign_long_weekly_critical`
  - `sp_gold_campaign_long_weekly_reconciliation`
  - `sp_gold_campaign_long_bronze_reconciliation` (focused end-to-end baseline checks)

---

## SA360 Daily Orchestration Schedule

**Schedule:** Daily @ **6:30 AM EST**  
**Full run sequence (build + QA):**

1. `CALL prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_master_orchestration();`
2. `CALL prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_qa_master_orchestration();`
3. `CALL prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_sa360_master_orchestration();`
4. `CALL prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_sa360_qa_master_orchestration();`
5. `CALL prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_master_orchestration();`
6. `CALL prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_qa_master_orchestration();`

**Operational expectation:** Dashboards consume **Gold Views** only after this sequence completes and QA is healthy (PASS).

---

## Runbook (End-to-End)

### 1) First-time setup (one time per environment)
1. Create common objects: `sql/01_common/*`
2. Run Bronze DDL + Bronze `test_results` DDL
3. Run Silver DDL + Silver `test_results` DDL
4. Run Gold DDL + Gold `test_results` DDL
5. Create QA dashboard views + final reporting views

### 2) Historical load (one-time backfill)
1. Bronze backfill (daily + entity)
2. **Run Bronze QA master** → review Bronze QA dashboard
3. Silver backfill
4. **Run Silver QA master** → review Silver QA dashboard
5. Gold backfill (daily wide → daily long → weekly wide → weekly long)
6. **Run Gold QA master** → review unified QA dashboards

### 3) Daily production run (recurring)
1. **Bronze master orchestration**
2. **Bronze QA master**
3. **Silver master orchestration**
4. **Silver QA master**
5. **Gold master orchestration**
6. **Gold QA master**
7. Dashboards consume **Gold Views** only when QA is healthy

---

## Design Principles (Why this is reliable)

- **Idempotent:** reruns are safe (MERGE patterns + deterministic dedupe)
- **Auditable:** Bronze keeps traceable records (lineage columns)
- **Separation of concerns:** ingestion ≠ transformation ≠ reporting
- **QA-first:** tests are stored, repeatable, and monitored (not ad-hoc)
- **Decoupled consumption:** dashboards read stable Gold views, not raw logic

---

## Non-goals (intentional)

This SA360 pipeline does **not**:
- embed dashboard-specific logic into Bronze/Silver
- skip reconciliation checks for “fast delivery”
- hard-code one downstream consumer as the only use case

---

## Quick Start (local/dev)

```bash
git clone https://github.com/khldsqmr/Digital-Analytics-Data-Platform.git
cd Digital-Analytics-Data-Platform