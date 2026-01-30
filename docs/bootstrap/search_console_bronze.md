# Google Search Console – Bronze Bootstrap

This document describes the **one-time bootstrap requirements**
for Google Search Console Bronze tables.

---

## Purpose

The Bronze layer stores **raw, minimally transformed Google Search Console data**
with full fidelity to the source.

Incremental ingestion pipelines assume these tables already exist.
Bootstrap is executed **once per environment** to establish physical tables,
schema, and storage layout.

---

## Project & Dataset

- **Project:** `prj-dbi-prd-1`
- **Dataset:** `ds_dbi_digitalmedia_automation`
- **Location:** `us-west1`

---

## Bronze Tables (One-Time Setup)

### 1. Query-Level Daily Metrics

**Table**
sdi_bronze_search_console_query_daily


**Grain**
- date  
- account_name  
- site_url  
- page  
- query  
- search_type  

**Description**  
Stores the most granular Search Console performance data available.
Each row represents performance for a specific query on a specific page,
for a given site and search type, on a given day.

This table enables deep SEO diagnostics such as:
- query intent analysis
- page-level performance
- search type (web / image / video) segmentation

**Partitioning**
- `date`

**Clustering**
- `account_name`, `site_url`, `page`, `query`

---

### 2. Site-Level Daily Totals

**Table**
sdi_bronze_search_console_site_totals_daily


**Grain**
- date  
- account_name  
- site_url  

**Description**  
Stores daily, site-level aggregated Search Console metrics.
No query or page breakdowns are included.

This table is used for:
- executive SEO KPIs
- high-level trend analysis
- validation and reconciliation against detailed data

**Partitioning**
- `date`

**Clustering**
- `account_name`, `site_url`

---

## Table Comparison

| Aspect        | Site Totals Table                        | Query-Level Table                                      |
|--------------|------------------------------------------|--------------------------------------------------------|
| Granularity  | Site × Day                               | Site × Day × Query × Page × Search Type                |
| Aggregation  | Highest (fully aggregated)               | Lowest (fully disaggregated)                           |
| Primary Use  | Executive KPIs, dashboards               | SEO analysis, diagnostics, investigations              |
| Typical Size | Small                                    | Large                                                  |
| Cost Profile | Low                                      | Higher (requires partition pruning)                    |
| Relationship | **Parent aggregate**                     | **Child detail**                                       |

---

## Source Tables (Improvado)

- `ds_dbi_improvado_master.google_search_console_site_totals_tmo`
- `ds_dbi_improvado_master.google_search_console_query_search_type_tmo`

The site totals table represents **pre-aggregated rollups** of the same
underlying Search Console data that feeds the query-level table.

---

## Bootstrap SQL (One-Time Only)

One-time table creation SQL is version-controlled under:
sql/bootstrap/bronze/search_console/


Files:
- `create_query_daily.sql`
- `create_site_totals_daily.sql`

These scripts:
- are executed **manually** in BigQuery UI
- must **never be scheduled**
- must **not be re-run** after initial table creation

---

## Incremental Load Strategy

- Daily ingestion uses `MERGE`
- Rolling lookback window handles late-arriving data
- Duplicate records are prevented via natural keys
- Only recent partitions are scanned for cost efficiency

Recurring incremental SQL lives under:
sql/bronze/search_console/


---

## Execution Order

1. Run bootstrap `CREATE TABLE` statements (one time only)
2. Run incremental `MERGE` SQL manually to validate results
3. Schedule incremental `MERGE` SQL for daily execution
4. Build Silver transformations on top of Bronze tables

---

## Explicit Non-Goals (by design)

The Bronze layer **does not**:
- derive business logic
- normalize or re-aggregate metrics
- apply reporting definitions
- enforce KPI rules

All business logic belongs in **Silver or Gold** layers.

---

## Ownership

- **Layer:** Bronze  
- **Source System:** Google Search Console  
- **Domain:** Digital Analytics  
