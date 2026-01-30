# Google Search Console – Bronze Bootstrap

This document describes the **one-time bootstrap requirements**
for Google Search Console Bronze tables.

---

## Purpose

Bronze tables store **raw Search Console metrics** with minimal
transformation and full fidelity.

Incremental MERGE pipelines assume these tables already exist.
Bootstrap is executed **once per environment**.

---

## Project & Dataset

- Project: `prj-dbi-prd-1`
- Dataset: `ds_dbi_digitalmedia_automation`
- Location: `us-west1`

---

## Bronze Tables (One-Time Setup)

### 1. Query-Level Daily Metrics

Table: sdi_bronze_search_console_query_daily


Grain:
- date
- property
- page
- query
- device
- country

Description:
Stores query-level Search Console metrics such as clicks,
impressions, CTR, and average position.

Partitioning:
- `date`

Clustering:
- `property`, `page`, `query`

---

### 2. Site-Level Daily Totals

Table: sdi_bronze_search_console_site_totals_daily


Grain:
- date
- property
- device
- country

Description:
Stores site-level aggregated Search Console metrics.

Partitioning:
- `date`

Clustering:
- `property`, `device`

---

## Incremental Load Strategy

- Daily ingestion uses `MERGE`
- Late-arriving data is updated
- Duplicate records are prevented
- Partition pruning ensures low cost

---

## Execution Order

1. Run bootstrap CREATE TABLE statements (one time)
2. Run MERGE SQL manually to validate
3. Schedule MERGE SQL for daily execution
4. Build Silver transformations on top

---

## Ownership

Layer: Bronze  
Source System: Google Search Console  
Domain: Digital Analytics  

