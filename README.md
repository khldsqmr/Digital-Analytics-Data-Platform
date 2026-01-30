# Digital Analytics Data Platform

An enterprise-grade digital analytics data platform built on a **medallion
architecture (Bronze / Silver / Gold)** to support scalable, auditable, and
reproducible analytics across SEO, paid media, and web analytics domains.

This repository serves as the **single source of truth** for transformation
logic, table definitions, and documentation that power downstream analytical
and operational use cases.

---

## Platform Architecture

The platform follows a layered medallion architecture:

### Bronze Layer
- Source-faithful ingestion from upstream systems
- Minimal transformation
- Incremental, idempotent loads
- Full auditability and replayability

### Silver Layer
- Business logic and normalization
- Canonical dimensions (e.g. channel, LOB, brand)
- Data quality enforcement
- Analytics-ready but not KPI-aggregated

### Gold Layer
- Curated, KPI-ready datasets
- Acts as the **source of truth** for downstream consumption
- Designed to power:
  - Logical semantic layers
  - APIs and data services
  - Forecasting and advanced analytics
  - External tools and applications

> Gold tables are intentionally decoupled from visualization tools so they can
> be reused consistently across multiple consumers.

---

## Repository Structure

| Folder Path | Purpose |
|------------|---------|
| `sql/bootstrap/` | One-time table creation (`CREATE TABLE`). Executed once per environment. |
| `sql/bronze/` | Incremental MERGE-based ingestion logic for raw source data. |
| `sql/silver/` | Business logic, normalization, and enrichment transformations. |
| `sql/gold/` | Curated KPI datasets and analytical fact tables. |
| `docs/bootstrap/` | Bootstrap documentation explaining one-time setup steps. |
| `docs/domain/` | Domain-specific documentation (e.g. Search Console, Paid Media). |
| `refs/` | Static reference mappings and lookup tables. |
| `orchestration/` | Scheduling and orchestration artifacts (future). |
| `infra/` | Infrastructure-as-code and environment setup (future). |

---

## Current Data Sources

### Google Search Console (via Improvado)

Implemented using a **parent–child Bronze design** to preserve full source
fidelity while enabling both executive and diagnostic analytics.

| Aspect | Site Totals Table | Query-Level Table |
|------|------------------|------------------|
| Granularity | Site × Day | Site × Day × Query × Page × Search Type |
| Aggregation Level | Highest | Lowest |
| Primary Use Case | Executive KPIs, trend analysis | SEO diagnostics, content analysis |
| Relationship | Parent aggregate | Child detail |

Design principles:
- Incremental MERGE-based ingestion
- Rolling lookback window for late-arriving data
- No business logic in Bronze
- Full auditability using metadata columns

Detailed documentation for Search Console lives under:
`docs/bootstrap/search_console_bronze.md`

---

## Execution Model

- SQL is executed using **BigQuery Scheduled Queries**
- Bootstrap SQL is executed manually (one time only)
- Incremental SQL is scheduled for daily execution
- Downstream layers depend strictly on upstream layers

Execution order:
1. Bootstrap tables (`sql/bootstrap/`)
2. Incremental Bronze ingestion (`sql/bronze/`)
3. Silver transformations
4. Gold curation

---

## Design Principles

- **Idempotency:** Pipelines can be safely re-run
- **Auditability:** All raw data is traceable to source files
- **Separation of concerns:** Each layer has a clear responsibility
- **Cost efficiency:** Partitioning and pruning are enforced
- **Scalability:** New sources and domains can be added without refactoring

---

## Non-Goals (By Design)

This platform intentionally does **not**:
- Embed visualization logic
- Apply business KPIs in Bronze
- Mix ingestion and reporting logic
- Hard-code downstream consumption assumptions

---

Refer to docs/bootstrap/ before running any SQL.

Ownership
- Domain: Digital Analytics
- Architecture: Medallion (Bronze / Silver / Gold)
- Warehouse: BigQuery
- Language: SQL

---

## How to Get Started

Clone the repository:

```bash
git clone https://github.com/khldsqmr/Digital-Analytics-Data-Platform.git
cd Digital-Analytics-Data-Platform
