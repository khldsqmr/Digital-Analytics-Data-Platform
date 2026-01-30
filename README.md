# Digital Analytics Data Platform

An enterprise-grade digital analytics data platform built on a **medallion
(Bronze / Silver / Gold) architecture** to produce **authoritative, reusable
analytical datasets**.

This repository acts as the **single source of truth** for ingestion logic,
transformations, reference mappings, and platform documentation on BigQuery.

---

## Repository Structure & Responsibilities

| Folder / Path              | Layer / Type        | Responsibility                                                                 |
|----------------------------|---------------------|--------------------------------------------------------------------------------|
| `sql/bootstrap/`           | Bootstrap SQL       | One-time table creation (DDL, partitioning, clustering). Never scheduled       |
| `sql/bronze/`              | Bronze              | Source-faithful incremental ingestion using MERGE                              |
| `sql/silver/`              | Silver              | Business logic, normalization, canonical dimensions                             |
| `sql/gold/`                | Gold                | Analytics-ready, reusable source-of-truth datasets                              |
| `docs/bootstrap/`          | Documentation       | Source-specific bootstrap and ingestion documentation                           |
| `refs/`                    | Reference Data      | Mapping tables (LOB, brand, channel, product, etc.)                             |
| `orchestration/`           | Orchestration       | Scheduling definitions and workflow orchestration (future)                     |
| `infra/`                   | Infrastructure      | Dataset setup, permissions, IaC, environment configuration                      |

---

## Architecture Overview

Upstream Sources
↓
Bronze Layer
↓
Silver Layer
↓
Gold Layer
↓
Downstream Consumers
(BI, APIs, ML, Products)


---

## Layer Responsibilities

### Bronze — Source-Faithful Ingestion
- Minimal transformation
- Incremental MERGE-based loads
- Partitioned and clustered storage
- Full source metadata retained

**Purpose:**  
Preserve upstream data exactly as delivered and enable safe reprocessing.

---

### Silver — Canonical Business Logic
- Standardization and normalization
- Shared business dimensions
- Cross-source consistency

**Purpose:**  
Translate raw data into **business-meaningful, reusable entities**.

---

### Gold — Analytical Source of Truth
- Fully curated, analytics-ready datasets
- Stable metric definitions and grains
- Designed for reuse across consumers

**Purpose:**  
Act as the **authoritative logical layer** feeding dashboards, APIs, ML pipelines,
and other downstream systems.

Gold datasets are **tool-agnostic** and not presentation-specific.

---

## Execution Model

| Execution Type | Location           | Description                                              |
|---------------|--------------------|----------------------------------------------------------|
| One-time      | `sql/bootstrap/`   | Physical table creation per environment                   |
| Recurring     | `sql/bronze/`      | Incremental ingestion with late-arriving data handling   |
| Downstream    | `sql/silver/`      | Business logic and standardization                        |
| Downstream    | `sql/gold/`        | Curated analytical datasets                               |

---

## Data Sources

The platform supports **multiple digital data sources**.  
Each source has **dedicated documentation** describing schema, bootstrap logic,
and ingestion strategy.

Source-specific documentation lives under:
docs/bootstrap/


Examples:
- Google Search Console
- Paid media platforms
- Web analytics platforms

---

## Design Principles

- Bronze remains immutable and source-faithful
- Business logic belongs in Silver
- Gold acts as the analytical source of truth
- Transformations are version-controlled
- SQL is readable, documented, and idempotent
- Platform is consumer-agnostic (BI, APIs, ML)

---

## Getting Started

1. Clone the repository:
   ```bash
   git clone https://github.com/khldsqmr/Digital-Analytics-Data-Platform.git
   cd Digital-Analytics-Data-Platform
2. Review source-specific documentation: docs/bootstrap/
3. Execute bootstrap SQL once per environment
4. Validate incremental ingestion logic
5. Schedule incremental SQL jobs in BigQuery
6. Build Silver and Gold layers incrementally

Ownership

Domain: Digital Analytics
Warehouse: BigQuery
Execution Engine: BigQuery Scheduled Queries


