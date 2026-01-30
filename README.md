# Digital Analytics Data Platform

Enterprise-grade digital analytics data platform implementing a medallion
(Bronze / Silver / Gold) architecture for SEO, paid media, and web analytics.

This repository contains SQL transformation logic, reference mappings, and
documentation for building a scalable, auditable digital analytics data
foundation.

---

## Architecture Overview

The platform follows a medallion architecture:

- **Bronze**  
  Source-faithful, incremental ingestion of upstream systems with minimal
  transformation.

- **Silver**  
  Business logic, normalization, and canonical dimensions such as LOB,
  brand, and channel mappings.

- **Gold**  
  KPI-ready datasets used for dashboards, forecasting, and advanced analytics.

SQL is executed using **BigQuery Scheduled Queries**.  
This repository acts as the **single source of truth** for all transformation
logic.


---

## Current Sources

### Google Search Console (via Improvado)

**Bronze models:**
- Site-level daily metrics
- Query and search-type level metrics

Design principles:
- Incremental MERGE-based loads
- Lookback window for late-arriving data
- No business assumptions in Bronze
- Full auditability via metadata columns

---

## How to Use

1. Clone the repository:
   ```bash
   git clone https://github.com/khldsqmr/Digital-Analytics-Data-Platform.git
   cd Digital-Analytics-Data-Platform
