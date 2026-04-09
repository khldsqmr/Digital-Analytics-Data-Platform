# Total Search Dashboard (TSD) Data Architecture

## Overview
The **Total Search Dashboard (TSD)** architecture is built using a **Bronze → Silver → Gold** medallion pattern.

It standardizes multiple search and search-adjacent data sources into a unified reporting model for Tableau. The design emphasizes:
- source-close ingestion in Bronze
- business conformance in Silver
- fact-like reporting marts in Gold
- final long-format semantic output for dashboard consumption

### Main objectives
- standardize heterogeneous source systems into a common reporting structure
- deduplicate snapshot-based source extracts before downstream use
- conform `LOB`, `channel`, `brand/nonbrand`, and reporting dates
- publish reporting-ready daily, weekly, monthly, and long-format outputs
- preserve `NULL` for non-applicable source/metric combinations instead of forcing artificial zeroes

---

## Architecture Pattern

## Medallion flow
| Layer | Role | Output style |
|---|---|---|
| Bronze | source-close standardization and deduplication | canonical source marts |
| Silver | business conformance and source-level reporting marts | conformed source marts |
| Gold | unified fact-like marts and final semantic layer | dashboard-ready reporting views |

## Star-schema-style reporting approach
The Gold layer behaves like a **star-schema-style reporting model**.

### Main Gold reporting model
The central operational reporting object is:

- **`vw_sdi_tsd_gold_unified_daily`**
  - grain: `event_date + lob + channel`

This acts like a central fact-like table that brings together:
- Adobe
- SA360
- GSC
- Platform Spend
- GMB

### Separate AI Search reporting model
AI Search is modeled separately through:
- **`vw_sdi_tsd_gold_profound_weekly`**
- **`vw_sdi_tsd_gold_profound_monthly`**

This is kept separate because:
- its source grain is weekly/monthly rather than daily
- the metrics are point-in-time competitor / citation / visibility metrics
- its measures are structurally different from operational search and media metrics

### Final semantic layer
The final Tableau-ready model is:
- **`vw_sdi_tsd_gold_long`**

This combines:
- operational weekly/monthly reporting marts
- AI Search weekly/monthly reporting marts

into one long-format reporting layer.

---

## End-to-End Data Flow

| Stage | What happens |
|---|---|
| Raw Sources | source system data is landed from Adobe, SA360, GSC, Platform Spend, GMB, ProFound, and GoFish |
| Bronze | canonical dates are parsed, snapshot records are deduplicated, and source-close marts are created |
| Silver | business rules are applied, channels are conformed, LOB is standardized, and source marts are aggregated to reporting grain |
| Gold | unified fact-like marts are created for operational reporting, AI Search marts are published separately, and long-format reporting output is produced |
| Tableau | dashboard consumes the final Gold long-format view |

---

## Source Families

| Source Family | Business Role | Downstream Purpose |
|---|---|---|
| Adobe | funnel and digital order metrics | search funnel and conversion reporting |
| SA360 | paid search performance | paid search clicks and cart-start reporting |
| GSC | organic query demand and visibility | organic search demand and impression reporting |
| Platform Spend | marketing spend | paid search spend reporting and spend alignment |
| GMB / GBP | maps and local search behavior | local discovery and local engagement reporting |
| ProFound | AI Search nonbrand visibility/citation metrics | competitive AI Search reporting |
| GoFish | AI Search brand visibility/citation metrics | branded AI Search reporting |

---

## Conformed Reporting Model

## Core conceptual dimensions
Although not persisted as separate physical dimension tables, the reporting model behaves as though it uses the following conformed dimensions:

| Dimension Concept | Meaning |
|---|---|
| Date | reporting date at daily, weekly, or monthly grain |
| LOB | line of business, primarily `POSTPAID` |
| Channel | conformed reporting channel |
| Data Source | Adobe, SA360, GSC, Platform Spend, GMB, ProFound, GoFish |
| Metric | metric published in long format via `metric_name` |

## Main conformed channels
| Channel | Meaning |
|---|---|
| `PAID SEARCH` | paid search family after channel rollups |
| `ORGANIC SEARCH` | natural / organic search family |
| `MAPS & LOCAL SEARCH` | local discovery / Google Business Profile reporting |
| `AI SEARCH` | ProFound / GoFish AI Search reporting |
| additional spend channels | retained in spend marts where applicable |

---

## Key Business Rules

## 1) Canonical date handling
| Reporting level | Date logic |
|---|---|
| Daily | parsed `event_date` |
| Weekly | week ending Saturday |
| Monthly | month end |

## 2) Snapshot deduplication
Most Bronze views use latest-row logic based on:
- `File_Load_datetime DESC`
- `Filename DESC`
- `__insert_date DESC`

This prevents duplicate reporting caused by repeated source snapshots.

## 3) LOB standardization
| Source family | LOB logic |
|---|---|
| Adobe / SA360 / GSC / Spend | standardized to `POSTPAID` |
| GMB | derived from `account_name` |
| ProFound / GoFish | derived from `tag` |

## 4) Channel conformance
| Raw channel behavior | Conformed channel |
|---|---|
| `NATURAL SEARCH` | `ORGANIC SEARCH` |
| `ORGANIC SEARCH` | `ORGANIC SEARCH` |
| paid-search child channels | `PAID SEARCH` |
| GMB | `MAPS & LOCAL SEARCH` |
| ProFound / GoFish | `AI SEARCH` |

## 5) Brand / nonbrand logic
| Source | Logic |
|---|---|
| SA360 | derived from resolved campaign name |
| GSC | derived from query-level regex classification |
| ProFound | treated as `NONBRAND` |
| GoFish | treated as `BRAND` |

## 6) Null-aware metric handling
The model preserves `NULL` for non-applicable source/channel combinations.

This helps distinguish:
- true zero
- not applicable
- no data

and avoids introducing misleading fake zeroes into Tableau.

---

## Bronze Layer

## Bronze purpose
Bronze is the **source-close standardization layer**. It:
- parses canonical dates
- deduplicates source snapshots
- preserves source truth
- applies only minimal standardization required for downstream processing

## Bronze objects

| Sources | Destination View | Grain | Summary |
|---|---|---|---|
| `adobe_analytics_custom_postpaid_voice_v2_tmo` | `vw_sdi_tsd_bronze_adobeV2_daily` | `event_date + lob + channel` | standardizes Adobe Postpaid funnel metrics |
| `vw_sdi_adobePpPulsePro_gold_orders_daily` | `vw_sdi_tsd_bronze_adobeOrders_daily` | `event_date + lob + channel` | standardizes Adobe digital order metrics |
| `adobe_cs_day_tmo` | `vw_sdi_tsd_bronze_adobeCartStartPlus_daily` | `event_date + lob + channel` | deduplicates Adobe cart-start-plus metric |
| `google_search_ads_360_campaigns_tmo` | `vw_sdi_tsd_bronze_sa360Perf_daily` | `account_id + campaign_id + event_date` | deduplicated SA360 campaign performance base |
| `google_search_ads_360_beta_campaign_entity_custom_tmo` | `vw_sdi_tsd_bronze_sa360Entity_daily` | `account_id + campaign_id + event_date` | deduplicated SA360 campaign metadata base |
| `google_search_console_query_search_type_tmo` | `vw_sdi_tsd_bronze_gscQuery_daily` | `account_id + site_url + page + query + search_type + event_date` | query-level GSC mart for brand/nonbrand classification |
| `google_search_console_site_totals_tmo` | `vw_sdi_tsd_bronze_gscSite_daily` | `account_id + site_url + event_date` | site-level GSC totals for QA and reconciliation |
| `agg_day_media_and_outcomes` | `vw_sdi_tsd_bronze_platformSpend_daily` | `event_date + lob + channel_raw` | source-close platform spend mart preserving raw channels |
| `google_business_profile_google_my_business_location_insights_tmo` | `vw_sdi_tsd_bronze_gmb_daily` | `event_date + date_yyyymmdd + account_name` | deduplicated GMB / GBP location-insights mart |
| `sdi_seo_profound_vis_tag_weekly_sunday_tmo`, `sdi_seo_profound_cit_tag_weekly_sunday_tmo`, `sdi_seo_profound_gofish_vis_tag_weekly_sunday_tmo`, `sdi_seo_profound_gofish_cit_tag_weekly_sunday_tmo` | `vw_sdi_tsd_bronze_profoundVisCitTag_weekly` | `period_date + lob + channel + company + brand_type + metric_source` | weekly canonical ProFound / GoFish mart |
| `sdi_seo_profound_vis_tag_monthly_tmo`, `sdi_seo_profound_cit_tag_monthly_tmo`, `sdi_seo_profound_gofish_vis_tag_monthly_tmo`, `sdi_seo_profound_gofish_cit_tag_monthly_tmo` | `vw_sdi_tsd_bronze_profoundVisCitTag_monthly` | `period_date + lob + channel + company + brand_type + metric_source` | monthly canonical ProFound / GoFish mart |
| `sdi_raw_adobe_pp_ts_pro_storelocator_visits_daily_tmo` | `vw_sdi_tsd_bronze_adobeStoreLocator_daily` | `event_date + lob + channel` | deduplicated Adobe Store Locator visits mart |

## Bronze design notes
- Bronze remains intentionally close to source truth
- SA360 performance and entity metadata are modeled separately on purpose
- GSC query-level and site-level outputs are separated so classification and QA stay clean
- ProFound / GoFish are standardized into canonical weekly and monthly source marts
- GMB preserves account context so Silver can derive LOB cleanly

---

## Silver Layer

## Silver purpose
Silver is the **business-conformance layer**. It:
- standardizes channels
- derives LOB where needed
- applies brand/nonbrand logic
- aligns source marts to a common reporting grain

## Silver objects

| Sources | Destination View | Grain | Summary |
|---|---|---|---|
| `vw_sdi_tsd_bronze_adobeV2_daily`, `vw_sdi_tsd_bronze_adobeOrders_daily`, `vw_sdi_tsd_bronze_adobeCartStartPlus_daily`, `vw_sdi_tsd_bronze_adobeStoreLocator_daily` | `vw_sdi_tsd_silver_adobe_daily` | `event_date + lob + channel` | conformed Adobe daily source mart |
| `vw_sdi_tsd_bronze_gscQuery_daily` | `vw_sdi_tsd_silver_gsc_daily` | `event_date + lob + channel` | query-level GSC classification and aggregation mart |
| `vw_sdi_tsd_bronze_platformSpend_daily` | `vw_sdi_tsd_silver_platformSpend_daily` | `event_date + lob + channel` | conformed platform spend mart |
| `vw_sdi_tsd_bronze_gmb_daily` | `vw_sdi_tsd_silver_gmb_daily` | `event_date + lob + channel` | conformed GMB mart |
| `vw_sdi_tsd_bronze_sa360Perf_daily`, `vw_sdi_tsd_bronze_sa360Entity_daily` | `vw_sdi_tsd_silver_sa360_daily` | `event_date + lob + channel` | conformed SA360 mart using performance + latest entity metadata |
| `vw_sdi_tsd_bronze_profoundVisCitTag_weekly` | `vw_sdi_tsd_silver_profound_weekly` | `period_date + lob + channel` | weekly AI Search wide reporting mart |
| `vw_sdi_tsd_bronze_profoundVisCitTag_monthly` | `vw_sdi_tsd_silver_profound_monthly` | `period_date + lob + channel` | monthly AI Search wide reporting mart |

## Silver conformance rules
| Rule Type | Description |
|---|---|
| channel normalization | paid-search variants collapse to `PAID SEARCH`; natural search maps to `ORGANIC SEARCH` |
| LOB derivation | GMB derives `POSTPAID` from account naming; most others are already standardized |
| Adobe consolidation | all Adobe sub-marts are channel-mapped first, re-aggregated, then joined by a unioned keyset |
| SA360 classification | latest entity metadata is joined to performance; in-scope campaigns are classified into brand/nonbrand |
| GSC classification | query-level regex classification is applied before aggregation |
| AI Search shaping | ProFound / GoFish are pivoted wide at weekly/monthly reporting grain |

## Silver design notes
- Silver is where business meaning is applied
- this is the layer where channel and classification consistency is enforced
- all daily operational marts are aligned to `event_date + lob + channel`
- AI Search marts are aligned to `period_date + lob + channel`
- nulls remain preserved so Gold can avoid creating fake zeroes

---

## Gold Layer

## Gold purpose
Gold is the **dashboard-facing reporting layer**. It:
- builds the main unified fact-like reporting mart
- publishes weekly and monthly rollups
- keeps AI Search reporting marts separate
- produces the final long-format Tableau layer

## Gold objects

| Sources | Destination View | Grain | Summary |
|---|---|---|---|
| `vw_sdi_tsd_silver_adobe_daily`, `vw_sdi_tsd_silver_sa360_daily`, `vw_sdi_tsd_silver_gsc_daily`, `vw_sdi_tsd_silver_platformSpend_daily`, `vw_sdi_tsd_silver_gmb_daily` | `vw_sdi_tsd_gold_unified_daily` | `event_date + lob + channel` | central unified operational fact-like mart |
| `vw_sdi_tsd_gold_unified_daily` | `vw_sdi_tsd_gold_unifiedSunSat_weekly` | `weekSunToSat + lob + channel` | weekly Sun-Sat rollup of unified daily reporting |
| `vw_sdi_tsd_gold_unified_daily` | `vw_sdi_tsd_gold_unified_monthly` | `monthEnd + lob + channel` | month-end rollup of unified daily reporting |
| `vw_sdi_tsd_silver_profound_weekly` | `vw_sdi_tsd_gold_profound_weekly` | `period_date + lob + channel` | weekly AI Search reporting mart |
| `vw_sdi_tsd_silver_profound_monthly` | `vw_sdi_tsd_gold_profound_monthly` | `period_date + lob + channel` | monthly AI Search reporting mart |
| `vw_sdi_tsd_gold_unifiedSunSat_weekly`, `vw_sdi_tsd_gold_unified_monthly`, `vw_sdi_tsd_gold_profound_weekly`, `vw_sdi_tsd_gold_profound_monthly` | `vw_sdi_tsd_gold_long` | `data_source + time_granularity + date + lob + channel + metric_name` | final Tableau-friendly long-format semantic layer |

## Gold reporting model

### 1) Unified operational fact-like mart
**`vw_sdi_tsd_gold_unified_daily`** is the central reporting object for operational search and local-search metrics.

**Grain:** `event_date + lob + channel`

It contains unified measures from:
- Adobe
- SA360
- GSC
- Platform Spend
- GMB

This is the main star-schema-style fact-like table in the architecture.

### 2) Unified rollups
From `vw_sdi_tsd_gold_unified_daily`, two higher-level reporting marts are created:

| Destination View | Grain | Purpose |
|---|---|---|
| `vw_sdi_tsd_gold_unifiedSunSat_weekly` | `weekSunToSat + lob + channel` | weekly reporting |
| `vw_sdi_tsd_gold_unified_monthly` | `monthEnd + lob + channel` | monthly reporting |

Both use **null-aware aggregation**, so if a metric is absent for all contributing rows, it stays `NULL`.

### 3) AI Search marts
AI Search metrics are published separately through:

| Destination View | Grain | Purpose |
|---|---|---|
| `vw_sdi_tsd_gold_profound_weekly` | `period_date + lob + channel` | weekly AI Search reporting |
| `vw_sdi_tsd_gold_profound_monthly` | `period_date + lob + channel` | monthly AI Search reporting |

These are intentionally separate from the unified operational mart.

### 4) Final semantic long-format table
The final dashboard-serving table is:

**`vw_sdi_tsd_gold_long`**

**Grain:**  
`data_source + time_granularity + time_granularity_type + date + lob + channel + metric_name`

This object:
- excludes daily rows intentionally
- publishes weekly and monthly reporting only
- emits only rows where `metric_value IS NOT NULL`
- preserves real zeroes while suppressing non-applicable null rows

---

## Metric Families

## Adobe metrics
| Family | Metrics |
|---|---|
| Funnel | `adobe_entries`, `adobe_pspv_actuals`, `adobe_cart_starts`, `adobe_cart_start_plus` |
| Checkout steps | `adobe_cart_checkout_visits`, `adobe_checkout_review_visits` |
| TSR orders | `adobe_postpaid_orders_tsr` |
| Digital orders | web/app assisted and unassisted order metrics, plus totals |
| Local intent | `adobe_storelocator_visits` |

## SA360 metrics
| Family | Metrics |
|---|---|
| Clicks | `sa360_clicks_brand`, `sa360_clicks_nonbrand`, `sa360_clicks_all` |
| Cart start plus | `sa360_cart_start_plus_brand`, `sa360_cart_start_plus_nonbrand`, `sa360_cart_start_plus_all` |

## GSC metrics
| Family | Metrics |
|---|---|
| Clicks | `gsc_clicks_brand`, `gsc_clicks_nonbrand`, `gsc_clicks_all` |
| Impressions | `gsc_impressions_brand`, `gsc_impressions_nonbrand`, `gsc_impressions_all` |

## Platform Spend
| Family | Metrics |
|---|---|
| Spend | `platform_spend` |

## GMB metrics
| Family | Metrics |
|---|---|
| Impressions | `gmb_search_impressions_all`, `gmb_maps_impressions_all`, `gmb_impressions_all` |
| Engagement | `gmb_call_clicks`, `gmb_website_clicks`, `gmb_directions_clicks` |

## ProFound / GoFish metrics
| Family | Metrics |
|---|---|
| Visibility | executions, visibility score |
| Citation | citation count, citation share |
| Competitor slices | TMO, ATT, Verizon |
| Source slices | ProFound, GoFish |

---

## Modeling Decisions Worth Calling Out

## Why a key spine in Gold daily?
A distinct key spine ensures that every valid `event_date + lob + channel` combination from any daily Silver mart is preserved before left joining source-specific metrics.

This:
- prevents accidental row loss
- avoids duplicate expansion
- supports sparse multi-source reporting cleanly

## Why keep AI Search separate from unified daily?
Because AI Search data:
- is periodic, not daily
- uses different metric families
- is conceptually closer to a separate fact mart than to a daily operational activity table

## Why query-level classification for GSC?
Brand/nonbrand logic must be applied before aggregation.  
Otherwise, clicks and impressions could be incorrectly rolled up.

## Why separate SA360 perf and entity in Bronze?
Performance and campaign metadata come from different source structures.  
Keeping them separate avoids premature joins and makes Silver logic cleaner and more maintainable.

## Why null-aware aggregation?
This keeps non-applicable rows from showing as zero in weekly/monthly output.  
It preserves the difference between:
- no data
- not applicable
- true zero

---

## Source-to-Target Summary

| Path | Flow |
|---|---|
| Adobe | Bronze Adobe marts -> Silver Adobe mart -> Gold Unified Daily -> Weekly / Monthly -> Gold Long |
| SA360 | Bronze Perf + Bronze Entity -> Silver SA360 mart -> Gold Unified Daily -> Weekly / Monthly -> Gold Long |
| GSC | Bronze GSC Query -> Silver GSC mart -> Gold Unified Daily -> Weekly / Monthly -> Gold Long |
| GSC QA | Bronze GSC Site -> reconciliation / QA support |
| Spend | Bronze Spend -> Silver Spend mart -> Gold Unified Daily -> Weekly / Monthly -> Gold Long |
| GMB | Bronze GMB -> Silver GMB mart -> Gold Unified Daily -> Weekly / Monthly -> Gold Long |
| AI Search | Bronze ProFound / GoFish -> Silver ProFound weekly/monthly -> Gold ProFound weekly/monthly -> Gold Long |

---

## Final Output Objects

| Destination View | Role |
|---|---|
| `vw_sdi_tsd_gold_unified_daily` | central operational fact-like reporting mart |
| `vw_sdi_tsd_gold_unifiedSunSat_weekly` | weekly Sun-Sat operational reporting mart |
| `vw_sdi_tsd_gold_unified_monthly` | month-end operational reporting mart |
| `vw_sdi_tsd_gold_profound_weekly` | weekly AI Search reporting mart |
| `vw_sdi_tsd_gold_profound_monthly` | monthly AI Search reporting mart |
| `vw_sdi_tsd_gold_long` | final Tableau-ready long-format semantic layer |

---

## Notes and Caveats

| Item | Note |
|---|---|
| Bronze GSC numbering | both GSC Bronze comments were labeled `07`; repository numbering can be normalized |
| Gold ProFound weekly block | the supplied weekly ProFound section appeared duplicated; only one canonical version should be retained |
| Daily output in final long | daily rows are intentionally excluded from `vw_sdi_tsd_gold_long` |
| GSC site totals | used primarily for QA / reconciliation, not directly in final Gold reporting path |
| Spend in final long | current final long logic emits `platform_spend` for `PAID SEARCH` only |

---

## End State
The final architecture gives the Total Search Dashboard a reporting model that is:
- source-standardized
- deduplicated
- conformed across business keys
- star-schema-like at the Gold layer
- safe for sparse multi-source reporting
- ready for Tableau through a final long-format semantic table