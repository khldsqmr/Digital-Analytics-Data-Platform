/* =================================================================================================
FILE: 02_vw_sdi_tsd_appendix_business_rules.sql
LAYER: Gold View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_appendix_business_rules

SOURCE:
  Manual metadata mapping maintained in SQL

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_appendix_business_rules

PURPOSE:
  Appendix D business-rules and caveats catalog for the Total Search Dashboard.
  This view documents the key modeling rules, conformance logic, and important caveats
  used across the Bronze, Silver, and Gold layers.

BUSINESS GRAIN:
  One row per:
      rule_id

OUTPUT COLUMNS:
  - rule_id
  - rule_category
  - rule_name
  - rule_description
  - impacted_sources
  - applied_in_layer
  - implementation_summary
  - reporting_impact
  - caveats

KEY MODELING NOTES:
  - This is a documentation / appendix lookup view, not a transactional reporting mart
  - Rules are written in business-friendly language while preserving technical traceability
  - Appendix D is intended to explain why numbers behave the way they do in reporting

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_appendix_business_rules`
AS

SELECT
    1 AS rule_id,
    'DATE LOGIC' AS rule_category,
    'Canonical Daily Date Parsing' AS rule_name,
    'Daily reporting uses parsed canonical event_date values derived from source date fields such as date_yyyymmdd.' AS rule_description,
    'Adobe, SA360, GSC, Platform Spend, GMB' AS impacted_sources,
    'Bronze' AS applied_in_layer,
    'Source date fields are parsed into event_date and then reused downstream as the daily reporting date.' AS implementation_summary,
    'Ensures all daily source marts align to a common reporting calendar key.' AS reporting_impact,
    'If a source date is malformed or missing, the record can be excluded from downstream reporting.' AS caveats

UNION ALL
SELECT
    2,
    'DATE LOGIC',
    'Weekly Reporting Date',
    'Weekly reporting uses week ending Saturday as the canonical weekly reporting date.' ,
    'Operational Gold marts and final Gold long output',
    'Gold',
    'Daily records are rolled up into weekSunToSat using week ending Saturday logic.',
    'Keeps weekly reporting consistent across Adobe, SA360, GSC, Spend, and GMB.',
    'Daily data must be reconciled to the correct week ending Saturday when comparing across layers.'

UNION ALL
SELECT
    3,
    'DATE LOGIC',
    'Monthly Reporting Date',
    'Monthly reporting uses month end as the canonical monthly reporting date.' ,
    'Operational Gold marts and final Gold long output',
    'Gold',
    'Daily records are rolled up using monthEnd derived from the last calendar day of the month.',
    'Creates one standard monthly reporting key for Tableau.',
    'Month-start or source-native monthly dates should not be compared directly to final monthEnd outputs without transformation.'

UNION ALL
SELECT
    4,
    'DEDUPLICATION',
    'Snapshot Latest-Row Selection',
    'Snapshot-style sources are deduplicated by selecting the latest available record using load metadata ordering.' ,
    'Adobe Cart Start Plus, Adobe Store Locator, Adobe T-Life App Visits, SA360 Performance, SA360 Entity, GSC Query, GSC Site, GMB, ProFound, GoFish',
    'Bronze',
    'Latest rows are generally selected using File_Load_datetime DESC, Filename DESC, and __insert_date DESC.',
    'Prevents duplicate counting caused by repeated source snapshots or re-landed files.',
    'If source systems resend the same business record with different metadata, row ordering determines the retained canonical version.'

UNION ALL
SELECT
    5,
    'LOB STANDARDIZATION',
    'POSTPAID LOB Standardization',
    'Most operational source families are standardized to POSTPAID as the reporting LOB.' ,
    'Adobe, SA360, GSC, Platform Spend',
    'Bronze / Silver',
    'LOB is set directly or preserved as POSTPAID where the source family is already known to represent Postpaid reporting.',
    'Creates one conformed LOB for reporting consistency.',
    'Source-specific expansion to other LOBs would require additional source rules and validation.'

UNION ALL
SELECT
    6,
    'LOB STANDARDIZATION',
    'Derived LOB for GMB and AI Search',
    'Some source families derive LOB from source attributes rather than receiving a ready-made reporting LOB field.' ,
    'GMB, ProFound, GoFish',
    'Bronze / Silver',
    'GMB derives LOB from account naming. ProFound and GoFish derive LOB from tag fields.',
    'Allows non-operational sources to align with the same reporting structure.',
    'Incorrect upstream tagging or account naming can affect downstream conformance.'

UNION ALL
SELECT
    7,
    'CHANNEL CONFORMANCE',
    'Natural Search to Organic Search Mapping',
    'Natural Search and Organic Search are standardized into one conformed reporting channel: ORGANIC SEARCH.' ,
    'Adobe, GSC where applicable',
    'Silver',
    'Raw channel values such as NATURAL SEARCH are mapped to ORGANIC SEARCH before source-mart consolidation.',
    'Prevents split reporting across natural and organic naming variants.',
    'Layer-to-layer reconciliation must apply the same mapping logic before comparing values.'

UNION ALL
SELECT
    8,
    'CHANNEL CONFORMANCE',
    'Paid Search Rollup',
    'Paid-search child channels are rolled up into one conformed reporting channel: PAID SEARCH.' ,
    'Adobe, SA360, Platform Spend where applicable',
    'Silver',
    'Child channels such as PAID SEARCH: BRAND, PAID SEARCH: NON-BRAND, PAID SEARCH: PLAS, and PERFORMANCE MAX are mapped to PAID SEARCH.',
    'Simplifies reporting and aligns paid-search activity across source families.',
    'Source-level raw channel detail is reduced in the conformed reporting model.'

UNION ALL
SELECT
    9,
    'CHANNEL CONFORMANCE',
    'Maps and Local Search Channel Standardization',
    'Google Business Profile reporting is standardized to MAPS & LOCAL SEARCH.' ,
    'GMB',
    'Silver',
    'GMB metrics are published under a single conformed reporting channel.',
    'Separates local-search activity from paid and organic search reporting.',
    'Channel-level comparison should respect that these metrics are intentionally isolated from Adobe, SA360, and GSC channels.'

UNION ALL
SELECT
    10,
    'CHANNEL CONFORMANCE',
    'AI Search Channel Standardization',
    'ProFound and GoFish are standardized to the AI SEARCH channel.' ,
    'ProFound, GoFish',
    'Bronze / Silver / Gold',
    'AI Search source families are modeled under a shared AI SEARCH reporting channel.',
    'Supports a separate AI Search reporting model in weekly and monthly views.',
    'AI Search is intentionally not mixed into the operational daily unified mart.'

UNION ALL
SELECT
    11,
    'CLASSIFICATION',
    'SA360 Brand and Nonbrand Classification',
    'SA360 campaign performance is classified into brand and nonbrand groups using campaign metadata and resolved campaign naming.' ,
    'SA360',
    'Silver',
    'Performance records are joined with entity metadata so campaign_type and naming logic can support brand / nonbrand rollups.',
    'Enables paid-search brand / nonbrand reporting for clicks and cart-start-plus metrics.',
    'Classification quality depends on campaign naming consistency and metadata completeness.'

UNION ALL
SELECT
    12,
    'CLASSIFICATION',
    'GSC Brand and Nonbrand Classification',
    'GSC brand / nonbrand logic is applied at the query level before aggregation.' ,
    'GSC Query',
    'Silver',
    'Regex-based classification is applied to query-level data before rolling up to channel grain.',
    'Preserves accurate brand / nonbrand splits in organic clicks and impressions.',
    'Applying classification after aggregation would distort the reporting output.'

UNION ALL
SELECT
    13,
    'CLASSIFICATION',
    'AI Search Brand and Nonbrand Treatment',
    'ProFound is treated as NONBRAND and GoFish is treated as BRAND within the AI Search reporting model.' ,
    'ProFound, GoFish',
    'Bronze / Silver',
    'Brand_type is assigned as part of the canonical AI Search source shaping logic.',
    'Separates AI Search reporting into nonbrand and brand source families.',
    'This treatment is a business modeling rule, not a direct raw-source attribute from all rows.'

UNION ALL
SELECT
    14,
    'NULL HANDLING',
    'Null-Aware Weekly and Monthly Aggregation',
    'Weekly and monthly operational marts preserve NULL when a metric is absent for all contributing daily rows.' ,
    'Gold unified weekly and monthly marts',
    'Gold',
    'CASE WHEN COUNT(metric) = 0 THEN NULL ELSE SUM(metric) END logic is used during aggregation.',
    'Prevents fake zero values for non-applicable source / channel combinations.',
    'Tableau consumers should distinguish between NULL, true zero, and no applicable source coverage.'

UNION ALL
SELECT
    15,
    'SPARSITY',
    'Sparse Multi-Source Reporting Preservation',
    'The unified Gold daily mart preserves source-specific sparsity so that non-applicable metrics remain NULL.' ,
    'Adobe, SA360, GSC, Platform Spend, GMB',
    'Gold',
    'A distinct key spine is built from all daily Silver marts, then LEFT JOIN is used to retain valid dimensional combinations without forcing metrics into unrelated rows.',
    'Supports a fact-like sparse reporting model without fabricating cross-source values.',
    'Rows may contain many NULL metric fields by design.'

UNION ALL
SELECT
    16,
    'SEMANTIC OUTPUT',
    'Gold Long Emits Only Non-Null Metrics',
    'The final long-format semantic table only emits rows where metric_value IS NOT NULL.' ,
    'Gold long output',
    'Gold',
    'UNPIVOT output is filtered so only non-null metric rows remain in the final long semantic layer.',
    'Reduces noise in Tableau while preserving real zeroes for valid metrics.',
    'Users should not expect one row for every possible metric on every channel and date.'

UNION ALL
SELECT
    17,
    'MODEL SEPARATION',
    'AI Search is Modeled Separately from Daily Unified Reporting',
    'AI Search reporting is intentionally separated from the operational daily unified reporting model.' ,
    'ProFound, GoFish',
    'Gold',
    'AI Search is published via dedicated weekly and monthly marts and only then merged into the final long view.',
    'Keeps structurally different metric families separate from daily operational metrics.',
    'AI Search should not be reconciled directly to the daily unified fact-like mart because it does not originate at daily grain.'

UNION ALL
SELECT
    18,
    'ADOBE INTEGRATION',
    'Adobe Source-Family Consolidation',
    'Adobe metrics are sourced from multiple Bronze Adobe marts and consolidated into one conformed Silver Adobe source mart.' ,
    'Adobe V2 Funnel, Adobe Orders, Adobe Cart Start Plus, Adobe Store Locator, Adobe T-Life App Visits',
    'Silver',
    'Each Adobe sub-mart is channel-mapped, re-aggregated, and then joined using a unioned keyset to avoid duplication.',
    'Produces one conformed Adobe reporting mart at event_date + lob + channel.',
    'Reconciliation from Bronze to Silver must apply the same channel mapping before comparing values.'

UNION ALL
SELECT
    19,
    'ADOBE INTEGRATION',
    'Adobe T-Life App Visits Independent Dedupe Rule',
    'Adobe T-Life App Visits is treated as an independent Adobe source family because it originates from its own snapshot-style Adobe source.' ,
    'Adobe T-Life App Visits',
    'Bronze / Silver / Gold',
    'Latest-row dedupe is applied in Bronze first, then the metric is conformed and integrated into the shared Adobe Silver mart and downstream Gold marts.',
    'Allows T-Life app activity to flow through the same reporting model without contaminating other Adobe source logic.',
    'Source vs Bronze reconciliation must compare against deduped source records rather than raw landed row counts.'

UNION ALL
SELECT
    20,
    'GOVERNANCE',
    'Primary Ownership Standardization',
    'Most TSD source families are documented under SDI Team ownership for governance and appendix documentation purposes.' ,
    'Most operational and AI Search source families',
    'Appendix / Documentation',
    'Ownership is standardized in appendix metadata views unless a separate team designation is explicitly required.',
    'Provides a simple and consistent governance reference for stakeholders.',
    'Ownership documentation can be refined later if formal stewardship changes.'
;