/* =================================================================================================
FILE: 02_create_sdi_clickstream_bronze_entryPagesMetrics_daily.sql
LAYER: Bronze
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
TABLE: sdi_clickstream_bronze_entryPagesMetrics_daily

SOURCE (RAW):
  prj-dbi-prd-1.ds_dbi_marketing.fact_all_hits

PURPOSE:
  Canonical Bronze session-level metrics table for Entry Pages funnel analysis.

  This table stores one row per Postpaid session/day with binary session-level flags indicating
  whether that session achieved key downstream behaviors.

WHY THIS TABLE EXISTS:
  We do NOT want Silver or Gold repeatedly scanning fact_all_hits to calculate session funnel flags.
  Materializing session metrics in Bronze keeps downstream reporting fast and cost-efficient.

BUSINESS GRAIN:
  session_id + session_day

CANONICAL DATE:
  session_day
  - sourced from fact_all_hits.day
  - same canonical raw date choice as the entry-session Bronze table

INITIAL FUNNEL METRICS INCLUDED:
  - has_pspv
  - has_cart_start
  - has_checkout
  - has_order

WHY THESE METRICS:
  They provide a practical first version of the Entry Pages funnel using clickstream only.
  Later, Adobe metrics can replace or complement these in reporting if desired.

PARTITION / CLUSTER:
  PARTITION BY session_day
  CLUSTER BY lob, session_id

NOTES:
  - These are session flags, not raw event counts.
  - Session-level flags are the correct shape for cohort-style funnel reporting.
  - We intentionally do NOT store entry_sessions_flag here because entry sessions are already defined
    by the dedicated entry-session Bronze table.
================================================================================================= */

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_clickstream_bronze_entryPagesMetrics_daily`
(
  session_id STRING,
  session_day DATE,
  lob STRING,

  has_pspv INT64,
  has_cart_start INT64,
  has_checkout INT64,
  has_order INT64
)
PARTITION BY session_day
CLUSTER BY lob, session_id
OPTIONS(
  description = "Bronze clickstream session-level funnel metrics table for Entry Pages analysis."
);