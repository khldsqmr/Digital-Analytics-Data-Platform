/* =================================================================================================
FILE: 01_create_sdi_clickstream_bronze_entryPagesSession_daily.sql
LAYER: Bronze
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
TABLE: sdi_clickstream_bronze_entryPagesSession_daily

SOURCE (RAW):
  prj-dbi-prd-1.ds_dbi_marketing.fact_all_hits

PURPOSE:
  Canonical Bronze session-entry table for Entry Pages analysis.

  This table stores exactly one row per Postpaid session/day representing the first valid pageview
  in that session. That row is treated as the "entry page" of the session.

WHY THIS TABLE EXISTS:
  fact_all_hits is a very large raw clickstream table. Re-reading it repeatedly in downstream views
  would be expensive and slow. By materializing this first-page extraction once in Bronze, we make
  Silver and Gold much cheaper and faster.

BUSINESS GRAIN:
  session_id + session_day

CANONICAL DATE:
  session_day
  - sourced from fact_all_hits.day
  - this is the single raw date retained in this pipeline for now
  - weekly / QGP mapping is intentionally deferred to Gold and later reporting logic

ENTRY PAGE LOGIC:
  A row qualifies as an entry page candidate when:
  - lob = 'Postpaid'
  - session_page_num = 1
  - page_views >= 1
  - page_name IS NOT NULL

IMPORTANT:
  - We are not grouping pages here.
  - We are not mapping channels here.
  - Bronze should stay close to raw logic and preserve traceability.

PARTITION / CLUSTER:
  PARTITION BY session_day
  CLUSTER BY lob, session_id, session_channel_name

WHY THIS CLUSTERING:
  - lob is frequently filtered
  - session_id is key for downstream joins to session metrics
  - session_channel_name is commonly used for later breakdowns

NOTES:
  - Raw page and URL fields are kept for diagnostics and future refinement.
  - Raw channel fields are retained even though no mapping is applied yet.
================================================================================================= */

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_clickstream_bronze_entryPagesSession_daily`
(
  session_id STRING,
  session_day DATE,
  lob STRING,

  entry_page_name_raw STRING,
  visit_start_pagename STRING,

  full_url STRING,
  url_path STRING,
  visit_start_page_url STRING,

  site_name STRING,
  device_type STRING,

  session_channel_name STRING,
  adobe_last_touch_channel_name STRING,
  page_view_channel_name STRING,

  session_page_num INT64,
  page_views INT64,
  hit_id STRING,
  date_time DATETIME,

  entry_row_rank INT64
)
PARTITION BY session_day
CLUSTER BY lob, session_id, session_channel_name
OPTIONS(
  description = "Bronze clickstream first-page session-entry table for Entry Pages analysis."
);