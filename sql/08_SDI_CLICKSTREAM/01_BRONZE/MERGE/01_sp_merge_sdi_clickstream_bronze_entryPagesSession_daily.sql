/* =================================================================================================
FILE: 01_sp_merge_sdi_clickstream_bronze_entryPagesSession_daily.sql
LAYER: Bronze
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROCEDURE: sp_merge_sdi_clickstream_bronze_entryPagesSession_daily

SOURCE (RAW):
  prj-dbi-prd-1.ds_dbi_marketing.fact_all_hits

TARGET:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_clickstream_bronze_entryPagesSession_daily

PURPOSE:
  Refresh Bronze Entry Pages session table by replacing recent affected dates and keeping the
  canonical first valid pageview row per session/day.

BUSINESS GRAIN:
  session_id + session_day

WHY THIS PROCEDURE EXISTS:
  fact_all_hits is large, so we do not want downstream layers to rescan it repeatedly.
  This procedure extracts and stores only the entry-page cohort needed for the Entry Pages tab.

REFRESH STRATEGY:
  This procedure is designed for incremental production refresh using a rolling lookback window.
  It deletes and reloads only recent session_day values.

LOOKBACK WINDOW:
  Default logic here uses last 14 days.
  This can be widened if your clickstream data arrives late.

ONE-TIME BACKFILL:
  For the initial historical load from 2025-01-01 onward, use the same logic but temporarily replace:
      day >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
  with:
      day >= DATE '2025-01-01'

ENTRY PAGE DEFINITION:
  A valid entry row is:
  - Postpaid
  - session_page_num = 1
  - page_views >= 1
  - page_name IS NOT NULL

DEDUPLICATION LOGIC:
  Multiple rows can occasionally qualify for the same session/day.
  We therefore assign:
      ROW_NUMBER() OVER (
        PARTITION BY session_id, day
        ORDER BY
          date_time ASC,
          session_page_num ASC,
          hit_id ASC
      )
  and keep rn = 1.

WHY THIS ORDER:
  - earliest event timestamp first
  - lowest session page number first
  - hit_id for deterministic tie-breaking

DELETE / INSERT STRATEGY:
  Matches your established Bronze pipeline pattern and is easy to reason about.
================================================================================================= */

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_sdi_clickstream_bronze_entryPagesSession_daily`()
OPTIONS(strict_mode=false)
BEGIN

  BEGIN TRANSACTION;

  DELETE FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_clickstream_bronze_entryPagesSession_daily`
  WHERE session_day IN (
    SELECT DISTINCT src.day
    FROM `prj-dbi-prd-1.ds_dbi_marketing.fact_all_hits` src
    WHERE src.day >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
      AND src.lob = 'Postpaid'
      AND src.session_page_num = 1
      AND src.page_views >= 1
      AND src.page_name IS NOT NULL
  );

  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_clickstream_bronze_entryPagesSession_daily`
  (
    session_id,
    session_day,
    lob,

    entry_page_name_raw,
    visit_start_pagename,

    full_url,
    url_path,
    visit_start_page_url,

    site_name,
    device_type,

    session_channel_name,
    adobe_last_touch_channel_name,
    page_view_channel_name,

    session_page_num,
    page_views,
    hit_id,
    date_time,

    entry_row_rank
  )
  WITH ranked AS (
    SELECT
      src.session_id,
      src.day AS session_day,
      'POSTPAID' AS lob,

      src.page_name AS entry_page_name_raw,
      src.visit_start_pagename,

      src.full_url,
      src.url_path,
      src.visit_start_page_url,

      src.site_name,
      src.device_type,

      src.session_channel_name,
      src.adobe_last_touch_channel_name,
      src.page_view_channel_name,

      src.session_page_num,
      src.page_views,
      src.hit_id,
      src.date_time,

      ROW_NUMBER() OVER (
        PARTITION BY src.session_id, src.day
        ORDER BY
          src.date_time ASC,
          src.session_page_num ASC,
          src.hit_id ASC
      ) AS entry_row_rank
    FROM `prj-dbi-prd-1.ds_dbi_marketing.fact_all_hits` src
    WHERE src.day >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
      AND src.lob = 'Postpaid'
      AND src.session_page_num = 1
      AND src.page_views >= 1
      AND src.page_name IS NOT NULL
  )
  SELECT
    session_id,
    session_day,
    lob,

    entry_page_name_raw,
    visit_start_pagename,

    full_url,
    url_path,
    visit_start_page_url,

    site_name,
    device_type,

    session_channel_name,
    adobe_last_touch_channel_name,
    page_view_channel_name,

    session_page_num,
    page_views,
    hit_id,
    date_time,

    entry_row_rank
  FROM ranked
  WHERE entry_row_rank = 1;

  COMMIT TRANSACTION;

END;