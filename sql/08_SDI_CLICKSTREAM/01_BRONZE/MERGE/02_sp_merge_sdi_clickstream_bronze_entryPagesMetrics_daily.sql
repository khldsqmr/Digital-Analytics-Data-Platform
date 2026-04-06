/* =================================================================================================
FILE: 02_sp_merge_sdi_clickstream_bronze_entryPagesMetrics_daily.sql
LAYER: Bronze
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROCEDURE: sp_merge_sdi_clickstream_bronze_entryPagesMetrics_daily

SOURCE (RAW):
  prj-dbi-prd-1.ds_dbi_marketing.fact_all_hits

TARGET:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_clickstream_bronze_entryPagesMetrics_daily

PURPOSE:
  Refresh Bronze session-level metrics table for Entry Pages funnel analysis using a recent
  lookback window.
  Refresh Bronze session-level metrics table from a caller-supplied start date.

BUSINESS GRAIN:
  session_id + session_day

WHY THIS PROCEDURE EXISTS:
  It materializes session-level funnel flags once so that Silver and Gold do not need to rescan
  the huge raw clickstream table.

REFRESH STRATEGY:
  Rolling lookback window of last 14 days for production refresh.

ONE-TIME BACKFILL:
  For initial historical backfill, replace:
      day >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
  with:
      day >= DATE '2025-01-01'

FLAG LOGIC:
  A session gets:
  - has_pspv = 1 if any hit in session has postpaid_voice_pspvplus > 0
  - has_cart_start = 1 if any hit in session has cart_opens > 0
  - has_checkout = 1 if any hit in session has checkouts > 0
  - has_order = 1 if any hit in session has orders > 0

NOTES:
  - This is intentionally session-based, not event-count based.
  - These flags are suitable for funnel conversion reporting by entry-page cohort.
================================================================================================= */

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_sdi_clickstream_bronze_entryPagesMetrics_daily`(p_start_date DATE)
OPTIONS(strict_mode=false)
BEGIN

  BEGIN TRANSACTION;

  DELETE FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_clickstream_bronze_entryPagesMetrics_daily`
  WHERE session_day IN (
    SELECT DISTINCT src.day
    FROM `prj-dbi-prd-1.ds_dbi_marketing.fact_all_hits` src
    WHERE src.day >= p_start_date
      AND src.lob = 'Postpaid'
  );

  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_clickstream_bronze_entryPagesMetrics_daily`
  (
    session_id,
    session_day,
    lob,
    has_pspv,
    has_cart_start,
    has_checkout,
    has_order
  )
  SELECT
    src.session_id,
    src.day AS session_day,
    'POSTPAID' AS lob,
    MAX(IF(COALESCE(src.postpaid_voice_pspvplus, 0) > 0, 1, 0)) AS has_pspv,
    MAX(IF(COALESCE(src.cart_opens, 0) > 0, 1, 0)) AS has_cart_start,
    MAX(IF(COALESCE(src.checkouts, 0) > 0, 1, 0)) AS has_checkout,
    MAX(IF(COALESCE(src.orders, 0) > 0, 1, 0)) AS has_order
  FROM `prj-dbi-prd-1.ds_dbi_marketing.fact_all_hits` src
  WHERE src.day >= p_start_date
    AND src.lob = 'Postpaid'
  GROUP BY
    src.session_id,
    src.day;

  COMMIT TRANSACTION;

END;