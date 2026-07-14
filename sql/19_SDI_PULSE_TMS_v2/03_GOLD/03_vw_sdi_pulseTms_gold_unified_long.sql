/* =================================================================================================
FILE:         03_vw_sdi_pulseTms_gold_unified_long.sql
LAYER:        Gold View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseTms_gold_unified_long

PURPOSE:
  Final unified Gold view for the PulseTMS pipeline.
  Single Tableau data source for all PulseTMS reporting.

  Pure pass-through view — zero computation here.
  All heavy processing (proration, WoW/YoY, CVR, channel allocation) lives in Silver SPs.
  This view simply assembles Silver outputs via named CTEs and stacks them with UNION ALL.

STRUCTURE:
  CTE 1 — AdobeVolume   : Adobe funnel volume metrics + inline CVR columns (ADOBE_VOLUME)
  CTE 2 — MfcChannel    : MFC spend at lob x channel_group grain (MFC_SPEND_CHANNEL)
  CTE 3 — MfcGranular   : MFC spend at finest grain (MFC_SPEND_GRANULAR)
  CTE 4 — PlatformSpend : Platform spend at lob x channel_group grain (PLATFORM_SPEND_CHANNEL)
  CTE 5 — UpvForecast   : UPV forecast channel-allocated (UPV_FORECAST)
  Final SELECT: UNION ALL of all five CTEs

DATA SOURCE VALUES:
  'ADOBE'                  — Adobe volume + CVR rows; lob = NULL
  'MFC_SPEND_CHANNEL'      — MFC spend at lob x channel_group; includes All Channels rollup
  'MFC_SPEND_GRANULAR'     — MFC spend at finest grain; mfc_* columns populated
  'PLATFORM_SPEND_CHANNEL' — Platform spend at lob x channel_group; POSTPAID + BROADBAND
  'UPV_FORECAST'           — UPV forecast channel-allocated; lob = NULL

  IMPORTANT: MFC contributes two sets of rows (CHANNEL + GRANULAR).
  Always filter on data_source before summing spend to avoid double-counting.

CHANNEL GROUPS (standard vocabulary):
  'All Channels' | 'Paid Search' | 'Paid Social' | 'Organic Search' |
  'Direct' | 'Programmatic' | 'Other'
  Note: Organic Search and Direct exist in ADOBE only.

LOB CANONICAL VALUES:
  'POSTPAID'  — MFC: CONSUMER POSTPAID / POSTPAID; Platform: POSTPAID
  'BROADBAND' — MFC: HSI / BROADBAND; Platform: BROADBAND
  'TFB'       — MFC: TFB / TBG (TBG is legacy)
  NULL        — ADOBE, UPV_FORECAST (no LOB dimension)

METRIC_TYPE VALUES:
  'ADOBE_VOLUME'       — raw Adobe funnel metrics (upv*, cartstart*, orders*)
  'MFC_SPEND_ACTUAL'   — MFC actual spend
  'MFC_SPEND_FORECAST' — MFC forecast spend
  'PLATFORM_SPEND'     — Platform actual spend
  'UPV_FORECAST'       — UPV forecast (upvForecast | upvWebAppForecast)
                         allocation_ratio column shows channel split source

COLUMN SCHEMA:
  data_source            — source and grain identifier
  qgp_date               — QGP period-end date (Saturday or quarter-end non-Saturday)
  week_type              — 'NORMAL' | 'BOUNDARY_STUB' | 'BOUNDARY_FIRST'
  qgp_quarter            — display string e.g. '2026 Q1'
  days_in_period         — 7 for NORMAL; <7 for BOUNDARY_STUB; remainder for BOUNDARY_FIRST
  is_complete_period     — TRUE when qgp_date <= CURRENT_DATE()
  lob                    — canonical LOB (NULL for ADOBE and UPV_FORECAST)
  channel_group          — standard channel group
  metric_name            — camelCase metric identifier
  metric_type            — see METRIC_TYPE VALUES above
  metric_value           — volume/spend/forecast value
  metric_value_ly        — prior year value
  wow_numerator          — NULL for BOUNDARY_STUB rows
  wow_denominator        — NULL for BOUNDARY_STUB rows
  wow_pct                — NULL for BOUNDARY_STUB or zero denominator
  yoy_numerator          — NULL for BOUNDARY_STUB rows
  yoy_denominator        — NULL for BOUNDARY_STUB rows
  yoy_pct                — NULL for BOUNDARY_STUB or zero denominator
  max_date               — most recent qgp_date with non-NULL metric_value
  adobe_cvr_value        — pre-computed weekly CVR; ADOBE only; NULL elsewhere
  adobe_cvr_numerator    — CVR numerator; ADOBE only; NULL elsewhere
  adobe_cvr_denominator  — CVR denominator; ADOBE only; NULL elsewhere
  mfc_channel            — MFC_SPEND_GRANULAR only; NULL elsewhere
  mfc_tactic             — MFC_SPEND_GRANULAR only; NULL elsewhere
  mfc_message_type       — MFC_SPEND_GRANULAR only; NULL elsewhere
  mfc_agency             — MFC_SPEND_GRANULAR only; NULL elsewhere
  allocation_ratio       — UPV_FORECAST only; channel share from prior year same quarter
                           NULL for all other data_source values

DOWNSTREAM:
  Tableau — direct connection to this view

FUTURE SOURCES:
  Add a new named CTE above the final SELECT following the template at the bottom,
  then add one UNION ALL line. No schema changes needed for existing Tableau calculations.

CHANGE LOG:
  - Added CTE 5 UpvForecast: UPV forecast channel-allocated (data_source = 'UPV_FORECAST').
  - Added allocation_ratio column to schema (NULL for all non-UPV_FORECAST rows).
================================================================================================= */

CREATE OR REPLACE VIEW
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_gold_unified_long`
AS

WITH

-- =============================================================================
-- CTE 1: ADOBE VOLUME METRICS
--        upv*, cartstart*, orders* at qgp_date x channel_group x metric_name
--        lob = NULL — Adobe has no LOB dimension
--        metric_type = 'ADOBE_VOLUME'
-- =============================================================================
AdobeVolume AS (
  SELECT
    'ADOBE'                                                               AS data_source,
    CAST(s.qgp_date AS DATE)                                              AS qgp_date,
    s.week_type,
    s.qgp_quarter,
    s.days_in_period,
    s.is_complete_period,
    CAST(NULL AS STRING)                                                  AS lob,
    s.channel_group,
    s.metric_name,
    s.metric_type,
    s.metric_value,
    s.metric_value_ly,
    s.wow_numerator,
    s.wow_denominator,
    s.wow_pct,
    s.yoy_numerator,
    s.yoy_denominator,
    s.yoy_pct,
    CAST(s.max_date AS DATE)                                              AS max_date,
    s.adobe_cvr_value,
    s.adobe_cvr_numerator,
    s.adobe_cvr_denominator,
    CAST(NULL AS STRING)                                                  AS mfc_channel,
    CAST(NULL AS STRING)                                                  AS mfc_tactic,
    CAST(NULL AS STRING)                                                  AS mfc_message_type,
    CAST(NULL AS STRING)                                                  AS mfc_agency,
    CAST(NULL AS FLOAT64)                                                 AS allocation_ratio
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_adobeFunnel_weekly` s
  WHERE s.metric_type = 'ADOBE_VOLUME'
),

-- =============================================================================
-- CTE 2: MFC SPEND — CHANNEL GRAIN
--        mfcSpendActual + mfcSpendForecast at lob x channel_group
--        Includes All Channels rollup per lob
--        metric_type = 'MFC_SPEND_ACTUAL' or 'MFC_SPEND_FORECAST'
-- =============================================================================
MfcChannel AS (
  SELECT
    s.data_source,
    CAST(s.qgp_date AS DATE)                                              AS qgp_date,
    s.week_type,
    s.qgp_quarter,
    s.days_in_period,
    s.is_complete_period,
    CASE s.lob_mfc
      WHEN 'CONSUMER POSTPAID' THEN 'POSTPAID'
      WHEN 'POSTPAID'          THEN 'POSTPAID'
      WHEN 'HSI'               THEN 'BROADBAND'
      WHEN 'BROADBAND'         THEN 'BROADBAND'
      WHEN 'TBG'               THEN 'TFB'
      WHEN 'TFB'               THEN 'TFB'
      ELSE s.lob_mfc
    END                                                                   AS lob,
    s.channel_group,
    s.metric_name,
    CASE s.metric_name
      WHEN 'mfcSpendActual'   THEN 'MFC_SPEND_ACTUAL'
      WHEN 'mfcSpendForecast' THEN 'MFC_SPEND_FORECAST'
    END                                                                   AS metric_type,
    s.metric_value,
    s.metric_value_ly,
    s.wow_numerator,
    s.wow_denominator,
    s.wow_pct,
    s.yoy_numerator,
    s.yoy_denominator,
    s.yoy_pct,
    CAST(s.max_date AS DATE)                                              AS max_date,
    CAST(NULL AS FLOAT64)                                                 AS adobe_cvr_value,
    CAST(NULL AS FLOAT64)                                                 AS adobe_cvr_numerator,
    CAST(NULL AS FLOAT64)                                                 AS adobe_cvr_denominator,
    CAST(NULL AS STRING)                                                  AS mfc_channel,
    CAST(NULL AS STRING)                                                  AS mfc_tactic,
    CAST(NULL AS STRING)                                                  AS mfc_message_type,
    CAST(NULL AS STRING)                                                  AS mfc_agency,
    CAST(NULL AS FLOAT64)                                                 AS allocation_ratio
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_mfcSpend_weekly` s
  WHERE s.data_source = 'MFC_SPEND_CHANNEL'
),

-- =============================================================================
-- CTE 3: MFC SPEND — GRANULAR GRAIN
--        mfcSpendActual + mfcSpendForecast at finest grain
--        mfc_* dimension columns populated
--        metric_type = 'MFC_SPEND_ACTUAL' or 'MFC_SPEND_FORECAST'
-- =============================================================================
MfcGranular AS (
  SELECT
    s.data_source,
    CAST(s.qgp_date AS DATE)                                              AS qgp_date,
    s.week_type,
    s.qgp_quarter,
    s.days_in_period,
    s.is_complete_period,
    CASE s.lob_mfc
      WHEN 'CONSUMER POSTPAID' THEN 'POSTPAID'
      WHEN 'POSTPAID'          THEN 'POSTPAID'
      WHEN 'HSI'               THEN 'BROADBAND'
      WHEN 'BROADBAND'         THEN 'BROADBAND'
      WHEN 'TBG'               THEN 'TFB'
      WHEN 'TFB'               THEN 'TFB'
      ELSE s.lob_mfc
    END                                                                   AS lob,
    s.channel_group,
    s.metric_name,
    CASE s.metric_name
      WHEN 'mfcSpendActual'   THEN 'MFC_SPEND_ACTUAL'
      WHEN 'mfcSpendForecast' THEN 'MFC_SPEND_FORECAST'
    END                                                                   AS metric_type,
    s.metric_value,
    s.metric_value_ly,
    s.wow_numerator,
    s.wow_denominator,
    s.wow_pct,
    s.yoy_numerator,
    s.yoy_denominator,
    s.yoy_pct,
    CAST(s.max_date AS DATE)                                              AS max_date,
    CAST(NULL AS FLOAT64)                                                 AS adobe_cvr_value,
    CAST(NULL AS FLOAT64)                                                 AS adobe_cvr_numerator,
    CAST(NULL AS FLOAT64)                                                 AS adobe_cvr_denominator,
    s.channel                                                             AS mfc_channel,
    s.tactic                                                              AS mfc_tactic,
    s.message_type                                                        AS mfc_message_type,
    s.agency                                                              AS mfc_agency,
    CAST(NULL AS FLOAT64)                                                 AS allocation_ratio
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_mfcSpend_weekly` s
  WHERE s.data_source = 'MFC_SPEND_GRANULAR'
),

-- =============================================================================
-- CTE 4: PLATFORM SPEND
--        platformSpend (actuals only) at lob x channel_group
--        LOBs: POSTPAID and BROADBAND; includes All Channels rollup per lob
--        metric_type = 'PLATFORM_SPEND'
-- =============================================================================
PlatformSpend AS (
  SELECT
    'PLATFORM_SPEND_CHANNEL'                                              AS data_source,
    CAST(s.qgp_date AS DATE)                                              AS qgp_date,
    s.week_type,
    s.qgp_quarter,
    s.days_in_period,
    s.is_complete_period,
    s.lob,
    s.channel_group,
    s.metric_name,
    'PLATFORM_SPEND'                                                      AS metric_type,
    s.metric_value,
    s.metric_value_ly,
    s.wow_numerator,
    s.wow_denominator,
    s.wow_pct,
    s.yoy_numerator,
    s.yoy_denominator,
    s.yoy_pct,
    CAST(s.max_date AS DATE)                                              AS max_date,
    CAST(NULL AS FLOAT64)                                                 AS adobe_cvr_value,
    CAST(NULL AS FLOAT64)                                                 AS adobe_cvr_numerator,
    CAST(NULL AS FLOAT64)                                                 AS adobe_cvr_denominator,
    CAST(NULL AS STRING)                                                  AS mfc_channel,
    CAST(NULL AS STRING)                                                  AS mfc_tactic,
    CAST(NULL AS STRING)                                                  AS mfc_message_type,
    CAST(NULL AS STRING)                                                  AS mfc_agency,
    CAST(NULL AS FLOAT64)                                                 AS allocation_ratio
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_platformSpend_weekly` s
),

-- =============================================================================
-- CTE 5: UPV FORECAST
--        upvForecast + upvWebAppForecast at qgp_date x channel_group
--        Channel split allocated from prior year same quarter Adobe actuals
--        lob = NULL — no LOB dimension (same as ADOBE)
--        metric_type = 'UPV_FORECAST'
--        allocation_ratio — channel share used; NULL for All Channels (ratio = 1.0)
--        Filter on data_source = 'UPV_FORECAST' in Tableau to isolate forecast rows
-- =============================================================================
UpvForecast AS (
  SELECT
    'UPV_FORECAST'                                                        AS data_source,
    CAST(s.qgp_date AS DATE)                                              AS qgp_date,
    s.week_type,
    s.qgp_quarter,
    s.days_in_period,
    s.is_complete_period,
    CAST(NULL AS STRING)                                                  AS lob,
    s.channel_group,
    s.metric_name,
    s.metric_type,
    s.metric_value,
    s.metric_value_ly,
    s.wow_numerator,
    s.wow_denominator,
    s.wow_pct,
    s.yoy_numerator,
    s.yoy_denominator,
    s.yoy_pct,
    CAST(s.max_date AS DATE)                                              AS max_date,
    CAST(NULL AS FLOAT64)                                                 AS adobe_cvr_value,
    CAST(NULL AS FLOAT64)                                                 AS adobe_cvr_numerator,
    CAST(NULL AS FLOAT64)                                                 AS adobe_cvr_denominator,
    CAST(NULL AS STRING)                                                  AS mfc_channel,
    CAST(NULL AS STRING)                                                  AS mfc_tactic,
    CAST(NULL AS STRING)                                                  AS mfc_message_type,
    CAST(NULL AS STRING)                                                  AS mfc_agency,
    s.allocation_ratio
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_upvForecast_weekly` s
)

-- =============================================================================
-- FINAL: Stack all five CTEs
-- =============================================================================
SELECT * FROM AdobeVolume
UNION ALL SELECT * FROM MfcChannel
UNION ALL SELECT * FROM MfcGranular
UNION ALL SELECT * FROM PlatformSpend
UNION ALL SELECT * FROM UpvForecast

/*
  =============================================================================
  FUTURE SOURCES — add a new named CTE above following this template,
  then add one UNION ALL line in the final SELECT above.
  No schema changes needed for existing Tableau calculations.
  =============================================================================
  NewSource AS (
    SELECT
      '<SOURCE_NAME>'           AS data_source,
      CAST(s.qgp_date AS DATE)  AS qgp_date,
      s.week_type,
      s.qgp_quarter,
      s.days_in_period,
      s.is_complete_period,
      s.lob,
      s.channel_group,
      s.metric_name,
      '<METRIC_TYPE>'           AS metric_type,
      s.metric_value,
      s.metric_value_ly,
      s.wow_numerator,
      s.wow_denominator,
      s.wow_pct,
      s.yoy_numerator,
      s.yoy_denominator,
      s.yoy_pct,
      CAST(s.max_date AS DATE)  AS max_date,
      CAST(NULL AS FLOAT64)     AS adobe_cvr_value,
      CAST(NULL AS FLOAT64)     AS adobe_cvr_numerator,
      CAST(NULL AS FLOAT64)     AS adobe_cvr_denominator,
      CAST(NULL AS STRING)      AS mfc_channel,
      CAST(NULL AS STRING)      AS mfc_tactic,
      CAST(NULL AS STRING)      AS mfc_message_type,
      CAST(NULL AS STRING)      AS mfc_agency,
      CAST(NULL AS FLOAT64)     AS allocation_ratio
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.<silver_table>` s
  )
  =============================================================================
*/
;