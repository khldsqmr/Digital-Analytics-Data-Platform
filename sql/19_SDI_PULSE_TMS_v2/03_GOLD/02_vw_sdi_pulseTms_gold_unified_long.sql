/* =================================================================================================
FILE:         08_vw_sdi_pulseTms_gold_unified_long.sql
LAYER:        Gold View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseTms_gold_unified_long

PURPOSE:
  Final unified Gold view for the PulseTMS pipeline.
  Single Tableau data source for all PulseTMS reporting.

  Pure pass-through view — zero computation here.
  All heavy processing (proration, WoW/YoY, CVR numerator/denominator) lives in Silver SPs.
  This view simply assembles Silver outputs via named CTEs and stacks them with UNION ALL.

STRUCTURE:
  CTE 1 — AdobeVolume   : Adobe funnel volume metrics + inline CVR columns (ADOBE_VOLUME)
  CTE 2 — MfcChannel    : MFC spend at lob x channel_group grain (MFC_SPEND_CHANNEL)
  CTE 3 — MfcGranular   : MFC spend at finest grain (MFC_SPEND_GRANULAR)
  CTE 4 — PlatformSpend : Platform spend at lob x channel_group grain (PLATFORM_SPEND_CHANNEL)
  Final SELECT: UNION ALL of all four CTEs

DATA SOURCE VALUES:
  'ADOBE'                  — Adobe volume + CVR rows; lob = NULL
  'MFC_SPEND_CHANNEL'      — MFC spend at lob x channel_group; includes All Channels rollup
  'MFC_SPEND_GRANULAR'     — MFC spend at finest grain; mfc_* columns populated
  'PLATFORM_SPEND_CHANNEL' — Platform spend at lob x channel_group; POSTPAID + BROADBAND

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
  NULL        — ADOBE (no LOB dimension)

METRIC_TYPE VALUES:
  'ADOBE_VOLUME'       — raw Adobe funnel metrics (upv*, cartstart*, orders*)
                         use SUM([metric_value]) in Tableau
                         each row also carries inline CVR columns:
                           adobe_cvr_value       = weekly CVR (AVG in Tableau)
                           adobe_cvr_numerator   = CVR numerator (SUM for QTD)
                           adobe_cvr_denominator = CVR denominator (SUM for QTD)
                         upvTotalAdobe has NULL CVR columns (no CVR defined)
  'MFC_SPEND_ACTUAL'   — MFC actual spend; use SUM([metric_value])
  'MFC_SPEND_FORECAST' — MFC forecast spend; use SUM([metric_value])
  'PLATFORM_SPEND'     — Platform actual spend; use SUM([metric_value])

COLUMN SCHEMA:
  data_source            — source and grain identifier
  qgp_date               — QGP period-end date (Saturday or quarter-end non-Saturday)
  week_type              — 'NORMAL' | 'BOUNDARY_STUB' | 'BOUNDARY_FIRST'
  qgp_quarter            — display string e.g. '2026 Q1'
  days_in_period         — 7 for NORMAL; <7 for BOUNDARY_STUB; remainder for BOUNDARY_FIRST
  is_complete_period     — TRUE when qgp_date <= CURRENT_DATE()
  lob                    — canonical LOB (NULL for ADOBE)
  channel_group          — standard channel group
  metric_name            — camelCase metric identifier
  metric_type            — see METRIC_TYPE VALUES above
  metric_value           — volume/spend value; NULL for CVR rows
  metric_value_ly        — prior year value; NULL for CVR rows
  wow_numerator          — NULL for BOUNDARY_STUB and CVR rows
  wow_denominator        — NULL for BOUNDARY_STUB and CVR rows
  wow_pct                — NULL for BOUNDARY_STUB, CVR, zero denominator
  yoy_numerator          — NULL for BOUNDARY_STUB and CVR rows
  yoy_denominator        — NULL for BOUNDARY_STUB and CVR rows
  yoy_pct                — NULL for BOUNDARY_STUB, CVR, zero denominator
  max_date               — most recent qgp_date with non-NULL metric_value
  adobe_cvr_value        — pre-computed weekly CVR; use AVG([adobe_cvr_value]) in Tableau
                           NULL for upvTotalAdobe and all non-ADOBE rows
  adobe_cvr_numerator    — CVR numerator; use SUM for QTD CVR calculation
                           NULL for upvTotalAdobe and all non-ADOBE rows
  adobe_cvr_denominator  — CVR denominator; use SUM for QTD CVR calculation
                           NULL for upvTotalAdobe and all non-ADOBE rows
  mfc_channel            — MFC_SPEND_GRANULAR only; NULL elsewhere
  mfc_tactic             — MFC_SPEND_GRANULAR only; NULL elsewhere
  mfc_message_type       — MFC_SPEND_GRANULAR only; NULL elsewhere
  mfc_agency             — MFC_SPEND_GRANULAR only; NULL elsewhere

DOWNSTREAM:
  Tableau — direct connection to this view

FUTURE SOURCES:
  Add a new named CTE following the template at the bottom,
  then add it to the final UNION ALL. No schema changes needed.
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
--        Use SUM([metric_value]) in Tableau
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
    CAST(NULL AS STRING)                                                  AS mfc_agency
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_adobeFunnel_weekly` s
  WHERE s.metric_type = 'ADOBE_VOLUME'
),

-- =============================================================================
-- CTE 2: MFC SPEND — CHANNEL GRAIN
--        mfcSpendActual + mfcSpendForecast at lob x channel_group
--        Includes All Channels rollup per lob
--        lob canonicalized from source values
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
    CAST(NULL AS STRING)                                                  AS mfc_agency
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_mfcSpend_weekly` s
  WHERE s.data_source = 'MFC_SPEND_CHANNEL'
),

-- =============================================================================
-- CTE 3: MFC SPEND — GRANULAR GRAIN
--        mfcSpendActual + mfcSpendForecast at finest grain
--        mfc_* dimension columns populated
--        lob canonicalized from source values
--        metric_type = 'MFC_SPEND_ACTUAL' or 'MFC_SPEND_FORECAST'
--        NOTE: never aggregate MFC_SPEND_CHANNEL + MFC_SPEND_GRANULAR together
--              — always filter on data_source to avoid double-counting
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
    s.agency                                                              AS mfc_agency
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_mfcSpend_weekly` s
  WHERE s.data_source = 'MFC_SPEND_GRANULAR'
),

-- =============================================================================
-- CTE 4: PLATFORM SPEND
--        platformSpend (actuals only) at lob x channel_group
--        LOBs: POSTPAID and BROADBAND
--        Includes All Channels rollup per lob
--        lob already canonical from Bronze — passed through directly
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
    CAST(NULL AS STRING)                                                  AS mfc_agency
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_platformSpend_weekly` s
)

-- =============================================================================
-- FINAL: Stack all five CTEs
-- =============================================================================
SELECT * FROM AdobeVolume
UNION ALL SELECT * FROM MfcChannel
UNION ALL SELECT * FROM MfcGranular
UNION ALL SELECT * FROM PlatformSpend

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
      CAST(NULL AS FLOAT64)     AS adobe_cvr_numerator,
      CAST(NULL AS FLOAT64)     AS adobe_cvr_denominator,
      CAST(NULL AS STRING)      AS mfc_channel,
      CAST(NULL AS STRING)      AS mfc_tactic,
      CAST(NULL AS STRING)      AS mfc_message_type,
      CAST(NULL AS STRING)      AS mfc_agency
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.<silver_table>` s
  )
  =============================================================================
*/
;