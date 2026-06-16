/* =================================================================================================
FILE:         08_vw_sdi_pulseTms_gold_unified_long.sql
LAYER:        Gold View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseTms_gold_unified_long

RAW SOURCES (via Silver):
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_adobeFunnel_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_mfcSpend_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_platformSpend_weekly

PURPOSE:
  Final unified Gold view for the PulseTMS pipeline.
  Single Tableau data source for all PulseTMS reporting.
  Pure UNION of all three Silver tables — zero additional logic or aggregation here.
  All business logic, WoW/YoY, and All Channels rollups live in Silver.

DATA SOURCE VALUES:
  'ADOBE'                  — Adobe UPV funnel at channel_group grain; lob = NULL
  'MFC_SPEND_CHANNEL'      — MFC spend at lob x channel_group; includes 'All Channels' rollup
  'MFC_SPEND_GRANULAR'     — MFC spend at finest grain; all MFC dim columns populated
  'PLATFORM_SPEND_CHANNEL' — Platform spend at lob x channel_group; includes 'All Channels' rollup

  IMPORTANT: MFC contributes two sets of rows (CHANNEL + GRANULAR).
  Always filter on data_source before summing spend to avoid double-counting.

CHANNEL GROUPS (standard vocabulary across all sources):
  'All Channels' | 'Paid Search' | 'Paid Social' | 'Organic Search' |
  'Direct' | 'Programmatic' | 'Other'
  Note: Organic Search and Direct exist in ADOBE only (traffic attribution, not paid media).

LOB CANONICAL VALUES (single lob column — canonical across all sources):
  'POSTPAID'  — MFC: CONSUMER POSTPAID / POSTPAID; Platform: POSTPAID
  'BROADBAND' — MFC: HSI / BROADBAND; Platform: HSI / BROADBAND
  'TFB'       — MFC: TFB / TBG (TBG is legacy)
  NULL        — ADOBE (no LOB dimension; flows are not LOB segments)

COLUMN SCHEMA:
  data_source       — source and grain identifier (see DATA SOURCE VALUES)
  qgp_date          — QGP period-end date (Saturday or quarter-end non-Saturday)
  week_type         — 'NORMAL' | 'BOUNDARY_STUB' | 'BOUNDARY_FIRST'
  qgp_quarter       — display string e.g. '2026 Q1'
  days_in_period    — 7 for NORMAL; <7 for BOUNDARY_STUB; remainder for BOUNDARY_FIRST
  is_complete_period — TRUE when qgp_date <= CURRENT_DATE()
  lob               — canonical LOB (see LOB CANONICAL VALUES above)
  channel_group     — standard channel group (see CHANNEL GROUPS above)
  metric_name       — camelCase metric identifier
  metric_value      — current period value (NULL for incomplete/future periods)
  metric_value_ly   — same metric, same ISO week, prior year
  wow_numerator     — WoW comparison numerator (NULL for BOUNDARY_STUB)
  wow_denominator   — WoW comparison denominator (NULL for BOUNDARY_STUB)
  wow_pct           — pre-computed WoW % (NULL for BOUNDARY_STUB or zero denominator)
  yoy_numerator     — YoY comparison numerator (NULL for BOUNDARY_STUB)
  yoy_denominator   — YoY comparison denominator (NULL for BOUNDARY_STUB)
  yoy_pct           — pre-computed YoY % (NULL for BOUNDARY_STUB or zero denominator)
  max_date          — most recent qgp_date with actual data per data_source x lob x metric_name
  mfc_channel       — MFC_SPEND_GRANULAR only; NULL for all other sources
  mfc_tactic        — MFC_SPEND_GRANULAR only; NULL for all other sources
  mfc_message_type  — MFC_SPEND_GRANULAR only; NULL for all other sources
  mfc_agency        — MFC_SPEND_GRANULAR only; NULL for all other sources

METRIC NAMES BY SOURCE:
  ADOBE:
    upvPostpaid, upvHsi, upvByod, upvFlowTotal, upvTotalAdobe,
    cartstartPostpaid, cartstartHsi, cartstartByod, cartstartTotal,
    ordersUnassistedPostpaid, ordersUnassistedHsi, ordersUnassistedByod, ordersUnassistedTotal,
    ordersAssistedPostpaid, ordersAssistedHsi, ordersAssistedByod, ordersAssistedTotal,
    ordersTotal

  MFC_SPEND_CHANNEL / MFC_SPEND_GRANULAR:
    mfcSpendActual, mfcSpendForecast

  PLATFORM_SPEND_CHANNEL:
    platformSpend

DOWNSTREAM:
  Tableau — direct connection to this view

FUTURE SOURCES:
  Add new UNION ALL branches following the template at the bottom of this file.
  No schema changes needed for existing Tableau calculations.
================================================================================================= */

CREATE OR REPLACE VIEW
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_gold_unified_long`
AS

-- =============================================================================
-- LOB CANONICAL MAPPING (applied consistently across all branches)
-- Raw → Canonical:
--   CONSUMER POSTPAID / POSTPAID → POSTPAID
--   HSI / BROADBAND              → BROADBAND
--   TFB / TBG                    → TFB
--   NULL (Adobe)                 → NULL
-- =============================================================================

-- =============================================================================
-- BRANCH 1: ADOBE
--           UPV funnel at qgp_date x channel_group x metric_name
--           lob = NULL (Adobe has no LOB dimension; flows are not LOB segments)
-- =============================================================================
SELECT
  'ADOBE'                                                                 AS data_source,
  CAST(s.qgp_date AS DATE)                                                AS qgp_date,
  s.week_type,
  s.qgp_quarter,
  s.days_in_period,
  s.is_complete_period,
  CAST(NULL AS STRING)                                                    AS lob,
  s.channel_group,
  s.metric_name,
  s.metric_value,
  s.metric_value_ly,
  s.wow_numerator,
  s.wow_denominator,
  s.wow_pct,
  s.yoy_numerator,
  s.yoy_denominator,
  s.yoy_pct,
  CAST(s.max_date AS DATE)                                                AS max_date,
  CAST(NULL AS STRING)                                                    AS mfc_channel,
  CAST(NULL AS STRING)                                                    AS mfc_tactic,
  CAST(NULL AS STRING)                                                    AS mfc_message_type,
  CAST(NULL AS STRING)                                                    AS mfc_agency
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_adobeFunnel_weekly` s

-- =============================================================================
-- BRANCH 2: MFC_SPEND_CHANNEL
--           MFC spend at lob x channel_group; includes 'All Channels' rollup
--           lob canonicalized from source values
-- =============================================================================
UNION ALL
SELECT
  s.data_source,
  CAST(s.qgp_date AS DATE)                                                AS qgp_date,
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
  END                                                                     AS lob,
  s.channel_group,
  s.metric_name,
  s.metric_value,
  s.metric_value_ly,
  s.wow_numerator,
  s.wow_denominator,
  s.wow_pct,
  s.yoy_numerator,
  s.yoy_denominator,
  s.yoy_pct,
  CAST(s.max_date AS DATE)                                                AS max_date,
  CAST(NULL AS STRING)                                                    AS mfc_channel,
  CAST(NULL AS STRING)                                                    AS mfc_tactic,
  CAST(NULL AS STRING)                                                    AS mfc_message_type,
  CAST(NULL AS STRING)                                                    AS mfc_agency
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_mfcSpend_weekly` s
WHERE s.data_source = 'MFC_SPEND_CHANNEL'

-- =============================================================================
-- BRANCH 3: MFC_SPEND_GRANULAR
--           MFC spend at finest grain; all MFC dim columns populated
--           lob canonicalized from source values
-- =============================================================================
UNION ALL
SELECT
  s.data_source,
  CAST(s.qgp_date AS DATE)                                                AS qgp_date,
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
  END                                                                     AS lob,
  s.channel_group,
  s.metric_name,
  s.metric_value,
  s.metric_value_ly,
  s.wow_numerator,
  s.wow_denominator,
  s.wow_pct,
  s.yoy_numerator,
  s.yoy_denominator,
  s.yoy_pct,
  CAST(s.max_date AS DATE)                                                AS max_date,
  s.channel                                                               AS mfc_channel,
  s.tactic                                                                AS mfc_tactic,
  s.message_type                                                          AS mfc_message_type,
  s.agency                                                                AS mfc_agency
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_mfcSpend_weekly` s
WHERE s.data_source = 'MFC_SPEND_GRANULAR'

-- =============================================================================
-- BRANCH 4: PLATFORM_SPEND_CHANNEL
--           Platform spend at lob x channel_group; includes 'All Channels' rollup
--           lob already canonicalized in Bronze — passed through directly
--           Actuals only (no forecast available from platform source)
-- =============================================================================
UNION ALL
SELECT
  'PLATFORM_SPEND_CHANNEL'                                                AS data_source,
  CAST(s.qgp_date AS DATE)                                                AS qgp_date,
  s.week_type,
  s.qgp_quarter,
  s.days_in_period,
  s.is_complete_period,
  -- LOB already canonical from Bronze (POSTPAID / BROADBAND) — pass through directly
  s.lob                                                                   AS lob,
  s.channel_group,
  s.metric_name,
  s.metric_value,
  s.metric_value_ly,
  s.wow_numerator,
  s.wow_denominator,
  s.wow_pct,
  s.yoy_numerator,
  s.yoy_denominator,
  s.yoy_pct,
  CAST(s.max_date AS DATE)                                                AS max_date,
  CAST(NULL AS STRING)                                                    AS mfc_channel,
  CAST(NULL AS STRING)                                                    AS mfc_tactic,
  CAST(NULL AS STRING)                                                    AS mfc_message_type,
  CAST(NULL AS STRING)                                                    AS mfc_agency
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_platformSpend_weekly` s

/*
  =============================================================================
  FUTURE SOURCES — add new UNION ALL branches here
  =============================================================================
  Template (replace <SOURCE_NAME> and <silver_table_name>):

  UNION ALL
  SELECT
    '<SOURCE_NAME>'         AS data_source,
    CAST(s.qgp_date AS DATE)  AS qgp_date,
    s.week_type,
    s.qgp_quarter,
    s.days_in_period,
    s.is_complete_period,
    s.lob,                  -- or canonical CASE mapping if needed
    s.channel_group,
    s.metric_name,
    s.metric_value,
    s.metric_value_ly,
    s.wow_numerator,
    s.wow_denominator,
    s.wow_pct,
    s.yoy_numerator,
    s.yoy_denominator,
    s.yoy_pct,
    CAST(s.max_date AS DATE)  AS max_date,
    CAST(NULL AS STRING)    AS mfc_channel,
    CAST(NULL AS STRING)    AS mfc_tactic,
    CAST(NULL AS STRING)    AS mfc_message_type,
    CAST(NULL AS STRING)    AS mfc_agency
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.<silver_table_name>` s
  =============================================================================
*/
;