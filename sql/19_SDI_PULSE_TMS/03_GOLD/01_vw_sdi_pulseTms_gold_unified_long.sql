/* =================================================================================================
FILE:         08_vw_sdi_pulseTms_gold_unified_long.sql
LAYER:        Gold View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseTms_gold_unified_long

RAW SOURCES (via Silver):
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_adobeFunnel_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_mfcSpend_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_platformSpend_weekly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_gold_unified_long

PURPOSE:
  Final unified Gold view for the PulseTMS pipeline.
  This is the single Tableau data source for all PulseTMS reporting.

  Pure UNION of all three Silver views — zero logic, zero aggregation here.
  All business logic, WoW/YoY computation, and All Channels rollups live in Silver.

DATA SOURCE VALUES:
  'ADOBE'                — Adobe UPV funnel metrics at channel_group grain
  'MFC_SPEND_CHANNEL'    — MFC spend aggregated to channel_group, CONSUMER POSTPAID only
  'MFC_SPEND_GRANULAR'   — MFC spend at finest grain, all LOBs
  'PLATFORM_SPEND_CHANNEL' — Platform spend at channel_group × POSTPAID

  When new sources are added (e.g. SA360, GSC), add a new UNION branch with
  an appropriate data_source value (e.g. 'SA360_CHANNEL', 'SA360_GRANULAR').
  No schema changes needed for existing Tableau calculations.

COLUMN SCHEMA:
  data_source       — identifies source and grain (see DATA SOURCE VALUES above)
  qgp_date          — QGP period-end date (Sat or quarter-end non-Sat)
  week_type         — 'NORMAL' | 'BOUNDARY_STUB' | 'BOUNDARY_FIRST'
  quarter           — e.g. '2026 Q1'
  days_in_period    — 7 for NORMAL; <7 for stubs
  channel_group     — 'All Channels' | 'Paid Search' | 'Paid Social' |
                      'Programmatic' | 'Other' | 'Direct' | 'Organic Search'
  metric_name       — camelCase metric identifier (see per-source metric lists below)
  metric_value      — current period value (NULL for incomplete/future periods)
  metric_value_ly   — same metric, same ISO week, prior year
  wow_numerator     — for correct WoW % calculation (NULL for BOUNDARY_STUB)
  wow_denominator   — prior period value for WoW (NULL for BOUNDARY_STUB)
  wow_pct           — wow_numerator / wow_denominator - 1 (NULL for BOUNDARY_STUB / zero denom)
  yoy_numerator     — for correct YoY % calculation (NULL for BOUNDARY_STUB)
  yoy_denominator   — prior year period value for YoY (NULL for BOUNDARY_STUB)
  yoy_pct           — yoy_numerator / yoy_denominator - 1 (NULL for BOUNDARY_STUB / zero denom)
  max_date          — most recent qgp_date with actual data, per data_source × metric_name
  lob_mfc           — MFC LOB: 'CONSUMER POSTPAID' for CHANNEL grain; all LOBs for GRANULAR;
                      NULL for ADOBE and PLATFORM rows
  lob_platform      — Platform LOB: 'POSTPAID' for PLATFORM rows; NULL for all other sources
  mfc_channel       — Raw MFC channel (MFC_SPEND_GRANULAR only; NULL elsewhere)
  mfc_tactic        — MFC tactic (MFC_SPEND_GRANULAR only; NULL elsewhere)
  mfc_message_type  — MFC message type (MFC_SPEND_GRANULAR only; NULL elsewhere)
  mfc_agency        — MFC agency (MFC_SPEND_GRANULAR only; NULL elsewhere)

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

BUSINESS RULES:
  - BOUNDARY_STUB rows included with NULL metric values and NULL WoW/YoY fields.
    Present so Tableau renders the QGP date spine correctly.
  - WoW % = wow_numerator / wow_denominator - 1 (pre-computed; use wow_pct directly)
  - YoY % = yoy_numerator / yoy_denominator - 1 (pre-computed; use yoy_pct directly)
  - MFC contributes TWO sets of rows (CHANNEL + GRANULAR); filter on data_source
    to avoid double-counting.
  - All Channels rows are pre-computed in Silver — no further aggregation needed.

DOWNSTREAM:
  Tableau — direct connection to this view
================================================================================================= */

CREATE OR REPLACE VIEW
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_gold_unified_long`
AS

-- =============================================================================
-- BRANCH 1: ADOBE
--           UPV funnel metrics at qgp_date × channel_group × metric_name
--           Includes 'All Channels' row from Adobe ALL source tables
-- =============================================================================
SELECT
  'ADOBE'                                                               AS data_source,
  s.qgp_date,
  s.week_type,
  s.quarter,
  s.days_in_period,
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
  s.max_date,
  CAST(NULL AS STRING)                                                  AS lob_mfc,
  CAST(NULL AS STRING)                                                  AS lob_platform,
  CAST(NULL AS STRING)                                                  AS mfc_channel,
  CAST(NULL AS STRING)                                                  AS mfc_tactic,
  CAST(NULL AS STRING)                                                  AS mfc_message_type,
  CAST(NULL AS STRING)                                                  AS mfc_agency

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_adobeFunnel_weekly` s

-- =============================================================================
-- BRANCH 2: MFC_SPEND_CHANNEL
--           MFC spend aggregated to channel_group, CONSUMER POSTPAID only
--           Includes 'All Channels' rollup
-- =============================================================================
UNION ALL
SELECT
  s.data_source,
  s.qgp_date,
  s.week_type,
  s.quarter,
  s.days_in_period,
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
  s.max_date,
  s.lob_mfc,
  CAST(NULL AS STRING)                                                  AS lob_platform,
  CAST(NULL AS STRING)                                                  AS mfc_channel,
  CAST(NULL AS STRING)                                                  AS mfc_tactic,
  CAST(NULL AS STRING)                                                  AS mfc_message_type,
  CAST(NULL AS STRING)                                                  AS mfc_agency

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_mfcSpend_weekly` s
WHERE s.data_source = 'MFC_SPEND_CHANNEL'

-- =============================================================================
-- BRANCH 3: MFC_SPEND_GRANULAR
--           MFC spend at finest grain, all LOBs, all MFC dims populated
--           No 'All Channels' rollup at granular level
-- =============================================================================
UNION ALL
SELECT
  s.data_source,
  s.qgp_date,
  s.week_type,
  s.quarter,
  s.days_in_period,
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
  s.max_date,
  s.lob_mfc,
  CAST(NULL AS STRING)                                                  AS lob_platform,
  s.channel                                                             AS mfc_channel,
  s.tactic                                                              AS mfc_tactic,
  s.message_type                                                        AS mfc_message_type,
  s.agency                                                              AS mfc_agency

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_mfcSpend_weekly` s
WHERE s.data_source = 'MFC_SPEND_GRANULAR'

-- =============================================================================
-- BRANCH 4: PLATFORM_SPEND_CHANNEL
--           Platform spend at channel_group × POSTPAID
--           Includes 'All Channels' per LOB rollup
-- =============================================================================
UNION ALL
SELECT
  'PLATFORM_SPEND_CHANNEL'                                              AS data_source,
  s.qgp_date,
  s.week_type,
  s.quarter,
  s.days_in_period,
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
  s.max_date,
  CAST(NULL AS STRING)                                                  AS lob_mfc,
  s.lob                                                                 AS lob_platform,
  CAST(NULL AS STRING)                                                  AS mfc_channel,
  CAST(NULL AS STRING)                                                  AS mfc_tactic,
  CAST(NULL AS STRING)                                                  AS mfc_message_type,
  CAST(NULL AS STRING)                                                  AS mfc_agency

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_platformSpend_weekly` s

/*
  =============================================================================
  FUTURE SOURCES — add new UNION ALL branches here
  =============================================================================
  Template:
  UNION ALL
  SELECT
    'SA360_CHANNEL'         AS data_source,   -- or 'SA360_GRANULAR', 'GSC_CHANNEL', etc.
    s.qgp_date,
    s.week_type,
    s.quarter,
    s.days_in_period,
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
    s.max_date,
    CAST(NULL AS STRING)    AS lob_mfc,
    CAST(NULL AS STRING)    AS lob_platform,
    CAST(NULL AS STRING)    AS mfc_channel,
    CAST(NULL AS STRING)    AS mfc_tactic,
    CAST(NULL AS STRING)    AS mfc_message_type,
    CAST(NULL AS STRING)    AS mfc_agency
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_<source>_weekly` s
  =============================================================================
*/
;