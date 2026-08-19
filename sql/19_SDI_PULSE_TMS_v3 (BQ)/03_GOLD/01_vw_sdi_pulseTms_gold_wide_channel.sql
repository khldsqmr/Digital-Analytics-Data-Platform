/* =================================================================================================
FILE:         09_vw_sdi_pulseTms_gold_wide_channel.sql
LAYER:        Gold View — Wide / Sense Check
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseTms_gold_wide_channel

PURPOSE:
  Wide pivot view for quick sense-checking of the PulseTMS pipeline.
  One row per qgp_date × channel_group, ordered by qgp_date DESC.

  Sources (channel grain only):
    ADOBE              — all UPV + cartstart + orders metrics (no LOB dimension)
    MFC_SPEND_CHANNEL  — mfcSpendActual + mfcSpendForecast (POSTPAID LOB only)
    PLATFORM_SPEND_CHANNEL — platformSpend (POSTPAID LOB only; actuals only)

  NOT included: MFC_SPEND_GRANULAR (use vw_sdi_pulseTms_gold_unified_long for that)

LOB NOTE:
  Spend columns (MFC + Platform) reflect POSTPAID only. Both Silver tables are filtered
  to POSTPAID in their Bronze CROSS JOIN. No LOB column is surfaced here since the grain
  is channel_group only — use vw_sdi_pulseTms_gold_unified_long for LOB-level analysis.

GRAIN:
  qgp_date × channel_group

KEY COLUMNS:
  is_complete_period — TRUE when qgp_date <= CURRENT_DATE(); use to filter to actual data
  week_type          — 'NORMAL' | 'BOUNDARY_STUB' | 'BOUNDARY_FIRST'
  qgp_quarter        — display string e.g. '2026 Q1'

ORDERING:
  qgp_date DESC, channel_group ASC

NOTE:
  This view is for sense-checking only — not intended as a Tableau data source.
  Use vw_sdi_pulseTms_gold_unified_long for all production reporting.

CHANGE LOG:
  - 'quarter' renamed to 'qgp_quarter' to match Silver output schema.
  - 'is_complete_period' added to allow filtering to complete periods.
  - LOB note added to header (spend columns are POSTPAID only).
  - Added WHERE metric_type = 'ADOBE_VOLUME' to Adobe CTE — future-safe filter.
  - Added 17 CVR columns (adobe_cvr_value pivoted per metric_name) to Adobe CTE
    and final SELECT for sense-checking conversion rates alongside volumes.
================================================================================================= */

CREATE OR REPLACE VIEW
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_gold_wide_channel`
AS

WITH

-- ---------------------------------------------------------------------------
-- Adobe metrics — pivot from long to wide at channel_group grain
-- ---------------------------------------------------------------------------
Adobe AS (
  SELECT
    qgp_date,
    week_type,
    qgp_quarter,
    days_in_period,
    is_complete_period,
    channel_group,
    MAX(IF(metric_name = 'upvPostpaid',              metric_value, NULL)) AS upvPostpaid,
    MAX(IF(metric_name = 'upvHsi',                   metric_value, NULL)) AS upvHsi,
    MAX(IF(metric_name = 'upvByod',                  metric_value, NULL)) AS upvByod,
    MAX(IF(metric_name = 'upvFlowTotal',             metric_value, NULL)) AS upvFlowTotal,
    MAX(IF(metric_name = 'upvTotalAdobe',            metric_value, NULL)) AS upvTotalAdobe,
    MAX(IF(metric_name = 'cartstartPostpaid',        metric_value, NULL)) AS cartstartPostpaid,
    MAX(IF(metric_name = 'cartstartHsi',             metric_value, NULL)) AS cartstartHsi,
    MAX(IF(metric_name = 'cartstartByod',            metric_value, NULL)) AS cartstartByod,
    MAX(IF(metric_name = 'cartstartTotal',           metric_value, NULL)) AS cartstartTotal,
    MAX(IF(metric_name = 'ordersUnassistedPostpaid', metric_value, NULL)) AS ordersUnassistedPostpaid,
    MAX(IF(metric_name = 'ordersUnassistedHsi',      metric_value, NULL)) AS ordersUnassistedHsi,
    MAX(IF(metric_name = 'ordersUnassistedByod',     metric_value, NULL)) AS ordersUnassistedByod,
    MAX(IF(metric_name = 'ordersUnassistedTotal',    metric_value, NULL)) AS ordersUnassistedTotal,
    MAX(IF(metric_name = 'ordersAssistedPostpaid',   metric_value, NULL)) AS ordersAssistedPostpaid,
    MAX(IF(metric_name = 'ordersAssistedHsi',        metric_value, NULL)) AS ordersAssistedHsi,
    MAX(IF(metric_name = 'ordersAssistedByod',       metric_value, NULL)) AS ordersAssistedByod,
    MAX(IF(metric_name = 'ordersAssistedTotal',      metric_value, NULL)) AS ordersAssistedTotal,
    MAX(IF(metric_name = 'ordersTotal',              metric_value, NULL)) AS ordersTotal,
    -- CVR values (pre-computed weekly rates — use AVG in Tableau for sense checking)
    MAX(IF(metric_name = 'upvFlowTotal',             adobe_cvr_value, NULL)) AS cvrUpvFlow,
    MAX(IF(metric_name = 'upvPostpaid',              adobe_cvr_value, NULL)) AS cvrUpvPostpaid,
    MAX(IF(metric_name = 'upvHsi',                   adobe_cvr_value, NULL)) AS cvrUpvHsi,
    MAX(IF(metric_name = 'upvByod',                  adobe_cvr_value, NULL)) AS cvrUpvByod,
    MAX(IF(metric_name = 'cartstartTotal',           adobe_cvr_value, NULL)) AS cvrCartstartTotal,
    MAX(IF(metric_name = 'cartstartPostpaid',        adobe_cvr_value, NULL)) AS cvrCartstartPostpaid,
    MAX(IF(metric_name = 'cartstartHsi',             adobe_cvr_value, NULL)) AS cvrCartstartHsi,
    MAX(IF(metric_name = 'cartstartByod',            adobe_cvr_value, NULL)) AS cvrCartstartByod,
    MAX(IF(metric_name = 'ordersTotal',              adobe_cvr_value, NULL)) AS cvrOrdersTotal,
    MAX(IF(metric_name = 'ordersUnassistedTotal',    adobe_cvr_value, NULL)) AS cvrOrdersUnassistedTotal,
    MAX(IF(metric_name = 'ordersAssistedTotal',      adobe_cvr_value, NULL)) AS cvrOrdersAssistedTotal,
    MAX(IF(metric_name = 'ordersUnassistedPostpaid', adobe_cvr_value, NULL)) AS cvrOrdersUnassistedPostpaid,
    MAX(IF(metric_name = 'ordersAssistedPostpaid',   adobe_cvr_value, NULL)) AS cvrOrdersAssistedPostpaid,
    MAX(IF(metric_name = 'ordersUnassistedHsi',      adobe_cvr_value, NULL)) AS cvrOrdersUnassistedHsi,
    MAX(IF(metric_name = 'ordersAssistedHsi',        adobe_cvr_value, NULL)) AS cvrOrdersAssistedHsi,
    MAX(IF(metric_name = 'ordersUnassistedByod',     adobe_cvr_value, NULL)) AS cvrOrdersUnassistedByod,
    MAX(IF(metric_name = 'ordersAssistedByod',       adobe_cvr_value, NULL)) AS cvrOrdersAssistedByod
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_adobeFunnel_weekly`
  WHERE metric_type = 'ADOBE_VOLUME'
  GROUP BY qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, channel_group
),

-- ---------------------------------------------------------------------------
-- MFC spend — POSTPAID channel grain only
-- ---------------------------------------------------------------------------
Mfc AS (
  SELECT
    qgp_date,
    channel_group,
    MAX(IF(metric_name = 'mfcSpendActual',   metric_value, NULL)) AS mfcSpendActual,
    MAX(IF(metric_name = 'mfcSpendForecast', metric_value, NULL)) AS mfcSpendForecast
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_mfcSpend_weekly`
  WHERE data_source = 'MFC_SPEND_CHANNEL'
    AND lob_mfc     = 'POSTPAID'            -- POSTPAID only for channel-level sense check
  GROUP BY qgp_date, channel_group
),

-- ---------------------------------------------------------------------------
-- Platform spend — POSTPAID channel grain only
-- ---------------------------------------------------------------------------
Platform AS (
  SELECT
    qgp_date,
    channel_group,
    MAX(IF(metric_name = 'platformSpend', metric_value, NULL)) AS platformSpend
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_platformSpend_weekly`
  WHERE lob = 'POSTPAID'                    -- POSTPAID only; matches MFC channel grain
  GROUP BY qgp_date, channel_group
)

-- ---------------------------------------------------------------------------
-- Final join — Adobe as spine, MFC + Platform joined in
-- ---------------------------------------------------------------------------
SELECT
  a.qgp_date,
  a.week_type,
  a.qgp_quarter,
  a.days_in_period,
  a.is_complete_period,
  a.channel_group,

  -- Adobe UPV
  a.upvPostpaid,
  a.upvHsi,
  a.upvByod,
  a.upvFlowTotal,
  a.upvTotalAdobe,

  -- Adobe Cartstart
  a.cartstartPostpaid,
  a.cartstartHsi,
  a.cartstartByod,
  a.cartstartTotal,

  -- Adobe Orders
  a.ordersUnassistedPostpaid,
  a.ordersUnassistedHsi,
  a.ordersUnassistedByod,
  a.ordersUnassistedTotal,
  a.ordersAssistedPostpaid,
  a.ordersAssistedHsi,
  a.ordersAssistedByod,
  a.ordersAssistedTotal,
  a.ordersTotal,

  -- MFC Spend (POSTPAID)
  m.mfcSpendActual,
  m.mfcSpendForecast,

  -- Platform Spend (POSTPAID, actuals only)
  p.platformSpend,

  -- Adobe CVR (pre-computed weekly rates)
  a.cvrUpvFlow,
  a.cvrUpvPostpaid,
  a.cvrUpvHsi,
  a.cvrUpvByod,
  a.cvrCartstartTotal,
  a.cvrCartstartPostpaid,
  a.cvrCartstartHsi,
  a.cvrCartstartByod,
  a.cvrOrdersTotal,
  a.cvrOrdersUnassistedTotal,
  a.cvrOrdersAssistedTotal,
  a.cvrOrdersUnassistedPostpaid,
  a.cvrOrdersAssistedPostpaid,
  a.cvrOrdersUnassistedHsi,
  a.cvrOrdersAssistedHsi,
  a.cvrOrdersUnassistedByod,
  a.cvrOrdersAssistedByod

FROM Adobe a
LEFT JOIN Mfc m
  ON  m.qgp_date      = a.qgp_date
  AND m.channel_group = a.channel_group
LEFT JOIN Platform p
  ON  p.qgp_date      = a.qgp_date
  AND p.channel_group = a.channel_group

ORDER BY a.qgp_date DESC, a.channel_group ASC;