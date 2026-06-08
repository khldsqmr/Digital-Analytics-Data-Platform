/* =================================================================================================
FILE:         01_vw_sdi_pulseTms_gold_wide_channel.sql
LAYER:        Gold View — Wide / Sense Check
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseTms_gold_wide_channel

PURPOSE:
  Wide pivot view for quick sense-checking of the PulseTMS pipeline.
  One row per qgp_date × channel_group, ordered by qgp_date DESC.

  Sources (channel grain only):
    ADOBE              — all UPV + cartstart + orders metrics
    MFC_SPEND_CHANNEL  — mfcSpendActual + mfcSpendForecast (CONSUMER POSTPAID only)
    PLATFORM_SPEND_CHANNEL — platformSpend (POSTPAID only)

  NOT included: MFC_SPEND_GRANULAR (lob/tactic/channel detail — use Gold long view for that)

GRAIN:
  qgp_date × channel_group

ORDERING:
  qgp_date DESC, channel_group ASC

NOTE:
  This view is for sense-checking only — not intended as a Tableau data source.
  Use vw_sdi_pulseTms_gold_unified_long for all production reporting.
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
    quarter,
    days_in_period,
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
    MAX(IF(metric_name = 'ordersTotal',              metric_value, NULL)) AS ordersTotal
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_adobeFunnel_weekly`
  GROUP BY qgp_date, week_type, quarter, days_in_period, channel_group
),

-- ---------------------------------------------------------------------------
-- MFC spend — CONSUMER POSTPAID channel grain only
-- ---------------------------------------------------------------------------
Mfc AS (
  SELECT
    qgp_date,
    channel_group,
    MAX(IF(metric_name = 'mfcSpendActual',   metric_value, NULL)) AS mfcSpendActual,
    MAX(IF(metric_name = 'mfcSpendForecast', metric_value, NULL)) AS mfcSpendForecast
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_mfcSpend_weekly`
  WHERE data_source = 'MFC_SPEND_CHANNEL'
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
  GROUP BY qgp_date, channel_group
)

-- ---------------------------------------------------------------------------
-- Final join — Adobe as spine, MFC + Platform joined in
-- ---------------------------------------------------------------------------
SELECT
  a.qgp_date,
  a.week_type,
  a.quarter,
  a.days_in_period,
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

  -- MFC Spend (CONSUMER POSTPAID)
  m.mfcSpendActual,
  m.mfcSpendForecast,

  -- Platform Spend (POSTPAID)
  p.platformSpend

FROM Adobe a
LEFT JOIN Mfc m
  ON  m.qgp_date     = a.qgp_date
  AND m.channel_group = a.channel_group
LEFT JOIN Platform p
  ON  p.qgp_date     = a.qgp_date
  AND p.channel_group = a.channel_group

ORDER BY a.qgp_date DESC, a.channel_group ASC;