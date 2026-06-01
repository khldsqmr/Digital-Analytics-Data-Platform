/* =================================================================================================
FILE:         03_vw_sdi_pulseTms_gold_unified_long.sql
LAYER:        Gold View — Long
VIEW NAME:    vw_sdi_pulseTms_gold_unified_long

SOURCES:
  vw_sdi_pulseTms_silver_adobe_weekly_long
  vw_sdi_pulseTms_silver_mfc_weekly_long

PURPOSE:
  Unified dashboard-ready Gold Long view containing Adobe and MFC only.

MFC SUPPORT:
  MFC rows retain:
    lob_supported   — CONSUMER POSTPAID or BROADBAND
    message_type
    channel_group
    source_channel
    tactic
    agency

  This supports:
    - Postpaid and Broadband spend summary cards
    - Actual, forecast, WoW, and YoY reporting
    - Message-type breakdowns
    - Filters for channel group, tactic, and agency

TABLEAU REPORTING RULE:
  For filtered MFC WoW and YoY values, calculate percentages from summed numerators and denominators.
  Do not sum the row-level wow_pct or yoy_pct fields.
================================================================================================= */

CREATE OR REPLACE VIEW
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_gold_unified_long`
AS

WITH adobe AS (
  SELECT
    week_sun_to_sat,
    time_granularity,
    data_source,
    channel,

    -- Explicit MFC-only dimensions remain NULL for Adobe.
    CAST(NULL AS STRING) AS channel_group,
    CAST(NULL AS STRING) AS source_channel,
    CAST(NULL AS STRING) AS tactic,
    CAST(NULL AS STRING) AS message_type,
    CAST(NULL AS STRING) AS agency,
    CAST(NULL AS STRING) AS lob_supported,

    -- Retained for compatibility with the earlier generic-dimension output.
    CAST(NULL AS STRING) AS dimension_name,
    CAST(NULL AS STRING) AS dimension_value,
    CAST(NULL AS STRING) AS dimension_name_2,
    CAST(NULL AS STRING) AS dimension_value_2,

    metric_name,
    metric_value,
    metric_value_wow,
    metric_value_ly,
    wow_pct,
    yoy_pct,

    CAST(NULL AS FLOAT64) AS act_vs_fcst_pct,
    CAST(NULL AS FLOAT64) AS act_vs_fcst_delta,
    CAST(NULL AS BOOL) AS is_mfc_pre_apportioned,

    max_data_date

  FROM
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_adobe_weekly_long`
),

mfc AS (
  SELECT
    week_sun_to_sat,
    time_granularity,
    data_source,
    channel,

    channel_group,
    source_channel,
    tactic,
    message_type,
    agency,
    lob_supported,

    -- Generic fields retained for compatibility with existing downstream usage.
    'TACTIC' AS dimension_name,
    tactic AS dimension_value,
    'AGENCY' AS dimension_name_2,
    agency AS dimension_value_2,

    metric_name,
    metric_value,
    metric_value_wow,
    metric_value_ly,
    wow_pct,
    yoy_pct,

    act_vs_fcst_pct,
    act_vs_fcst_delta,
    is_mfc_pre_apportioned,

    max_data_date

  FROM
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_mfc_weekly_long`
)

SELECT * FROM adobe
UNION ALL
SELECT * FROM mfc
;
