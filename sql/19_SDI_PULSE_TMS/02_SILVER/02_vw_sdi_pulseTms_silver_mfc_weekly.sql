/* =================================================================================================
FILE:         02_vw_sdi_pulseTms_silver_mfc_weekly_long.sql
LAYER:        Silver View — Long
VIEW NAME:    vw_sdi_pulseTms_silver_mfc_weekly_long

PURPOSE:
  Dashboard-ready normalized weekly MFC Silver for:
    - Consumer Postpaid and Broadband summary cards
    - Message-type spend splits
    - Filters for channel group, tactic, and agency
    - Actual, forecast, display, WoW, and YoY reporting

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_gold_spendGranular_weekly

OUTPUT GRAIN:
  One row per:
    week_sun_to_sat
    × lob_supported
    × message_type
    × channel_group
    × source_channel
    × tactic
    × agency
    × metric_name

METRICS:
  mfc_spend_actual
  mfc_spend_forecast
  mfc_spend_display

IMPORTANT REPORTING RULE:
  metric_value, metric_value_wow, and metric_value_ly are additive across the detail grain.
  Dashboard percentages must be calculated after aggregation:
    filtered_wow_pct = SAFE_DIVIDE(SUM(metric_value) - SUM(metric_value_wow), SUM(metric_value_wow))
    filtered_yoy_pct = SAFE_DIVIDE(SUM(metric_value) - SUM(metric_value_ly),  SUM(metric_value_ly))

  Do NOT sum row-level wow_pct or yoy_pct in Tableau.

BOUNDARY-WEEK HANDLING:
  Quarter-boundary rows are collapsed back to the corresponding Sun-to-Sat calendar week by deriving
  week_sun_to_sat = DATE_TRUNC(Period_Start, WEEK(SUNDAY)) and summing at the full output dimension
  grain. No downstream partial-week multiplication should be applied to this normalized Silver.

COMPARISON-SPINE HANDLING:
  The comparison key spine includes current keys and keys shifted forward by 7 and 364 days.
  This preserves prior-only dimensional combinations so aggregate WoW and YoY denominators do not
  silently drop tactics, agencies, or message types that existed in the comparison period but not
  in the current period.
================================================================================================= */

CREATE OR REPLACE VIEW
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_mfc_weekly_long`
AS

WITH base AS (
  SELECT
    DATE_TRUNC(Period_Start, WEEK(SUNDAY)) AS week_sun_to_sat,

    CASE
      WHEN Channel = 'Paid Search'
        THEN 'PAID SEARCH'
      WHEN Channel = 'Paid Social'
        THEN 'SOCIAL'
      WHEN Channel IN ('Display', 'OLV', 'OTT')
        THEN 'PROGRAMMATIC'
      WHEN Channel = 'OOH'
       AND Tactic IN ('OOH - Programmatic', 'OOH - Digital')
        THEN 'PROGRAMMATIC'
      WHEN Channel IN ('Affiliate', 'Audio', 'DJs', 'Print', 'Radio', 'Spot TV', 'TV')
        THEN 'OTHER'
      WHEN Channel = 'OOH'
       AND Tactic = 'OOH - Static'
        THEN 'OTHER'
      ELSE 'OTHER'
    END AS channel_group,

    CAST(Channel AS STRING) AS source_channel,
    CAST(Tactic AS STRING) AS tactic,
    CAST(Message_Type AS STRING) AS message_type,
    CAST(Agency AS STRING) AS agency,
    CAST(LOB_Supported AS STRING) AS lob_supported,

    CAST(spend_actual AS FLOAT64) AS spend_actual,
    CAST(spend_forecast AS FLOAT64) AS spend_forecast,
    CAST(spend_display AS FLOAT64) AS spend_display

  FROM
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_gold_spendGranular_weekly`

  WHERE LOB_Supported IN ('CONSUMER POSTPAID', 'BROADBAND')
),

-- Collapse quarter-boundary halves and any repeated source rows while retaining
-- all dashboard dimensions required for filtering and message-type analysis.
aggregated AS (
  SELECT
    week_sun_to_sat,
    lob_supported,
    message_type,
    channel_group,
    source_channel,
    tactic,
    agency,

    SUM(spend_actual) AS spend_actual,
    SUM(spend_forecast) AS spend_forecast,
    SUM(spend_display) AS spend_display

  FROM base

  GROUP BY
    week_sun_to_sat,
    lob_supported,
    message_type,
    channel_group,
    source_channel,
    tactic,
    agency
),

date_bounds AS (
  SELECT
    MIN(week_sun_to_sat) AS min_week,
    MAX(week_sun_to_sat) AS max_week
  FROM aggregated
),

-- Add shifted comparison keys so prior-only rows remain available when a tactic,
-- agency, or message type is no longer present in the current period.
comparison_keys AS (
  SELECT
    week_sun_to_sat,
    lob_supported,
    message_type,
    channel_group,
    source_channel,
    tactic,
    agency
  FROM aggregated

  UNION DISTINCT

  SELECT
    DATE_ADD(a.week_sun_to_sat, INTERVAL 7 DAY) AS week_sun_to_sat,
    a.lob_supported,
    a.message_type,
    a.channel_group,
    a.source_channel,
    a.tactic,
    a.agency
  FROM aggregated a
  CROSS JOIN date_bounds b
  WHERE DATE_ADD(a.week_sun_to_sat, INTERVAL 7 DAY) <= b.max_week

  UNION DISTINCT

  SELECT
    DATE_ADD(a.week_sun_to_sat, INTERVAL 364 DAY) AS week_sun_to_sat,
    a.lob_supported,
    a.message_type,
    a.channel_group,
    a.source_channel,
    a.tactic,
    a.agency
  FROM aggregated a
  CROSS JOIN date_bounds b
  WHERE DATE_ADD(a.week_sun_to_sat, INTERVAL 364 DAY) <= b.max_week
),

with_comparisons AS (
  SELECT
    k.week_sun_to_sat,
    k.lob_supported,
    k.message_type,
    k.channel_group,
    k.source_channel,
    k.tactic,
    k.agency,

    c.spend_actual,
    c.spend_forecast,
    c.spend_display,

    w.spend_actual AS spend_actual_wow,
    w.spend_forecast AS spend_forecast_wow,
    w.spend_display AS spend_display_wow,

    l.spend_actual AS spend_actual_ly,
    l.spend_forecast AS spend_forecast_ly,
    l.spend_display AS spend_display_ly,

    ROUND(SAFE_DIVIDE(c.spend_actual - c.spend_forecast, c.spend_forecast), 6)
      AS act_vs_fcst_pct,

    c.spend_actual - c.spend_forecast
      AS act_vs_fcst_delta

  FROM comparison_keys k

  LEFT JOIN aggregated c
    ON c.week_sun_to_sat = k.week_sun_to_sat
   AND c.lob_supported = k.lob_supported
   AND c.message_type IS NOT DISTINCT FROM k.message_type
   AND c.channel_group = k.channel_group
   AND c.source_channel IS NOT DISTINCT FROM k.source_channel
   AND c.tactic IS NOT DISTINCT FROM k.tactic
   AND c.agency IS NOT DISTINCT FROM k.agency

  LEFT JOIN aggregated w
    ON w.week_sun_to_sat = DATE_SUB(k.week_sun_to_sat, INTERVAL 7 DAY)
   AND w.lob_supported = k.lob_supported
   AND w.message_type IS NOT DISTINCT FROM k.message_type
   AND w.channel_group = k.channel_group
   AND w.source_channel IS NOT DISTINCT FROM k.source_channel
   AND w.tactic IS NOT DISTINCT FROM k.tactic
   AND w.agency IS NOT DISTINCT FROM k.agency

  LEFT JOIN aggregated l
    ON l.week_sun_to_sat = DATE_SUB(k.week_sun_to_sat, INTERVAL 364 DAY)
   AND l.lob_supported = k.lob_supported
   AND l.message_type IS NOT DISTINCT FROM k.message_type
   AND l.channel_group = k.channel_group
   AND l.source_channel IS NOT DISTINCT FROM k.source_channel
   AND l.tactic IS NOT DISTINCT FROM k.tactic
   AND l.agency IS NOT DISTINCT FROM k.agency
),

metric_rows AS (
  SELECT
    c.week_sun_to_sat,
    'WEEKLY' AS time_granularity,
    'MFC' AS data_source,

    c.lob_supported,
    c.message_type,
    c.channel_group,
    c.source_channel,
    c.tactic,
    c.agency,

    m.metric_name,
    m.metric_value,
    m.metric_value_wow,
    m.metric_value_ly,

    ROUND(SAFE_DIVIDE(m.metric_value - m.metric_value_wow, m.metric_value_wow), 6)
      AS wow_pct,

    ROUND(SAFE_DIVIDE(m.metric_value - m.metric_value_ly, m.metric_value_ly), 6)
      AS yoy_pct,

    c.act_vs_fcst_pct,
    c.act_vs_fcst_delta

  FROM with_comparisons c

  CROSS JOIN UNNEST([
    STRUCT(
      'mfc_spend_actual' AS metric_name,
      c.spend_actual AS metric_value,
      c.spend_actual_wow AS metric_value_wow,
      c.spend_actual_ly AS metric_value_ly
    ),
    STRUCT(
      'mfc_spend_forecast',
      c.spend_forecast,
      c.spend_forecast_wow,
      c.spend_forecast_ly
    ),
    STRUCT(
      'mfc_spend_display',
      c.spend_display,
      c.spend_display_wow,
      c.spend_display_ly
    )
  ]) m
),

max_date_by_lob AS (
  SELECT
    lob_supported,
    MAX(
      IF(
        metric_name IN ('mfc_spend_actual', 'mfc_spend_display')
        AND metric_value IS NOT NULL,
        week_sun_to_sat,
        NULL
      )
    ) AS max_data_date
  FROM metric_rows
  GROUP BY lob_supported
)

SELECT
  m.week_sun_to_sat,
  m.time_granularity,
  m.data_source,

  -- Existing Gold-compatible channel field.
  m.channel_group AS channel,

  -- Explicit MFC dashboard dimensions.
  m.channel_group,
  m.source_channel,
  m.tactic,
  m.message_type,
  m.agency,
  m.lob_supported,

  m.metric_name,
  m.metric_value,
  m.metric_value_wow,
  m.metric_value_ly,
  m.wow_pct,
  m.yoy_pct,
  m.act_vs_fcst_pct,
  m.act_vs_fcst_delta,

  -- Boundary halves are already collapsed into complete weekly values.
  FALSE AS is_mfc_pre_apportioned,

  d.max_data_date

FROM metric_rows m
LEFT JOIN max_date_by_lob d
  USING (lob_supported)
;
