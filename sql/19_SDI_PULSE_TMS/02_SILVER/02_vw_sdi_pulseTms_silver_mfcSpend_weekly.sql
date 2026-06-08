/* =================================================================================================
FILE:         02_vw_sdi_pulseTms_silver_mfcSpend_weekly.sql
LAYER:        Silver View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseTms_silver_mfcSpend_weekly

RAW SOURCES (via Bronze):
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_bronze_mfcSpend_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_mfcSpend_weekly

PURPOSE:
  Silver view for MFC spend data in the PulseTMS pipeline.
  All heavy lifting happens here — Bronze is source-close only.

  Produces TWO grains in one view via UNION ALL:

  1. MFC_SPEND_CHANNEL grain (aggregated, CONSUMER POSTPAID only):
     - Filtered to lob = 'CONSUMER POSTPAID'
     - Aggregated to qgp_date × channel_group × metric_name
     - Includes 'All Channels' rollup row (SUM across all channel_groups)
     - lob_mfc = 'CONSUMER POSTPAID', all other MFC dims = NULL

  2. MFC_SPEND_GRANULAR grain (finest detail, all LOBs):
     - All LOBs: CONSUMER POSTPAID, BROADBAND, TFB, CONSUMER PREPAID, T-MOBILE MONEY
     - qgp_date × lob × channel_group × channel × tactic × message_type × agency × metric_name
     - No 'All Channels' rollup (granular is for drill-down only)
     - All MFC dims populated

  Both grains include:
    - WoW / YoY numerators and denominators
    - wow_pct and yoy_pct
    - Completeness enforcement (metric_value = NULL for incomplete periods)
    - max_date per metric_name (MAX qgp_date where metric_value IS NOT NULL)

BUSINESS GRAIN:
  MFC_SPEND_CHANNEL  : qgp_date × channel_group × metric_name
  MFC_SPEND_GRANULAR : qgp_date × lob × channel_group × channel × tactic × message_type × agency × metric_name

METRIC NAMES (camelCase):
  mfcSpendActual    — actual executed spend (NULL for future/unexecuted periods)
  mfcSpendForecast  — forecasted/planned spend

WoW / YoY LOGIC:
  Same boundary logic as Adobe Silver.
  MFC has pre-apportioned BOUNDARY_WEEK spend — do NOT re-weight values.
  All Channels row: SUM(wow_numerator), SUM(wow_denominator) across channel_groups.

DOWNSTREAM:
  08_vw_sdi_pulseTms_gold_unified_long
================================================================================================= */

CREATE OR REPLACE VIEW
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_mfcSpend_weekly`
AS

WITH

-- ---------------------------------------------------------------------------
-- STEP 1: Join Bronze MFC to QGP calendar
--         MFC already has BOUNDARY_WEEK rows split by the source
-- ---------------------------------------------------------------------------
BronzeWithCalendar AS (
  SELECT
    cal.qgp_date,
    cal.week_type,
    cal.quarter,
    cal.days_in_period,
    cal.is_complete_period,
    cal.is_current_quarter,
    cal.wow_prior_qgp_date,
    cal.prior_year_qgp_date,
    cal.boundary_stub_date,
    cal.iso_week_number,
    cal.iso_year,

    b.lob,
    b.channel_group,
    b.channel,
    b.tactic,
    b.message_type,
    b.agency,

    -- Apply completeness rule
    IF(cal.is_complete_period, b.spend_actual,   NULL)                  AS spend_actual,
    IF(cal.is_complete_period, b.spend_forecast, NULL)                  AS spend_forecast,

    b.file_load_date

  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar` cal
  LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_bronze_mfcSpend_weekly` b
    ON b.qgp_week = cal.qgp_date
  WHERE cal.is_current_quarter = TRUE
     OR cal.qgp_date < DATE_TRUNC(CURRENT_DATE(), QUARTER)
),

-- ---------------------------------------------------------------------------
-- STEP 2A: Unpivot GRANULAR grain (all LOBs, finest detail)
--          camelCase metric names
-- ---------------------------------------------------------------------------
UnpivotedGranular AS (

  SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period,
         is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
         boundary_stub_date, iso_week_number, iso_year,
         lob, channel_group, channel, tactic, message_type, agency,
         'mfcSpendActual'             AS metric_name,
         spend_actual                 AS metric_value
  FROM BronzeWithCalendar
  WHERE lob IS NOT NULL

  UNION ALL
  SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period,
         is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
         boundary_stub_date, iso_week_number, iso_year,
         lob, channel_group, channel, tactic, message_type, agency,
         'mfcSpendForecast',          spend_forecast
  FROM BronzeWithCalendar
  WHERE lob IS NOT NULL
),

-- ---------------------------------------------------------------------------
-- STEP 2B: Unpivot CHANNEL grain (CONSUMER POSTPAID only, aggregated)
--          Aggregate to qgp_date × channel_group × metric_name
--          Then UNION with All Channels rollup
-- ---------------------------------------------------------------------------
UnpivotedChannelBase AS (

  SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period,
         is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
         boundary_stub_date, iso_week_number, iso_year,
         channel_group,
         'mfcSpendActual'             AS metric_name,
         SUM(spend_actual)            AS metric_value
  FROM BronzeWithCalendar
  WHERE lob = 'CONSUMER POSTPAID'
  GROUP BY qgp_date, week_type, quarter, days_in_period, is_complete_period,
           is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
           boundary_stub_date, iso_week_number, iso_year, channel_group

  UNION ALL
  SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period,
         is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
         boundary_stub_date, iso_week_number, iso_year,
         channel_group,
         'mfcSpendForecast',          SUM(spend_forecast)
  FROM BronzeWithCalendar
  WHERE lob = 'CONSUMER POSTPAID'
  GROUP BY qgp_date, week_type, quarter, days_in_period, is_complete_period,
           is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
           boundary_stub_date, iso_week_number, iso_year, channel_group
),

-- All Channels rollup for CHANNEL grain (SUM across all channel_groups)
UnpivotedChannelAllChannels AS (

  SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period,
         is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
         boundary_stub_date, iso_week_number, iso_year,
         'All Channels'               AS channel_group,
         metric_name,
         SUM(metric_value)            AS metric_value
  FROM UnpivotedChannelBase
  GROUP BY qgp_date, week_type, quarter, days_in_period, is_complete_period,
           is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
           boundary_stub_date, iso_week_number, iso_year, metric_name
),

-- Combined CHANNEL grain: individual channel_groups + All Channels
UnpivotedChannel AS (
  SELECT * FROM UnpivotedChannelBase
  UNION ALL
  SELECT * FROM UnpivotedChannelAllChannels
),

-- ---------------------------------------------------------------------------
-- STEP 3: Metric lookups for WoW / YoY joins
--         Separate lookups for each grain
-- ---------------------------------------------------------------------------
MetricLookupGranular AS (
  SELECT qgp_date, lob, channel_group, channel, tactic, message_type, agency,
         metric_name, metric_value
  FROM UnpivotedGranular
),

MetricLookupChannel AS (
  SELECT qgp_date, channel_group, metric_name, metric_value
  FROM UnpivotedChannel
),

-- ---------------------------------------------------------------------------
-- STEP 4A: WoW / YoY for CHANNEL grain
-- ---------------------------------------------------------------------------
ChannelWithWowYoy AS (
  SELECT
    u.qgp_date,
    u.week_type,
    u.quarter,
    u.days_in_period,
    u.channel_group,
    u.metric_name,
    u.metric_value,

    ly_lookup.metric_value                                              AS metric_value_ly,

    CASE u.week_type
      WHEN 'BOUNDARY_STUB'  THEN NULL
      WHEN 'BOUNDARY_FIRST' THEN u.metric_value + stub_lookup.metric_value
      ELSE                       u.metric_value
    END                                                                 AS wow_numerator,

    CASE u.week_type
      WHEN 'BOUNDARY_STUB' THEN NULL
      ELSE                      wow_prior_lookup.metric_value
    END                                                                 AS wow_denominator,

    CASE u.week_type
      WHEN 'BOUNDARY_STUB'  THEN NULL
      WHEN 'BOUNDARY_FIRST' THEN u.metric_value + stub_lookup.metric_value
      ELSE                       u.metric_value
    END                                                                 AS yoy_numerator,

    CASE u.week_type
      WHEN 'BOUNDARY_STUB'  THEN NULL
      WHEN 'BOUNDARY_FIRST' THEN yoy_bf_lookup.metric_value + yoy_stub_lookup.metric_value
      ELSE                       ly_lookup.metric_value
    END                                                                 AS yoy_denominator,

    -- lob_mfc fixed as CONSUMER POSTPAID for channel grain
    'CONSUMER POSTPAID'                                                 AS lob_mfc,
    CAST(NULL AS STRING)                                                AS channel,
    CAST(NULL AS STRING)                                                AS tactic,
    CAST(NULL AS STRING)                                                AS message_type,
    CAST(NULL AS STRING)                                                AS agency

  FROM UnpivotedChannel u

  LEFT JOIN MetricLookupChannel wow_prior_lookup
    ON  wow_prior_lookup.qgp_date      = u.wow_prior_qgp_date
    AND wow_prior_lookup.channel_group = u.channel_group
    AND wow_prior_lookup.metric_name   = u.metric_name

  LEFT JOIN MetricLookupChannel ly_lookup
    ON  ly_lookup.qgp_date      = u.prior_year_qgp_date
    AND ly_lookup.channel_group = u.channel_group
    AND ly_lookup.metric_name   = u.metric_name

  LEFT JOIN MetricLookupChannel stub_lookup
    ON  stub_lookup.qgp_date      = u.boundary_stub_date
    AND stub_lookup.channel_group = u.channel_group
    AND stub_lookup.metric_name   = u.metric_name

  LEFT JOIN MetricLookupChannel yoy_bf_lookup
    ON  yoy_bf_lookup.qgp_date      = u.prior_year_qgp_date
    AND yoy_bf_lookup.channel_group = u.channel_group
    AND yoy_bf_lookup.metric_name   = u.metric_name

  LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar` ly_cal
    ON  ly_cal.qgp_date = u.prior_year_qgp_date
  LEFT JOIN MetricLookupChannel yoy_stub_lookup
    ON  yoy_stub_lookup.qgp_date      = ly_cal.boundary_stub_date
    AND yoy_stub_lookup.channel_group = u.channel_group
    AND yoy_stub_lookup.metric_name   = u.metric_name
),

-- ---------------------------------------------------------------------------
-- STEP 4B: WoW / YoY for GRANULAR grain
-- ---------------------------------------------------------------------------
GranularWithWowYoy AS (
  SELECT
    u.qgp_date,
    u.week_type,
    u.quarter,
    u.days_in_period,
    u.channel_group,
    u.metric_name,
    u.metric_value,

    ly_lookup.metric_value                                              AS metric_value_ly,

    CASE u.week_type
      WHEN 'BOUNDARY_STUB'  THEN NULL
      WHEN 'BOUNDARY_FIRST' THEN u.metric_value + stub_lookup.metric_value
      ELSE                       u.metric_value
    END                                                                 AS wow_numerator,

    CASE u.week_type
      WHEN 'BOUNDARY_STUB' THEN NULL
      ELSE                      wow_prior_lookup.metric_value
    END                                                                 AS wow_denominator,

    CASE u.week_type
      WHEN 'BOUNDARY_STUB'  THEN NULL
      WHEN 'BOUNDARY_FIRST' THEN u.metric_value + stub_lookup.metric_value
      ELSE                       u.metric_value
    END                                                                 AS yoy_numerator,

    CASE u.week_type
      WHEN 'BOUNDARY_STUB'  THEN NULL
      WHEN 'BOUNDARY_FIRST' THEN yoy_bf_lookup.metric_value + yoy_stub_lookup.metric_value
      ELSE                       ly_lookup.metric_value
    END                                                                 AS yoy_denominator,

    u.lob                                                               AS lob_mfc,
    u.channel,
    u.tactic,
    u.message_type,
    u.agency

  FROM UnpivotedGranular u

  LEFT JOIN MetricLookupGranular wow_prior_lookup
    ON  wow_prior_lookup.qgp_date      = u.wow_prior_qgp_date
    AND wow_prior_lookup.lob           = u.lob
    AND wow_prior_lookup.channel_group = u.channel_group
    AND wow_prior_lookup.channel       = u.channel
    AND wow_prior_lookup.tactic        = u.tactic
    AND wow_prior_lookup.message_type  = u.message_type
    AND wow_prior_lookup.agency        = u.agency
    AND wow_prior_lookup.metric_name   = u.metric_name

  LEFT JOIN MetricLookupGranular ly_lookup
    ON  ly_lookup.qgp_date      = u.prior_year_qgp_date
    AND ly_lookup.lob           = u.lob
    AND ly_lookup.channel_group = u.channel_group
    AND ly_lookup.channel       = u.channel
    AND ly_lookup.tactic        = u.tactic
    AND ly_lookup.message_type  = u.message_type
    AND ly_lookup.agency        = u.agency
    AND ly_lookup.metric_name   = u.metric_name

  LEFT JOIN MetricLookupGranular stub_lookup
    ON  stub_lookup.qgp_date      = u.boundary_stub_date
    AND stub_lookup.lob           = u.lob
    AND stub_lookup.channel_group = u.channel_group
    AND stub_lookup.channel       = u.channel
    AND stub_lookup.tactic        = u.tactic
    AND stub_lookup.message_type  = u.message_type
    AND stub_lookup.agency        = u.agency
    AND stub_lookup.metric_name   = u.metric_name

  LEFT JOIN MetricLookupGranular yoy_bf_lookup
    ON  yoy_bf_lookup.qgp_date      = u.prior_year_qgp_date
    AND yoy_bf_lookup.lob           = u.lob
    AND yoy_bf_lookup.channel_group = u.channel_group
    AND yoy_bf_lookup.channel       = u.channel
    AND yoy_bf_lookup.tactic        = u.tactic
    AND yoy_bf_lookup.message_type  = u.message_type
    AND yoy_bf_lookup.agency        = u.agency
    AND yoy_bf_lookup.metric_name   = u.metric_name

  LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar` ly_cal
    ON  ly_cal.qgp_date = u.prior_year_qgp_date
  LEFT JOIN MetricLookupGranular yoy_stub_lookup
    ON  yoy_stub_lookup.qgp_date      = ly_cal.boundary_stub_date
    AND yoy_stub_lookup.lob           = u.lob
    AND yoy_stub_lookup.channel_group = u.channel_group
    AND yoy_stub_lookup.channel       = u.channel
    AND yoy_stub_lookup.tactic        = u.tactic
    AND yoy_stub_lookup.message_type  = u.message_type
    AND yoy_stub_lookup.agency        = u.agency
    AND yoy_stub_lookup.metric_name   = u.metric_name
),

-- ---------------------------------------------------------------------------
-- STEP 5: Combine both grains with grain identifier
-- ---------------------------------------------------------------------------
Combined AS (
  SELECT 'MFC_SPEND_CHANNEL'   AS data_source, * FROM ChannelWithWowYoy
  UNION ALL
  SELECT 'MFC_SPEND_GRANULAR'  AS data_source, * FROM GranularWithWowYoy
)

-- ---------------------------------------------------------------------------
-- STEP 6: Final output — add wow_pct, yoy_pct, max_date
--         max_date per data_source × metric_name
-- ---------------------------------------------------------------------------
SELECT
  data_source,
  qgp_date,
  week_type,
  quarter,
  days_in_period,
  channel_group,
  metric_name,
  metric_value,
  metric_value_ly,
  wow_numerator,
  wow_denominator,

  CASE
    WHEN wow_denominator IS NULL OR wow_denominator = 0 THEN NULL
    ELSE wow_numerator / wow_denominator - 1
  END                                                                   AS wow_pct,

  yoy_numerator,
  yoy_denominator,

  CASE
    WHEN yoy_denominator IS NULL OR yoy_denominator = 0 THEN NULL
    ELSE yoy_numerator / yoy_denominator - 1
  END                                                                   AS yoy_pct,

  -- max_date per data_source × metric_name
  MAX(CASE WHEN metric_value IS NOT NULL THEN qgp_date END)
    OVER (PARTITION BY data_source, metric_name)                        AS max_date,

  lob_mfc,
  channel,
  tactic,
  message_type,
  agency

FROM Combined
;