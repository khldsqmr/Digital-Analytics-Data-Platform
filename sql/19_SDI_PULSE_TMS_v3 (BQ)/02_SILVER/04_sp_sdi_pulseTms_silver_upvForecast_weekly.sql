/* =================================================================================================
FILE:         04_sp_sdi_pulseTms_silver_upvForecast_weekly.sql
LAYER:        Stored Procedure
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROCEDURE:    sp_sdi_pulseTms_silver_upvForecast_weekly

PURPOSE:
  Creates/refreshes physical table sdi_pulseTms_silver_upvForecast_weekly.
  Produces channel-allocated UPV forecast rows. Until per-channel forecasts are available,
  All Channels receives the full Bronze value and every other channel_group receives NULL.

  When real per-channel forecasts become available, Steps 2A/2B/2C (Adobe ratio CTEs) and
  the JOIN in Step 3 can be restored from the prior version of this file to re-enable
  ratio-based allocation -- nothing else in the procedure needs to change at that point.

SOURCE:
  Bronze : sdi_pulseTms_bronze_upvForecast_weekly
             - one row per week_sun_sat (Saturday or corrected quarter-end date)
             - all-channels grain only; boundary weeks already prorated by the upload notebook
             - columns: upv_forecast, upv_webapp_forecast

CHANNEL ALLOCATION LOGIC (simplified -- prior-year ratio logic removed temporarily):
  All Channels  : allocation_ratio = 1.0  -> metric_value = Bronze value (passthrough)
  All others    : allocation_ratio = NULL -> metric_value = NULL (until channel forecasts exist)

  Channel list is hardcoded to match Adobe Silver's channel_group values:
    Direct, Organic Search, Paid Search, Paid Social, Programmatic, Other, All Channels

BOUNDARY WEEK HANDLING (unchanged):
  Bronze values are already prorated by the upload notebook -- NO x days_in_period / 7
  proration applied here. BOUNDARY_STUB rows in the calendar spine with no matching Bronze
  row get NULL metric_value.

METRICS (long format, unchanged):
  'upvForecast'       -- maps to upv_forecast / upvTotalAdobe
  'upvWebAppForecast' -- UPV Web + App forecast

GRAIN (unchanged):
  One row per qgp_date x channel_group x metric_name

WoW LOGIC (unchanged):
  NORMAL         : numerator = current; denominator = prior QGP value
                   (if prior was BOUNDARY_FIRST: denominator = BF + its stub)
  BOUNDARY_STUB  : numerator = NULL, denominator = NULL
  BOUNDARY_FIRST : numerator = current + preceding stub
                   denominator = last NORMAL week before the stub

EXECUTION ORDER:
  Must run AFTER:
    notebook upload  (populates Bronze; manual/ad hoc, not dependent on Adobe Silver)

  NOTE: No longer depends on sp_sdi_pulseTms_silver_adobeFunnel_weekly.
        Adobe Silver was only needed for ratio allocation, which is temporarily removed.

DOWNSTREAM (unchanged):
  vw_sdi_pulseTms_gold_unified_long -- CTE 'UpvForecast',
                                      data_source = 'UPV_FORECAST'

CHANGE LOG:
  - Initial version.
  - Channel allocation simplified: prior-year Adobe ratio CTEs
    (Steps 2A/2B/2C) removed.
  - All Channels now receives the full Bronze value
    (allocation_ratio = 1.0).
  - All other channel_group values receive NULL
    (allocation_ratio = NULL).
  - Step 3 changed from a JOIN on ChannelRatios to a CROSS JOIN on a
    hardcoded channel list.
  - Adobe Silver dependency removed.
  - Boundary handling, WoW, YoY, long-format output, and max_date logic unchanged.
================================================================================================= */

CREATE OR REPLACE PROCEDURE
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_pulseTms_silver_upvForecast_weekly`()
OPTIONS (strict_mode = false)
BEGIN

  CREATE OR REPLACE TABLE
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_upvForecast_weekly`
  PARTITION BY qgp_date
  CLUSTER BY channel_group, metric_name
  OPTIONS (
    description = 'PulseTMS Silver — UPV forecast long format with channel allocation and WoW/YoY. '
                  'One row per qgp_date x channel_group x metric_name. '
                  'All Channels receives the full Bronze value (allocation_ratio = 1.0); '
                  'all other channels are NULL (allocation_ratio = NULL) until per-channel '
                  'forecasts are available. '
                  'metric_name: upvForecast | upvWebAppForecast. '
                  'metric_type: UPV_FORECAST. '
                  'Partitioned by qgp_date, clustered by channel_group, metric_name. '
                  'Refreshed ad hoc after each quarterly Bronze upload, via '
                  'sp_sdi_pulseTms_silver_upvForecast_weekly.'
  )
  AS
  WITH

  -- ===========================================================================
  -- STEP 1: Join Bronze forecast -> QGP calendar.
  --         No proration -- Bronze values are already boundary-aware (notebook).
  --         BOUNDARY_STUB calendar rows with no Bronze row -> NULL metric values.
  -- ===========================================================================
  BronzeWithCalendar AS (
    SELECT
      cal.qgp_date,
      cal.week_type,
      cal.quarter                                          AS qgp_quarter,
      EXTRACT(QUARTER FROM cal.qgp_date)                   AS qgp_quarter_num,
      cal.days_in_period,
      cal.is_complete_period,
      cal.is_current_quarter,
      cal.wow_prior_qgp_date,
      cal.boundary_stub_date,
      cal.iso_week_number,
      cal.iso_year,
      b.upv_forecast,
      b.upv_webapp_forecast

    FROM
      `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar` cal

    LEFT JOIN
      `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_bronze_upvForecast_weekly` b
      ON b.week_sun_sat = cal.qgp_date

    WHERE
      cal.qgp_date < DATE_TRUNC(CURRENT_DATE(), QUARTER)
      OR (
        cal.qgp_date >= DATE_TRUNC(CURRENT_DATE(), QUARTER)
        AND cal.qgp_date <= DATE_SUB(
          DATE_ADD(
            DATE_TRUNC(CURRENT_DATE(), QUARTER),
            INTERVAL 3 MONTH
          ),
          INTERVAL 1 DAY
        )
      )
  ),

  -- ===========================================================================
  -- STEP 2: Hardcoded channel list.
  --         All Channels -> ratio = 1.0 (passthrough of Bronze value).
  --         All others   -> ratio = NULL (metric_value will be NULL in Step 3).
  --
  --         Channel list matches Adobe Silver's channel_group values exactly.
  --
  --         When per-channel forecasts become available, replace this CTE with
  --         the prior-year Adobe ratio CTEs (Steps 2A/2B/2C from the prior
  --         version) and update Step 3 to JOIN on iso_year/quarter_num instead
  --         of CROSS JOIN.
  -- ===========================================================================
  ChannelList AS (
    SELECT
      'All Channels' AS channel_group,
      CAST(1.0 AS FLOAT64) AS allocation_ratio

    UNION ALL

    SELECT
      'Direct' AS channel_group,
      CAST(NULL AS FLOAT64) AS allocation_ratio

    UNION ALL

    SELECT
      'Organic Search' AS channel_group,
      CAST(NULL AS FLOAT64) AS allocation_ratio

    UNION ALL

    SELECT
      'Paid Search' AS channel_group,
      CAST(NULL AS FLOAT64) AS allocation_ratio

    UNION ALL

    SELECT
      'Paid Social' AS channel_group,
      CAST(NULL AS FLOAT64) AS allocation_ratio

    UNION ALL

    SELECT
      'Programmatic' AS channel_group,
      CAST(NULL AS FLOAT64) AS allocation_ratio

    UNION ALL

    SELECT
      'Other' AS channel_group,
      CAST(NULL AS FLOAT64) AS allocation_ratio
  ),

  -- ===========================================================================
  -- STEP 3: Apply channel list -> channel-level forecast values.
  --         CROSS JOIN: every calendar week gets one row per channel.
  --         All Channels: metric_value = Bronze value (ratio = 1.0).
  --         All others:   metric_value = NULL (ratio = NULL -> NULL * value).
  -- ===========================================================================
  ForecastWithChannels AS (
    SELECT
      bwc.qgp_date,
      bwc.week_type,
      bwc.qgp_quarter,
      bwc.days_in_period,
      bwc.is_complete_period,
      bwc.is_current_quarter,
      bwc.wow_prior_qgp_date,
      bwc.boundary_stub_date,
      bwc.iso_week_number,
      bwc.iso_year,
      cl.channel_group,
      cl.allocation_ratio,
      bwc.upv_forecast
        * cl.allocation_ratio                             AS upv_forecast_allocated,
      bwc.upv_webapp_forecast
        * cl.allocation_ratio                             AS upv_webapp_forecast_allocated

    FROM BronzeWithCalendar bwc
    CROSS JOIN ChannelList cl
  ),

  -- ===========================================================================
  -- STEP 4: Unpivot to long format.
  --         One row per qgp_date x channel_group x metric_name.
  --         (Unchanged)
  -- ===========================================================================
  Unpivoted AS (
    SELECT
      qgp_date,
      week_type,
      qgp_quarter,
      days_in_period,
      is_complete_period,
      is_current_quarter,
      wow_prior_qgp_date,
      boundary_stub_date,
      iso_week_number,
      iso_year,
      channel_group,
      allocation_ratio,
      'upvForecast'                                       AS metric_name,
      upv_forecast_allocated                              AS metric_value
    FROM ForecastWithChannels

    UNION ALL

    SELECT
      qgp_date,
      week_type,
      qgp_quarter,
      days_in_period,
      is_complete_period,
      is_current_quarter,
      wow_prior_qgp_date,
      boundary_stub_date,
      iso_week_number,
      iso_year,
      channel_group,
      allocation_ratio,
      'upvWebAppForecast'                                 AS metric_name,
      upv_webapp_forecast_allocated                       AS metric_value
    FROM ForecastWithChannels
  ),

  -- ===========================================================================
  -- STEP 5: Metric lookup CTEs for WoW / YoY self-joins.
  --         (Unchanged)
  -- ===========================================================================
  MetricLookup AS (
    SELECT
      qgp_date,
      channel_group,
      metric_name,
      metric_value
    FROM Unpivoted
  ),

  LYWeeklyLookup AS (
    SELECT
      iso_year,
      iso_week_number,
      channel_group,
      metric_name,
      SUM(metric_value)                                   AS ly_weekly_metric_value
    FROM Unpivoted
    WHERE metric_value IS NOT NULL
    GROUP BY
      iso_year,
      iso_week_number,
      channel_group,
      metric_name
  ),

  -- ===========================================================================
  -- STEP 6: WoW / YoY -- same pattern as every other Silver SP.
  --         (Unchanged)
  -- ===========================================================================
  WithWowYoy AS (
    SELECT
      u.qgp_date,
      u.week_type,
      u.qgp_quarter,
      u.days_in_period,
      u.is_complete_period,
      u.channel_group,
      u.metric_name,
      u.metric_value,
      u.allocation_ratio,

      ROUND(
        ly_week.ly_weekly_metric_value
          * SAFE_DIVIDE(u.days_in_period, 7),
        2
      )                                                   AS metric_value_ly,

      -- WoW numerator
      CASE u.week_type
        WHEN 'BOUNDARY_STUB'
          THEN NULL
        WHEN 'BOUNDARY_FIRST'
          THEN u.metric_value + COALESCE(stub_lookup.metric_value, 0)
        ELSE u.metric_value
      END                                                 AS wow_numerator,

      -- WoW denominator
      CASE
        WHEN u.metric_value IS NULL
          THEN NULL
        WHEN u.week_type = 'BOUNDARY_STUB'
          THEN NULL
        WHEN wow_prior_stub.metric_value IS NOT NULL
          THEN COALESCE(wow_prior_lookup.metric_value, 0)
             + COALESCE(wow_prior_stub.metric_value, 0)
        ELSE COALESCE(wow_prior_lookup.metric_value, 0)
      END                                                 AS wow_denominator,

      -- YoY numerator
      CASE u.week_type
        WHEN 'BOUNDARY_STUB'
          THEN NULL
        WHEN 'BOUNDARY_FIRST'
          THEN u.metric_value + COALESCE(stub_lookup.metric_value, 0)
        ELSE u.metric_value
      END                                                 AS yoy_numerator,

      -- YoY denominator
      CASE
        WHEN u.metric_value IS NULL
          THEN NULL
        WHEN u.week_type = 'BOUNDARY_STUB'
          THEN NULL
        ELSE ly_week.ly_weekly_metric_value
      END                                                 AS yoy_denominator

    FROM Unpivoted u

    LEFT JOIN MetricLookup wow_prior_lookup
      ON wow_prior_lookup.qgp_date = u.wow_prior_qgp_date
      AND wow_prior_lookup.channel_group = u.channel_group
      AND wow_prior_lookup.metric_name = u.metric_name

    LEFT JOIN MetricLookup stub_lookup
      ON stub_lookup.qgp_date = u.boundary_stub_date
      AND stub_lookup.channel_group = u.channel_group
      AND stub_lookup.metric_name = u.metric_name

    LEFT JOIN LYWeeklyLookup ly_week
      ON ly_week.iso_year = u.iso_year - 1
      AND ly_week.iso_week_number = u.iso_week_number
      AND ly_week.channel_group = u.channel_group
      AND ly_week.metric_name = u.metric_name

    LEFT JOIN
      `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar` prior_cal
      ON prior_cal.qgp_date = u.wow_prior_qgp_date

    LEFT JOIN MetricLookup wow_prior_stub
      ON wow_prior_stub.qgp_date = prior_cal.boundary_stub_date
      AND wow_prior_stub.channel_group = u.channel_group
      AND wow_prior_stub.metric_name = u.metric_name
  )

  -- ===========================================================================
  -- FINAL SELECT
  -- (Unchanged)
  -- ===========================================================================
  SELECT
    qgp_date,
    week_type,
    qgp_quarter,
    days_in_period,
    is_complete_period,
    channel_group,
    metric_name,
    'UPV_FORECAST'                                        AS metric_type,
    metric_value,
    metric_value_ly,
    allocation_ratio,

    wow_numerator,
    wow_denominator,

    CASE
      WHEN wow_denominator IS NULL OR wow_denominator = 0
        THEN NULL
      ELSE wow_numerator / wow_denominator - 1
    END                                                   AS wow_pct,

    yoy_numerator,
    yoy_denominator,

    CASE
      WHEN yoy_denominator IS NULL OR yoy_denominator = 0
        THEN NULL
      ELSE yoy_numerator / yoy_denominator - 1
    END                                                   AS yoy_pct,

    MAX(
      CASE
        WHEN metric_value IS NOT NULL THEN qgp_date
      END
    ) OVER (
      PARTITION BY channel_group, metric_name
    )                                                     AS max_date

  FROM WithWowYoy;

END;
