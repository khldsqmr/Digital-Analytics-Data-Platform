/* =================================================================================================
FILE:         04_sp_sdi_pulseTms_silver_upvForecast_weekly.sql
LAYER:        Stored Procedure
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROCEDURE:    sp_sdi_pulseTms_silver_upvForecast_weekly

PURPOSE:
  Creates/refreshes physical table sdi_pulseTms_silver_upvForecast_weekly.
  Called by 00_call_all_sp_pulseTms.sql as part of the weekly refresh.

  Produces channel-allocated UPV forecast rows by applying prior-year same-quarter
  channel mix ratios (from Adobe Silver actuals) to the Bronze all-channels forecast.

SOURCE:
  Bronze : sdi_pulseTms_bronze_upvForecast_weekly
             - one row per week_sun_sat (Saturday or corrected quarter-end date)
             - all-channels grain only; boundary weeks already prorated by notebook
             - columns: upv_forecast, upv_webapp_forecast

  Adobe Silver : sdi_pulseTms_silver_adobeFunnel_weekly
             - allocation ratio source
             - metric_name = 'upvTotalAdobe', metric_type = 'ADOBE_VOLUME'
             - base period: prior year same quarter, complete non-stub weeks only

CHANNEL ALLOCATION LOGIC:
  ratio per channel = SUM(upvTotalAdobe for channel X in prior year same quarter)
                    / SUM(upvTotalAdobe for All Channels in prior year same quarter)

  'All Channels' row always included with ratio = 1.0 (passthrough).
  If no prior year same quarter actuals exist, metric_value will be NULL.

BOUNDARY WEEK HANDLING:
  Bronze values are already prorated by the notebook — NO × days_in_period / 7 applied here.
  BOUNDARY_STUB rows in the calendar spine with no Bronze row get NULL metric_value.

METRICS (long format):
  'upvForecast'       — maps to upvTotalAdobe
  'upvWebAppForecast' — UPV Web + App forecast

GRAIN:
  One row per qgp_date × channel_group × metric_name

WoW LOGIC (same pattern as all other Silver SPs):
  NORMAL         : numerator = current; denominator = prior QGP value
                   (if prior was BOUNDARY_FIRST: denominator = BF + its stub)
  BOUNDARY_STUB  : numerator = NULL, denominator = NULL
  BOUNDARY_FIRST : numerator = current + preceding stub
                   denominator = last NORMAL week before the stub

EXECUTION ORDER:
  Must run AFTER:
    sp_sdi_pulseTms_silver_adobeFunnel_weekly  (allocation ratio source)
    notebook upload                             (bronze_upvForecast_weekly)

DOWNSTREAM:
  vw_sdi_pulseTms_gold_unified_long — CTE 'UpvForecast', data_source = 'UPV_FORECAST'

CHANGE LOG:
  - Initial version.
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
    description = 'PulseTMS Silver — UPV forecast long format with channel allocation and WoW. '
                  'One row per qgp_date x channel_group x metric_name. '
                  'Channel split derived from prior year same quarter Adobe actuals ratios. '
                  'Includes allocation_ratio column for Tableau inspection. '
                  'metric_name: upvForecast | upvWebAppForecast. metric_type: UPV_FORECAST. '
                  'Partitioned by qgp_date, clustered by channel_group, metric_name. '
                  'Refreshed weekly via sp_sdi_pulseTms_silver_upvForecast_weekly.'
  )
  AS
  WITH

  -- ===========================================================================
  -- STEP 1: Join Bronze forecast → QGP calendar
  --         No proration — Bronze values are already boundary-aware (notebook).
  --         BOUNDARY_STUB calendar rows with no Bronze row → NULL metric values.
  -- ===========================================================================
  BronzeWithCalendar AS (
    SELECT
      cal.qgp_date,
      cal.week_type,
      cal.quarter                                                         AS qgp_quarter,
      cal.qgp_quarter_num,
      cal.days_in_period,
      cal.is_complete_period,
      cal.is_current_quarter,
      cal.wow_prior_qgp_date,
      cal.boundary_stub_date,
      cal.iso_week_number,
      cal.iso_year,
      b.upv_forecast,
      b.upv_webapp_forecast

    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar` cal
    LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_bronze_upvForecast_weekly` b
      ON b.week_sun_sat = cal.qgp_date

    WHERE
      cal.qgp_date < DATE_TRUNC(CURRENT_DATE(), QUARTER)
      OR (
        cal.qgp_date >= DATE_TRUNC(CURRENT_DATE(), QUARTER)
        AND cal.qgp_date <= DATE_SUB(
              DATE_ADD(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 3 MONTH),
              INTERVAL 1 DAY
            )
      )
  ),

  -- ===========================================================================
  -- STEP 2A: Prior year same quarter — All Channels total (denominator)
  -- ===========================================================================
  AdobePriorYearAllChannels AS (
    SELECT
      EXTRACT(ISOYEAR  FROM qgp_date) AS iso_year,
      EXTRACT(QUARTER  FROM qgp_date) AS quarter_num,
      SUM(metric_value)               AS total_all_channels
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_adobeFunnel_weekly`
    WHERE
      metric_name        = 'upvTotalAdobe'
      AND metric_type    = 'ADOBE_VOLUME'
      AND channel_group  = 'All Channels'
      AND is_complete_period = TRUE
      AND week_type     != 'BOUNDARY_STUB'
    GROUP BY 1, 2
  ),

  -- ===========================================================================
  -- STEP 2B: Prior year same quarter — per channel total (numerator)
  -- ===========================================================================
  AdobePriorYearByChannel AS (
    SELECT
      EXTRACT(ISOYEAR  FROM qgp_date) AS iso_year,
      EXTRACT(QUARTER  FROM qgp_date) AS quarter_num,
      channel_group,
      SUM(metric_value)               AS total_channel
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_adobeFunnel_weekly`
    WHERE
      metric_name        = 'upvTotalAdobe'
      AND metric_type    = 'ADOBE_VOLUME'
      AND channel_group != 'All Channels'
      AND is_complete_period = TRUE
      AND week_type     != 'BOUNDARY_STUB'
    GROUP BY 1, 2, 3
  ),

  -- ===========================================================================
  -- STEP 2C: Channel allocation ratios
  --          ratio = channel total / All Channels total for that iso_year x quarter
  --          'All Channels' always ratio = 1.0
  -- ===========================================================================
  ChannelRatios AS (
    SELECT
      ch.iso_year,
      ch.quarter_num,
      ch.channel_group,
      SAFE_DIVIDE(ch.total_channel, ac.total_all_channels) AS allocation_ratio
    FROM AdobePriorYearByChannel ch
    LEFT JOIN AdobePriorYearAllChannels ac
      ON  ac.iso_year    = ch.iso_year
      AND ac.quarter_num = ch.quarter_num

    UNION ALL

    SELECT
      iso_year,
      quarter_num,
      'All Channels' AS channel_group,
      1.0            AS allocation_ratio
    FROM AdobePriorYearAllChannels
  ),

  -- ===========================================================================
  -- STEP 3: Apply ratios → channel-level forecast values
  --         Join key: prior year iso_year = current iso_year - 1
  --                   prior year quarter  = current qgp_quarter_num
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
      cr.channel_group,
      cr.allocation_ratio,
      bwc.upv_forecast        * cr.allocation_ratio AS upv_forecast_allocated,
      bwc.upv_webapp_forecast * cr.allocation_ratio AS upv_webapp_forecast_allocated
    FROM BronzeWithCalendar bwc
    JOIN ChannelRatios cr
      ON  cr.iso_year    = bwc.iso_year - 1
      AND cr.quarter_num = bwc.qgp_quarter_num
  ),

  -- ===========================================================================
  -- STEP 4: Unpivot to long format
  --         One row per qgp_date × channel_group × metric_name
  -- ===========================================================================
  Unpivoted AS (
    SELECT
      qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period,
      is_current_quarter, wow_prior_qgp_date, boundary_stub_date,
      iso_week_number, iso_year, channel_group, allocation_ratio,
      'upvForecast'          AS metric_name,
      upv_forecast_allocated AS metric_value
    FROM ForecastWithChannels

    UNION ALL

    SELECT
      qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period,
      is_current_quarter, wow_prior_qgp_date, boundary_stub_date,
      iso_week_number, iso_year, channel_group, allocation_ratio,
      'upvWebAppForecast'            AS metric_name,
      upv_webapp_forecast_allocated  AS metric_value
    FROM ForecastWithChannels
  ),

  -- ===========================================================================
  -- STEP 5: Metric lookup CTEs for WoW / YoY self-joins
  -- ===========================================================================
  MetricLookup AS (
    SELECT qgp_date, channel_group, metric_name, metric_value
    FROM Unpivoted
  ),

  LYWeeklyLookup AS (
    SELECT
      iso_year,
      iso_week_number,
      channel_group,
      metric_name,
      SUM(metric_value) AS ly_weekly_metric_value
    FROM Unpivoted
    WHERE metric_value IS NOT NULL
    GROUP BY iso_year, iso_week_number, channel_group, metric_name
  ),

  -- ===========================================================================
  -- STEP 6: WoW / YoY — identical pattern to silver_platformSpend_weekly
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
        ly_week.ly_weekly_metric_value * SAFE_DIVIDE(u.days_in_period, 7),
        2
      )                                                                   AS metric_value_ly,

      -- WoW numerator
      CASE u.week_type
        WHEN 'BOUNDARY_STUB'  THEN NULL
        WHEN 'BOUNDARY_FIRST' THEN u.metric_value + COALESCE(stub_lookup.metric_value, 0)
        ELSE                       u.metric_value
      END                                                                 AS wow_numerator,

      -- WoW denominator
      CASE
        WHEN u.metric_value IS NULL        THEN NULL
        WHEN u.week_type = 'BOUNDARY_STUB' THEN NULL
        WHEN wow_prior_stub.metric_value IS NOT NULL
          THEN COALESCE(wow_prior_lookup.metric_value, 0)
               + COALESCE(wow_prior_stub.metric_value, 0)
        ELSE COALESCE(wow_prior_lookup.metric_value, 0)
      END                                                                 AS wow_denominator,

      -- YoY numerator
      CASE u.week_type
        WHEN 'BOUNDARY_STUB'  THEN NULL
        WHEN 'BOUNDARY_FIRST' THEN u.metric_value + COALESCE(stub_lookup.metric_value, 0)
        ELSE                       u.metric_value
      END                                                                 AS yoy_numerator,

      -- YoY denominator
      CASE
        WHEN u.metric_value IS NULL        THEN NULL
        WHEN u.week_type = 'BOUNDARY_STUB' THEN NULL
        ELSE ly_week.ly_weekly_metric_value
      END                                                                 AS yoy_denominator

    FROM Unpivoted u

    LEFT JOIN MetricLookup wow_prior_lookup
      ON  wow_prior_lookup.qgp_date      = u.wow_prior_qgp_date
      AND wow_prior_lookup.channel_group = u.channel_group
      AND wow_prior_lookup.metric_name   = u.metric_name

    LEFT JOIN MetricLookup stub_lookup
      ON  stub_lookup.qgp_date      = u.boundary_stub_date
      AND stub_lookup.channel_group = u.channel_group
      AND stub_lookup.metric_name   = u.metric_name

    LEFT JOIN LYWeeklyLookup ly_week
      ON  ly_week.iso_year        = u.iso_year - 1
      AND ly_week.iso_week_number = u.iso_week_number
      AND ly_week.channel_group   = u.channel_group
      AND ly_week.metric_name     = u.metric_name

    LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar` prior_cal
      ON  prior_cal.qgp_date = u.wow_prior_qgp_date

    LEFT JOIN MetricLookup wow_prior_stub
      ON  wow_prior_stub.qgp_date      = prior_cal.boundary_stub_date
      AND wow_prior_stub.channel_group = u.channel_group
      AND wow_prior_stub.metric_name   = u.metric_name
  )

  -- ===========================================================================
  -- FINAL SELECT
  -- ===========================================================================
  SELECT
    qgp_date,
    week_type,
    qgp_quarter,
    days_in_period,
    is_complete_period,
    channel_group,
    metric_name,
    'UPV_FORECAST'                                                        AS metric_type,
    metric_value,
    metric_value_ly,
    allocation_ratio,

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

    MAX(CASE WHEN metric_value IS NOT NULL THEN qgp_date END)
      OVER (PARTITION BY channel_group, metric_name)                      AS max_date

  FROM WithWowYoy;

END;