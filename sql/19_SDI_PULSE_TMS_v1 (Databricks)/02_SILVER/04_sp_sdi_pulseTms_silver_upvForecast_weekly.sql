/* =================================================================================================
FILE:         sdi_sp_dashboardPulseTms_silver_upvForecast_weekly.sql
LAYER:        Stored Procedure
PROCEDURE:    sdi_sp_dashboardPulseTms_silver_upvForecast_weekly

PURPOSE:
  Creates/refreshes physical table sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly.
  Produces channel-allocated UPV forecast rows by applying prior-year same-quarter
  channel mix ratios (from Adobe Silver actuals) to the Bronze all-channels forecast.

  This is a direct port of the BQ version -- see CHANGE LOG below for every translation
  decision, including the two flagged for verification against a real Databricks environment.

SOURCE:
  Bronze : sdi_tbl_dashboardPulseTms_bronze_upvForecast_weekly
             - one row per week_sun_sat (Saturday or corrected quarter-end date)
             - all-channels grain only; boundary weeks already prorated by the upload notebook
               (see that notebook's date-validation step -- boundary stub weeks are entered
               using the exact quarter-end date directly, not a natural-week Saturday, so
               unlike platformSpend/adobeFunnel there's no natural-week rollup ambiguity here
               and no bf-style second join is needed)
             - columns: upv_forecast, upv_webapp_forecast

  Adobe Silver : sdi_tbl_dashboardPulseTms_silver_adobeFunnel_weekly
             - allocation ratio source
             - metric_name = 'upvTotalAdobe', metric_type = 'ADOBE_VOLUME'
             - base period: prior year same quarter, complete non-stub weeks only

CHANNEL ALLOCATION LOGIC (unchanged from BQ):
  ratio per channel = SUM(upvTotalAdobe for channel X in prior year same quarter)
                    / SUM(upvTotalAdobe for All Channels in prior year same quarter)

  'All Channels' row always included with ratio = 1.0 (passthrough).
  If no prior year same quarter actuals exist, metric_value will be NULL.
  Ratios are computed across ALL of Adobe's channel groups (Paid Search, Paid Social,
  Organic Search, Direct, Programmatic, Other), not just paid ones -- this forecasts total
  UPV volume, which Adobe tracks regardless of paid/organic/direct origin.

BOUNDARY WEEK HANDLING:
  Bronze values are already prorated by the upload notebook -- NO x days_in_period / 7
  proration applied here, unlike adobeFunnel/platformSpend Silver. BOUNDARY_STUB rows in the
  calendar spine with no matching Bronze row get NULL metric_value.

METRICS (long format):
  'upvForecast'       -- maps to upv_forecast / upvTotalAdobe
  'upvWebAppForecast' -- UPV Web + App forecast

GRAIN:
  One row per qgp_date x channel_group x metric_name

WoW LOGIC (same pattern as every other Silver SP in this pipeline):
  NORMAL         : numerator = current; denominator = prior QGP value
                   (if prior was BOUNDARY_FIRST: denominator = BF + its stub)
  BOUNDARY_STUB  : numerator = NULL, denominator = NULL
  BOUNDARY_FIRST : numerator = current + preceding stub
                   denominator = last NORMAL week before the stub

EXECUTION ORDER:
  Must run AFTER:
    sdi_sp_dashboardPulseTms_silver_adobeFunnel_weekly  (allocation ratio source)
    upvForecast_bronze_upload.py notebook                (populates Bronze; manual/ad hoc,
                                                            not on the weekly schedule)

DOWNSTREAM:
  sdi_vw_dashboardPulseTms_gold_unified_long -- CTE 'UpvForecast', data_source = 'UPV_FORECAST'

PORTING NOTES (BQ -> Databricks), applies to this file only:
  - OPTIONS(strict_mode=false) ... BEGIN...END -> LANGUAGE SQL AS BEGIN...END
  - CREATE TABLE ... PARTITION BY qgp_date CLUSTER BY channel_group, metric_name
    OPTIONS(description=...) -> CREATE TABLE ... USING DELTA
    CLUSTER BY (qgp_date, channel_group, metric_name) COMMENT '...'
  - SAFE_DIVIDE(a, b) -> try_divide(a, b)
  - DATE_TRUNC(x, QUARTER) -> trunc(x, 'QUARTER')
  - DATE_SUB(DATE_ADD(x, INTERVAL 3 MONTH), INTERVAL 1 DAY) -> date_sub(add_months(x, 3), 1)
  - ⚠ EXTRACT(ISOYEAR FROM x) -> EXTRACT(YEAROFWEEK FROM x). Spark SQL's YEAROFWEEK extract
    field is the ISO-8601 week-year, the direct equivalent of BQ's ISOYEAR -- confident in
    this translation but, consistent with how every other extract-field substitution has been
    flagged throughout this port, worth a quick confirmation against a real Databricks run
    before trusting it blindly.
  - ⚠ cal.qgp_quarter_num -> EXTRACT(QUARTER FROM cal.qgp_date), computed directly rather than
    assumed to exist as a named column on the calendar view. I don't have confirmed evidence
    sdi_vw_dashboardPulseTms_dim_qgp_calendar exposes a qgp_quarter_num column the way the BQ
    calendar apparently did -- computing it from qgp_date is deterministic and gives the
    identical result either way, so this substitution is safe regardless of whether that
    column turns out to actually exist.
  - `` `project.dataset.table` `` backtick-qualified BQ table refs -> unquoted
    catalog.schema.table Databricks refs throughout.

CHANGE LOG:
  - Ported from BQ (04_sp_sdi_pulseTms_silver_upvForecast_weekly.sql). Business logic
    (channel allocation via prior-year ratios, boundary handling, WoW/YoY) preserved exactly --
    see PORTING NOTES above for the handful of syntax/schema substitutions.
================================================================================================= */

CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_upvForecast_weekly()
LANGUAGE SQL
AS
BEGIN

  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly
  USING DELTA
  CLUSTER BY (qgp_date, channel_group, metric_name)
  COMMENT 'PulseTMS Silver — UPV forecast long format with channel allocation and WoW/YoY. One row per qgp_date x channel_group x metric_name. Channel split derived from prior year same quarter Adobe actuals ratios. Includes allocation_ratio column for Tableau inspection. metric_name: upvForecast | upvWebAppForecast. metric_type: UPV_FORECAST. Clustered by qgp_date, channel_group, metric_name. Refreshed ad hoc after each quarterly Bronze upload, via sdi_sp_dashboardPulseTms_silver_upvForecast_weekly.'
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
      cal.quarter                                                         AS qgp_quarter,
      EXTRACT(QUARTER FROM cal.qgp_date)                                  AS qgp_quarter_num,
      cal.days_in_period,
      cal.is_complete_period,
      cal.is_current_quarter,
      cal.wow_prior_qgp_date,
      cal.boundary_stub_date,
      cal.iso_week_number,
      cal.iso_year,
      b.upv_forecast,
      b.upv_webapp_forecast

    FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar cal
    LEFT JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_upvForecast_weekly b
      ON b.week_sun_sat = cal.qgp_date

    WHERE
      cal.qgp_date < trunc(current_date(), 'QUARTER')
      OR (
        cal.qgp_date >= trunc(current_date(), 'QUARTER')
        AND cal.qgp_date <= date_sub(add_months(trunc(current_date(), 'QUARTER'), 3), 1)
      )
  ),

  -- ===========================================================================
  -- STEP 2A: Prior year same quarter — All Channels total (denominator)
  -- ===========================================================================
  AdobePriorYearAllChannels AS (
    SELECT
      EXTRACT(YEAROFWEEK FROM qgp_date) AS iso_year,
      EXTRACT(QUARTER    FROM qgp_date) AS quarter_num,
      SUM(metric_value)                 AS total_all_channels
    FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_adobeFunnel_weekly
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
      EXTRACT(YEAROFWEEK FROM qgp_date) AS iso_year,
      EXTRACT(QUARTER    FROM qgp_date) AS quarter_num,
      channel_group,
      SUM(metric_value)                 AS total_channel
    FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_adobeFunnel_weekly
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
      try_divide(ch.total_channel, ac.total_all_channels) AS allocation_ratio
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
  -- STEP 3: Apply ratios -> channel-level forecast values
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
  --         One row per qgp_date x channel_group x metric_name
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
  -- STEP 6: WoW / YoY — same pattern as every other Silver SP
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
        ly_week.ly_weekly_metric_value * try_divide(u.days_in_period, 7),
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

    LEFT JOIN prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar prior_cal
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