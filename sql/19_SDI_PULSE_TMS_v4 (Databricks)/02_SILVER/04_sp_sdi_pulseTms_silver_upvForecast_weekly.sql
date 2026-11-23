/* =================================================================================================
FILE:         03_sdi_sp_dashboardPulseTms_silver_upvForecast_weekly.sql
LAYER:        Stored Procedure
PROCEDURE:    sdi_sp_dashboardPulseTms_silver_upvForecast_weekly

PURPOSE:
  Creates/refreshes physical table sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly.
  Produces channel-allocated UPV forecast rows. Until per-channel forecasts are available,
  All Channels receives the full Bronze value and every other channel_group receives NULL.

  When real per-channel forecasts become available, Steps 2A/2B/2C (Adobe ratio CTEs) and
  the CROSS JOIN in Step 3 can be restored from the prior version of this file to re-enable
  ratio-based allocation -- nothing else in the procedure needs to change at that point.

SOURCE:
  Bronze : sdi_tbl_dashboardPulseTms_bronze_upvForecast_weekly
             - one row per week_sun_sat (Saturday or corrected quarter-end date)
             - all-channels grain only; boundary weeks already prorated by the upload notebook
             - columns: upv_forecast, upv_webapp_forecast

CHANNEL ALLOCATION LOGIC (simplified -- prior-year ratio logic removed temporarily):
  All Channels  : allocation_ratio = 1.0  ->  metric_value = Bronze value (passthrough)
  All others    : allocation_ratio = NULL ->  metric_value = NULL (until channel forecasts exist)

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

WoW / YoY LOGIC (full Sunday-Saturday week):
  Every WoW / YoY value is built from a FULL Sunday-Saturday week value (FullWeek CTE):

  NORMAL          full week = metric_value
  BOUNDARY_FIRST  full week = BOUNDARY_STUB piece (old quarter) + BOUNDARY_FIRST piece (new quarter)
                  If ONE piece is missing, the missing days are estimated from the other piece's
                  daily rate (forecast rule: show the forecast that exists, estimate only the days
                  needed to compare a whole week):
                    stub missing  : full week = first x 7 / first_days
                    first missing : full week = stub  x 7 / stub_days     (stub_days = 7 - first_days)
                    both missing  : full week = NULL
  BOUNDARY_STUB   no full week of its own (NULL); its value is used by the BOUNDARY_FIRST row

  wow_numerator   = full week of this row
  wow_denominator = full week of wow_prior_qgp_date (NULL, never 0, when it does not exist)
  yoy_numerator   = full week of this row
  yoy_denominator = full week of the prior-year Saturday in the same ISO week
  metric_value_ly = prior-year full week x days_in_period / 7

  The estimate is used ONLY inside the full week. metric_value (the forecast shown for each QGP
  period, and summed into quarter totals) is never changed, so no forecast is invented for a
  period that did not have one.

  Example (upvForecast, week ending 2026-07-04; the forecast upload starts 2026-07-01):
    stub   2026-06-30  3 days  NULL      (no forecast before the Q3 upload)
    first  2026-07-04  4 days  876,000
    full week = 876,000 x 7 / 4 = 1,533,000 (previously 876,000 presented as a whole week)
    2026-07-11 WoW = 1,460,000 / 1,533,000 - 1 = -4.8% (previously +66.7%)

EXECUTION ORDER:
  Must run AFTER:
    upvForecast_bronze_upload.py notebook  (populates Bronze; manual/ad hoc,
                                            not on the weekly schedule)
  NOTE: No longer depends on sdi_sp_dashboardPulseTms_silver_adobeFunnel_weekly
        (Adobe Silver was only needed for ratio allocation, which is temporarily removed).

DOWNSTREAM (unchanged):
  sdi_vw_dashboardPulseTms_gold_unified_long -- CTE 'UpvForecast', data_source = 'UPV_FORECAST'

CHANGE LOG:
  - Ported from BQ (04_sp_sdi_pulseTms_silver_upvForecast_weekly.sql). Business logic
    (channel allocation via prior-year ratios, boundary handling, WoW/YoY) preserved exactly.
  - Channel allocation simplified: prior-year Adobe ratio CTEs (Steps 2A/2B/2C) removed.
    All Channels now receives the full Bronze value (ratio = 1.0); all other channel_group
    values receive NULL (ratio = NULL). Step 3 changed from a JOIN on ChannelRatios to a
    CROSS JOIN on a hardcoded channel list. All other steps unchanged.
  - Boundary-week WoW / YoY corrected (see WoW / YoY LOGIC):
      - A missing BOUNDARY_STUB / BOUNDARY_FIRST piece is now estimated from the other piece's
        daily rate inside the full week only (was COALESCEd to 0, so a 4-day piece was presented
        as a whole week).
      - wow_denominator now comes from the prior row's own full week (so "prior week was a
        boundary week" is decided by the calendar, not by whether its stub has data) and is NULL,
        not 0, when the prior week does not exist.
      - LY built from prior-year full weeks (same ISO week Saturday).
      - metric_value and every output column are unchanged.
================================================================================================= */

CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_upvForecast_weekly()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN

  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly
  USING DELTA
  CLUSTER BY (qgp_date, channel_group, metric_name)
  COMMENT 'PulseTMS Silver — UPV forecast long format with channel allocation and WoW/YoY. One row per qgp_date x channel_group x metric_name. All Channels receives the full Bronze value (allocation_ratio = 1.0); all other channels are NULL (allocation_ratio = NULL) until per-channel forecasts are available. metric_name: upvForecast | upvWebAppForecast. metric_type: UPV_FORECAST. Clustered by qgp_date, channel_group, metric_name. Refreshed ad hoc after each quarterly Bronze upload, via sdi_sp_dashboardPulseTms_silver_upvForecast_weekly.'
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
  -- STEP 2: Hardcoded channel list.
  --         All Channels -> ratio = 1.0 (passthrough of Bronze value).
  --         All others   -> ratio = NULL (metric_value will be NULL in Step 3).
  --
  --         Channel list matches Adobe Silver's channel_group values exactly.
  --         When per-channel forecasts become available, replace this CTE with
  --         the prior-year Adobe ratio CTEs (Steps 2A/2B/2C from the prior version)
  --         and update Step 3 to JOIN on iso_year/quarter_num instead of CROSS JOIN.
  -- ===========================================================================
  ChannelList AS (
    SELECT 'All Channels'   AS channel_group, 1.0  AS allocation_ratio UNION ALL
    SELECT 'Direct'         AS channel_group, NULL AS allocation_ratio UNION ALL
    SELECT 'Organic Search' AS channel_group, NULL AS allocation_ratio UNION ALL
    SELECT 'Paid Search'    AS channel_group, NULL AS allocation_ratio UNION ALL
    SELECT 'Paid Social'    AS channel_group, NULL AS allocation_ratio UNION ALL
    SELECT 'Programmatic'   AS channel_group, NULL AS allocation_ratio UNION ALL
    SELECT 'Other'          AS channel_group, NULL AS allocation_ratio
  ),

  -- ===========================================================================
  -- STEP 3: Apply channel list -> channel-level forecast values.
  --         CROSS JOIN: every calendar week gets one row per channel.
  --         All Channels: metric_value = Bronze value (ratio = 1.0).
  --         All others:   metric_value = NULL (ratio = NULL -> NULL * value = NULL).
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
      bwc.upv_forecast        * cl.allocation_ratio AS upv_forecast_allocated,
      bwc.upv_webapp_forecast * cl.allocation_ratio AS upv_webapp_forecast_allocated
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
      'upvWebAppForecast'           AS metric_name,
      upv_webapp_forecast_allocated AS metric_value
    FROM ForecastWithChannels
  ),

  -- ===========================================================================
  -- STEP 5: Current-period lookup (one row per QGP period), used to find the
  --         BOUNDARY_STUB piece of a BOUNDARY_FIRST week.
  -- ===========================================================================
  MetricLookup AS (
    SELECT qgp_date, channel_group, metric_name, metric_value
    FROM Unpivoted
  ),

  -- ===========================================================================
  -- STEP 6: Full Sunday-Saturday week value.
  --         NORMAL         = metric_value
  --         BOUNDARY_FIRST = stub + first; one piece missing -> estimated from the
  --                          other piece's daily rate; both missing -> NULL
  --         BOUNDARY_STUB  = NULL (consumed by the BOUNDARY_FIRST row)
  -- ===========================================================================
  FullWeek AS (
    SELECT
      u.qgp_date,
      u.week_type,
      u.iso_year,
      u.iso_week_number,
      u.channel_group,
      u.metric_name,

      CASE
        WHEN u.week_type = 'BOUNDARY_STUB'
          THEN CAST(NULL AS DOUBLE)

        WHEN u.week_type = 'BOUNDARY_FIRST' THEN
          CASE
            WHEN u.metric_value IS NOT NULL AND stub_lookup.metric_value IS NOT NULL
              THEN u.metric_value + stub_lookup.metric_value

            WHEN u.metric_value IS NOT NULL
              THEN u.metric_value * 7.0 / u.days_in_period

            WHEN stub_lookup.metric_value IS NOT NULL
              THEN stub_lookup.metric_value * 7.0 / (7 - u.days_in_period)

            ELSE CAST(NULL AS DOUBLE)
          END

        ELSE u.metric_value
      END                                                                 AS full_week_value

    FROM Unpivoted u

    LEFT JOIN MetricLookup stub_lookup
      ON  stub_lookup.qgp_date      = u.boundary_stub_date
      AND stub_lookup.channel_group = u.channel_group
      AND stub_lookup.metric_name   = u.metric_name
  ),

  -- ===========================================================================
  -- STEP 7: Prior-year full week lookup.
  --         Every ISO week (Monday-Sunday) contains exactly one Saturday, so the
  --         full week of the non-stub row in an ISO week is that week's value.
  -- ===========================================================================
  LYWeeklyLookup AS (
    SELECT
      iso_year,
      iso_week_number,
      channel_group,
      metric_name,
      MAX(full_week_value) AS ly_weekly_metric_value
    FROM FullWeek
    WHERE week_type <> 'BOUNDARY_STUB'
    GROUP BY iso_year, iso_week_number, channel_group, metric_name
  ),

  -- ===========================================================================
  -- STEP 8: WoW / YoY from full weeks.
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

      -- WoW numerator: full week of this row
      fw.full_week_value                                                  AS wow_numerator,

      -- WoW denominator: full week of the prior QGP Saturday (NULL, not 0, if missing)
      CASE
        WHEN fw.full_week_value IS NULL THEN CAST(NULL AS DOUBLE)
        ELSE prior_fw.full_week_value
      END                                                                 AS wow_denominator,

      -- YoY numerator: full week of this row
      fw.full_week_value                                                  AS yoy_numerator,

      -- YoY denominator: prior-year full week, same ISO week
      CASE
        WHEN fw.full_week_value IS NULL THEN CAST(NULL AS DOUBLE)
        ELSE ly_week.ly_weekly_metric_value
      END                                                                 AS yoy_denominator

    FROM Unpivoted u

    LEFT JOIN FullWeek fw
      ON  fw.qgp_date      = u.qgp_date
      AND fw.channel_group = u.channel_group
      AND fw.metric_name   = u.metric_name

    LEFT JOIN FullWeek prior_fw
      ON  prior_fw.qgp_date      = u.wow_prior_qgp_date
      AND prior_fw.channel_group = u.channel_group
      AND prior_fw.metric_name   = u.metric_name

    LEFT JOIN LYWeeklyLookup ly_week
      ON  ly_week.iso_year        = u.iso_year - 1
      AND ly_week.iso_week_number = u.iso_week_number
      AND ly_week.channel_group   = u.channel_group
      AND ly_week.metric_name     = u.metric_name
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