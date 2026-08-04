/* =================================================================================================
FILE:         07_sdi_sp_dashboardPulseTms_silver_platformSpend_weekly.sql
LAYER:        Stored Procedure
PROCEDURE:    sdi_sp_dashboardPulseTms_silver_platformSpend_weekly

PURPOSE:
  Creates/refreshes physical table sdi_tbl_dashboardPulseTms_silver_platformSpend_weekly.
  Reshapes Bronze's week_sun_sat x lob x channel_group grain into the same long
  metric_name/metric_value format as the rest of PulseTMS Silver, with WoW/YoY computed the
  same way as mfcSpend Silver's Channel grain -- this file mirrors that grain's logic, not
  MFC's Granular grain (Platform Spend has no channel/tactic/message_type/agency breakdown to
  offer, so there's only one grain here, not two).

  data_source = 'PLATFORM_SPEND_CHANNEL' (matches the data_source literal already sitting in
  gold_unified_long's commented-out PlatformSpend CTE). metric_name = 'platformSpend' (matches
  gold_unified_wide's commented-out Platform CTE). Single metric only -- Bronze confirmed this
  source is actuals-only, no forecast column, so there's no 'platformSpend' vs
  'platformSpendForecast' split the way MFC has Actual vs Forecast.

WHY THIS FILE IS SIMPLER THAN mfcSpend SILVER, IN ONE RESPECT:
  - Single grain (no Granular equivalent) -- Bronze has nothing deeper than lob x channel_group.
  - No Monday-anchored day-count workaround. MFC's Silver computes a local
    mfc_days_in_period because MFC's raw data is genuinely Monday-Sunday and the shared
    calendar's days_in_period is Sunday-anchored. Platform Spend's week_sun_sat is ALREADY
    Sunday-Saturday native by the time it leaves Bronze (rolled forward from the raw
    Monday-Sunday Date column via date_add/DAYOFWEEK) -- there's no day-of-week ANCHORING
    mismatch to work around here.
  - 'All Channels' rollup per lob included, matching MFC Channel grain's own pattern (not
    explicitly requested, but added for consistency with how the wide/long Gold views already
    expect this source to behave alongside MFC -- flagging this as an inferred-from-precedent
    choice, not something separately confirmed this session).

CORRECTION -- this file is NOT simpler than adobeFunnel Silver on quarter-boundary handling,
despite an earlier version of this header claiming so:
  Ruling out MFC's day-of-week ANCHORING mismatch (above) is a different question from whether
  QUARTER-BOUNDARY proration is needed, and the first version of this file conflated the two --
  concluding no proration was needed at all, which was wrong. Any source built on NATURAL
  Sunday-Saturday week rollups (Adobe and Platform Spend both -- MFC is exempt only because its
  upstream Gold source already pre-splits QGP_Week before MFC's own Bronze ever sees it) needs
  the SAME quarter-boundary proration Adobe Silver already has, for the same reason: a natural
  week straddling a quarter boundary produces exactly one Bronze row (keyed at that week's real
  Saturday, the BOUNDARY_FIRST date), containing all 7 days including days that actually belong
  to the prior quarter. Without proration, BOUNDARY_STUB comes back null (nothing keyed at its
  earlier date) and BOUNDARY_FIRST silently absorbs the whole natural week's spend, overstated
  by however many days actually belonged to the prior quarter -- confirmed as the exact bug
  present in the first version of this file once real Gold output was checked.

  FIXED, mirroring adobeFunnel Silver's exact mechanism (see that file's header for the general
  explanation): BronzeWithCalendar now has a second LEFT JOIN (aliased bf) to Bronze, active
  only for BOUNDARY_STUB rows, matching on bf.week_sun_sat = date_add(cal.qgp_date, 7 -
  DAYOFWEEK(cal.qgp_date)) -- rolling the stub's own quarter-end date forward to that same
  natural week's Saturday, landing on the identical row the main join (b) already reads for
  BOUNDARY_FIRST. Both periods now read from that one natural-week total and take their own
  proportional share via days_in_period / 7, same as every metric in Adobe Silver already does.

WoW/YoY LOGIC (mirrors mfcSpend Silver's Channel grain specifically, including that grain's
known quirk):
  NORMAL         : numerator = current value; denominator = prior QGP value
                   (if prior was BOUNDARY_FIRST: denominator = BF + its stub)
  BOUNDARY_STUB  : numerator = NULL, denominator = NULL
  BOUNDARY_FIRST : numerator = current + preceding stub
                   denominator = last NORMAL week before the stub
  LY             : prior-year same ISO week weekly total x current days_in_period / 7
  Per mfcSpend Silver's own flagged open question: the Channel grain's numerator has no
  explicit `NOT is_complete_period` guard the way MFC's Granular grain does -- this file
  matches that same (Channel-grain) behavior rather than inventing a third pattern. In
  practice this doesn't produce a different result here: BronzeWithCalendar's spend CASE has
  no ELSE branch, so metric_value is already NULL for future/incomplete weeks by the time
  WoW/YoY math runs, before any additional guard would matter.
================================================================================================= */

CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_platformSpend_weekly()
LANGUAGE SQL
AS
BEGIN

  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_platformSpend_weekly
  USING DELTA
  CLUSTER BY (qgp_date, lob, channel_group)
  COMMENT 'PulseTMS Silver — Platform (paid media) spend, lob x channel_group grain, long format with WoW/YoY. data_source = PLATFORM_SPEND_CHANNEL, metric_name = platformSpend (actuals only, no forecast). Includes an All Channels rollup per lob, matching mfcSpend Silver''s Channel-grain pattern. Clustered by qgp_date, lob, channel_group. Refreshed weekly via sdi_sp_dashboardPulseTms_silver_platformSpend_weekly.'
  AS
  WITH

  BronzeWithCalendar AS (
    SELECT
      cal.qgp_date,
      cal.week_type,
      cal.quarter                                                         AS qgp_quarter,
      cal.days_in_period,
      cal.is_complete_period,
      cal.wow_prior_qgp_date,
      cal.boundary_stub_date,
      cal.iso_week_number,
      cal.iso_year,
      channels.lob,
      channels.channel_group,

      -- Quarter-boundary proration, mirroring adobeFunnel Silver's exact mechanism (see that
      -- file's header for the general explanation). Bronze's week_sun_sat is a NATURAL
      -- Sunday-Saturday week-ending date -- for a natural week that straddles a quarter
      -- boundary, Bronze has exactly ONE row for that whole week (keyed at the week's real
      -- Saturday, which is the BOUNDARY_FIRST date), containing ALL 7 days' spend, including
      -- the days that actually belong to the prior quarter. Both the BOUNDARY_STUB and
      -- BOUNDARY_FIRST periods read from that SAME single Bronze row (via bf and b
      -- respectively -- bf's join condition rolls the stub's own quarter-end date forward to
      -- that same natural week's Saturday, landing on the identical row b already reads) and
      -- each takes its own proportional share via days_in_period / 7.
      CASE WHEN cal.week_type = 'BOUNDARY_STUB'  AND cal.is_complete_period THEN bf.spend * cal.days_in_period / 7
           WHEN cal.week_type = 'BOUNDARY_FIRST' AND cal.is_complete_period THEN b.spend  * cal.days_in_period / 7
           WHEN cal.is_complete_period                                     THEN b.spend
      END                                                                 AS spend

    FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar cal
    CROSS JOIN (
      SELECT DISTINCT lob, channel_group
      FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_platformSpend_weekly
    ) channels
    LEFT JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_platformSpend_weekly b
      ON  b.week_sun_sat  = cal.qgp_date
      AND b.lob            = channels.lob
      AND b.channel_group  = channels.channel_group
    LEFT JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_platformSpend_weekly bf
      ON  cal.week_type    = 'BOUNDARY_STUB'
      AND bf.week_sun_sat  = date_add(cal.qgp_date, 7 - EXTRACT(DAYOFWEEK FROM cal.qgp_date))
      AND bf.lob            = channels.lob
      AND bf.channel_group  = channels.channel_group
    WHERE
      cal.qgp_date < trunc(current_date(), 'QUARTER')
      OR (
        cal.qgp_date >= trunc(current_date(), 'QUARTER')
        AND cal.qgp_date <= date_sub(add_months(trunc(current_date(), 'QUARTER'), 3), 1)
      )
  ),

  UnpivotedBase AS (
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year, lob, channel_group, 'platformSpend' AS metric_name, spend AS metric_value
    FROM BronzeWithCalendar
    WHERE lob IS NOT NULL
  ),

  UnpivotedAllChannels AS (
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year, lob, 'All Channels' AS channel_group, metric_name, SUM(metric_value) AS metric_value
    FROM UnpivotedBase
    GROUP BY qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year, lob, metric_name
  ),

  Unpivoted AS (
    SELECT * FROM UnpivotedBase
    UNION ALL SELECT * FROM UnpivotedAllChannels
  ),

  MetricLookup AS (
    SELECT qgp_date, lob, channel_group, metric_name, metric_value
    FROM Unpivoted
  ),

  LYWeeklyLookup AS (
    SELECT
      iso_year,
      iso_week_number,
      lob,
      channel_group,
      metric_name,
      SUM(metric_value) AS ly_weekly_metric_value
    FROM Unpivoted
    WHERE metric_value IS NOT NULL
    GROUP BY iso_year, iso_week_number, lob, channel_group, metric_name
  ),

  WithWowYoy AS (
    SELECT
      u.qgp_date, u.week_type, u.qgp_quarter, u.days_in_period, u.is_complete_period,
      u.lob, u.channel_group, u.metric_name, u.metric_value,

      ROUND(
        ly_week.ly_weekly_metric_value * try_divide(u.days_in_period, 7),
        2
      )                                                                   AS metric_value_ly,

      CASE u.week_type
        WHEN 'BOUNDARY_STUB'  THEN NULL
        WHEN 'BOUNDARY_FIRST' THEN COALESCE(u.metric_value, 0) + COALESCE(stub_lookup.metric_value, 0)
        ELSE                       u.metric_value
      END                                                                 AS wow_numerator,

      CASE
        WHEN u.metric_value IS NULL        THEN NULL
        WHEN u.week_type = 'BOUNDARY_STUB' THEN NULL
        WHEN wow_prior_stub.metric_value IS NOT NULL
          THEN COALESCE(wow_prior_lookup.metric_value, 0) + COALESCE(wow_prior_stub.metric_value, 0)
        ELSE COALESCE(wow_prior_lookup.metric_value, 0)
      END                                                                 AS wow_denominator,

      CASE u.week_type
        WHEN 'BOUNDARY_STUB'  THEN NULL
        WHEN 'BOUNDARY_FIRST' THEN COALESCE(u.metric_value, 0) + COALESCE(stub_lookup.metric_value, 0)
        ELSE                       u.metric_value
      END                                                                 AS yoy_numerator,

      CASE
        WHEN u.metric_value IS NULL        THEN NULL
        WHEN u.week_type = 'BOUNDARY_STUB' THEN NULL
        ELSE ly_week.ly_weekly_metric_value
      END                                                                 AS yoy_denominator

    FROM Unpivoted u
    LEFT JOIN MetricLookup wow_prior_lookup
      ON  wow_prior_lookup.qgp_date      = u.wow_prior_qgp_date
      AND wow_prior_lookup.lob           = u.lob
      AND wow_prior_lookup.channel_group = u.channel_group
      AND wow_prior_lookup.metric_name   = u.metric_name
    LEFT JOIN prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar prior_cal
      ON  prior_cal.qgp_date = u.wow_prior_qgp_date
    LEFT JOIN MetricLookup wow_prior_stub
      ON  wow_prior_stub.qgp_date      = prior_cal.boundary_stub_date
      AND wow_prior_stub.lob           = u.lob
      AND wow_prior_stub.channel_group = u.channel_group
      AND wow_prior_stub.metric_name   = u.metric_name
    LEFT JOIN MetricLookup stub_lookup
      ON  stub_lookup.qgp_date      = u.boundary_stub_date
      AND stub_lookup.lob           = u.lob
      AND stub_lookup.channel_group = u.channel_group
      AND stub_lookup.metric_name   = u.metric_name
    LEFT JOIN LYWeeklyLookup ly_week
      ON  ly_week.iso_year        = u.iso_year - 1
      AND ly_week.iso_week_number = u.iso_week_number
      AND ly_week.lob             = u.lob
      AND ly_week.channel_group   = u.channel_group
      AND ly_week.metric_name     = u.metric_name
  )

  SELECT
    'PLATFORM_SPEND_CHANNEL'                                               AS data_source,
    qgp_date,
    week_type,
    qgp_quarter,
    days_in_period,
    is_complete_period,
    lob,
    channel_group,
    metric_name,
    metric_value,
    metric_value_ly,
    wow_numerator,
    wow_denominator,
    CASE WHEN wow_denominator IS NULL OR wow_denominator = 0 THEN NULL
         ELSE wow_numerator / wow_denominator - 1
    END                                                                   AS wow_pct,
    yoy_numerator,
    yoy_denominator,
    CASE WHEN yoy_denominator IS NULL OR yoy_denominator = 0 THEN NULL
         ELSE yoy_numerator / yoy_denominator - 1
    END                                                                   AS yoy_pct,
    MAX(CASE WHEN metric_value IS NOT NULL THEN qgp_date END)
      OVER (PARTITION BY lob, channel_group, metric_name)                 AS max_date
  FROM WithWowYoy;

END;