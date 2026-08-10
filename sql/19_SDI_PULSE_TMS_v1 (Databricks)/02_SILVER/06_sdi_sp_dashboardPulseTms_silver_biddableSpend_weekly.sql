/* =================================================================================================
FILE:         sdi_sp_dashboardPulseTms_silver_biddableSpend_weekly.sql
LAYER:        Stored Procedure
PROCEDURE:    sdi_sp_dashboardPulseTms_silver_biddableSpend_weekly

PURPOSE:
  Creates/refreshes physical table sdi_tbl_dashboardPulseTms_silver_biddableSpend_weekly.
  Reshapes Bronze's week_sun_sat x lob x channel_group x platform grain into the same long
  metric_name/metric_value format as the rest of PulseTMS Silver, at CHANNEL grain (platform
  collapsed away -- see PLATFORM COLLAPSE note below). Mirrors platformSpend Silver's pattern
  exactly, including its boundary-proration mechanism, since biddableSpend Bronze has the same
  natural-week rollup and would have the identical bug if that fix weren't carried over.

  data_source = 'BIDDABLE_SPEND_CHANNEL' -- coexists with PLATFORM_SPEND_CHANNEL and
  MFC_SPEND_CHANNEL as a separate, toggle-able Gold data_source, not a replacement for either.
  metric_name = 'biddableSpend' (single metric -- all 3 raw sources are actuals-only, no
  forecast concept, same as platformSpend).

LOB SCOPE (unfiltered here, by design):
  This Silver carries every LOB Bronze has (POSTPAID, HSI, PREPAID, TFB, METRO, FIBER, ARCHIVED,
  etc.) -- no WHERE lob IN (...) filter. The POSTPAID/BROADBAND-only scope Khalid wants lives at
  Gold, not here, on the stated assumption that "only have postpaid and broadband in the gold"
  meant literally that layer -- flagged to Khalid as an assumption, not yet explicitly confirmed.
  HSI -> BROADBAND canonicalization also happens at Gold, not here, matching MFC's own precedent
  (see Bronze's header for the full reasoning) -- this Silver's lob column still reads 'HSI'.

PLATFORM COLLAPSE:
  Bronze's grain includes `platform` (DSP / channel / ad_platform, unified). This Silver
  collapses it away (SUM over platform) in the first CTE, since BIDDABLE_SPEND_CHANNEL is
  meant to be a straight channel-grain rollup, mirroring MFC_SPEND_CHANNEL. A future
  BIDDABLE_SPEND_GRANULAR Silver would skip that collapse and carry platform through instead --
  Bronze already supports that without any changes, only new Silver/Gold work would be needed.

BOUNDARY-PRORATION (mirrors platformSpend Silver's fix exactly -- see that file's header for the
full mechanism explanation):
  Bronze's week_sun_sat is a natural Sunday-Saturday week-ending date. For a natural week
  straddling a quarter boundary, Bronze has exactly one row (keyed at that week's real Saturday,
  the BOUNDARY_FIRST date) containing all 7 days' spend. Both BOUNDARY_STUB and BOUNDARY_FIRST
  read from that SAME row (via a second bf join that rolls the stub's own quarter-end date
  forward to that same natural Saturday) and each takes its own days_in_period / 7 share.
  Without this, BOUNDARY_STUB comes back null and BOUNDARY_FIRST is silently overstated --
  confirmed as a real bug once already this session in platformSpend Silver, not hypothetical.

'All Channels' ROLLUP:
  Included per lob, matching MFC_SPEND_CHANNEL and platformSpend Silver's own precedent --
  inferred-from-pattern, not separately requested for this source specifically.

WoW/YoY LOGIC (same pattern as every other Silver SP in this pipeline):
  NORMAL         : numerator = current value; denominator = prior QGP value
                   (if prior was BOUNDARY_FIRST: denominator = BF + its stub)
  BOUNDARY_STUB  : numerator = NULL, denominator = NULL
  BOUNDARY_FIRST : numerator = current + preceding stub
                   denominator = last NORMAL week before the stub
  LY             : prior-year same ISO week weekly total x current days_in_period / 7

⚠ KNOWN CAVEAT, confirmed from live Gold output (2026-08-08): Programmatic's source
(pbi_programmatic_browsers_currentyr) only has one year of history (2026 only, confirmed via
MIN/MAX(date) this session) -- so Programmatic's own yoy_pct is always null, as expected. But
this ALSO silently inflates every 'All Channels' yoy_pct for any lob where Programmatic
contributes real current-year spend: the numerator sums all 3 channel_groups, but
metric_value_ly structurally can only ever sum Paid Search + Paid Social (Programmatic
contributes zero to the LY side, every week, permanently, until that source accumulates a
second year). Confirmed concretely: POSTPAID/All Channels showed yoy_pct=+231% and
PREPAID/All Channels showed +829% off real 2026-08-08 data -- both driven by comparing a
3-channel current total against a structurally 2-channel prior-year baseline, not real growth.
This is correct SQL behavior (SUM() correctly skips the null), not a bug to fix here -- but
anyone reading 'All Channels' yoy_pct off Gold should know it's currently comparing an
incomplete baseline until Programmatic has real YoY history of its own.
================================================================================================= */

CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_biddableSpend_weekly()
LANGUAGE SQL
AS
BEGIN

  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_biddableSpend_weekly
  USING DELTA
  CLUSTER BY (qgp_date, lob, channel_group)
  COMMENT 'PulseTMS Silver — Biddable spend (Programmatic + Paid Social + Paid Search combined), lob x channel_group grain, long format with WoW/YoY. data_source = BIDDABLE_SPEND_CHANNEL, metric_name = biddableSpend (actuals only, no forecast). Platform/DSP detail collapsed away here — see Bronze for the granular grain. Unfiltered on lob — POSTPAID/BROADBAND scope applied at Gold. Includes an All Channels rollup per lob. Refreshed weekly via sdi_sp_dashboardPulseTms_silver_biddableSpend_weekly.'
  AS
  WITH

  BronzeAgg AS (
    -- Collapse platform away: this Silver is the CHANNEL-grain rollup, not the granular one.
    SELECT
      week_sun_sat,
      lob,
      channel_group,
      SUM(spend) AS spend
    FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_biddableSpend_weekly
    GROUP BY week_sun_sat, lob, channel_group
  ),

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

      -- Quarter-boundary proration -- see header for the full mechanism explanation.
      CASE WHEN cal.week_type = 'BOUNDARY_STUB'  AND cal.is_complete_period THEN bf.spend * cal.days_in_period / 7
           WHEN cal.week_type = 'BOUNDARY_FIRST' AND cal.is_complete_period THEN b.spend  * cal.days_in_period / 7
           WHEN cal.is_complete_period                                     THEN b.spend
      END                                                                 AS spend

    FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar cal
    CROSS JOIN (
      SELECT DISTINCT lob, channel_group
      FROM BronzeAgg
    ) channels
    LEFT JOIN BronzeAgg b
      ON  b.week_sun_sat  = cal.qgp_date
      AND b.lob            = channels.lob
      AND b.channel_group  = channels.channel_group
    LEFT JOIN BronzeAgg bf
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
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year, lob, channel_group, 'biddableSpend' AS metric_name, spend AS metric_value
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
    'BIDDABLE_SPEND_CHANNEL'                                               AS data_source,
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