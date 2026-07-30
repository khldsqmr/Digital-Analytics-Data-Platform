/* =================================================================================================
FILE:         06_sdi_sp_dashboardPulseTms_silver_mfcSpend_weekly.sql   (Databricks port)
LAYER:        Stored Procedure
PROCEDURE:    sdi_sp_dashboardPulseTms_silver_mfcSpend_weekly

PURPOSE:
  Creates/refreshes physical table sdi_tbl_dashboardPulseTms_silver_mfcSpend_weekly.
  Called as part of the weekly refresh.

  Produces two grains in one table, distinguished by data_source:
    MFC_SPEND_CHANNEL  — rolled up to lob x channel_group, includes 'All Channels' rollup
    MFC_SPEND_GRANULAR — finest grain: lob x channel_group x channel x tactic x message_type x agency

  Both grains carry spend_actual and spend_forecast with full WoW/YoY.

WoW LOGIC (same for both grains):
  NORMAL week      : numerator = current value
                     denominator = prior QGP date value
                     (if prior was BOUNDARY_FIRST: denominator = BOUNDARY_FIRST + its stub)
  BOUNDARY_STUB    : numerator = NULL, denominator = NULL
  BOUNDARY_FIRST   : numerator = current + preceding stub
                     denominator = last NORMAL week before the stub

⚠ OPEN QUESTION carried over from BQ (not resolved here, not silently changed):
  Granular grain's WoW/YoY numerator/denominator guards on `NOT u.is_complete_period` (forcing
  future/forecast weeks to NULL); Channel grain does not have this guard, despite the header
  comment above claiming the logic is "same for both grains." Preserved exactly as in your
  validated BQ source — flagging again per your notes so it isn't lost in the port.

PRESERVED EXACTLY, per your instructions — the MFC/shared-calendar day-anchoring split:
  - `days_in_period` (from the shared calendar) stays Sunday-anchored in the SELECT's output
    column, unchanged, consistent with every other data_source in the unified Gold view.
  - `mfc_days_in_period`, computed locally in BronzeWithCalendar via trunc(date, 'WEEK')
    (Monday-anchored), is used ONLY for the metric_value_ly line in both grains — exactly as
    in your BQ version. The shared calendar itself is not touched.

PORTING NOTES (BQ -> Databricks), applies to this file only:
  - DATE_TRUNC(x, WEEK(MONDAY))        -> trunc(x, 'WEEK')   (Databricks WEEK truncation is
                                           Monday-anchored by definition — direct equivalent)
  - DATE_DIFF(a, b, DAY)               -> DATEDIFF(a, b)     (same argument order: a - b, in days)
  - SAFE_DIVIDE(a, b)                  -> try_divide(a, b)
  - IF(cond, a, b)                     -> unchanged; Databricks SQL supports IF() as a CASE WHEN alias
  - DATE_TRUNC(x, QUARTER)             -> trunc(x, 'QUARTER')
  - DATE_SUB(DATE_ADD(x, INTERVAL 3 MONTH), INTERVAL 1 DAY) -> date_sub(add_months(x, 3), 1)
  - NOT u.is_complete_period           -> unchanged; Databricks SQL supports boolean NOT directly
  - CREATE TABLE ... PARTITION BY x CLUSTER BY y,z ... OPTIONS(description=...)
                                        -> CREATE TABLE ... CLUSTER BY (x, y, z) COMMENT '...'
  - OPTIONS(strict_mode=false)         -> dropped; no Databricks equivalent
  - CREATE OR REPLACE PROCEDURE ... BEGIN...END -> needs explicit LANGUAGE SQL clause

CHANGE LOG:
  - dim calendar column 'quarter' aliased as 'qgp_quarter' in output.
  - FIX: Channel-grain wow_numerator and yoy_numerator now COALESCE the current
    side (u.metric_value) before adding the stub's value, matching the
    denominator formulas and the Granular grain. Previously, u.metric_value
    being NULL (one side missing at a boundary week) silently nulled the
    entire numerator, discarding a real value sitting at the paired stub.
  - FIX: metric_value_ly (both grains) now uses a locally-computed
    mfc_days_in_period instead of the shared calendar's days_in_period.
    MFC's raw data is genuinely Monday-Sunday; the shared PulseTMS calendar's
    days_in_period is Sunday-anchored — correct for Adobe/Platform's native
    Sun-Sat weeks, but off by one day for MFC's boundary weeks. Rather than
    change the shared calendar (which would break Adobe/Platform), this
    procedure computes its own Monday-anchored day-count locally, used only
    for this one trend-line calculation. The days_in_period *output* column
    is left as-is (still the shared calendar's value), consistent with every
    other data_source in the unified Gold view.
================================================================================================= */

CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_mfcSpend_weekly()
LANGUAGE SQL
AS
BEGIN

  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_mfcSpend_weekly
  USING DELTA
  CLUSTER BY (qgp_date, data_source, channel_group, metric_name)
  COMMENT 'PulseTMS Silver — MFC spend long format with WoW/YoY. Contains MFC_SPEND_CHANNEL (lob x channel_group) and MFC_SPEND_GRANULAR (full grain) data_source values. Clustered by qgp_date, data_source, channel_group, metric_name. Refreshed weekly via sdi_sp_dashboardPulseTms_silver_mfcSpend_weekly.'
  AS
  WITH
  BronzeWithCalendar AS (
    SELECT
      cal.qgp_date,
      cal.week_type,
      cal.quarter                                                         AS qgp_quarter,
      cal.days_in_period,
      CASE cal.week_type
        WHEN 'NORMAL' THEN 7
        WHEN 'BOUNDARY_STUB' THEN
          DATEDIFF(cal.qgp_date, trunc(cal.qgp_date, 'WEEK')) + 1
        WHEN 'BOUNDARY_FIRST' THEN
          7 - (DATEDIFF(cal.boundary_stub_date, trunc(cal.boundary_stub_date, 'WEEK')) + 1)
      END                                                                 AS mfc_days_in_period,
      cal.is_complete_period,
      cal.is_current_quarter,
      cal.wow_prior_qgp_date,
      cal.prior_year_qgp_date,
      cal.boundary_stub_date,
      cal.iso_week_number,
      cal.iso_year,
      channels.lob,
      channels.channel_group,
      channels.channel,
      channels.tactic,
      channels.message_type,
      channels.agency,
      IF(cal.is_complete_period, b.spend_actual,   NULL)                  AS spend_actual,
      b.spend_forecast                                                    AS spend_forecast,
      b.file_load_date
    FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar cal
    CROSS JOIN (
      SELECT DISTINCT lob, channel_group, channel, tactic, message_type, agency
      FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_mfcSpend_weekly
    ) channels
    LEFT JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_mfcSpend_weekly b
      ON  b.qgp_week      = cal.qgp_date
      AND b.lob           = channels.lob
      AND b.channel_group = channels.channel_group
      AND b.channel       = channels.channel
      AND b.tactic        = channels.tactic
      AND b.message_type  = channels.message_type
      AND b.agency        = channels.agency
    WHERE
      cal.qgp_date < trunc(current_date(), 'QUARTER')
      OR (
        cal.qgp_date >= trunc(current_date(), 'QUARTER')
        AND cal.qgp_date <= date_sub(add_months(trunc(current_date(), 'QUARTER'), 3), 1)
      )
  ),

  UnpivotedGranular AS (
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, mfc_days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, lob, channel_group, channel, tactic, message_type, agency, 'mfcSpendActual'   AS metric_name, spend_actual   AS metric_value FROM BronzeWithCalendar WHERE lob IS NOT NULL
    UNION ALL
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, mfc_days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, lob, channel_group, channel, tactic, message_type, agency, 'mfcSpendForecast' AS metric_name, spend_forecast AS metric_value FROM BronzeWithCalendar WHERE lob IS NOT NULL
  ),

  UnpivotedChannelBase AS (
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, mfc_days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, lob, channel_group, 'mfcSpendActual'   AS metric_name, SUM(spend_actual)   AS metric_value FROM BronzeWithCalendar WHERE lob IS NOT NULL GROUP BY qgp_date, week_type, qgp_quarter, days_in_period, mfc_days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, lob, channel_group
    UNION ALL
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, mfc_days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, lob, channel_group, 'mfcSpendForecast' AS metric_name, SUM(spend_forecast) AS metric_value FROM BronzeWithCalendar WHERE lob IS NOT NULL GROUP BY qgp_date, week_type, qgp_quarter, days_in_period, mfc_days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, lob, channel_group
  ),
  UnpivotedChannelAllChannels AS (
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, mfc_days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, lob, 'All Channels' AS channel_group, metric_name, SUM(metric_value) AS metric_value
    FROM UnpivotedChannelBase
    GROUP BY qgp_date, week_type, qgp_quarter, days_in_period, mfc_days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, lob, metric_name
  ),
  UnpivotedChannel AS (
    SELECT * FROM UnpivotedChannelBase
    UNION ALL SELECT * FROM UnpivotedChannelAllChannels
  ),

  MetricLookupChannel AS (
    SELECT qgp_date, lob, channel_group, metric_name, metric_value
    FROM UnpivotedChannel
  ),

  LYWeeklyLookupChannel AS (
    SELECT
      iso_year,
      iso_week_number,
      lob,
      channel_group,
      metric_name,
      SUM(metric_value) AS ly_weekly_metric_value
    FROM UnpivotedChannel
    WHERE metric_value IS NOT NULL
    GROUP BY iso_year, iso_week_number, lob, channel_group, metric_name
  ),

  MetricLookupGranular AS (
    SELECT qgp_date, lob, channel_group, channel, tactic, message_type, agency, metric_name, metric_value
    FROM UnpivotedGranular
  ),

  LYWeeklyLookupGranular AS (
    SELECT
      iso_year,
      iso_week_number,
      lob,
      channel_group,
      channel,
      tactic,
      message_type,
      agency,
      metric_name,
      SUM(metric_value) AS ly_weekly_metric_value
    FROM UnpivotedGranular
    WHERE metric_value IS NOT NULL
    GROUP BY iso_year, iso_week_number, lob, channel_group, channel, tactic, message_type, agency, metric_name
  ),

  ChannelWithWowYoy AS (
    SELECT
      u.qgp_date, u.week_type, u.qgp_quarter, u.days_in_period, u.is_complete_period,
      u.channel_group, u.metric_name, u.metric_value,
      ROUND(
        ly_week.ly_weekly_metric_value * try_divide(u.mfc_days_in_period, 7),
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
        WHEN wow_prior_stub_ch.metric_value IS NOT NULL
          THEN COALESCE(wow_prior_lookup.metric_value, 0) + COALESCE(wow_prior_stub_ch.metric_value, 0)
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
      END                                                                 AS yoy_denominator,
      u.lob                                                               AS lob_mfc,
      CAST(NULL AS STRING)                                                AS channel,
      CAST(NULL AS STRING)                                                AS tactic,
      CAST(NULL AS STRING)                                                AS message_type,
      CAST(NULL AS STRING)                                                AS agency
    FROM UnpivotedChannel u
    LEFT JOIN MetricLookupChannel wow_prior_lookup
      ON  wow_prior_lookup.qgp_date      = u.wow_prior_qgp_date
      AND wow_prior_lookup.lob           = u.lob
      AND wow_prior_lookup.channel_group = u.channel_group
      AND wow_prior_lookup.metric_name   = u.metric_name
    LEFT JOIN MetricLookupChannel stub_lookup
      ON  stub_lookup.qgp_date      = u.boundary_stub_date
      AND stub_lookup.lob           = u.lob
      AND stub_lookup.channel_group = u.channel_group
      AND stub_lookup.metric_name   = u.metric_name
    LEFT JOIN LYWeeklyLookupChannel ly_week
      ON  ly_week.iso_year        = u.iso_year - 1
      AND ly_week.iso_week_number = u.iso_week_number
      AND ly_week.lob             = u.lob
      AND ly_week.channel_group   = u.channel_group
      AND ly_week.metric_name     = u.metric_name
    LEFT JOIN prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar prior_cal_ch
      ON  prior_cal_ch.qgp_date = u.wow_prior_qgp_date
    LEFT JOIN MetricLookupChannel wow_prior_stub_ch
      ON  wow_prior_stub_ch.qgp_date      = prior_cal_ch.boundary_stub_date
      AND wow_prior_stub_ch.lob           = u.lob
      AND wow_prior_stub_ch.channel_group = u.channel_group
      AND wow_prior_stub_ch.metric_name   = u.metric_name
  ),

  GranularWithWowYoy AS (
    SELECT
      u.qgp_date, u.week_type, u.qgp_quarter, u.days_in_period, u.is_complete_period,
      u.channel_group, u.metric_name, u.metric_value,
      ROUND(
        ly_week.ly_weekly_metric_value * try_divide(u.mfc_days_in_period, 7),
        2
      )                                                                   AS metric_value_ly,
      CASE
        WHEN NOT u.is_complete_period     THEN NULL
        WHEN u.week_type = 'BOUNDARY_STUB'  THEN NULL
        WHEN u.week_type = 'BOUNDARY_FIRST' THEN COALESCE(u.metric_value, 0) + COALESCE(stub_lookup.metric_value, 0)
        ELSE u.metric_value
      END                                                                 AS wow_numerator,
      CASE
        WHEN NOT u.is_complete_period     THEN NULL
        WHEN u.week_type = 'BOUNDARY_STUB' THEN NULL
        WHEN wow_prior_stub_gr.metric_value IS NOT NULL
          THEN COALESCE(wow_prior_lookup.metric_value, 0) + COALESCE(wow_prior_stub_gr.metric_value, 0)
        ELSE COALESCE(wow_prior_lookup.metric_value, 0)
      END                                                                 AS wow_denominator,
      CASE u.week_type
        WHEN 'BOUNDARY_STUB'  THEN NULL
        WHEN 'BOUNDARY_FIRST' THEN COALESCE(u.metric_value, 0) + COALESCE(stub_lookup.metric_value, 0)
        ELSE u.metric_value
      END                                                                 AS yoy_numerator,
      CASE
        WHEN NOT u.is_complete_period      THEN NULL
        WHEN u.week_type = 'BOUNDARY_STUB' THEN NULL
        ELSE ly_week.ly_weekly_metric_value
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
    LEFT JOIN MetricLookupGranular stub_lookup
      ON  stub_lookup.qgp_date      = u.boundary_stub_date
      AND stub_lookup.lob           = u.lob
      AND stub_lookup.channel_group = u.channel_group
      AND stub_lookup.channel       = u.channel
      AND stub_lookup.tactic        = u.tactic
      AND stub_lookup.message_type  = u.message_type
      AND stub_lookup.agency        = u.agency
      AND stub_lookup.metric_name   = u.metric_name
    LEFT JOIN LYWeeklyLookupGranular ly_week
      ON  ly_week.iso_year        = u.iso_year - 1
      AND ly_week.iso_week_number = u.iso_week_number
      AND ly_week.lob             = u.lob
      AND ly_week.channel_group   = u.channel_group
      AND ly_week.channel         = u.channel
      AND ly_week.tactic          = u.tactic
      AND ly_week.message_type    = u.message_type
      AND ly_week.agency          = u.agency
      AND ly_week.metric_name     = u.metric_name
    LEFT JOIN prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar prior_cal_gr
      ON  prior_cal_gr.qgp_date = u.wow_prior_qgp_date
    LEFT JOIN MetricLookupGranular wow_prior_stub_gr
      ON  wow_prior_stub_gr.qgp_date      = prior_cal_gr.boundary_stub_date
      AND wow_prior_stub_gr.lob           = u.lob
      AND wow_prior_stub_gr.channel_group = u.channel_group
      AND wow_prior_stub_gr.channel       = u.channel
      AND wow_prior_stub_gr.tactic        = u.tactic
      AND wow_prior_stub_gr.message_type  = u.message_type
      AND wow_prior_stub_gr.agency        = u.agency
      AND wow_prior_stub_gr.metric_name   = u.metric_name
  ),

  Combined AS (
    SELECT 'MFC_SPEND_CHANNEL'  AS data_source, * FROM ChannelWithWowYoy
    UNION ALL
    SELECT 'MFC_SPEND_GRANULAR' AS data_source, * FROM GranularWithWowYoy
  )

  SELECT
    data_source,
    qgp_date,
    week_type,
    qgp_quarter,
    days_in_period,
    is_complete_period,
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
      OVER (PARTITION BY data_source, metric_name)                        AS max_date,
    lob_mfc,
    channel,
    tactic,
    message_type,
    agency
  FROM Combined;

END;