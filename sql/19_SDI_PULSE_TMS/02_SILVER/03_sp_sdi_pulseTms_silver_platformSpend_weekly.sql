/* =================================================================================================
FILE:         03_sp_sdi_pulseTms_silver_platformSpend_weekly.sql
LAYER:        Stored Procedure
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROCEDURE:    sp_sdi_pulseTms_silver_platformSpend_weekly

PURPOSE:
  Creates/refreshes physical table sdi_pulseTms_silver_platformSpend_weekly.
  Called by 00_call_all_sp_pulseTms.sql as part of the weekly refresh.
================================================================================================= */

CREATE OR REPLACE PROCEDURE
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_pulseTms_silver_platformSpend_weekly`()
OPTIONS (strict_mode = false)
BEGIN

  CREATE OR REPLACE TABLE
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_platformSpend_weekly`
  PARTITION BY qgp_date
  CLUSTER BY channel_group, metric_name
  OPTIONS (
    description = 'PulseTMS Silver — Platform spend long format with WoW/YoY. One row per qgp_date x lob x channel_group x metric_name. Partitioned by qgp_date, clustered by channel_group and metric_name. Refreshed weekly via sp_sdi_pulseTms_silver_platformSpend_weekly.'
  )
  AS
  WITH
  BronzeWithCalendar AS (
    SELECT
      cal.qgp_date, cal.week_type, cal.quarter, cal.days_in_period,
      cal.is_complete_period, cal.is_current_quarter,
      cal.wow_prior_qgp_date, cal.prior_year_qgp_date,
      cal.boundary_stub_date, cal.iso_week_number, cal.iso_year,
      channels.lob, channels.channel_group,
      IF(cal.is_complete_period, b.spend, NULL) AS spend
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar` cal
    CROSS JOIN (
      SELECT DISTINCT lob, channel_group
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_bronze_platformSpend_weekly`
      WHERE lob = 'POSTPAID'
    ) channels
    LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_bronze_platformSpend_weekly` b
      ON b.week_sun_sat = cal.qgp_date
      AND b.lob = channels.lob
      AND b.channel_group = channels.channel_group
    WHERE
      -- All historical quarters (fully complete)
      cal.qgp_date < DATE_TRUNC(CURRENT_DATE(), QUARTER)
      -- Full current quarter spine including future weeks — so Tableau shows the complete quarter
      OR (
        cal.qgp_date >= DATE_TRUNC(CURRENT_DATE(), QUARTER)
        AND cal.qgp_date <= DATE_SUB(
              DATE_ADD(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 3 MONTH),
              INTERVAL 1 DAY
            )
      )
  ),
  UnpivotedBase AS (
    SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, lob, channel_group, 'platformSpend' AS metric_name, spend AS metric_value FROM BronzeWithCalendar WHERE lob IS NOT NULL
  ),
  AllChannelsRollup AS (
    SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, lob, 'All Channels' AS channel_group, metric_name, SUM(metric_value) AS metric_value FROM UnpivotedBase GROUP BY qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, lob, metric_name
  ),
  Unpivoted AS (
    SELECT * FROM UnpivotedBase
    UNION ALL SELECT * FROM AllChannelsRollup
  ),
  MetricLookup AS (SELECT qgp_date, lob, channel_group, metric_name, metric_value FROM Unpivoted),
  WithWowYoy AS (
    SELECT
      u.qgp_date, u.week_type, u.quarter, u.days_in_period, u.lob, u.channel_group, u.metric_name, u.metric_value,
      ly_lookup.metric_value AS metric_value_ly,
      CASE u.week_type WHEN 'BOUNDARY_STUB' THEN NULL ELSE u.metric_value END AS wow_numerator,
      CASE WHEN u.metric_value IS NULL THEN NULL WHEN u.week_type = 'BOUNDARY_STUB' THEN NULL ELSE wow_prior_lookup.metric_value END AS wow_denominator,
      CASE u.week_type WHEN 'BOUNDARY_STUB' THEN NULL ELSE u.metric_value END AS yoy_numerator,
      CASE WHEN u.metric_value IS NULL THEN NULL WHEN u.week_type = 'BOUNDARY_STUB' THEN NULL ELSE ly_lookup.metric_value END AS yoy_denominator
    FROM Unpivoted u
    LEFT JOIN MetricLookup wow_prior_lookup ON wow_prior_lookup.qgp_date = u.wow_prior_qgp_date AND wow_prior_lookup.lob = u.lob AND wow_prior_lookup.channel_group = u.channel_group AND wow_prior_lookup.metric_name = u.metric_name
    LEFT JOIN MetricLookup ly_lookup ON ly_lookup.qgp_date = u.prior_year_qgp_date AND ly_lookup.lob = u.lob AND ly_lookup.channel_group = u.channel_group AND ly_lookup.metric_name = u.metric_name
  )
  SELECT
    qgp_date, week_type, quarter, days_in_period, lob, channel_group, metric_name,
    metric_value, metric_value_ly, wow_numerator, wow_denominator,
    CASE WHEN wow_denominator IS NULL OR wow_denominator = 0 THEN NULL ELSE wow_numerator / wow_denominator - 1 END AS wow_pct,
    yoy_numerator, yoy_denominator,
    CASE WHEN yoy_denominator IS NULL OR yoy_denominator = 0 THEN NULL ELSE yoy_numerator / yoy_denominator - 1 END AS yoy_pct,
    MAX(CASE WHEN metric_value IS NOT NULL THEN qgp_date END) OVER (PARTITION BY metric_name) AS max_date
  FROM WithWowYoy;

END;