/* =================================================================================================
FILE:         01_sp_sdi_pulseTms_silver_adobeFunnel_weekly.sql
LAYER:        Stored Procedure
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROCEDURE:    sp_sdi_pulseTms_silver_adobeFunnel_weekly

PURPOSE:
  Creates/refreshes physical table sdi_pulseTms_silver_adobeFunnel_weekly.
  Called by 00_call_all_sp_pulseTms.sql as part of the weekly refresh.
================================================================================================= */

CREATE OR REPLACE PROCEDURE
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_pulseTms_silver_adobeFunnel_weekly`()
OPTIONS (strict_mode = false)
BEGIN

  CREATE OR REPLACE TABLE
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_adobeFunnel_weekly`
  PARTITION BY qgp_date
  CLUSTER BY channel_group, metric_name
  OPTIONS (
    description = 'PulseTMS Silver — Adobe UPV funnel metrics in long format with WoW/YoY. One row per qgp_date x channel_group x metric_name. Partitioned by qgp_date, clustered by channel_group and metric_name. Refreshed weekly via sp_sdi_pulseTms_silver_adobeFunnel_weekly.'
  )
  AS
  WITH
  BronzeWithCalendar AS (
    SELECT
      cal.qgp_date, cal.week_type, cal.quarter, cal.days_in_period,
      cal.is_complete_period, cal.is_current_quarter,
      cal.wow_prior_qgp_date, cal.prior_year_qgp_date,
      cal.boundary_stub_date, cal.iso_week_number, cal.iso_year,
      CASE b.channel_group WHEN 'ALL' THEN 'All Channels' ELSE b.channel_group END AS channel_group,
      IF(cal.is_complete_period, b.upvPostpaid,              NULL) AS upvPostpaid,
      IF(cal.is_complete_period, b.upvHsi,                   NULL) AS upvHsi,
      IF(cal.is_complete_period, b.upvByod,                  NULL) AS upvByod,
      IF(cal.is_complete_period, b.upvFlowTotal,             NULL) AS upvFlowTotal,
      IF(cal.is_complete_period, b.upvTotalAdobe,            NULL) AS upvTotalAdobe,
      IF(cal.is_complete_period, b.cartstartPostpaid,        NULL) AS cartstartPostpaid,
      IF(cal.is_complete_period, b.cartstartHsi,             NULL) AS cartstartHsi,
      IF(cal.is_complete_period, b.cartstartByod,            NULL) AS cartstartByod,
      IF(cal.is_complete_period, b.ordersUnassistedPostpaid, NULL) AS ordersUnassistedPostpaid,
      IF(cal.is_complete_period, b.ordersUnassistedHsi,      NULL) AS ordersUnassistedHsi,
      IF(cal.is_complete_period, b.ordersUnassistedByod,     NULL) AS ordersUnassistedByod,
      IF(cal.is_complete_period, b.ordersAssistedPostpaid,   NULL) AS ordersAssistedPostpaid,
      IF(cal.is_complete_period, b.ordersAssistedHsi,        NULL) AS ordersAssistedHsi,
      IF(cal.is_complete_period, b.ordersAssistedByod,       NULL) AS ordersAssistedByod,
      IF(cal.is_complete_period, b.cartstartPostpaid + b.cartstartHsi + b.cartstartByod, NULL) AS cartstartTotal,
      IF(cal.is_complete_period, b.ordersUnassistedPostpaid + b.ordersUnassistedHsi + b.ordersUnassistedByod, NULL) AS ordersUnassistedTotal,
      IF(cal.is_complete_period, b.ordersAssistedPostpaid + b.ordersAssistedHsi + b.ordersAssistedByod, NULL) AS ordersAssistedTotal,
      IF(cal.is_complete_period,
         (b.ordersUnassistedPostpaid + b.ordersUnassistedHsi + b.ordersUnassistedByod)
         + (b.ordersAssistedPostpaid + b.ordersAssistedHsi + b.ordersAssistedByod), NULL) AS ordersTotal
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar` cal
    LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_bronze_adobeFunnel_weekly` b
      ON b.week_sun_sat = cal.qgp_date
    WHERE cal.is_current_quarter = TRUE OR cal.qgp_date < DATE_TRUNC(CURRENT_DATE(), QUARTER)
  ),
  Unpivoted AS (
    SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'upvPostpaid' AS metric_name, upvPostpaid AS metric_value FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'upvHsi', upvHsi FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'upvByod', upvByod FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'upvFlowTotal', upvFlowTotal FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'upvTotalAdobe', upvTotalAdobe FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'cartstartPostpaid', cartstartPostpaid FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'cartstartHsi', cartstartHsi FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'cartstartByod', cartstartByod FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'cartstartTotal', cartstartTotal FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'ordersUnassistedPostpaid', ordersUnassistedPostpaid FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'ordersUnassistedHsi', ordersUnassistedHsi FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'ordersUnassistedByod', ordersUnassistedByod FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'ordersUnassistedTotal', ordersUnassistedTotal FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'ordersAssistedPostpaid', ordersAssistedPostpaid FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'ordersAssistedHsi', ordersAssistedHsi FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'ordersAssistedByod', ordersAssistedByod FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'ordersAssistedTotal', ordersAssistedTotal FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'ordersTotal', ordersTotal FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
  ),
  MetricLookup AS (
    SELECT qgp_date, channel_group, metric_name, metric_value FROM Unpivoted
  ),
  WithWowYoy AS (
    SELECT
      u.qgp_date, u.week_type, u.quarter, u.days_in_period, u.is_complete_period,
      u.is_current_quarter, u.channel_group, u.metric_name, u.metric_value,
      ly_lookup.metric_value AS metric_value_ly,
      CASE u.week_type WHEN 'BOUNDARY_STUB' THEN NULL WHEN 'BOUNDARY_FIRST' THEN u.metric_value + stub_lookup.metric_value ELSE u.metric_value END AS wow_numerator,
      CASE u.week_type WHEN 'BOUNDARY_STUB' THEN NULL ELSE wow_prior_lookup.metric_value END AS wow_denominator,
      CASE u.week_type WHEN 'BOUNDARY_STUB' THEN NULL WHEN 'BOUNDARY_FIRST' THEN u.metric_value + stub_lookup.metric_value ELSE u.metric_value END AS yoy_numerator,
      CASE u.week_type WHEN 'BOUNDARY_STUB' THEN NULL WHEN 'BOUNDARY_FIRST' THEN yoy_bf_lookup.metric_value + yoy_stub_lookup.metric_value ELSE ly_lookup.metric_value END AS yoy_denominator
    FROM Unpivoted u
    LEFT JOIN MetricLookup wow_prior_lookup ON wow_prior_lookup.qgp_date = u.wow_prior_qgp_date AND wow_prior_lookup.channel_group = u.channel_group AND wow_prior_lookup.metric_name = u.metric_name
    LEFT JOIN MetricLookup ly_lookup ON ly_lookup.qgp_date = u.prior_year_qgp_date AND ly_lookup.channel_group = u.channel_group AND ly_lookup.metric_name = u.metric_name
    LEFT JOIN MetricLookup stub_lookup ON stub_lookup.qgp_date = u.boundary_stub_date AND stub_lookup.channel_group = u.channel_group AND stub_lookup.metric_name = u.metric_name
    LEFT JOIN MetricLookup yoy_bf_lookup ON yoy_bf_lookup.qgp_date = u.prior_year_qgp_date AND yoy_bf_lookup.channel_group = u.channel_group AND yoy_bf_lookup.metric_name = u.metric_name
    LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar` ly_cal ON ly_cal.qgp_date = u.prior_year_qgp_date
    LEFT JOIN MetricLookup yoy_stub_lookup ON yoy_stub_lookup.qgp_date = ly_cal.boundary_stub_date AND yoy_stub_lookup.channel_group = u.channel_group AND yoy_stub_lookup.metric_name = u.metric_name
  )
  SELECT
    qgp_date, week_type, quarter, days_in_period, channel_group, metric_name,
    metric_value, metric_value_ly, wow_numerator, wow_denominator,
    CASE WHEN wow_denominator IS NULL OR wow_denominator = 0 THEN NULL ELSE wow_numerator / wow_denominator - 1 END AS wow_pct,
    yoy_numerator, yoy_denominator,
    CASE WHEN yoy_denominator IS NULL OR yoy_denominator = 0 THEN NULL ELSE yoy_numerator / yoy_denominator - 1 END AS yoy_pct,
    MAX(CASE WHEN metric_value IS NOT NULL THEN qgp_date END) OVER (PARTITION BY metric_name) AS max_date
  FROM WithWowYoy;

END;