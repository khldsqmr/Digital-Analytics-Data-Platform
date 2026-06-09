/* =================================================================================================
FILE:         02_sp_sdi_pulseTms_silver_mfcSpend_weekly.sql
LAYER:        Stored Procedure
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROCEDURE:    sp_sdi_pulseTms_silver_mfcSpend_weekly

PURPOSE:
  Creates/refreshes physical table sdi_pulseTms_silver_mfcSpend_weekly.
  Called by 00_call_all_sp_pulseTms.sql as part of the weekly refresh.
================================================================================================= */

CREATE OR REPLACE PROCEDURE
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_pulseTms_silver_mfcSpend_weekly`()
OPTIONS (strict_mode = false)
BEGIN

  CREATE OR REPLACE TABLE
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_silver_mfcSpend_weekly`
  PARTITION BY qgp_date
  CLUSTER BY data_source, channel_group, metric_name
  OPTIONS (
    description = 'PulseTMS Silver — MFC spend long format with WoW/YoY. Contains MFC_SPEND_CHANNEL and MFC_SPEND_GRANULAR grains. Partitioned by qgp_date, clustered by data_source, channel_group, metric_name. Refreshed weekly via sp_sdi_pulseTms_silver_mfcSpend_weekly.'
  )
  AS
  WITH
  BronzeWithCalendar AS (
    SELECT
      cal.qgp_date, cal.week_type, cal.quarter, cal.days_in_period,
      cal.is_complete_period, cal.is_current_quarter,
      cal.wow_prior_qgp_date, cal.prior_year_qgp_date,
      cal.boundary_stub_date, cal.iso_week_number, cal.iso_year,
      channels.lob, channels.channel_group, b.channel, b.tactic, b.message_type, b.agency,
      IF(cal.is_complete_period, b.spend_actual,   NULL) AS spend_actual,
      IF(cal.is_complete_period, b.spend_forecast, NULL) AS spend_forecast,
      b.file_load_date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar` cal
    CROSS JOIN (
      SELECT DISTINCT lob, channel_group
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_bronze_mfcSpend_weekly`
    ) channels
    LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_bronze_mfcSpend_weekly` b
      ON b.qgp_week = cal.qgp_date
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
  UnpivotedGranular AS (
    SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, lob, channel_group, channel, tactic, message_type, agency, 'mfcSpendActual' AS metric_name, spend_actual AS metric_value FROM BronzeWithCalendar WHERE lob IS NOT NULL
    UNION ALL SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, lob, channel_group, channel, tactic, message_type, agency, 'mfcSpendForecast', spend_forecast FROM BronzeWithCalendar WHERE lob IS NOT NULL
  ),
  UnpivotedChannelBase AS (
    SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'mfcSpendActual' AS metric_name, SUM(spend_actual) AS metric_value FROM BronzeWithCalendar WHERE lob = 'CONSUMER POSTPAID' GROUP BY qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group
    UNION ALL SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group, 'mfcSpendForecast', SUM(spend_forecast) FROM BronzeWithCalendar WHERE lob = 'CONSUMER POSTPAID' GROUP BY qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, channel_group
  ),
  UnpivotedChannelAllChannels AS (
    SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, 'All Channels' AS channel_group, metric_name, SUM(metric_value) AS metric_value FROM UnpivotedChannelBase GROUP BY qgp_date, week_type, quarter, days_in_period, is_complete_period, is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date, boundary_stub_date, iso_week_number, iso_year, metric_name
  ),
  UnpivotedChannel AS (
    SELECT * FROM UnpivotedChannelBase
    UNION ALL SELECT * FROM UnpivotedChannelAllChannels
  ),
  MetricLookupGranular AS (SELECT qgp_date, lob, channel_group, channel, tactic, message_type, agency, metric_name, metric_value FROM UnpivotedGranular),
  MetricLookupChannel  AS (SELECT qgp_date, channel_group, metric_name, metric_value FROM UnpivotedChannel),
  ChannelWithWowYoy AS (
    SELECT
      u.qgp_date, u.week_type, u.quarter, u.days_in_period, u.channel_group, u.metric_name, u.metric_value,
      ly_lookup.metric_value AS metric_value_ly,
      CASE u.week_type WHEN 'BOUNDARY_STUB' THEN NULL WHEN 'BOUNDARY_FIRST' THEN u.metric_value + stub_lookup.metric_value ELSE u.metric_value END AS wow_numerator,
      CASE WHEN u.metric_value IS NULL THEN NULL WHEN u.week_type = 'BOUNDARY_STUB' THEN NULL ELSE wow_prior_lookup.metric_value END AS wow_denominator,
      CASE u.week_type WHEN 'BOUNDARY_STUB' THEN NULL WHEN 'BOUNDARY_FIRST' THEN u.metric_value + stub_lookup.metric_value ELSE u.metric_value END AS yoy_numerator,
      CASE WHEN u.metric_value IS NULL THEN NULL WHEN u.week_type = 'BOUNDARY_STUB' THEN NULL WHEN u.week_type = 'BOUNDARY_FIRST' THEN yoy_bf_lookup.metric_value + yoy_stub_lookup.metric_value ELSE ly_lookup.metric_value END AS yoy_denominator,
      'CONSUMER POSTPAID' AS lob_mfc, CAST(NULL AS STRING) AS channel, CAST(NULL AS STRING) AS tactic, CAST(NULL AS STRING) AS message_type, CAST(NULL AS STRING) AS agency
    FROM UnpivotedChannel u
    LEFT JOIN MetricLookupChannel wow_prior_lookup ON wow_prior_lookup.qgp_date = u.wow_prior_qgp_date AND wow_prior_lookup.channel_group = u.channel_group AND wow_prior_lookup.metric_name = u.metric_name
    LEFT JOIN MetricLookupChannel ly_lookup ON ly_lookup.qgp_date = u.prior_year_qgp_date AND ly_lookup.channel_group = u.channel_group AND ly_lookup.metric_name = u.metric_name
    LEFT JOIN MetricLookupChannel stub_lookup ON stub_lookup.qgp_date = u.boundary_stub_date AND stub_lookup.channel_group = u.channel_group AND stub_lookup.metric_name = u.metric_name
    LEFT JOIN MetricLookupChannel yoy_bf_lookup ON yoy_bf_lookup.qgp_date = u.prior_year_qgp_date AND yoy_bf_lookup.channel_group = u.channel_group AND yoy_bf_lookup.metric_name = u.metric_name
    LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar` ly_cal ON ly_cal.qgp_date = u.prior_year_qgp_date
    LEFT JOIN MetricLookupChannel yoy_stub_lookup ON yoy_stub_lookup.qgp_date = ly_cal.boundary_stub_date AND yoy_stub_lookup.channel_group = u.channel_group AND yoy_stub_lookup.metric_name = u.metric_name
  ),
  GranularWithWowYoy AS (
    SELECT
      u.qgp_date, u.week_type, u.quarter, u.days_in_period, u.channel_group, u.metric_name, u.metric_value,
      ly_lookup.metric_value AS metric_value_ly,
      CASE u.week_type WHEN 'BOUNDARY_STUB' THEN NULL WHEN 'BOUNDARY_FIRST' THEN u.metric_value + stub_lookup.metric_value ELSE u.metric_value END AS wow_numerator,
      CASE WHEN u.metric_value IS NULL THEN NULL WHEN u.week_type = 'BOUNDARY_STUB' THEN NULL ELSE wow_prior_lookup.metric_value END AS wow_denominator,
      CASE u.week_type WHEN 'BOUNDARY_STUB' THEN NULL WHEN 'BOUNDARY_FIRST' THEN u.metric_value + stub_lookup.metric_value ELSE u.metric_value END AS yoy_numerator,
      CASE WHEN u.metric_value IS NULL THEN NULL WHEN u.week_type = 'BOUNDARY_STUB' THEN NULL WHEN u.week_type = 'BOUNDARY_FIRST' THEN yoy_bf_lookup.metric_value + yoy_stub_lookup.metric_value ELSE ly_lookup.metric_value END AS yoy_denominator,
      u.lob AS lob_mfc, u.channel, u.tactic, u.message_type, u.agency
    FROM UnpivotedGranular u
    LEFT JOIN MetricLookupGranular wow_prior_lookup ON wow_prior_lookup.qgp_date = u.wow_prior_qgp_date AND wow_prior_lookup.lob = u.lob AND wow_prior_lookup.channel_group = u.channel_group AND wow_prior_lookup.channel = u.channel AND wow_prior_lookup.tactic = u.tactic AND wow_prior_lookup.message_type = u.message_type AND wow_prior_lookup.agency = u.agency AND wow_prior_lookup.metric_name = u.metric_name
    LEFT JOIN MetricLookupGranular ly_lookup ON ly_lookup.qgp_date = u.prior_year_qgp_date AND ly_lookup.lob = u.lob AND ly_lookup.channel_group = u.channel_group AND ly_lookup.channel = u.channel AND ly_lookup.tactic = u.tactic AND ly_lookup.message_type = u.message_type AND ly_lookup.agency = u.agency AND ly_lookup.metric_name = u.metric_name
    LEFT JOIN MetricLookupGranular stub_lookup ON stub_lookup.qgp_date = u.boundary_stub_date AND stub_lookup.lob = u.lob AND stub_lookup.channel_group = u.channel_group AND stub_lookup.channel = u.channel AND stub_lookup.tactic = u.tactic AND stub_lookup.message_type = u.message_type AND stub_lookup.agency = u.agency AND stub_lookup.metric_name = u.metric_name
    LEFT JOIN MetricLookupGranular yoy_bf_lookup ON yoy_bf_lookup.qgp_date = u.prior_year_qgp_date AND yoy_bf_lookup.lob = u.lob AND yoy_bf_lookup.channel_group = u.channel_group AND yoy_bf_lookup.channel = u.channel AND yoy_bf_lookup.tactic = u.tactic AND yoy_bf_lookup.message_type = u.message_type AND yoy_bf_lookup.agency = u.agency AND yoy_bf_lookup.metric_name = u.metric_name
    LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar` ly_cal ON ly_cal.qgp_date = u.prior_year_qgp_date
    LEFT JOIN MetricLookupGranular yoy_stub_lookup ON yoy_stub_lookup.qgp_date = ly_cal.boundary_stub_date AND yoy_stub_lookup.lob = u.lob AND yoy_stub_lookup.channel_group = u.channel_group AND yoy_stub_lookup.channel = u.channel AND yoy_stub_lookup.tactic = u.tactic AND yoy_stub_lookup.message_type = u.message_type AND yoy_stub_lookup.agency = u.agency AND yoy_stub_lookup.metric_name = u.metric_name
  ),
  Combined AS (
    SELECT 'MFC_SPEND_CHANNEL'  AS data_source, * FROM ChannelWithWowYoy
    UNION ALL
    SELECT 'MFC_SPEND_GRANULAR' AS data_source, * FROM GranularWithWowYoy
  )
  SELECT
    data_source, qgp_date, week_type, quarter, days_in_period, channel_group, metric_name,
    metric_value, metric_value_ly, wow_numerator, wow_denominator,
    CASE WHEN wow_denominator IS NULL OR wow_denominator = 0 THEN NULL ELSE wow_numerator / wow_denominator - 1 END AS wow_pct,
    yoy_numerator, yoy_denominator,
    CASE WHEN yoy_denominator IS NULL OR yoy_denominator = 0 THEN NULL ELSE yoy_numerator / yoy_denominator - 1 END AS yoy_pct,
    MAX(CASE WHEN metric_value IS NOT NULL THEN qgp_date END) OVER (PARTITION BY data_source, metric_name) AS max_date,
    lob_mfc, channel, tactic, message_type, agency
  FROM Combined;

END;