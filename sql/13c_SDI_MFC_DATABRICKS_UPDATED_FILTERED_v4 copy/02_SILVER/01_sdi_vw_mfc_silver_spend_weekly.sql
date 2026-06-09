-- ============================================================
-- MFC SPEND PIPELINE: SILVER 1 - NON-GRANULAR / LOB-LEVEL
-- Stored Procedure — creates/refreshes physical table
-- prdrzranalytics.lab42.sdi_mfc_silver_spend_weekly
-- ============================================================

CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_mfc_silver_spend_weekly()
COMMENT 'Creates/refreshes sdi_mfc_silver_spend_weekly. LOB-level spend with PulseTMS-style WoW. Refreshed weekly.'
BEGIN

  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_mfc_silver_spend_weekly
  USING DELTA
  COMMENT 'MFC Silver — LOB-level spend with WoW. One row per QGP_Week x LOB_Supported. Refreshed via sdi_sp_mfc_silver_spend_weekly.'
  AS

  WITH

  actual_clean AS (
    SELECT
      Quarter, Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, FileLoad_Date,
      UPPER(TRIM(LOB_Supported)) AS LOB_Supported,
      weekly_actual,
      UPPER(TRIM(week_type))     AS source_week_type
    FROM prdrzranalytics.lab42.sdi_mfc_bronze_spendActuals_weekly
  ),

  forecast_clean AS (
    SELECT
      Quarter, Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, FileLoad_Date,
      UPPER(TRIM(LOB_Supported)) AS LOB_Supported,
      weekly_forecast,
      UPPER(TRIM(week_type))     AS source_week_type
    FROM prdrzranalytics.lab42.sdi_mfc_bronze_spendForecasts_weekly
  ),

  source_joined AS (
    SELECT
      COALESCE(a.Quarter, f.Quarter)                           AS Source_Quarter,
      COALESCE(a.Week_Beginning_Monday, f.Week_Beginning_Monday) AS Week_Beginning_Monday,
      COALESCE(a.Week_Ending_Sunday,    f.Week_Ending_Sunday)    AS Week_Ending_Sunday,
      COALESCE(a.QGP_Week, f.QGP_Week)                         AS Source_QGP_Week,
      COALESCE(GREATEST(a.FileLoad_Date, f.FileLoad_Date),
               a.FileLoad_Date, f.FileLoad_Date)               AS FileLoad_Date,
      COALESCE(a.LOB_Supported, f.LOB_Supported)               AS LOB_Supported,
      a.weekly_actual,
      f.weekly_forecast,
      COALESCE(NULLIF(a.weekly_actual, 0),
               NULLIF(f.weekly_forecast, 0))                   AS weekly_display,
      UPPER(TRIM(COALESCE(a.source_week_type, f.source_week_type))) AS source_week_type
    FROM actual_clean a
    FULL OUTER JOIN forecast_clean f
      ON  a.Quarter       = f.Quarter
      AND a.QGP_Week      = f.QGP_Week
      AND a.LOB_Supported = f.LOB_Supported
  ),

  lob_universe AS (
    SELECT DISTINCT LOB_Supported FROM source_joined WHERE Source_QGP_Week IS NOT NULL
  ),

  spine AS (
    SELECT
      cal.qgp_date, cal.week_type, cal.quarter, cal.days_in_period,
      cal.is_complete_period, cal.is_current_quarter,
      cal.wow_prior_qgp_date, cal.prior_year_qgp_date,
      cal.boundary_stub_date, cal.iso_week_number, cal.iso_year,
      cal.quarter_end_date,
      lob.LOB_Supported
    FROM prdrzranalytics.lab42.sdi_vw_mfc_dim_qgp_calendar cal
    CROSS JOIN lob_universe lob
    WHERE cal.qgp_date >= (SELECT MIN(Source_QGP_Week) FROM source_joined WHERE Source_QGP_Week IS NOT NULL)
      AND cal.qgp_date <= (SELECT MAX(Source_QGP_Week) FROM source_joined WHERE Source_QGP_Week IS NOT NULL)
  ),

  aggregated AS (
    SELECT
      Source_QGP_Week AS QGP_Week,
      LOB_Supported,
      MAX(FileLoad_Date)                           AS FileLoad_Date,
      NULLIF(SUM(COALESCE(weekly_actual, 0)), 0)   AS spend_actual,
      NULLIF(SUM(COALESCE(weekly_forecast, 0)), 0) AS spend_forecast,
      NULLIF(SUM(COALESCE(weekly_display, 0)), 0)  AS spend_display
    FROM source_joined
    WHERE Source_QGP_Week IS NOT NULL
    GROUP BY Source_QGP_Week, LOB_Supported
  ),

  dense_spend AS (
    SELECT
      s.qgp_date, s.week_type, s.quarter, s.days_in_period,
      s.is_complete_period, s.is_current_quarter,
      s.wow_prior_qgp_date, s.prior_year_qgp_date,
      s.boundary_stub_date, s.iso_week_number, s.iso_year,
      s.quarter_end_date, s.LOB_Supported,
      COALESCE(a.FileLoad_Date,
        MAX(a.FileLoad_Date) OVER (PARTITION BY s.LOB_Supported)) AS FileLoad_Date,
      CASE WHEN s.is_complete_period THEN a.spend_actual   ELSE NULL END AS spend_actual,
      a.spend_forecast,
      CASE WHEN s.is_complete_period THEN a.spend_display
           ELSE a.spend_forecast END AS spend_display
    FROM spine s
    LEFT JOIN aggregated a
      ON a.QGP_Week = s.qgp_date AND a.LOB_Supported = s.LOB_Supported
  ),

  metric_lookup AS (
    SELECT qgp_date, LOB_Supported, spend_actual FROM dense_spend
  ),

  with_spend_for_wow AS (
    SELECT
      d.*,
      CASE
        WHEN d.week_type = 'BOUNDARY_STUB'  THEN NULL
        WHEN d.week_type = 'BOUNDARY_FIRST'
          THEN COALESCE(d.spend_actual, 0) + COALESCE(stub_lkp.spend_actual, 0)
        WHEN d.spend_actual IS NULL THEN NULL
        ELSE d.spend_actual
      END AS spend_actual_for_wow,
      CASE
        WHEN d.week_type = 'BOUNDARY_STUB' THEN NULL
        WHEN prior_stub_lkp.spend_actual IS NOT NULL
          THEN COALESCE(prior_lkp.spend_actual, 0) + COALESCE(prior_stub_lkp.spend_actual, 0)
        ELSE COALESCE(prior_lkp.spend_actual, 0)
      END AS spend_for_wow
    FROM dense_spend d
    LEFT JOIN metric_lookup stub_lkp
      ON stub_lkp.qgp_date = d.boundary_stub_date
      AND stub_lkp.LOB_Supported = d.LOB_Supported
    LEFT JOIN metric_lookup prior_lkp
      ON prior_lkp.qgp_date = d.wow_prior_qgp_date
      AND prior_lkp.LOB_Supported = d.LOB_Supported
    LEFT JOIN prdrzranalytics.lab42.sdi_vw_mfc_dim_qgp_calendar prior_cal
      ON prior_cal.qgp_date = d.wow_prior_qgp_date
    LEFT JOIN metric_lookup prior_stub_lkp
      ON prior_stub_lkp.qgp_date = prior_cal.boundary_stub_date
      AND prior_stub_lkp.LOB_Supported = d.LOB_Supported
  )

  SELECT
    quarter                                          AS Quarter,
    TO_DATE(DATE_TRUNC('quarter', qgp_date))         AS Quarter_Start_Date,
    quarter_end_date                                 AS Quarter_End_Date,
    DATE_SUB(qgp_date, days_in_period - 1)           AS Period_Start,
    qgp_date                                         AS Period_End,
    qgp_date                                         AS QGP_Week,
    FileLoad_Date,
    LOB_Supported,
    spend_actual     AS weekly_actual,
    spend_forecast   AS weekly_forecast,
    spend_display    AS weekly_display,
    spend_actual,
    spend_forecast,
    spend_display,
    spend_actual_for_wow,
    spend_for_wow,
    CASE
      WHEN spend_actual_for_wow IS NOT NULL AND spend_for_wow IS NOT NULL AND spend_for_wow != 0
      THEN ROUND(((spend_actual_for_wow - spend_for_wow) / spend_for_wow) * 100, 2)
      ELSE NULL
    END                                              AS spend_actual_wow_pct,
    CASE WHEN week_type IN ('BOUNDARY_STUB', 'BOUNDARY_FIRST') THEN 'BOUNDARY_WEEK'
         ELSE 'NORMAL' END                           AS week_type,
    CASE WHEN days_in_period < 7 THEN 'Partial' ELSE 'Full' END AS is_partial_week
  FROM with_spend_for_wow;

END;