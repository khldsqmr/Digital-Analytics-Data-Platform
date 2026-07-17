-- ============================================================
-- MFC SPEND PIPELINE: SILVER 1 - NON-GRANULAR / LOB-LEVEL — BigQuery
-- Converted from Databricks sdi_sp_mfc_silver_spend_weekly
--
-- Logic:
--   1. Join Bronze Actuals + Forecasts tables (kept per-Quarter row
--      so boundary weeks split correctly in daily disaggregation)
--   2. Derive Source_Quarter_Start/End dates from the Quarter string
--      (e.g. "Q1'26") — NOT from Week_Beginning_Monday
--   3. Prorate spend to the portion within each quarter
--   4. Explode to daily via GENERATE_DATE_ARRAY
--   5. Map each day to its QGP_Week (next Saturday or quarter-end)
--   6. Aggregate daily back to QGP_Week x LOB
--   7. For boundary weeks: recalculate using combined total / 7 x days_in_period
--      Both non-NULL : total = stub + first, allocate by days_in_period
--      One NULL      : that QGP date stays NULL, other gets value / 7 x days_in_period
--   8. Build full spine from QGP calendar x LOB universe
--   9. Join aggregated spend onto spine (dense fill)
--  10. Compute WoW
--
-- CHANGE LOG:
--   - Boundary week allocation updated: sum Q_prev + Q_next then allocate
--     by days_in_period / 7. NULL handling: if one side is NULL, that QGP
--     date stays NULL; the other uses its own value / 7 x days_in_period.
-- ============================================================
CREATE OR REPLACE PROCEDURE
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_sp_mfc_silver_spend_weekly`()
OPTIONS (strict_mode = false)
BEGIN

  CREATE OR REPLACE TABLE
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_mfc_silver_spend_weekly`
  OPTIONS (
    description = 'MFC Silver — LOB-level spend with WoW. One row per QGP_Week x LOB_Supported. Boundary weeks allocated as (stub + first) / 7 x days_in_period. Refreshed via sdi_sp_mfc_silver_spend_weekly.'
  )
  AS

  WITH

  actual_clean AS (
    SELECT
      Quarter,
      Week_Beginning_Monday,
      Week_Ending_Sunday,
      QGP_Week,
      FileLoad_Date,
      UPPER(TRIM(LOB_Supported))      AS LOB_Supported,
      weekly_actual,
      UPPER(TRIM(week_type))          AS source_week_type
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_mfc_bronze_spendActuals_weekly`
    WHERE Week_Beginning_Monday IS NOT NULL
      AND Week_Ending_Sunday    IS NOT NULL
  ),

  forecast_clean AS (
    SELECT
      Quarter,
      Week_Beginning_Monday,
      Week_Ending_Sunday,
      QGP_Week,
      FileLoad_Date,
      UPPER(TRIM(LOB_Supported))      AS LOB_Supported,
      weekly_forecast,
      UPPER(TRIM(week_type))          AS source_week_type
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_mfc_bronze_spendForecasts_weekly`
    WHERE Week_Beginning_Monday IS NOT NULL
      AND Week_Ending_Sunday    IS NOT NULL
  ),

  source_joined AS (
    SELECT
      COALESCE(a.Quarter,               f.Quarter)               AS Source_Quarter,
      COALESCE(a.Week_Beginning_Monday, f.Week_Beginning_Monday) AS Week_Beginning_Monday,
      COALESCE(a.Week_Ending_Sunday,    f.Week_Ending_Sunday)    AS Week_Ending_Sunday,
      COALESCE(a.QGP_Week,              f.QGP_Week)              AS Source_QGP_Week,
      (
        SELECT MAX(d)
        FROM UNNEST([a.FileLoad_Date, f.FileLoad_Date]) AS d
      )                                                          AS FileLoad_Date,
      COALESCE(a.LOB_Supported, f.LOB_Supported)                 AS LOB_Supported,
      a.weekly_actual,
      f.weekly_forecast,
      COALESCE(
        IF(a.weekly_actual != 0, a.weekly_actual, NULL),
        f.weekly_forecast
      )                                                          AS weekly_display,
      UPPER(TRIM(COALESCE(a.source_week_type, f.source_week_type))) AS source_week_type
    FROM actual_clean a
    FULL OUTER JOIN forecast_clean f
      ON  a.Quarter       = f.Quarter
      AND a.QGP_Week      = f.QGP_Week
      AND a.LOB_Supported = f.LOB_Supported
  ),

  with_source_quarter_bounds AS (
    SELECT
      *,
      CAST(SUBSTR(Source_Quarter, 2, 1) AS INT64) AS qtr_num,
      CAST(SUBSTR(Source_Quarter, 4, 2) AS INT64) AS qtr_year_2digit
    FROM source_joined
    WHERE Week_Beginning_Monday IS NOT NULL
      AND Week_Ending_Sunday    IS NOT NULL
  ),

  with_bounds AS (
    SELECT
      *,
      DATE(CAST(CONCAT(
        CASE WHEN qtr_year_2digit < 50 THEN '20' ELSE '19' END,
        LPAD(CAST(qtr_year_2digit AS STRING), 2, '0'), '-',
        LPAD(CAST(((qtr_num - 1) * 3) + 1 AS STRING), 2, '0'), '-01'
      ) AS DATE)) AS Source_Quarter_Start_Date,
      DATE_SUB(DATE_ADD(
        DATE(CAST(CONCAT(
          CASE WHEN qtr_year_2digit < 50 THEN '20' ELSE '19' END,
          LPAD(CAST(qtr_year_2digit AS STRING), 2, '0'), '-',
          LPAD(CAST(qtr_num * 3 AS STRING), 2, '0'), '-01'
        ) AS DATE)),
        INTERVAL 1 MONTH), INTERVAL 1 DAY
      ) AS Source_Quarter_End_Date
    FROM with_source_quarter_bounds
  ),

  source_periods AS (
    SELECT
      *,
      IF(Week_Beginning_Monday > Source_Quarter_Start_Date,
         Week_Beginning_Monday, Source_Quarter_Start_Date)       AS Period_Start,
      IF(Week_Ending_Sunday    < Source_Quarter_End_Date,
         Week_Ending_Sunday,    Source_Quarter_End_Date)         AS Period_End,
      DATE_DIFF(Week_Ending_Sunday, Week_Beginning_Monday, DAY) + 1 AS source_total_week_days
    FROM with_bounds
  ),

  normalized_weekly AS (
    SELECT
      FileLoad_Date,
      LOB_Supported,
      Period_Start,
      Period_End,
      Source_QGP_Week,
      CASE
        WHEN source_week_type != 'BOUNDARY_WEEK'
          AND Week_Ending_Sunday > Source_Quarter_End_Date
          THEN ROUND(weekly_actual
                 * (DATE_DIFF(Source_Quarter_End_Date, Week_Beginning_Monday, DAY) + 1)
                 / source_total_week_days, 2)
        ELSE weekly_actual
      END AS weekly_actual,
      CASE
        WHEN source_week_type != 'BOUNDARY_WEEK'
          AND Week_Ending_Sunday > Source_Quarter_End_Date
          THEN ROUND(weekly_forecast
                 * (DATE_DIFF(Source_Quarter_End_Date, Week_Beginning_Monday, DAY) + 1)
                 / source_total_week_days, 2)
        ELSE weekly_forecast
      END AS weekly_forecast,
      CASE
        WHEN source_week_type != 'BOUNDARY_WEEK'
          AND Week_Ending_Sunday > Source_Quarter_End_Date
          THEN ROUND(weekly_display
                 * (DATE_DIFF(Source_Quarter_End_Date, Week_Beginning_Monday, DAY) + 1)
                 / source_total_week_days, 2)
        ELSE weekly_display
      END AS weekly_display
    FROM source_periods
    WHERE Period_End >= Period_Start
  ),

  daily_spend AS (
    SELECT
      FileLoad_Date,
      LOB_Supported,
      Source_QGP_Week,
      weekly_actual   / (DATE_DIFF(Period_End, Period_Start, DAY) + 1) AS daily_actual,
      weekly_forecast / (DATE_DIFF(Period_End, Period_Start, DAY) + 1) AS daily_forecast,
      weekly_display  / (DATE_DIFF(Period_End, Period_Start, DAY) + 1) AS daily_display,
      calendar_date
    FROM normalized_weekly,
    UNNEST(GENERATE_DATE_ARRAY(Period_Start, Period_End)) AS calendar_date
  ),

  daily_with_qgp AS (
    SELECT
      FileLoad_Date,
      LOB_Supported,
      calendar_date,
      daily_actual,
      daily_forecast,
      daily_display,
      LEAST(
        CASE
          WHEN DATE_ADD(
                 calendar_date,
                 INTERVAL MOD(7 - EXTRACT(DAYOFWEEK FROM calendar_date) + 7, 7) DAY
               ) > DATE_SUB(
                     DATE_ADD(DATE_TRUNC(calendar_date, QUARTER), INTERVAL 3 MONTH),
                     INTERVAL 1 DAY
                   )
            THEN DATE_SUB(
                   DATE_ADD(DATE_TRUNC(calendar_date, QUARTER), INTERVAL 3 MONTH),
                   INTERVAL 1 DAY
                 )
          ELSE DATE_ADD(
                 calendar_date,
                 INTERVAL MOD(7 - EXTRACT(DAYOFWEEK FROM calendar_date) + 7, 7) DAY
               )
        END,
        Source_QGP_Week
      ) AS QGP_Week
    FROM daily_spend
  ),

  aggregated AS (
    SELECT
      QGP_Week,
      LOB_Supported,
      MAX(FileLoad_Date)                           AS FileLoad_Date,
      NULLIF(ROUND(SUM(daily_actual),   2), 0)     AS spend_actual,
      NULLIF(ROUND(SUM(daily_forecast), 2), 0)     AS spend_forecast,
      NULLIF(ROUND(SUM(daily_display),  2), 0)     AS spend_display
    FROM daily_with_qgp
    GROUP BY QGP_Week, LOB_Supported
  ),

  -- ===========================================================================
  -- NEW: Boundary week reallocation
  --
  -- Pull stub + first pairs from QGP calendar, join to aggregated values,
  -- then reallocate using: total / 7 x days_in_period per QGP date.
  --
  -- NULL rules:
  --   Both non-NULL : stub_new = (stub + first) / 7 x stub_days
  --                  first_new = (stub + first) / 7 x first_days
  --   Stub NULL     : stub_new = NULL
  --                  first_new = first / 7 x first_days
  --   First NULL    : stub_new = stub / 7 x stub_days
  --                  first_new = NULL
  --   Both NULL     : both stay NULL
  -- ===========================================================================
  boundary_pairs AS (
    SELECT
      stub_cal.qgp_date                             AS stub_date,
      first_cal.qgp_date                            AS first_date,
      stub_cal.days_in_period                       AS stub_days,
      first_cal.days_in_period                      AS first_days,
      lobs.LOB_Supported,
      GREATEST(
        COALESCE(a_stub.FileLoad_Date, DATE '2000-01-01'),
        COALESCE(a_first.FileLoad_Date, DATE '2000-01-01')
      )                                             AS FileLoad_Date,
      a_stub.spend_actual                           AS stub_actual,
      a_first.spend_actual                          AS first_actual,
      a_stub.spend_forecast                         AS stub_forecast,
      a_first.spend_forecast                        AS first_forecast,
      a_stub.spend_display                          AS stub_display,
      a_first.spend_display                         AS first_display
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_dim_qgp_calendar` stub_cal
    JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_dim_qgp_calendar` first_cal
      ON  first_cal.boundary_stub_date = stub_cal.qgp_date
      AND stub_cal.week_type           = 'BOUNDARY_STUB'
      AND first_cal.week_type          = 'BOUNDARY_FIRST'
    JOIN (SELECT DISTINCT LOB_Supported FROM aggregated) lobs
      ON  TRUE
    LEFT JOIN aggregated a_stub
      ON  a_stub.QGP_Week      = stub_cal.qgp_date
      AND a_stub.LOB_Supported = lobs.LOB_Supported
    LEFT JOIN aggregated a_first
      ON  a_first.QGP_Week      = first_cal.qgp_date
      AND a_first.LOB_Supported = lobs.LOB_Supported
    WHERE a_stub.QGP_Week IS NOT NULL
       OR a_first.QGP_Week IS NOT NULL
  ),

  boundary_reallocated AS (
    -- Stub rows with new values
    SELECT
      stub_date    AS QGP_Week,
      LOB_Supported,
      FileLoad_Date,
      NULLIF(CASE
        WHEN stub_actual IS NULL THEN NULL
        WHEN first_actual IS NULL THEN ROUND(stub_actual / 7 * stub_days, 2)
        ELSE ROUND((stub_actual + first_actual) / 7 * stub_days, 2)
      END, 0)      AS spend_actual,
      NULLIF(CASE
        WHEN stub_forecast IS NULL THEN NULL
        WHEN first_forecast IS NULL THEN ROUND(stub_forecast / 7 * stub_days, 2)
        ELSE ROUND((stub_forecast + first_forecast) / 7 * stub_days, 2)
      END, 0)      AS spend_forecast,
      NULLIF(CASE
        WHEN stub_display IS NULL THEN NULL
        WHEN first_display IS NULL THEN ROUND(stub_display / 7 * stub_days, 2)
        ELSE ROUND((stub_display + first_display) / 7 * stub_days, 2)
      END, 0)      AS spend_display
    FROM boundary_pairs

    UNION ALL

    -- First rows with new values
    SELECT
      first_date   AS QGP_Week,
      LOB_Supported,
      FileLoad_Date,
      NULLIF(CASE
        WHEN first_actual IS NULL THEN NULL
        WHEN stub_actual IS NULL THEN ROUND(first_actual / 7 * first_days, 2)
        ELSE ROUND((stub_actual + first_actual) / 7 * first_days, 2)
      END, 0)      AS spend_actual,
      NULLIF(CASE
        WHEN first_forecast IS NULL THEN NULL
        WHEN stub_forecast IS NULL THEN ROUND(first_forecast / 7 * first_days, 2)
        ELSE ROUND((stub_forecast + first_forecast) / 7 * first_days, 2)
      END, 0)      AS spend_forecast,
      NULLIF(CASE
        WHEN first_display IS NULL THEN NULL
        WHEN stub_display IS NULL THEN ROUND(first_display / 7 * first_days, 2)
        ELSE ROUND((stub_display + first_display) / 7 * first_days, 2)
      END, 0)      AS spend_display
    FROM boundary_pairs
  ),

  -- Replace boundary week values; non-boundary weeks pass through unchanged
  aggregated_final AS (
    SELECT
      a.QGP_Week,
      a.LOB_Supported,
      COALESCE(br.FileLoad_Date, a.FileLoad_Date)  AS FileLoad_Date,
      CASE
        WHEN bc.week_type IN ('BOUNDARY_STUB', 'BOUNDARY_FIRST')
          THEN br.spend_actual
        ELSE a.spend_actual
      END AS spend_actual,
      CASE
        WHEN bc.week_type IN ('BOUNDARY_STUB', 'BOUNDARY_FIRST')
          THEN br.spend_forecast
        ELSE a.spend_forecast
      END AS spend_forecast,
      CASE
        WHEN bc.week_type IN ('BOUNDARY_STUB', 'BOUNDARY_FIRST')
          THEN br.spend_display
        ELSE a.spend_display
      END AS spend_display
    FROM aggregated a
    LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_dim_qgp_calendar` bc
      ON  bc.qgp_date = a.QGP_Week
    LEFT JOIN boundary_reallocated br
      ON  br.QGP_Week      = a.QGP_Week
      AND br.LOB_Supported = a.LOB_Supported
  ),

  lob_universe AS (
    SELECT DISTINCT LOB_Supported FROM aggregated_final
  ),

  spine AS (
    SELECT
      cal.qgp_date,
      cal.week_type,
      cal.quarter,
      cal.days_in_period,
      cal.is_complete_period,
      cal.is_current_quarter,
      cal.wow_prior_qgp_date,
      cal.prior_year_qgp_date,
      cal.boundary_stub_date,
      cal.iso_week_number,
      cal.iso_year,
      cal.quarter_end_date,
      lob.LOB_Supported
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_dim_qgp_calendar` cal
    CROSS JOIN lob_universe lob
    WHERE cal.qgp_date >= (SELECT MIN(QGP_Week) FROM aggregated_final)
      AND cal.qgp_date <= (SELECT MAX(QGP_Week) FROM aggregated_final)
  ),

  dense_spend AS (
    SELECT
      s.qgp_date,
      s.week_type,
      s.quarter,
      s.days_in_period,
      s.is_complete_period,
      s.is_current_quarter,
      s.wow_prior_qgp_date,
      s.prior_year_qgp_date,
      s.boundary_stub_date,
      s.iso_week_number,
      s.iso_year,
      s.quarter_end_date,
      s.LOB_Supported,
      COALESCE(
        a.FileLoad_Date,
        MAX(a.FileLoad_Date) OVER (PARTITION BY s.LOB_Supported)
      )                                              AS FileLoad_Date,
      IF(s.is_complete_period, a.spend_actual,  NULL) AS spend_actual,
      a.spend_forecast,
      IF(s.is_complete_period, a.spend_display, a.spend_forecast) AS spend_display
    FROM spine s
    LEFT JOIN aggregated_final a
      ON  a.QGP_Week      = s.qgp_date
      AND a.LOB_Supported = s.LOB_Supported
  ),

  metric_lookup AS (
    SELECT qgp_date, LOB_Supported, spend_actual
    FROM dense_spend
  ),

  with_spend_for_wow AS (
    SELECT
      d.*,
      CASE
        WHEN d.week_type = 'BOUNDARY_STUB'  THEN NULL
        WHEN d.week_type = 'BOUNDARY_FIRST'
          THEN COALESCE(d.spend_actual, 0) + COALESCE(stub_lkp.spend_actual, 0)
        WHEN d.spend_actual IS NULL         THEN NULL
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
      ON  stub_lkp.qgp_date      = d.boundary_stub_date
      AND stub_lkp.LOB_Supported = d.LOB_Supported
    LEFT JOIN metric_lookup prior_lkp
      ON  prior_lkp.qgp_date      = d.wow_prior_qgp_date
      AND prior_lkp.LOB_Supported = d.LOB_Supported
    LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_dim_qgp_calendar` prior_cal
      ON  prior_cal.qgp_date = d.wow_prior_qgp_date
    LEFT JOIN metric_lookup prior_stub_lkp
      ON  prior_stub_lkp.qgp_date      = prior_cal.boundary_stub_date
      AND prior_stub_lkp.LOB_Supported = d.LOB_Supported
  )

  SELECT
    quarter                                                          AS Quarter,
    DATE_TRUNC(qgp_date, QUARTER)                                    AS Quarter_Start_Date,
    quarter_end_date                                                 AS Quarter_End_Date,
    DATE_SUB(qgp_date, INTERVAL (days_in_period - 1) DAY)            AS Period_Start,
    qgp_date                                                         AS Period_End,
    qgp_date                                                         AS QGP_Week,
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
      WHEN spend_actual_for_wow IS NOT NULL
        AND spend_for_wow       IS NOT NULL
        AND spend_for_wow       != 0
        THEN ROUND(((spend_actual_for_wow - spend_for_wow) / spend_for_wow) * 100, 2)
      ELSE NULL
    END                                                              AS spend_actual_wow_pct,
    CASE
      WHEN week_type IN ('BOUNDARY_STUB', 'BOUNDARY_FIRST') THEN 'BOUNDARY_WEEK'
      ELSE 'NORMAL'
    END                                                              AS week_type,
    CASE
      WHEN days_in_period < 7 THEN 'Partial'
      ELSE 'Full'
    END                                                              AS is_partial_week
  FROM with_spend_for_wow
  ;

END;