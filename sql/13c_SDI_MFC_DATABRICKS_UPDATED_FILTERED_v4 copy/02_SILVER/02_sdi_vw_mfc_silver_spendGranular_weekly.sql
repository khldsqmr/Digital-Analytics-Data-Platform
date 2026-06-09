-- ============================================================
-- MFC SPEND PIPELINE: SILVER 2 - GRANULAR
-- Stored Procedure — creates/refreshes physical table
-- prdrzranalytics.lab42.sdi_mfc_silver_spendGranular_weekly
-- ============================================================

CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_mfc_silver_spendGranular_weekly()
SQL SECURITY DEFINER
COMMENT 'Creates/refreshes sdi_mfc_silver_spendGranular_weekly. Granular spend with PulseTMS-style WoW. Refreshed weekly.'
BEGIN

  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_mfc_silver_spendGranular_weekly
  USING DELTA
  COMMENT 'MFC Silver — granular spend with WoW. One row per QGP_Week x LOB x Channel x Tactic x Message_Type x Agency. Refreshed via sdi_sp_mfc_silver_spendGranular_weekly.'
  AS

  WITH

  actual_clean AS (
    SELECT
      Quarter, Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, FileLoad_Date,
      UPPER(TRIM(LOB_Supported)) AS LOB_Supported,
      UPPER(TRIM(Channel))       AS Channel,
      UPPER(TRIM(Tactic))        AS Tactic,
      UPPER(TRIM(Message_Type))  AS Message_Type,
      TRIM(Agency)               AS Agency,
      weekly_actual,
      UPPER(TRIM(week_type))     AS source_week_type
    FROM prdrzranalytics.lab42.sdi_mfc_bronze_spendActualsGranular_weekly
  ),

  forecast_clean AS (
    SELECT
      Quarter, Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, FileLoad_Date,
      UPPER(TRIM(LOB_Supported)) AS LOB_Supported,
      UPPER(TRIM(Channel))       AS Channel,
      UPPER(TRIM(Tactic))        AS Tactic,
      UPPER(TRIM(Message_Type))  AS Message_Type,
      TRIM(Agency)               AS Agency,
      weekly_forecast,
      UPPER(TRIM(week_type))     AS source_week_type
    FROM prdrzranalytics.lab42.sdi_mfc_bronze_spendForecastsGranular_weekly
  ),

  -- ── Aggregate actuals across all quarters per QGP_Week x combination ─────────
  -- Boundary weeks have both Q1 and Q2 actuals tagged to same QGP_Week in raw.
  -- Summing them gives the full 7-day total for that calendar week.
  -- The daily disaggregation below then splits them correctly by quarter-end date.
  actual_aggregated AS (
    SELECT
      QGP_Week,
      -- Use the Week_Beginning_Monday and Week_Ending_Sunday from the row
      -- that spans the full 7 days (prefer the one where wbm < quarter end)
      MIN(Week_Beginning_Monday) AS Week_Beginning_Monday,
      MAX(Week_Ending_Sunday)    AS Week_Ending_Sunday,
      LOB_Supported, Channel, Tactic, Message_Type, Agency,
      MAX(FileLoad_Date)         AS FileLoad_Date,
      SUM(weekly_actual)         AS weekly_actual,
      MAX(source_week_type)      AS source_week_type
    FROM actual_clean
    WHERE QGP_Week IS NOT NULL
      AND Week_Beginning_Monday IS NOT NULL
      AND Week_Ending_Sunday IS NOT NULL
    GROUP BY QGP_Week, LOB_Supported, Channel, Tactic, Message_Type, Agency
  ),

  -- Forecasts: one row per QGP_Week x combination (latest FileLoad_Date)
  forecast_deduped AS (
    SELECT * FROM (
      SELECT
        Quarter, Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, FileLoad_Date,
        LOB_Supported, Channel, Tactic, Message_Type, Agency,
        weekly_forecast, source_week_type,
        ROW_NUMBER() OVER (
          PARTITION BY QGP_Week, LOB_Supported, Channel, Tactic, Message_Type, Agency
          ORDER BY FileLoad_Date DESC
        ) AS rn
      FROM forecast_clean
      WHERE QGP_Week IS NOT NULL
    )
    WHERE rn = 1
  ),

  -- ── Join actuals + forecasts ──────────────────────────────────────────────────
  source_joined AS (
    SELECT
      COALESCE(a.QGP_Week,             f.QGP_Week)             AS Source_QGP_Week,
      COALESCE(a.Week_Beginning_Monday, f.Week_Beginning_Monday) AS Week_Beginning_Monday,
      COALESCE(a.Week_Ending_Sunday,    f.Week_Ending_Sunday)    AS Week_Ending_Sunday,
      COALESCE(a.LOB_Supported,        f.LOB_Supported)         AS LOB_Supported,
      COALESCE(a.Channel,              f.Channel)               AS Channel,
      COALESCE(a.Tactic,               f.Tactic)                AS Tactic,
      COALESCE(a.Message_Type,         f.Message_Type)          AS Message_Type,
      COALESCE(a.Agency,               f.Agency)                AS Agency,
      COALESCE(GREATEST(a.FileLoad_Date, f.FileLoad_Date),
               a.FileLoad_Date, f.FileLoad_Date)                AS FileLoad_Date,
      a.weekly_actual,
      f.weekly_forecast,
      COALESCE(NULLIF(a.weekly_actual, 0),
               NULLIF(f.weekly_forecast, 0))                    AS weekly_display,
      COALESCE(a.source_week_type, f.source_week_type)          AS source_week_type
    FROM actual_aggregated a
    FULL OUTER JOIN forecast_deduped f
      ON  a.QGP_Week      = f.QGP_Week
      AND a.LOB_Supported <=> f.LOB_Supported
      AND a.Channel       <=> f.Channel
      AND a.Tactic        <=> f.Tactic
      AND a.Message_Type  <=> f.Message_Type
      AND a.Agency        <=> f.Agency
  ),

  -- ── Quarter bounds ────────────────────────────────────────────────────────────
  with_quarter_numbers AS (
    SELECT *,
      CAST(SUBSTR(Source_QGP_Week, 1, 4) AS INT)        AS qgp_year,
      QUARTER(TO_DATE(Source_QGP_Week))                  AS qgp_quarter_num
    FROM source_joined
  ),

  with_source_quarter_bounds AS (
    SELECT *,
      TO_DATE(DATE_TRUNC('quarter', TO_DATE(Source_QGP_Week))) AS Source_Quarter_Start_Date,
      LAST_DAY(ADD_MONTHS(TO_DATE(DATE_TRUNC('quarter', TO_DATE(Source_QGP_Week))), 2))
                                                               AS Source_Quarter_End_Date
    FROM with_quarter_numbers
  ),

  source_periods AS (
    SELECT *,
      GREATEST(Week_Beginning_Monday, Source_Quarter_Start_Date) AS Period_Start,
      LEAST(Week_Ending_Sunday,       Source_Quarter_End_Date)   AS Period_End,
      DATEDIFF(Week_Ending_Sunday, Week_Beginning_Monday) + 1    AS source_total_week_days
    FROM with_source_quarter_bounds
    WHERE Week_Beginning_Monday IS NOT NULL
      AND Week_Ending_Sunday IS NOT NULL
  ),

  -- ── Prorate actuals/forecasts to the portion within each quarter ──────────────
  normalized_weekly AS (
    SELECT
      FileLoad_Date, LOB_Supported, Channel, Tactic, Message_Type, Agency,
      Period_Start, Period_End, Source_QGP_Week,
      CASE WHEN source_week_type <> 'BOUNDARY_WEEK' AND Week_Ending_Sunday > Source_Quarter_End_Date
        THEN ROUND(weekly_actual   * ((DATEDIFF(Source_Quarter_End_Date, Week_Beginning_Monday) + 1) / source_total_week_days), 2)
        ELSE weekly_actual   END AS weekly_actual,
      CASE WHEN source_week_type <> 'BOUNDARY_WEEK' AND Week_Ending_Sunday > Source_Quarter_End_Date
        THEN ROUND(weekly_forecast * ((DATEDIFF(Source_Quarter_End_Date, Week_Beginning_Monday) + 1) / source_total_week_days), 2)
        ELSE weekly_forecast END AS weekly_forecast,
      CASE WHEN source_week_type <> 'BOUNDARY_WEEK' AND Week_Ending_Sunday > Source_Quarter_End_Date
        THEN ROUND(weekly_display  * ((DATEDIFF(Source_Quarter_End_Date, Week_Beginning_Monday) + 1) / source_total_week_days), 2)
        ELSE weekly_display  END AS weekly_display
    FROM source_periods
    WHERE Period_End >= Period_Start
  ),

  -- ── Explode to daily, then map to QGP_Week ────────────────────────────────────
  daily_spend AS (
    SELECT
      FileLoad_Date, LOB_Supported, Channel, Tactic, Message_Type, Agency,
      weekly_actual   / (DATEDIFF(Period_End, Period_Start) + 1) AS daily_actual,
      weekly_forecast / (DATEDIFF(Period_End, Period_Start) + 1) AS daily_forecast,
      weekly_display  / (DATEDIFF(Period_End, Period_Start) + 1) AS daily_display,
      EXPLODE(SEQUENCE(Period_Start, Period_End, INTERVAL 1 DAY)) AS calendar_date
    FROM normalized_weekly
  ),

  daily_with_qgp AS (
    SELECT
      FileLoad_Date, LOB_Supported, Channel, Tactic, Message_Type, Agency,
      calendar_date, daily_actual, daily_forecast, daily_display,
      CASE
        WHEN DATE_ADD(calendar_date, CASE WHEN DAYOFWEEK(calendar_date) = 7 THEN 0 ELSE 7 - DAYOFWEEK(calendar_date) END)
             > LAST_DAY(ADD_MONTHS(TO_DATE(DATE_TRUNC('quarter', calendar_date)), 2))
        THEN LAST_DAY(ADD_MONTHS(TO_DATE(DATE_TRUNC('quarter', calendar_date)), 2))
        ELSE DATE_ADD(calendar_date, CASE WHEN DAYOFWEEK(calendar_date) = 7 THEN 0 ELSE 7 - DAYOFWEEK(calendar_date) END)
      END AS QGP_Week
    FROM daily_spend
  ),

  -- ── Aggregate daily back to QGP_Week x combination ───────────────────────────
  aggregated AS (
    SELECT
      QGP_Week, LOB_Supported, Channel, Tactic, Message_Type, Agency,
      MAX(FileLoad_Date)                           AS FileLoad_Date,
      NULLIF(ROUND(SUM(daily_actual),   2), 0)     AS spend_actual,
      NULLIF(ROUND(SUM(daily_forecast), 2), 0)     AS spend_forecast,
      NULLIF(ROUND(SUM(daily_display),  2), 0)     AS spend_display
    FROM daily_with_qgp
    GROUP BY QGP_Week, LOB_Supported, Channel, Tactic, Message_Type, Agency
  ),

  -- ── All combinations that ever appeared ──────────────────────────────────────
  valid_combinations AS (
    SELECT DISTINCT LOB_Supported, Channel, Tactic, Message_Type, Agency
    FROM aggregated
  ),

  -- ── Full spine: every QGP date x every combination ───────────────────────────
  spine AS (
    SELECT
      cal.qgp_date, cal.week_type, cal.quarter, cal.days_in_period,
      cal.is_complete_period, cal.is_current_quarter,
      cal.wow_prior_qgp_date, cal.prior_year_qgp_date,
      cal.boundary_stub_date, cal.iso_week_number, cal.iso_year,
      cal.quarter_end_date,
      dims.LOB_Supported, dims.Channel, dims.Tactic, dims.Message_Type, dims.Agency
    FROM prdrzranalytics.lab42.sdi_vw_mfc_dim_qgp_calendar cal
    CROSS JOIN valid_combinations dims
    WHERE cal.qgp_date >= (SELECT MIN(QGP_Week) FROM aggregated)
      AND cal.qgp_date <= (SELECT MAX(QGP_Week) FROM aggregated)
  ),

  -- ── Join aggregated spend onto spine ─────────────────────────────────────────
  dense_spend AS (
    SELECT
      s.qgp_date, s.week_type, s.quarter, s.days_in_period,
      s.is_complete_period, s.quarter_end_date,
      s.wow_prior_qgp_date, s.prior_year_qgp_date, s.boundary_stub_date,
      s.LOB_Supported, s.Channel, s.Tactic, s.Message_Type, s.Agency,
      COALESCE(a.FileLoad_Date,
        MAX(a.FileLoad_Date) OVER (PARTITION BY s.LOB_Supported)) AS FileLoad_Date,
      CASE WHEN s.is_complete_period THEN a.spend_actual   ELSE NULL END AS spend_actual,
      a.spend_forecast,
      CASE WHEN s.is_complete_period THEN a.spend_display
           ELSE a.spend_forecast END AS spend_display
    FROM spine s
    LEFT JOIN aggregated a
      ON  a.QGP_Week      = s.qgp_date
      AND a.LOB_Supported <=> s.LOB_Supported
      AND a.Channel       <=> s.Channel
      AND a.Tactic        <=> s.Tactic
      AND a.Message_Type  <=> s.Message_Type
      AND a.Agency        <=> s.Agency
  ),

  -- ── Metric lookup for WoW stub joins ─────────────────────────────────────────
  metric_lookup AS (
    SELECT qgp_date, LOB_Supported, Channel, Tactic, Message_Type, Agency, spend_actual
    FROM dense_spend
  ),

  -- ── WoW computation ───────────────────────────────────────────────────────────
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
      ON  stub_lkp.qgp_date      = d.boundary_stub_date
      AND stub_lkp.LOB_Supported <=> d.LOB_Supported
      AND stub_lkp.Channel       <=> d.Channel
      AND stub_lkp.Tactic        <=> d.Tactic
      AND stub_lkp.Message_Type  <=> d.Message_Type
      AND stub_lkp.Agency        <=> d.Agency
    LEFT JOIN metric_lookup prior_lkp
      ON  prior_lkp.qgp_date      = d.wow_prior_qgp_date
      AND prior_lkp.LOB_Supported <=> d.LOB_Supported
      AND prior_lkp.Channel       <=> d.Channel
      AND prior_lkp.Tactic        <=> d.Tactic
      AND prior_lkp.Message_Type  <=> d.Message_Type
      AND prior_lkp.Agency        <=> d.Agency
    LEFT JOIN prdrzranalytics.lab42.sdi_vw_mfc_dim_qgp_calendar prior_cal
      ON prior_cal.qgp_date = d.wow_prior_qgp_date
    LEFT JOIN metric_lookup prior_stub_lkp
      ON  prior_stub_lkp.qgp_date      = prior_cal.boundary_stub_date
      AND prior_stub_lkp.LOB_Supported <=> d.LOB_Supported
      AND prior_stub_lkp.Channel       <=> d.Channel
      AND prior_stub_lkp.Tactic        <=> d.Tactic
      AND prior_stub_lkp.Message_Type  <=> d.Message_Type
      AND prior_stub_lkp.Agency        <=> d.Agency
  )

  SELECT
    quarter                                          AS Quarter,
    TO_DATE(DATE_TRUNC('quarter', qgp_date))         AS Quarter_Start_Date,
    quarter_end_date                                 AS Quarter_End_Date,
    DATE_SUB(qgp_date, days_in_period - 1)           AS Period_Start,
    qgp_date                                         AS Period_End,
    qgp_date                                         AS QGP_Week,
    FileLoad_Date,
    LOB_Supported, Channel, Tactic, Message_Type, Agency,
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