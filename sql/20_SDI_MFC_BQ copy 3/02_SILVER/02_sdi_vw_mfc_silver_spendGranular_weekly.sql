-- ============================================================
-- MFC SPEND PIPELINE: SILVER 2 - GRANULAR — BigQuery
-- Converted from Databricks sdi_sp_mfc_silver_spendGranular_weekly
--
-- Logic:
--   Same as Silver 1 but all dimension keys expand to include
--   Channel, Tactic, Message_Type, Agency throughout.
--   FULL OUTER JOIN on granular key with BigQuery null-safe Agency logic.
--
-- CHANGE LOG:
--   - Boundary week allocation updated: sum Q_prev + Q_next then allocate
--     by days_in_period / 7. NULL handling: if one side is NULL, that QGP
--     date stays NULL; the other uses its own value / 7 x days_in_period.
-- ============================================================

CREATE OR REPLACE PROCEDURE
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_sp_mfc_silver_spendGranular_weekly`()
OPTIONS (strict_mode = false)
BEGIN

  CREATE OR REPLACE TABLE
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_mfc_silver_spendGranular_weekly`
  OPTIONS (
    description = 'MFC Silver — granular spend with WoW. One row per QGP_Week x LOB x Channel x Tactic x Message_Type x Agency. Boundary weeks allocated as (stub + first) / 7 x days_in_period. Refreshed via sdi_sp_mfc_silver_spendGranular_weekly.'
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
      UPPER(TRIM(LOB_Supported)) AS LOB_Supported,
      UPPER(TRIM(Channel)) AS Channel,
      UPPER(TRIM(Tactic)) AS Tactic,
      UPPER(TRIM(Message_Type)) AS Message_Type,
      TRIM(Agency) AS Agency,
      weekly_actual,
      UPPER(TRIM(week_type)) AS source_week_type
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_mfc_bronze_spendActualsGranular_weekly`
    WHERE Week_Beginning_Monday IS NOT NULL
      AND Week_Ending_Sunday IS NOT NULL
  ),

  forecast_clean AS (
    SELECT
      Quarter,
      Week_Beginning_Monday,
      Week_Ending_Sunday,
      QGP_Week,
      FileLoad_Date,
      UPPER(TRIM(LOB_Supported)) AS LOB_Supported,
      UPPER(TRIM(Channel)) AS Channel,
      UPPER(TRIM(Tactic)) AS Tactic,
      UPPER(TRIM(Message_Type)) AS Message_Type,
      TRIM(Agency) AS Agency,
      weekly_forecast,
      UPPER(TRIM(week_type)) AS source_week_type
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_mfc_bronze_spendForecastsGranular_weekly`
    WHERE Week_Beginning_Monday IS NOT NULL
      AND Week_Ending_Sunday IS NOT NULL
  ),

  source_joined AS (
    SELECT
      COALESCE(a.Quarter, f.Quarter) AS Source_Quarter,
      COALESCE(a.Week_Beginning_Monday, f.Week_Beginning_Monday) AS Week_Beginning_Monday,
      COALESCE(a.Week_Ending_Sunday, f.Week_Ending_Sunday) AS Week_Ending_Sunday,
      COALESCE(a.QGP_Week, f.QGP_Week) AS Source_QGP_Week,
      (
        SELECT MAX(d)
        FROM UNNEST([a.FileLoad_Date, f.FileLoad_Date]) AS d
      ) AS FileLoad_Date,
      COALESCE(a.LOB_Supported, f.LOB_Supported) AS LOB_Supported,
      COALESCE(a.Channel, f.Channel) AS Channel,
      COALESCE(a.Tactic, f.Tactic) AS Tactic,
      COALESCE(a.Message_Type, f.Message_Type) AS Message_Type,
      COALESCE(a.Agency, f.Agency) AS Agency,
      a.weekly_actual,
      f.weekly_forecast,
      COALESCE(
        IF(a.weekly_actual != 0, a.weekly_actual, NULL),
        f.weekly_forecast
      ) AS weekly_display,
      UPPER(TRIM(COALESCE(a.source_week_type, f.source_week_type))) AS source_week_type
    FROM actual_clean a
    FULL OUTER JOIN forecast_clean f
      ON  a.Quarter = f.Quarter
      AND a.QGP_Week = f.QGP_Week
      AND a.LOB_Supported = f.LOB_Supported
      AND a.Channel = f.Channel
      AND a.Tactic = f.Tactic
      AND a.Message_Type = f.Message_Type
      AND a.Agency IS NOT DISTINCT FROM f.Agency
  ),

  with_source_quarter_bounds AS (
    SELECT
      *,
      CAST(SUBSTR(Source_Quarter, 2, 1) AS INT64) AS qtr_num,
      CAST(SUBSTR(Source_Quarter, 4, 2) AS INT64) AS qtr_year_2digit
    FROM source_joined
    WHERE Week_Beginning_Monday IS NOT NULL
      AND Week_Ending_Sunday IS NOT NULL
      AND Source_Quarter IS NOT NULL
  ),

  with_bounds AS (
    SELECT
      *,
      DATE(CAST(CONCAT(
        CASE WHEN qtr_year_2digit < 50 THEN '20' ELSE '19' END,
        LPAD(CAST(qtr_year_2digit AS STRING), 2, '0'),
        '-',
        LPAD(CAST(((qtr_num - 1) * 3) + 1 AS STRING), 2, '0'),
        '-01'
      ) AS DATE)) AS Source_Quarter_Start_Date,
      DATE_SUB(
        DATE_ADD(
          DATE(CAST(CONCAT(
            CASE WHEN qtr_year_2digit < 50 THEN '20' ELSE '19' END,
            LPAD(CAST(qtr_year_2digit AS STRING), 2, '0'),
            '-',
            LPAD(CAST(qtr_num * 3 AS STRING), 2, '0'),
            '-01'
          ) AS DATE)),
          INTERVAL 1 MONTH
        ),
        INTERVAL 1 DAY
      ) AS Source_Quarter_End_Date
    FROM with_source_quarter_bounds
  ),

  source_periods AS (
    SELECT
      *,
      IF(
        Week_Beginning_Monday > Source_Quarter_Start_Date,
        Week_Beginning_Monday,
        Source_Quarter_Start_Date
      ) AS Period_Start,
      IF(
        Week_Ending_Sunday < Source_Quarter_End_Date,
        Week_Ending_Sunday,
        Source_Quarter_End_Date
      ) AS Period_End,
      DATE_DIFF(Week_Ending_Sunday, Week_Beginning_Monday, DAY) + 1 AS source_total_week_days
    FROM with_bounds
  ),

  normalized_weekly AS (
    SELECT
      FileLoad_Date,
      LOB_Supported,
      Channel,
      Tactic,
      Message_Type,
      Agency,
      Period_Start,
      Period_End,
      Source_QGP_Week,
      CASE
        WHEN source_week_type != 'BOUNDARY_WEEK'
          AND Week_Ending_Sunday > Source_Quarter_End_Date
          THEN ROUND(
            weekly_actual
            * (DATE_DIFF(Source_Quarter_End_Date, Week_Beginning_Monday, DAY) + 1)
            / source_total_week_days, 2)
        ELSE weekly_actual
      END AS weekly_actual,
      CASE
        WHEN source_week_type != 'BOUNDARY_WEEK'
          AND Week_Ending_Sunday > Source_Quarter_End_Date
          THEN ROUND(
            weekly_forecast
            * (DATE_DIFF(Source_Quarter_End_Date, Week_Beginning_Monday, DAY) + 1)
            / source_total_week_days, 2)
        ELSE weekly_forecast
      END AS weekly_forecast,
      CASE
        WHEN source_week_type != 'BOUNDARY_WEEK'
          AND Week_Ending_Sunday > Source_Quarter_End_Date
          THEN ROUND(
            weekly_display
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
      Channel,
      Tactic,
      Message_Type,
      Agency,
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
      Channel,
      Tactic,
      Message_Type,
      Agency,
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
      Channel,
      Tactic,
      Message_Type,
      Agency,
      MAX(FileLoad_Date) AS FileLoad_Date,
      NULLIF(ROUND(SUM(daily_actual),   2), 0) AS spend_actual,
      NULLIF(ROUND(SUM(daily_forecast), 2), 0) AS spend_forecast,
      NULLIF(ROUND(SUM(daily_display),  2), 0) AS spend_display
    FROM daily_with_qgp
    GROUP BY QGP_Week, LOB_Supported, Channel, Tactic, Message_Type, Agency
  ),

  -- ===========================================================================
  -- NEW: Boundary week reallocation (granular grain)
  --
  -- Same logic as Silver 1 but extended to include Channel/Tactic/Message_Type/Agency
  -- in the join keys so reallocation is correct at granular grain.
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
      dims.LOB_Supported,
      dims.Channel,
      dims.Tactic,
      dims.Message_Type,
      dims.Agency,
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
    JOIN (
      SELECT DISTINCT LOB_Supported, Channel, Tactic, Message_Type, Agency
      FROM aggregated
    ) dims
      ON  TRUE
    LEFT JOIN aggregated a_stub
      ON  a_stub.QGP_Week                    = stub_cal.qgp_date
      AND a_stub.LOB_Supported               = dims.LOB_Supported
      AND a_stub.Channel                     = dims.Channel
      AND a_stub.Tactic                      = dims.Tactic
      AND a_stub.Message_Type                = dims.Message_Type
      AND a_stub.Agency IS NOT DISTINCT FROM dims.Agency
    LEFT JOIN aggregated a_first
      ON  a_first.QGP_Week                   = first_cal.qgp_date
      AND a_first.LOB_Supported              = dims.LOB_Supported
      AND a_first.Channel                    = dims.Channel
      AND a_first.Tactic                     = dims.Tactic
      AND a_first.Message_Type               = dims.Message_Type
      AND a_first.Agency IS NOT DISTINCT FROM dims.Agency
    WHERE a_stub.QGP_Week IS NOT NULL
       OR a_first.QGP_Week IS NOT NULL
  ),

  boundary_reallocated AS (
    -- Stub rows with new values
    SELECT
      stub_date    AS QGP_Week,
      LOB_Supported,
      Channel,
      Tactic,
      Message_Type,
      Agency,
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
      Channel,
      Tactic,
      Message_Type,
      Agency,
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
      a.Channel,
      a.Tactic,
      a.Message_Type,
      a.Agency,
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
      ON  br.QGP_Week                    = a.QGP_Week
      AND br.LOB_Supported               = a.LOB_Supported
      AND br.Channel                     = a.Channel
      AND br.Tactic                      = a.Tactic
      AND br.Message_Type                = a.Message_Type
      AND br.Agency IS NOT DISTINCT FROM a.Agency
  ),

  valid_combinations AS (
    SELECT DISTINCT LOB_Supported, Channel, Tactic, Message_Type, Agency
    FROM aggregated_final
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
      cal.quarter_end_date,
      dims.LOB_Supported,
      dims.Channel,
      dims.Tactic,
      dims.Message_Type,
      dims.Agency
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_dim_qgp_calendar` cal
    CROSS JOIN valid_combinations dims
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
      s.quarter_end_date,
      s.wow_prior_qgp_date,
      s.prior_year_qgp_date,
      s.boundary_stub_date,
      s.LOB_Supported,
      s.Channel,
      s.Tactic,
      s.Message_Type,
      s.Agency,
      COALESCE(
        a.FileLoad_Date,
        MAX(a.FileLoad_Date) OVER (
          PARTITION BY s.LOB_Supported, s.Channel, s.Tactic, s.Message_Type, s.Agency
        )
      ) AS FileLoad_Date,
      IF(s.is_complete_period, a.spend_actual, NULL) AS spend_actual,
      a.spend_forecast,
      IF(s.is_complete_period, a.spend_display, a.spend_forecast) AS spend_display
    FROM spine s
    LEFT JOIN aggregated_final a
      ON  a.QGP_Week                    = s.qgp_date
      AND a.LOB_Supported               = s.LOB_Supported
      AND a.Channel                     = s.Channel
      AND a.Tactic                      = s.Tactic
      AND a.Message_Type                = s.Message_Type
      AND a.Agency IS NOT DISTINCT FROM s.Agency
  ),

  metric_lookup AS (
    SELECT qgp_date, LOB_Supported, Channel, Tactic, Message_Type, Agency, spend_actual
    FROM dense_spend
  ),

  with_spend_for_wow AS (
    SELECT
      d.*,
      CASE
        WHEN d.week_type = 'BOUNDARY_STUB' THEN NULL
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
      ON  stub_lkp.qgp_date                    = d.boundary_stub_date
      AND stub_lkp.LOB_Supported               = d.LOB_Supported
      AND stub_lkp.Channel                     = d.Channel
      AND stub_lkp.Tactic                      = d.Tactic
      AND stub_lkp.Message_Type                = d.Message_Type
      AND stub_lkp.Agency IS NOT DISTINCT FROM d.Agency
    LEFT JOIN metric_lookup prior_lkp
      ON  prior_lkp.qgp_date                   = d.wow_prior_qgp_date
      AND prior_lkp.LOB_Supported              = d.LOB_Supported
      AND prior_lkp.Channel                    = d.Channel
      AND prior_lkp.Tactic                     = d.Tactic
      AND prior_lkp.Message_Type               = d.Message_Type
      AND prior_lkp.Agency IS NOT DISTINCT FROM d.Agency
    LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_dim_qgp_calendar` prior_cal
      ON prior_cal.qgp_date = d.wow_prior_qgp_date
    LEFT JOIN metric_lookup prior_stub_lkp
      ON  prior_stub_lkp.qgp_date                   = prior_cal.boundary_stub_date
      AND prior_stub_lkp.LOB_Supported              = d.LOB_Supported
      AND prior_stub_lkp.Channel                    = d.Channel
      AND prior_stub_lkp.Tactic                     = d.Tactic
      AND prior_stub_lkp.Message_Type               = d.Message_Type
      AND prior_stub_lkp.Agency IS NOT DISTINCT FROM d.Agency
  )

  SELECT
    quarter AS Quarter,
    DATE_TRUNC(qgp_date, QUARTER) AS Quarter_Start_Date,
    quarter_end_date AS Quarter_End_Date,
    DATE_SUB(qgp_date, INTERVAL (days_in_period - 1) DAY) AS Period_Start,
    qgp_date AS Period_End,
    qgp_date AS QGP_Week,
    FileLoad_Date,
    LOB_Supported,
    Channel,
    Tactic,
    Message_Type,
    Agency,
    spend_actual AS weekly_actual,
    spend_forecast AS weekly_forecast,
    spend_display AS weekly_display,
    spend_actual,
    spend_forecast,
    spend_display,
    spend_actual_for_wow,
    spend_for_wow,
    CASE
      WHEN spend_actual_for_wow IS NOT NULL
        AND spend_for_wow IS NOT NULL
        AND spend_for_wow != 0
        THEN ROUND(((spend_actual_for_wow - spend_for_wow) / spend_for_wow) * 100, 2)
      ELSE NULL
    END AS spend_actual_wow_pct,
    CASE
      WHEN week_type IN ('BOUNDARY_STUB', 'BOUNDARY_FIRST') THEN 'BOUNDARY_WEEK'
      ELSE 'NORMAL'
    END AS week_type,
    CASE
      WHEN days_in_period < 7 THEN 'Partial'
      ELSE 'Full'
    END AS is_partial_week
  FROM with_spend_for_wow
  ;

END;