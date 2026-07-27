
-- ============================================================
-- SILVER 1 — SPEND, NON-GRANULAR / LOB LEVEL
-- Includes actual-priority routing fix: a disagreeing single-
-- quarter forecast follows its matching actual's routing.
-- ============================================================
CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_mfc_silver_spend_weekly()
SQL SECURITY DEFINER
COMMENT 'Creates/refreshes sdi_tbl_mfc_silver_spend_weekly. Refreshed weekly.'
BEGIN
  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_mfc_silver_spend_weekly
  USING DELTA
  COMMENT 'MFC Silver Spend (non-granular / LOB level), resolved at campaign grain from Bronze 2/4 then rolled up. Forecast defers to Actual''s quarter when both are single-quarter and disagree — refreshed via sdi_sp_mfc_silver_spend_weekly.'
  AS
  WITH
  calendar_resolved AS (
    SELECT
      c.qgp_date          AS QGP_Week,
      c.week_type,
      c.quarter            AS new_quarter,
      c.days_in_period     AS new_days,
      stub.qgp_date        AS stub_QGP_Week,
      stub.quarter         AS old_quarter,
      stub.days_in_period  AS old_days
    FROM prdrzranalytics.lab42.sdi_vw_mfc_dim_qgp_calendar c
    LEFT JOIN prdrzranalytics.lab42.sdi_vw_mfc_dim_qgp_calendar stub
      ON c.boundary_stub_date = stub.qgp_date
     AND stub.week_type = 'BOUNDARY_STUB'
    WHERE c.week_type IN ('NORMAL', 'BOUNDARY_FIRST')
  ),
  actual_quarter_totals AS (
    SELECT
      Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, LOB_Supported,
      Channel, Tactic, Message_Type, Agency,
      CONCAT('20', SUBSTRING(Quarter, 4, 2), ' ', SUBSTRING(Quarter, 1, 2)) AS Quarter,
      SUM(weekly_actual) AS quarter_total_actual,
      MAX(FileLoad_Date) AS FileLoad_Date,
      MAX(Source_File_Date) AS Source_File_Date
    FROM prdrzranalytics.lab42.sdi_tbl_mfc_bronze_spendActualsGranular_weekly
    GROUP BY Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, LOB_Supported, Channel, Tactic, Message_Type, Agency, Quarter
  ),
  actual_totals AS (
    SELECT
      Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, LOB_Supported,
      Channel, Tactic, Message_Type, Agency,
      SUM(quarter_total_actual) AS total_actual,
      COUNT(DISTINCT Quarter) AS n_quarters,
      MAX(FileLoad_Date) AS FileLoad_Date,
      MAX(Source_File_Date) AS Source_File_Date
    FROM actual_quarter_totals
    GROUP BY Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, LOB_Supported, Channel, Tactic, Message_Type, Agency
  ),
  actual_reallocated AS (
    SELECT t.Week_Beginning_Monday, t.Week_Ending_Sunday, t.QGP_Week, t.LOB_Supported,
      t.FileLoad_Date, t.Source_File_Date,
      cr.new_quarter AS Quarter,
      t.total_actual * cr.new_days / 7.0 AS weekly_actual,
      cr.week_type AS week_type
    FROM actual_totals t
    JOIN calendar_resolved cr ON t.QGP_Week = cr.QGP_Week
    WHERE t.n_quarters = 2 AND cr.week_type = 'BOUNDARY_FIRST' AND cr.stub_QGP_Week IS NOT NULL
    UNION ALL
    SELECT t.Week_Beginning_Monday, t.Week_Ending_Sunday, cr.stub_QGP_Week AS QGP_Week, t.LOB_Supported,
      t.FileLoad_Date, t.Source_File_Date,
      cr.old_quarter AS Quarter,
      t.total_actual * cr.old_days / 7.0 AS weekly_actual,
      'BOUNDARY_STUB' AS week_type
    FROM actual_totals t
    JOIN calendar_resolved cr ON t.QGP_Week = cr.QGP_Week
    WHERE t.n_quarters = 2 AND cr.week_type = 'BOUNDARY_FIRST' AND cr.stub_QGP_Week IS NOT NULL
    UNION ALL
    SELECT qt.Week_Beginning_Monday, qt.Week_Ending_Sunday,
      CASE WHEN qt.Quarter = cr.old_quarter AND cr.stub_QGP_Week IS NOT NULL THEN cr.stub_QGP_Week ELSE cr.QGP_Week END AS QGP_Week,
      qt.LOB_Supported,
      qt.FileLoad_Date, qt.Source_File_Date,
      qt.Quarter AS Quarter,
      qt.quarter_total_actual AS weekly_actual,
      CASE WHEN qt.Quarter = cr.old_quarter AND cr.stub_QGP_Week IS NOT NULL THEN 'BOUNDARY_STUB' ELSE cr.week_type END AS week_type
    FROM actual_quarter_totals qt
    JOIN actual_totals t ON qt.QGP_Week = t.QGP_Week AND qt.LOB_Supported = t.LOB_Supported
    JOIN calendar_resolved cr ON qt.QGP_Week = cr.QGP_Week
    WHERE NOT (t.n_quarters = 2 AND cr.week_type = 'BOUNDARY_FIRST' AND cr.stub_QGP_Week IS NOT NULL)
  ),
  forecast_quarter_totals AS (
    SELECT
      Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, LOB_Supported,
      Channel, Tactic, Message_Type, Agency,
      CONCAT('20', SUBSTRING(Quarter, 4, 2), ' ', SUBSTRING(Quarter, 1, 2)) AS Quarter,
      SUM(weekly_forecast) AS quarter_total_forecast,
      MAX(FileLoad_Date) AS FileLoad_Date,
      MAX(Source_File_Date) AS Source_File_Date
    FROM prdrzranalytics.lab42.sdi_tbl_mfc_bronze_spendForecastGranular_weekly
    GROUP BY Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, LOB_Supported, Channel, Tactic, Message_Type, Agency, Quarter
  ),
  forecast_totals AS (
    SELECT
      Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, LOB_Supported,
      Channel, Tactic, Message_Type, Agency,
      SUM(quarter_total_forecast) AS total_forecast,
      COUNT(DISTINCT Quarter) AS n_quarters,
      MAX(FileLoad_Date) AS FileLoad_Date,
      MAX(Source_File_Date) AS Source_File_Date
    FROM forecast_quarter_totals
    GROUP BY Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, LOB_Supported, Channel, Tactic, Message_Type, Agency
  ),
  actual_single_quarter AS (
    SELECT
      Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, LOB_Supported,
      Channel, Tactic, Message_Type, Agency,
      n_quarters, MAX(Quarter) AS the_quarter
    FROM (
      SELECT t.*, qt.Quarter
      FROM actual_totals t
      JOIN actual_quarter_totals qt
        ON t.QGP_Week = qt.QGP_Week AND t.LOB_Supported = qt.LOB_Supported
       AND t.Channel <=> qt.Channel AND t.Tactic <=> qt.Tactic
       AND t.Message_Type <=> qt.Message_Type AND t.Agency <=> qt.Agency
    )
    GROUP BY Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, LOB_Supported, Channel, Tactic, Message_Type, Agency, n_quarters
  ),
  forecast_quarter_totals_resolved AS (
    SELECT
      qt.*,
      CASE WHEN a.n_quarters = 1 THEN a.the_quarter ELSE qt.Quarter END AS effective_quarter
    FROM forecast_quarter_totals qt
    LEFT JOIN actual_single_quarter a
      ON qt.QGP_Week = a.QGP_Week AND qt.LOB_Supported = a.LOB_Supported
     AND qt.Channel <=> a.Channel AND qt.Tactic <=> a.Tactic
     AND qt.Message_Type <=> a.Message_Type AND qt.Agency <=> a.Agency
  ),
  forecast_reallocated AS (
    SELECT t.Week_Beginning_Monday, t.Week_Ending_Sunday, t.QGP_Week, t.LOB_Supported,
      t.FileLoad_Date, t.Source_File_Date,
      cr.new_quarter AS Quarter,
      t.total_forecast * cr.new_days / 7.0 AS weekly_forecast,
      cr.week_type AS week_type
    FROM forecast_totals t
    JOIN calendar_resolved cr ON t.QGP_Week = cr.QGP_Week
    WHERE t.n_quarters = 2 AND cr.week_type = 'BOUNDARY_FIRST' AND cr.stub_QGP_Week IS NOT NULL
    UNION ALL
    SELECT t.Week_Beginning_Monday, t.Week_Ending_Sunday, cr.stub_QGP_Week AS QGP_Week, t.LOB_Supported,
      t.FileLoad_Date, t.Source_File_Date,
      cr.old_quarter AS Quarter,
      t.total_forecast * cr.old_days / 7.0 AS weekly_forecast,
      'BOUNDARY_STUB' AS week_type
    FROM forecast_totals t
    JOIN calendar_resolved cr ON t.QGP_Week = cr.QGP_Week
    WHERE t.n_quarters = 2 AND cr.week_type = 'BOUNDARY_FIRST' AND cr.stub_QGP_Week IS NOT NULL
    UNION ALL
    SELECT qt.Week_Beginning_Monday, qt.Week_Ending_Sunday,
      CASE WHEN qt.effective_quarter = cr.old_quarter AND cr.stub_QGP_Week IS NOT NULL THEN cr.stub_QGP_Week ELSE cr.QGP_Week END AS QGP_Week,
      qt.LOB_Supported,
      qt.FileLoad_Date, qt.Source_File_Date,
      qt.effective_quarter AS Quarter,
      qt.quarter_total_forecast AS weekly_forecast,
      CASE WHEN qt.effective_quarter = cr.old_quarter AND cr.stub_QGP_Week IS NOT NULL THEN 'BOUNDARY_STUB' ELSE cr.week_type END AS week_type
    FROM forecast_quarter_totals_resolved qt
    JOIN forecast_totals t ON qt.QGP_Week = t.QGP_Week AND qt.LOB_Supported = t.LOB_Supported
    JOIN calendar_resolved cr ON qt.QGP_Week = cr.QGP_Week
    WHERE NOT (t.n_quarters = 2 AND cr.week_type = 'BOUNDARY_FIRST' AND cr.stub_QGP_Week IS NOT NULL)
  ),
  resolved_granular AS (
    SELECT
      COALESCE(a.Quarter, f.Quarter) AS Quarter,
      COALESCE(a.Week_Beginning_Monday, f.Week_Beginning_Monday) AS Week_Beginning_Monday,
      COALESCE(a.Week_Ending_Sunday, f.Week_Ending_Sunday) AS Week_Ending_Sunday,
      COALESCE(a.QGP_Week, f.QGP_Week) AS QGP_Week,
      COALESCE(a.LOB_Supported, f.LOB_Supported) AS LOB_Supported,
      a.weekly_actual AS Spend_Actual,
      f.weekly_forecast AS Spend_Forecast,
      COALESCE(a.weekly_actual, f.weekly_forecast) AS Spend_Final,
      COALESCE(a.week_type, f.week_type) AS week_type,
      a.FileLoad_Date AS Actual_FileLoad_Date, a.Source_File_Date AS Actual_Source_File_Date,
      f.FileLoad_Date AS Forecast_FileLoad_Date, f.Source_File_Date AS Forecast_Source_File_Date
    FROM actual_reallocated a
    FULL OUTER JOIN forecast_reallocated f
      ON a.QGP_Week = f.QGP_Week
     AND a.LOB_Supported = f.LOB_Supported
     AND a.Channel <=> f.Channel
     AND a.Tactic <=> f.Tactic
     AND a.Message_Type <=> f.Message_Type
     AND a.Agency <=> f.Agency
  ),
  rolled_up AS (
    SELECT
      Quarter, Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, week_type, LOB_Supported,
      SUM(Spend_Actual)   AS Spend_Actual,
      SUM(Spend_Forecast) AS Spend_Forecast,
      SUM(Spend_Final)    AS Spend_Final,
      MAX(Actual_FileLoad_Date) AS Actual_FileLoad_Date,
      MAX(Actual_Source_File_Date) AS Actual_Source_File_Date,
      MAX(Forecast_FileLoad_Date) AS Forecast_FileLoad_Date,
      MAX(Forecast_Source_File_Date) AS Forecast_Source_File_Date
    FROM resolved_granular
    GROUP BY Quarter, Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, week_type, LOB_Supported
  )
  SELECT
    Quarter, Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, LOB_Supported,
    Spend_Actual, Spend_Forecast, Spend_Final,
    SUM(Spend_Actual)   OVER (PARTITION BY Week_Beginning_Monday, LOB_Supported) AS Spend_Actual_FullWeek,
    SUM(Spend_Forecast) OVER (PARTITION BY Week_Beginning_Monday, LOB_Supported) AS Spend_Forecast_FullWeek,
    SUM(Spend_Final)    OVER (PARTITION BY Week_Beginning_Monday, LOB_Supported) AS Spend_Final_FullWeek,
    CASE
      WHEN Spend_Actual IS NOT NULL AND Spend_Forecast IS NOT NULL THEN 'Actual+Forecast'
      WHEN Spend_Actual IS NOT NULL THEN 'Actual'
      WHEN Spend_Forecast IS NOT NULL THEN 'Forecast'
    END AS Spend_Status,
    week_type,
    Actual_FileLoad_Date, Actual_Source_File_Date,
    Forecast_FileLoad_Date, Forecast_Source_File_Date
  FROM rolled_up
  ;
END;

