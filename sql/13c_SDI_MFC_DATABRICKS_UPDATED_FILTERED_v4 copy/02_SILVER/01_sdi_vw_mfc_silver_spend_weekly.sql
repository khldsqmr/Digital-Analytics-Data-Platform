-- ============================================================
-- SILVER 1 — SPEND, NON-GRANULAR / LOB LEVEL
-- Forecast now follows Actual's exact resolved shape whenever
-- Actual exists at all (single- or dual-quarter) — not just the
-- single-quarter-disagreement case from the prior fix.
-- ============================================================
CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_mfc_silver_spend_weekly()
SQL SECURITY DEFINER
COMMENT 'Creates/refreshes sdi_tbl_mfc_silver_spend_weekly. Refreshed weekly.'
BEGIN
  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_mfc_silver_spend_weekly
  USING DELTA
  COMMENT 'MFC Silver Spend (non-granular / LOB level), resolved at campaign grain from Bronze 2/4 then rolled up. Forecast follows Actual''s resolved shape whenever Actual exists — refreshed via sdi_sp_mfc_silver_spend_weekly.'
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
      t.Channel, t.Tactic, t.Message_Type, t.Agency,
      t.FileLoad_Date, t.Source_File_Date,
      cr.new_quarter AS Quarter,
      t.total_actual * cr.new_days / 7.0 AS weekly_actual,
      cr.week_type AS week_type
    FROM actual_totals t
    JOIN calendar_resolved cr ON t.QGP_Week = cr.QGP_Week
    WHERE t.n_quarters = 2 AND cr.week_type = 'BOUNDARY_FIRST' AND cr.stub_QGP_Week IS NOT NULL
    UNION ALL
    SELECT t.Week_Beginning_Monday, t.Week_Ending_Sunday, cr.stub_QGP_Week AS QGP_Week, t.LOB_Supported,
      t.Channel, t.Tactic, t.Message_Type, t.Agency,
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
      qt.Channel, qt.Tactic, qt.Message_Type, qt.Agency,
      qt.FileLoad_Date, qt.Source_File_Date,
      qt.Quarter AS Quarter,
      qt.quarter_total_actual AS weekly_actual,
      CASE WHEN qt.Quarter = cr.old_quarter AND cr.stub_QGP_Week IS NOT NULL THEN 'BOUNDARY_STUB' ELSE cr.week_type END AS week_type
    FROM actual_quarter_totals qt
    JOIN actual_totals t
      ON qt.QGP_Week = t.QGP_Week AND qt.LOB_Supported = t.LOB_Supported
     AND qt.Channel <=> t.Channel AND qt.Tactic <=> t.Tactic
     AND qt.Message_Type <=> t.Message_Type AND qt.Agency <=> t.Agency
    JOIN calendar_resolved cr ON qt.QGP_Week = cr.QGP_Week
    WHERE NOT (t.n_quarters = 2 AND cr.week_type = 'BOUNDARY_FIRST' AND cr.stub_QGP_Week IS NOT NULL)
  ),
  -- NEW: what fraction of this campaign+week's Actual total landed at each
  -- resolved destination. 1 row = 100% there; 2 rows (genuine split) sum to 100%.
  actual_shape AS (
    SELECT
      Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, LOB_Supported,
      Channel, Tactic, Message_Type, Agency, Quarter, week_type,
      weekly_actual / SUM(weekly_actual) OVER (
        PARTITION BY Week_Beginning_Monday, LOB_Supported, Channel, Tactic, Message_Type, Agency
      ) AS shape_pct
    FROM actual_reallocated
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
  forecast_reallocated AS (
    -- CASE A: Actual has any presence for this campaign+week — Forecast's raw total
    -- (ignoring Forecast's own quarter tags entirely) follows Actual's exact shape.
    SELECT
      shp.Week_Beginning_Monday, shp.Week_Ending_Sunday, shp.QGP_Week, shp.LOB_Supported,
      shp.Channel, shp.Tactic, shp.Message_Type, shp.Agency,
      ft.FileLoad_Date, ft.Source_File_Date,
      shp.Quarter,
      ft.total_forecast * shp.shape_pct AS weekly_forecast,
      shp.week_type
    FROM forecast_totals ft
    JOIN actual_shape shp
      ON ft.Week_Beginning_Monday = shp.Week_Beginning_Monday
     AND ft.LOB_Supported = shp.LOB_Supported
     AND ft.Channel <=> shp.Channel AND ft.Tactic <=> shp.Tactic
     AND ft.Message_Type <=> shp.Message_Type AND ft.Agency <=> shp.Agency
    UNION ALL
    -- CASE B: no Actual at all for this campaign+week — Forecast routes independently,
    -- same day-split / passthrough logic as originally designed.
    SELECT t.Week_Beginning_Monday, t.Week_Ending_Sunday, t.QGP_Week, t.LOB_Supported,
      t.Channel, t.Tactic, t.Message_Type, t.Agency,
      t.FileLoad_Date, t.Source_File_Date,
      cr.new_quarter AS Quarter,
      t.total_forecast * cr.new_days / 7.0 AS weekly_forecast,
      cr.week_type AS week_type
    FROM forecast_totals t
    JOIN calendar_resolved cr ON t.QGP_Week = cr.QGP_Week
    WHERE t.n_quarters = 2 AND cr.week_type = 'BOUNDARY_FIRST' AND cr.stub_QGP_Week IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM actual_shape shp
        WHERE shp.Week_Beginning_Monday = t.Week_Beginning_Monday AND shp.LOB_Supported = t.LOB_Supported
          AND shp.Channel <=> t.Channel AND shp.Tactic <=> t.Tactic
          AND shp.Message_Type <=> t.Message_Type AND shp.Agency <=> t.Agency
      )
    UNION ALL
    SELECT t.Week_Beginning_Monday, t.Week_Ending_Sunday, cr.stub_QGP_Week AS QGP_Week, t.LOB_Supported,
      t.Channel, t.Tactic, t.Message_Type, t.Agency,
      t.FileLoad_Date, t.Source_File_Date,
      cr.old_quarter AS Quarter,
      t.total_forecast * cr.old_days / 7.0 AS weekly_forecast,
      'BOUNDARY_STUB' AS week_type
    FROM forecast_totals t
    JOIN calendar_resolved cr ON t.QGP_Week = cr.QGP_Week
    WHERE t.n_quarters = 2 AND cr.week_type = 'BOUNDARY_FIRST' AND cr.stub_QGP_Week IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM actual_shape shp
        WHERE shp.Week_Beginning_Monday = t.Week_Beginning_Monday AND shp.LOB_Supported = t.LOB_Supported
          AND shp.Channel <=> t.Channel AND shp.Tactic <=> t.Tactic
          AND shp.Message_Type <=> t.Message_Type AND shp.Agency <=> t.Agency
      )
    UNION ALL
    SELECT qt.Week_Beginning_Monday, qt.Week_Ending_Sunday,
      CASE WHEN qt.Quarter = cr.old_quarter AND cr.stub_QGP_Week IS NOT NULL THEN cr.stub_QGP_Week ELSE cr.QGP_Week END AS QGP_Week,
      qt.LOB_Supported,
      qt.Channel, qt.Tactic, qt.Message_Type, qt.Agency,
      qt.FileLoad_Date, qt.Source_File_Date,
      qt.Quarter AS Quarter,
      qt.quarter_total_forecast AS weekly_forecast,
      CASE WHEN qt.Quarter = cr.old_quarter AND cr.stub_QGP_Week IS NOT NULL THEN 'BOUNDARY_STUB' ELSE cr.week_type END AS week_type
    FROM forecast_quarter_totals qt
    JOIN forecast_totals t ON qt.QGP_Week = t.QGP_Week AND qt.LOB_Supported = t.LOB_Supported
     AND qt.Channel <=> t.Channel AND qt.Tactic <=> t.Tactic AND qt.Message_Type <=> t.Message_Type AND qt.Agency <=> t.Agency
    JOIN calendar_resolved cr ON qt.QGP_Week = cr.QGP_Week
    WHERE NOT (t.n_quarters = 2 AND cr.week_type = 'BOUNDARY_FIRST' AND cr.stub_QGP_Week IS NOT NULL)
      AND NOT EXISTS (
        SELECT 1 FROM actual_shape shp
        WHERE shp.Week_Beginning_Monday = qt.Week_Beginning_Monday AND shp.LOB_Supported = qt.LOB_Supported
          AND shp.Channel <=> qt.Channel AND shp.Tactic <=> qt.Tactic
          AND shp.Message_Type <=> qt.Message_Type AND shp.Agency <=> qt.Agency
      )
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

