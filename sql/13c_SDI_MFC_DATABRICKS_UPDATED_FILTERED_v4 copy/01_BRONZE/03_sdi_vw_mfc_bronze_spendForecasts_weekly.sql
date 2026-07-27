
-- ============================================================
-- BRONZE 3 — SPEND FORECAST, NON-GRANULAR / LOB LEVEL
-- ============================================================
CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_mfc_bronze_spendForecast_weekly()
SQL SECURITY DEFINER
COMMENT 'Creates/refreshes sdi_tbl_mfc_bronze_spendForecast_weekly. Refreshed weekly.'
BEGIN
  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_mfc_bronze_spendForecast_weekly
  USING DELTA
  COMMENT 'MFC Bronze Forecast (non-granular / LOB level) — refreshed via sdi_sp_mfc_bronze_spendForecast_weekly.'
  AS
  WITH raw AS (
    SELECT
      Quarter,
      TRY_CAST(NULLIF(CAST(Week_Beginning_Monday AS STRING), 'None') AS DATE) AS Week_Beginning_Monday,
      COALESCE(
        TRY_CAST(NULLIF(CAST(Week_Ending_Sunday AS STRING), 'None') AS DATE),
        DATE_ADD(TRY_CAST(NULLIF(CAST(Week_Beginning_Monday AS STRING), 'None') AS DATE), 6)
      ) AS Week_Ending_Sunday,
      TRY_CAST(NULLIF(CAST(QGP_Week AS STRING), 'None') AS DATE) AS QGP_Week,
      TRY_CAST(CAST(FileLoad_Date AS STRING) AS DATE) AS FileLoad_Date,
      TRY_CAST(File_Date AS DATE) AS Source_File_Date,
      UPPER(TRIM(LOB_Supported)) AS LOB_Supported,
      Spend AS Spend_Forecast
    FROM prdrzranalytics.lab42.raw_media_flowchart
    WHERE 1=1
      -- Scope to specific lines of business
      AND UPPER(TRIM(LOB_Supported)) IN ('CONSUMER POSTPAID', 'BROADBAND', 'TFB')
      -- Only "Working" media (excludes non-working/placeholder rows)
      AND UPPER(TRIM(WM_V_NWM)) = 'WORKING'
      -- Exclude rows with no Channel assigned
      AND Channel IS NOT NULL
      -- Exclude specific non-reportable channel buckets
      AND Channel NOT IN ('OTHER (do not use)', 'Non-Working', 'Budget Held')
      -- Require a parseable Week_Beginning_Monday
      AND TRY_CAST(NULLIF(CAST(Week_Beginning_Monday AS STRING), 'None') AS DATE) IS NOT NULL
      -- Require a parseable QGP_Week
      AND TRY_CAST(NULLIF(CAST(QGP_Week AS STRING), 'None') AS DATE) IS NOT NULL
      -- Require a parseable FileLoad_Date
      AND TRY_CAST(CAST(FileLoad_Date AS STRING) AS DATE) IS NOT NULL
      -- Exclude "Micro" message-type rows
      AND UPPER(TRIM(Message_Type)) NOT IN ('MICRO')
      -- Exclude specific micro-targeted campaign line items
      AND UPPER(TRIM(Message)) NOT IN ('SEM POSTPAID/MICRO', 'MICRO POSTPAID OFFERS')
      -- Require a non-null Quarter tag
      AND Quarter IS NOT NULL
      -- Require Quarter to match the expected "Q#'YY" format
      AND Quarter RLIKE "^Q[1-4]'[0-9]{2}$"
      -- Forecast only (Bronze 1/2 use 'ACTUAL' instead)
      AND UPPER(TRIM(QGP)) = 'FORECAST'
      -- Exclude rows with no spend value
      AND Spend IS NOT NULL
  ),
  weekly_snapshots AS (
    SELECT
      Quarter, Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week,
      FileLoad_Date, Source_File_Date, LOB_Supported,
      SUM(Spend_Forecast) AS weekly_forecast
    FROM raw
    WHERE Week_Beginning_Monday <= Week_Ending_Sunday
    GROUP BY Quarter, Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, FileLoad_Date, Source_File_Date, LOB_Supported
  ),
  ranked AS (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY Quarter, QGP_Week, LOB_Supported
        ORDER BY FileLoad_Date DESC, Source_File_Date DESC
      ) AS rn
    FROM weekly_snapshots
  ),
  best AS (
    SELECT * FROM ranked WHERE rn = 1
  ),
  week_type AS (
    SELECT
      QGP_Week,
      CASE WHEN COUNT(DISTINCT Quarter) > 1 THEN 'boundary_week' ELSE 'normal' END AS week_type
    FROM best
    GROUP BY QGP_Week
  )
  SELECT
    b.Quarter, b.Week_Beginning_Monday, b.Week_Ending_Sunday, b.QGP_Week,
    b.FileLoad_Date, b.Source_File_Date, b.LOB_Supported, b.weekly_forecast, w.week_type
  FROM best b
  JOIN week_type w ON b.QGP_Week = w.QGP_Week
  WHERE b.weekly_forecast IS NOT NULL AND b.weekly_forecast != 0
  ;
END;

