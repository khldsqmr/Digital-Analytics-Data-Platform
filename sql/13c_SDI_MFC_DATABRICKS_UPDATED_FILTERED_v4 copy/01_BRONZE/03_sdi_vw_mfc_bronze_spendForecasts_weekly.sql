
-- ============================================================
-- BRONZE 3 — SPEND FORECAST, NON-GRANULAR / LOB LEVEL (unchanged)
-- ============================================================
CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_mfc_bronze_spendForecast_weekly()
SQL SECURITY DEFINER
COMMENT 'Creates/refreshes sdi_tbl_mfc_bronze_spendForecast_weekly. Refreshed weekly.'
BEGIN
  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_mfc_bronze_spendForecast_weekly
  USING DELTA
  COMMENT 'MFC Bronze Forecast (non-granular / LOB level), ranked at campaign grain then rolled up — refreshed via sdi_sp_mfc_bronze_spendForecast_weekly.'
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
      Channel, Tactic, Message_Type,
      CASE
        WHEN LOWER(TRIM(Agency)) = 'ini' THEN 'Initiative'
        WHEN LOWER(TRIM(Agency)) IN ('in house', 'inhouse', 'internal', 'progact', 'search') THEN 'In-House (TMO)'
        WHEN Agency IS NULL THEN NULL
        ELSE TRIM(Agency)
      END AS Agency,
      Spend AS Spend_Forecast
    FROM prdrzranalytics.lab42.raw_media_flowchart
    WHERE 1=1
      AND UPPER(TRIM(LOB_Supported)) IN ('CONSUMER POSTPAID', 'BROADBAND', 'TFB')
      AND UPPER(TRIM(WM_V_NWM)) = 'WORKING'
      AND Channel IS NOT NULL
      AND Channel NOT IN ('OTHER (do not use)', 'Non-Working', 'Budget Held')
      AND TRY_CAST(NULLIF(CAST(Week_Beginning_Monday AS STRING), 'None') AS DATE) IS NOT NULL
      AND TRY_CAST(NULLIF(CAST(QGP_Week AS STRING), 'None') AS DATE) IS NOT NULL
      AND TRY_CAST(CAST(FileLoad_Date AS STRING) AS DATE) IS NOT NULL
      AND UPPER(TRIM(Message_Type)) NOT IN ('MICRO')
      AND UPPER(TRIM(Message)) NOT IN ('SEM POSTPAID/MICRO', 'MICRO POSTPAID OFFERS')
      AND Quarter IS NOT NULL
      AND Quarter RLIKE "^Q[1-4]'[0-9]{2}$"
      AND UPPER(TRIM(QGP)) = 'FORECAST'
      AND Spend IS NOT NULL
  ),
  weekly_snapshots AS (
    SELECT
      Quarter, Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week,
      FileLoad_Date, Source_File_Date, LOB_Supported, Channel, Tactic, Message_Type, Agency,
      SUM(Spend_Forecast) AS weekly_forecast
    FROM raw
    WHERE Week_Beginning_Monday <= Week_Ending_Sunday
    GROUP BY Quarter, Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, FileLoad_Date, Source_File_Date, LOB_Supported, Channel, Tactic, Message_Type, Agency
  ),
  ranked AS (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY Quarter, QGP_Week, LOB_Supported, Channel, Tactic, Message_Type, Agency
        ORDER BY FileLoad_Date DESC, Source_File_Date DESC
      ) AS rn
    FROM weekly_snapshots
  ),
  best AS (
    SELECT * FROM ranked WHERE rn = 1
  ),
  rolled_up AS (
    SELECT
      Quarter, Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, LOB_Supported,
      SUM(weekly_forecast) AS weekly_forecast,
      MAX(FileLoad_Date) AS FileLoad_Date,
      MAX(Source_File_Date) AS Source_File_Date
    FROM best
    GROUP BY Quarter, Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, LOB_Supported
  ),
  week_type AS (
    SELECT
      QGP_Week,
      CASE WHEN COUNT(DISTINCT Quarter) > 1 THEN 'boundary_week' ELSE 'normal' END AS week_type
    FROM rolled_up
    GROUP BY QGP_Week
  )
  SELECT
    r.Quarter, r.Week_Beginning_Monday, r.Week_Ending_Sunday, r.QGP_Week,
    r.FileLoad_Date, r.Source_File_Date, r.LOB_Supported, r.weekly_forecast, w.week_type
  FROM rolled_up r
  JOIN week_type w ON r.QGP_Week = w.QGP_Week
  WHERE r.weekly_forecast IS NOT NULL AND r.weekly_forecast != 0
  ;
END;

