-- ============================================================
-- BRONZE 1 — SPEND ACTUALS, NON-GRANULAR / LOB LEVEL
-- ============================================================
CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_mfc_bronze_spendActuals_weekly()
SQL SECURITY DEFINER
COMMENT 'Creates/refreshes sdi_tbl_mfc_bronze_spendActuals_weekly. Refreshed weekly. Uses latest-file-snapshot selection per (Quarter, QGP_Week, LOB) — no per-campaign fallback across files.'
BEGIN
  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_mfc_bronze_spendActuals_weekly
  USING DELTA
  COMMENT 'MFC Bronze Actuals (non-granular / LOB level). For each (Quarter, QGP_Week, LOB), uses only the single most recent file that has any actual data. Includes legacy LOB codes HSI (Broadband) and TBG (TFB), normalized to canonical values.'
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
      CASE
        WHEN UPPER(TRIM(LOB_Supported)) = 'HSI' THEN 'BROADBAND'
        WHEN UPPER(TRIM(LOB_Supported)) = 'TBG' THEN 'TFB'
        ELSE UPPER(TRIM(LOB_Supported))
      END AS LOB_Supported,
      Channel, Tactic, Message_Type,
      CASE
        WHEN LOWER(TRIM(Agency)) = 'ini' THEN 'Initiative'
        WHEN LOWER(TRIM(Agency)) IN ('in house', 'inhouse', 'internal', 'progact', 'search') THEN 'In-House (TMO)'
        WHEN Agency IS NULL THEN NULL
        ELSE TRIM(Agency)
      END AS Agency,
      Spend AS Spend_Actual
    FROM prdrzranalytics.lab42.raw_media_flowchart
    WHERE 1=1
      AND UPPER(TRIM(LOB_Supported)) IN ('CONSUMER POSTPAID', 'BROADBAND', 'TFB', 'HSI', 'TBG')
      AND UPPER(TRIM(WM_V_NWM)) = 'WORKING'
      AND Channel IS NOT NULL
      AND Channel NOT IN ('OTHER (do not use)', 'Non-Working', 'Budget Held')
      AND TRY_CAST(NULLIF(CAST(Week_Beginning_Monday AS STRING), 'None') AS DATE) IS NOT NULL
      AND TRY_CAST(NULLIF(CAST(QGP_Week AS STRING), 'None') AS DATE) IS NOT NULL
      AND TRY_CAST(CAST(FileLoad_Date AS STRING) AS DATE) IS NOT NULL
      --AND UPPER(TRIM(Message_Type)) NOT IN ('MICRO')
      --AND UPPER(TRIM(Message)) NOT IN ('SEM POSTPAID/MICRO', 'MICRO POSTPAID OFFERS')
      AND Quarter IS NOT NULL
      AND Quarter RLIKE "^Q[1-4]'[0-9]{2}$"
      AND UPPER(TRIM(QGP)) = 'ACTUAL'
      AND Spend IS NOT NULL
  ),
  weekly_snapshots AS (
    SELECT
      Quarter, Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week,
      FileLoad_Date, Source_File_Date, LOB_Supported, Channel, Tactic, Message_Type, Agency,
      SUM(Spend_Actual) AS weekly_actual
    FROM raw
    WHERE Week_Beginning_Monday <= Week_Ending_Sunday
    GROUP BY Quarter, Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, FileLoad_Date, Source_File_Date, LOB_Supported, Channel, Tactic, Message_Type, Agency
  ),
  latest_file_per_week AS (
    SELECT Quarter, QGP_Week, LOB_Supported, MAX(FileLoad_Date) AS latest_FileLoad_Date
    FROM weekly_snapshots
    GROUP BY Quarter, QGP_Week, LOB_Supported
  ),
  latest_source_file_per_week AS (
    SELECT ws.Quarter, ws.QGP_Week, ws.LOB_Supported, lf.latest_FileLoad_Date,
      MAX(ws.Source_File_Date) AS latest_Source_File_Date
    FROM weekly_snapshots ws
    JOIN latest_file_per_week lf
      ON ws.Quarter = lf.Quarter AND ws.QGP_Week = lf.QGP_Week AND ws.LOB_Supported = lf.LOB_Supported
     AND ws.FileLoad_Date = lf.latest_FileLoad_Date
    GROUP BY ws.Quarter, ws.QGP_Week, ws.LOB_Supported, lf.latest_FileLoad_Date
  ),
  best AS (
    SELECT ws.*
    FROM weekly_snapshots ws
    JOIN latest_source_file_per_week lsf
      ON ws.Quarter = lsf.Quarter AND ws.QGP_Week = lsf.QGP_Week AND ws.LOB_Supported = lsf.LOB_Supported
     AND ws.FileLoad_Date = lsf.latest_FileLoad_Date
     AND ws.Source_File_Date <=> lsf.latest_Source_File_Date
  ),
  rolled_up AS (
    SELECT
      Quarter, Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, LOB_Supported,
      SUM(weekly_actual) AS weekly_actual,
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
    r.FileLoad_Date, r.Source_File_Date, r.LOB_Supported, r.weekly_actual, w.week_type
  FROM rolled_up r
  JOIN week_type w ON r.QGP_Week = w.QGP_Week
  WHERE r.weekly_actual IS NOT NULL AND r.weekly_actual != 0
  ;
END;

