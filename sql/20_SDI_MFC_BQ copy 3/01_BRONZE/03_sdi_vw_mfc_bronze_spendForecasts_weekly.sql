
-- ============================================================
-- BRONZE 3 — SPEND FORECAST, NON-GRANULAR / LOB LEVEL — BigQuery
-- ============================================================
CREATE OR REPLACE PROCEDURE
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_sp_mfc_bronze_spendForecast_weekly`()
OPTIONS (strict_mode=false)
BEGIN
  CREATE OR REPLACE TABLE
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_mfc_bronze_spendForecast_weekly`
  OPTIONS (
    description = 'MFC Bronze Forecast (non-granular / LOB level). For each (Quarter, QGP_Week, LOB), uses only the single most recent file that has any forecast data. Includes legacy LOB codes HSI (Broadband) and TBG (TFB), normalized to canonical values.'
  )
  AS
  WITH raw AS (
    SELECT
      Quarter,
      SAFE_CAST(NULLIF(CAST(Week_Beginning_Monday AS STRING), 'None') AS DATE) AS Week_Beginning_Monday,
      COALESCE(
        SAFE_CAST(NULLIF(CAST(Week_Ending_Sunday AS STRING), 'None') AS DATE),
        DATE_ADD(SAFE_CAST(NULLIF(CAST(Week_Beginning_Monday AS STRING), 'None') AS DATE), INTERVAL 6 DAY)
      ) AS Week_Ending_Sunday,
      SAFE_CAST(NULLIF(CAST(QGP_Week AS STRING), 'None') AS DATE) AS QGP_Week,
      SAFE_CAST(CAST(FileLoad_Date AS STRING) AS DATE) AS FileLoad_Date,
      SAFE_CAST(File_Date AS DATE) AS Source_File_Date,
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
      Spend AS Spend_Forecast
    FROM `prj-dbi-prd-1.ds_dbi_marketing.ma_mfc_raw`
    WHERE 1=1
      AND UPPER(TRIM(LOB_Supported)) IN ('CONSUMER POSTPAID', 'BROADBAND', 'TFB', 'HSI', 'TBG')
      AND UPPER(TRIM(WM_NWM)) = 'WORKING'
      AND Channel IS NOT NULL
      AND Channel NOT IN ('OTHER (do not use)', 'Non-Working', 'Budget Held')
      AND SAFE_CAST(NULLIF(CAST(Week_Beginning_Monday AS STRING), 'None') AS DATE) IS NOT NULL
      AND SAFE_CAST(NULLIF(CAST(QGP_Week AS STRING), 'None') AS DATE) IS NOT NULL
      AND SAFE_CAST(CAST(FileLoad_Date AS STRING) AS DATE) IS NOT NULL
      --AND UPPER(TRIM(Message_Type)) NOT IN ('MICRO')
      --AND UPPER(TRIM(Message)) NOT IN ('SEM POSTPAID/MICRO', 'MICRO POSTPAID OFFERS')
      AND Quarter IS NOT NULL
      AND REGEXP_CONTAINS(Quarter, r"^Q[1-4]'[0-9]{2}$")
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
     AND ws.Source_File_Date = lsf.latest_Source_File_Date
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

