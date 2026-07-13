-- ============================================================
-- BRONZE 1: ACTUALS - NON-GRANULAR / LOB LEVEL — BigQuery
-- Uses TRUE raw FileLoad_Date for latest snapshot selection
-- ============================================================

CREATE OR REPLACE PROCEDURE
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_sp_mfc_bronze_spendActuals_weekly`()
BEGIN

  CREATE OR REPLACE TABLE
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_mfc_bronze_spendActuals_weekly`
  OPTIONS (
    description = 'MFC Bronze Actuals Weekly — refreshed via sdi_sp_mfc_bronze_spendActuals_weekly.'
  )
  AS

  WITH raw AS (
    SELECT
      Quarter,
      SAFE_CAST(Week_Beginning_Monday AS DATE) AS Week_Beginning_Monday,
      SAFE_CAST(Week_Ending_Sunday AS DATE) AS Week_Ending_Sunday,
      SAFE_CAST(QGP_Week AS DATE) AS QGP_Week,

      SAFE_CAST(CAST(FileLoad_Date AS STRING) AS DATE) AS FileLoad_Date,
      SAFE_CAST(File_Date AS DATE) AS Source_File_Date,

      UPPER(TRIM(LOB_Supported)) AS LOB_Supported,

      CASE
        WHEN UPPER(TRIM(QGP)) = 'ACTUAL' THEN Spend
        ELSE NULL
      END AS Spend_Actual

    FROM `prj-dbi-prd-1.ds_dbi_marketing.ma_mfc_raw`
    WHERE UPPER(TRIM(LOB_Supported)) IN ('CONSUMER POSTPAID', 'BROADBAND')
      AND UPPER(TRIM(WM_NWM)) = 'WORKING'
      AND Channel IS NOT NULL
      AND Channel NOT IN (
        'OTHER (do not use)',
        'Non-Working',
        'Unallocated',
        'Budget Held'
      )
      AND Week_Beginning_Monday IS NOT NULL
      AND Week_Beginning_Monday != 'None'
      AND Week_Ending_Sunday IS NOT NULL
      AND Week_Ending_Sunday != 'None'
      AND QGP_Week IS NOT NULL
      AND QGP_Week != 'None'
      AND SAFE_CAST(Week_Beginning_Monday AS DATE) IS NOT NULL
      AND SAFE_CAST(Week_Ending_Sunday AS DATE) IS NOT NULL
      AND SAFE_CAST(QGP_Week AS DATE) IS NOT NULL
      AND SAFE_CAST(CAST(FileLoad_Date AS STRING) AS DATE) IS NOT NULL
      AND UPPER(TRIM(Message_Type)) NOT IN ('MICRO')
      AND UPPER(TRIM(Message)) NOT IN (
        'SEM POSTPAID/MICRO',
        'MICRO POSTPAID OFFERS'
      )
      AND Quarter IS NOT NULL
      AND REGEXP_CONTAINS(Quarter, r"^Q[1-4]'[0-9]{2}$")
      AND UPPER(TRIM(QGP)) = 'ACTUAL'
      AND Spend IS NOT NULL
  ),

  weekly_snapshots AS (
    SELECT
      Quarter,
      Week_Beginning_Monday,
      Week_Ending_Sunday,
      QGP_Week,
      FileLoad_Date,
      Source_File_Date,
      LOB_Supported,
      SUM(Spend_Actual) AS weekly_actual
    FROM raw
    WHERE Week_Beginning_Monday <= Week_Ending_Sunday
    GROUP BY
      Quarter,
      Week_Beginning_Monday,
      Week_Ending_Sunday,
      QGP_Week,
      FileLoad_Date,
      Source_File_Date,
      LOB_Supported
  ),

  ranked AS (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY Quarter, QGP_Week, LOB_Supported
        ORDER BY FileLoad_Date DESC, Source_File_Date DESC
      ) AS rn
    FROM weekly_snapshots
  ),

  best AS (
    SELECT *
    FROM ranked
    WHERE rn = 1
  ),

  week_type AS (
    SELECT
      QGP_Week,
      CASE
        WHEN COUNT(DISTINCT Quarter) > 1 THEN 'boundary_week'
        ELSE 'normal'
      END AS week_type
    FROM best
    GROUP BY QGP_Week
  )

  SELECT
    b.Quarter,
    b.Week_Beginning_Monday,
    b.Week_Ending_Sunday,
    b.QGP_Week,
    b.FileLoad_Date,
    b.LOB_Supported,
    b.weekly_actual,
    w.week_type
  FROM best b
  JOIN week_type w
    ON b.QGP_Week = w.QGP_Week
  WHERE b.weekly_actual IS NOT NULL
    AND b.weekly_actual != 0
  ;

END;

