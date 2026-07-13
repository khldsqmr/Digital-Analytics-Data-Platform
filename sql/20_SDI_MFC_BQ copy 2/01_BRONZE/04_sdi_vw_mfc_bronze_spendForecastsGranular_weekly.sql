
-- ============================================================
-- BRONZE 4: FORECASTS - GRANULAR
-- ============================================================

CREATE OR REPLACE PROCEDURE
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_sp_mfc_bronze_spendForecastsGranular_weekly`()
BEGIN

  CREATE OR REPLACE TABLE
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_mfc_bronze_spendForecastsGranular_weekly`
  OPTIONS (
    description = 'MFC Bronze Forecasts Granular Weekly'
  )
  AS

  WITH raw AS (
    SELECT
      Quarter,

      SAFE_CAST(NULLIF(CAST(Week_Beginning_Monday AS STRING), 'None') AS DATE) AS Week_Beginning_Monday,

      COALESCE(
        SAFE_CAST(NULLIF(CAST(Week_Ending_Sunday AS STRING), 'None') AS DATE),
        DATE_ADD(
          SAFE_CAST(NULLIF(CAST(Week_Beginning_Monday AS STRING), 'None') AS DATE),
          INTERVAL 6 DAY
        )
      ) AS Week_Ending_Sunday,

      SAFE_CAST(NULLIF(CAST(QGP_Week AS STRING), 'None') AS DATE) AS QGP_Week,

      SAFE_CAST(CAST(FileLoad_Date AS STRING) AS DATE) AS FileLoad_Date,
      SAFE_CAST(File_Date AS DATE) AS Source_File_Date,

      UPPER(TRIM(LOB_Supported)) AS LOB_Supported,
      Channel,
      Tactic,
      Message_Type,

      CASE
        WHEN LOWER(TRIM(Agency)) = 'ini'
          THEN 'Initiative'
        WHEN LOWER(TRIM(Agency)) IN ('in house', 'inhouse', 'internal', 'progact', 'search')
          THEN 'In-House (TMO)'
        WHEN Agency IS NULL
          THEN NULL
        ELSE TRIM(Agency)
      END AS Agency,

      CASE
        WHEN UPPER(TRIM(QGP)) = 'FORECAST' THEN Spend
        ELSE NULL
      END AS Spend_Forecast,

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
      AND SAFE_CAST(NULLIF(CAST(Week_Beginning_Monday AS STRING), 'None') AS DATE) IS NOT NULL
      AND SAFE_CAST(NULLIF(CAST(QGP_Week AS STRING), 'None') AS DATE) IS NOT NULL
      AND SAFE_CAST(CAST(FileLoad_Date AS STRING) AS DATE) IS NOT NULL
      AND UPPER(TRIM(Message_Type)) NOT IN ('MICRO')
      AND UPPER(TRIM(Message)) NOT IN (
        'SEM POSTPAID/MICRO',
        'MICRO POSTPAID OFFERS'
      )
      AND Quarter IS NOT NULL
      AND REGEXP_CONTAINS(Quarter, r"^Q[1-4]'[0-9]{2}$")
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
      Channel,
      Tactic,
      Message_Type,
      Agency,
      SUM(Spend_Forecast) AS weekly_forecast
    FROM raw
    WHERE Week_Beginning_Monday <= Week_Ending_Sunday
      AND Spend_Forecast IS NOT NULL
    GROUP BY
      Quarter,
      Week_Beginning_Monday,
      Week_Ending_Sunday,
      QGP_Week,
      FileLoad_Date,
      Source_File_Date,
      LOB_Supported,
      Channel,
      Tactic,
      Message_Type,
      Agency
  ),

  first_actual_date AS (
    SELECT
      Quarter,
      QGP_Week,
      LOB_Supported,
      Channel,
      Tactic,
      Message_Type,
      Agency,
      MIN(FileLoad_Date) AS first_actual_file_load_date
    FROM raw
    WHERE Week_Beginning_Monday <= Week_Ending_Sunday
      AND Spend_Actual IS NOT NULL
      AND Spend_Actual != 0
    GROUP BY
      Quarter,
      QGP_Week,
      LOB_Supported,
      Channel,
      Tactic,
      Message_Type,
      Agency
  ),

  ranked AS (
    SELECT
      s.*,
      ROW_NUMBER() OVER (
        PARTITION BY
          s.Quarter,
          s.QGP_Week,
          s.LOB_Supported,
          s.Channel,
          s.Tactic,
          s.Message_Type,
          s.Agency
        ORDER BY s.FileLoad_Date DESC, s.Source_File_Date DESC
      ) AS rn
    FROM weekly_snapshots s
    LEFT JOIN first_actual_date a
      ON  s.Quarter = a.Quarter
      AND s.QGP_Week = a.QGP_Week
      AND s.LOB_Supported = a.LOB_Supported
      AND s.Channel = a.Channel
      AND s.Tactic = a.Tactic
      AND s.Message_Type = a.Message_Type
      AND s.Agency IS NOT DISTINCT FROM a.Agency
    WHERE a.first_actual_file_load_date IS NULL
       OR s.FileLoad_Date < a.first_actual_file_load_date
  ),

  best AS (
    SELECT *
    FROM ranked
    WHERE rn = 1
  ),

  actuals_quarters AS (
    SELECT DISTINCT
      Quarter,
      QGP_Week
    FROM raw
    WHERE Week_Beginning_Monday <= Week_Ending_Sunday
      AND Spend_Actual IS NOT NULL
      AND Spend_Actual != 0
  ),

  week_type AS (
    SELECT
      QGP_Week,
      CASE
        WHEN COUNT(DISTINCT Quarter) > 1 THEN 'boundary_week'
        ELSE 'normal'
      END AS week_type
    FROM (
      SELECT Quarter, QGP_Week FROM best
      UNION DISTINCT
      SELECT Quarter, QGP_Week FROM actuals_quarters
    )
    GROUP BY QGP_Week
  )

  SELECT
    b.Quarter,
    b.Week_Beginning_Monday,
    b.Week_Ending_Sunday,
    b.QGP_Week,
    b.FileLoad_Date,
    b.LOB_Supported,
    b.Channel,
    b.Tactic,
    b.Message_Type,
    b.Agency,
    b.weekly_forecast,
    w.week_type,
    FALSE AS is_derived
  FROM best b
  JOIN week_type w
    ON b.QGP_Week = w.QGP_Week
  WHERE b.weekly_forecast IS NOT NULL
    AND b.weekly_forecast != 0
  ;

END;
