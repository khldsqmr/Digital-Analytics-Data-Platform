CREATE OR REPLACE PROCEDURE
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_sp_mfc_bronze_spendForecasts_weekly`()
BEGIN

  CREATE OR REPLACE TABLE
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_mfc_bronze_spendForecasts_weekly`
  OPTIONS (
    description = 'MFC Bronze Forecasts Weekly — latest forecast snapshot included regardless of actual arrival.'
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
        --'Unallocated',
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
  ),

  boundary_dates AS (
    SELECT
      QGP_Week,
      quarter_end_in_week
    FROM (
      SELECT DISTINCT
        QGP_Week,
        CASE
          WHEN DATE(EXTRACT(YEAR FROM Week_Beginning_Monday), 3, 31)
               BETWEEN Week_Beginning_Monday AND Week_Ending_Sunday
            THEN DATE(EXTRACT(YEAR FROM Week_Beginning_Monday), 3, 31)
          WHEN DATE(EXTRACT(YEAR FROM Week_Beginning_Monday), 6, 30)
               BETWEEN Week_Beginning_Monday AND Week_Ending_Sunday
            THEN DATE(EXTRACT(YEAR FROM Week_Beginning_Monday), 6, 30)
          WHEN DATE(EXTRACT(YEAR FROM Week_Beginning_Monday), 9, 30)
               BETWEEN Week_Beginning_Monday AND Week_Ending_Sunday
            THEN DATE(EXTRACT(YEAR FROM Week_Beginning_Monday), 9, 30)
          WHEN DATE(EXTRACT(YEAR FROM Week_Beginning_Monday), 12, 31)
               BETWEEN Week_Beginning_Monday AND Week_Ending_Sunday
            THEN DATE(EXTRACT(YEAR FROM Week_Beginning_Monday), 12, 31)
        END AS quarter_end_in_week
      FROM weekly_snapshots
    )
    WHERE quarter_end_in_week IS NOT NULL
  ),

  boundary_with_forecast AS (
    SELECT *
    FROM (
      SELECT
        b.Quarter AS source_quarter,
        b.QGP_Week,
        b.Week_Beginning_Monday,
        b.Week_Ending_Sunday,
        b.FileLoad_Date,
        b.LOB_Supported,
        b.weekly_forecast AS source_forecast,
        bd.quarter_end_in_week,

        CASE
          WHEN b.Week_Beginning_Monday <= bd.quarter_end_in_week
            THEN DATE_DIFF(bd.quarter_end_in_week, b.Week_Beginning_Monday, DAY) + 1
          ELSE DATE_DIFF(b.Week_Ending_Sunday, bd.quarter_end_in_week, DAY)
        END AS source_days,

        CASE
          WHEN b.Week_Beginning_Monday <= bd.quarter_end_in_week
            THEN DATE_DIFF(b.Week_Ending_Sunday, bd.quarter_end_in_week, DAY)
          ELSE DATE_DIFF(bd.quarter_end_in_week, b.Week_Beginning_Monday, DAY) + 1
        END AS missing_days

      FROM best b
      JOIN week_type w
        ON b.QGP_Week = w.QGP_Week
       AND w.week_type = 'boundary_week'
      JOIN boundary_dates bd
        ON b.QGP_Week = bd.QGP_Week
      WHERE NOT EXISTS (
        SELECT 1
        FROM best ob
        WHERE ob.QGP_Week = b.QGP_Week
          AND ob.Quarter != b.Quarter
          AND ob.LOB_Supported = b.LOB_Supported
      )
    )
    WHERE source_days > 0
      AND missing_days > 0
  ),

  derived_forecasts AS (
    SELECT
      aq.Quarter,
      bwf.Week_Beginning_Monday,
      bwf.Week_Ending_Sunday,
      bwf.QGP_Week,
      bwf.FileLoad_Date,
      bwf.LOB_Supported,
      ROUND((bwf.source_forecast / bwf.source_days) * bwf.missing_days, 2) AS weekly_forecast,
      'boundary_week' AS week_type,
      TRUE AS is_derived
    FROM boundary_with_forecast bwf
    JOIN actuals_quarters aq
      ON aq.QGP_Week = bwf.QGP_Week
     AND aq.Quarter != bwf.source_quarter
    LEFT JOIN best existing
      ON existing.QGP_Week = bwf.QGP_Week
     AND existing.Quarter = aq.Quarter
     AND existing.LOB_Supported = bwf.LOB_Supported
    WHERE existing.Quarter IS NULL
  )

  SELECT *
  FROM (
    SELECT
      b.Quarter,
      b.Week_Beginning_Monday,
      b.Week_Ending_Sunday,
      b.QGP_Week,
      b.FileLoad_Date,
      b.LOB_Supported,
      b.weekly_forecast,
      w.week_type,
      FALSE AS is_derived
    FROM best b
    JOIN week_type w
      ON b.QGP_Week = w.QGP_Week

    UNION ALL

    SELECT
      Quarter,
      Week_Beginning_Monday,
      Week_Ending_Sunday,
      QGP_Week,
      FileLoad_Date,
      LOB_Supported,
      weekly_forecast,
      week_type,
      is_derived
    FROM derived_forecasts
  )
  WHERE weekly_forecast IS NOT NULL
    AND weekly_forecast != 0
  ;

END;
