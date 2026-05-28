CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_gold_spend_weekly` AS

WITH deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        CONCAT(
          CASE WHEN CAST(SUBSTR(Quarter, 4, 2) AS INT64) < 50 THEN '20' ELSE '19' END,
          LPAD(SUBSTR(Quarter, 4, 2), 2, '0'), ' Q', SUBSTR(Quarter, 2, 1)
        ),
        CASE
          WHEN UPPER(week_type) = 'BOUNDARY_WEEK'
            AND Period_Start = Quarter_End_Date THEN Quarter_End_Date
          ELSE QGP_Week
        END,
        UPPER(TRIM(LOB_Supported))
      ORDER BY FileLoad_Date DESC
    ) AS rn
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_silver_spendGranular_weekly`
)

SELECT
  Quarter,
  Period_Start,
  Period_End,
  QGP_Week,
  Quarter_End_Date,
  FileLoad_Date,
  UPPER(TRIM(LOB_Supported))  AS LOB_Supported,
  SUM(weekly_actual)          AS spend_actual,
  SUM(weekly_forecast)        AS spend_forecast,
  SUM(weekly_display)         AS spend_display,
  UPPER(TRIM(week_type))      AS week_type
FROM deduped
WHERE rn = 1
GROUP BY
  Quarter,
  Period_Start,
  Period_End,
  QGP_Week,
  Quarter_End_Date,
  FileLoad_Date,
  UPPER(TRIM(LOB_Supported)),
  UPPER(TRIM(week_type))
ORDER BY LOB_Supported DESC, Quarter DESC, QGP_Week DESC;