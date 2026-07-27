CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_gold_spendGranularReAllocatedByQgp_weekly` AS

WITH latest_file AS (
  SELECT
    Quarter,
    QGP_Week,
    LOB_Supported,
    MAX(FileLoad_Date) AS latest_file_load_date
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_silver_spendGranular_weekly`
  GROUP BY Quarter, QGP_Week, LOB_Supported
),

silver_latest AS (
  SELECT s.*
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_silver_spendGranular_weekly` s
  JOIN latest_file f
    ON s.Quarter       = f.Quarter
   AND s.QGP_Week      = f.QGP_Week
   AND s.LOB_Supported = f.LOB_Supported
   AND s.FileLoad_Date = f.latest_file_load_date
),

daily_spend AS (
  SELECT
    FileLoad_Date,
    LOB_Supported,
    Channel,
    Tactic,
    Message_Type,
    Agency,
    Period_Start,
    Period_End,
    LAST_DAY(
      DATE(
        EXTRACT(YEAR FROM Period_End),
        CASE
          WHEN EXTRACT(MONTH FROM Period_End) <= 3  THEN 3
          WHEN EXTRACT(MONTH FROM Period_End) <= 6  THEN 6
          WHEN EXTRACT(MONTH FROM Period_End) <= 9  THEN 9
          ELSE 12
        END,
        1
      ), MONTH
    ) AS Quarter_End_Date,
    weekly_actual   / (DATE_DIFF(Period_End, Period_Start, DAY) + 1) AS daily_actual,
    weekly_forecast / (DATE_DIFF(Period_End, Period_Start, DAY) + 1) AS daily_forecast,
    weekly_display  / (DATE_DIFF(Period_End, Period_Start, DAY) + 1) AS daily_display,
    calendar_date
  FROM silver_latest
  CROSS JOIN UNNEST(
    GENERATE_DATE_ARRAY(Period_Start, Period_End, INTERVAL 1 DAY)
  ) AS calendar_date
  WHERE Period_End >= Period_Start
),

daily_with_qgp AS (
  SELECT
    FileLoad_Date,
    LOB_Supported,
    Channel,
    Tactic,
    Message_Type,
    Agency,
    Quarter_End_Date,
    daily_actual,
    daily_forecast,
    daily_display,
    calendar_date,
    CASE
      WHEN DATE_ADD(
             calendar_date,
             INTERVAL (
               CASE
                 WHEN EXTRACT(DAYOFWEEK FROM calendar_date) = 1 THEN 6
                 ELSE 7 - EXTRACT(DAYOFWEEK FROM calendar_date)
               END
             ) DAY
           ) > Quarter_End_Date
      THEN Quarter_End_Date
      ELSE DATE_ADD(
             calendar_date,
             INTERVAL (
               CASE
                 WHEN EXTRACT(DAYOFWEEK FROM calendar_date) = 1 THEN 6
                 ELSE 7 - EXTRACT(DAYOFWEEK FROM calendar_date)
               END
             ) DAY
           )
    END AS QGP_Date
  FROM daily_spend
),

aggregated AS (
  SELECT
    QGP_Date,
    MIN(calendar_date)            AS Period_Start,
    MAX(calendar_date)            AS Period_End,
    MAX(FileLoad_Date)            AS FileLoad_Date,
    LOB_Supported,
    Channel,
    Tactic,
    Message_Type,
    Agency,
    ROUND(SUM(daily_actual),   2) AS spend_actual,
    ROUND(SUM(daily_forecast), 2) AS spend_forecast,
    ROUND(SUM(daily_display),  2) AS spend_display
  FROM daily_with_qgp
  GROUP BY
    QGP_Date,
    LOB_Supported,
    Channel,
    Tactic,
    Message_Type,
    Agency
)

SELECT
  CONCAT(
    CAST(EXTRACT(YEAR FROM QGP_Date) AS STRING), ' Q',
    CAST(
      CASE
        WHEN EXTRACT(MONTH FROM QGP_Date) <= 3 THEN 1
        WHEN EXTRACT(MONTH FROM QGP_Date) <= 6 THEN 2
        WHEN EXTRACT(MONTH FROM QGP_Date) <= 9 THEN 3
        ELSE 4
      END AS STRING
    )
  ) AS Quarter,
  LAST_DAY(
    DATE(
      EXTRACT(YEAR FROM QGP_Date),
      CASE
        WHEN EXTRACT(MONTH FROM QGP_Date) <= 3  THEN 3
        WHEN EXTRACT(MONTH FROM QGP_Date) <= 6  THEN 6
        WHEN EXTRACT(MONTH FROM QGP_Date) <= 9  THEN 9
        ELSE 12
      END,
      1
    ), MONTH
  ) AS Quarter_End_Date,
  Period_Start,
  Period_End,
  QGP_Date,
  FileLoad_Date,
  LOB_Supported,
  Channel,
  Tactic,
  Message_Type,
  Agency,
  spend_actual,
  spend_forecast,
  spend_display
FROM aggregated
ORDER BY QGP_Date DESC, LOB_Supported, Channel, Tactic;