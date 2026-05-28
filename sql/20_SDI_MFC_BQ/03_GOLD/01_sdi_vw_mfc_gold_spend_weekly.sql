CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_gold_spend_weekly` AS

WITH deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        Quarter,
        QGP_Week,
        LOB_Supported,
        Channel,
        Tactic,
        Message_Type,
        Agency
      ORDER BY FileLoad_Date DESC
    ) AS rn
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_silver_spendGranular_weekly`
),

latest AS (
  SELECT * FROM deduped WHERE rn = 1
)

SELECT
  Quarter,
  MIN(Period_Start)                                                          AS Period_Start,
  MAX(Period_End)                                                            AS Period_End,
  QGP_Week,
  Quarter_End_Date,
  MAX(FileLoad_Date)                                                         AS FileLoad_Date,
  LOB_Supported,
  NULLIF(SUM(weekly_actual), 0)                                              AS spend_actual,
  NULLIF(SUM(weekly_forecast), 0)                                            AS spend_forecast,
  COALESCE(NULLIF(SUM(weekly_actual), 0), NULLIF(SUM(weekly_forecast), 0))  AS spend_display,
  week_type
FROM latest
GROUP BY
  Quarter,
  QGP_Week,
  Quarter_End_Date,
  LOB_Supported,
  week_type
ORDER BY LOB_Supported DESC, Quarter DESC, QGP_Week DESC;