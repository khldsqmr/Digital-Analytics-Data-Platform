CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_gold_spend_weekly` AS

SELECT
  Quarter,
  Period_Start,
  Period_End,
  QGP_Week,
  Quarter_End_Date,
  FileLoad_Date,
  UPPER(TRIM(LOB_Supported))     AS LOB_Supported,
  SUM(weekly_actual)             AS spend_actual,
  SUM(weekly_forecast)           AS spend_forecast,
  SUM(weekly_display)            AS spend_display,
  UPPER(TRIM(week_type))         AS week_type
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_silver_spendGranular_weekly`
GROUP BY
  Quarter,
  Period_Start,
  Period_End,
  QGP_Week,
  Quarter_End_Date,
  FileLoad_Date,
  UPPER(TRIM(LOB_Supported)),
  UPPER(TRIM(week_type))
ORDER BY
  LOB_Supported DESC,
  Quarter DESC,
  QGP_Week DESC;