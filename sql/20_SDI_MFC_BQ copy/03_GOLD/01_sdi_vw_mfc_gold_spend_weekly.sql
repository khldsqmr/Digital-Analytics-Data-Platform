-- ============================================================
-- GOLD 1: NON-GRANULAR / LOB-LEVEL — BigQuery
-- ============================================================
CREATE OR REPLACE VIEW
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_gold_spend_weekly`
AS

SELECT
  Quarter, Quarter_Start_Date, Quarter_End_Date,
  Period_Start, Period_End, QGP_Week, FileLoad_Date,
  LOB_Supported,
  spend_actual, spend_forecast, spend_display,
  spend_actual_for_wow, spend_for_wow, spend_actual_wow_pct,
  week_type, is_partial_week
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_silver_spend_weekly`
;