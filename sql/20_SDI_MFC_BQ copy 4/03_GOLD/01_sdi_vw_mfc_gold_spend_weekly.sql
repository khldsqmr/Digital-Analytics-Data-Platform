-- ============================================================
-- GOLD 1 — NON-GRANULAR / LOB LEVEL (WoW / YoY) — BigQuery
-- ============================================================
CREATE OR REPLACE VIEW
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_gold_spend_weekly`
AS
SELECT
  cur.Quarter, cur.Week_Beginning_Monday, cur.Week_Ending_Sunday, cur.QGP_Week, cur.week_type,
  cur.LOB_Supported, cur.Spend_Actual, cur.Spend_Forecast, cur.Spend_Final,
  wow.Spend_Final_FullWeek AS Spend_Final_Prior_Week,
  cur.Spend_Final_FullWeek - wow.Spend_Final_FullWeek AS WoW_Delta,
  (cur.Spend_Final_FullWeek - wow.Spend_Final_FullWeek) / NULLIF(wow.Spend_Final_FullWeek, 0) AS WoW_Pct,
  yoy.Spend_Final_FullWeek AS Spend_Final_Prior_Year,
  cur.Spend_Final_FullWeek - yoy.Spend_Final_FullWeek AS YoY_Delta,
  (cur.Spend_Final_FullWeek - yoy.Spend_Final_FullWeek) / NULLIF(yoy.Spend_Final_FullWeek, 0) AS YoY_Pct
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_mfc_silver_spend_weekly` cur
JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_dim_qgp_calendar` cal
  ON cur.QGP_Week = cal.qgp_date
LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_mfc_silver_spend_weekly` wow
  ON wow.QGP_Week = cal.wow_prior_qgp_date AND wow.LOB_Supported = cur.LOB_Supported
LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_mfc_silver_spend_weekly` yoy
  ON yoy.QGP_Week = cal.prior_year_qgp_date AND yoy.LOB_Supported = cur.LOB_Supported;

