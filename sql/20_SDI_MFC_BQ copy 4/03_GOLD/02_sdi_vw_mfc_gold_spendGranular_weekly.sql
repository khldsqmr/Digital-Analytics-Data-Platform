
-- ============================================================
-- GOLD 2 — GRANULAR — BigQuery
-- ============================================================
CREATE OR REPLACE VIEW
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_gold_spendGranular_weekly`
AS
SELECT
  cur.Quarter, cur.Week_Beginning_Monday, cur.Week_Ending_Sunday, cur.QGP_Week, cur.week_type,
  cur.LOB_Supported, cur.Channel, cur.Tactic, cur.Message_Type, cur.Agency,
  cur.Spend_Actual, cur.Spend_Forecast, cur.Spend_Final,
  cur.Spend_Final_FullWeek AS wow_numerator,
  wow.Spend_Final_FullWeek AS wow_denominator,
  cur.Spend_Final_FullWeek AS yoy_numerator,
  yoy.Spend_Final_FullWeek AS yoy_denominator
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_mfc_silver_spendGranular_weekly` cur
JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_dim_qgp_calendar` cal
  ON cur.QGP_Week = cal.qgp_date
LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_mfc_silver_spendGranular_weekly` wow
  ON wow.QGP_Week = cal.wow_prior_qgp_date AND wow.LOB_Supported = cur.LOB_Supported
 AND wow.Channel IS NOT DISTINCT FROM cur.Channel AND wow.Tactic IS NOT DISTINCT FROM cur.Tactic
 AND wow.Message_Type IS NOT DISTINCT FROM cur.Message_Type AND wow.Agency IS NOT DISTINCT FROM cur.Agency
LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_mfc_silver_spendGranular_weekly` yoy
  ON yoy.QGP_Week = cal.prior_year_qgp_date AND yoy.LOB_Supported = cur.LOB_Supported
 AND yoy.Channel IS NOT DISTINCT FROM cur.Channel AND yoy.Tactic IS NOT DISTINCT FROM cur.Tactic
 AND yoy.Message_Type IS NOT DISTINCT FROM cur.Message_Type AND yoy.Agency IS NOT DISTINCT FROM cur.Agency;