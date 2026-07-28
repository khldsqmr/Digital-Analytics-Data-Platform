-- ============================================================
-- GOLD 1: NON-GRANULAR / LOB-LEVEL — BigQuery
-- ============================================================
CREATE OR REPLACE VIEW
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_gold_spend_weekly`
AS
SELECT
  cur.Quarter, cur.Week_Beginning_Monday, cur.Week_Ending_Sunday, cur.QGP_Week, cur.week_type,
  cur.LOB_Supported, cur.Spend_Actual, cur.Spend_Forecast, cur.Spend_Status,
  GREATEST(COALESCE(cur.Actual_FileLoad_Date, DATE '1900-01-01'),
           COALESCE(cur.Forecast_FileLoad_Date, DATE '1900-01-01')) AS FileLoad_Date,
  cur.Actual_FileLoad_Date, cur.Forecast_FileLoad_Date,
  cur.Spend_Actual_FullWeek AS actual_wow_numerator,
  wow.Spend_Actual_FullWeek AS actual_wow_denominator,
  cur.Spend_Actual_FullWeek AS actual_yoy_numerator,
  yoy.Spend_Actual_FullWeek AS actual_yoy_denominator,
  cur.Spend_Forecast_FullWeek AS forecast_wow_numerator,
  wow.Spend_Forecast_FullWeek AS forecast_wow_denominator,
  cur.Spend_Forecast_FullWeek AS forecast_yoy_numerator,
  yoy.Spend_Forecast_FullWeek AS forecast_yoy_denominator
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_mfc_silver_spend_weekly` cur
JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_dim_qgp_calendar` cal
  ON cur.QGP_Week = cal.qgp_date
LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_mfc_silver_spend_weekly` wow
  ON wow.QGP_Week = cal.wow_prior_qgp_date AND wow.LOB_Supported = cur.LOB_Supported
LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_mfc_silver_spend_weekly` yoy
  ON yoy.QGP_Week = cal.prior_year_qgp_date AND yoy.LOB_Supported = cur.LOB_Supported;

