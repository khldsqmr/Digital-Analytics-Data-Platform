
-- =============================================
-- SILVER: Spend Detail Weekly
-- Aggregates to final grain, passes through
-- all columns including spend_wow_ref and
-- exclude_wow_helper_from_display flag
-- =============================================
CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_pulseMFC_silver_spendDetail_weekly` AS

SELECT
  Actual_Quarter,
  Quarter,
  Period_Start,
  Period_End,
  QGP_Week,
  Quarter_End_Date,
  MAX(FileLoad_Date)                                              AS FileLoad_Date,
  LOB_Supported,
  Channel,
  Channel_Group,
  Tactic,
  Message_Type,
  Agency,
  SUM(spend_actual)                                               AS spend_actual,
  SUM(spend_forecast)                                             AS spend_forecast,
  SUM(spend_display)                                              AS spend_display,
  SUM(spend_wow_ref)                                              AS spend_wow_ref,
  week_type,
  period_days,
  exclude_wow_helper_from_display
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_pulseMFC_bronze_spendDetail_weekly`
GROUP BY
  Actual_Quarter,
  Quarter,
  Period_Start,
  Period_End,
  QGP_Week,
  Quarter_End_Date,
  LOB_Supported,
  Channel,
  Channel_Group,
  Tactic,
  Message_Type,
  Agency,
  week_type,
  period_days,
  exclude_wow_helper_from_display;

