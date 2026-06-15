
-- ============================================================
-- GOLD 2: GRANULAR — BigQuery
-- References sdi_mfc_silver_spendGranular_weekly (table, not view)
-- ============================================================
CREATE OR REPLACE VIEW
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_gold_spendGranular_weekly`
OPTIONS (strict_mode = false)
AS
SELECT
  Quarter,
  Quarter_Start_Date,
  Quarter_End_Date,
  Period_Start,
  Period_End,
  QGP_Week,
  FileLoad_Date,
  LOB_Supported,
  Channel,
  Tactic,
  Message_Type,
  Agency,
  spend_actual,
  spend_forecast,
  spend_display,
  spend_actual_for_wow,
  spend_for_wow,
  CASE
    WHEN spend_actual_for_wow IS NOT NULL
      AND spend_for_wow       IS NOT NULL
      AND spend_for_wow       != 0
      THEN ROUND(((spend_actual_for_wow - spend_for_wow) / spend_for_wow) * 100, 2)
    ELSE NULL
  END AS spend_actual_wow_pct,
  week_type,
  is_partial_week
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_mfc_silver_spendGranular_weekly`
;