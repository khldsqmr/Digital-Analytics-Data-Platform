-- =============================================
-- GOLD: Spend Detail Weekly
-- Final Tableau-ready view
--
-- HOW TO USE IN TABLEAU:
-- 1. Connect to this view as data source
-- 2. Quarter parameter filter:
--    Use [Quarter] column (adjusted) — NOT [Actual_Quarter]
--    This ensures WoW helper rows are included
-- 3. Add [exclude_wow_helper_from_display] to Filters
--    Set to TRUE — DO NOT add to context
--    This hides helper rows from display
--    but keeps them visible to LOOKUP
-- 4. Use [Actual_Quarter] for quarterly totals
--    and quarter-level display
-- 5. WoW calculated field in Tableau:
--    IF ATTR([week_type]) = 'BOUNDARY_WEEK'
--      AND ATTR([QGP_Week]) = ATTR([Quarter_End_Date])
--    THEN NULL
--    ELSEIF NOT ISNULL(LOOKUP(SUM([spend_actual]), -1))
--      AND LOOKUP(SUM([spend_actual]), -1) <> 0
--    THEN
--      (SUM([spend_actual]) - LOOKUP(SUM([spend_actual]), -1))
--      / ABS(LOOKUP(SUM([spend_actual]), -1))
--    ELSE NULL
--    END
-- =============================================
CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_pulseMFC_gold_spendDetail_weekly` AS

SELECT
  Actual_Quarter,
  Quarter,
  Period_Start,
  Period_End,
  QGP_Week,
  Quarter_End_Date,
  FileLoad_Date,
  LOB_Supported,
  Channel,
  Channel_Group,
  Tactic,
  Message_Type,
  Agency,
  spend_actual,
  spend_forecast,
  spend_display,
  week_type,
  exclude_wow_helper_from_display
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_pulseMFC_silver_spendDetail_weekly`
ORDER BY
  Quarter DESC,
  QGP_Week DESC,
  LOB_Supported,
  Channel_Group,
  Channel,
  Tactic,
  Message_Type,
  Agency;