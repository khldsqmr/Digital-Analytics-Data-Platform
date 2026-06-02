
-- =============================================
-- GOLD: Spend Detail Weekly
-- Final Tableau-ready view
--
-- KEY COLUMNS:
--   Actual_Quarter  — real quarter, use for
--                     display and quarterly totals
--   Quarter         — adjusted quarter, use for
--                     parameter filter
--   spend_actual    — actual spend only
--   spend_forecast  — forecast spend only
--   spend_display   — actuals if available,
--                     else forecast
--   spend_wow_ref   — for WoW calculation:
--                     normal weeks = spend_actual
--                     saturday boundary = combined
--                       full week actual spend
--                     quarter-end boundary = NULL
--                     helper rows = spend_actual
--
-- HOW TO USE IN TABLEAU:
-- 1. Parameter filter uses [Quarter] (adjusted)
--    NOT [Actual_Quarter]
-- 2. Filter [exclude_wow_helper_from_display]
--    set to TRUE — DO NOT add to context
--    Hides helpers but keeps them for LOOKUP
-- 3. Use [Actual_Quarter] for quarterly totals
--    and display
-- 4. WoW calculated field:
--    IF ATTR([week_type]) = 'BOUNDARY_WEEK'
--      AND ATTR([QGP_Week]) = ATTR([Quarter_End_Date])
--    THEN NULL
--    ELSEIF NOT ISNULL(LOOKUP(SUM([spend_wow_ref]), -1))
--      AND LOOKUP(SUM([spend_wow_ref]), -1) <> 0
--    THEN
--      (SUM([spend_wow_ref]) - LOOKUP(SUM([spend_wow_ref]), -1))
--      / ABS(LOOKUP(SUM([spend_wow_ref]), -1))
--    ELSE NULL
--    END
-- =============================================
CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_pulseMFC_gold_spendDetail_weekly AS

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
  spend_wow_ref,
  week_type,
  exclude_wow_helper_from_display
FROM prdrzranalytics.lab42.sdi_vw_pulseMFC_silver_spendDetail_weekly
ORDER BY
  Quarter DESC,
  QGP_Week DESC,
  LOB_Supported,
  Channel_Group,
  Channel,
  Tactic,
  Message_Type,
  Agency;