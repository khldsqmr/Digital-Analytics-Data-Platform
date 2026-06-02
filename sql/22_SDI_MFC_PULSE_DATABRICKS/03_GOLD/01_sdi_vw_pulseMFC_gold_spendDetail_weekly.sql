
-- =============================================
-- GOLD: Spend Detail Weekly
-- Final Tableau-ready view
--
-- KEY COLUMNS:
--   Actual_Quarter  — real quarter for display
--                     and quarterly totals
--   Quarter         — adjusted quarter for
--                     parameter filter
--   spend_actual    — actual spend (NULL for helpers)
--   spend_forecast  — forecast spend (NULL for helpers)
--   spend_display   — display spend (NULL for helpers)
--   spend_wow_ref   — WoW reference value:
--                     normal = spend_actual
--                     saturday boundary = combined
--                       full week actual
--                     quarter-end boundary = NULL
--                     helper rows = reference value
--   is_wow_helper   — TRUE for helper rows
--   exclude_wow_helper_from_display — always FALSE
--
-- HOW TO USE IN TABLEAU:
-- 1. Quarter parameter filter:
--    [Quarter] = [p.Year Quarter]
--    DO NOT filter on Actual_Quarter
-- 2. DO NOT put is_wow_helper on Filters shelf
--    — keeps all rows in same partition for LOOKUP
-- 3. Add is_wow_helper to Detail marks card only
-- 4. Use SUM([spend_actual]) directly for display
--    — helper rows have NULL so no inflation
-- 5. WoW calculated field:
--    IF NOT ATTR([is_wow_helper])
--      AND NOT ISNULL(LOOKUP(SUM([spend_wow_ref]), -1))
--      AND LOOKUP(SUM([spend_wow_ref]), -1) <> 0
--    THEN
--      (SUM([spend_wow_ref]) - LOOKUP(SUM([spend_wow_ref]), -1))
--      / ABS(LOOKUP(SUM([spend_wow_ref]), -1))
--    ELSE NULL
--    END
-- 6. Table calculation: Compute using QGP_Week
-- 7. Use Actual_Quarter for quarterly totals
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
  is_wow_helper,
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