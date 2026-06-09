
-- ============================================================
-- VALIDATION 1:
-- NON-GRANULAR SILVER MUST HAVE ZERO DUPLICATE KEYS
-- ============================================================
SELECT
  QGP_Week,
  LOB_Supported,
  COUNT(*) AS row_count
FROM prdrzranalytics.lab42.sdi_vw_mfc_silver_spend_weekly
GROUP BY
  QGP_Week,
  LOB_Supported
HAVING COUNT(*) > 1
ORDER BY row_count DESC;


-- ============================================================
-- VALIDATION 2:
-- GRANULAR SILVER MUST HAVE ZERO DUPLICATE KEYS
-- ============================================================
SELECT
  QGP_Week,
  LOB_Supported,
  Channel,
  Tactic,
  Message_Type,
  Agency,
  COUNT(*) AS row_count
FROM prdrzranalytics.lab42.sdi_vw_mfc_silver_spendGranular_weekly
GROUP BY
  QGP_Week,
  LOB_Supported,
  Channel,
  Tactic,
  Message_Type,
  Agency
HAVING COUNT(*) > 1
ORDER BY row_count DESC;


-- ============================================================
-- VALIDATION 3:
-- INSPECT Q1-TO-Q2 QUARTER TRANSITION
-- ============================================================
SELECT
  Quarter,
  QGP_Week,
  LOB_Supported,
  Channel,
  Tactic,
  Message_Type,
  Agency,
  spend_actual,
  spend_for_wow,
  spend_actual_wow_pct,
  is_partial_week
FROM prdrzranalytics.lab42.sdi_vw_mfc_gold_spendGranular_weekly
WHERE QGP_Week BETWEEN DATE '2026-03-21' AND DATE '2026-04-18'
ORDER BY
  LOB_Supported,
  Channel,
  Tactic,
  Message_Type,
  Agency,
  QGP_Week;


-- ============================================================
-- VALIDATION 4:
-- CONFIRM MISSING GRANULAR COMBINATIONS REMAIN NULL
-- ============================================================
SELECT
  QGP_Week,
  LOB_Supported,
  Channel,
  Tactic,
  Message_Type,
  Agency,
  spend_actual,
  spend_for_wow,
  is_partial_week
FROM prdrzranalytics.lab42.sdi_vw_mfc_gold_spendGranular_weekly
WHERE spend_actual IS NULL
ORDER BY
  QGP_Week DESC,
  LOB_Supported,
  Channel,
  Tactic,
  Message_Type,
  Agency;


-- ============================================================
-- VALIDATION 5:
-- COMPARE NON-GRANULAR TOTALS AGAINST GRANULAR SUMS
--
-- These may differ if the current bronze snapshot-ranking rules
-- select different FileLoad_Date values at different grains.
-- This query identifies any such differences.
-- ============================================================
WITH granular AS (
  SELECT
    QGP_Week,
    LOB_Supported,
    ROUND(SUM(spend_actual), 2) AS granular_spend_actual
  FROM prdrzranalytics.lab42.sdi_vw_mfc_silver_spendGranular_weekly
  GROUP BY
    QGP_Week,
    LOB_Supported
),

non_granular AS (
  SELECT
    QGP_Week,
    LOB_Supported,
    spend_actual AS non_granular_spend_actual
  FROM prdrzranalytics.lab42.sdi_vw_mfc_silver_spend_weekly
)

SELECT
  COALESCE(n.QGP_Week, g.QGP_Week) AS QGP_Week,
  COALESCE(n.LOB_Supported, g.LOB_Supported) AS LOB_Supported,
  n.non_granular_spend_actual,
  g.granular_spend_actual,
  ROUND(
    COALESCE(g.granular_spend_actual, 0)
    - COALESCE(n.non_granular_spend_actual, 0),
    2
  ) AS granular_minus_non_granular

FROM non_granular n

FULL OUTER JOIN granular g
  ON  n.QGP_Week      = g.QGP_Week
  AND n.LOB_Supported = g.LOB_Supported

WHERE COALESCE(g.granular_spend_actual, 0)
      <> COALESCE(n.non_granular_spend_actual, 0)

ORDER BY
  QGP_Week DESC,
  LOB_Supported;


-- ============================================================
-- VALIDATION 6:
-- VERIFY COMPLETE-WEEK WOW INPUTS AROUND A QUARTER BOUNDARY
-- ============================================================
SELECT
  Quarter,
  QGP_Week,
  LOB_Supported,
  spend_actual,
  spend_actual_for_wow,
  spend_for_wow,
  spend_actual_wow_pct,
  is_partial_week

FROM prdrzranalytics.lab42.sdi_vw_mfc_gold_spend_weekly

WHERE QGP_Week BETWEEN DATE '2026-03-28' AND DATE '2026-04-18'

ORDER BY
  LOB_Supported,
  QGP_Week;
