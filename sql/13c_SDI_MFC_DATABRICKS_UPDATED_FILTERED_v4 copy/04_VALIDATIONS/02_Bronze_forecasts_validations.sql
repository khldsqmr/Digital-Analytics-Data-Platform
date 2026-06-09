
-- ============================================================
-- VALIDATION 1:
-- ACTUAL GRANULAR TOTALS SHOULD MATCH NON-GRANULAR TOTALS
-- ============================================================
WITH non_granular AS (
  SELECT
    Quarter,
    QGP_Week,
    LOB_Supported,
    weekly_actual AS non_granular_weekly_actual
  FROM prdrzranalytics.lab42.sdi_vw_mfc_bronze_spendActuals_weekly
),

granular AS (
  SELECT
    Quarter,
    QGP_Week,
    LOB_Supported,
    SUM(weekly_actual) AS granular_weekly_actual
  FROM prdrzranalytics.lab42.sdi_vw_mfc_bronze_spendActualsGranular_weekly
  GROUP BY
    Quarter,
    QGP_Week,
    LOB_Supported
)

SELECT
  COALESCE(n.Quarter, g.Quarter) AS Quarter,
  COALESCE(n.QGP_Week, g.QGP_Week) AS QGP_Week,
  COALESCE(n.LOB_Supported, g.LOB_Supported) AS LOB_Supported,
  n.non_granular_weekly_actual,
  g.granular_weekly_actual,
  ROUND(
    COALESCE(g.granular_weekly_actual, 0)
    - COALESCE(n.non_granular_weekly_actual, 0),
    2
  ) AS granular_minus_non_granular

FROM non_granular n

FULL OUTER JOIN granular g
  ON  n.Quarter       = g.Quarter
  AND n.QGP_Week      = g.QGP_Week
  AND n.LOB_Supported = g.LOB_Supported

WHERE ABS(
  COALESCE(g.granular_weekly_actual, 0)
  - COALESCE(n.non_granular_weekly_actual, 0)
) > 0.05

ORDER BY
  QGP_Week DESC,
  Quarter DESC,
  LOB_Supported;


-- ============================================================
-- VALIDATION 2:
-- FORECAST GRANULAR TOTALS SHOULD MATCH NON-GRANULAR TOTALS
-- ============================================================
WITH non_granular AS (
  SELECT
    Quarter,
    QGP_Week,
    LOB_Supported,
    weekly_forecast AS non_granular_weekly_forecast
  FROM prdrzranalytics.lab42.sdi_vw_mfc_bronze_spendForecasts_weekly
),

granular AS (
  SELECT
    Quarter,
    QGP_Week,
    LOB_Supported,
    SUM(weekly_forecast) AS granular_weekly_forecast
  FROM prdrzranalytics.lab42.sdi_vw_mfc_bronze_spendForecastsGranular_weekly
  GROUP BY
    Quarter,
    QGP_Week,
    LOB_Supported
)

SELECT
  COALESCE(n.Quarter, g.Quarter) AS Quarter,
  COALESCE(n.QGP_Week, g.QGP_Week) AS QGP_Week,
  COALESCE(n.LOB_Supported, g.LOB_Supported) AS LOB_Supported,
  n.non_granular_weekly_forecast,
  g.granular_weekly_forecast,
  ROUND(
    COALESCE(g.granular_weekly_forecast, 0)
    - COALESCE(n.non_granular_weekly_forecast, 0),
    2
  ) AS granular_minus_non_granular

FROM non_granular n

FULL OUTER JOIN granular g
  ON  n.Quarter       = g.Quarter
  AND n.QGP_Week      = g.QGP_Week
  AND n.LOB_Supported = g.LOB_Supported

WHERE ABS(
  COALESCE(g.granular_weekly_forecast, 0)
  - COALESCE(n.non_granular_weekly_forecast, 0)
) > 0.10

ORDER BY
  QGP_Week DESC,
  Quarter DESC,
  LOB_Supported;


-- ============================================================
-- VALIDATION 3:
-- EACH GRANULAR ACTUAL LOB-WEEK MUST USE ONE FILE LOAD DATE
-- ============================================================
SELECT
  Quarter,
  QGP_Week,
  LOB_Supported,
  COUNT(DISTINCT FileLoad_Date) AS file_load_date_count

FROM prdrzranalytics.lab42.sdi_vw_mfc_bronze_spendActualsGranular_weekly

GROUP BY
  Quarter,
  QGP_Week,
  LOB_Supported

HAVING COUNT(DISTINCT FileLoad_Date) > 1

ORDER BY
  QGP_Week DESC,
  Quarter DESC,
  LOB_Supported;


-- ============================================================
-- VALIDATION 4:
-- EACH GRANULAR FORECAST LOB-WEEK MUST USE ONE FILE LOAD DATE
-- ============================================================
SELECT
  Quarter,
  QGP_Week,
  LOB_Supported,
  COUNT(DISTINCT FileLoad_Date) AS file_load_date_count

FROM prdrzranalytics.lab42.sdi_vw_mfc_bronze_spendForecastsGranular_weekly

GROUP BY
  Quarter,
  QGP_Week,
  LOB_Supported

HAVING COUNT(DISTINCT FileLoad_Date) > 1

ORDER BY
  QGP_Week DESC,
  Quarter DESC,
  LOB_Supported;
