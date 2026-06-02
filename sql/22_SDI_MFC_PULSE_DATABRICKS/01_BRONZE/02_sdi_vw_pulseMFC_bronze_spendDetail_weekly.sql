
-- =============================================
-- BRONZE: Spend Detail Weekly
--
-- Three row types:
-- 1. Original rows — is_wow_helper = FALSE
--    spend_actual, spend_forecast, spend_display populated
--    spend_wow_ref:
--      normal weeks = spend_actual
--      saturday boundary = combined_actual (full week)
--      quarter-end boundary = NULL
--
-- 2. Combined boundary helper — is_wow_helper = TRUE
--    spend_actual = NULL (no inflation in Tableau SUM)
--    spend_forecast = NULL
--    spend_display = NULL
--    spend_wow_ref = combined_actual (full week for LOOKUP)
--    Created for ALL boundary weeks (partial_count 1 or 2)
--    If only one partial exists, combined = that partial
--
-- 3. Prior quarter last normal week — is_wow_helper = TRUE
--    spend_actual = NULL
--    spend_forecast = NULL
--    spend_display = NULL
--    spend_wow_ref = spend_actual (for LOOKUP prior week)
--    Bumped into next quarter for LOOKUP(-1)
--
-- All rows: exclude_wow_helper_from_display = FALSE
-- This keeps ALL rows in same Tableau partition
-- so LOOKUP(-1) traverses helper rows freely
-- =============================================
CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_pulseMFC_bronze_spendDetail_weekly AS

WITH base AS (
  SELECT
    Quarter                                                       AS Actual_Quarter,
    Quarter                                                       AS Quarter,
    CAST(Period_Start AS DATE)                                    AS Period_Start,
    CAST(Period_End AS DATE)                                      AS Period_End,
    CAST(QGP_Week AS DATE)                                        AS QGP_Week,
    CAST(Quarter_End_Date AS DATE)                                AS Quarter_End_Date,
    CAST(FileLoad_Date AS DATE)                                   AS FileLoad_Date,
    UPPER(TRIM(LOB_Supported))                                    AS LOB_Supported,
    UPPER(TRIM(Channel))                                          AS Channel,
    UPPER(TRIM(Tactic))                                           AS Tactic,
    UPPER(TRIM(Message_Type))                                     AS Message_Type,
    Agency,
    CASE
      WHEN UPPER(TRIM(Channel)) = 'PAID SEARCH'  THEN 'Paid Search'
      WHEN UPPER(TRIM(Channel)) = 'PAID SOCIAL'  THEN 'Paid Social'
      WHEN UPPER(TRIM(Channel)) IN ('DISPLAY', 'OLV', 'AUDIO') THEN 'Programmatic'
      WHEN UPPER(TRIM(Channel)) = 'OTT'
        AND UPPER(TRIM(Tactic)) LIKE '%PROGRAMMATIC%' THEN 'Programmatic'
      WHEN UPPER(TRIM(Channel)) = 'OOH'
        AND UPPER(TRIM(Tactic)) LIKE '%PROGRAMMATIC%' THEN 'Programmatic'
      ELSE 'Other'
    END                                                           AS Channel_Group,
    spend_actual,
    spend_forecast,
    spend_display,
    UPPER(TRIM(week_type))                                        AS week_type,
    DATEDIFF(
      CAST(Period_End AS DATE),
      CAST(Period_Start AS DATE)
    ) + 1                                                         AS period_days
  FROM prdrzranalytics.lab42.sdi_vw_mfc_gold_spendGranular_weekly
  WHERE UPPER(TRIM(LOB_Supported)) IN ('CONSUMER POSTPAID', 'BROADBAND')
    AND Channel IS NOT NULL
    AND Channel NOT IN ('OTHER (do not use)', 'Non-Working', 'Unallocated', 'Budget Held')
    AND UPPER(TRIM(Message_Type)) NOT IN ('MICRO')
    AND spend_display IS NOT NULL
    AND spend_display != 0
),

-- -----------------------------------------------
-- Combine boundary week partials per calendar week
-- Groups by Monday to pair different QGP_Weeks
-- No partial_count restriction — creates helper
-- for ALL boundary weeks regardless of how many
-- partials exist. If only one partial exists,
-- combined = that partial's spend_actual
-- -----------------------------------------------
boundary_combined AS (
  SELECT
    CAST(DATE_TRUNC('week', Period_Start) AS DATE) AS week_monday,
    CAST(MAX(QGP_Week) AS DATE)                    AS saturday_qgp_week,
    LOB_Supported,
    Channel,
    Channel_Group,
    Tactic,
    Message_Type,
    Agency,
    SUM(spend_actual)                              AS combined_actual,
    SUM(spend_forecast)                            AS combined_forecast,
    SUM(spend_display)                             AS combined_display,
    MAX(FileLoad_Date)                             AS FileLoad_Date,
    COUNT(DISTINCT QGP_Week)                       AS partial_count
  FROM base
  WHERE week_type = 'BOUNDARY_WEEK'
  GROUP BY
    CAST(DATE_TRUNC('week', Period_Start) AS DATE),
    LOB_Supported,
    Channel,
    Channel_Group,
    Tactic,
    Message_Type,
    Agency
),

-- -----------------------------------------------
-- Last normal week per quarter
-- Used to create prior quarter reference rows
-- -----------------------------------------------
last_qgp_week_per_quarter AS (
  SELECT
    Actual_Quarter,
    CAST(MAX(QGP_Week) AS DATE)                    AS last_qgp_week
  FROM base
  WHERE week_type = 'NORMAL'
  GROUP BY Actual_Quarter
),

prior_quarter_reference AS (
  SELECT
    b.Actual_Quarter,
    CASE
      WHEN CAST(SUBSTR(b.Actual_Quarter, 7, 1) AS INT) = 4
        THEN CONCAT(
          CAST(CAST(SUBSTR(b.Actual_Quarter, 1, 4) AS INT) + 1 AS STRING),
          ' Q1'
        )
      ELSE CONCAT(
        SUBSTR(b.Actual_Quarter, 1, 4),
        ' Q',
        CAST(CAST(SUBSTR(b.Actual_Quarter, 7, 1) AS INT) + 1 AS STRING)
      )
    END                                                           AS Quarter,
    b.Period_Start,
    b.Period_End,
    b.QGP_Week,
    b.Quarter_End_Date,
    b.FileLoad_Date,
    b.LOB_Supported,
    b.Channel,
    b.Channel_Group,
    b.Tactic,
    b.Message_Type,
    b.Agency,
    b.spend_actual                                                AS spend_wow_ref,
    b.week_type,
    b.period_days
  FROM base b
  JOIN last_qgp_week_per_quarter l
    ON b.Actual_Quarter = l.Actual_Quarter
   AND b.QGP_Week       = l.last_qgp_week
  WHERE b.week_type = 'NORMAL'
)

-- -----------------------------------------------
-- Part 1: Original rows — displayed in Tableau
-- spend_actual, spend_forecast, spend_display
-- are populated for correct display values
-- spend_wow_ref used for WoW LOOKUP
-- -----------------------------------------------
SELECT
  b.Actual_Quarter,
  b.Quarter,
  b.Period_Start,
  b.Period_End,
  b.QGP_Week,
  b.Quarter_End_Date,
  b.FileLoad_Date,
  b.LOB_Supported,
  b.Channel,
  b.Channel_Group,
  b.Tactic,
  b.Message_Type,
  b.Agency,
  b.spend_actual,
  b.spend_forecast,
  b.spend_display,
  CASE
    WHEN b.week_type = 'BOUNDARY_WEEK'
      AND b.QGP_Week != b.Quarter_End_Date
      AND bc.combined_actual IS NOT NULL
      THEN bc.combined_actual
    WHEN b.week_type = 'BOUNDARY_WEEK'
      AND b.QGP_Week = b.Quarter_End_Date
      THEN NULL
    ELSE b.spend_actual
  END                                                             AS spend_wow_ref,
  b.week_type,
  b.period_days,
  FALSE                                                           AS is_wow_helper,
  FALSE                                                           AS exclude_wow_helper_from_display
FROM base b
LEFT JOIN boundary_combined bc
  ON CAST(DATE_TRUNC('week', b.Period_Start) AS DATE) = bc.week_monday
 AND b.LOB_Supported                                  = bc.LOB_Supported
 AND b.Channel                                        = bc.Channel
 AND b.Tactic                                         = bc.Tactic
 AND b.Message_Type                                   = bc.Message_Type
 AND b.Agency                                         = bc.Agency
 AND b.week_type                                      = 'BOUNDARY_WEEK'
 AND b.QGP_Week                                      != b.Quarter_End_Date

UNION ALL

-- -----------------------------------------------
-- Part 2: Combined boundary helper rows
-- is_wow_helper = TRUE
-- spend_actual/forecast/display = NULL
--   → NULLs don't inflate Tableau SUM
-- spend_wow_ref = combined_actual
--   → full week value for LOOKUP(-1)
-- Created for ALL boundary weeks
-- exclude_wow_helper_from_display = FALSE
--   → same Tableau partition, LOOKUP can traverse
-- -----------------------------------------------
SELECT
  CONCAT(
    CAST(YEAR(bc.saturday_qgp_week) AS STRING),
    ' Q',
    CAST(
      CASE
        WHEN MONTH(bc.saturday_qgp_week) <= 3 THEN 1
        WHEN MONTH(bc.saturday_qgp_week) <= 6 THEN 2
        WHEN MONTH(bc.saturday_qgp_week) <= 9 THEN 3
        ELSE 4
      END AS STRING
    )
  )                                                               AS Actual_Quarter,
  CONCAT(
    CAST(YEAR(bc.saturday_qgp_week) AS STRING),
    ' Q',
    CAST(
      CASE
        WHEN MONTH(bc.saturday_qgp_week) <= 3 THEN 1
        WHEN MONTH(bc.saturday_qgp_week) <= 6 THEN 2
        WHEN MONTH(bc.saturday_qgp_week) <= 9 THEN 3
        ELSE 4
      END AS STRING
    )
  )                                                               AS Quarter,
  bc.week_monday                                                  AS Period_Start,
  CAST(DATE_ADD(bc.week_monday, 6) AS DATE)                      AS Period_End,
  bc.saturday_qgp_week                                            AS QGP_Week,
  CAST(
    LAST_DAY(
      TO_DATE(
        CONCAT(
          CAST(YEAR(bc.saturday_qgp_week) AS STRING), '-',
          LPAD(CAST(
            CASE
              WHEN MONTH(bc.saturday_qgp_week) <= 3 THEN 3
              WHEN MONTH(bc.saturday_qgp_week) <= 6 THEN 6
              WHEN MONTH(bc.saturday_qgp_week) <= 9 THEN 9
              ELSE 12
            END AS STRING), 2, '0'), '-01'
        ), 'yyyy-MM-dd'
      )
    ) AS DATE
  )                                                               AS Quarter_End_Date,
  CAST(bc.FileLoad_Date AS DATE)                                  AS FileLoad_Date,
  bc.LOB_Supported,
  bc.Channel,
  bc.Channel_Group,
  bc.Tactic,
  bc.Message_Type,
  bc.Agency,
  NULL                                                            AS spend_actual,
  NULL                                                            AS spend_forecast,
  NULL                                                            AS spend_display,
  bc.combined_actual                                              AS spend_wow_ref,
  'BOUNDARY_WEEK'                                                 AS week_type,
  7                                                               AS period_days,
  TRUE                                                            AS is_wow_helper,
  FALSE                                                           AS exclude_wow_helper_from_display
FROM boundary_combined bc

UNION ALL

-- -----------------------------------------------
-- Part 3: Prior quarter last normal week reference
-- is_wow_helper = TRUE
-- spend_actual/forecast/display = NULL
--   → NULLs don't inflate Tableau SUM
-- spend_wow_ref = spend_actual of last normal week
--   → prior week reference for LOOKUP(-1)
-- Bumped into next quarter
-- exclude_wow_helper_from_display = FALSE
--   → same Tableau partition, LOOKUP can traverse
-- -----------------------------------------------
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
  NULL                                                            AS spend_actual,
  NULL                                                            AS spend_forecast,
  NULL                                                            AS spend_display,
  spend_wow_ref,
  week_type,
  period_days,
  TRUE                                                            AS is_wow_helper,
  FALSE                                                           AS exclude_wow_helper_from_display
FROM prior_quarter_reference;

