
-- =============================================
-- BRONZE: Spend Detail Weekly
-- Pulls from gold granular, adds:
--   - Channel_Group mapping
--   - Actual_Quarter (real quarter for display)
--   - Quarter (adjusted for WoW LOOKUP)
--   - spend_wow_ref (spend_actual based, full week
--     for boundary Saturday row, NULL for
--     quarter-end boundary row)
--   - exclude_wow_helper_from_display flag
--
-- Three types of rows:
-- 1. Original rows (displayed, FALSE)
-- 2. Combined boundary helper rows (hidden, TRUE)
--    Only when both partials exist (partial_count=2)
-- 3. Prior quarter last normal week (hidden, TRUE)
--    Bumped into next quarter for LOOKUP(-1)
--
-- Filters explicitly applied in base CTE so
-- pulseMFC pipeline stays correctly scoped
-- when underlying filters are removed later.
-- =============================================
CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_pulseMFC_bronze_spendDetail_weekly AS

WITH base AS (
  SELECT
    Quarter                                                       AS Actual_Quarter,
    Quarter                                                       AS Quarter,
    Period_Start,
    Period_End,
    QGP_Week,
    Quarter_End_Date,
    FileLoad_Date,
    UPPER(TRIM(LOB_Supported))                                    AS LOB_Supported,
    UPPER(TRIM(Channel))                                          AS Channel,
    UPPER(TRIM(Tactic))                                           AS Tactic,
    UPPER(TRIM(Message_Type))                                     AS Message_Type,
    Agency,
    CASE
      WHEN UPPER(TRIM(Channel)) = 'PAID SEARCH'
        THEN 'Paid Search'
      WHEN UPPER(TRIM(Channel)) = 'PAID SOCIAL'
        THEN 'Paid Social'
      WHEN UPPER(TRIM(Channel)) IN ('DISPLAY', 'OLV', 'AUDIO')
        THEN 'Programmatic'
      WHEN UPPER(TRIM(Channel)) = 'OTT'
        AND UPPER(TRIM(Tactic)) LIKE '%PROGRAMMATIC%'
        THEN 'Programmatic'
      WHEN UPPER(TRIM(Channel)) = 'OOH'
        AND UPPER(TRIM(Tactic)) LIKE '%PROGRAMMATIC%'
        THEN 'Programmatic'
      ELSE 'Other'
    END                                                           AS Channel_Group,
    spend_actual,
    spend_forecast,
    spend_display,
    UPPER(TRIM(week_type))                                        AS week_type,
    DATEDIFF(Period_End, Period_Start) + 1                        AS period_days
  FROM prdrzranalytics.lab42.sdi_vw_mfc_gold_spendGranular_weekly
  -- Explicit filters: kept here so pulseMFC pipeline stays
  -- correctly scoped when underlying views are broadened later
  WHERE UPPER(TRIM(LOB_Supported)) IN ('CONSUMER POSTPAID', 'BROADBAND')
    AND Channel IS NOT NULL
    AND Channel NOT IN ('OTHER (do not use)', 'Non-Working', 'Unallocated', 'Budget Held')
    AND UPPER(TRIM(Message_Type)) NOT IN ('MICRO')
    AND spend_display IS NOT NULL
    AND spend_display != 0
),

-- -----------------------------------------------
-- Combined boundary week rows
-- Both partials (quarter-end + Saturday) summed
-- per calendar week + full grain.
-- Only meaningful when both partials exist
-- (partial_count = 2).
-- Used to:
--   1. Populate spend_wow_ref on Saturday
--      displayed row (combined_actual)
--   2. Create hidden helper row for LOOKUP(-1)
-- -----------------------------------------------
boundary_combined AS (
  SELECT
    DATE_TRUNC('week', Period_Start)          AS week_monday,
    MAX(QGP_Week)                             AS saturday_qgp_week,
    LOB_Supported,
    Channel,
    Channel_Group,
    Tactic,
    Message_Type,
    Agency,
    SUM(spend_actual)                         AS combined_actual,
    SUM(spend_forecast)                       AS combined_forecast,
    SUM(spend_display)                        AS combined_display,
    MAX(FileLoad_Date)                        AS FileLoad_Date,
    COUNT(DISTINCT QGP_Week)                  AS partial_count
  FROM base
  WHERE week_type = 'BOUNDARY_WEEK'
  GROUP BY
    DATE_TRUNC('week', Period_Start),
    LOB_Supported,
    Channel,
    Channel_Group,
    Tactic,
    Message_Type,
    Agency
),

-- -----------------------------------------------
-- Prior quarter last normal week reference
-- Finds the single last QGP_Week per quarter
-- so only ONE week's rows get bumped into the
-- next quarter — not every combination's last
-- appearance independently.
-- -----------------------------------------------
last_qgp_week_per_quarter AS (
  SELECT
    Actual_Quarter,
    MAX(QGP_Week)                             AS last_qgp_week
  FROM base
  WHERE week_type = 'NORMAL'
  GROUP BY Actual_Quarter
),

prior_quarter_reference AS (
  SELECT
    b.Actual_Quarter,
    -- Bump Quarter to next quarter for LOOKUP(-1)
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
    b.spend_actual,
    b.spend_forecast,
    b.spend_display,
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
-- Part 1: All original rows — displayed in Tableau
-- spend_wow_ref:
--   Normal weeks           → spend_actual
--   Saturday boundary row  → combined_actual
--                            (full week for WoW)
--   Quarter-end boundary   → NULL (WoW is null)
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
  FALSE                                                           AS exclude_wow_helper_from_display
FROM base b
LEFT JOIN boundary_combined bc
  ON DATE_TRUNC('week', b.Period_Start)       = bc.week_monday
 AND b.LOB_Supported                          = bc.LOB_Supported
 AND b.Channel                                = bc.Channel
 AND b.Tactic                                 = bc.Tactic
 AND b.Message_Type                           = bc.Message_Type
 AND b.Agency                                 = bc.Agency
 AND b.week_type                              = 'BOUNDARY_WEEK'
 AND b.QGP_Week                              != b.Quarter_End_Date
 AND bc.partial_count                         = 2

UNION ALL

-- -----------------------------------------------
-- Part 2: Combined boundary week helper rows
-- Hidden from display (exclude = TRUE)
-- Only created when both partials exist
-- spend_wow_ref = combined_actual
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
  DATE_ADD(bc.week_monday, 6)                                     AS Period_End,
  bc.saturday_qgp_week                                            AS QGP_Week,
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
  )                                                               AS Quarter_End_Date,
  bc.FileLoad_Date,
  bc.LOB_Supported,
  bc.Channel,
  bc.Channel_Group,
  bc.Tactic,
  bc.Message_Type,
  bc.Agency,
  bc.combined_actual                                              AS spend_actual,
  bc.combined_forecast                                            AS spend_forecast,
  bc.combined_display                                             AS spend_display,
  bc.combined_actual                                              AS spend_wow_ref,
  'BOUNDARY_WEEK'                                                 AS week_type,
  7                                                               AS period_days,
  TRUE                                                            AS exclude_wow_helper_from_display
FROM boundary_combined bc
WHERE bc.partial_count = 2

UNION ALL

-- -----------------------------------------------
-- Part 3: Prior quarter last normal week reference
-- Hidden from display (exclude = TRUE)
-- Bumped into next quarter so LOOKUP(-1) works
-- for the first week of each quarter
-- spend_wow_ref = spend_actual
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
  spend_actual,
  spend_forecast,
  spend_display,
  spend_wow_ref,
  week_type,
  period_days,
  TRUE                                                            AS exclude_wow_helper_from_display
FROM prior_quarter_reference;

