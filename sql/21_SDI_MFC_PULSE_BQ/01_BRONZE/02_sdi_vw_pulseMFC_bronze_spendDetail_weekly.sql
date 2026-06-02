-- =============================================
-- BRONZE: Spend Detail Weekly
-- Pulls from gold granular, adds Channel Group,
-- Actual_Quarter, adjusted Quarter, and
-- WoW helper rows for Tableau LOOKUP
-- =============================================
CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_pulseMFC_bronze_spendDetail_weekly` AS

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
    DATE_DIFF(Period_End, Period_Start, DAY) + 1                  AS period_days
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_gold_spendGranular_weekly`
),

-- -----------------------------------------------
-- WoW Helper 1: Combined boundary week rows
-- Both partials (Q-end + Saturday) summed into
-- one row keyed on Saturday QGP_Week.
-- Only created when BOTH partials exist
-- (partial_count = 2) — ensures future boundary
-- weeks with only one partial don't get a helper.
-- exclude_wow_helper_from_display = TRUE
-- -----------------------------------------------
boundary_combined AS (
  SELECT
    DATE_TRUNC(Period_Start, WEEK(MONDAY))    AS week_monday,
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
    DATE_TRUNC(Period_Start, WEEK(MONDAY)),
    LOB_Supported,
    Channel,
    Channel_Group,
    Tactic,
    Message_Type,
    Agency
),

-- -----------------------------------------------
-- WoW Helper 2: Prior quarter last normal week
-- The last full normal week of each quarter
-- is duplicated and bumped into the next quarter
-- so LOOKUP(-1) can reach it for the first week
-- of the next quarter.
-- exclude_wow_helper_from_display = TRUE
-- -----------------------------------------------
last_normal_week AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        Actual_Quarter,
        LOB_Supported,
        Channel,
        Tactic,
        Message_Type,
        Agency
      ORDER BY QGP_Week DESC
    ) AS rn
  FROM base
  WHERE week_type = 'NORMAL'
),

prior_quarter_reference AS (
  SELECT
    Actual_Quarter,
    -- Bump Quarter to next quarter
    CASE
      WHEN CAST(SUBSTR(Actual_Quarter, 7, 1) AS INT64) = 4
        THEN CONCAT(
          CAST(CAST(SUBSTR(Actual_Quarter, 1, 4) AS INT64) + 1 AS STRING),
          ' Q1'
        )
      ELSE CONCAT(
        SUBSTR(Actual_Quarter, 1, 4),
        ' Q',
        CAST(CAST(SUBSTR(Actual_Quarter, 7, 1) AS INT64) + 1 AS STRING)
      )
    END                                                           AS Quarter,
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
    period_days
  FROM last_normal_week
  WHERE rn = 1
)

-- -----------------------------------------------
-- Part 1: All original rows — displayed in Tableau
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
  week_type,
  period_days,
  FALSE                                                           AS exclude_wow_helper_from_display
FROM base

UNION ALL

-- -----------------------------------------------
-- Part 2: Combined boundary week rows
-- Hidden from display, used for LOOKUP(-1)
-- Only created when both partials exist
-- -----------------------------------------------
SELECT
  CONCAT(
    CAST(EXTRACT(YEAR FROM bc.saturday_qgp_week) AS STRING),
    ' Q',
    CAST(
      CASE
        WHEN EXTRACT(MONTH FROM bc.saturday_qgp_week) <= 3 THEN 1
        WHEN EXTRACT(MONTH FROM bc.saturday_qgp_week) <= 6 THEN 2
        WHEN EXTRACT(MONTH FROM bc.saturday_qgp_week) <= 9 THEN 3
        ELSE 4
      END AS STRING
    )
  )                                                               AS Actual_Quarter,
  CONCAT(
    CAST(EXTRACT(YEAR FROM bc.saturday_qgp_week) AS STRING),
    ' Q',
    CAST(
      CASE
        WHEN EXTRACT(MONTH FROM bc.saturday_qgp_week) <= 3 THEN 1
        WHEN EXTRACT(MONTH FROM bc.saturday_qgp_week) <= 6 THEN 2
        WHEN EXTRACT(MONTH FROM bc.saturday_qgp_week) <= 9 THEN 3
        ELSE 4
      END AS STRING
    )
  )                                                               AS Quarter,
  bc.week_monday                                                  AS Period_Start,
  DATE_ADD(bc.week_monday, INTERVAL 6 DAY)                       AS Period_End,
  bc.saturday_qgp_week                                            AS QGP_Week,
  LAST_DAY(
    DATE(
      EXTRACT(YEAR FROM bc.saturday_qgp_week),
      CASE
        WHEN EXTRACT(MONTH FROM bc.saturday_qgp_week) <= 3 THEN 3
        WHEN EXTRACT(MONTH FROM bc.saturday_qgp_week) <= 6 THEN 6
        WHEN EXTRACT(MONTH FROM bc.saturday_qgp_week) <= 9 THEN 9
        ELSE 12
      END,
      1
    ), MONTH
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
  'BOUNDARY_WEEK'                                                 AS week_type,
  7                                                               AS period_days,
  TRUE                                                            AS exclude_wow_helper_from_display

FROM boundary_combined bc
WHERE bc.partial_count = 2  -- Only when both boundary partials exist

UNION ALL

-- -----------------------------------------------
-- Part 3: Prior quarter last normal week reference
-- Hidden from display, bumped into next quarter
-- so LOOKUP(-1) works for first week of quarter
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
  week_type,
  period_days,
  TRUE                                                            AS exclude_wow_helper_from_display

FROM prior_quarter_reference;


