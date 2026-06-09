-- ============================================================
-- MFC QGP CALENDAR DIMENSION
-- Databricks / Spark SQL
-- Converted from PulseTMS BigQuery calendar logic
--
-- Grain: one row per QGP date
-- Week types:
--   NORMAL         → every Saturday
--   BOUNDARY_STUB  → quarter-end non-Saturday
--   BOUNDARY_FIRST → first Saturday after a BOUNDARY_STUB
-- ============================================================

CREATE OR REPLACE VIEW
  prdrzranalytics.lab42.sdi_vw_mfc_dim_qgp_calendar
AS

WITH

-- Daily date spine: 2020-01-01 through end of next calendar year
date_spine AS (
  SELECT EXPLODE(SEQUENCE(
    DATE'2020-01-01',
    DATE_ADD(DATE_TRUNC('year', ADD_MONTHS(CURRENT_DATE(), 12)), -1)
  )) AS day
),

-- All Gregorian quarter-end dates within the spine
quarter_ends AS (
  SELECT DISTINCT
    LAST_DAY(ADD_MONTHS(TO_DATE(DATE_TRUNC('quarter', day)), 2)) AS quarter_end_date
  FROM date_spine
),

-- NORMAL: every Saturday
saturdays AS (
  SELECT
    day              AS qgp_date,
    'NORMAL'         AS week_type,
    CAST(NULL AS DATE) AS boundary_stub_date
  FROM date_spine
  WHERE DAYOFWEEK(day) = 7
),

-- BOUNDARY_STUB: quarter-end dates that fall on a non-Saturday
boundary_stubs AS (
  SELECT
    quarter_end_date AS qgp_date,
    'BOUNDARY_STUB'  AS week_type,
    CAST(NULL AS DATE) AS boundary_stub_date
  FROM quarter_ends
  WHERE DAYOFWEEK(quarter_end_date) != 7
),

-- BOUNDARY_FIRST: first Saturday after each BOUNDARY_STUB
boundary_firsts AS (
  SELECT
    s.day              AS qgp_date,
    'BOUNDARY_FIRST'   AS week_type,
    bs.qgp_date        AS boundary_stub_date
  FROM boundary_stubs bs
  JOIN date_spine s
    ON  s.day > bs.qgp_date
    AND DAYOFWEEK(s.day) = 7
  QUALIFY ROW_NUMBER() OVER (PARTITION BY bs.qgp_date ORDER BY s.day ASC) = 1
),

-- Combine all QGP dates — BOUNDARY_FIRST overrides NORMAL for same Saturday
all_qgp_dates AS (
  SELECT qgp_date, week_type, boundary_stub_date FROM boundary_stubs
  UNION ALL
  SELECT qgp_date, week_type, boundary_stub_date FROM boundary_firsts
  UNION ALL
  SELECT s.qgp_date, s.week_type, s.boundary_stub_date
  FROM saturdays s
  WHERE s.qgp_date NOT IN (SELECT qgp_date FROM boundary_firsts)
),

-- Enrich with calendar attributes
enriched AS (
  SELECT
    aq.qgp_date,
    aq.week_type,
    aq.boundary_stub_date,

    YEAR(aq.qgp_date)                                                    AS qgp_year,
    QUARTER(aq.qgp_date)                                                 AS qgp_quarter_num,
    CONCAT(CAST(YEAR(aq.qgp_date) AS STRING), ' Q',
           CAST(QUARTER(aq.qgp_date) AS STRING))                         AS quarter,

    LAST_DAY(ADD_MONTHS(TO_DATE(DATE_TRUNC('quarter', aq.qgp_date)), 2)) AS quarter_end_date,

    WEEKOFYEAR(aq.qgp_date)                                              AS iso_week_number,
    YEAR(aq.qgp_date)                                                    AS iso_year,

    -- Days in period
    CASE aq.week_type
      WHEN 'NORMAL' THEN 7
      WHEN 'BOUNDARY_STUB' THEN
        DATEDIFF(aq.qgp_date,
          DATE_SUB(aq.qgp_date, DAYOFWEEK(aq.qgp_date) - 1)
        ) + 1
      WHEN 'BOUNDARY_FIRST' THEN
        7 - (DATEDIFF(aq.boundary_stub_date,
               DATE_SUB(aq.boundary_stub_date, DAYOFWEEK(aq.boundary_stub_date) - 1)
             ) + 1)
    END                                                                  AS days_in_period,

    aq.qgp_date <= CURRENT_DATE()                                        AS is_complete_period,
    TO_DATE(DATE_TRUNC('quarter', aq.qgp_date))
      = TO_DATE(DATE_TRUNC('quarter', CURRENT_DATE()))                   AS is_current_quarter

  FROM all_qgp_dates aq
),

-- WoW prior date via LAG
with_wow AS (
  SELECT
    e.*,
    CASE
      WHEN e.week_type = 'BOUNDARY_STUB'  THEN NULL
      WHEN e.week_type = 'BOUNDARY_FIRST' THEN LAG(e.qgp_date, 2) OVER (ORDER BY e.qgp_date)
      ELSE                                     LAG(e.qgp_date, 1) OVER (ORDER BY e.qgp_date)
    END AS wow_prior_qgp_date
  FROM enriched e
),

-- Prior year lookup for YoY
prior_year_lookup AS (
  SELECT qgp_date, iso_week_number, iso_year, week_type
  FROM enriched
  WHERE week_type IN ('NORMAL', 'BOUNDARY_FIRST')
)

-- Non-stub rows with YoY lookup
SELECT
  w.qgp_date, w.week_type, w.boundary_stub_date,
  w.qgp_year, w.qgp_quarter_num, w.quarter, w.quarter_end_date,
  w.iso_week_number, w.iso_year, w.days_in_period,
  w.is_complete_period, w.is_current_quarter,
  w.wow_prior_qgp_date,
  ly.qgp_date AS prior_year_qgp_date
FROM with_wow w
LEFT JOIN prior_year_lookup ly
  ON  ly.iso_week_number = w.iso_week_number
  AND ly.iso_year        = w.iso_year - 1
  AND ly.week_type       = w.week_type
WHERE w.week_type != 'BOUNDARY_STUB'

UNION ALL

-- Stub rows — all period lookups NULL
SELECT
  w.qgp_date, w.week_type, w.boundary_stub_date,
  w.qgp_year, w.qgp_quarter_num, w.quarter, w.quarter_end_date,
  w.iso_week_number, w.iso_year, w.days_in_period,
  w.is_complete_period, w.is_current_quarter,
  NULL AS wow_prior_qgp_date,
  NULL AS prior_year_qgp_date
FROM with_wow w
WHERE w.week_type = 'BOUNDARY_STUB'
;