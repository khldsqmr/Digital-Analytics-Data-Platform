-- ============================================================
-- MFC QGP CALENDAR DIMENSION — BigQuery
-- Grain: one row per QGP date
-- Week types:
--   NORMAL         → every Saturday
--   BOUNDARY_STUB  → quarter-end non-Saturday
--   BOUNDARY_FIRST → first Saturday after a BOUNDARY_STUB
-- ============================================================
CREATE OR REPLACE VIEW
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_dim_qgp_calendar`
AS

WITH

-- Daily date spine: 2020-01-01 through end of next calendar year
DateSpine AS (
  SELECT day
  FROM UNNEST(GENERATE_DATE_ARRAY(
    DATE '2020-01-01',
    DATE_ADD(DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 YEAR), YEAR), INTERVAL -1 DAY)
  )) AS day
),

-- All Gregorian quarter-end dates within the spine
QuarterEnds AS (
  SELECT DISTINCT
    DATE_SUB(
      DATE_ADD(DATE_TRUNC(day, QUARTER), INTERVAL 3 MONTH),
      INTERVAL 1 DAY
    ) AS quarter_end_date
  FROM DateSpine
),

-- NORMAL: every Saturday
Saturdays AS (
  SELECT
    day                AS qgp_date,
    'NORMAL'           AS week_type,
    CAST(NULL AS DATE) AS boundary_stub_date
  FROM DateSpine
  WHERE EXTRACT(DAYOFWEEK FROM day) = 7
),

-- BOUNDARY_STUB: quarter-end dates that fall on a non-Saturday
BoundaryStubs AS (
  SELECT
    quarter_end_date   AS qgp_date,
    'BOUNDARY_STUB'    AS week_type,
    CAST(NULL AS DATE) AS boundary_stub_date
  FROM QuarterEnds
  WHERE EXTRACT(DAYOFWEEK FROM quarter_end_date) != 7
),

-- BOUNDARY_FIRST: first Saturday after each BOUNDARY_STUB
BoundaryFirsts AS (
  SELECT
    s.day            AS qgp_date,
    'BOUNDARY_FIRST' AS week_type,
    bs.qgp_date      AS boundary_stub_date
  FROM BoundaryStubs bs
  JOIN DateSpine s
    ON  s.day > bs.qgp_date
    AND EXTRACT(DAYOFWEEK FROM s.day) = 7
  QUALIFY ROW_NUMBER() OVER (PARTITION BY bs.qgp_date ORDER BY s.day ASC) = 1
),

-- Combine all QGP dates — BOUNDARY_FIRST overrides NORMAL for same Saturday
AllQgpDates AS (
  SELECT qgp_date, week_type, boundary_stub_date FROM BoundaryStubs
  UNION ALL
  SELECT qgp_date, week_type, boundary_stub_date FROM BoundaryFirsts
  UNION ALL
  SELECT s.qgp_date, s.week_type, s.boundary_stub_date
  FROM Saturdays s
  WHERE s.qgp_date NOT IN (SELECT qgp_date FROM BoundaryFirsts)
),

-- Enrich with calendar attributes
Enriched AS (
  SELECT
    aq.qgp_date,
    aq.week_type,
    aq.boundary_stub_date,

    EXTRACT(YEAR    FROM aq.qgp_date) AS qgp_year,
    EXTRACT(QUARTER FROM aq.qgp_date) AS qgp_quarter_num,
    CONCAT(
      CAST(EXTRACT(YEAR    FROM aq.qgp_date) AS STRING), ' Q',
      CAST(EXTRACT(QUARTER FROM aq.qgp_date) AS STRING)
    )                                 AS quarter,

    DATE_SUB(
      DATE_ADD(DATE_TRUNC(aq.qgp_date, QUARTER), INTERVAL 3 MONTH),
      INTERVAL 1 DAY
    )                                 AS quarter_end_date,

    EXTRACT(ISOWEEK FROM aq.qgp_date) AS iso_week_number,
    EXTRACT(ISOYEAR FROM aq.qgp_date) AS iso_year,

    -- Days in period
    CASE aq.week_type
      WHEN 'NORMAL' THEN 7
      WHEN 'BOUNDARY_STUB' THEN
        DATE_DIFF(
          aq.qgp_date,
          DATE_SUB(aq.qgp_date, INTERVAL (EXTRACT(DAYOFWEEK FROM aq.qgp_date) - 1) DAY),
          DAY
        ) + 1
      WHEN 'BOUNDARY_FIRST' THEN
        7 - (
          DATE_DIFF(
            aq.boundary_stub_date,
            DATE_SUB(aq.boundary_stub_date, INTERVAL (EXTRACT(DAYOFWEEK FROM aq.boundary_stub_date) - 1) DAY),
            DAY
          ) + 1
        )
    END                               AS days_in_period,

    aq.qgp_date <= CURRENT_DATE()     AS is_complete_period,
    DATE_TRUNC(aq.qgp_date, QUARTER)
      = DATE_TRUNC(CURRENT_DATE(), QUARTER) AS is_current_quarter

  FROM AllQgpDates aq
),

-- WoW prior date via LAG
WithWow AS (
  SELECT
    e.*,
    CASE
      WHEN e.week_type = 'BOUNDARY_STUB'  THEN NULL
      WHEN e.week_type = 'BOUNDARY_FIRST' THEN LAG(e.qgp_date, 2) OVER (ORDER BY e.qgp_date ASC)
      ELSE                                     LAG(e.qgp_date, 1) OVER (ORDER BY e.qgp_date ASC)
    END AS wow_prior_qgp_date
  FROM Enriched e
),

-- Prior year lookup for YoY
PriorYearLookup AS (
  SELECT qgp_date, iso_week_number, iso_year, week_type
  FROM Enriched
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
FROM WithWow w
LEFT JOIN PriorYearLookup ly
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
FROM WithWow w
WHERE w.week_type = 'BOUNDARY_STUB'
;