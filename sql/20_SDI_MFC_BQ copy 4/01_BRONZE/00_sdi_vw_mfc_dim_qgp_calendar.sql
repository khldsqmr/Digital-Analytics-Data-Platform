-- ============================================================
-- MFC QGP CALENDAR DIMENSION — BigQuery
-- ============================================================
CREATE OR REPLACE VIEW
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_dim_qgp_calendar`
AS

WITH

date_spine AS (
  SELECT day
  FROM UNNEST(GENERATE_DATE_ARRAY(
    DATE '2020-01-01',
    DATE_ADD(
      GREATEST(
        LAST_DAY(CURRENT_DATE(), YEAR),
        LAST_DAY(DATE_ADD(CURRENT_DATE(), INTERVAL 3 MONTH), QUARTER)
      ),
      INTERVAL 7 DAY
    )
  )) AS day
),

quarter_ends AS (
  SELECT DISTINCT
    LAST_DAY(day, QUARTER) AS quarter_end_date
  FROM date_spine
),

saturdays AS (
  SELECT
    day AS qgp_date,
    'NORMAL' AS week_type,
    CAST(NULL AS DATE) AS boundary_stub_date
  FROM date_spine
  WHERE EXTRACT(DAYOFWEEK FROM day) = 7
),

boundary_stubs AS (
  SELECT
    quarter_end_date AS qgp_date,
    'BOUNDARY_STUB' AS week_type,
    CAST(NULL AS DATE) AS boundary_stub_date
  FROM quarter_ends
  WHERE EXTRACT(DAYOFWEEK FROM quarter_end_date) != 7
),

boundary_firsts AS (
  SELECT
    s.day AS qgp_date,
    'BOUNDARY_FIRST' AS week_type,
    bs.qgp_date AS boundary_stub_date
  FROM boundary_stubs bs
  JOIN date_spine s
    ON  s.day > bs.qgp_date
    AND EXTRACT(DAYOFWEEK FROM s.day) = 7
  QUALIFY ROW_NUMBER() OVER (PARTITION BY bs.qgp_date ORDER BY s.day ASC) = 1
),

all_qgp_dates AS (
  SELECT qgp_date, week_type, boundary_stub_date FROM boundary_stubs
  UNION ALL
  SELECT qgp_date, week_type, boundary_stub_date FROM boundary_firsts
  UNION ALL
  SELECT s.qgp_date, s.week_type, s.boundary_stub_date
  FROM saturdays s
  WHERE s.qgp_date NOT IN (SELECT qgp_date FROM boundary_firsts)
),

enriched AS (
  SELECT
    aq.qgp_date,
    aq.week_type,
    aq.boundary_stub_date,

    EXTRACT(YEAR FROM aq.qgp_date) AS qgp_year,
    EXTRACT(QUARTER FROM aq.qgp_date) AS qgp_quarter_num,
    CONCAT(CAST(EXTRACT(YEAR FROM aq.qgp_date) AS STRING), ' Q',
           CAST(EXTRACT(QUARTER FROM aq.qgp_date) AS STRING)) AS quarter,

    LAST_DAY(aq.qgp_date, QUARTER) AS quarter_end_date,

    EXTRACT(ISOWEEK FROM aq.qgp_date) AS iso_week_number,
    EXTRACT(YEAR FROM aq.qgp_date) AS iso_year,

    -- Days in period (Monday-anchored via WEEK(MONDAY) truncation)
    CASE aq.week_type
      WHEN 'NORMAL' THEN 7
      WHEN 'BOUNDARY_STUB' THEN
        DATE_DIFF(aq.qgp_date, DATE_TRUNC(aq.qgp_date, WEEK(MONDAY)), DAY) + 1
      WHEN 'BOUNDARY_FIRST' THEN
        7 - (DATE_DIFF(aq.boundary_stub_date, DATE_TRUNC(aq.boundary_stub_date, WEEK(MONDAY)), DAY) + 1)
    END AS days_in_period,

    aq.qgp_date <= CURRENT_DATE() AS is_complete_period,
    DATE_TRUNC(aq.qgp_date, QUARTER) = DATE_TRUNC(CURRENT_DATE(), QUARTER) AS is_current_quarter

  FROM all_qgp_dates aq
),

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

prior_year_lookup AS (
  SELECT qgp_date, iso_week_number, iso_year, week_type
  FROM enriched
  WHERE week_type IN ('NORMAL', 'BOUNDARY_FIRST')
)

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