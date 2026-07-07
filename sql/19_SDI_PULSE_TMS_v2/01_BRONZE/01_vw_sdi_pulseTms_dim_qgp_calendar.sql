/* =================================================================================================
FILE:         01_vw_sdi_pulseTms_dim_qgp_calendar.sql
LAYER:        Dimension View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseTms_dim_qgp_calendar

RAW SOURCES:
  None — derived entirely from the Gregorian calendar using GENERATE_DATE_ARRAY.

PURPOSE:
  Foundational QGP (Quarter-Grand-Period) calendar dimension for the PulseTMS pipeline.
  All Bronze, Silver, and Gold views join to this dim for date alignment, week typing,
  and WoW / YoY period lookups.

  QGP dates are defined as:
    1. Every week-ending Saturday                          -> week_type = 'NORMAL'
    2. Quarter-end dates that fall on a non-Saturday       -> week_type = 'BOUNDARY_STUB'
       Example: Mar 31 if it falls Mon-Fri or Sunday
    3. The first Saturday after a BOUNDARY_STUB            -> week_type = 'BOUNDARY_FIRST'
       Example: Apr 4 or Apr 5 depending on the year

  Quarter boundaries follow standard Gregorian quarters:
    Q1: Jan 1  - Mar 31
    Q2: Apr 1  - Jun 30
    Q3: Jul 1  - Sep 30
    Q4: Oct 1  - Dec 31

BUSINESS GRAIN:
  One row per QGP date.

KEY COLUMNS:
  qgp_date                — Period-end date; Saturday or quarter-end non-Saturday
  qgp_year                — Calendar year of qgp_date
  qgp_quarter_num         — Quarter number 1-4
  quarter                 — Display string, e.g. '2026 Q1'
  quarter_end_date        — Last calendar date of the quarter that contains qgp_date
  iso_week_number         — ISO week number of qgp_date
  iso_year                — ISO year of qgp_date
  week_type               — 'NORMAL' | 'BOUNDARY_STUB' | 'BOUNDARY_FIRST'
  days_in_period          — 7 for NORMAL; partial days for BOUNDARY_STUB / BOUNDARY_FIRST
  is_complete_period      — TRUE when qgp_date <= CURRENT_DATE()
  is_current_quarter      — TRUE when qgp_date falls in current Gregorian quarter
  boundary_stub_date      — For BOUNDARY_FIRST only, the preceding stub date
  wow_prior_qgp_date      — Previous comparable QGP date for WoW denominator lookup

NEW LY DESIGN:
  This version fixes duplicate LY rows by making prior-year mapping deterministic.

  Problem in prior logic:
    BOUNDARY_STUB rows joined to prior year using only:
      iso_week_number + iso_year - 1

    That could match multiple prior-year QGP rows in the same ISO week:
      NORMAL
      BOUNDARY_STUB
      BOUNDARY_FIRST

    Result:
      One current qgp_date could produce multiple prior_year_qgp_date values,
      which created duplicate long rows with multiple metric_value_ly values.

  Corrected design:
    Define a canonical source week-ending Saturday for every QGP row:

      NORMAL          -> source_week_end_date = qgp_date
      BOUNDARY_FIRST  -> source_week_end_date = qgp_date
      BOUNDARY_STUB   -> source_week_end_date = next Saturday after qgp_date

    Then map the current source week to exactly one prior-year source Saturday:

      prior_year_qgp_date =
        Saturday in prior ISO year with same source_iso_week_number

    Silver then computes LY as:
      prior_year_full_week_value * current days_in_period / 7

    If prior year's source Saturday was a BOUNDARY_FIRST, Silver recombines:
      prior-year BOUNDARY_STUB + prior-year BOUNDARY_FIRST

    before applying the current year days_in_period allocation.

BUSINESS RULES:
  - BOUNDARY_STUB rows exist to hold partial-period metric values.
  - WoW is suppressed for BOUNDARY_STUB rows.
  - BOUNDARY_FIRST rows carry the remaining part of the split week.
  - The sum of BOUNDARY_STUB + BOUNDARY_FIRST equals the full source week.
  - LY trend values are allocated by current-year days_in_period / 7 in Silver.
  - This calendar view must always return exactly one row per qgp_date.

DOWNSTREAM:
  02_sp_sdi_pulseTms_bronze_adobeFunnel_weekly
  03_sp_sdi_pulseTms_bronze_mfcSpend_weekly
  04_sp_sdi_pulseTms_bronze_platformSpend_weekly
  05_sp_sdi_pulseTms_silver_adobeFunnel_weekly
  06_sp_sdi_pulseTms_silver_mfcSpend_weekly
  07_sp_sdi_pulseTms_silver_platformSpend_weekly
  08_vw_sdi_pulseTms_gold_unified_long
  09_vw_sdi_pulseTms_gold_wide_channel

CHANGE LOG:
  - Replaced loose BOUNDARY_STUB prior-year lookup.
  - Added source_week_end_date, source_iso_week_number, source_iso_year.
  - prior_year_qgp_date now points to one prior-year source Saturday.
  - prior_year_days_in_period is retained for backward compatibility and set to 7.
  - Calendar output is one row per qgp_date.
================================================================================================= */

CREATE OR REPLACE VIEW
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar`
AS

WITH

-- ---------------------------------------------------------------------------
-- STEP 1: Generate daily date spine
--         Rolling range: 2020-01-01 through end of next calendar year.
-- ---------------------------------------------------------------------------
DateSpine AS (
  SELECT day
  FROM UNNEST(
    GENERATE_DATE_ARRAY(
      DATE '2020-01-01',
      DATE_ADD(
        DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 YEAR), YEAR),
        INTERVAL -1 DAY
      )
    )
  ) AS day
),

-- ---------------------------------------------------------------------------
-- STEP 2: Identify all Gregorian quarter-end dates within the spine.
-- ---------------------------------------------------------------------------
QuarterEnds AS (
  SELECT DISTINCT
    DATE_SUB(
      DATE_TRUNC(DATE_ADD(day, INTERVAL 1 DAY), QUARTER),
      INTERVAL 1 DAY
    ) AS quarter_end_date
  FROM DateSpine
),

-- ---------------------------------------------------------------------------
-- STEP 3A: All Saturdays become NORMAL candidate rows.
-- ---------------------------------------------------------------------------
Saturdays AS (
  SELECT
    day                AS qgp_date,
    'NORMAL'           AS week_type,
    CAST(NULL AS DATE) AS boundary_stub_date
  FROM DateSpine
  WHERE EXTRACT(DAYOFWEEK FROM day) = 7
),

-- ---------------------------------------------------------------------------
-- STEP 3B: Quarter-end dates that are not Saturdays become BOUNDARY_STUB rows.
-- ---------------------------------------------------------------------------
BoundaryStubs AS (
  SELECT
    quarter_end_date   AS qgp_date,
    'BOUNDARY_STUB'    AS week_type,
    CAST(NULL AS DATE) AS boundary_stub_date
  FROM QuarterEnds
  WHERE EXTRACT(DAYOFWEEK FROM quarter_end_date) != 7
),

-- ---------------------------------------------------------------------------
-- STEP 3C: First Saturday after each BOUNDARY_STUB becomes BOUNDARY_FIRST.
--          This Saturday is also present in Saturdays, so it will override NORMAL.
-- ---------------------------------------------------------------------------
BoundaryFirsts AS (
  SELECT
    s.day             AS qgp_date,
    'BOUNDARY_FIRST'  AS week_type,
    bs.qgp_date       AS boundary_stub_date
  FROM BoundaryStubs bs
  JOIN DateSpine s
    ON  s.day > bs.qgp_date
    AND EXTRACT(DAYOFWEEK FROM s.day) = 7
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY bs.qgp_date
    ORDER BY s.day ASC
  ) = 1
),

-- ---------------------------------------------------------------------------
-- STEP 4: Combine all QGP dates.
--         BOUNDARY_FIRST overrides the same Saturday's NORMAL entry.
-- ---------------------------------------------------------------------------
AllQgpDates AS (
  SELECT
    qgp_date,
    week_type,
    boundary_stub_date
  FROM BoundaryStubs

  UNION ALL

  SELECT
    qgp_date,
    week_type,
    boundary_stub_date
  FROM BoundaryFirsts

  UNION ALL

  SELECT
    s.qgp_date,
    s.week_type,
    s.boundary_stub_date
  FROM Saturdays s
  WHERE s.qgp_date NOT IN (
    SELECT qgp_date
    FROM BoundaryFirsts
  )
),

-- ---------------------------------------------------------------------------
-- STEP 5: Enrich QGP rows with calendar attributes and source week Saturday.
-- ---------------------------------------------------------------------------
EnrichedBase AS (
  SELECT
    aq.qgp_date,
    aq.week_type,
    aq.boundary_stub_date,

    EXTRACT(YEAR FROM aq.qgp_date)                                        AS qgp_year,
    EXTRACT(QUARTER FROM aq.qgp_date)                                     AS qgp_quarter_num,

    CONCAT(
      CAST(EXTRACT(YEAR FROM aq.qgp_date) AS STRING),
      ' Q',
      CAST(EXTRACT(QUARTER FROM aq.qgp_date) AS STRING)
    )                                                                     AS quarter,

    DATE_SUB(
      DATE_ADD(DATE_TRUNC(aq.qgp_date, QUARTER), INTERVAL 3 MONTH),
      INTERVAL 1 DAY
    )                                                                     AS quarter_end_date,

    EXTRACT(ISOWEEK FROM aq.qgp_date)                                     AS iso_week_number,
    EXTRACT(ISOYEAR FROM aq.qgp_date)                                     AS iso_year,

    -- Days in period:
    --   NORMAL         : full 7-day week
    --   BOUNDARY_STUB  : days from Sunday through quarter-end
    --   BOUNDARY_FIRST : remaining days after the stub through Saturday
    CASE aq.week_type
      WHEN 'NORMAL' THEN 7

      WHEN 'BOUNDARY_STUB' THEN
        DATE_DIFF(
          aq.qgp_date,
          DATE_SUB(
            aq.qgp_date,
            INTERVAL (EXTRACT(DAYOFWEEK FROM aq.qgp_date) - 1) DAY
          ),
          DAY
        ) + 1

      WHEN 'BOUNDARY_FIRST' THEN
        7 - (
          DATE_DIFF(
            aq.boundary_stub_date,
            DATE_SUB(
              aq.boundary_stub_date,
              INTERVAL (EXTRACT(DAYOFWEEK FROM aq.boundary_stub_date) - 1) DAY
            ),
            DAY
          ) + 1
        )
    END                                                                   AS days_in_period,

    aq.qgp_date <= CURRENT_DATE()                                         AS is_complete_period,

    DATE_TRUNC(aq.qgp_date, QUARTER) = DATE_TRUNC(CURRENT_DATE(), QUARTER)
                                                                            AS is_current_quarter,

    -- Canonical source week-ending Saturday:
    --   NORMAL / BOUNDARY_FIRST use the Saturday qgp_date.
    --   BOUNDARY_STUB points forward to the Saturday that closes the same Sun-Sat week.
    CASE
      WHEN aq.week_type = 'BOUNDARY_STUB'
        THEN DATE_ADD(
          aq.qgp_date,
          INTERVAL (7 - EXTRACT(DAYOFWEEK FROM aq.qgp_date)) DAY
        )
      ELSE aq.qgp_date
    END                                                                   AS source_week_end_date

  FROM AllQgpDates aq
),

-- ---------------------------------------------------------------------------
-- STEP 6: Add ISO attributes of the source week-ending Saturday.
--         LY uses these fields, not the stub's own qgp_date ISO week.
-- ---------------------------------------------------------------------------
Enriched AS (
  SELECT
    eb.*,
    EXTRACT(ISOWEEK FROM eb.source_week_end_date)                         AS source_iso_week_number,
    EXTRACT(ISOYEAR FROM eb.source_week_end_date)                         AS source_iso_year
  FROM EnrichedBase eb
),

-- ---------------------------------------------------------------------------
-- STEP 7: Compute WoW prior QGP date.
--         BOUNDARY_STUB has no WoW point.
--         BOUNDARY_FIRST skips over the stub to the last full NORMAL week.
-- ---------------------------------------------------------------------------
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

-- ---------------------------------------------------------------------------
-- STEP 8: Build eligible prior-year source Saturdays.
--         Only Saturdays can be prior_year_qgp_date anchors.
-- ---------------------------------------------------------------------------
PriorYearSourceSaturdays AS (
  SELECT
    qgp_date,
    source_iso_week_number,
    source_iso_year
  FROM Enriched
  WHERE EXTRACT(DAYOFWEEK FROM qgp_date) = 7
)

-- ---------------------------------------------------------------------------
-- STEP 9: Final calendar output.
--         One row per qgp_date.
--         prior_year_qgp_date is one prior-year source Saturday.
-- ---------------------------------------------------------------------------
SELECT
  w.qgp_date,
  w.week_type,
  w.boundary_stub_date,
  w.qgp_year,
  w.qgp_quarter_num,
  w.quarter,
  w.quarter_end_date,
  w.iso_week_number,
  w.iso_year,
  w.days_in_period,
  w.is_complete_period,
  w.is_current_quarter,
  w.wow_prior_qgp_date,

  -- Source-week anchor fields.
  -- These are useful for debugging and make the LY mapping transparent.
  w.source_week_end_date,
  w.source_iso_week_number,
  w.source_iso_year,

  -- Prior-year source Saturday.
  -- Silver joins to this date and, if needed, recombines its BOUNDARY_STUB.
  ly.qgp_date AS prior_year_qgp_date,

  -- Compatibility field.
  -- Silver now calculates LY using full source week * current days_in_period / 7.
  CAST(7 AS INT64) AS prior_year_days_in_period

FROM WithWow w
LEFT JOIN PriorYearSourceSaturdays ly
  ON  ly.source_iso_week_number = w.source_iso_week_number
  AND ly.source_iso_year        = w.source_iso_year - 1
;