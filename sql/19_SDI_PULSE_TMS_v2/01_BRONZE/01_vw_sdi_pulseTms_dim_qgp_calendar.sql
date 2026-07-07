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
    1. Every week-ending Saturday                          → week_type = 'NORMAL'
    2. Quarter-end dates that fall on a non-Saturday       → week_type = 'BOUNDARY_STUB'
       (e.g. Mar 31 if it falls Mon–Fri)
    3. The first Saturday after a BOUNDARY_STUB            → week_type = 'BOUNDARY_FIRST'
       (e.g. Apr 5 if Mar 31 was the stub)

  Quarter boundaries follow standard Gregorian quarters:
    Q1: Jan 1  – Mar 31
    Q2: Apr 1  – Jun 30
    Q3: Jul 1  – Sep 30
    Q4: Oct 1  – Dec 31

BUSINESS GRAIN:
  One row per QGP date.

KEY COLUMNS:
  qgp_date                — Period-end date (Sat or quarter-end non-Sat)
  qgp_year                — Calendar year of qgp_date
  qgp_quarter_num         — Quarter number 1–4
  quarter                 — Display string e.g. '2026 Q1'
  quarter_end_date        — Last calendar date of the quarter this period belongs to
  iso_week_number         — ISO 8601 week number (used for YoY same-week matching)
  iso_year                — ISO year (may differ from calendar year near year boundaries)
  week_type               — 'NORMAL' | 'BOUNDARY_STUB' | 'BOUNDARY_FIRST'
  days_in_period          — 7 for NORMAL; <7 for BOUNDARY_STUB; remainder days for BOUNDARY_FIRST
  is_complete_period      — TRUE when qgp_date <= CURRENT_DATE()
  is_current_quarter      — TRUE when qgp_date falls in the current calendar quarter
  boundary_stub_date      — For BOUNDARY_FIRST rows: the preceding stub date (e.g. Mar 31)
                            NULL for NORMAL and BOUNDARY_STUB rows
  wow_prior_qgp_date      — The immediately preceding QGP date (for WoW denominator lookup)
                            NULL for BOUNDARY_STUB rows (WoW not shown for stubs)
  prior_year_qgp_date          — Deprecated legacy compatibility field. Always NULL.
                                 LY is no longer resolved by matching to one prior-year QGP date.
  prior_year_days_in_period    — Deprecated legacy compatibility field. Always 7.
  prior_year_iso_year          — ISO year used for LY lookup: iso_year - 1.
  prior_year_iso_week_number   — ISO week used for LY lookup: iso_week_number.
  ly_day_allocation_factor     — days_in_period / 7. Silver applies this to the
                                 prior-year same ISO week total for LY trend values.

BUSINESS RULES:
  - BOUNDARY_STUB rows exist solely to hold partial-period metric values.
    They are never shown as standalone WoW or YoY comparison points.
    wow_prior_qgp_date is NULL for these rows.
  - LY is based on the same ISO week number in the prior ISO year.
    Silver aggregates that prior-year ISO week into one weekly value and allocates
    it by current-row days_in_period / 7. The calendar does not join to prior-year
    QGP rows, which prevents duplicate BOUNDARY_STUB dates.
  - BOUNDARY_FIRST rows carry combined WoW numerator = current value + preceding stub value.
    wow_denominator = value at wow_prior_qgp_date (the last full NORMAL week).
  - The second NORMAL Saturday after a quarter boundary uses
    (BOUNDARY_FIRST value + stub value) as its WoW denominator — handled in Silver via
    boundary_stub_date lookup on the prior QGP date.
  - is_complete_period uses qgp_date <= CURRENT_DATE() inclusive — if today IS the
    week-ending Saturday or quarter-end date, that period is considered complete.
  - Date spine covers 2020-01-01 through end of the following calendar year (rolling).
  - ISO week matching for LY/YoY: same iso_week_number in prior iso_year.
    Partial periods use days_in_period / 7 allocation in Silver.

DOWNSTREAM:
  02_sp_sdi_pulseTms_bronze_adobeFunnel_weekly   (joined in Silver)
  03_sp_sdi_pulseTms_bronze_mfcSpend_weekly      (joined in Silver)
  04_sp_sdi_pulseTms_bronze_platformSpend_weekly (joined in Silver)
  05_sp_sdi_pulseTms_silver_adobeFunnel_weekly
  06_sp_sdi_pulseTms_silver_mfcSpend_weekly
  07_sp_sdi_pulseTms_silver_platformSpend_weekly

CHANGE LOG:
  - Fixed duplicate BOUNDARY_STUB rows caused by prior-year ISO-week joins.
  - Removed prior-year QGP-date resolution from calendar. LY is now calculated
    in Silver as prior-year same ISO week weekly total × current days_in_period / 7.
  - Kept prior_year_qgp_date and prior_year_days_in_period as deprecated legacy
    fields for compatibility; added ISO-week helper fields for QA/debugging.
================================================================================================= */

CREATE OR REPLACE VIEW
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar`
AS

WITH

-- ---------------------------------------------------------------------------
-- STEP 1: Generate a daily date spine
--         Rolling range: 2020-01-01 through end of next calendar year.
--         Auto-extends each year — no manual updates needed.
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
-- STEP 2: Identify all Gregorian quarter-end dates within the spine
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
-- STEP 3A: All Saturdays → NORMAL weeks
-- ---------------------------------------------------------------------------
Saturdays AS (
  SELECT
    day                  AS qgp_date,
    'NORMAL'             AS week_type,
    CAST(NULL AS DATE)   AS boundary_stub_date
  FROM DateSpine
  WHERE EXTRACT(DAYOFWEEK FROM day) = 7  -- 7 = Saturday in BigQuery
),

-- ---------------------------------------------------------------------------
-- STEP 3B: Quarter-end dates that fall on a non-Saturday → BOUNDARY_STUB
-- ---------------------------------------------------------------------------
BoundaryStubs AS (
  SELECT
    quarter_end_date     AS qgp_date,
    'BOUNDARY_STUB'      AS week_type,
    CAST(NULL AS DATE)   AS boundary_stub_date
  FROM QuarterEnds
  WHERE EXTRACT(DAYOFWEEK FROM quarter_end_date) != 7
),

-- ---------------------------------------------------------------------------
-- STEP 3C: First Saturday after each BOUNDARY_STUB → BOUNDARY_FIRST
--          This Saturday is already in Saturdays CTE as NORMAL;
--          we override its week_type here and carry the stub date.
-- ---------------------------------------------------------------------------
BoundaryFirsts AS (
  SELECT
    s.day                       AS qgp_date,
    'BOUNDARY_FIRST'            AS week_type,
    bs.qgp_date                 AS boundary_stub_date
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
-- STEP 4: Combine all QGP dates
--         BOUNDARY_FIRST overrides the same Saturday's NORMAL entry
-- ---------------------------------------------------------------------------
AllQgpDates AS (
  SELECT qgp_date, week_type, boundary_stub_date FROM BoundaryStubs
  UNION ALL
  SELECT qgp_date, week_type, boundary_stub_date FROM BoundaryFirsts
  UNION ALL
  -- NORMAL Saturdays that are NOT a BOUNDARY_FIRST
  SELECT s.qgp_date, s.week_type, s.boundary_stub_date
  FROM Saturdays s
  WHERE s.qgp_date NOT IN (SELECT qgp_date FROM BoundaryFirsts)
),

-- ---------------------------------------------------------------------------
-- STEP 5: Enrich with calendar attributes
-- ---------------------------------------------------------------------------
Enriched AS (
  SELECT
    aq.qgp_date,
    aq.week_type,
    aq.boundary_stub_date,

    EXTRACT(YEAR    FROM aq.qgp_date)                                   AS qgp_year,
    EXTRACT(QUARTER FROM aq.qgp_date)                                   AS qgp_quarter_num,
    CONCAT(
      CAST(EXTRACT(YEAR    FROM aq.qgp_date) AS STRING),
      ' Q',
      CAST(EXTRACT(QUARTER FROM aq.qgp_date) AS STRING)
    )                                                                   AS quarter,

    -- Quarter end date: last day of the quarter containing qgp_date
    DATE_SUB(
      DATE_ADD(DATE_TRUNC(aq.qgp_date, QUARTER), INTERVAL 3 MONTH),
      INTERVAL 1 DAY
    )                                                                   AS quarter_end_date,

    EXTRACT(ISOWEEK  FROM aq.qgp_date)                                  AS iso_week_number,
    EXTRACT(ISOYEAR  FROM aq.qgp_date)                                  AS iso_year,

    -- Days in period:
    --   NORMAL         : always 7
    --   BOUNDARY_STUB  : days from Sunday that opened this week through the quarter-end date
    --   BOUNDARY_FIRST : complement = 7 - stub_days_in_period
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
    END                                                                 AS days_in_period,

    aq.qgp_date <= CURRENT_DATE()                                       AS is_complete_period,
    DATE_TRUNC(aq.qgp_date, QUARTER) = DATE_TRUNC(CURRENT_DATE(), QUARTER)
                                                                        AS is_current_quarter
  FROM AllQgpDates aq
),

-- ---------------------------------------------------------------------------
-- STEP 6: Compute wow_prior_qgp_date via LAG
--         BOUNDARY_STUB rows get NULL — WoW not shown for partial periods
--         BOUNDARY_FIRST skips past the stub (LAG 2) to reach last NORMAL week
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
)

-- ---------------------------------------------------------------------------
-- STEP 7: Final output
--
-- LY logic is intentionally NOT resolved by joining to a prior-year QGP date.
-- Boundary weeks can have two QGP rows in the same ISO week (stub + first),
-- which caused duplicate calendar rows and downstream row multiplication.
--
-- Silver should calculate LY as:
--   prior-year same ISO week weekly total × days_in_period / 7
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

  -- Deprecated compatibility fields. Do not use these for LY logic.
  CAST(NULL AS DATE) AS prior_year_qgp_date,
  CAST(7 AS INT64)  AS prior_year_days_in_period,

  -- LY helper fields used by Silver QA/debugging.
  w.iso_year - 1 AS prior_year_iso_year,
  w.iso_week_number AS prior_year_iso_week_number,
  SAFE_DIVIDE(w.days_in_period, 7) AS ly_day_allocation_factor
FROM WithWow w
;