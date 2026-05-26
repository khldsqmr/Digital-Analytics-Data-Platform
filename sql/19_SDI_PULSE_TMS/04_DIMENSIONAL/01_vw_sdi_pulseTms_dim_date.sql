/* =================================================================================================
FILE:         01_vw_sdi_pulseTms_dim_date.sql
LAYER:        Dimension View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseTms_dim_date

PURPOSE:
  Date dimension for the pulseTms reporting layer.
  Maps weeks to quarters, computes partial weights for boundary weeks,
  assigns display dates, and tags each row with section_name.

  Produces two distinct row sets:

  ── section_name = 'Table' ──────────────────────────────────────────────
  Driven from the QUARTER SPINE — all Saturdays within any started quarter,
  including future weeks within the current quarter that have no data yet.
  These rows ensure the table always shows a full quarter of columns even
  when data is still loading for upcoming weeks.

  Rules:
    - Only quarters where quarter_start <= CURRENT_DATE() are included
      (no phantom future quarters like 2031-Q4 in the dropdown)
    - All weeks within a started quarter are included, even future ones
    - Straddling weeks appear TWICE — once per quarter they touch
    - partial_weight = days_in_quarter / 7.0
    - display_date = quarter_end for prior-quarter portion of straddling week
    - display_date = week_sun_to_sat for all other rows

  ── section_name = 'Trend' ──────────────────────────────────────────────
  Driven from GOLD LONG actual data — only weeks where data exists.
  No future phantom weeks ever appear on trend lines.
  partial_weight = 1.0 always (raw full week value, never distorted).
  year_quarter = NULL (trend lines are not quarter-scoped).

QUARTER CONVENTION:
  Standard calendar quarters:
    Q1 = Jan 1  – Mar 31
    Q2 = Apr 1  – Jun 30
    Q3 = Jul 1  – Sep 30
    Q4 = Oct 1  – Dec 31

STRADDLING WEEK LOGIC:
  Week runs Sunday through Saturday:
    week_start = week_sun_to_sat - 6 DAYS
    week_end   = week_sun_to_sat
  days_in_quarter = overlapping days between week and quarter (inclusive)
  partial_weight  = days_in_quarter / 7.0

  Example — week ending Apr-04, Q2 starts Apr-01:
    Q1 row: days = 3 (Mar 29, 30, 31), weight = 3/7 ≈ 0.428571
            display_date = Mar-31 (quarter_end)
    Q2 row: days = 4 (Apr 1, 2, 3, 4),  weight = 4/7 ≈ 0.571429
            display_date = Apr-04 (week_sun_to_sat)

WoW NOTE:
  wow_pct in the reporting view is NEVER affected by partial_weight.
  WoW always compares full week to full week regardless of section_name.

ADDING NEW SOURCES (MFC Spend, Platform, etc.):
  No changes needed to this view.
  dim_date is source-agnostic — it maps weeks to quarters independently
  of which sources are in Gold Long. New sources automatically flow
  through the reporting view without any dim_date changes.

OUTPUT SCHEMA:
  week_sun_to_sat  : DATE    — Actual week ending Saturday (Sun-to-Sat)
  display_date     : DATE    — Date label to use in dashboard column headers
  year_quarter     : STRING  — '2026-Q2' format; NULL for Trend rows
  quarter_start    : DATE    — First day of quarter; NULL for Trend rows
  quarter_end      : DATE    — Last day of quarter; NULL for Trend rows
  days_in_quarter  : INT64   — Days of this week in this quarter (1–7)
  partial_weight   : FLOAT64 — days_in_quarter / 7.0 (1.0 for full weeks)
  is_partial_week  : BOOL    — TRUE if week straddles a quarter boundary
  week_of_quarter  : INT64   — Week number within quarter (1-based); NULL for Trend
  week_label       : STRING  — e.g. 'W01 (Apr-04)'; NULL for Trend
  section_name     : STRING  — 'Table' or 'Trend'

DOWNSTREAM:
  11_vw_sdi_pulseTms_reporting
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_date`
AS

WITH

-- -----------------------------------------------------------------------
-- STEP 1: Define quarter boundaries
-- Only include quarters that have already started (quarter_start <= today)
-- This prevents phantom future quarters from appearing in dropdowns
-- Current quarter IS included even though it has future weeks within it
-- -----------------------------------------------------------------------
quarters AS (
    SELECT
        quarter_start,
        DATE_SUB(
            DATE_ADD(quarter_start, INTERVAL 3 MONTH),
            INTERVAL 1 DAY
        )                                               AS quarter_end,
        CONCAT(
            CAST(EXTRACT(YEAR  FROM quarter_start) AS STRING),
            '-Q',
            CAST(EXTRACT(QUARTER FROM quarter_start) AS STRING)
        )                                               AS year_quarter
    FROM UNNEST([
        -- 2024
        DATE '2024-01-01', DATE '2024-04-01',
        DATE '2024-07-01', DATE '2024-10-01',
        -- 2025
        DATE '2025-01-01', DATE '2025-04-01',
        DATE '2025-07-01', DATE '2025-10-01',
        -- 2026
        DATE '2026-01-01', DATE '2026-04-01',
        DATE '2026-07-01', DATE '2026-10-01',
        -- 2027
        DATE '2027-01-01', DATE '2027-04-01',
        DATE '2027-07-01', DATE '2027-10-01',
        -- 2028
        DATE '2028-01-01', DATE '2028-04-01',
        DATE '2028-07-01', DATE '2028-10-01'
    ]) AS quarter_start
    -- Only quarters that have started — no phantom future quarter dropdowns
    WHERE quarter_start <= CURRENT_DATE()
),

-- -----------------------------------------------------------------------
-- STEP 2: Generate week spine for Table rows
-- All possible Saturdays that overlap with any started quarter
-- For each quarter, walk forward in 7-day steps from the first Saturday
-- that overlaps with the quarter start
-- This correctly handles weeks that straddle quarter boundaries
-- -----------------------------------------------------------------------
quarter_week_spine AS (
    SELECT
        q.year_quarter,
        q.quarter_start,
        q.quarter_end,
        -- Generate all Saturdays that could overlap with this quarter
        -- Start from the first Saturday on or before quarter_start
        -- (which is the Saturday of the week containing quarter_start)
        DATE_ADD(
            -- Find the Saturday of the week containing quarter_start
            -- Week convention: Sunday start, so Saturday = Sunday + 6
            DATE_ADD(
                DATE_TRUNC(q.quarter_start, WEEK(SUNDAY)),
                INTERVAL 6 DAY
            ),
            INTERVAL n WEEK
        )                                               AS week_sun_to_sat
    FROM quarters q
    -- Generate enough weeks to cover any quarter (max 14 weeks covers all cases)
    CROSS JOIN UNNEST(GENERATE_ARRAY(0, 13)) AS n
),

-- -----------------------------------------------------------------------
-- STEP 3: Filter to weeks that actually overlap with their quarter
-- A week overlaps a quarter if:
--   week_start (Saturday - 6) <= quarter_end
--   AND week_end (Saturday) >= quarter_start
-- -----------------------------------------------------------------------
table_week_quarter AS (
    SELECT
        s.week_sun_to_sat,
        s.year_quarter,
        s.quarter_start,
        s.quarter_end,

        -- Days of this week falling in this quarter (inclusive overlap)
        GREATEST(0,
            DATE_DIFF(
                LEAST(s.week_sun_to_sat, s.quarter_end),
                GREATEST(
                    DATE_SUB(s.week_sun_to_sat, INTERVAL 6 DAY),
                    s.quarter_start
                ),
                DAY
            ) + 1
        )                                               AS days_in_quarter

    FROM quarter_week_spine s
    -- Keep only weeks that overlap with the quarter
    WHERE DATE_SUB(s.week_sun_to_sat, INTERVAL 6 DAY) <= s.quarter_end
      AND s.week_sun_to_sat                            >= s.quarter_start
),

-- -----------------------------------------------------------------------
-- STEP 4: Compute partial_weight, is_partial_week, week_of_quarter
-- -----------------------------------------------------------------------
table_with_weights AS (
    SELECT
        week_sun_to_sat,
        year_quarter,
        quarter_start,
        quarter_end,
        days_in_quarter,
        ROUND(days_in_quarter / 7.0, 6)                AS partial_weight,
        days_in_quarter < 7                             AS is_partial_week,

        -- Week number within the quarter (1-based)
        -- Straddling week at quarter start = W01
        -- Straddling week at quarter end = highest week number
        ROW_NUMBER() OVER (
            PARTITION BY year_quarter
            ORDER BY week_sun_to_sat
        )                                               AS week_of_quarter

    FROM table_week_quarter
    WHERE days_in_quarter > 0
),

-- -----------------------------------------------------------------------
-- STEP 5: Assign display_date for Table rows
--
-- Straddling week, prior-quarter portion:
--   The Saturday falls AFTER the quarter_end — meaning the bulk of the
--   week is in the next quarter. This row represents the days at the
--   tail end of the quarter. Display at quarter_end (e.g. Mar-31, Jun-30).
--
-- All other rows (full weeks + leading portion of straddling week):
--   Display at week_sun_to_sat (the actual Saturday).
-- -----------------------------------------------------------------------
table_rows AS (
    SELECT
        week_sun_to_sat,
        year_quarter,
        quarter_start,
        quarter_end,
        days_in_quarter,
        partial_weight,
        is_partial_week,
        week_of_quarter,

        CASE
            WHEN is_partial_week
             AND week_sun_to_sat > quarter_end
                THEN quarter_end      -- prior-quarter tail → show at quarter end date
            ELSE week_sun_to_sat      -- full week or leading partial → show at Saturday
        END                                             AS display_date,

        -- Dashboard column label
        CONCAT(
            'W',
            LPAD(CAST(week_of_quarter AS STRING), 2, '0'),
            ' (',
            FORMAT_DATE('%b-%d', week_sun_to_sat),
            ')'
        )                                               AS week_label,

        'Table'                                         AS section_name

    FROM table_with_weights
),

-- -----------------------------------------------------------------------
-- STEP 6: Build Trend rows
-- Driven from Gold Long actual data — no phantom future weeks
-- One row per distinct week_sun_to_sat that exists in Gold Long
-- partial_weight = 1.0 always (raw metric value, never apportioned)
-- year_quarter = NULL (trend lines are not quarter-scoped)
-- -----------------------------------------------------------------------
trend_rows AS (
    SELECT DISTINCT
        week_sun_to_sat,
        week_sun_to_sat                                 AS display_date,
        CAST(NULL AS STRING)                            AS year_quarter,
        CAST(NULL AS DATE)                              AS quarter_start,
        CAST(NULL AS DATE)                              AS quarter_end,
        7                                               AS days_in_quarter,
        1.0                                             AS partial_weight,
        FALSE                                           AS is_partial_week,
        CAST(NULL AS INT64)                             AS week_of_quarter,
        CAST(NULL AS STRING)                            AS week_label,
        'Trend'                                         AS section_name
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_gold_unified_long`
)

-- -----------------------------------------------------------------------
-- FINAL OUTPUT
-- UNION ALL of Table rows and Trend rows
--
-- Full week (entirely within one quarter):
--   1 Table row  — partial_weight = 1.0, display_date = week_sun_to_sat
--   1 Trend row  — partial_weight = 1.0, year_quarter = NULL
--
-- Straddling week (crosses a quarter boundary):
--   2 Table rows — one per quarter, each with proportional partial_weight
--                  prior-quarter row: display_date = quarter_end
--                  current-quarter row: display_date = week_sun_to_sat
--   1 Trend row  — partial_weight = 1.0, raw full week value, year_quarter = NULL
--
-- Future weeks within current quarter (no data yet):
--   1 Table row  — shows as column in table with NULL metric values
--   0 Trend rows — Trend spine is data-driven, never shows future weeks
-- -----------------------------------------------------------------------
SELECT
    week_sun_to_sat,
    display_date,
    year_quarter,
    quarter_start,
    quarter_end,
    days_in_quarter,
    partial_weight,
    is_partial_week,
    week_of_quarter,
    week_label,
    section_name
FROM table_rows

UNION ALL

SELECT
    week_sun_to_sat,
    display_date,
    year_quarter,
    quarter_start,
    quarter_end,
    days_in_quarter,
    partial_weight,
    is_partial_week,
    week_of_quarter,
    week_label,
    section_name
FROM trend_rows

ORDER BY
    week_sun_to_sat  ASC,
    section_name     ASC,   -- Table before Trend alphabetically
    year_quarter     ASC
;