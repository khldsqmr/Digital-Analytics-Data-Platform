-- ============================================================================
-- FILE  : 01_sdi_vw_mip_control_fiscalCalendar_static.sql
-- LAYER : CONTROL
-- PURPOSE:
--   Gregorian calendar quarters + Sunday-Saturday reporting weeks.
--
-- DOWNSTREAM COMPATIBILITY:
--   The output column names and data types are intentionally unchanged so that
--   Gold 01/02/03 and downstream App/API contracts continue to work as-is.
--
-- REPORTING CONTRACT:
--   - Quarter boundaries are Gregorian:
--       Q1 = Jan 1  - Mar 31
--       Q2 = Apr 1  - Jun 30
--       Q3 = Jul 1  - Sep 30
--       Q4 = Oct 1  - Dec 31
--   - Reporting weeks are always Sunday-Saturday.
--   - A cross-quarter/cross-year week is assigned using its Wednesday
--     (the midpoint of a Sunday-Saturday reporting week).
--   - weekOfYear is continuous within the reporting year: W1, W2, ... W52/W53.
--   - fiscalWeekOfQuarter is retained unchanged for backward compatibility,
--     but fiscalWeekCode/weekLabel use the year-level week number.
--   - LY comparison maps the same reporting-week number in the prior reporting year.
-- ============================================================================
CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static
COMMENT 'Control view: Gregorian quarters, Sunday-Saturday reporting weeks, and comparison-week relationships.'
AS
WITH weeks AS (
    SELECT
        weekStartDate,
        date_add(weekStartDate,6) AS weekEndDate,
        date_add(weekStartDate,3) AS wednesday
    FROM (
        SELECT explode(
            sequence(
                DATE '2023-12-31',
                DATE '2029-12-30',
                INTERVAL 7 DAYS
            )
        ) AS weekStartDate
    )
),
labelled AS (
    SELECT
        weekStartDate,
        weekEndDate,

        -- Keep existing output naming for downstream compatibility.
        year(wednesday) AS fiscalYear,
        quarter(wednesday) AS fiscalQuarter,

        -- Existing quarter-relative week number retained as-is.
        CAST(
            row_number() OVER (
                PARTITION BY year(wednesday),quarter(wednesday)
                ORDER BY weekStartDate
            ) AS INT
        ) AS fiscalWeekOfQuarter,

        -- Internal reporting-week number used for the report Week dropdown and LY.
        CAST(
            row_number() OVER (
                PARTITION BY year(wednesday)
                ORDER BY weekStartDate
            ) AS INT
        ) AS weekOfYear
    FROM weeks
),
finalCalendar AS (
    SELECT
        cur.weekStartDate,
        cur.weekEndDate,
        cur.fiscalYear,
        cur.fiscalQuarter,

        concat(
            CAST(cur.fiscalYear AS STRING),
            ' Q',
            CAST(cur.fiscalQuarter AS STRING)
        ) AS fiscalQuarterLabel,

        -- Retain this output column and its existing quarter-relative meaning.
        cur.fiscalWeekOfQuarter,

        -- Same output column name; week number now represents the reporting year.
        concat(
            CAST(cur.fiscalYear AS STRING),
            'Q',
            CAST(cur.fiscalQuarter AS STRING),
            'W',
            CAST(cur.weekOfYear AS STRING)
        ) AS fiscalWeekCode,

        -- Same output column name; W number now continues across the year.
        concat(
            'W',
            CAST(cur.weekOfYear AS STRING),
            ' · ',
            CASE
                WHEN month(cur.weekStartDate)=month(cur.weekEndDate)
                    THEN date_format(cur.weekStartDate,'d')
                ELSE date_format(cur.weekStartDate,'d MMM')
            END,
            ' to ',
            date_format(cur.weekEndDate,'d MMM')
        ) AS weekLabel,

        concat(
            'Week ending ',
            date_format(cur.weekEndDate,'d MMM yyyy')
        ) AS weekEndingLabel,

        date_add(cur.weekStartDate,-7) AS priorWeekStartDate,

        -- Four complete comparator weeks immediately preceding the target week.
        date_add(cur.weekStartDate,-28) AS fourWeekAvgStartDate,
        date_add(cur.weekStartDate,-7) AS fourWeekAvgEndDate,

        -- Strictly prefer the same reporting-week number in the prior year.
        -- The -364 fallback preserves the existing non-null contract at a rare
        -- 52/53-week year boundary where the exact prior-year W number is absent.
        coalesce(
            ly.weekStartDate,
            date_add(cur.weekStartDate,-364)
        ) AS sameWeekLastYearStartDate

    FROM labelled cur
    LEFT JOIN labelled ly
      ON ly.fiscalYear=cur.fiscalYear-1
     AND ly.weekOfYear=cur.weekOfYear
)
SELECT
    -- IMPORTANT: exact existing output schema/order retained.
    weekStartDate,
    weekEndDate,
    fiscalYear,
    fiscalQuarter,
    fiscalQuarterLabel,
    fiscalWeekOfQuarter,
    fiscalWeekCode,
    weekLabel,
    weekEndingLabel,
    priorWeekStartDate,
    fourWeekAvgStartDate,
    fourWeekAvgEndDate,
    sameWeekLastYearStartDate
FROM finalCalendar;


-- ============================================================================
-- VALIDATION / COMPATIBILITY CHECKS
-- ============================================================================

-- A. OUTPUT SCHEMA
-- Expected: same 13 columns consumed by existing Gold code.
-- DESCRIBE prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static;

-- B. WEEK NUMBER CONTINUES THROUGH THE YEAR
-- Example expectation: Q2 starts around W14 rather than resetting to W1.
-- SELECT
--     weekStartDate,
--     weekEndDate,
--     fiscalQuarterLabel,
--     fiscalWeekOfQuarter,
--     fiscalWeekCode,
--     weekLabel
-- FROM prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static
-- WHERE fiscalYear=2026
-- ORDER BY weekStartDate;

-- C. QUARTER DROPDOWN / WEEK DROPDOWN CONTRACT
-- Quarter filters the rows; Week retains year-level W number.
-- SELECT
--     fiscalQuarterLabel,
--     fiscalWeekCode,
--     weekLabel,
--     weekStartDate,
--     weekEndDate
-- FROM prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static
-- WHERE fiscalQuarterLabel='2026 Q4'
-- ORDER BY weekStartDate;

-- D. LY SAME-WEEK-NUMBER CHECK
-- Expected: current W50 maps to prior-year W50.
-- WITH x AS (
--     SELECT
--         weekStartDate,
--         fiscalYear,
--         fiscalWeekCode,
--         regexp_extract(fiscalWeekCode,'W([0-9]+)$',1) AS weekNumber,
--         sameWeekLastYearStartDate
--     FROM prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static
-- )
-- SELECT
--     cur.fiscalYear AS currentYear,
--     cur.fiscalWeekCode AS currentWeek,
--     cur.weekStartDate AS currentWeekStart,
--     ly.fiscalYear AS priorYear,
--     ly.fiscalWeekCode AS priorYearWeek,
--     ly.weekStartDate AS priorYearWeekStart
-- FROM x cur
-- LEFT JOIN x ly
--   ON ly.weekStartDate=cur.sameWeekLastYearStartDate
-- WHERE cur.fiscalYear=2026
--   AND cur.weekNumber='50';

-- E. GOLD-REQUIRED COMPARISON DATES ARE NON-NULL
-- Expected: 0 for the normal supported calendar range.
-- SELECT COUNT(*) AS invalidRows
-- FROM prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static
-- WHERE priorWeekStartDate IS NULL
--    OR fourWeekAvgStartDate IS NULL
--    OR fourWeekAvgEndDate IS NULL
--    OR sameWeekLastYearStartDate IS NULL;
