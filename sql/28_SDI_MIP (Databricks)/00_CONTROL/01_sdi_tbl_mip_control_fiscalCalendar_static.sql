-- ###########################################################################
-- BEGIN control/01_sdi_tbl_mip_control_fiscalCalendar_static.sql
-- ###########################################################################

-- ============================================================================
-- FILE  : 01_sdi_tbl_mip_control_fiscalCalendar_static.sql
-- LAYER : CONTROL
-- PURPOSE:
--   Reporting calendar and week-comparison relationships.
--
-- EXAMPLES:
--   fiscalQuarterLabel = 2026 Q2
--   fiscalWeekCode     = 2026Q2W7
-- ============================================================================

USE CATALOG prdrzranalytics;
USE SCHEMA lab42;

-- ----------------------------------------------------------------------------
-- Fiscal calendar table
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sdi_tbl_mip_control_fiscalCalendar_static (
    weekStartDate              DATE,
    weekEndDate                DATE,

    fiscalYear                 INT,
    fiscalQuarter              INT,
    fiscalQuarterLabel         STRING,

    fiscalWeekOfQuarter        INT,
    fiscalWeekCode             STRING,

    weekLabel                  STRING,
    weekEndingLabel            STRING,

    priorWeekStartDate         DATE,

    fourWeekAvgStartDate       DATE,
    fourWeekAvgEndDate         DATE,

    sameWeekLastYearStartDate  DATE
)
USING DELTA
COMMENT 'Control: Sunday-Saturday reporting weeks and comparison-week relationships.';


-- ----------------------------------------------------------------------------
-- Fiscal calendar procedure
-- ----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE sdi_sp_mip_control_fiscalCalendar_static()
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
AS
BEGIN

    WITH weeks AS (
        SELECT
            weekStartDate,
            date_add(weekStartDate, 6) AS weekEndDate,
            date_add(weekStartDate, 3) AS wednesday
        FROM (
            SELECT explode(
                sequence(
                    DATE'2023-12-31',
                    DATE'2029-12-30',
                    INTERVAL 7 DAYS
                )
            ) AS weekStartDate
        )
    ),

    labelled AS (
        SELECT
            weekStartDate,
            weekEndDate,

            year(wednesday) AS fiscalYear,
            quarter(wednesday) AS fiscalQuarter,

            row_number() OVER (
                PARTITION BY
                    year(wednesday),
                    quarter(wednesday)
                ORDER BY weekStartDate
            ) AS fiscalWeekOfQuarter

        FROM weeks
    ),

    finalCalendar AS (
        SELECT
            cur.weekStartDate,
            cur.weekEndDate,

            cur.fiscalYear,
            cur.fiscalQuarter,

            -- Example: 2026 Q2
            concat(
                cast(cur.fiscalYear AS STRING),
                ' Q',
                cast(cur.fiscalQuarter AS STRING)
            ) AS fiscalQuarterLabel,

            cur.fiscalWeekOfQuarter,

            -- Example: 2026Q2W7
            concat(
                cast(cur.fiscalYear AS STRING),
                'Q',
                cast(cur.fiscalQuarter AS STRING),
                'W',
                cast(cur.fiscalWeekOfQuarter AS STRING)
            ) AS fiscalWeekCode,

            -- Example: W7 · 10 to 16 May
            concat(
                'W',
                cast(cur.fiscalWeekOfQuarter AS STRING),
                ' · ',
                CASE
                    WHEN month(cur.weekStartDate) = month(cur.weekEndDate)
                        THEN date_format(cur.weekStartDate, 'd')
                    ELSE date_format(cur.weekStartDate, 'd MMM')
                END,
                ' to ',
                date_format(cur.weekEndDate, 'd MMM')
            ) AS weekLabel,

            -- Example: Week ending 16 May 2026
            concat(
                'Week ending ',
                date_format(cur.weekEndDate, 'd MMM yyyy')
            ) AS weekEndingLabel,

            -- Previous reporting week
            date_add(
                cur.weekStartDate,
                -7
            ) AS priorWeekStartDate,

            -- Previous four complete weeks
            date_add(
                cur.weekStartDate,
                -28
            ) AS fourWeekAvgStartDate,

            date_add(
                cur.weekStartDate,
                -7
            ) AS fourWeekAvgEndDate,

            -- Same fiscal quarter/week position in previous year.
            -- Fall back to exactly 52 weeks earlier if needed.
            coalesce(
                ly.weekStartDate,
                date_add(cur.weekStartDate, -364)
            ) AS sameWeekLastYearStartDate

        FROM labelled cur

        LEFT JOIN labelled ly
            ON  ly.fiscalYear = cur.fiscalYear - 1
            AND ly.fiscalQuarter = cur.fiscalQuarter
            AND ly.fiscalWeekOfQuarter = cur.fiscalWeekOfQuarter
    )

    INSERT OVERWRITE TABLE sdi_tbl_mip_control_fiscalCalendar_static
    SELECT
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

END;

-- ###########################################################################
-- END control/01_sdi_tbl_mip_control_fiscalCalendar_static.sql
-- ###########################################################################