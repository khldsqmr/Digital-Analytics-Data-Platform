-- ###########################################################################
-- BEGIN control/01_sdi_tbl_mip_control_fiscalCalendar_static.sql
-- ###########################################################################

-- ============================================================================
-- FILE  : 01_sdi_tbl_mip_control_fiscalCalendar_static.sql
-- LAYER : CONTROL
-- PURPOSE:
--   Reporting calendar and week-comparison relationships.
--
-- OUTPUT EXAMPLES:
--   fiscalQuarterLabel = 2026 Q2
--   fiscalWeekCode     = 2026Q2W7
--   weekLabel          = W7 · 10 to 16 May
--   weekEndingLabel    = Week ending 16 May 2026
--
-- WEEK DEFINITION:
--   Sunday through Saturday.
--
-- COMPARISON DEFINITIONS:
--   priorWeekStartDate        = immediately preceding reporting week
--   fourWeekAvgStartDate      = four weeks before current week
--   fourWeekAvgEndDate        = immediately preceding reporting week
--   sameWeekLastYearStartDate = same quarter/week position in prior year
-- ============================================================================
 
USE CATALOG prdrzranalytics;
USE SCHEMA lab42;

-- ----------------------------------------------------------------------------
-- Fiscal calendar
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
-- Procedure
-- ----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE sdi_sp_mip_control_fiscalCalendar_static()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN

  INSERT OVERWRITE sdi_tbl_mip_control_fiscalCalendar_static

  WITH weeks AS (
    SELECT
      weekStartDate,
      date_add(weekStartDate, 6) AS weekEndDate,

      -- Wednesday is used as the anchor day when assigning the
      -- Sunday-Saturday reporting week to a calendar quarter.
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

      year(wednesday)    AS fiscalYear,
      quarter(wednesday) AS fiscalQuarter,

      row_number() OVER (
        PARTITION BY
          year(wednesday),
          quarter(wednesday)
        ORDER BY weekStartDate
      ) AS fiscalWeekOfQuarter

    FROM weeks
  )

  SELECT
    cur.weekStartDate,
    cur.weekEndDate,

    cur.fiscalYear,
    cur.fiscalQuarter,

    -- Example: 2026 Q2
    concat(
      cur.fiscalYear,
      ' Q',
      cur.fiscalQuarter
    ) AS fiscalQuarterLabel,

    cur.fiscalWeekOfQuarter,

    -- Example: 2026Q2W7
    concat(
      cur.fiscalYear,
      'Q',
      cur.fiscalQuarter,
      'W',
      cur.fiscalWeekOfQuarter
    ) AS fiscalWeekCode,

    -- Example:
    --   W7 · 10 to 16 May
    --   W7 · 28 Jun to 4 Jul
    concat(
      'W',
      cur.fiscalWeekOfQuarter,
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

    -- Prior four complete reporting weeks
    date_add(
      cur.weekStartDate,
      -28
    ) AS fourWeekAvgStartDate,

    date_add(
      cur.weekStartDate,
      -7
    ) AS fourWeekAvgEndDate,

    -- Prefer the same quarter/week position from the prior year.
    -- Fall back to 52 weeks earlier if a direct quarter/week match
    -- is unavailable.
    coalesce(
      ly.weekStartDate,
      date_add(cur.weekStartDate, -364)
    ) AS sameWeekLastYearStartDate

  FROM labelled cur

  LEFT JOIN labelled ly
    ON  ly.fiscalYear          = cur.fiscalYear - 1
    AND ly.fiscalQuarter       = cur.fiscalQuarter
    AND ly.fiscalWeekOfQuarter = cur.fiscalWeekOfQuarter;

END;

-- ----------------------------------------------------------------------------
-- One-time / definition-refresh execution
-- ----------------------------------------------------------------------------
-- CALL sdi_sp_mip_control_fiscalCalendar_static();

-- ###########################################################################
-- END control/01_sdi_tbl_mip_control_fiscalCalendar_static.sql
-- ###########################################################################