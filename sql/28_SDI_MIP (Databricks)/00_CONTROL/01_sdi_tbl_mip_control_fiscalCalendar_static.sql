
-- ###########################################################################
-- BEGIN control/01_sdi_tbl_mip_control_fiscalCalendar_static.sql
-- ###########################################################################

-- ============================================================================
-- FILE  : 01_sdi_tbl_mip_control_fiscalCalendar_static.sql
-- LAYER : CONTROL
-- PURPOSE:
--   Reporting calendar and week-comparison relationships.
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
      date_add(weekStartDate, 3) AS wednesday
    FROM (
      SELECT explode(sequence(DATE'2023-12-31', DATE'2029-12-30', INTERVAL 7 DAYS)) AS weekStartDate
    )
  ),
  labelled AS (
    SELECT
      weekStartDate,
      weekEndDate,
      year(wednesday) AS fiscalYear,
      quarter(wednesday) AS fiscalQuarter,
      row_number() OVER (
        PARTITION BY year(wednesday), quarter(wednesday)
        ORDER BY weekStartDate
      ) AS fiscalWeekOfQuarter
    FROM weeks
  )
  SELECT
    cur.weekStartDate,
    cur.weekEndDate,
    cur.fiscalYear,
    cur.fiscalQuarter,
    concat('Q', cur.fiscalQuarter, ' ', cur.fiscalYear) AS fiscalQuarterLabel,
    cur.fiscalWeekOfQuarter,
    concat('Q', cur.fiscalQuarter, 'W', cur.fiscalWeekOfQuarter) AS fiscalWeekCode,
    concat(
      'W', cur.fiscalWeekOfQuarter, ' · ',
      CASE
        WHEN month(cur.weekStartDate) = month(cur.weekEndDate)
          THEN date_format(cur.weekStartDate, 'd')
        ELSE date_format(cur.weekStartDate, 'd MMM')
      END,
      ' to ', date_format(cur.weekEndDate, 'd MMM')
    ) AS weekLabel,
    concat('Week ending ', date_format(cur.weekEndDate, 'd MMM yyyy')) AS weekEndingLabel,
    date_add(cur.weekStartDate, -7)  AS priorWeekStartDate,
    date_add(cur.weekStartDate, -28) AS fourWeekAvgStartDate,
    date_add(cur.weekStartDate, -7)  AS fourWeekAvgEndDate,
    coalesce(ly.weekStartDate, date_add(cur.weekStartDate, -364)) AS sameWeekLastYearStartDate
  FROM labelled cur
  LEFT JOIN labelled ly
    ON  ly.fiscalYear = cur.fiscalYear - 1
    AND ly.fiscalQuarter = cur.fiscalQuarter
    AND ly.fiscalWeekOfQuarter = cur.fiscalWeekOfQuarter;
END;

-- ----------------------------------------------------------------------------


-- ###########################################################################
-- END control/01_sdi_tbl_mip_control_fiscalCalendar_static.sql
-- ###########################################################################

