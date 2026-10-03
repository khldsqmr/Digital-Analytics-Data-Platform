-- ============================================================================
-- FILE  : 01_sdi_vw_mip_control_fiscalCalendar_static.sql
-- LAYER : CONTROL
-- PURPOSE:
--   Reporting calendar and week-comparison relationships.
-- ============================================================================
CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static
COMMENT 'Control view: Sunday-Saturday reporting weeks and comparison-week relationships.'
AS
WITH weeks AS (
    SELECT weekStartDate,date_add(weekStartDate,6) AS weekEndDate,date_add(weekStartDate,3) AS wednesday
    FROM (
        SELECT explode(sequence(DATE '2023-12-31',DATE '2029-12-30',INTERVAL 7 DAYS)) AS weekStartDate
    )
),
labelled AS (
    SELECT
        weekStartDate,
        weekEndDate,
        year(wednesday) AS fiscalYear,
        quarter(wednesday) AS fiscalQuarter,
        CAST(row_number() OVER (
            PARTITION BY year(wednesday),quarter(wednesday)
            ORDER BY weekStartDate
        ) AS INT) AS fiscalWeekOfQuarter
    FROM weeks
),
finalCalendar AS (
    SELECT
        cur.weekStartDate,
        cur.weekEndDate,
        cur.fiscalYear,
        cur.fiscalQuarter,
        concat(CAST(cur.fiscalYear AS STRING),' Q',CAST(cur.fiscalQuarter AS STRING)) AS fiscalQuarterLabel,
        cur.fiscalWeekOfQuarter,
        concat(CAST(cur.fiscalYear AS STRING),'Q',CAST(cur.fiscalQuarter AS STRING),'W',CAST(cur.fiscalWeekOfQuarter AS STRING)) AS fiscalWeekCode,
        concat(
            'W',CAST(cur.fiscalWeekOfQuarter AS STRING),' · ',
            CASE WHEN month(cur.weekStartDate)=month(cur.weekEndDate)
                 THEN date_format(cur.weekStartDate,'d')
                 ELSE date_format(cur.weekStartDate,'d MMM') END,
            ' to ',date_format(cur.weekEndDate,'d MMM')
        ) AS weekLabel,
        concat('Week ending ',date_format(cur.weekEndDate,'d MMM yyyy')) AS weekEndingLabel,
        date_add(cur.weekStartDate,-7) AS priorWeekStartDate,
        date_add(cur.weekStartDate,-28) AS fourWeekAvgStartDate,
        date_add(cur.weekStartDate,-7) AS fourWeekAvgEndDate,
        coalesce(ly.weekStartDate,date_add(cur.weekStartDate,-364)) AS sameWeekLastYearStartDate
    FROM labelled cur
    LEFT JOIN labelled ly
      ON ly.fiscalYear=cur.fiscalYear-1
     AND ly.fiscalQuarter=cur.fiscalQuarter
     AND ly.fiscalWeekOfQuarter=cur.fiscalWeekOfQuarter
)
SELECT
    weekStartDate,weekEndDate,fiscalYear,fiscalQuarter,fiscalQuarterLabel,
    fiscalWeekOfQuarter,fiscalWeekCode,weekLabel,weekEndingLabel,
    priorWeekStartDate,fourWeekAvgStartDate,fourWeekAvgEndDate,sameWeekLastYearStartDate
FROM finalCalendar;
