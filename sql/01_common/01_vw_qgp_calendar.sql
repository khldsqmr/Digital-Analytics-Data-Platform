/*
===============================================================================
COMMON | QGP CALENDAR VIEW (QA/Debug)
File Name: 01_vw_qgp_calendar.sql
===============================================================================
*/

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_qgp_calendar` AS
WITH d AS (
  SELECT day AS calendar_date
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2020-01-01', DATE '2032-12-31')) AS day
),
mapped AS (
  SELECT
    calendar_date,
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.fn_qgp_week`(calendar_date) AS qgp_week,
    LAST_DAY(calendar_date, QUARTER) AS quarter_end_date,
    DATE_ADD(calendar_date, INTERVAL (7 - EXTRACT(DAYOFWEEK FROM calendar_date)) DAY) AS week_end_saturday
  FROM d
)
SELECT
  calendar_date,
  qgp_week,
  quarter_end_date,
  week_end_saturday,
  (qgp_week = quarter_end_date AND quarter_end_date < week_end_saturday) AS is_quarter_end_partial_day
FROM mapped;