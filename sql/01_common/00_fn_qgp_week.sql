/*
===============================================================================
COMMON | QGP WEEK FUNCTION
File Name: 00_fn_qgp_week.sql
===============================================================================
DEFINITION:
  qgp_week is the period end date:
    - Normally: week ending Saturday (NEXT Saturday on/after event_date)
    - If quarter_end occurs BEFORE that Saturday:
        dates in the tail map to quarter_end (partial)
===============================================================================
*/

CREATE OR REPLACE FUNCTION
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.fn_qgp_week`(event_date DATE)
AS (
  (
    WITH x AS (
      SELECT
        event_date AS d,
        -- DAYOFWEEK: 1=Sunday ... 7=Saturday
        DATE_ADD(event_date, INTERVAL (7 - EXTRACT(DAYOFWEEK FROM event_date)) DAY) AS week_end_saturday,
        LAST_DAY(event_date, QUARTER) AS quarter_end
    )
    SELECT
      CASE
        WHEN quarter_end < week_end_saturday THEN quarter_end
        ELSE week_end_saturday
      END
    FROM x
  )
);