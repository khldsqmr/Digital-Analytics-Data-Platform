SELECT
  validation_run_id,
  data_as_of_date,
  data_source,
  metric_name,
  source_value,
  bronze_value,
  bronze_comparable_value,
  silver_value,
  gold_value,
  source_bronze_variance_pct,
  bronze_silver_variance_pct,
  silver_gold_variance_pct,
  issue_layer,
  status,
  notes,
  next_step

FROM
  prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_validation_history_perRun

ORDER BY
  validation_run_ts DESC,
  data_source,
  metric_name;


-- Useful verification queries
-- After deployment, this is the query I would run first:

SELECT
  data_source,
  metric_name,

  source_value,
  bronze_value,
  bronze_comparable_value,
  silver_value,
  gold_value,

  source_bronze_variance_pct,
  bronze_silver_variance_pct,
  silver_gold_variance_pct,

  comparison_method,
  issue_layer,
  status,

  notes,
  next_step

FROM
  prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_validation_latest_perRun

ORDER BY
  CASE status
    WHEN 'Failed'  THEN 1
    WHEN 'Warning' THEN 2
    ELSE 3
  END,
  data_source,
  metric_name;

-- For only actual problems:

SELECT
  data_source,
  metric_name,

  source_value,
  bronze_value,
  bronze_comparable_value,
  silver_value,
  gold_value,

  issue_layer,
  status,

  notes,
  next_step

FROM
  prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_validation_issues_perRun

WHERE
  validation_run_id = (
    SELECT validation_run_id
    FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_validation_runs_perRun
    WHERE is_latest_run = TRUE
  )

ORDER BY
  status_priority,
  data_source,
  metric_name;

-- For a summary of the latest run and And high-level health:
SELECT
  data_source,
  metric_count,
  healthy_count,
  warning_count,
  failed_count,
  overall_status,
  issue_metrics,
  issue_summary,
  next_step_summary

FROM
  prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_validation_summary_perRun

WHERE
  validation_run_id = (
    SELECT validation_run_id
    FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_validation_runs_perRun
    WHERE is_latest_run = TRUE
  )

ORDER BY
  CASE
    WHEN summary_level = 'OVERALL' THEN 1
    ELSE 2
  END,
  data_source;


  /* =================================================================================================
FILE:     05_pulseTms_fixQuickChecks_adhoc.sql
PURPOSE:  Quick health checks for the PulseTMS fixes (Biddable Programmatic archive, QGP and UPV
          Forecast boundary weeks). Run right after deploying, and any time later (e.g. after a
          weekly run, or in the first week of January after the source rolls over).

          Every query says what it must return. Most return 0 rows when healthy.
          Checks Q1-Q9 need nothing else. Q10 needs the backups from
          00_pulseTms_fixDeploy_verify_adhoc.sql PART 0 (skip it once those are dropped).
================================================================================================= */


/* -------------------------------------------------------------------------------------------------
   Q1. Archive: last run, coverage and freshness
       Expect: runs_in_bronze = 1, silver_rows = bronze_rows, archive_max_date = source_max_date,
               archive_min_date <= source_min_date (after the January rollover the archive keeps
               the prior year, so archive_min_date becomes EARLIER than source_min_date: correct).
   ------------------------------------------------------------------------------------------------- */
SELECT
  (SELECT MAX(archive_loaded_ts)        FROM prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_bronze_retained_weekly)  AS last_archive_run_ts,
  (SELECT COUNT(DISTINCT archive_run_id) FROM prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_bronze_retained_weekly) AS runs_in_bronze,
  (SELECT COUNT(*) FROM prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_bronze_retained_weekly)                      AS bronze_rows,
  (SELECT COUNT(*) FROM prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_silver_conformed_weekly)                     AS silver_rows,
  (SELECT MIN(spend_date) FROM prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_gold_curated_weekly)                  AS archive_min_date,
  (SELECT MAX(spend_date) FROM prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_gold_curated_weekly)                  AS archive_max_date,
  (SELECT MIN(CAST(`date` AS DATE)) FROM prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr)                  AS source_min_date,
  (SELECT MAX(CAST(`date` AS DATE)) FROM prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr)                  AS source_max_date;


/* -------------------------------------------------------------------------------------------------
   Q2. Archive: one copy per date in Silver (expect 0 rows)
   ------------------------------------------------------------------------------------------------- */
SELECT spend_date, COUNT(DISTINCT archive_run_id) AS runs
FROM   prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_silver_conformed_weekly
GROUP BY spend_date
HAVING COUNT(DISTINCT archive_run_id) > 1;


/* -------------------------------------------------------------------------------------------------
   Q3. Archive Gold vs source, last 4 weeks, by date x LOB (expect 0 rows)
       Dates the source still holds must match it exactly.
   ------------------------------------------------------------------------------------------------- */
WITH S AS (
  SELECT CAST(`date` AS DATE) AS d, UPPER(TRIM(lob)) AS lob, SUM(TRY_CAST(spend AS DOUBLE)) AS spend
  FROM   prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr
  WHERE  CAST(`date` AS DATE) >= date_sub(current_date(), 28)
  GROUP BY 1, 2),
G AS (
  SELECT spend_date AS d, lob, SUM(spend) AS spend
  FROM   prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_gold_curated_weekly
  WHERE  spend_date >= date_sub(current_date(), 28)
  GROUP BY 1, 2)
SELECT COALESCE(S.d, G.d) AS d, COALESCE(S.lob, G.lob) AS lob, S.spend AS source_spend, G.spend AS archive_spend
FROM S FULL OUTER JOIN G ON S.d = G.d AND S.lob <=> G.lob
WHERE ABS(COALESCE(S.spend, 0) - COALESCE(G.spend, 0)) > 0.005
ORDER BY 1 DESC, 2;


/* -------------------------------------------------------------------------------------------------
   Q4. PulseTMS Biddable Bronze Programmatic = archive Gold (in-scope LOBs), by week x LOB
       (expect 0 rows)
   ------------------------------------------------------------------------------------------------- */
WITH A AS (
  SELECT date_add(spend_date, 7 - dayofweek(spend_date)) AS week_sun_sat,
         UPPER(TRIM(lob)) AS lob, SUM(COALESCE(spend, 0)) AS spend
  FROM   prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_gold_curated_weekly
  WHERE  UPPER(TRIM(lob)) IN ('POSTPAID', 'CONSUMER POSTPAID', 'HSI', 'BROADBAND')
  GROUP BY 1, 2),
B AS (
  SELECT week_sun_sat, lob, SUM(spend) AS spend
  FROM   prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_biddableSpend_weekly
  WHERE  channel_group = 'Programmatic'
  GROUP BY 1, 2)
SELECT COALESCE(A.week_sun_sat, B.week_sun_sat) AS week_sun_sat, COALESCE(A.lob, B.lob) AS lob,
       A.spend AS archive_spend, B.spend AS bronze_spend
FROM A FULL OUTER JOIN B ON A.week_sun_sat = B.week_sun_sat AND A.lob = B.lob
WHERE ABS(COALESCE(A.spend, 0) - COALESCE(B.spend, 0)) > 0.005
ORDER BY 1 DESC, 2;


/* -------------------------------------------------------------------------------------------------
   Q5. QGP Digital %: every full week is a valid percentage that sits between its two pieces
       (expect 0 rows; the old bug produced values like 0.96 = two pieces added)
   ------------------------------------------------------------------------------------------------- */
SELECT f.metric_name, f.metric_type, f.qgp_date,
       s.metric_value AS stub_piece, f.metric_value AS first_piece, f.wow_numerator AS full_week
FROM   prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly f
JOIN   prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar c
  ON   c.qgp_date = f.qgp_date
LEFT JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly s
  ON   s.qgp_date = c.boundary_stub_date AND s.metric_name = f.metric_name AND s.metric_type = f.metric_type
WHERE  f.metric_name LIKE 'digitalPct%'
  AND  f.week_type = 'BOUNDARY_FIRST'
  AND  f.wow_numerator IS NOT NULL
  AND  (   f.wow_numerator > 1
        OR f.wow_numerator < LEAST(s.metric_value, f.metric_value) - 1e-9
        OR f.wow_numerator > GREATEST(s.metric_value, f.metric_value) + 1e-9);


/* -------------------------------------------------------------------------------------------------
   Q6. QGP counts: boundary full week = stub + first when both exist, blank when one is missing
       (expect 0 rows)
   ------------------------------------------------------------------------------------------------- */
SELECT f.metric_name, f.metric_type, f.qgp_date,
       s.metric_value AS stub_piece, f.metric_value AS first_piece, f.wow_numerator AS full_week
FROM   prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly f
JOIN   prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar c
  ON   c.qgp_date = f.qgp_date
LEFT JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly s
  ON   s.qgp_date = c.boundary_stub_date AND s.metric_name = f.metric_name AND s.metric_type = f.metric_type
WHERE  f.metric_name NOT LIKE 'digitalPct%'
  AND  f.week_type = 'BOUNDARY_FIRST'
  AND  NOT (f.wow_numerator <=> (s.metric_value + f.metric_value));


/* -------------------------------------------------------------------------------------------------
   Q7. UPV Forecast: boundary full week follows the rule (expect 0 rows)
       both pieces -> stub + first; one missing -> scaled from the other piece; both missing -> blank
   ------------------------------------------------------------------------------------------------- */
SELECT f.metric_name, f.channel_group, f.qgp_date,
       s.metric_value AS stub_piece, f.metric_value AS first_piece, f.days_in_period AS first_days,
       f.wow_numerator AS full_week
FROM   prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly f
JOIN   prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar c
  ON   c.qgp_date = f.qgp_date
LEFT JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly s
  ON   s.qgp_date = c.boundary_stub_date AND s.channel_group = f.channel_group AND s.metric_name = f.metric_name
WHERE  f.week_type = 'BOUNDARY_FIRST'
  AND  NOT (ROUND(f.wow_numerator, 2) <=> ROUND(
         CASE
           WHEN f.metric_value IS NOT NULL AND s.metric_value IS NOT NULL THEN f.metric_value + s.metric_value
           WHEN f.metric_value IS NOT NULL THEN f.metric_value * 7.0 / f.days_in_period
           WHEN s.metric_value IS NOT NULL THEN s.metric_value * 7.0 / (7 - f.days_in_period)
         END, 2));


/* -------------------------------------------------------------------------------------------------
   Q8. No zero denominators left in the two fixed Silvers (expect 0 rows)
       A missing prior week must now be blank, never 0.
   ------------------------------------------------------------------------------------------------- */
SELECT 'QGP' AS silver, metric_name, metric_type AS detail, qgp_date
FROM   prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly
WHERE  wow_denominator = 0
UNION ALL
SELECT 'UPV_FORECAST', metric_name, channel_group, qgp_date
FROM   prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly
WHERE  wow_denominator = 0;


/* -------------------------------------------------------------------------------------------------
   Q9. The two known weeks, end to end in Gold long (eyeball)
       Expect: Digital % (Total incl. assisted) 2026-07-04 full week 0.481875 actual / 0.434495
               target, WoW about +1.2%; 2026-07-11 WoW about -2.9%.
               UPV forecast 2026-07-04 full week 1,533,000; 2026-07-11 WoW about -4.8%.
   ------------------------------------------------------------------------------------------------- */
SELECT data_source, metric_name, metric_type, qgp_date, week_type, metric_value,
       ROUND(wow_numerator, 6) AS full_week, ROUND(wow_denominator, 6) AS prior_full_week,
       ROUND(wow_pct, 4) AS wow_pct
FROM   prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long
WHERE  qgp_date IN (DATE'2026-07-04', DATE'2026-07-11')
  AND  (   (data_source = 'QGP_SCORECARD' AND metric_name = 'digitalPctConsumerPostpaidActivationsTotalInclAssisted')
        OR (data_source = 'UPV_FORECAST'  AND metric_name = 'upvForecast' AND channel_group = 'All Channels'))
ORDER BY data_source, metric_type, qgp_date;


/* -------------------------------------------------------------------------------------------------
   Q10. Nothing else moved: Gold long row counts and metric_value totals per data_source vs the
        PART 0 backup (expect same_rows = TRUE and value_diff = 0 for every data_source, apart from
        the latest week or two if sources loaded new data in between)
   ------------------------------------------------------------------------------------------------- */
WITH N AS (
  SELECT data_source, COUNT(*) AS rows_after, ROUND(SUM(metric_value), 2) AS total_after
  FROM   prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long
  WHERE  qgp_date <= date_sub(current_date(), 21)
  GROUP BY data_source),
O AS (
  SELECT data_source, COUNT(*) AS rows_before, ROUND(SUM(metric_value), 2) AS total_before
  FROM   prdrzranalytics.lab42.tmp_pulseFix_before_goldLong
  WHERE  qgp_date <= date_sub(current_date(), 21)
  GROUP BY data_source)
SELECT COALESCE(N.data_source, O.data_source) AS data_source,
       O.rows_before, N.rows_after, (O.rows_before <=> N.rows_after) AS same_rows,
       O.total_before, N.total_after, ROUND(COALESCE(N.total_after, 0) - COALESCE(O.total_before, 0), 2) AS value_diff
FROM N FULL OUTER JOIN O ON N.data_source = O.data_source
ORDER BY 1;


/* -------------------------------------------------------------------------------------------------
   Q11. Latest validation run (expect overall status no worse than before the change)
   ------------------------------------------------------------------------------------------------- */
SELECT run_label, overall_status, healthy_count, warning_count, failed_count, issue_metrics
FROM   prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_validation_runs_perRun
ORDER BY validation_run_ts DESC
LIMIT 3;