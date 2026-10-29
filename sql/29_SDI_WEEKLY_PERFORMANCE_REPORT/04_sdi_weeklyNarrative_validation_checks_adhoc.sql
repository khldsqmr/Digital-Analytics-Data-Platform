/* =================================================================================================
FILE:     04_sdi_weeklyNarrative_validation_checks_adhoc.sql
PURPOSE:  Run these after deploying files 01-03. Every check except 4 and 5 should return 0 rows.
================================================================================================= */


/* -------------------------------------------------------------------------------------------------
   1. Registry: duplicate metric_key (a view cannot enforce a primary key)
   ------------------------------------------------------------------------------------------------- */
SELECT metric_key, COUNT(*) AS n
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_control_metricRegistry_static
GROUP BY metric_key
HAVING COUNT(*) > 1;


/* -------------------------------------------------------------------------------------------------
   2. Registry: target_metric_key points at a key that does not exist
   ------------------------------------------------------------------------------------------------- */
SELECT r.metric_key, r.target_metric_key
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_control_metricRegistry_static r
LEFT JOIN prdrzranalytics.lab42.sdi_vw_weeklyNarrative_control_metricRegistry_static t
  ON t.metric_key = r.target_metric_key
WHERE r.target_metric_key IS NOT NULL
  AND t.metric_key IS NULL;


/* -------------------------------------------------------------------------------------------------
   3. Registry vs Gold: active slices that match NOTHING in Gold (typo in name / type / lob)
      Checked across all weeks, so a slice that is simply empty this week is not flagged.
   ------------------------------------------------------------------------------------------------- */
SELECT r.metric_key, r.data_source, r.metric_name, r.metric_type, r.source_lob, r.channel_group
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_control_metricRegistry_static r
WHERE  r.is_active = TRUE
  AND NOT EXISTS (
    SELECT 1
    FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long g
    WHERE g.data_source   =   r.data_source
      AND g.metric_name   =   r.metric_name
      AND g.metric_type   <=> r.metric_type
      AND ((r.source_lob IS NULL AND g.true_lob IS NULL) OR array_contains(r.source_lob, g.true_lob))
      AND g.channel_group <=> r.channel_group
  );


/* -------------------------------------------------------------------------------------------------
   4. Take a snapshot (latest completed Saturday), then a backfill example
   ------------------------------------------------------------------------------------------------- */
CALL prdrzranalytics.lab42.sdi_sp_weeklyNarrative_gold_factsHistory_long(NULL, 'MANUAL', NULL);

-- CALL prdrzranalytics.lab42.sdi_sp_weeklyNarrative_gold_factsHistory_long(DATE'2026-10-24', 'MANUAL', NULL);


/* -------------------------------------------------------------------------------------------------
   5. Look at what the AI will see: headline rows for the latest week
   ------------------------------------------------------------------------------------------------- */
SELECT
  week_end, report_section, metric_lob, display_name, unit,
  week_value, wow_pct, yoy_pct,
  target_week_value, vs_target_abs, vs_target_pct, is_target_same_as_actual,
  avg_prior_4wk, avg_prior_13wk, avg_prior_52wk, zscore_prior_13wk, prior_52wk_weeks,
  mtd_avg_weekly, qtd_avg_weekly, ytd_avg_weekly,
  qtd_total, qtd_target_total, qtd_vs_target,
  is_data_current, source_validation_status
FROM prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_factsLatest_long
WHERE row_role = 'ACTUAL'
  AND week_end = (SELECT MAX(week_end) FROM prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_factsLatest_long)
ORDER BY metric_lob, sort_order;


/* -------------------------------------------------------------------------------------------------
   6. Registry slices missing from the latest snapshot (expected on weeks where a source lags,
      e.g. QGP actuals reporting lag; investigate anything else)
   ------------------------------------------------------------------------------------------------- */
SELECT r.metric_key, r.display_name, r.metric_role
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_control_metricRegistry_static r
WHERE  r.is_active = TRUE
  AND NOT EXISTS (
    SELECT 1
    FROM prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_factsLatest_long f
    WHERE f.metric_key = r.metric_key
      AND f.row_role IN ('ACTUAL', 'TARGET')
      AND f.week_end = (SELECT MAX(week_end) FROM prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_factsLatest_long)
  );


/* -------------------------------------------------------------------------------------------------
   7. Reconcile with the PulseTMS wide sense-check view (should differ only on BOUNDARY_FIRST
      weeks, where the snapshot uses the full natural week)
   ------------------------------------------------------------------------------------------------- */
SELECT
  f.week_end,
  f.week_type,
  f.week_value       AS snapshot_upv_total,
  w.upvTotalAdobe    AS wide_upv_total,
  f.week_value - w.upvTotalAdobe AS diff
FROM prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_factsLatest_long f
JOIN prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_wide w
  ON  w.qgp_date      = f.week_end
  AND w.channel_group = 'All Channels'
WHERE f.metric_key = 'pulseTms.upvTotalAdobe'
  AND f.row_role   = 'ACTUAL'
  AND ABS(COALESCE(f.week_value - w.upvTotalAdobe, 0)) > 0.01;


/* -------------------------------------------------------------------------------------------------
   8. Lineage: active registry rows pointing at an upstream the snapshot procedure does not read
      (only PulseTMS Gold is wired today; any row here needs a new adapter before it will snapshot)
   ------------------------------------------------------------------------------------------------- */
SELECT metric_key, source_project, source_object
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_control_metricRegistry_static
WHERE  is_active = TRUE
  AND  NOT (source_project = 'PULSE_TMS'
            AND source_object = 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long');


/* -------------------------------------------------------------------------------------------------
   9. LOB: metric_lob with an unknown LOB code, or several LOBs not in the fixed order
      (POSTPAID, BROADBAND, BYOD, FIBER, TFB, METRO). NULL is allowed (metric has no LOB).
   ------------------------------------------------------------------------------------------------- */
WITH Lobs AS (
  SELECT metric_key, metric_lob,
         transform(split(metric_lob, ' \\+ '),
                   x -> array_position(array('POSTPAID', 'BROADBAND', 'BYOD', 'FIBER', 'TFB', 'METRO'), x)) AS pos
  FROM prdrzranalytics.lab42.sdi_vw_weeklyNarrative_control_metricRegistry_static
  WHERE metric_lob IS NOT NULL
)
SELECT metric_key, metric_lob
FROM   Lobs
WHERE  array_contains(pos, 0)
   OR  pos <> array_sort(pos)
   OR  size(pos) <> size(array_distinct(pos));


/* -------------------------------------------------------------------------------------------------
   10. LOB: source_lob that sums several LOBs on a non-additive unit (PCT), or an empty array
       (use NULL, not [], for a source with no LOB)
   ------------------------------------------------------------------------------------------------- */
SELECT metric_key, unit, source_lob
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_control_metricRegistry_static
WHERE  size(source_lob) = 0
   OR  (size(source_lob) > 1 AND unit = 'PCT');


/* -------------------------------------------------------------------------------------------------
   11. LOB: source_lob values that never appear in PulseTMS Gold for that metric (typo or
       a LOB PulseTMS does not carry yet, e.g. METRO)
   ------------------------------------------------------------------------------------------------- */
SELECT r.metric_key, m.member_lob
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_control_metricRegistry_static r
LATERAL VIEW explode(r.source_lob) m AS member_lob
WHERE  r.is_active = TRUE
  AND  NOT EXISTS (
    SELECT 1
    FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long g
    WHERE g.data_source = r.data_source
      AND g.metric_name = r.metric_name
      AND g.metric_type <=> r.metric_type
      AND g.true_lob    = m.member_lob
  );


/* -------------------------------------------------------------------------------------------------
   12. LOB: partial summed totals in the latest snapshot (one of the source_lob values missing this week)
   ------------------------------------------------------------------------------------------------- */
SELECT week_end, metric_key, channel_group, source_lob
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_factsLatest_long
WHERE  is_lob_rollup_complete = FALSE;


/* -------------------------------------------------------------------------------------------------
   13. LOB: MFC Postpaid + Broadband rollup must equal the PulseTMS wide view total
   ------------------------------------------------------------------------------------------------- */
SELECT f.week_end, f.channel_group, f.metric_value AS snapshot_total, w.mfcSpendActualTotal AS wide_total
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_factsLatest_long f
JOIN   prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_wide w
  ON   w.qgp_date      = f.week_end
 AND   w.channel_group = f.channel_group
WHERE  f.metric_key = 'pulseTms.mfcSpendActualTotal'
  AND  ABS(COALESCE(f.metric_value, 0) - COALESCE(w.mfcSpendActualTotal, 0)) > 0.01;


/* -------------------------------------------------------------------------------------------------
   14. Target: weeks where target equals actual (PulseTMS MFC forecast fallback). Expected
       for weeks with no MFC forecast; the narrative must say "no target available".
   ------------------------------------------------------------------------------------------------- */
SELECT week_end, metric_key, channel_group, week_value, target_week_value
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_factsLatest_long
WHERE  is_target_same_as_actual = TRUE
  AND  row_role = 'ACTUAL'
ORDER BY week_end DESC, metric_key;


/* -------------------------------------------------------------------------------------------------
   15. Naming convention: codes must be UPPER_SNAKE, labels must not be empty
   ------------------------------------------------------------------------------------------------- */
SELECT metric_key, metric_lob, metric_role, unit, source_project, report_section
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_control_metricRegistry_static
WHERE  NOT COALESCE(metric_lob, 'NULL') RLIKE '^[A-Z][A-Z0-9_]*( \\+ [A-Z][A-Z0-9_]*)*$'
   OR  metric_role IS NULL OR metric_role NOT IN ('ACTUAL', 'TARGET')
   OR  NOT unit           RLIKE '^[A-Z][A-Z0-9_]*$'
   OR  NOT source_project RLIKE '^[A-Z][A-Z0-9_]*$'
   OR  NULLIF(TRIM(report_section), '')   IS NULL;


/* -------------------------------------------------------------------------------------------------
   16. Backfill coverage: Saturdays in the PulseTMS calendar with no snapshot (after running 05).
       Expect rows only before p_start_week_end or for weeks PulseTMS has no data.
   ------------------------------------------------------------------------------------------------- */
SELECT cal.qgp_date AS missing_week_end
FROM   prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar cal
WHERE  DAYOFWEEK(cal.qgp_date) = 7
  AND  cal.is_complete_period = TRUE
  AND  cal.qgp_date >= (SELECT MIN(week_end) FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_factsHistory_long)
  AND  NOT EXISTS (
    SELECT 1 FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_factsHistory_long h
    WHERE h.week_end = cal.qgp_date
  )
ORDER BY cal.qgp_date;


/* -------------------------------------------------------------------------------------------------
   17. What moved: one row per headline metric per week (should return 0 rows)
   ------------------------------------------------------------------------------------------------- */
SELECT week_end, metric_key, COUNT(*) AS n
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_whatMoved_long
GROUP BY week_end, metric_key
HAVING COUNT(*) > 1;


/* -------------------------------------------------------------------------------------------------
   18. What moved: highlights for the latest week, in rank order (eyeball against the report)
   ------------------------------------------------------------------------------------------------- */
SELECT
  highlight_rank, report_section, metric_lob, display_name,
  highlight_reasons, ROUND(highlight_score, 2) AS score,
  direction, wow_sentiment, target_position, target_sentiment,
  top_driver_channel, ROUND(top_driver_share_of_change, 2) AS driver_share,
  source_validation_status
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_whatMoved_long
WHERE  week_end = (SELECT MAX(week_end) FROM prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_whatMoved_long)
  AND  is_highlight = TRUE
ORDER BY highlight_rank;


/* -------------------------------------------------------------------------------------------------
   19. What moved: rows blocked by the data quality gate in the latest week
   ------------------------------------------------------------------------------------------------- */
SELECT display_name, metric_lob, data_quality_issue, highlight_reasons
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_whatMoved_long
WHERE  week_end = (SELECT MAX(week_end) FROM prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_whatMoved_long)
  AND  data_quality_issue IS NOT NULL
ORDER BY sort_order;