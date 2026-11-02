/* =================================================================================================
FILE:     00_pulseTms_fixDeploy_verify_adhoc.sql
PURPOSE:  Step-by-step deployment and verification for the PulseTMS fixes:

            FIX 3  Biddable Programmatic archive (new 3-layer medallion, same pattern as the QGP
                   archive) + Biddable Bronze / validation read its Gold
            FIX 1  QGP Silver: Digital % boundary weeks = day-weighted average; missing piece = NULL
            FIX 2  UPV Forecast Silver: missing boundary piece estimated from the other piece's
                   daily rate (full week only); prior week NULL instead of 0

          NO column is added, removed, renamed or reordered in any existing table or view.
          metric_value is NOT changed for any existing row.

HOW TO USE:
  Run one PART at a time, top to bottom, in a Databricks SQL editor / notebook.
  Every CHECK says what it must return. If a CHECK fails, stop and use PART 9 (rollback).

OBJECTS DEPLOYED (files in this folder):
  01_sdi_sp_biddProgrammaticArchive_bronze_snapshot_weekly.sql          NEW  -> ..._bronze_retained_weekly
  02_sdi_sp_biddProgrammaticArchive_silver_conformed_weekly.sql          NEW  -> ..._silver_conformed_weekly
  03_sdi_sp_biddProgrammaticArchive_gold_curated_weekly.sql              NEW  -> ..._gold_curated_weekly
  04_sdi_sp_biddProgrammaticArchive_orchestration_allLayers_weekly.sql   NEW  (Bronze -> Silver -> Gold)
  sdi_sp_dashboardPulseTms_bronze_biddableSpend_weekly.sql          UPDATED (Programmatic source)
  sdi_sp_dashboardPulseTms_silver_qgp_weekly.sql                    UPDATED (WoW / YoY / LY logic)
  sdi_sp_dashboardPulseTms_silver_upvForecast_weekly.sql            UPDATED (WoW / YoY / LY logic)
  sdi_sp_dashboardPulseTms_validation_history_perRun.sql            UPDATED (Programmatic source)
  sdi_sp_dashboardPulseTms_orchestration_allLayers_weekly.sql       UPDATED (calls the archive)
  Keep the ORIGINAL versions of the five updated files: re-deploying them is the procedure rollback.

================================================================================================= */


/* =================================================================================================
   PART 0 - BEFORE ANYTHING: schema snapshot + backups + Delta versions
   ================================================================================================= */

-- 0.1 Column snapshot of every existing object these fixes touch or feed
CREATE OR REPLACE TABLE prdrzranalytics.lab42.tmp_pulseFix_schema_before AS
SELECT table_name, ordinal_position, column_name, data_type
FROM   prdrzranalytics.information_schema.columns
WHERE  table_schema = 'lab42'
  AND  table_name IN (
         'sdi_tbl_dashboardpulsetms_bronze_biddablespend_weekly',
         'sdi_tbl_dashboardpulsetms_silver_biddablespend_weekly',
         'sdi_tbl_dashboardpulsetms_silver_qgp_weekly',
         'sdi_tbl_dashboardpulsetms_silver_upvforecast_weekly',
         'sdi_vw_dashboardpulsetms_gold_unified_long',
         'sdi_vw_dashboardpulsetms_gold_unified_wide',
         'sdi_tbl_dashboardpulsetms_validation_history_perrun');

-- 0.2 Backups (frozen copies used for every comparison below)
CREATE OR REPLACE TABLE prdrzranalytics.lab42.tmp_pulseFix_before_bronzeBiddable AS
SELECT * FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_biddableSpend_weekly;

CREATE OR REPLACE TABLE prdrzranalytics.lab42.tmp_pulseFix_before_silverBiddable AS
SELECT * FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_biddableSpend_weekly;

CREATE OR REPLACE TABLE prdrzranalytics.lab42.tmp_pulseFix_before_silverQgp AS
SELECT * FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly;

CREATE OR REPLACE TABLE prdrzranalytics.lab42.tmp_pulseFix_before_silverUpvForecast AS
SELECT * FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly;

CREATE OR REPLACE TABLE prdrzranalytics.lab42.tmp_pulseFix_before_goldLong AS
SELECT * FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long;

-- 0.3 Write down the current Delta version of each table (needed only for a rollback, PART 9)
DESCRIBE HISTORY prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_biddableSpend_weekly LIMIT 1;
DESCRIBE HISTORY prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_biddableSpend_weekly LIMIT 1;
DESCRIBE HISTORY prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly          LIMIT 1;
DESCRIBE HISTORY prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly  LIMIT 1;


/* =================================================================================================
   PART 1 - FIX 3a: create and load the Biddable Programmatic archive
            (new objects only; nothing existing changes)
   ================================================================================================= */

-- 1.1 Deploy files 01, 02, 03 and 04 (the four biddProgrammaticArchive procedures), then:
CALL prdrzranalytics.lab42.sdi_sp_biddProgrammaticArchive_orchestration_allLayers_weekly();

-- CHECK 1.2: Bronze and Silver hold exactly the source
--            (expect bronze_rows_match = TRUE, silver_rows_match = TRUE, dates_match = TRUE, runs = 1)
SELECT
  (SELECT COUNT(*) FROM prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr WHERE CAST(`date` AS DATE) IS NOT NULL)
    = (SELECT COUNT(*) FROM prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_bronze_retained_weekly)   AS bronze_rows_match,
  (SELECT COUNT(*) FROM prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr WHERE CAST(`date` AS DATE) IS NOT NULL)
    = (SELECT COUNT(*) FROM prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_silver_conformed_weekly)  AS silver_rows_match,
  (SELECT COUNT(DISTINCT CAST(`date` AS DATE)) FROM prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr WHERE CAST(`date` AS DATE) IS NOT NULL)
    = (SELECT COUNT(DISTINCT spend_date) FROM prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_gold_curated_weekly) AS dates_match,
  (SELECT COUNT(DISTINCT archive_run_id) FROM prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_bronze_retained_weekly)     AS runs;

-- CHECK 1.3: Gold spend per date x LOB x DSP identical to the source (expect 0 rows)
WITH S AS (
  SELECT CAST(`date` AS DATE) AS d, UPPER(TRIM(lob)) AS lob, TRIM(DSP) AS dsp, SUM(TRY_CAST(spend AS DOUBLE)) AS spend
  FROM prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr
  WHERE CAST(`date` AS DATE) IS NOT NULL
  GROUP BY 1, 2, 3)
SELECT COALESCE(S.d, G.spend_date) AS d, COALESCE(S.lob, G.lob) AS lob, COALESCE(S.dsp, G.dsp) AS dsp,
       S.spend AS source_spend, G.spend AS gold_spend
FROM S
FULL OUTER JOIN prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_gold_curated_weekly G
  ON S.d = G.spend_date AND S.lob <=> G.lob AND S.dsp <=> G.dsp
WHERE ABS(COALESCE(S.spend, 0) - COALESCE(G.spend, 0)) > 0.005;

-- CHECK 1.4: re-run and confirm it still holds ONE copy per date
--            (expect runs = 1 and the same archive_rows as before the re-run)
CALL prdrzranalytics.lab42.sdi_sp_biddProgrammaticArchive_orchestration_allLayers_weekly();
SELECT COUNT(DISTINCT archive_run_id) AS runs,
       COUNT(*)                       AS archive_rows
FROM prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_bronze_retained_weekly;


/* =================================================================================================
   PART 2 - FIX 3b: Biddable Bronze reads the archive (output must be IDENTICAL)
   ================================================================================================= */

-- 2.1 Deploy sdi_sp_dashboardPulseTms_bronze_biddableSpend_weekly.sql (updated), then:
CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_bronze_biddableSpend_weekly();
CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_biddableSpend_weekly();

-- CHECK 2.2: Bronze identical to the backup (expect 0 rows)
SELECT COALESCE(n.week_sun_sat, o.week_sun_sat) AS week_sun_sat, COALESCE(n.lob, o.lob) AS lob,
       COALESCE(n.channel_group, o.channel_group) AS channel_group, COALESCE(n.platform, o.platform) AS platform,
       o.spend AS before_spend, n.spend AS after_spend
FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_biddableSpend_weekly n
FULL OUTER JOIN prdrzranalytics.lab42.tmp_pulseFix_before_bronzeBiddable o
  ON  n.week_sun_sat = o.week_sun_sat AND n.lob <=> o.lob
  AND n.channel_group <=> o.channel_group AND n.platform <=> o.platform
WHERE NOT (ROUND(n.spend, 2) <=> ROUND(o.spend, 2));

-- CHECK 2.3: Silver identical to the backup (expect 0 rows)
SELECT COALESCE(n.qgp_date, o.qgp_date) AS qgp_date, COALESCE(n.lob, o.lob) AS lob,
       COALESCE(n.channel_group, o.channel_group) AS channel_group
FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_biddableSpend_weekly n
FULL OUTER JOIN prdrzranalytics.lab42.tmp_pulseFix_before_silverBiddable o
  ON  n.qgp_date = o.qgp_date AND n.lob <=> o.lob AND n.channel_group <=> o.channel_group
  AND n.metric_name <=> o.metric_name
WHERE NOT (ROUND(n.metric_value, 2)    <=> ROUND(o.metric_value, 2))
   OR NOT (ROUND(n.wow_numerator, 2)   <=> ROUND(o.wow_numerator, 2))
   OR NOT (ROUND(n.wow_denominator, 2) <=> ROUND(o.wow_denominator, 2))
   OR NOT (ROUND(n.yoy_denominator, 2) <=> ROUND(o.yoy_denominator, 2));
-- Note: the source keeps loading during the day. If 2.2 / 2.3 show rows ONLY for the latest
-- one or two weeks, that is new spend that landed between the backup and this run, not the fix.


/* =================================================================================================
   PART 3 - FIX 1: QGP Silver
   ================================================================================================= */

-- 3.1 Deploy sdi_sp_dashboardPulseTms_silver_qgp_weekly.sql (updated), then:
CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_qgp_weekly();

-- CHECK 3.2: row count unchanged and metric_value unchanged on EVERY row
--            (expect rows_before = rows_after, metric_value_changed = 0)
SELECT
  (SELECT COUNT(*) FROM prdrzranalytics.lab42.tmp_pulseFix_before_silverQgp)                AS rows_before,
  (SELECT COUNT(*) FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly)  AS rows_after,
  (SELECT COUNT(*)
   FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly n
   JOIN prdrzranalytics.lab42.tmp_pulseFix_before_silverQgp o
     ON n.qgp_date = o.qgp_date AND n.metric_name = o.metric_name AND n.metric_type = o.metric_type
   WHERE NOT (n.metric_value <=> o.metric_value))                                            AS metric_value_changed;

-- CHECK 3.3: every changed row with its reason (expect NO row starting with 'UNEXPECTED';
--            rows starting with 'review' are prior-year boundary-week LY changes: eyeball them)
WITH D AS (
  SELECT o.qgp_date, o.week_type, o.metric_name, o.metric_type,
         o.metric_value,
         o.wow_numerator   AS before_wow_num,  n.wow_numerator   AS after_wow_num,
         o.wow_denominator AS before_wow_den,  n.wow_denominator AS after_wow_den,
         o.wow_pct         AS before_wow_pct,  n.wow_pct         AS after_wow_pct,
         o.yoy_denominator AS before_yoy_den,  n.yoy_denominator AS after_yoy_den,
         o.metric_value_ly AS before_ly,       n.metric_value_ly AS after_ly,
         n.metric_name IN ('digitalPctPhoneNewActsNoAssistPlusAssist',
                           'digitalPctConsumerPostpaidActivationsTotalInclAssisted',
                           'digitalPctNoAssistanceActivations',
                           'digitalPctAssistanceActivations') AS is_ratio,
         NOT (n.metric_value <=> o.metric_value) AS value_changed
  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly n
  JOIN prdrzranalytics.lab42.tmp_pulseFix_before_silverQgp o
    ON n.qgp_date = o.qgp_date AND n.metric_name = o.metric_name AND n.metric_type = o.metric_type
  WHERE NOT (n.metric_value    <=> o.metric_value)
     OR NOT (n.wow_numerator   <=> o.wow_numerator)
     OR NOT (n.wow_denominator <=> o.wow_denominator)
     OR NOT (n.wow_pct         <=> o.wow_pct)
     OR NOT (n.yoy_numerator   <=> o.yoy_numerator)
     OR NOT (n.yoy_denominator <=> o.yoy_denominator)
     OR NOT (n.yoy_pct         <=> o.yoy_pct)
     OR NOT (n.metric_value_ly <=> o.metric_value_ly)
)
SELECT
  CASE
    WHEN value_changed                                                    THEN 'UNEXPECTED: metric_value changed'
    WHEN is_ratio                                                         THEN 'expected: Digital % ratio rule (boundary week, week after, LY, or LY now 3 decimals)'
    WHEN week_type = 'BOUNDARY_FIRST' AND after_wow_num IS NULL
         AND before_wow_num IS NOT NULL                                   THEN 'expected: boundary piece missing -> full week blank'
    WHEN before_wow_den = 0 AND after_wow_den IS NULL
         AND before_wow_pct <=> after_wow_pct                             THEN 'expected: no prior week, 0 -> blank (wow_pct unchanged)'
    WHEN after_wow_den IS NULL AND before_wow_den IS NOT NULL             THEN 'expected: prior boundary week missing a piece -> blank'
    WHEN NOT (before_yoy_den <=> after_yoy_den) OR NOT (before_ly <=> after_ly)
                                                                          THEN 'review: LY changed (prior-year boundary week)'
    ELSE 'review'
  END AS reason,
  *
FROM D
ORDER BY reason, metric_name, metric_type, qgp_date;

-- CHECK 3.4: the known week (expect actual 0.481875 / target 0.434495 on 2026-07-04,
--            wow_denominator 0.481875 / 0.434495 on 2026-07-11)
SELECT qgp_date, week_type, metric_type, metric_value,
       ROUND(wow_numerator, 6) AS full_week, ROUND(wow_denominator, 6) AS prior_full_week, ROUND(wow_pct, 4) AS wow_pct
FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly
WHERE metric_name = 'digitalPctConsumerPostpaidActivationsTotalInclAssisted'
  AND qgp_date IN (DATE'2026-06-30', DATE'2026-07-04', DATE'2026-07-11')
ORDER BY metric_type, qgp_date;


/* =================================================================================================
   PART 4 - FIX 2: UPV Forecast Silver
   ================================================================================================= */

-- 4.1 Deploy sdi_sp_dashboardPulseTms_silver_upvForecast_weekly.sql (updated), then:
CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_upvForecast_weekly();

-- CHECK 4.2: row count and metric_value unchanged (expect rows equal, metric_value_changed = 0)
SELECT
  (SELECT COUNT(*) FROM prdrzranalytics.lab42.tmp_pulseFix_before_silverUpvForecast)                AS rows_before,
  (SELECT COUNT(*) FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly)  AS rows_after,
  (SELECT COUNT(*)
   FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly n
   JOIN prdrzranalytics.lab42.tmp_pulseFix_before_silverUpvForecast o
     ON n.qgp_date = o.qgp_date AND n.channel_group = o.channel_group AND n.metric_name = o.metric_name
   WHERE NOT (n.metric_value <=> o.metric_value))                                                    AS metric_value_changed;

-- CHECK 4.3: every changed row with its reason (expect NO 'UNEXPECTED'; today expect only
--            2026-07-04 and 2026-07-11 for upvForecast and upvWebAppForecast, All Channels, plus
--            first-forecast-week denominators going from 0 to blank)
WITH D AS (
  SELECT o.qgp_date, o.week_type, o.channel_group, o.metric_name, o.metric_value,
         o.wow_numerator   AS before_wow_num,  n.wow_numerator   AS after_wow_num,
         o.wow_denominator AS before_wow_den,  n.wow_denominator AS after_wow_den,
         o.wow_pct         AS before_wow_pct,  n.wow_pct         AS after_wow_pct,
         o.yoy_denominator AS before_yoy_den,  n.yoy_denominator AS after_yoy_den,
         o.metric_value_ly AS before_ly,       n.metric_value_ly AS after_ly,
         NOT (n.metric_value <=> o.metric_value) AS value_changed
  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly n
  JOIN prdrzranalytics.lab42.tmp_pulseFix_before_silverUpvForecast o
    ON n.qgp_date = o.qgp_date AND n.channel_group = o.channel_group AND n.metric_name = o.metric_name
  WHERE NOT (n.metric_value    <=> o.metric_value)
     OR NOT (n.wow_numerator   <=> o.wow_numerator)
     OR NOT (n.wow_denominator <=> o.wow_denominator)
     OR NOT (n.wow_pct         <=> o.wow_pct)
     OR NOT (n.yoy_numerator   <=> o.yoy_numerator)
     OR NOT (n.yoy_denominator <=> o.yoy_denominator)
     OR NOT (n.yoy_pct         <=> o.yoy_pct)
     OR NOT (n.metric_value_ly <=> o.metric_value_ly)
)
SELECT
  CASE
    WHEN value_changed                                                    THEN 'UNEXPECTED: metric_value changed'
    WHEN week_type = 'BOUNDARY_FIRST' AND NOT (before_wow_num <=> after_wow_num)
                                                                          THEN 'expected: missing boundary piece estimated'
    WHEN before_wow_den = 0 AND after_wow_den IS NULL
         AND before_wow_pct <=> after_wow_pct                             THEN 'expected: no prior week, 0 -> blank (wow_pct unchanged)'
    WHEN NOT (before_wow_den <=> after_wow_den)                           THEN 'expected: prior boundary week now a full week'
    WHEN NOT (before_yoy_den <=> after_yoy_den) OR NOT (before_ly <=> after_ly)
                                                                          THEN 'review: LY changed (prior-year boundary week)'
    ELSE 'review'
  END AS reason,
  *
FROM D
ORDER BY reason, metric_name, channel_group, qgp_date;

-- CHECK 4.4: the known weeks (expect 2026-07-04 full week 1,533,000; 2026-07-11 wow_pct -0.0476;
--            2026-10-03 full week 1,597,531 unchanged)
SELECT qgp_date, week_type, metric_name, metric_value,
       wow_numerator AS full_week, wow_denominator AS prior_full_week, ROUND(wow_pct, 4) AS wow_pct
FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly
WHERE channel_group = 'All Channels'
  AND qgp_date IN (DATE'2026-07-04', DATE'2026-07-11', DATE'2026-10-03', DATE'2026-10-10')
ORDER BY metric_name, qgp_date;


/* =================================================================================================
   PART 5 - Validation + orchestration procedures, then one full normal run
   ================================================================================================= */

-- 5.1 Deploy sdi_sp_dashboardPulseTms_validation_history_perRun.sql (updated)
-- 5.2 Deploy sdi_sp_dashboardPulseTms_orchestration_allLayers_weekly.sql (updated)
-- 5.3 Run the PulseTMS weekly Job (or the scheduler notebook manually). This runs every layer,
--     including the archive, and writes a new validation run.


/* =================================================================================================
   PART 6 - End-to-end checks after the full run
   ================================================================================================= */

-- CHECK 6.1: Gold long metric_value unchanged for every row that existed before (expect 0 rows,
--            apart from the latest week or two if sources loaded new data in between)
SELECT n.data_source, n.metric_name, n.metric_type, n.true_lob, n.channel_group, n.qgp_date,
       o.metric_value AS before_value, n.metric_value AS after_value
FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long n
JOIN prdrzranalytics.lab42.tmp_pulseFix_before_goldLong o
  ON  n.data_source <=> o.data_source AND n.metric_name <=> o.metric_name
  AND n.metric_type <=> o.metric_type AND n.true_lob <=> o.true_lob
  AND n.channel_group <=> o.channel_group AND n.qgp_date = o.qgp_date
  AND n.mfc_channel <=> o.mfc_channel AND n.mfc_tactic <=> o.mfc_tactic
  AND n.mfc_message_type <=> o.mfc_message_type AND n.mfc_agency <=> o.mfc_agency
WHERE NOT (ROUND(n.metric_value, 2) <=> ROUND(o.metric_value, 2))
ORDER BY n.qgp_date DESC, n.data_source, n.metric_name;

-- CHECK 6.2: Gold long WoW changed ONLY for QGP Digital % and UPV forecast
--            (expect 0 rows for any other data_source / metric)
SELECT n.data_source, n.metric_name, COUNT(*) AS rows_with_wow_change
FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long n
JOIN prdrzranalytics.lab42.tmp_pulseFix_before_goldLong o
  ON  n.data_source <=> o.data_source AND n.metric_name <=> o.metric_name
  AND n.metric_type <=> o.metric_type AND n.true_lob <=> o.true_lob
  AND n.channel_group <=> o.channel_group AND n.qgp_date = o.qgp_date
  AND n.mfc_channel <=> o.mfc_channel AND n.mfc_tactic <=> o.mfc_tactic
  AND n.mfc_message_type <=> o.mfc_message_type AND n.mfc_agency <=> o.mfc_agency
WHERE NOT (ROUND(n.wow_pct, 6) <=> ROUND(o.wow_pct, 6))
   OR NOT (ROUND(n.yoy_pct, 6) <=> ROUND(o.yoy_pct, 6))
GROUP BY n.data_source, n.metric_name
ORDER BY 1, 2;

-- CHECK 6.3: columns of every existing object unchanged (expect 0 rows in both directions)
WITH AfterCols AS (
  SELECT table_name, ordinal_position, column_name, data_type
  FROM   prdrzranalytics.information_schema.columns
  WHERE  table_schema = 'lab42'
    AND  table_name IN (SELECT DISTINCT table_name FROM prdrzranalytics.lab42.tmp_pulseFix_schema_before))
SELECT 'missing or changed after' AS issue, x.* FROM (
  SELECT * FROM prdrzranalytics.lab42.tmp_pulseFix_schema_before EXCEPT SELECT * FROM AfterCols) x
UNION ALL
SELECT 'new or changed after', y.* FROM (
  SELECT * FROM AfterCols EXCEPT SELECT * FROM prdrzranalytics.lab42.tmp_pulseFix_schema_before) y;

-- CHECK 6.4: validation statuses per metric, latest run vs the run before it
--            (expect no metric to get WORSE, e.g. Healthy -> Warning/Failed, because of the fixes)
WITH Runs AS (
  SELECT validation_run_id, ROW_NUMBER() OVER (ORDER BY validation_run_ts DESC, validation_run_id DESC) AS rn
  FROM (SELECT DISTINCT validation_run_id, validation_run_ts
        FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_validation_history_perRun)),
Cur  AS (SELECT h.* FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_validation_history_perRun h
         JOIN Runs r ON r.validation_run_id = h.validation_run_id AND r.rn = 1),
Prev AS (SELECT h.* FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_validation_history_perRun h
         JOIN Runs r ON r.validation_run_id = h.validation_run_id AND r.rn = 2)
SELECT COALESCE(c.data_source, p.data_source) AS data_source,
       COALESCE(c.metric_name, p.metric_name) AS metric_name,
       p.status AS previous_status, c.status AS latest_status,
       c.issue_layer, c.notes
FROM Cur c FULL OUTER JOIN Prev p
  ON c.data_source = p.data_source AND c.metric_name = p.metric_name AND c.metric_type <=> p.metric_type
WHERE NOT (c.status <=> p.status)
ORDER BY 1, 2;

-- 6.5 Refresh the Tableau extract, then spot check:
--     Digital % (Total incl. assisted) WoW for week ending 2026-07-04 = about +1.2% (was about +102%)
--     UPV forecast WoW for week ending 2026-07-11 = about -4.8% (was about +66.7%)


/* =================================================================================================
   PART 7 - Weekly narrative (only after PART 6 passes)
   ================================================================================================= */

-- 7.1 Deploy the updated weekly narrative procedure
--     02_sdi_sp_weeklyNarrative_gold_factsHistory_long.sql (same rules as PulseTMS Silver for a
--     missing boundary piece). No column changes.
-- 7.2 Rebuild history so the UPV forecast target for week ending 2026-07-04 uses 1,533,000
--     (use the same start Saturday as your first backfill):
-- CALL prdrzranalytics.lab42.sdi_sp_weeklyNarrative_orchestration_factsHistoryBackfill_perRun(DATE'2024-11-02', NULL, FALSE);


/* =================================================================================================
   PART 8 - Clean up (after everything is signed off)
   ================================================================================================= */

-- DROP TABLE IF EXISTS prdrzranalytics.lab42.tmp_pulseFix_schema_before;
-- DROP TABLE IF EXISTS prdrzranalytics.lab42.tmp_pulseFix_before_bronzeBiddable;
-- DROP TABLE IF EXISTS prdrzranalytics.lab42.tmp_pulseFix_before_silverBiddable;
-- DROP TABLE IF EXISTS prdrzranalytics.lab42.tmp_pulseFix_before_silverQgp;
-- DROP TABLE IF EXISTS prdrzranalytics.lab42.tmp_pulseFix_before_silverUpvForecast;
-- DROP TABLE IF EXISTS prdrzranalytics.lab42.tmp_pulseFix_before_goldLong;


/* =================================================================================================
   PART 9 - ROLLBACK (only if a CHECK fails)
   ================================================================================================= */

-- 9.1 Put the data back (use the versions written down in 0.3). Views follow automatically.
-- RESTORE TABLE prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_biddableSpend_weekly TO VERSION AS OF <v>;
-- RESTORE TABLE prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_biddableSpend_weekly TO VERSION AS OF <v>;
-- RESTORE TABLE prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly          TO VERSION AS OF <v>;
-- RESTORE TABLE prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly  TO VERSION AS OF <v>;

-- 9.2 Put the code back: re-deploy the ORIGINAL version of whichever procedure file was updated,
--     so the next scheduled run does not re-apply the change.

-- 9.3 The Biddable Programmatic archive objects are new and nothing else writes to them. They can
--     stay in place even if Biddable Bronze is rolled back (it simply goes back to the raw source).
--     Keep the archive running weekly regardless: it is what protects the history.


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