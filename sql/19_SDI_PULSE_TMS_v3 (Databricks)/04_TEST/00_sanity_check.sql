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

WHAT EACH CHECK COMPARES AGAINST (no data copies are made):
  - The raw source, wherever the raw source holds the same thing (Programmatic archive and
    Biddable Programmatic).
  - The table's OWN previous Delta version (time travel: VERSION AS OF), for the PulseTMS Bronze
    and Silver tables. WoW / YoY columns do not exist in any raw source, so "before the change"
    is the only meaningful comparison for them.
  - One tiny column-list snapshot (metadata only, no data rows) to prove no column changed;
    views keep no version history, so this is the only way to compare their columns.

  Replace the four placeholders <V_BRONZE_BIDDABLE>, <V_SILVER_BIDDABLE>, <V_SILVER_QGP> and
  <V_SILVER_UPV> with the version numbers written down in PART 0.2 (Find and Replace).
  Run the comparisons the same day as the change: old versions stay readable until VACUUM
  removes their files (7 days by default).

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
   PART 0 - BEFORE ANYTHING: column snapshot + Delta versions (no data is copied)
   ================================================================================================= */

-- 0.1 Column snapshot (metadata only) of every existing object these fixes touch or feed
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

-- 0.2 Write down the CURRENT Delta version of each table that will change (the "version" column of
--     the first row). Every "before" comparison below and the rollback in PART 9 use these.
--       <V_BRONZE_BIDDABLE>  sdi_tbl_dashboardPulseTms_bronze_biddableSpend_weekly
--       <V_SILVER_BIDDABLE>  sdi_tbl_dashboardPulseTms_silver_biddableSpend_weekly
--       <V_SILVER_QGP>       sdi_tbl_dashboardPulseTms_silver_qgp_weekly
--       <V_SILVER_UPV>       sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly
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

-- CHECK 2.0: Programmatic in the NEW Bronze equals the RAW source run through the same Bronze
--            logic (week, LOB scope, DSP normalization). Expect 0 rows.
--            Valid while the raw source still holds the whole year (i.e. before January).
WITH RawProg AS (
  SELECT
    date_add(CAST(raw.`date` AS DATE), 7 - dayofweek(CAST(raw.`date` AS DATE))) AS week_sun_sat,
    UPPER(TRIM(raw.lob))                                                      AS lob,
    CASE
      WHEN UPPER(TRIM(raw.DSP)) IN ('AMAZON', 'AMAZON DSP')                   THEN 'Amazon DSP'
      WHEN UPPER(TRIM(raw.DSP)) IN ('THE TRADE DESK', 'TTD')                  THEN 'The Trade Desk'
      WHEN UPPER(TRIM(raw.DSP)) IN ('DV360', 'DBM', 'DISPLAY & VIDEO 360')    THEN 'DV360'
      WHEN UPPER(TRIM(raw.DSP)) = 'BLIS'                                      THEN 'Blis'
      WHEN UPPER(TRIM(raw.DSP)) IN ('GOOGLE', 'GOOGLE ADS')                   THEN 'Google Ads'
      WHEN NULLIF(TRIM(raw.DSP), '') IS NOT NULL                              THEN TRIM(raw.DSP)
      ELSE 'Unknown'
    END                                                                       AS platform,
    TRY_CAST(raw.spend AS DOUBLE)                                             AS spend
  FROM prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr raw
  WHERE raw.`date` IS NOT NULL
    AND UPPER(TRIM(raw.lob)) IN ('POSTPAID', 'CONSUMER POSTPAID', 'HSI', 'BROADBAND')),
RawWeekly AS (
  SELECT week_sun_sat, lob, platform, SUM(COALESCE(spend, 0)) AS spend
  FROM RawProg
  WHERE week_sun_sat IS NOT NULL
  GROUP BY 1, 2, 3),
NewBronze AS (
  SELECT week_sun_sat, lob, platform, spend
  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_biddableSpend_weekly
  WHERE channel_group = 'Programmatic')
SELECT COALESCE(r.week_sun_sat, n.week_sun_sat) AS week_sun_sat, COALESCE(r.lob, n.lob) AS lob,
       COALESCE(r.platform, n.platform) AS platform, r.spend AS raw_source_spend, n.spend AS bronze_spend
FROM RawWeekly r
FULL OUTER JOIN NewBronze n
  ON r.week_sun_sat = n.week_sun_sat AND r.lob = n.lob AND r.platform = n.platform
WHERE ABS(COALESCE(r.spend, 0) - COALESCE(n.spend, 0)) > 0.005
ORDER BY 1 DESC, 2, 3;

-- CHECK 2.2: Bronze identical to its own version before the change (expect 0 rows)
SELECT COALESCE(n.week_sun_sat, o.week_sun_sat) AS week_sun_sat, COALESCE(n.lob, o.lob) AS lob,
       COALESCE(n.channel_group, o.channel_group) AS channel_group, COALESCE(n.platform, o.platform) AS platform,
       o.spend AS before_spend, n.spend AS after_spend
FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_biddableSpend_weekly n
FULL OUTER JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_biddableSpend_weekly VERSION AS OF <V_BRONZE_BIDDABLE> o
  ON  n.week_sun_sat = o.week_sun_sat AND n.lob <=> o.lob
  AND n.channel_group <=> o.channel_group AND n.platform <=> o.platform
WHERE NOT (ROUND(n.spend, 2) <=> ROUND(o.spend, 2));

-- CHECK 2.3: Silver identical to its own version before the change (expect 0 rows)
SELECT COALESCE(n.qgp_date, o.qgp_date) AS qgp_date, COALESCE(n.lob, o.lob) AS lob,
       COALESCE(n.channel_group, o.channel_group) AS channel_group
FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_biddableSpend_weekly n
FULL OUTER JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_biddableSpend_weekly VERSION AS OF <V_SILVER_BIDDABLE> o
  ON  n.qgp_date = o.qgp_date AND n.lob <=> o.lob AND n.channel_group <=> o.channel_group
  AND n.metric_name <=> o.metric_name
WHERE NOT (ROUND(n.metric_value, 2)    <=> ROUND(o.metric_value, 2))
   OR NOT (ROUND(n.wow_numerator, 2)   <=> ROUND(o.wow_numerator, 2))
   OR NOT (ROUND(n.wow_denominator, 2) <=> ROUND(o.wow_denominator, 2))
   OR NOT (ROUND(n.yoy_denominator, 2) <=> ROUND(o.yoy_denominator, 2));
-- Note: the sources keep loading during the day. If 2.2 / 2.3 show rows ONLY for the latest one
-- or two weeks, that is new spend that landed between the previous run and this one, not the fix
-- (CHECK 2.0 compares against the raw source directly and is not affected by this).


/* =================================================================================================
   PART 3 - FIX 1: QGP Silver
   ================================================================================================= */

-- 3.1 Deploy sdi_sp_dashboardPulseTms_silver_qgp_weekly.sql (updated), then:
CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_qgp_weekly();

-- CHECK 3.2: row count unchanged and metric_value unchanged on EVERY row
--            (expect rows_before = rows_after, metric_value_changed = 0)
SELECT
  (SELECT COUNT(*) FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly VERSION AS OF <V_SILVER_QGP>)                AS rows_before,
  (SELECT COUNT(*) FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly)  AS rows_after,
  (SELECT COUNT(*)
   FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly n
   JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly VERSION AS OF <V_SILVER_QGP> o
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
  JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly VERSION AS OF <V_SILVER_QGP> o
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
  (SELECT COUNT(*) FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly VERSION AS OF <V_SILVER_UPV>)                AS rows_before,
  (SELECT COUNT(*) FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly)  AS rows_after,
  (SELECT COUNT(*)
   FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly n
   JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly VERSION AS OF <V_SILVER_UPV> o
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
  JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly VERSION AS OF <V_SILVER_UPV> o
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

-- CHECK 6.1: after the full run, metric_value in every changed table still equals its version
--            before the change (expect 0 for all four; small counts ONLY in the latest week or two
--            mean sources loaded new data in between). Gold long and Gold wide are views on these
--            tables, so equal tables mean equal Gold.
SELECT 'bronze_biddableSpend' AS table_checked, COUNT(*) AS rows_with_different_value
FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_biddableSpend_weekly n
JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_biddableSpend_weekly VERSION AS OF <V_BRONZE_BIDDABLE> o
  ON n.week_sun_sat = o.week_sun_sat AND n.lob <=> o.lob AND n.channel_group <=> o.channel_group AND n.platform <=> o.platform
WHERE NOT (ROUND(n.spend, 2) <=> ROUND(o.spend, 2))
UNION ALL
SELECT 'silver_biddableSpend', COUNT(*)
FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_biddableSpend_weekly n
JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_biddableSpend_weekly VERSION AS OF <V_SILVER_BIDDABLE> o
  ON n.qgp_date = o.qgp_date AND n.lob <=> o.lob AND n.channel_group <=> o.channel_group AND n.metric_name <=> o.metric_name
WHERE NOT (ROUND(n.metric_value, 2) <=> ROUND(o.metric_value, 2))
UNION ALL
SELECT 'silver_qgp', COUNT(*)
FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly n
JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly VERSION AS OF <V_SILVER_QGP> o
  ON n.qgp_date = o.qgp_date AND n.metric_name = o.metric_name AND n.metric_type = o.metric_type
WHERE NOT (n.metric_value <=> o.metric_value)
UNION ALL
SELECT 'silver_upvForecast', COUNT(*)
FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly n
JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly VERSION AS OF <V_SILVER_UPV> o
  ON n.qgp_date = o.qgp_date AND n.channel_group = o.channel_group AND n.metric_name = o.metric_name
WHERE NOT (n.metric_value <=> o.metric_value);

-- CHECK 6.2: Gold long shows the corrected boundary weeks (eyeball; same values as CHECK 3.4 / 4.4)
SELECT data_source, metric_name, metric_type, qgp_date, ROUND(wow_numerator, 6) AS full_week,
       ROUND(wow_denominator, 6) AS prior_full_week, ROUND(wow_pct, 4) AS wow_pct
FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long
WHERE qgp_date IN (DATE'2026-07-04', DATE'2026-07-11')
  AND (   (data_source = 'QGP_SCORECARD' AND metric_name = 'digitalPctConsumerPostpaidActivationsTotalInclAssisted')
       OR (data_source = 'UPV_FORECAST'  AND metric_name = 'upvForecast' AND channel_group = 'All Channels'))
ORDER BY data_source, metric_type, qgp_date;

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

-- DROP TABLE IF EXISTS prdrzranalytics.lab42.tmp_pulseFix_schema_before;   -- the only helper table (metadata)


/* =================================================================================================
   PART 9 - ROLLBACK (only if a CHECK fails)
   ================================================================================================= */

-- 9.1 Put the data back (versions written down in PART 0.2). Views follow automatically.
-- RESTORE TABLE prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_biddableSpend_weekly TO VERSION AS OF <V_BRONZE_BIDDABLE>;
-- RESTORE TABLE prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_biddableSpend_weekly TO VERSION AS OF <V_SILVER_BIDDABLE>;
-- RESTORE TABLE prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly          TO VERSION AS OF <V_SILVER_QGP>;
-- RESTORE TABLE prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly  TO VERSION AS OF <V_SILVER_UPV>;

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
          Every check compares against the raw source, the archive, or (Q10 only) a table's own
          previous Delta version. No backup tables are needed.
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
   Q10. Nothing else moved: metric_value in the two fixed Silvers vs their version before the
        change (time travel). Replace <V_SILVER_QGP> / <V_SILVER_UPV> with the versions from
        DESCRIBE HISTORY (the version just before the fix was deployed). Run within 7 days of the
        change (old versions are removed by VACUUM after that).
        Expect 0 for both (small counts ONLY in the latest week or two = new data loaded since).
   ------------------------------------------------------------------------------------------------- */
SELECT 'silver_qgp' AS table_checked, COUNT(*) AS rows_with_different_value
FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly n
JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly VERSION AS OF <V_SILVER_QGP> o
  ON n.qgp_date = o.qgp_date AND n.metric_name = o.metric_name AND n.metric_type = o.metric_type
WHERE NOT (n.metric_value <=> o.metric_value)
UNION ALL
SELECT 'silver_upvForecast', COUNT(*)
FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly n
JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly VERSION AS OF <V_SILVER_UPV> o
  ON n.qgp_date = o.qgp_date AND n.channel_group = o.channel_group AND n.metric_name = o.metric_name
WHERE NOT (n.metric_value <=> o.metric_value);


/* -------------------------------------------------------------------------------------------------
   Q11. Latest validation run (expect overall status no worse than before the change)
   ------------------------------------------------------------------------------------------------- */
SELECT run_label, overall_status, healthy_count, warning_count, failed_count, issue_metrics
FROM   prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_validation_runs_perRun
ORDER BY validation_run_ts DESC
LIMIT 3;