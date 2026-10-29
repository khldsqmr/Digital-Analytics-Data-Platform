/* =================================================================================================
FILE:           02_sdi_sp_weeklyNarrative_gold_factsHistory_long.sql
PLATFORM:       Databricks
LAYER:          Gold (frozen facts for the weekly narrative report)
CATALOG.SCHEMA: prdrzranalytics.lab42
PROCEDURE:      sdi_sp_weeklyNarrative_gold_factsHistory_long
TABLE:          sdi_tbl_weeklyNarrative_gold_factsHistory_long

SOURCE LINEAGE (this project is separate from PulseTMS):
  Upstream project : PulseTMS
  Upstream objects : prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long   (numbers)
                     prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar    (week selection)
                     prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_validation_runs_perRun
                     prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_validation_summary_perRun
  weeklyNarrative never recomputes a PulseTMS metric. Every row stamps source_project and
  source_object; columns copied from upstream are documented as "PulseTMS Gold ...", and
  upstream validation fields are prefixed source_. "gold" in this project's own object
  names refers to the weeklyNarrative layers, not PulseTMS.

  Adding another upstream project later = new registry rows with their own source_project /
  source_object, plus one more adapter block (Reg / Gold / GoldHist filtered to that object)
  unioned into AllRows, the same way PulseTMS Gold appends a new source CTE.

PURPOSE:
  Takes an append-only snapshot of the registry metrics for ONE reporting week from
  sdi_vw_dashboardPulseTms_gold_unified_long.

  This is Track A's single source of every number in the weekly narrative report.
  The AI never reads Gold directly; it reads a snapshot_id from this table.

WHY APPEND-ONLY (same pattern as the PulseTMS validation history):
  - Every run writes a new snapshot_id. Nothing is ever updated or deleted.
  - The published report pins the snapshot_id it was written from, so a published week
    never changes when Gold restates, and any restatement is visible by comparing snapshots.
  - sdi_vw_weeklyNarrative_gold_factsLatest_long returns the newest snapshot per week.

WEEK SELECTION:
  p_week_end = NULL  -> latest completed Saturday from the PulseTMS QGP calendar
                        (same rule the PulseTMS validation procedure uses).
  p_week_end = date  -> that Saturday (backfills and backtests).
  Must be a Saturday: the report is a natural Sunday-Saturday week. BOUNDARY_STUB dates are
  never snapshotted on their own.

WEEK VALUE (boundary-safe):
  week_value = COALESCE(wow_numerator, metric_value)

  For NORMAL weeks wow_numerator = metric_value.
  For BOUNDARY_FIRST weeks Gold's metric_value is only the post-quarter-end days, while
  wow_numerator = current + preceding stub = the full natural week. Using wow_numerator
  keeps the report on a whole Sunday-Saturday week, consistent with wow_pct.
  The raw metric_value is kept alongside for traceability.

LINE OF BUSINESS:
  report_lob (from the registry) gives every row one LOB vocabulary across sources:
  POSTPAID | BROADBAND | BYOD | FIBER | TFB | METRO | TOTAL | NOT_LOB_SPECIFIC.
  Registry rows with lob_rollup (e.g. MFC [POSTPAID, BROADBAND]) are SUMMED across their
  member true_lob values for every week in the window; WoW / YoY are recomputed from the
  summed numerators and denominators, and is_lob_rollup_complete flags a partial total.
  Scaling to TFB / METRO = registry rows only; no change to this procedure.

ROW ROLES:
  HEADLINE          registry is_headline = TRUE, headline channel_group
  DRIVER            same metric, other channel_group (registry include_channel_drivers)
  REFERENCE         registry is_headline = FALSE (a target row: forecast or QGP target)
  REFERENCE_DRIVER  channel-level reference rows (e.g. MFC forecast per channel)

TARGET COMPARISON:
  Target = the forecast for MFC spend and UPV, and the QGP target for QGP scorecard metrics.
  Each row is joined to its target_metric_key row (same week, same channel_group).
    vs_target_abs = week_value - target_week_value
    vs_target_pct = week_value / target_week_value - 1     (NULL for unit = 'PCT')
  For unit = 'PCT', vs_target_abs is the gap in percentage POINTS (ratio difference).

  MFC FORECAST FALLBACK: PulseTMS MFC Bronze fills spend_forecast with spend_actual for any
  week that has no forecast at all. Those weeks would read as "exactly on target", so
  is_target_same_as_actual = TRUE flags them and the narrative must say "no target
  available" instead of "on target".

CONTEXT STATS (averages, totals, normal swing):
  Computed at snapshot time from Gold history up to and including week_end only, so a
  backfilled week sees the same window it would have seen live (dates only; Gold values
  themselves are today's values).

  Trailing windows (full Sunday-Saturday weeks BEFORE week_end, Saturdays only,
  using the same boundary-safe week value as week_value):
    avg_prior_4wk / avg_prior_13wk / avg_prior_52wk   average weekly value
    stddev_prior_13wk                                  normal weekly swing (for "what moved")
    prior_52wk_weeks                                   weeks actually available in the 52-week
                                                       window (short history = weak average)

  Calendar to-date windows (month / QGP quarter / year containing week_end, up to week_end):
    Built from Gold metric_value over every QGP period in the window, so quarter and year
    boundaries are exact (stubs land in the right quarter).
    Months use the week-ending convention: a week belongs to the month its Saturday falls in.

    COUNT / USD : *_total      = sum of period values
                  *_avg_weekly = *_total / (days covered / 7)   (average per 7 days)
    PCT         : *_avg_weekly = day-weighted average of the ratio; *_total is NULL

  Comparisons (ratio for COUNT / USD, percentage POINTS for PCT):
    vs_avg_prior_4wk / 13wk / 52wk   week_value vs the trailing average
    zscore_prior_13wk                (week_value - avg_prior_13wk) / stddev_prior_13wk
    qtd_vs_target                    QTD total vs QTD target total (PCT: QTD avg vs target avg)

VALIDATION LINEAGE:
  Stamps the PulseTMS validation run for the same data_as_of_date (if one exists) and the
  per-data-source validation status, so the narrative can caveat a source that is in
  Warning / Failed instead of writing confidently about it.

NAMING CONVENTION (same as the registry):
  Codes (filter / join / branch) UPPER_SNAKE: report_lob, unit, source_project, row_role,
  run_type, plus PulseTMS codes kept as-is. Labels for the report text in their own columns:
  display_name, report_section, report_lob_label. Upstream PulseTMS values are never re-cased.

METADATA:
  Every column carries a COMMENT in the CREATE TABLE below. CREATE TABLE IF NOT EXISTS does
  not update comments on an existing table, so if an earlier version of this table was
  already created, drop it once (it only holds test snapshots at this stage) and re-run.

FAILURE BEHAVIOUR:
  Raises an error if the week is not a Saturday, is in the future, or if zero rows were
  written (Gold not populated for that week).

CHANGE LOG:
  - is_target_same_as_actual now uses a relative tolerance; the earlier fixed 0.005 flagged
    real gaps of up to half a point on PCT metrics as "same as actual".
  - Future weeks are refused instead of freezing NULL / partial values.
  - PulseTMS Gold scan restricted to the data sources the registry uses.

ORCHESTRATION:
  Run as a separate Databricks Job task that depends on the PulseTMS orchestration task,
  so a failure here never blocks the PulseTMS dashboards. Example SQL task:
    CALL prdrzranalytics.lab42.sdi_sp_weeklyNarrative_gold_factsHistory_long(
      NULL, 'JOB', '{{job.run_id}}'
    );

================================================================================================= */

CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_weeklyNarrative_gold_factsHistory_long
(
  IN p_week_end   DATE   DEFAULT NULL,
  IN p_run_type   STRING DEFAULT NULL,
  IN p_job_run_id STRING DEFAULT NULL
)

LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Weekly narrative report: appends one frozen snapshot of PulseTMS-sourced registry metrics for one Sunday-Saturday week from sdi_vw_dashboardPulseTms_gold_unified_long into sdi_tbl_weeklyNarrative_gold_factsHistory_long. p_week_end NULL = latest completed Saturday.'

AS
BEGIN

  /* ===============================================================================================
     STEP 0 - EXECUTION CONTEXT
     =============================================================================================== */

  DECLARE v_ts                TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE v_snapshot_id       STRING;
  DECLARE v_run_type          STRING;
  DECLARE v_job_run_id        STRING;
  DECLARE v_week_end          DATE;
  DECLARE v_validation_run_id STRING;
  DECLARE v_row_count         BIGINT;


  SET v_snapshot_id =
    CONCAT('WKNARR_FACTS_', DATE_FORMAT(v_ts, 'yyyyMMdd_HHmmss_SSS'));


  SET v_run_type =
    CASE
      WHEN UPPER(TRIM(COALESCE(sdi_sp_weeklyNarrative_gold_factsHistory_long.p_run_type, ''))) = 'JOB'
        THEN 'JOB'
      ELSE 'MANUAL'
    END;


  SET v_job_run_id =
    COALESCE(
      NULLIF(TRIM(sdi_sp_weeklyNarrative_gold_factsHistory_long.p_job_run_id), ''),
      v_snapshot_id
    );


  /* Week: explicit parameter, else latest completed Saturday. */
  SET v_week_end =
    COALESCE(
      sdi_sp_weeklyNarrative_gold_factsHistory_long.p_week_end,
      (
        SELECT cal.qgp_date
        FROM   prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar cal
        WHERE  cal.is_complete_period = TRUE
          AND  cal.qgp_date <= CURRENT_DATE()
          AND  DAYOFWEEK(cal.qgp_date) = 7
        ORDER BY cal.qgp_date DESC
        LIMIT 1
      )
    );


  IF v_week_end IS NULL OR DAYOFWEEK(v_week_end) <> 7 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT =
        'Weekly narrative facts snapshot: week_end must resolve to a Saturday (Sunday-Saturday reporting week).';
  END IF;


  /* A week that has not finished yet would freeze NULL / partial values into history. */
  IF v_week_end > CURRENT_DATE() THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT =
        'Weekly narrative facts snapshot: week_end is in the future. Only completed Sunday-Saturday weeks can be snapshotted.';
  END IF;


  /* Latest PulseTMS validation run for the same week, if any. */
  SET v_validation_run_id =
  (
    SELECT r.validation_run_id
    FROM   prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_validation_runs_perRun r
    WHERE  r.data_as_of_date = v_week_end
    ORDER BY r.validation_run_ts DESC, r.validation_run_id DESC
    LIMIT 1
  );


  /* ===============================================================================================
     STEP 1 - CREATE TABLE IF NEEDED
     =============================================================================================== */

  CREATE TABLE IF NOT EXISTS
    prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_factsHistory_long
  (
    snapshot_id          STRING     COMMENT 'Snapshot identifier, WKNARR_FACTS_yyyyMMdd_HHmmss_SSS. One per procedure run. Published reports pin this value.',
    snapshot_ts          TIMESTAMP  COMMENT 'When the snapshot was taken (UTC).',
    run_type             STRING     COMMENT 'JOB = scheduled Databricks Job run. MANUAL = interactive or ad hoc call (including backfills).',
    job_run_id           STRING     COMMENT 'Databricks Job Run ID for JOB runs; the snapshot_id for MANUAL runs.',

    week_end             DATE       COMMENT 'Reporting week end date. Always a Saturday; the week runs Sunday to Saturday.',
    week_type            STRING     COMMENT 'From the PulseTMS QGP calendar: NORMAL or BOUNDARY_FIRST. BOUNDARY_FIRST = first Saturday after a quarter end that fell mid-week.',
    qgp_quarter          STRING     COMMENT 'QGP quarter label of week_end, e.g. 2026 Q4.',
    days_in_period       INT        COMMENT 'Days in the QGP period ending on week_end: 7 for NORMAL, fewer for BOUNDARY_FIRST. Explains why metric_value can be partial.',
    is_complete_period   BOOLEAN    COMMENT 'Calendar completeness flag from the PulseTMS QGP calendar. Does NOT guarantee every source has settled; see is_data_current and source_validation_status.',

    registry_version     STRING     COMMENT 'registry_version of sdi_vw_weeklyNarrative_control_metricRegistry_static used for this snapshot.',
    metric_key           STRING     COMMENT 'Registry primary key of the metric slice.',
    display_name         STRING     COMMENT 'Report-facing metric name from the registry.',
    report_section       STRING     COMMENT 'Report section display label from the registry (Traffic, Funnel, Spend, Activations, Retail, Virtual Retail).',
    report_lob           STRING     COMMENT 'Code (UPPER_SNAKE) of the report-facing line of business from the registry: POSTPAID, BROADBAND, BYOD, FIBER, TFB, METRO, TOTAL or NOT_LOB_SPECIFIC. One vocabulary across all sources; use this (not true_lob) to group the report by LOB.',
    report_lob_label     STRING     COMMENT 'Display label of report_lob for the narrative text (Postpaid, Broadband, BYOD, Fiber, TFB, Metro, Total, Not LOB specific). Filter on report_lob, write with report_lob_label.',
    sort_order           INT        COMMENT 'Report display order from the registry.',
    row_role             STRING     COMMENT 'HEADLINE = reportable metric at its headline channel. DRIVER = same metric at another channel_group, used to explain moves. REFERENCE = forecast or target row. REFERENCE_DRIVER = channel-level forecast or target.',

    source_project       STRING     COMMENT 'Code of the upstream project the numbers come from: PULSE_TMS. weeklyNarrative only reads these values, it never recomputes them.',
    source_object        STRING     COMMENT 'Fully qualified upstream object read for this row: prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long (PulseTMS Gold).',

    data_source          STRING     COMMENT 'PulseTMS Gold data_source the row came from.',
    metric_name          STRING     COMMENT 'PulseTMS Gold metric_name the row came from.',
    metric_type          STRING     COMMENT 'PulseTMS Gold metric_type the row came from.',
    source_lob           STRING     COMMENT 'PulseTMS Gold lob column as-is (business-facing grouping, e.g. Postpaid + Broadband, Biddable, POSTPAID).',
    true_lob             STRING     COMMENT 'PulseTMS Gold true_lob (canonical or reporting LOB). NULL where PulseTMS Gold has no LOB concept. For lob_rollup rows, the comma-separated member LOBs actually found, e.g. BROADBAND, POSTPAID.',
    lob_rollup_members   STRING     COMMENT 'For LOB rollup rows: the LOBs the registry says to sum, e.g. POSTPAID + BROADBAND. NULL for single-LOB rows.',
    is_lob_rollup_complete BOOLEAN  COMMENT 'For LOB rollup rows: TRUE when every member LOB had a value this week. FALSE means the total is partial (a member LOB is missing) and must not be reported as a full total. NULL for single-LOB rows.',
    channel_group        STRING     COMMENT 'PulseTMS Gold channel_group. Headline rows are normally All Channels; DRIVER rows carry the individual channel. NULL for QGP_SCORECARD.',

    unit                 STRING     COMMENT 'COUNT | USD | PCT. PCT values are ratios (0.31 = 31%).',
    higher_is_better     BOOLEAN    COMMENT 'Direction of good performance from the registry. NULL = neutral.',

    metric_value         DOUBLE     COMMENT 'PulseTMS Gold metric_value for the QGP period ending week_end, unchanged. Partial on BOUNDARY_FIRST weeks. Kept for traceability; report on week_value instead.',
    week_value           DOUBLE     COMMENT 'Value for the full Sunday-Saturday week = COALESCE(wow_numerator, metric_value). Equals metric_value on NORMAL weeks; adds the preceding quarter-end stub on BOUNDARY_FIRST weeks. The number the report uses.',
    prior_week_value     DOUBLE     COMMENT 'PulseTMS Gold wow_denominator: prior full week value used for WoW (includes stub when the prior week was BOUNDARY_FIRST).',
    wow_pct              DOUBLE     COMMENT 'Week-over-week change as a ratio (0.12 = +12%). PulseTMS Gold wow_pct for single-LOB rows; recomputed from summed WoW numerators and denominators for LOB rollup rows.',
    ly_week_value        DOUBLE     COMMENT 'PulseTMS Gold yoy_denominator: same ISO week last year, full weekly total.',
    yoy_pct              DOUBLE     COMMENT 'Year-over-year change as a ratio (0.04 = +4%). PulseTMS Gold yoy_pct for single-LOB rows; recomputed from summed numerators and denominators for LOB rollup rows. NULL where prior-year history does not exist (e.g. Programmatic, Fiber).',

    target_metric_key    STRING     COMMENT 'Registry key of the target this row is compared against: the forecast for MFC spend and UPV, the QGP target for QGP metrics. NULL if none.',
    target_week_value    DOUBLE     COMMENT 'Target for the same week_end and channel_group (full Sunday-Saturday week). NULL if no target or not populated.',
    vs_target_abs        DOUBLE     COMMENT 'Actual minus target: week_value - target_week_value. For unit = PCT, the gap in points as a ratio (0.02 = 2 pts).',
    vs_target_pct        DOUBLE     COMMENT 'Actual vs target as a ratio: week_value / target_week_value - 1 (-0.03 = 3% below target). NULL for unit = PCT.',
    is_target_same_as_actual BOOLEAN COMMENT 'TRUE when target equals actual exactly. For MFC this means PulseTMS filled a missing forecast with the actual (week-level fallback in MFC Bronze), so the narrative must say no target was available, not on target. NULL when there is no target.',

    avg_prior_4wk        DOUBLE     COMMENT 'Average weekly value of the 4 full weeks before week_end (current week excluded).',
    avg_prior_13wk       DOUBLE     COMMENT 'Average weekly value of the 13 full weeks (about one quarter) before week_end.',
    avg_prior_52wk       DOUBLE     COMMENT 'Average weekly value of the 52 full weeks (about one year) before week_end.',
    stddev_prior_13wk    DOUBLE     COMMENT 'Sample standard deviation of weekly values over the 13 weeks before week_end: the metric normal weekly swing.',
    prior_52wk_weeks     INT        COMMENT 'Number of weeks with data in the 52-week window. Below 52 means short history and a weaker average.',
    vs_avg_prior_4wk     DOUBLE     COMMENT 'week_value vs avg_prior_4wk. Ratio for COUNT/USD (0.08 = 8% above average); points for PCT (0.01 = 1 pt).',
    vs_avg_prior_13wk    DOUBLE     COMMENT 'week_value vs avg_prior_13wk. Ratio for COUNT/USD; points for PCT.',
    vs_avg_prior_52wk    DOUBLE     COMMENT 'week_value vs avg_prior_52wk. Ratio for COUNT/USD; points for PCT.',
    zscore_prior_13wk    DOUBLE     COMMENT '(week_value - avg_prior_13wk) / stddev_prior_13wk. How unusual this week is versus normal swing; |z| >= 2 is a strong highlight candidate.',

    mtd_avg_weekly       DOUBLE     COMMENT 'Month-to-date average per 7 days, for weeks ending in the calendar month of week_end. PCT: day-weighted average ratio.',
    qtd_avg_weekly       DOUBLE     COMMENT 'QGP quarter-to-date average per 7 days, quarter-boundary exact. PCT: day-weighted average ratio.',
    ytd_avg_weekly       DOUBLE     COMMENT 'Year-to-date average per 7 days, year-boundary exact. PCT: day-weighted average ratio.',
    mtd_total            DOUBLE     COMMENT 'Month-to-date total (weeks ending in the month of week_end). NULL for PCT.',
    qtd_total            DOUBLE     COMMENT 'QGP quarter-to-date total through week_end, quarter-boundary exact. NULL for PCT.',
    ytd_total            DOUBLE     COMMENT 'Year-to-date total through week_end. NULL for PCT.',
    qtd_target_avg_weekly DOUBLE    COMMENT 'Quarter-to-date target, average per 7 days (same channel_group).',
    qtd_target_total     DOUBLE     COMMENT 'Quarter-to-date target total. NULL for PCT.',
    qtd_vs_target        DOUBLE     COMMENT 'Quarter-to-date pacing vs target. COUNT/USD: qtd_total / qtd_target_total - 1 (ratio). PCT: qtd_avg_weekly - qtd_target_avg_weekly (points).',

    max_date             DATE       COMMENT 'Latest qgp_date with a non-null value for this metric in PulseTMS Gold at snapshot time (PulseTMS Gold max_date). For LOB rollups, the earliest max_date of the member LOBs.',
    is_data_current      BOOLEAN    COMMENT 'TRUE when max_date >= week_end, i.e. the source has reported through this week. FALSE flags reporting lag (common for QGP actuals).',

    source_validation_run_id STRING COMMENT 'Latest PulseTMS validation run (sdi_vw_dashboardPulseTms_validation_runs_perRun) for the same data_as_of_date as week_end. NULL if no validation ran for that week (e.g. backfills).',
    source_validation_status STRING COMMENT 'PulseTMS validation status (sdi_vw_dashboardPulseTms_validation_summary_perRun) for this row data source in that run: Healthy, Warning or Failed. NULL when no run exists.',

    created_ts           TIMESTAMP  COMMENT 'Row insert timestamp.'
  )
  USING DELTA
  CLUSTER BY (week_end, metric_key)
  COMMENT 'Weekly narrative report (project weeklyNarrative, separate from PulseTMS): append-only frozen snapshots of registry metrics read from PulseTMS Gold (sdi_vw_dashboardPulseTms_gold_unified_long); every row stamps source_project and source_object. One row per snapshot_id x metric_key x channel_group. Published reports pin a snapshot_id. Written by sdi_sp_weeklyNarrative_gold_factsHistory_long.';


  /* ===============================================================================================
     STEP 2 - SNAPSHOT
     =============================================================================================== */

  WITH

  Reg AS (
    SELECT *
    FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_control_metricRegistry_static
    WHERE  is_active = TRUE
      /* PulseTMS adapter: this procedure only reads PulseTMS Gold. Rows for any other
         source_object are ignored here and flagged by check 8 in 04_sdi_weeklyNarrative_validation_checks_adhoc.sql. */
      AND  source_project = 'PULSE_TMS'
      AND  source_object  = 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long'
  ),


  /* PulseTMS Gold for week_end and the 53 weeks before it (never after week_end). */
  GoldHist AS (
    SELECT
      data_source, metric_name, metric_type, lob, true_lob, channel_group,
      qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period,
      metric_value, wow_numerator, wow_denominator, wow_pct,
      yoy_numerator, yoy_denominator, yoy_pct, max_date
    FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long
    WHERE qgp_date BETWEEN date_sub(v_week_end, 371) AND v_week_end
      /* Only the PulseTMS data sources the registry uses. Keeps MFC_SPEND_GRANULAR (large,
         never reported) and PLATFORM_SPEND_CHANNEL out of the scan. */
      AND data_source IN (SELECT DISTINCT data_source FROM Reg)
  ),


  /* Every PulseTMS Gold row that belongs to a registry slice, for every date in the window.
       true_lob : single-LOB slice (NULL-safe) OR member of lob_rollup
       channel  : headline channel_group (HEADLINE / REFERENCE)
                  OR another channel allowed by driver_channel_like (DRIVER / REFERENCE_DRIVER) */
  Matched AS (
    SELECT
      r.metric_key,
      CASE
        WHEN g.channel_group <=> r.channel_group THEN CASE WHEN r.is_headline THEN 'HEADLINE' ELSE 'REFERENCE' END
        ELSE                                          CASE WHEN r.is_headline THEN 'DRIVER'   ELSE 'REFERENCE_DRIVER' END
      END                                                     AS row_role,
      CASE WHEN r.lob_rollup IS NOT NULL THEN TRUE ELSE FALSE END AS is_rollup,
      g.*
    FROM Reg r
    JOIN GoldHist g
      ON  g.data_source = r.data_source
      AND g.metric_name = r.metric_name
      AND g.metric_type <=> r.metric_type
      AND (
            (r.lob_rollup IS NULL     AND g.true_lob <=> r.true_lob)
         OR (r.lob_rollup IS NOT NULL AND array_contains(r.lob_rollup, g.true_lob))
          )
      AND (
            g.channel_group <=> r.channel_group
         OR (    r.include_channel_drivers = TRUE
             AND g.channel_group IS NOT NULL
             AND NOT (g.channel_group <=> r.channel_group)
             AND g.channel_group LIKE r.driver_channel_like)
          )
  ),


  /* One row per metric_key x channel_group x qgp_date. Single-LOB slices pass through
     (one Gold row each); lob_rollup slices are summed across their member LOBs. */
  Agg AS (
    SELECT
      metric_key,
      row_role,
      channel_group,
      qgp_date,
      MAX(is_rollup)                                                       AS is_rollup,
      MAX(week_type)                                                       AS week_type,
      MAX(qgp_quarter)                                                     AS qgp_quarter,
      MAX(days_in_period)                                                  AS days_in_period,
      MAX(CAST(is_complete_period AS INT)) = 1                             AS is_complete_period,
      NULLIF(array_join(sort_array(collect_set(lob)), ', '), '')           AS source_lob,
      NULLIF(array_join(sort_array(collect_set(true_lob)), ', '), '')      AS true_lob,
      COUNT(DISTINCT CASE WHEN metric_value IS NOT NULL THEN true_lob END) AS lobs_with_value,
      SUM(metric_value)                                                    AS metric_value,
      SUM(COALESCE(wow_numerator, metric_value))                           AS week_value,
      SUM(wow_numerator)                                                   AS wow_numerator,
      SUM(wow_denominator)                                                 AS prior_week_value,
      SUM(yoy_numerator)                                                   AS yoy_numerator,
      SUM(yoy_denominator)                                                 AS ly_week_value,
      MAX(wow_pct)                                                         AS source_wow_pct,
      MAX(yoy_pct)                                                         AS source_yoy_pct,
      MIN(max_date)                                                        AS max_date
    FROM Matched
    GROUP BY metric_key, row_role, channel_group, qgp_date
  ),


  /* Current week rows + registry attributes. WoW / YoY: PulseTMS values for single-LOB
     slices; recomputed from summed numerators / denominators for lob_rollup slices. */
  AllRows AS (
    SELECT
      r.registry_version, r.metric_key, r.display_name, r.report_section, r.report_lob, r.report_lob_label,
      r.sort_order, r.unit, r.higher_is_better, r.target_metric_key,
      r.source_project, r.source_object,
      r.data_source, r.metric_name, r.metric_type,
      CASE WHEN r.lob_rollup IS NOT NULL THEN array_join(r.lob_rollup, ' + ') END       AS lob_rollup_members,
      CASE WHEN r.lob_rollup IS NOT NULL THEN a.lobs_with_value = size(r.lob_rollup) END AS is_lob_rollup_complete,
      a.row_role, a.channel_group, a.week_type, a.qgp_quarter, a.days_in_period, a.is_complete_period,
      a.source_lob, a.true_lob,
      a.metric_value, a.week_value, a.prior_week_value, a.ly_week_value, a.max_date,
      CASE WHEN a.is_rollup THEN try_divide(a.wow_numerator, a.prior_week_value) - 1
           ELSE a.source_wow_pct END                                                    AS wow_pct,
      CASE WHEN a.is_rollup THEN try_divide(a.yoy_numerator, a.ly_week_value) - 1
           ELSE a.source_yoy_pct END                                                    AS yoy_pct
    FROM Agg a
    JOIN Reg r
      ON r.metric_key = a.metric_key
    WHERE a.qgp_date = v_week_end
  ),


  /* Target value for the same week and channel_group. */
  WithTarget AS (
    SELECT
      a.*,
      t.week_value AS target_week_value
    FROM AllRows a
    LEFT JOIN AllRows t
      ON  t.metric_key    = a.target_metric_key
      AND t.channel_group <=> a.channel_group
  ),


  /* Trailing and calendar to-date stats per metric_key x channel_group, from Agg history. */
  Stats AS (
    SELECT
      h.metric_key,
      h.channel_group,

      /* Trailing full weeks before week_end: Saturdays only, boundary-safe week value. */
      AVG(CASE WHEN DAYOFWEEK(h.qgp_date) = 7
                AND h.qgp_date BETWEEN date_sub(v_week_end, 28) AND date_sub(v_week_end, 7)
               THEN h.week_value END)                                  AS avg_prior_4wk,

      AVG(CASE WHEN DAYOFWEEK(h.qgp_date) = 7
                AND h.qgp_date BETWEEN date_sub(v_week_end, 91) AND date_sub(v_week_end, 7)
               THEN h.week_value END)                                  AS avg_prior_13wk,

      AVG(CASE WHEN DAYOFWEEK(h.qgp_date) = 7
                AND h.qgp_date BETWEEN date_sub(v_week_end, 364) AND date_sub(v_week_end, 7)
               THEN h.week_value END)                                  AS avg_prior_52wk,

      STDDEV_SAMP(CASE WHEN DAYOFWEEK(h.qgp_date) = 7
                        AND h.qgp_date BETWEEN date_sub(v_week_end, 91) AND date_sub(v_week_end, 7)
                       THEN h.week_value END)                          AS stddev_prior_13wk,

      COUNT(CASE WHEN DAYOFWEEK(h.qgp_date) = 7
                  AND h.qgp_date BETWEEN date_sub(v_week_end, 364) AND date_sub(v_week_end, 7)
                 THEN h.week_value END)                                AS prior_52wk_weeks,

      /* Calendar to-date: every QGP period in the window (exact at quarter / year boundaries). */
      SUM(CASE WHEN h.qgp_date >= trunc(v_week_end, 'MM')      THEN h.metric_value END)                    AS mtd_sum,
      SUM(CASE WHEN h.qgp_date >= trunc(v_week_end, 'MM')      AND h.metric_value IS NOT NULL THEN h.days_in_period END) AS mtd_days,
      SUM(CASE WHEN h.qgp_date >= trunc(v_week_end, 'MM')      THEN h.metric_value * h.days_in_period END) AS mtd_valdays,

      SUM(CASE WHEN h.qgp_date >= trunc(v_week_end, 'QUARTER') THEN h.metric_value END)                    AS qtd_sum,
      SUM(CASE WHEN h.qgp_date >= trunc(v_week_end, 'QUARTER') AND h.metric_value IS NOT NULL THEN h.days_in_period END) AS qtd_days,
      SUM(CASE WHEN h.qgp_date >= trunc(v_week_end, 'QUARTER') THEN h.metric_value * h.days_in_period END) AS qtd_valdays,

      SUM(CASE WHEN h.qgp_date >= trunc(v_week_end, 'YEAR')    THEN h.metric_value END)                    AS ytd_sum,
      SUM(CASE WHEN h.qgp_date >= trunc(v_week_end, 'YEAR')    AND h.metric_value IS NOT NULL THEN h.days_in_period END) AS ytd_days,
      SUM(CASE WHEN h.qgp_date >= trunc(v_week_end, 'YEAR')    THEN h.metric_value * h.days_in_period END) AS ytd_valdays

    FROM Agg h
    GROUP BY h.metric_key, h.channel_group
  ),


  /* Unit-aware averages and totals, computed once and reused for the row and its target. */
  StatsFinal AS (
    SELECT
      s.*,
      CASE WHEN r.unit = 'PCT' THEN try_divide(s.mtd_valdays, s.mtd_days) ELSE try_divide(s.mtd_sum, s.mtd_days / 7.0) END AS mtd_avg_weekly,
      CASE WHEN r.unit = 'PCT' THEN try_divide(s.qtd_valdays, s.qtd_days) ELSE try_divide(s.qtd_sum, s.qtd_days / 7.0) END AS qtd_avg_weekly,
      CASE WHEN r.unit = 'PCT' THEN try_divide(s.ytd_valdays, s.ytd_days) ELSE try_divide(s.ytd_sum, s.ytd_days / 7.0) END AS ytd_avg_weekly,
      CASE WHEN r.unit = 'PCT' THEN CAST(NULL AS DOUBLE) ELSE s.mtd_sum END AS mtd_total,
      CASE WHEN r.unit = 'PCT' THEN CAST(NULL AS DOUBLE) ELSE s.qtd_sum END AS qtd_total,
      CASE WHEN r.unit = 'PCT' THEN CAST(NULL AS DOUBLE) ELSE s.ytd_sum END AS ytd_total
    FROM Stats s
    JOIN Reg r
      ON r.metric_key = s.metric_key
  ),


  /* PulseTMS validation status per data source for this week (if a run exists). */
  ValidationBySource AS (
    SELECT
      v.data_source    AS validation_data_source,
      v.overall_status AS validation_status
    FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_validation_summary_perRun v
    WHERE v.validation_run_id = v_validation_run_id
      AND v.summary_level     = 'DATA_SOURCE'
  )


  INSERT INTO
    prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_factsHistory_long
  (
    snapshot_id, snapshot_ts, run_type, job_run_id,
    week_end, week_type, qgp_quarter, days_in_period, is_complete_period,
    registry_version, metric_key, display_name, report_section, report_lob, report_lob_label, sort_order, row_role,
    source_project, source_object,
    data_source, metric_name, metric_type, source_lob, true_lob,
    lob_rollup_members, is_lob_rollup_complete,
    channel_group,
    unit, higher_is_better,
    metric_value, week_value, prior_week_value, wow_pct, ly_week_value, yoy_pct,
    target_metric_key, target_week_value, vs_target_abs, vs_target_pct, is_target_same_as_actual,
    avg_prior_4wk, avg_prior_13wk, avg_prior_52wk, stddev_prior_13wk, prior_52wk_weeks,
    vs_avg_prior_4wk, vs_avg_prior_13wk, vs_avg_prior_52wk, zscore_prior_13wk,
    mtd_avg_weekly, qtd_avg_weekly, ytd_avg_weekly, mtd_total, qtd_total, ytd_total,
    qtd_target_avg_weekly, qtd_target_total, qtd_vs_target,
    max_date, is_data_current,
    source_validation_run_id, source_validation_status,
    created_ts
  )

  SELECT
    v_snapshot_id,
    v_ts,
    v_run_type,
    v_job_run_id,

    v_week_end,
    w.week_type,
    w.qgp_quarter,
    CAST(w.days_in_period AS INT),
    w.is_complete_period,

    w.registry_version,
    w.metric_key,
    w.display_name,
    w.report_section,
    w.report_lob,
    w.report_lob_label,
    CAST(w.sort_order AS INT),
    w.row_role,

    w.source_project,
    w.source_object,

    w.data_source,
    w.metric_name,
    w.metric_type,
    w.source_lob,
    w.true_lob,
    w.lob_rollup_members,
    w.is_lob_rollup_complete,
    w.channel_group,

    w.unit,
    w.higher_is_better,

    CAST(w.metric_value      AS DOUBLE),
    CAST(w.week_value        AS DOUBLE),
    CAST(w.prior_week_value  AS DOUBLE),
    CAST(w.wow_pct           AS DOUBLE),
    CAST(w.ly_week_value     AS DOUBLE),
    CAST(w.yoy_pct           AS DOUBLE),

    w.target_metric_key,
    CAST(w.target_week_value AS DOUBLE),
    CAST(w.week_value - w.target_week_value AS DOUBLE),
    CASE
      WHEN w.unit = 'PCT' THEN CAST(NULL AS DOUBLE)
      ELSE CAST(try_divide(w.week_value, w.target_week_value) - 1 AS DOUBLE)
    END,
    CASE
      WHEN w.target_week_value IS NULL OR w.week_value IS NULL THEN CAST(NULL AS BOOLEAN)
      /* Relative tolerance: the PulseTMS fallback copies the actual exactly, so only float
         rounding from summing in a different order should be tolerated. A fixed 0.005 would
         wrongly flag a 0.4 pt gap on PCT (ratio) metrics. */
      ELSE ABS(w.week_value - w.target_week_value)
           <= 1e-7 * GREATEST(ABS(w.week_value), ABS(w.target_week_value), 1.0)
    END,

    CAST(s.avg_prior_4wk     AS DOUBLE),
    CAST(s.avg_prior_13wk    AS DOUBLE),
    CAST(s.avg_prior_52wk    AS DOUBLE),
    CAST(s.stddev_prior_13wk AS DOUBLE),
    CAST(s.prior_52wk_weeks  AS INT),

    CAST(CASE WHEN w.unit = 'PCT' THEN w.week_value - s.avg_prior_4wk
              ELSE try_divide(w.week_value, s.avg_prior_4wk) - 1 END  AS DOUBLE),
    CAST(CASE WHEN w.unit = 'PCT' THEN w.week_value - s.avg_prior_13wk
              ELSE try_divide(w.week_value, s.avg_prior_13wk) - 1 END AS DOUBLE),
    CAST(CASE WHEN w.unit = 'PCT' THEN w.week_value - s.avg_prior_52wk
              ELSE try_divide(w.week_value, s.avg_prior_52wk) - 1 END AS DOUBLE),
    CAST(try_divide(w.week_value - s.avg_prior_13wk, s.stddev_prior_13wk) AS DOUBLE),

    CAST(s.mtd_avg_weekly AS DOUBLE),
    CAST(s.qtd_avg_weekly AS DOUBLE),
    CAST(s.ytd_avg_weekly AS DOUBLE),
    CAST(s.mtd_total      AS DOUBLE),
    CAST(s.qtd_total      AS DOUBLE),
    CAST(s.ytd_total      AS DOUBLE),

    CAST(ts.qtd_avg_weekly AS DOUBLE),
    CAST(ts.qtd_total      AS DOUBLE),
    CAST(CASE WHEN w.unit = 'PCT' THEN s.qtd_avg_weekly - ts.qtd_avg_weekly
              ELSE try_divide(s.qtd_total, ts.qtd_total) - 1 END AS DOUBLE),

    CAST(w.max_date AS DATE),
    CAST(w.max_date AS DATE) >= v_week_end,

    v_validation_run_id,
    vs.validation_status,

    CURRENT_TIMESTAMP()

  FROM WithTarget w

  LEFT JOIN StatsFinal s
    ON  s.metric_key    = w.metric_key
    AND s.channel_group <=> w.channel_group

  LEFT JOIN StatsFinal ts
    ON  ts.metric_key    = w.target_metric_key
    AND ts.channel_group <=> w.channel_group

  LEFT JOIN ValidationBySource vs
    ON vs.validation_data_source =
       CASE w.data_source
         WHEN 'MFC_SPEND_CHANNEL'      THEN 'MFC_SPEND'
         WHEN 'MFC_SPEND_GRANULAR'     THEN 'MFC_SPEND'
         WHEN 'PLATFORM_SPEND_CHANNEL' THEN 'PLATFORM_SPEND'
         WHEN 'BIDDABLE_SPEND_CHANNEL' THEN 'BIDDABLE_SPEND'
         ELSE w.data_source                -- ADOBE, QGP_SCORECARD, UPV_FORECAST match as-is
       END
  ;


  /* ===============================================================================================
     STEP 3 - GUARD
     =============================================================================================== */

  SET v_row_count =
  (
    SELECT COUNT(*)
    FROM   prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_factsHistory_long
    WHERE  snapshot_id = v_snapshot_id
  );


  IF v_row_count = 0 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT =
        'Weekly narrative facts snapshot wrote 0 rows. Gold has no registry metrics for the requested week.';
  END IF;

END;