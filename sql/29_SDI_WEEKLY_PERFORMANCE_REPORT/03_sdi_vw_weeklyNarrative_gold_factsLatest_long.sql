/* =================================================================================================
FILE:           03_sdi_vw_weeklyNarrative_gold_factsLatest_long.sql
PLATFORM:       Databricks
LAYER:          Gold view
CATALOG.SCHEMA: prdrzranalytics.lab42
VIEW:           sdi_vw_weeklyNarrative_gold_factsLatest_long

PURPOSE:
  Newest facts snapshot per reporting week (one snapshot_id per week_end).

  Used while a week is being DRAFTED. Once a report is published it must read its pinned
  snapshot_id from sdi_tbl_weeklyNarrative_gold_factsHistory_long, never this view, so a
  later re-snapshot cannot change a published week.

GRAIN:
  week_end x metric_key x channel_group  (for the latest snapshot of that week)

METADATA:
  Column comments mirror sdi_tbl_weeklyNarrative_gold_factsHistory_long. The column list
  is explicit (no h.*) so a new history column must be added here deliberately.

================================================================================================= */

CREATE OR REPLACE VIEW
  prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_factsLatest_long
(
  snapshot_id          COMMENT 'Snapshot identifier of the newest snapshot for this week_end.',
  snapshot_ts          COMMENT 'When the snapshot was taken (UTC).',
  run_type             COMMENT 'JOB = scheduled Databricks Job run. MANUAL = interactive or ad hoc call (including backfills).',
  job_run_id           COMMENT 'Databricks Job Run ID for JOB runs; the snapshot_id for MANUAL runs.',

  week_end             COMMENT 'Reporting week end date. Always a Saturday; the week runs Sunday to Saturday.',
  week_type            COMMENT 'From the PulseTMS QGP calendar: NORMAL or BOUNDARY_FIRST.',
  qgp_quarter          COMMENT 'QGP quarter label of week_end, e.g. 2026 Q4.',
  days_in_period       COMMENT 'Days in the QGP period ending on week_end: 7 for NORMAL, fewer for BOUNDARY_FIRST.',
  is_complete_period   COMMENT 'Calendar completeness flag. Does NOT guarantee every source has settled; see is_data_current and source_validation_status.',

  registry_version     COMMENT 'registry_version of the metric registry used for this snapshot.',
  metric_key           COMMENT 'Registry primary key of the metric slice.',
  display_name         COMMENT 'Report-facing metric name.',
  report_section       COMMENT 'Report section display label (Traffic, Funnel, Spend, Activations, Retail, Virtual Retail).',
  metric_lob           COMMENT 'LOB(s) the metric covers, from the registry: one LOB (POSTPAID, BROADBAND, BYOD, FIBER, TFB, METRO) or several joined with + (e.g. POSTPAID + BROADBAND). NULL = metric has no LOB (e.g. total site UPV, store traffic). Group the report by this.',
  sort_order           COMMENT 'Report display order.',
  row_role             COMMENT 'HEADLINE, DRIVER (other channel_group of a headline metric), REFERENCE (forecast or target), REFERENCE_DRIVER (channel-level forecast or target).',

  source_project       COMMENT 'Code of the upstream project the numbers come from: PULSE_TMS.',
  source_object        COMMENT 'Upstream object read: prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long (PulseTMS Gold).',

  data_source          COMMENT 'PulseTMS Gold data_source the row came from.',
  metric_name          COMMENT 'PulseTMS Gold metric_name the row came from.',
  metric_type          COMMENT 'PulseTMS Gold metric_type the row came from.',
  source_lob           COMMENT 'PulseTMS Gold true_lob value(s) read. NULL = source has no LOB. Several joined with + = summed total.',
  is_lob_rollup_complete COMMENT 'Only for summed totals (several source_lob values): FALSE = a LOB is missing this week, so the total is partial. NULL otherwise.',
  channel_group        COMMENT 'PulseTMS Gold channel_group. NULL for QGP_SCORECARD.',

  unit                 COMMENT 'COUNT | USD | PCT. PCT values are ratios (0.31 = 31%).',
  higher_is_better     COMMENT 'Direction of good performance. NULL = neutral.',

  metric_value         COMMENT 'PulseTMS Gold metric_value for the QGP period, unchanged. Partial on BOUNDARY_FIRST weeks; report on week_value.',
  week_value           COMMENT 'Full Sunday-Saturday week value = COALESCE(wow_numerator, metric_value). The number the report uses.',
  prior_week_value     COMMENT 'Prior full week value used for WoW (PulseTMS Gold wow_denominator).',
  wow_pct              COMMENT 'Week-over-week change as a ratio (0.12 = +12%).',
  ly_week_value        COMMENT 'Same ISO week last year, full weekly total (PulseTMS Gold yoy_denominator).',
  yoy_pct              COMMENT 'Year-over-year change as a ratio. NULL where prior-year history does not exist.',

  target_metric_key    COMMENT 'Registry key of the target: the forecast for MFC spend and UPV, the QGP target for QGP metrics. NULL if none.',
  target_week_value    COMMENT 'Target for the same week_end and channel_group.',
  vs_target_abs        COMMENT 'Actual minus target. For unit = PCT, the gap in points as a ratio.',
  vs_target_pct        COMMENT 'Actual vs target as a ratio (-0.03 = 3% below target). NULL for unit = PCT.',
  is_target_same_as_actual COMMENT 'TRUE = target equals actual exactly (PulseTMS MFC fills a missing forecast with the actual). Say no target was available, not on target.',

  avg_prior_4wk        COMMENT 'Average weekly value of the 4 full weeks before week_end.',
  avg_prior_13wk       COMMENT 'Average weekly value of the 13 full weeks before week_end.',
  avg_prior_52wk       COMMENT 'Average weekly value of the 52 full weeks before week_end.',
  stddev_prior_13wk    COMMENT 'Standard deviation of weekly values over the prior 13 weeks: normal weekly swing.',
  prior_52wk_weeks     COMMENT 'Weeks with data in the 52-week window. Below 52 = short history.',
  vs_avg_prior_4wk     COMMENT 'week_value vs 4-week average. Ratio for COUNT/USD; points for PCT.',
  vs_avg_prior_13wk    COMMENT 'week_value vs 13-week average. Ratio for COUNT/USD; points for PCT.',
  vs_avg_prior_52wk    COMMENT 'week_value vs 52-week average. Ratio for COUNT/USD; points for PCT.',
  zscore_prior_13wk    COMMENT 'How unusual this week is versus normal swing. |z| >= 2 is a strong highlight candidate.',

  mtd_avg_weekly       COMMENT 'Month-to-date average per 7 days (weeks ending in the month). PCT: day-weighted average ratio.',
  qtd_avg_weekly       COMMENT 'QGP quarter-to-date average per 7 days. PCT: day-weighted average ratio.',
  ytd_avg_weekly       COMMENT 'Year-to-date average per 7 days. PCT: day-weighted average ratio.',
  mtd_total            COMMENT 'Month-to-date total. NULL for PCT.',
  qtd_total            COMMENT 'QGP quarter-to-date total. NULL for PCT.',
  ytd_total            COMMENT 'Year-to-date total. NULL for PCT.',
  qtd_target_avg_weekly COMMENT 'Quarter-to-date target, average per 7 days.',
  qtd_target_total     COMMENT 'Quarter-to-date target total. NULL for PCT.',
  qtd_vs_target        COMMENT 'Quarter-to-date pacing vs target. Ratio for COUNT/USD; points for PCT.',

  max_date             COMMENT 'Latest qgp_date with a value for this metric in PulseTMS Gold at snapshot time.',
  is_data_current      COMMENT 'TRUE when the source has reported through week_end. FALSE flags reporting lag.',

  source_validation_run_id COMMENT 'PulseTMS validation run for the same week, if any.',
  source_validation_status COMMENT 'PulseTMS validation status for this data source: Healthy, Warning, Failed, or NULL if no run.',

  created_ts           COMMENT 'Row insert timestamp.'
)
COMMENT 'Weekly narrative report (project weeklyNarrative, numbers sourced from PulseTMS Gold): newest facts snapshot per Sunday-Saturday week from sdi_tbl_weeklyNarrative_gold_factsHistory_long. For drafting only; published reports read their pinned snapshot_id from the history table.'
AS

SELECT
  h.snapshot_id,
  h.snapshot_ts,
  h.run_type,
  h.job_run_id,

  h.week_end,
  h.week_type,
  h.qgp_quarter,
  h.days_in_period,
  h.is_complete_period,

  h.registry_version,
  h.metric_key,
  h.display_name,
  h.report_section,
  h.metric_lob,
  h.sort_order,
  h.row_role,

  h.source_project,
  h.source_object,

  h.data_source,
  h.metric_name,
  h.metric_type,
  h.source_lob,
  h.is_lob_rollup_complete,
  h.channel_group,

  h.unit,
  h.higher_is_better,

  h.metric_value,
  h.week_value,
  h.prior_week_value,
  h.wow_pct,
  h.ly_week_value,
  h.yoy_pct,

  h.target_metric_key,
  h.target_week_value,
  h.vs_target_abs,
  h.vs_target_pct,
  h.is_target_same_as_actual,

  h.avg_prior_4wk,
  h.avg_prior_13wk,
  h.avg_prior_52wk,
  h.stddev_prior_13wk,
  h.prior_52wk_weeks,
  h.vs_avg_prior_4wk,
  h.vs_avg_prior_13wk,
  h.vs_avg_prior_52wk,
  h.zscore_prior_13wk,

  h.mtd_avg_weekly,
  h.qtd_avg_weekly,
  h.ytd_avg_weekly,
  h.mtd_total,
  h.qtd_total,
  h.ytd_total,
  h.qtd_target_avg_weekly,
  h.qtd_target_total,
  h.qtd_vs_target,

  h.max_date,
  h.is_data_current,

  h.source_validation_run_id,
  h.source_validation_status,

  h.created_ts

FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_factsHistory_long h

QUALIFY
  DENSE_RANK() OVER (
    PARTITION BY h.week_end
    ORDER BY h.snapshot_ts DESC, h.snapshot_id DESC
  ) = 1
;