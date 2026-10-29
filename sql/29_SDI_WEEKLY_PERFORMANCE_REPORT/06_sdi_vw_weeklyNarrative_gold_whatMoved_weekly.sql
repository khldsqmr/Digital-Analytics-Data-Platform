/* =================================================================================================
FILE:           06_sdi_vw_weeklyNarrative_gold_whatMoved_weekly.sql
PLATFORM:       Databricks
LAYER:          Gold view
CATALOG.SCHEMA: prdrzranalytics.lab42
VIEW:           sdi_vw_weeklyNarrative_gold_whatMoved_weekly

PURPOSE:
  Decides which metrics are worth talking about each week. The history table holds every
  number; this view applies fixed, explainable rules on top of it and ranks the highlights.
  It creates no new numbers.

  The AI explains the highlights; it does not decide what is news. Because the rules are SQL,
  the manager can see why something was picked, thresholds are tuned in one place
  (Thresholds CTE below), and the backtest can check the rules against past reports.

SOURCE:
  sdi_vw_weeklyNarrative_gold_factsLatest_weekly (newest snapshot per week).
  For a published report, the same rules must be applied to its pinned snapshot_id.

GRAIN:
  week_end x metric_key, HEADLINE rows only (one row per reportable metric per week,
  highlighted or not, so it is always visible why a metric was or was not picked).

RULES (each scored so that 1.0 = exactly at threshold):
  UNUSUAL_MOVE      |zscore_prior_13wk| >= 2.0
  BIG_WOW           |WoW| >= 10%  (PCT metrics: >= 2 pts)
  OFF_TARGET        |actual vs target| >= 5%  (PCT: >= 1 pt)
  QTD_OFF_PACE      |QTD vs target| >= 3%  (PCT: >= 0.5 pt)
  NEW_13WK_HIGH     week_value above every one of the previous 13 weeks
  NEW_13WK_LOW      week_value below every one of the previous 13 weeks
  highlight_score = the strongest of the scores above.

  Target rules are skipped when is_target_same_as_actual = TRUE (PulseTMS MFC forecast
  fallback): there was no real target that week.

DATA QUALITY GATE (row is never highlighted, the reason is shown in data_quality_issue;
first matching reason wins):
  NO_VALUE                  no value for this week
  PARTIAL_LOB_TOTAL         a member LOB of a rollup total is missing
  DATA_NOT_CURRENT          source has not reported through week_end (e.g. QGP lag)
  SOURCE_VALIDATION_FAILED  PulseTMS validation status Failed for the data source

TOP DRIVER:
  For metrics with channel DRIVER rows, the channel with the largest week-over-week change
  in the same direction as the headline (falls back to the largest change in any direction).
  top_driver_share_of_change = that channel change / headline change. Adobe channel_groups
  plus All Channels may not sum exactly, so treat the share as indicative.

NAMING CONVENTION:
  Codes UPPER_SNAKE (highlight_reasons, direction, target_position, *_sentiment,
  data_quality_issue). Labels come from the registry (display_name, report_lob_label).

================================================================================================= */

CREATE OR REPLACE VIEW
  prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_whatMoved_weekly
(
  week_end                    COMMENT 'Reporting week end date (Saturday). The week runs Sunday to Saturday.',
  snapshot_id                 COMMENT 'Facts snapshot these numbers come from (newest snapshot for the week).',
  week_type                   COMMENT 'NORMAL or BOUNDARY_FIRST, from the PulseTMS QGP calendar.',

  metric_key                  COMMENT 'Registry key of the metric, e.g. pulseTms.upvTotalAdobe.',
  display_name                COMMENT 'Metric name to use in the narrative (sentence case).',
  report_section              COMMENT 'Report section label (Traffic, Funnel, Spend, Activations, Retail, Virtual Retail).',
  report_lob                  COMMENT 'LOB code: POSTPAID, BROADBAND, BYOD, FIBER, TFB, METRO, TOTAL, NOT_LOB_SPECIFIC.',
  report_lob_label            COMMENT 'LOB label to use in the narrative (Postpaid, Broadband, ...).',
  sort_order                  COMMENT 'Registry display order.',
  unit                        COMMENT 'COUNT | USD | PCT. PCT values are ratios (0.31 = 31%).',
  higher_is_better            COMMENT 'TRUE = increase is good, FALSE = decrease is good, NULL = neutral.',
  channel_group               COMMENT 'Headline channel_group (All Channels, or NULL for QGP).',

  week_value                  COMMENT 'This week (full Sunday-Saturday week).',
  prior_week_value            COMMENT 'Previous full week.',
  wow_change_abs              COMMENT 'week_value - prior_week_value (for PCT, the change in points as a ratio).',
  wow_pct                     COMMENT 'Week-over-week change as a ratio (0.12 = +12%). For PCT metrics use wow_change_abs (points).',
  yoy_pct                     COMMENT 'Year-over-year change as a ratio. NULL where no prior-year history.',

  target_metric_key           COMMENT 'Registry key of the target (forecast for MFC / UPV, QGP target for QGP). NULL if none.',
  target_week_value           COMMENT 'Target for this week.',
  vs_target_abs               COMMENT 'Actual minus target (PCT: points as a ratio).',
  vs_target_pct               COMMENT 'Actual vs target as a ratio. NULL for PCT.',
  is_target_same_as_actual    COMMENT 'TRUE = no real target this week (PulseTMS MFC forecast fallback). Target rules are skipped.',
  qtd_vs_target               COMMENT 'Quarter-to-date pacing vs target (ratio; points for PCT).',

  avg_prior_13wk              COMMENT 'Average of the 13 weeks before this one.',
  zscore_prior_13wk           COMMENT 'How unusual this week is versus the normal 13-week swing.',
  max_prior_13wk              COMMENT 'Highest weekly value in the 13 weeks before this one (from earlier snapshots).',
  min_prior_13wk              COMMENT 'Lowest weekly value in the 13 weeks before this one.',
  prior_13wk_weeks_seen       COMMENT 'How many of the 13 prior weeks have a snapshot with a value. Highs / lows are only flagged when all 13 are present.',
  is_13wk_high                COMMENT 'TRUE when this week is above every one of the previous 13 weeks.',
  is_13wk_low                 COMMENT 'TRUE when this week is below every one of the previous 13 weeks.',

  top_driver_channel          COMMENT 'Channel with the largest WoW change, preferring the same direction as the headline. NULL when the metric has no channel drivers (e.g. QGP).',
  top_driver_wow_change       COMMENT 'That channel week-over-week change (same unit as the metric).',
  top_driver_share_of_change  COMMENT 'top_driver_wow_change / headline wow_change_abs. Indicative only (channels may not sum exactly to All Channels).',

  direction                   COMMENT 'UP, DOWN or FLAT versus the previous week.',
  wow_sentiment               COMMENT 'GOOD, BAD or NEUTRAL: direction combined with higher_is_better.',
  target_position             COMMENT 'ABOVE_TARGET, BELOW_TARGET, ON_TARGET, or NULL when there is no usable target.',
  target_sentiment            COMMENT 'GOOD, BAD or NEUTRAL: target_position combined with higher_is_better.',

  data_quality_issue          COMMENT 'NULL when the row is usable. Otherwise NO_VALUE, PARTIAL_LOB_TOTAL, DATA_NOT_CURRENT or SOURCE_VALIDATION_FAILED (first match wins); the row is never highlighted.',
  source_validation_status    COMMENT 'PulseTMS validation status for the data source (Healthy, Warning, Failed, or NULL). Warning does not block a highlight but should be caveated.',

  highlight_reasons           COMMENT 'Rules this metric triggered: UNUSUAL_MOVE, BIG_WOW, OFF_TARGET, QTD_OFF_PACE, NEW_13WK_HIGH, NEW_13WK_LOW. Empty when none.',
  highlight_score             COMMENT 'Strongest rule score; 1.0 = exactly at a threshold, 2.0 = twice the threshold. Used for ranking.',
  is_highlight                COMMENT 'TRUE when no data quality issue and highlight_score >= 1.0.',
  highlight_rank              COMMENT 'Rank among this week highlights, 1 = most newsworthy. NULL when not a highlight.',
  section_highlight_rank      COMMENT 'Rank among this week highlights within the same report_section. NULL when not a highlight.'
)
COMMENT 'Weekly narrative report: rule-based highlight picker over sdi_vw_weeklyNarrative_gold_factsLatest_weekly (numbers from PulseTMS Gold). One row per headline metric per week with the rules it triggered, a score, data quality gate and top channel driver. Thresholds live in the Thresholds CTE.'
AS

WITH

/* ===============================================================================================
   THRESHOLDS: the only place to tune what counts as news.
   =============================================================================================== */

Thresholds AS (
  SELECT
    2.0   AS z_threshold,               -- |z| vs 13-week normal swing
    0.10  AS wow_pct_threshold,         -- COUNT / USD: 10% week over week
    0.02  AS wow_pts_threshold,         -- PCT: 2 points week over week
    0.05  AS target_pct_threshold,      -- COUNT / USD: 5% off target
    0.01  AS target_pts_threshold,      -- PCT: 1 point off target
    0.03  AS qtd_pct_threshold,         -- COUNT / USD: 3% QTD off pace
    0.005 AS qtd_pts_threshold          -- PCT: 0.5 point QTD off pace
),


/* ===============================================================================================
   HEADLINE ROWS + 13-WEEK HIGH / LOW FROM EARLIER SNAPSHOTS
   =============================================================================================== */

Headline AS (
  SELECT
    f.*,
    MAX(f.week_value)   OVER w13 AS max_prior_13wk,
    MIN(f.week_value)   OVER w13 AS min_prior_13wk,
    COUNT(f.week_value) OVER w13 AS prior_13wk_weeks_seen
  FROM prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_factsLatest_weekly f
  WHERE f.row_role = 'HEADLINE'
  WINDOW w13 AS (
    PARTITION BY f.metric_key
    ORDER BY unix_date(f.week_end)            -- days since epoch, so the frame is in days
    RANGE BETWEEN 91 PRECEDING AND 7 PRECEDING  -- the 13 Saturdays before week_end
  )
),


/* ===============================================================================================
   TOP CHANNEL DRIVER
   =============================================================================================== */

Drivers AS (
  SELECT
    d.week_end,
    d.metric_key,
    d.channel_group,
    d.week_value - d.prior_week_value AS wow_change
  FROM prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_factsLatest_weekly d
  WHERE d.row_role = 'DRIVER'
    AND d.week_value       IS NOT NULL
    AND d.prior_week_value IS NOT NULL
),


TopDriver AS (
  SELECT
    d.week_end,
    d.metric_key,
    d.channel_group AS top_driver_channel,
    d.wow_change    AS top_driver_wow_change
  FROM Drivers d
  JOIN Headline h
    ON  h.week_end   = d.week_end
    AND h.metric_key = d.metric_key
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY d.week_end, d.metric_key
    ORDER BY
      CASE WHEN SIGN(d.wow_change) = SIGN(h.week_value - h.prior_week_value) THEN 0 ELSE 1 END,
      ABS(d.wow_change) DESC,
      d.channel_group
  ) = 1
),


/* ===============================================================================================
   SIGNALS
   =============================================================================================== */

Signals AS (
  SELECT
    h.*,
    td.top_driver_channel,
    td.top_driver_wow_change,

    h.week_value - h.prior_week_value AS wow_change_abs,

    CASE
      WHEN h.week_value IS NULL                    THEN 'NO_VALUE'
      WHEN h.is_lob_rollup_complete = FALSE        THEN 'PARTIAL_LOB_TOTAL'
      WHEN h.is_data_current = FALSE               THEN 'DATA_NOT_CURRENT'
      WHEN h.source_validation_status = 'Failed'   THEN 'SOURCE_VALIDATION_FAILED'
    END AS data_quality_issue,

    /* A target only counts if it exists and is not the MFC fallback copy of the actual. */
    (h.target_week_value IS NOT NULL AND COALESCE(h.is_target_same_as_actual, FALSE) = FALSE) AS has_usable_target,

    /* 13-week high / low only with a full 13 weeks of earlier snapshots. */
    (h.prior_13wk_weeks_seen >= 13 AND h.week_value > h.max_prior_13wk) AS is_13wk_high,
    (h.prior_13wk_weeks_seen >= 13 AND h.week_value < h.min_prior_13wk) AS is_13wk_low,

    /* Rule scores: 1.0 = exactly at threshold. */
    ABS(h.zscore_prior_13wk) / t.z_threshold AS s_unusual,

    CASE
      WHEN h.unit = 'PCT' THEN ABS(h.week_value - h.prior_week_value) / t.wow_pts_threshold
      ELSE                     ABS(h.wow_pct) / t.wow_pct_threshold
    END AS s_wow,

    CASE
      WHEN h.target_week_value IS NULL OR COALESCE(h.is_target_same_as_actual, FALSE) THEN CAST(NULL AS DOUBLE)
      WHEN h.unit = 'PCT' THEN ABS(h.vs_target_abs) / t.target_pts_threshold
      ELSE                     ABS(h.vs_target_pct) / t.target_pct_threshold
    END AS s_target,

    CASE
      WHEN h.target_week_value IS NULL OR COALESCE(h.is_target_same_as_actual, FALSE) THEN CAST(NULL AS DOUBLE)
      WHEN h.unit = 'PCT' THEN ABS(h.qtd_vs_target) / t.qtd_pts_threshold
      ELSE                     ABS(h.qtd_vs_target) / t.qtd_pct_threshold
    END AS s_qtd

  FROM Headline h
  CROSS JOIN Thresholds t
  LEFT JOIN TopDriver td
    ON  td.week_end   = h.week_end
    AND td.metric_key = h.metric_key
),


Scored AS (
  SELECT
    s.*,

    filter(
      array(
        CASE WHEN s.s_unusual >= 1 THEN 'UNUSUAL_MOVE'  END,
        CASE WHEN s.s_wow     >= 1 THEN 'BIG_WOW'       END,
        CASE WHEN s.s_target  >= 1 THEN 'OFF_TARGET'    END,
        CASE WHEN s.s_qtd     >= 1 THEN 'QTD_OFF_PACE'  END,
        CASE WHEN s.is_13wk_high   THEN 'NEW_13WK_HIGH' END,
        CASE WHEN s.is_13wk_low    THEN 'NEW_13WK_LOW'  END
      ),
      x -> x IS NOT NULL
    ) AS highlight_reasons,

    GREATEST(
      COALESCE(s.s_unusual, 0),
      COALESCE(s.s_wow,     0),
      COALESCE(s.s_target,  0),
      COALESCE(s.s_qtd,     0),
      CASE WHEN s.is_13wk_high OR s.is_13wk_low THEN 1.0 ELSE 0 END
    ) AS highlight_score,

    CASE
      WHEN s.week_value IS NULL OR s.prior_week_value IS NULL THEN CAST(NULL AS STRING)
      WHEN s.week_value > s.prior_week_value                  THEN 'UP'
      WHEN s.week_value < s.prior_week_value                  THEN 'DOWN'
      ELSE 'FLAT'
    END AS direction,

    CASE
      WHEN NOT s.has_usable_target OR s.week_value IS NULL     THEN CAST(NULL AS STRING)
      WHEN s.week_value > s.target_week_value                  THEN 'ABOVE_TARGET'
      WHEN s.week_value < s.target_week_value                  THEN 'BELOW_TARGET'
      ELSE 'ON_TARGET'
    END AS target_position

  FROM Signals s
),


Final AS (
  SELECT
    sc.*,

    CASE
      WHEN sc.direction IS NULL OR sc.direction = 'FLAT' OR sc.higher_is_better IS NULL THEN 'NEUTRAL'
      WHEN (sc.direction = 'UP') = sc.higher_is_better                                  THEN 'GOOD'
      ELSE 'BAD'
    END AS wow_sentiment,

    CASE
      WHEN sc.target_position IS NULL OR sc.target_position = 'ON_TARGET' OR sc.higher_is_better IS NULL THEN 'NEUTRAL'
      WHEN (sc.target_position = 'ABOVE_TARGET') = sc.higher_is_better                                   THEN 'GOOD'
      ELSE 'BAD'
    END AS target_sentiment,

    (sc.data_quality_issue IS NULL AND sc.highlight_score >= 1.0) AS is_highlight

  FROM Scored sc
)


SELECT
  f.week_end,
  f.snapshot_id,
  f.week_type,

  f.metric_key,
  f.display_name,
  f.report_section,
  f.report_lob,
  f.report_lob_label,
  f.sort_order,
  f.unit,
  f.higher_is_better,
  f.channel_group,

  f.week_value,
  f.prior_week_value,
  f.wow_change_abs,
  f.wow_pct,
  f.yoy_pct,

  f.target_metric_key,
  f.target_week_value,
  f.vs_target_abs,
  f.vs_target_pct,
  f.is_target_same_as_actual,
  f.qtd_vs_target,

  f.avg_prior_13wk,
  f.zscore_prior_13wk,
  f.max_prior_13wk,
  f.min_prior_13wk,
  f.prior_13wk_weeks_seen,
  f.is_13wk_high,
  f.is_13wk_low,

  f.top_driver_channel,
  f.top_driver_wow_change,
  try_divide(f.top_driver_wow_change, f.wow_change_abs) AS top_driver_share_of_change,

  f.direction,
  f.wow_sentiment,
  f.target_position,
  f.target_sentiment,

  f.data_quality_issue,
  f.source_validation_status,

  f.highlight_reasons,
  f.highlight_score,
  f.is_highlight,

  CASE WHEN f.is_highlight THEN
    ROW_NUMBER() OVER (
      PARTITION BY f.week_end
      ORDER BY CASE WHEN f.is_highlight THEN 0 ELSE 1 END, f.highlight_score DESC, f.sort_order
    )
  END AS highlight_rank,

  CASE WHEN f.is_highlight THEN
    ROW_NUMBER() OVER (
      PARTITION BY f.week_end, f.report_section
      ORDER BY CASE WHEN f.is_highlight THEN 0 ELSE 1 END, f.highlight_score DESC, f.sort_order
    )
  END AS section_highlight_rank

FROM Final f
;