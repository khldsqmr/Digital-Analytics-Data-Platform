/*
===============================================================================
FILE: 05_sp_gold_campaign_weekly_business_logic.sql
LAYER: Gold QA (Business Logic)

PURPOSE:
  Validate QGP week bucketing rules used for Gold Weekly.

RULES:
  A) qgp_week is ALWAYS a "period end date":
     - Either a Saturday (normal week ending), OR
     - The quarter_end_date (partial-week bucket at quarter end)

  B) For dates that fall after the last Saturday before quarter end (and <= quarter end),
     those days MUST roll up into qgp_week = quarter_end_date (NOT the Saturday week end).

WINDOW:
  Lookback N days (default 120) to ensure at least 1 quarter boundary is covered.

NOTE:
  These are non-blocking by default (MEDIUM), but you can upgrade to HIGH if needed.

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_campaign_weekly_business_logic`()
BEGIN

  DECLARE v_table_name STRING DEFAULT 'sdi-gold-sa360-campaign-weekly';
  DECLARE v_now TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

  DECLARE v_lookback_days INT64 DEFAULT 120;
  DECLARE v_window_start DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL v_lookback_days DAY);

  -- TEST 1: qgp_week must be Saturday OR Quarter End (MEDIUM)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH w AS (
    SELECT
      qgp_week,
      -- Saturday check: BigQuery DAYOFWEEK: Sunday=1 ... Saturday=7
      (EXTRACT(DAYOFWEEK FROM qgp_week) = 7) AS is_saturday,

      -- Quarter-end check
      (qgp_week = DATE_SUB(DATE_ADD(DATE_TRUNC(qgp_week, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY)) AS is_quarter_end
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi-gold-sa360-campaign-weekly`
    WHERE qgp_week >= v_window_start
    GROUP BY qgp_week
  ),
  bad AS (
    SELECT COUNT(*) AS invalid_cnt
    FROM w
    WHERE NOT (is_saturday OR is_quarter_end)
  )
  SELECT
    v_now,
    CURRENT_DATE(),
    v_table_name,
    'business_logic',
    'QGP Week End Date Validity (Sat OR Quarter-End)',
    'MEDIUM',
    0.0,
    CAST(invalid_cnt AS FLOAT64),
    CAST(invalid_cnt AS FLOAT64),
    IF(invalid_cnt = 0, 'PASS', 'FAIL'),
    IF(invalid_cnt = 0, '🟢', '🔴'),
    IF(invalid_cnt = 0,
      'All qgp_week values are valid period-end dates (Saturday or quarter end).',
      'Found qgp_week values that are neither Saturday nor quarter-end.'
    ),
    IF(invalid_cnt = 0,
      'No action required.',
      'Fix qgp_week calculation in weekly build; ensure CASE produces only week_end_date(Sat) or quarter_end_date.'
    ),
    FALSE,
    IF(invalid_cnt = 0, TRUE, FALSE),
    IF(invalid_cnt = 0, FALSE, TRUE)
  FROM bad;

  -- TEST 2: QGP bucketing correctness vs recomputation (MEDIUM)
  --   Compare the set of qgp_week values derived from daily dates vs those present in weekly table.
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH derived_qgp AS (
    WITH base AS (
      SELECT
        d.date,
        DATE_ADD(DATE_TRUNC(d.date, WEEK(SUNDAY)), INTERVAL 6 DAY) AS week_end_date,
        DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY) AS quarter_end_date,
        DATE_SUB(
          DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY),
          INTERVAL MOD(EXTRACT(DAYOFWEEK FROM DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY)), 7) DAY
        ) AS last_saturday_before_qe
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi-gold-sa360-campaign-daily` d
      WHERE d.date >= v_window_start
      GROUP BY d.date
    )
    SELECT DISTINCT
      CASE
        WHEN date > last_saturday_before_qe AND date <= quarter_end_date THEN quarter_end_date
        ELSE week_end_date
      END AS qgp_week
    FROM base
  ),
  weekly_qgp AS (
    SELECT DISTINCT qgp_week
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi-gold-sa360-campaign-weekly`
    WHERE qgp_week >= v_window_start
  ),
  miss AS (
    SELECT COUNT(*) AS missing_qgp_cnt
    FROM derived_qgp d
    LEFT JOIN weekly_qgp w USING (qgp_week)
    WHERE w.qgp_week IS NULL
  )
  SELECT
    v_now,
    CURRENT_DATE(),
    v_table_name,
    'business_logic',
    'Weekly QGP Week Coverage vs Derived from Daily',
    'MEDIUM',
    0.0,
    CAST(missing_qgp_cnt AS FLOAT64),
    CAST(missing_qgp_cnt AS FLOAT64),
    IF(missing_qgp_cnt = 0, 'PASS', 'FAIL'),
    IF(missing_qgp_cnt = 0, '🟢', '🔴'),
    IF(missing_qgp_cnt = 0,
      'Weekly contains all QGP week buckets that are implied by daily dates.',
      'Some QGP week buckets implied by daily dates are missing from weekly.'
    ),
    IF(missing_qgp_cnt = 0,
      'No action required.',
      'Increase weekly MERGE lookback window or fix weekly recomputation logic.'
    ),
    FALSE,
    IF(missing_qgp_cnt = 0, TRUE, FALSE),
    IF(missing_qgp_cnt = 0, FALSE, TRUE)
  FROM miss;

END;
