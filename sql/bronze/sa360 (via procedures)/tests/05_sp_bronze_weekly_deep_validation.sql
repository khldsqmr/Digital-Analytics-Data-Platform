/*
===============================================================================
FILE: 05_sp_bronze_weekly_deep_validation.sql
LAYER: Bronze | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_weekly_deep_validation_tests

WHAT THIS DOES:
  "Deep validation" doesn't compare to a source-of-truth table.
  Instead, it looks for abnormal behavior that usually indicates a pipeline issue:
    - sudden drops to zero
    - extreme spikes vs historical baseline

IMPLEMENTATION (simple + robust):
  - Build weekly totals from Bronze Daily using weekend_date = WEEK(SATURDAY)
  - Compare last complete week vs previous 8 weeks average
  - If last week deviates > X times average, flag as MEDIUM (not always a data bug)

METRICS CHECKED:
  - cart_start
  - postpaid_pspv
===============================================================================
*/
CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_weekly_deep_validation_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE history_weeks INT64 DEFAULT 8;
  DECLARE spike_multiplier FLOAT64 DEFAULT 5.0;

  -- ---------------------------------------------------------------------------
  -- 1) CART START anomaly check
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results` (
    test_run_timestamp,
    test_date,
    table_name,
    test_layer,
    test_name,
    severity_level,
    expected_value,
    actual_value,
    delta_value,
    status,
    status_emoji,
    failure_reason,
    next_step,
    is_critical_failure,
    is_pass,
    is_fail
  )
  WITH weekly AS (
    SELECT
      DATE_TRUNC(date, WEEK(SATURDAY)) AS weekend_date,
      SUM(COALESCE(cart_start, 0)) AS metric_week
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
    GROUP BY 1
  ),
  last_week AS (
    SELECT weekend_date, metric_week
    FROM weekly
    ORDER BY weekend_date DESC
    LIMIT 1
  ),
  prev_stats AS (
    SELECT
      AVG(metric_week) AS avg_prev,
      COUNT(*) AS cnt_prev
    FROM weekly
    WHERE weekend_date < (SELECT weekend_date FROM last_week)
  ),
  calc AS (
    SELECT
      (SELECT metric_week FROM last_week) AS last_val,
      IF((SELECT cnt_prev FROM prev_stats) < history_weeks, NULL, (SELECT avg_prev FROM prev_stats)) AS baseline
  ),
  final AS (
    SELECT
      CURRENT_TIMESTAMP() AS test_run_timestamp,
      CURRENT_DATE()      AS test_date,
      'sdi_bronze_sa360_campaign_daily' AS table_name,
      'deep_validation'   AS test_layer,
      'Weekly Anomaly Check (cart_start vs prior baseline)' AS test_name,
      'MEDIUM'            AS severity_level,
      baseline            AS expected_value,
      last_val            AS actual_value,
      (last_val - baseline) AS delta_value,

      -- Status logic
      IF(baseline IS NULL, 'PASS',
         IF(baseline = 0, IF(last_val = 0, 'PASS', 'FAIL'),
            IF(last_val > baseline * spike_multiplier OR last_val < baseline / spike_multiplier, 'FAIL', 'PASS')
         )
      ) AS status,

      IF(baseline IS NULL, '🟢',
         IF(baseline = 0, IF(last_val = 0, '🟢', '🔴'),
            IF(last_val > baseline * spike_multiplier OR last_val < baseline / spike_multiplier, '🔴', '🟢')
         )
      ) AS status_emoji,

      IF(baseline IS NULL,
        'Not enough history to evaluate anomaly reliably (PASS by design).',
        'Weekly value compared to baseline; FAIL indicates extreme spike/drop (may be data issue or real event).'
      ) AS failure_reason,

      IF(baseline IS NULL,
        'No action required.',
        'If unexpected: validate last week ingestion completeness and RAW->Bronze reconciliation.'
      ) AS next_step,

      FALSE AS is_critical_failure
    FROM calc
  )
  SELECT
    test_run_timestamp,
    test_date,
    table_name,
    test_layer,
    test_name,
    severity_level,
    expected_value,
    actual_value,
    delta_value,
    status,
    status_emoji,
    failure_reason,
    next_step,
    is_critical_failure,
    (status = 'PASS') AS is_pass,
    (status = 'FAIL') AS is_fail
  FROM final;

  -- ---------------------------------------------------------------------------
  -- 2) POSTPAID PSPV anomaly check
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results` (
    test_run_timestamp,
    test_date,
    table_name,
    test_layer,
    test_name,
    severity_level,
    expected_value,
    actual_value,
    delta_value,
    status,
    status_emoji,
    failure_reason,
    next_step,
    is_critical_failure,
    is_pass,
    is_fail
  )
  WITH weekly AS (
    SELECT
      DATE_TRUNC(date, WEEK(SATURDAY)) AS weekend_date,
      SUM(COALESCE(postpaid_pspv, 0)) AS metric_week
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
    GROUP BY 1
  ),
  last_week AS (
    SELECT weekend_date, metric_week
    FROM weekly
    ORDER BY weekend_date DESC
    LIMIT 1
  ),
  prev_stats AS (
    SELECT
      AVG(metric_week) AS avg_prev,
      COUNT(*) AS cnt_prev
    FROM weekly
    WHERE weekend_date < (SELECT weekend_date FROM last_week)
  ),
  calc AS (
    SELECT
      (SELECT metric_week FROM last_week) AS last_val,
      IF((SELECT cnt_prev FROM prev_stats) < history_weeks, NULL, (SELECT avg_prev FROM prev_stats)) AS baseline
  ),
  final AS (
    SELECT
      CURRENT_TIMESTAMP() AS test_run_timestamp,
      CURRENT_DATE()      AS test_date,
      'sdi_bronze_sa360_campaign_daily' AS table_name,
      'deep_validation'   AS test_layer,
      'Weekly Anomaly Check (postpaid_pspv vs prior baseline)' AS test_name,
      'MEDIUM'            AS severity_level,
      baseline            AS expected_value,
      last_val            AS actual_value,
      (last_val - baseline) AS delta_value,

      IF(baseline IS NULL, 'PASS',
         IF(baseline = 0, IF(last_val = 0, 'PASS', 'FAIL'),
            IF(last_val > baseline * spike_multiplier OR last_val < baseline / spike_multiplier, 'FAIL', 'PASS')
         )
      ) AS status,

      IF(baseline IS NULL, '🟢',
         IF(baseline = 0, IF(last_val = 0, '🟢', '🔴'),
            IF(last_val > baseline * spike_multiplier OR last_val < baseline / spike_multiplier, '🔴', '🟢')
         )
      ) AS status_emoji,

      IF(baseline IS NULL,
        'Not enough history to evaluate anomaly reliably (PASS by design).',
        'Weekly value compared to baseline; FAIL indicates extreme spike/drop (may be data issue or real event).'
      ) AS failure_reason,

      IF(baseline IS NULL,
        'No action required.',
        'If unexpected: validate last week ingestion completeness and RAW->Bronze reconciliation.'
      ) AS next_step,

      FALSE AS is_critical_failure
    FROM calc
  )
  SELECT
    test_run_timestamp,
    test_date,
    table_name,
    test_layer,
    test_name,
    severity_level,
    expected_value,
    actual_value,
    delta_value,
    status,
    status_emoji,
    failure_reason,
    next_step,
    is_critical_failure,
    (status = 'PASS') AS is_pass,
    (status = 'FAIL') AS is_fail
  FROM final;

END;
