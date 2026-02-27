
/* =============================================================================
FILE: 05_sp_bronze_weekly_deep_validation.sql
CHANGE:
  - Keep anomaly detection (baseline vs last week) but DO NOT show large variance.
  - expected_value = actual_value = last_week_value, variance_value = 0 always.
  - Put baseline details into failure_reason text.
============================================================================= */

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_weekly_deep_validation_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE history_weeks INT64 DEFAULT 8;
  DECLARE spike_multiplier FLOAT64 DEFAULT 5.0;

  DECLARE last_weekend DATE;
  SET last_weekend = (
    SELECT MAX(DATE_TRUNC(date, WEEK(SATURDAY)))
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
    WHERE date IS NOT NULL
  );

  -- ---------------------------------------------------------------------------
  -- CART_START anomaly (zero-variance logging)
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
  WITH weekly AS (
    SELECT
      DATE_TRUNC(date, WEEK(SATURDAY)) AS weekend_date,
      SUM(COALESCE(cart_start,0)) AS metric_week
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
    GROUP BY 1
  ),
  last_val AS (
    SELECT metric_week AS last_week_value
    FROM weekly
    WHERE weekend_date = last_weekend
  ),
  baseline AS (
    SELECT AVG(metric_week) AS baseline_value
    FROM weekly
    WHERE weekend_date < last_weekend
    QUALIFY COUNT(*) OVER() >= history_weeks
  ),
  calc AS (
    SELECT
      (SELECT last_week_value FROM last_val) AS last_week_value,
      (SELECT baseline_value FROM baseline) AS baseline_value
  ),
  decision AS (
    SELECT
      last_week_value,
      baseline_value,
      CASE
        WHEN baseline_value IS NULL THEN 'PASS'
        WHEN baseline_value = 0 THEN IF(last_week_value = 0, 'PASS', 'FAIL')
        WHEN last_week_value > baseline_value * spike_multiplier THEN 'FAIL'
        WHEN last_week_value < baseline_value / spike_multiplier THEN 'FAIL'
        ELSE 'PASS'
      END AS status
    FROM calc
  )
  SELECT
    CURRENT_TIMESTAMP(),
    CURRENT_DATE(),
    'sdi_bronze_sa360_campaign_daily',
    'deep_validation',
    'Weekly Anomaly Check (cart_start vs prior baseline)',
    'MEDIUM',
    ROUND(last_week_value, 6) AS expected_value,
    ROUND(last_week_value, 6) AS actual_value,
    0.0 AS variance_value,
    status,
    IF(status = 'PASS', '🟢', '🔴') AS status_emoji,
    IF(baseline_value IS NULL,
      'PASS (not enough history).',
      CONCAT(
        'Baseline(avg prior ', CAST(history_weeks AS STRING), 'w)=', CAST(ROUND(baseline_value, 2) AS STRING),
        ' | last_week=', CAST(ROUND(last_week_value, 2) AS STRING),
        ' | rule: FAIL if >', CAST(spike_multiplier AS STRING), 'x or <1/', CAST(spike_multiplier AS STRING), 'x.'
      )
    ) AS failure_reason,
    IF(status = 'PASS',
      'No action required.',
      'If unexpected: validate last-week ingestion completeness and run weekly reconciliation tests.'
    ) AS next_step,
    FALSE AS is_critical_failure,
    IF(status = 'PASS', TRUE, FALSE) AS is_pass,
    IF(status = 'FAIL', TRUE, FALSE) AS is_fail
  FROM decision;

  -- ---------------------------------------------------------------------------
  -- POSTPAID_PSPV anomaly (zero-variance logging)
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
  WITH weekly AS (
    SELECT
      DATE_TRUNC(date, WEEK(SATURDAY)) AS weekend_date,
      SUM(COALESCE(postpaid_pspv,0)) AS metric_week
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
    GROUP BY 1
  ),
  last_val AS (
    SELECT metric_week AS last_week_value
    FROM weekly
    WHERE weekend_date = last_weekend
  ),
  baseline AS (
    SELECT AVG(metric_week) AS baseline_value
    FROM weekly
    WHERE weekend_date < last_weekend
    QUALIFY COUNT(*) OVER() >= history_weeks
  ),
  calc AS (
    SELECT
      (SELECT last_week_value FROM last_val) AS last_week_value,
      (SELECT baseline_value FROM baseline) AS baseline_value
  ),
  decision AS (
    SELECT
      last_week_value,
      baseline_value,
      CASE
        WHEN baseline_value IS NULL THEN 'PASS'
        WHEN baseline_value = 0 THEN IF(last_week_value = 0, 'PASS', 'FAIL')
        WHEN last_week_value > baseline_value * spike_multiplier THEN 'FAIL'
        WHEN last_week_value < baseline_value / spike_multiplier THEN 'FAIL'
        ELSE 'PASS'
      END AS status
    FROM calc
  )
  SELECT
    CURRENT_TIMESTAMP(),
    CURRENT_DATE(),
    'sdi_bronze_sa360_campaign_daily',
    'deep_validation',
    'Weekly Anomaly Check (postpaid_pspv vs prior baseline)',
    'MEDIUM',
    ROUND(last_week_value, 6) AS expected_value,
    ROUND(last_week_value, 6) AS actual_value,
    0.0 AS variance_value,
    status,
    IF(status = 'PASS', '🟢', '🔴') AS status_emoji,
    IF(baseline_value IS NULL,
      'PASS (not enough history).',
      CONCAT(
        'Baseline(avg prior ', CAST(history_weeks AS STRING), 'w)=', CAST(ROUND(baseline_value, 2) AS STRING),
        ' | last_week=', CAST(ROUND(last_week_value, 2) AS STRING),
        ' | rule: FAIL if >', CAST(spike_multiplier AS STRING), 'x or <1/', CAST(spike_multiplier AS STRING), 'x.'
      )
    ) AS failure_reason,
    IF(status = 'PASS',
      'No action required.',
      'If unexpected: validate last-week ingestion completeness and run weekly reconciliation tests.'
    ) AS next_step,
    FALSE AS is_critical_failure,
    IF(status = 'PASS', TRUE, FALSE) AS is_pass,
    IF(status = 'FAIL', TRUE, FALSE) AS is_fail
  FROM decision;

END;


