/*
===============================================================================
FILE: 03_sp_gold_campaign_weekly_critical.sql
LAYER: Gold QA (Critical)

PURPOSE:
  Blocking checks for Gold Weekly:
    1) Duplicate grain check (account_id,campaign_id,qgp_week)
    2) Null identifiers check
    3) Freshness lag vs Gold Daily (max qgp_week should be >= max daily date - 7)

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_campaign_weekly_critical`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE v_now TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE v_table STRING DEFAULT 'sdi_gold_sa360_campaign_weekly';

  -- 1) Duplicate grain check (HIGH)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH dup AS (
    SELECT COUNT(*) AS dup_cnt
    FROM (
      SELECT account_id, campaign_id, qgp_week, COUNT(*) c
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
      WHERE qgp_week >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
      GROUP BY 1,2,3
      HAVING c > 1
    )
  )
  SELECT
    v_now, CURRENT_DATE(), v_table,
    'critical',
    'Duplicate Grain Check (90-day)',
    'HIGH',
    0.0,
    CAST(dup_cnt AS FLOAT64),
    CAST(dup_cnt AS FLOAT64),
    IF(dup_cnt=0,'PASS','FAIL'),
    IF(dup_cnt=0,'🟢','🔴'),
    IF(dup_cnt=0,
      'No duplicate grain detected.',
      CONCAT('Duplicate weekly grain rows found: ', CAST(dup_cnt AS STRING), '.')
    ),
    IF(dup_cnt=0,'No action required.','Fix weekly MERGE key or recompute logic.'),
    IF(dup_cnt=0,FALSE,TRUE),
    IF(dup_cnt=0,TRUE,FALSE),
    IF(dup_cnt=0,FALSE,TRUE)
  FROM dup;

  -- 2) Null identifiers check (HIGH)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH bad AS (
    SELECT COUNT(*) AS bad_cnt
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE qgp_week >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
      AND (account_id IS NULL OR campaign_id IS NULL OR qgp_week IS NULL)
  )
  SELECT
    v_now, CURRENT_DATE(), v_table,
    'critical',
    'Null Identifier Check (90-day)',
    'HIGH',
    0.0,
    CAST(bad_cnt AS FLOAT64),
    CAST(bad_cnt AS FLOAT64),
    IF(bad_cnt=0,'PASS','FAIL'),
    IF(bad_cnt=0,'🟢','🔴'),
    IF(bad_cnt=0,
      'All identifiers valid.',
      CONCAT('Null identifiers found: ', CAST(bad_cnt AS STRING), '.')
    ),
    IF(bad_cnt=0,'No action required.','Fix weekly build/merge logic.'),
    IF(bad_cnt=0,FALSE,TRUE),
    IF(bad_cnt=0,TRUE,FALSE),
    IF(bad_cnt=0,FALSE,TRUE)
  FROM bad;

  -- 3) Freshness vs Gold Daily (HIGH)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH mx AS (
    SELECT
      (SELECT MAX(date) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`) AS daily_max_date,
      (SELECT MAX(qgp_week) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`) AS weekly_max_qgp
  ),
  calc AS (
    SELECT
      DATE_DIFF(daily_max_date, weekly_max_qgp, DAY) AS lag_days
    FROM mx
  )
  SELECT
    v_now, CURRENT_DATE(), v_table,
    'critical',
    'Freshness vs Gold Daily (lag days)',
    'HIGH',
    7.0,
    CAST(lag_days AS FLOAT64),
    CAST(lag_days - 7 AS FLOAT64),
    IF(lag_days <= 7,'PASS','FAIL'),
    IF(lag_days <= 7,'🟢','🔴'),
    IF(lag_days <= 7,
      CONCAT('Freshness OK. Weekly lag vs Daily = ', CAST(lag_days AS STRING), ' day(s).'),
      CONCAT('Weekly is stale. Lag vs Daily = ', CAST(lag_days AS STRING), ' day(s).')
    ),
    IF(lag_days <= 7,
      'No action required.',
      'Run weekly MERGE; increase lookback; verify schedule order (daily then weekly).'
    ),
    IF(lag_days <= 7,FALSE,TRUE),
    IF(lag_days <= 7,TRUE,FALSE),
    IF(lag_days <= 7,FALSE,TRUE)
  FROM calc;

END;
