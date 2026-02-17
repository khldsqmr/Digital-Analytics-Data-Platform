/*
===============================================================================
FILE: 01_sp_gold_campaign_daily_critical.sql
LAYER: Gold QA (Critical)

PURPOSE:
  Blocking checks for Gold Daily:
    1) Duplicate grain check
    2) Null identifiers check
    3) Freshness check vs Silver (lag days)

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_campaign_daily_critical`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE v_now TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE v_table STRING DEFAULT 'sdi_gold_sa360_campaign_daily';

  -- 1) Duplicate grain check (HIGH)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH dup AS (
    SELECT COUNT(*) AS dup_cnt
    FROM (
      SELECT account_id, campaign_id, date, COUNT(*) c
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
      WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
      GROUP BY 1,2,3
      HAVING c > 1
    )
  )
  SELECT
    v_now, CURRENT_DATE(), v_table,
    'critical',
    'Duplicate Grain Check (30-day)',
    'HIGH',
    0.0,
    CAST(dup_cnt AS FLOAT64),
    CAST(dup_cnt AS FLOAT64),
    IF(dup_cnt=0,'PASS','FAIL'),
    IF(dup_cnt=0,'🟢','🔴'),
    IF(dup_cnt=0,
      'No duplicate grain detected.',
      CONCAT('Duplicate grain rows found: ', CAST(dup_cnt AS STRING), '.')
    ),
    IF(dup_cnt=0,
      'No action required.',
      'Fix Gold Daily MERGE key or upstream duplication.'
    ),
    IF(dup_cnt=0,FALSE,TRUE),
    IF(dup_cnt=0,TRUE,FALSE),
    IF(dup_cnt=0,FALSE,TRUE)
  FROM dup;

  -- 2) Null identifiers check (HIGH)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH bad AS (
    SELECT COUNT(*) AS bad_cnt
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
      AND (account_id IS NULL OR campaign_id IS NULL OR date IS NULL)
  )
  SELECT
    v_now, CURRENT_DATE(), v_table,
    'critical',
    'Null Identifier Check (30-day)',
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
    IF(bad_cnt=0,
      'No action required.',
      'Fix Gold Daily source selection / MERGE insert logic.'
    ),
    IF(bad_cnt=0,FALSE,TRUE),
    IF(bad_cnt=0,TRUE,FALSE),
    IF(bad_cnt=0,FALSE,TRUE)
  FROM bad;

  -- 3) Freshness lag vs Silver (HIGH)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH mx AS (
    SELECT
      (SELECT MAX(date) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`) AS silver_max_date,
      (SELECT MAX(date) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`) AS gold_max_date
  ),
  lag AS (
    SELECT
      DATE_DIFF(silver_max_date, gold_max_date, DAY) AS lag_days
    FROM mx
  )
  SELECT
    v_now, CURRENT_DATE(), v_table,
    'critical',
    'Partition Freshness vs Silver (lag days)',
    'HIGH',
    2.0,
    CAST(lag_days AS FLOAT64),
    CAST(lag_days - 2 AS FLOAT64),
    IF(lag_days <= 2,'PASS','FAIL'),
    IF(lag_days <= 2,'🟢','🔴'),
    IF(lag_days <= 2,
      CONCAT('Freshness OK. Gold lag vs Silver = ', CAST(lag_days AS STRING), ' day(s).'),
      CONCAT('Gold is stale. Lag vs Silver = ', CAST(lag_days AS STRING), ' day(s).')
    ),
    IF(lag_days <= 2,
      'No action required.',
      'Run Gold Daily merge; verify scheduler; verify dataset region and permissions.'
    ),
    IF(lag_days <= 2,FALSE,TRUE),
    IF(lag_days <= 2,TRUE,FALSE),
    IF(lag_days <= 2,FALSE,TRUE)
  FROM lag;

END;
