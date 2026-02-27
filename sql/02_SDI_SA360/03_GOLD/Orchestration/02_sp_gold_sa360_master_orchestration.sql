/*
===============================================================================
FILE: 02_sp_gold_master_orchestration.sql
LAYER: Gold
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_master_orchestration

PURPOSE:
  Daily Gold build orchestration (incremental only).
  Runs Gold merge procedures in dependency order:
    1) Wide Daily
    2) Wide Weekly (from Gold Daily)
    3) Long Daily (from Gold Wide Daily)
    4) Long Weekly (from Gold Wide Weekly / or Gold Long Daily depending build)

NOTES:
  - This procedure should NOT run QA tests.
  - QA runs in sp_gold_sa360_qa_master_orchestration.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_master_orchestration`()
OPTIONS(strict_mode=false)
BEGIN
  -- ---------------------------------------------------------------------------
  -- Wide facts
  -- ---------------------------------------------------------------------------
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_gold_sa360_campaign_daily`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_gold_sa360_campaign_weekly`();

  -- ---------------------------------------------------------------------------
  -- Long facts
  -- ---------------------------------------------------------------------------
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_gold_sa360_campaign_daily_long`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_gold_sa360_campaign_weekly_long`();

END;