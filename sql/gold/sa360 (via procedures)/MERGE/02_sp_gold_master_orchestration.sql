/*
===============================================================================
FILE: 02_sp_gold_master_orchestration.sql
LAYER: Gold
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_master_orchestration

PURPOSE:
  One-call Gold refresh:
    1) Upsert Gold Daily from Silver
    2) Upsert Gold Weekly from Gold Daily
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_master_orchestration`()
OPTIONS(strict_mode=false)
BEGIN
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_gold_sa360_campaign_daily`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_gold_sa360_campaign_weekly`();
END;
