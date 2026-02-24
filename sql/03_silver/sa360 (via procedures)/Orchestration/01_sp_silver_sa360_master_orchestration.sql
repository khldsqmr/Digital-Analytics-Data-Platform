/*
===============================================================================
FILE: 01_sp_silver_master_orchestration.sql
LAYER: Silver
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_sa360_master_orchestration

PURPOSE:
  Daily Silver build orchestration (incremental only).
  Runs Silver merge procedures in the correct dependency order.

NOTES:
  - Silver should run only AFTER Bronze build + Bronze QA.
  - Adjust procedure names if yours differ slightly.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_sa360_master_orchestration`()
OPTIONS(strict_mode=false)
BEGIN
  -- ---------------------------------------------------------------------------
  -- Silver Campaign Daily (joined with Entity for Campaign Name Info)
  -- ---------------------------------------------------------------------------
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_silver_sa360_campaign_daily`();

END;