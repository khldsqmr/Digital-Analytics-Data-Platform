/*
===============================================================================
FILE: 01_sp_bronze_master_orchestration.sql
LAYER: Bronze
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_master_orchestration

PURPOSE:
  Daily Bronze build orchestration (incremental only).
  Runs Bronze merge procedures in the correct order.

NOTES:
  - Backfill scripts are NOT called here (those are manual SQL scripts).
  - Add/remove CALLs below based on the Bronze merge procedures you actually have.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_master_orchestration`()
OPTIONS(strict_mode=false)
BEGIN
  -- ---------------------------------------------------------------------------
  -- Bronze Campaign Daily and Campaign Entity Merge Procedures
  -- ---------------------------------------------------------------------------
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_bronze_sa360_campaign_daily`(); 
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_bronze_sa360_campaign_entity`();

END;