/* =================================================================================================
FILE: 05_create_vw_sdi_profound_gold_visibility_asset_daily.sql
LAYER: Gold
OBJECT TYPE: View
OBJECT NAME: vw_sdi_profound_gold_visibility_asset_daily
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily

PURPOSE:
  Tableau-facing Gold view for ProFound Visibility by Asset at daily grain.

WHY THIS VIEW EXISTS:
  - Provides a dashboard-ready semantic layer for asset-level visibility
  - Keeps the Bronze business grain intact
  - Excludes operational metadata fields

BUSINESS GRAIN:
  One row per:
    - date
    - account_id
    - asset_id

NOTES:
  - Asset name is included as a descriptive reporting dimension
  - Execution, mentions, share of voice, and visibility score are exposed directly

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_gold_visibility_asset_daily` AS

SELECT
  /* ----------------------------------------------------------------------------------------------
     BUSINESS DATE
     ---------------------------------------------------------------------------------------------- */
  date,

  /* ----------------------------------------------------------------------------------------------
     ACCOUNT DIMENSIONS
     ---------------------------------------------------------------------------------------------- */
  account_id,
  account_name,

  /* ----------------------------------------------------------------------------------------------
     ASSET DIMENSIONS
     ---------------------------------------------------------------------------------------------- */
  asset_id,
  asset_name,

  /* ----------------------------------------------------------------------------------------------
     METRICS
     ---------------------------------------------------------------------------------------------- */
  executions,
  mentions_count,
  share_of_voice AS share_of_voice,
  visibility_score

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily`;