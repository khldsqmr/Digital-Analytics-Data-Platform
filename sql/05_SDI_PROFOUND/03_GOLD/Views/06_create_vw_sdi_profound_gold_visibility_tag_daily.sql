/* =================================================================================================
FILE: 06_create_vw_sdi_profound_gold_visibility_tag_daily.sql
LAYER: Gold
OBJECT TYPE: View
OBJECT NAME: vw_sdi_profound_gold_visibility_tag_daily
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_tag_daily

PURPOSE:
  Tableau-facing Gold view for ProFound Visibility by Tag at daily grain.

WHY THIS VIEW EXISTS:
  - Supports tag-level visibility reporting in Tableau
  - Keeps the dashboard layer clean and stable
  - Prevents accidental use of Bronze lineage fields in reporting

BUSINESS GRAIN:
  One row per:
    - date
    - account_id
    - tag

IMPORTANT:
  - This source family has shown upstream duplicate patterns in raw
  - Gold should remain thin and trustworthy rather than trying to over-correct reporting logic

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_gold_visibility_tag_daily` AS

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
     TAG DIMENSION
     ---------------------------------------------------------------------------------------------- */
  tag,

  /* ----------------------------------------------------------------------------------------------
     METRICS
     ---------------------------------------------------------------------------------------------- */
  executions,
  mentions_count,
  share_of_voice AS share_of_voice,
  visibility_score

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_tag_daily`;