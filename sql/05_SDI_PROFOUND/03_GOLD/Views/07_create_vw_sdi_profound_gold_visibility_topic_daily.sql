/* =================================================================================================
FILE: 07_create_vw_sdi_profound_gold_visibility_topic_daily.sql
LAYER: Gold
OBJECT TYPE: View
OBJECT NAME: vw_sdi_profound_gold_visibility_topic_daily
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_topic_daily

PURPOSE:
  Tableau-facing Gold view for ProFound Visibility by Topic at daily grain.

WHY THIS VIEW EXISTS:
  - Makes topic-level visibility reporting easy for dashboards
  - Preserves business grain and semantic consistency
  - Avoids unnecessary transformations in the reporting layer

BUSINESS GRAIN:
  One row per:
    - date
    - account_id
    - topic

NOTES:
  - This is a straightforward semantic Gold view
  - Since Bronze already standardizes the structure, Gold stays intentionally simple

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_gold_visibility_topic_daily` AS

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
     TOPIC DIMENSION
     ---------------------------------------------------------------------------------------------- */
  topic,

  /* ----------------------------------------------------------------------------------------------
     METRICS
     ---------------------------------------------------------------------------------------------- */
  executions,
  mentions_count,
  share_of_voice AS share_of_voice,
  visibility_score

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_topic_daily`;