/* =================================================================================================
FILE: 08_create_vw_sdi_profound_gold_visibility_topic_tag_daily.sql
LAYER: Gold
OBJECT TYPE: View
OBJECT NAME: vw_sdi_profound_gold_visibility_topic_tag_daily
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_topic_tag_daily

PURPOSE:
  Tableau-facing Gold view for ProFound Visibility by Topic + Tag at daily grain.

WHY THIS VIEW EXISTS:
  - Provides the most detailed visibility reporting layer for Tableau
  - Preserves the Bronze-resolved business grain
  - Keeps dashboarding separate from ingestion and QA logic

BUSINESS GRAIN:
  One row per:
    - date
    - account_id
    - topic
    - tag

IMPORTANT:
  - This view is intentionally not re-aggregated
  - Re-aggregation in Gold could hide raw-source issues and complicate QA comparisons
  - Tableau should aggregate from this view as needed for charts and dashboards

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_gold_visibility_topic_tag_daily` AS

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
     CONTENT DIMENSIONS
     ---------------------------------------------------------------------------------------------- */
  topic,
  tag,

  /* ----------------------------------------------------------------------------------------------
     METRICS
     ---------------------------------------------------------------------------------------------- */
  executions,
  mentions_count,
  share_of_voice AS share_of_voice,
  visibility_score

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_topic_tag_daily`;