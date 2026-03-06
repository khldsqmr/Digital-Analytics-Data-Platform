/* =================================================================================================
FILE: 03_create_vw_sdi_profound_gold_citations_topic_daily.sql
LAYER: Gold
OBJECT TYPE: View
OBJECT NAME: vw_sdi_profound_gold_citations_topic_daily
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_topic_daily

PURPOSE:
  Tableau-facing Gold view for ProFound Citations by Topic at daily grain.

WHY THIS VIEW EXISTS:
  - Presents a clean dashboard-ready structure
  - Preserves business meaning and reporting grain
  - Excludes technical lineage attributes from end-user reporting

BUSINESS GRAIN:
  One row per:
    - date
    - account_id
    - topic

NOTES:
  - Topic values are preserved exactly as standardized in Bronze
  - No additional transformations are applied here
  - This makes Gold easy to maintain and easy to trust

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_gold_citations_topic_daily` AS

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
  count AS citations_count,
  share_of_voice AS share_of_voice

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_topic_daily`;