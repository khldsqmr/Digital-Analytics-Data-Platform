/* =================================================================================================
FILE: 04_create_vw_sdi_profound_gold_citations_topic_tag_daily.sql
LAYER: Gold
OBJECT TYPE: View
OBJECT NAME: vw_sdi_profound_gold_citations_topic_tag_daily
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_topic_tag_daily

PURPOSE:
  Tableau-facing Gold view for ProFound Citations by Topic + Tag at daily grain.

WHY THIS VIEW EXISTS:
  - Exposes the richest citations breakdown for Tableau
  - Preserves the Bronze business grain without extra summarization
  - Avoids mixing reporting logic with ingestion/debug logic

BUSINESS GRAIN:
  One row per:
    - date
    - account_id
    - root_domain
    - topic
    - tag

IMPORTANT:
  - This is one of the tables where upstream duplicate-metric issues were observed in raw
  - The Gold view itself is not the source of that issue
  - This view simply exposes the Bronze-resolved structure for reporting

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_gold_citations_topic_tag_daily` AS

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
  root_domain,
  topic,
  tag,

  /* ----------------------------------------------------------------------------------------------
     METRICS
     ---------------------------------------------------------------------------------------------- */
  count AS citations_count,
  share_of_voice AS share_of_voice

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_topic_tag_daily`;