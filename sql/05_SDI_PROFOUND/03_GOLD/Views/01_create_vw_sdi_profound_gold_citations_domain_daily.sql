/* =================================================================================================
FILE: 01_create_vw_sdi_profound_gold_citations_domain_daily.sql
LAYER: Gold
OBJECT TYPE: View
OBJECT NAME: vw_sdi_profound_gold_citations_domain_daily
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_domain_daily

PURPOSE:
  Tableau-facing Gold view for ProFound Citations by Domain at daily grain.

WHY THIS VIEW EXISTS:
  - Exposes only business-facing reporting columns
  - Hides Bronze lineage/debug fields not needed in Tableau
  - Keeps the Bronze business grain intact
  - Avoids unnecessary re-aggregation that could mask upstream source issues

BUSINESS GRAIN:
  One row per:
    - date
    - account_id
    - root_domain

NOTES:
  - This view is intentionally thin and presentation-focused
  - file_load_datetime / filename / insert_date are excluded by design
  - Metrics are passed through from Bronze as-is

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_gold_citations_domain_daily` AS

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
     DOMAIN DIMENSION
     ---------------------------------------------------------------------------------------------- */
  root_domain,

  /* ----------------------------------------------------------------------------------------------
     METRICS
     ---------------------------------------------------------------------------------------------- */
  count AS citations_count,
  share_of_voice AS share_of_voice

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_domain_daily`;