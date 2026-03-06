/* =================================================================================================
FILE: 02_create_vw_sdi_profound_gold_citations_tag_daily.sql
LAYER: Gold
OBJECT TYPE: View
OBJECT NAME: vw_sdi_profound_gold_citations_tag_daily
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_tag_daily

PURPOSE:
  Tableau-facing Gold view for ProFound Citations by Tag at daily grain.

WHY THIS VIEW EXISTS:
  - Provides a clean semantic layer for dashboarding
  - Preserves the Bronze business grain
  - Avoids exposing Bronze operational metadata in Tableau
  - Keeps the view stable while upstream platform issues are being resolved

BUSINESS GRAIN:
  One row per:
    - date
    - account_id
    - tag

NOTES:
  - Upstream source may still occasionally produce duplicate-metric issues
  - This view does not attempt to "fix" source logic beyond what Bronze already resolved
  - Tableau should use this view as the reporting source, not raw or Bronze directly

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_gold_citations_tag_daily` AS

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
  count AS citations_count,
  share_of_voice AS share_of_voice

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_tag_daily`;