/* =================================================================================================
FILE: 06_vw_sdi_tsd_gold_profound_monthly.sql
LAYER: Gold View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_gold_profound_monthly

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_profound_monthly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_profound_monthly

PURPOSE:
  Gold monthly ProFound / GoFish reporting view for the Total Search Dashboard.

BUSINESS GRAIN:
  One row per:
      period_date
      lob
      channel

KEY MODELING NOTES:
  - Thin gold wrapper over the monthly ProFound silver
  - No joins required because the silver is already conformed and wide

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_profound_monthly`
AS

SELECT
    period_date,
    UPPER(TRIM(lob)) AS lob,
    UPPER(TRIM(channel)) AS channel,

    profound_tmo_citation_share_brand,
    profound_tmo_citation_share_nonbrand,
    profound_tmo_visibility_score_brand,
    profound_tmo_visibility_score_nonbrand,

    profound_att_citation_share_brand,
    profound_att_citation_share_nonbrand,
    profound_att_visibility_score_brand,
    profound_att_visibility_score_nonbrand,

    profound_verizon_citation_share_brand,
    profound_verizon_citation_share_nonbrand,
    profound_verizon_visibility_score_brand,
    profound_verizon_visibility_score_nonbrand

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_profound_monthly`
;