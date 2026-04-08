
/* =================================================================================================
FILE: 06_vw_sdi_tsd_silver_profound_weekly.sql
LAYER: Silver View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_silver_profound_weekly

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_profoundVisCitTag_weekly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_profound_weekly

PURPOSE:
  Canonical Silver weekly ProFound / GoFish source mart for the Total Search Dashboard.
  This view pivots the Bronze object into the 12 wide reporting metrics.

BUSINESS GRAIN:
  One row per:
      period_date
      lob
      channel

OUTPUT METRICS:
  - profound_tmo_citation_share_brand
  - profound_tmo_citation_share_nonbrand
  - profound_tmo_visibility_score_brand
  - profound_tmo_visibility_score_nonbrand
  - profound_att_citation_share_brand
  - profound_att_citation_share_nonbrand
  - profound_att_visibility_score_brand
  - profound_att_visibility_score_nonbrand
  - profound_verizon_citation_share_brand
  - profound_verizon_citation_share_nonbrand
  - profound_verizon_visibility_score_brand
  - profound_verizon_visibility_score_nonbrand

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_profound_weekly`
AS

SELECT
    period_date,
    UPPER(TRIM(lob)) AS lob,
    UPPER(TRIM(channel)) AS channel,

    MAX(CASE WHEN company = 'TMO'     AND brand_type = 'BRAND'    AND metric_source = 'CITATION'   THEN citation_share END)   AS profound_tmo_citation_share_brand,
    MAX(CASE WHEN company = 'TMO'     AND brand_type = 'NONBRAND' AND metric_source = 'CITATION'   THEN citation_share END)   AS profound_tmo_citation_share_nonbrand,
    MAX(CASE WHEN company = 'TMO'     AND brand_type = 'BRAND'    AND metric_source = 'VISIBILITY' THEN visibility_score END) AS profound_tmo_visibility_score_brand,
    MAX(CASE WHEN company = 'TMO'     AND brand_type = 'NONBRAND' AND metric_source = 'VISIBILITY' THEN visibility_score END) AS profound_tmo_visibility_score_nonbrand,

    MAX(CASE WHEN company = 'ATT'     AND brand_type = 'BRAND'    AND metric_source = 'CITATION'   THEN citation_share END)   AS profound_att_citation_share_brand,
    MAX(CASE WHEN company = 'ATT'     AND brand_type = 'NONBRAND' AND metric_source = 'CITATION'   THEN citation_share END)   AS profound_att_citation_share_nonbrand,
    MAX(CASE WHEN company = 'ATT'     AND brand_type = 'BRAND'    AND metric_source = 'VISIBILITY' THEN visibility_score END) AS profound_att_visibility_score_brand,
    MAX(CASE WHEN company = 'ATT'     AND brand_type = 'NONBRAND' AND metric_source = 'VISIBILITY' THEN visibility_score END) AS profound_att_visibility_score_nonbrand,

    MAX(CASE WHEN company = 'VERIZON' AND brand_type = 'BRAND'    AND metric_source = 'CITATION'   THEN citation_share END)   AS profound_verizon_citation_share_brand,
    MAX(CASE WHEN company = 'VERIZON' AND brand_type = 'NONBRAND' AND metric_source = 'CITATION'   THEN citation_share END)   AS profound_verizon_citation_share_nonbrand,
    MAX(CASE WHEN company = 'VERIZON' AND brand_type = 'BRAND'    AND metric_source = 'VISIBILITY' THEN visibility_score END) AS profound_verizon_visibility_score_brand,
    MAX(CASE WHEN company = 'VERIZON' AND brand_type = 'NONBRAND' AND metric_source = 'VISIBILITY' THEN visibility_score END) AS profound_verizon_visibility_score_nonbrand

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_profoundVisCitTag_weekly`
GROUP BY
    period_date,
    lob,
    channel
;
