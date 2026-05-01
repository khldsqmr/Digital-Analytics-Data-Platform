/* =================================================================================================
FILE: 05_vw_sdi_tsd_gold_profound_monthly.sql
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
  - period_date is already standardized to month-end in Silver

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_profound_monthly`
AS

SELECT
    period_date,
    UPPER(TRIM(lob)) AS lob,
    UPPER(TRIM(channel)) AS channel,

    profound_tmo_executions,
    profound_tmo_citation_count,
    profound_tmo_citation_share,
    profound_tmo_visibility_score,

    profound_att_executions,
    profound_att_citation_count,
    profound_att_citation_share,
    profound_att_visibility_score,

    profound_verizon_executions,
    profound_verizon_citation_count,
    profound_verizon_citation_share,
    profound_verizon_visibility_score,

    gofish_tmo_executions,
    gofish_tmo_citation_count,
    gofish_tmo_citation_share,
    gofish_tmo_visibility_score,

    gofish_att_executions,
    gofish_att_citation_count,
    gofish_att_citation_share,
    gofish_att_visibility_score,

    gofish_verizon_executions,
    gofish_verizon_citation_count,
    gofish_verizon_citation_share,
    gofish_verizon_visibility_score

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_profound_monthly`
;