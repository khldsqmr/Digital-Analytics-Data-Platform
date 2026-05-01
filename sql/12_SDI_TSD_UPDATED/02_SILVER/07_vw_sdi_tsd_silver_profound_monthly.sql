/* =================================================================================================
FILE: 08_vw_sdi_tsd_silver_profound_monthly.sql
LAYER: Silver View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_silver_profound_monthly

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_profoundVisCitTag_monthly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_profound_monthly

PURPOSE:
  Canonical Silver monthly ProFound / GoFish source mart for the Total Search Dashboard.

BUSINESS GRAIN:
  One row per:
      period_date
      lob
      channel

KEY MODELING NOTES:
  - Converts monthly source period to month-end
  - Preserves point-in-time metrics via MAX()
  - Includes executions for visibility and citation_count for citation reports
  - Nulls are preserved; no new zeroes are introduced

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_profound_monthly`
AS

SELECT
    DATE_SUB(DATE_ADD(DATE_TRUNC(period_date, MONTH), INTERVAL 1 MONTH), INTERVAL 1 DAY) AS period_date,
    UPPER(TRIM(lob)) AS lob,
    UPPER(TRIM(channel)) AS channel,

    MAX(CASE WHEN source_system = 'PROFOUND' AND company = 'TMO'     AND metric_source = 'VISIBILITY' THEN executions END)       AS profound_tmo_executions,
    MAX(CASE WHEN source_system = 'PROFOUND' AND company = 'TMO'     AND metric_source = 'CITATION'   THEN citation_count END)   AS profound_tmo_citation_count,
    MAX(CASE WHEN source_system = 'PROFOUND' AND company = 'TMO'     AND metric_source = 'CITATION'   THEN citation_share END)   AS profound_tmo_citation_share,
    MAX(CASE WHEN source_system = 'PROFOUND' AND company = 'TMO'     AND metric_source = 'VISIBILITY' THEN visibility_score END) AS profound_tmo_visibility_score,

    MAX(CASE WHEN source_system = 'PROFOUND' AND company = 'ATT'     AND metric_source = 'VISIBILITY' THEN executions END)       AS profound_att_executions,
    MAX(CASE WHEN source_system = 'PROFOUND' AND company = 'ATT'     AND metric_source = 'CITATION'   THEN citation_count END)   AS profound_att_citation_count,
    MAX(CASE WHEN source_system = 'PROFOUND' AND company = 'ATT'     AND metric_source = 'CITATION'   THEN citation_share END)   AS profound_att_citation_share,
    MAX(CASE WHEN source_system = 'PROFOUND' AND company = 'ATT'     AND metric_source = 'VISIBILITY' THEN visibility_score END) AS profound_att_visibility_score,

    MAX(CASE WHEN source_system = 'PROFOUND' AND company = 'VERIZON' AND metric_source = 'VISIBILITY' THEN executions END)       AS profound_verizon_executions,
    MAX(CASE WHEN source_system = 'PROFOUND' AND company = 'VERIZON' AND metric_source = 'CITATION'   THEN citation_count END)   AS profound_verizon_citation_count,
    MAX(CASE WHEN source_system = 'PROFOUND' AND company = 'VERIZON' AND metric_source = 'CITATION'   THEN citation_share END)   AS profound_verizon_citation_share,
    MAX(CASE WHEN source_system = 'PROFOUND' AND company = 'VERIZON' AND metric_source = 'VISIBILITY' THEN visibility_score END) AS profound_verizon_visibility_score,

    MAX(CASE WHEN source_system = 'GOFISH' AND company = 'TMO'     AND metric_source = 'VISIBILITY' THEN executions END)       AS gofish_tmo_executions,
    MAX(CASE WHEN source_system = 'GOFISH' AND company = 'TMO'     AND metric_source = 'CITATION'   THEN citation_count END)   AS gofish_tmo_citation_count,
    MAX(CASE WHEN source_system = 'GOFISH' AND company = 'TMO'     AND metric_source = 'CITATION'   THEN citation_share END)   AS gofish_tmo_citation_share,
    MAX(CASE WHEN source_system = 'GOFISH' AND company = 'TMO'     AND metric_source = 'VISIBILITY' THEN visibility_score END) AS gofish_tmo_visibility_score,

    MAX(CASE WHEN source_system = 'GOFISH' AND company = 'ATT'     AND metric_source = 'VISIBILITY' THEN executions END)       AS gofish_att_executions,
    MAX(CASE WHEN source_system = 'GOFISH' AND company = 'ATT'     AND metric_source = 'CITATION'   THEN citation_count END)   AS gofish_att_citation_count,
    MAX(CASE WHEN source_system = 'GOFISH' AND company = 'ATT'     AND metric_source = 'CITATION'   THEN citation_share END)   AS gofish_att_citation_share,
    MAX(CASE WHEN source_system = 'GOFISH' AND company = 'ATT'     AND metric_source = 'VISIBILITY' THEN visibility_score END) AS gofish_att_visibility_score,

    MAX(CASE WHEN source_system = 'GOFISH' AND company = 'VERIZON' AND metric_source = 'VISIBILITY' THEN executions END)       AS gofish_verizon_executions,
    MAX(CASE WHEN source_system = 'GOFISH' AND company = 'VERIZON' AND metric_source = 'CITATION'   THEN citation_count END)   AS gofish_verizon_citation_count,
    MAX(CASE WHEN source_system = 'GOFISH' AND company = 'VERIZON' AND metric_source = 'CITATION'   THEN citation_share END)   AS gofish_verizon_citation_share,
    MAX(CASE WHEN source_system = 'GOFISH' AND company = 'VERIZON' AND metric_source = 'VISIBILITY' THEN visibility_score END) AS gofish_verizon_visibility_score

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_profoundVisCitTag_monthly`
GROUP BY 1, 2, 3
;