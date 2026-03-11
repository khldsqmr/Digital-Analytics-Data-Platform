/* =================================================================================================
FILE: 43_test_sdi_profound_silver_sanity.sql
PURPOSE:
  Sanity checks for Silver views.
  - row counts
  - report / grain distribution
================================================================================================= */

SELECT 'vw_sdi_profound_silver_visibility_weekly' AS view_name, COUNT(*) AS row_count
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_silver_visibility_weekly`
UNION ALL
SELECT 'vw_sdi_profound_silver_visibility_monthly', COUNT(*)
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_silver_visibility_monthly`
UNION ALL
SELECT 'vw_sdi_profound_silver_citation_weekly', COUNT(*)
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_silver_citation_weekly`
UNION ALL
SELECT 'vw_sdi_profound_silver_citation_monthly', COUNT(*)
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_silver_citation_monthly`;

SELECT
  report_type,
  time_granularity,
  grain_type,
  COUNT(*) AS row_count
FROM (
  SELECT * FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_silver_visibility_weekly`
  UNION ALL
  SELECT * FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_silver_visibility_monthly`
  UNION ALL
  SELECT * FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_silver_citation_weekly`
  UNION ALL
  SELECT * FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_silver_citation_monthly`
)
GROUP BY report_type, time_granularity, grain_type
ORDER BY report_type, time_granularity, grain_type;