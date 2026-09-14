SELECT
  qgp_date,
  COUNT(*) AS row_count
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar`
GROUP BY qgp_date
HAVING COUNT(*) > 1;

SELECT
  data_source,
  qgp_date,
  channel_group,
  metric_name,
  COUNT(*) AS row_count
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_gold_unified_long`
WHERE data_source = 'ADOBE'
  AND metric_name = 'upvTotalAdobe'
  AND LOWER(channel_group) LIKE '%all%'
GROUP BY 1,2,3,4
HAVING COUNT(*) > 1
ORDER BY qgp_date DESC;