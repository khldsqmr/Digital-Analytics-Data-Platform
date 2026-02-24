/*
===============================================================================
VIEW: vw_gold_sa360_weekly_long_reporting
SOURCE: prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly_long

GRAIN (UNIQUE ROW):
  - account_name
  - campaign_type
  - qgp_week
  - lob
  - ad_platform
  - metric_name

PURPOSE:
  Reporting-friendly weekly long view using only requested dimensions + metrics.

NOTES:
  - Gold Weekly Long already has metric_name/metric_value
  - Aggregation preserves correct reporting grain after dropping account_id/campaign_id
===============================================================================
*/

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_gold_sa360_weekly_long_reporting` AS
SELECT
  account_name AS `Account Name`,
  campaign_type AS `Campaign Type`,
  qgp_week AS `QGP Week`,
  lob AS `Lob`,
  ad_platform AS `Ad Platform`,
  metric_name,
  SUM(COALESCE(metric_value, 0)) AS metric_value
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly_long`
WHERE qgp_week IS NOT NULL
GROUP BY 1,2,3,4,5,6;