/* =================================================================================================
FILE: 02_vw_sdi_gmb_gold_locationInsights_weekly.sql
LAYER: Gold View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_gmb_gold_locationInsights_weekly

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_gmb_gold_locationInsights_daily

PURPOSE:
  Final weekly Gold view for GMB location insights using QGP week rollup.

BUSINESS GRAIN:
  qgp_week
  + channel
  + account_name
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_gmb_gold_locationInsights_weekly` AS

SELECT
  qgp_week,
  channel,
  account_name,

  SUM(all_search_impressions) AS all_search_impressions,
  SUM(all_maps_impressions) AS all_maps_impressions,
  SUM(all_impressions) AS all_impressions,

  SUM(call_clicks) AS call_clicks,
  SUM(website_clicks) AS website_clicks,
  SUM(directions_click) AS directions_click
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_gmb_gold_locationInsights_daily`
GROUP BY
  1, 2, 3;