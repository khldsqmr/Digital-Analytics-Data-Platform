/* =================================================================================================
FILE: 01_vw_sdi_gmb_gold_locationInsights_daily.sql
LAYER: Gold View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_gmb_gold_locationInsights_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_gmb_silver_locationInsights_daily
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.fn_qgp_week

PURPOSE:
  Final daily Gold view for GMB location insights with QGP reporting week attached.

BUSINESS GRAIN:
  date
  + qgp_week
  + channel
  + account_name
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_gmb_gold_locationInsights_daily` AS

SELECT
  date,
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.fn_qgp_week`(date) AS qgp_week,
  channel,
  account_name,

  all_search_impressions,
  all_maps_impressions,
  all_impressions,

  call_clicks,
  website_clicks,
  directions_click
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_gmb_silver_locationInsights_daily`;