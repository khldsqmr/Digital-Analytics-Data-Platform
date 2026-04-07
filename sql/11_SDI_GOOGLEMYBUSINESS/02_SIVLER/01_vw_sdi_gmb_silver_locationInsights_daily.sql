/* =================================================================================================
FILE: 01_vw_sdi_gmb_silver_locationInsights_daily.sql
LAYER: Silver View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_gmb_silver_locationInsights_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_gmb_bronze_locationInsights_daily

PURPOSE:
  Clean Silver daily view for GMB location insights with standardized business-ready dimensions and
  metrics.

BUSINESS GRAIN:
  date
  + channel
  + account_name
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_gmb_silver_locationInsights_daily` AS

SELECT
  date,
  channel,
  account_name,

  business_impressions_desktop_maps,
  business_impressions_desktop_search,
  business_impressions_mobile_maps,
  business_impressions_mobile_search,

  all_search_impressions,
  all_maps_impressions,
  all_impressions,

  call_clicks,
  website_clicks,
  directions_click
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_gmb_bronze_locationInsights_daily`;