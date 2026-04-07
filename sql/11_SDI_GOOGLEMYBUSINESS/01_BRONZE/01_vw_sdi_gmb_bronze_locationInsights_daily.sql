/* =================================================================================================
FILE: 01_vw_sdi_gmb_bronze_locationInsights_daily.sql
LAYER: Bronze View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_gmb_bronze_locationInsights_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_improvado_master.google_business_profile_google_my_business_location_insights_tmo

PURPOSE:
  Canonical Bronze daily view for Google Business Profile (GMB) location insights, aggregated to
  date + channel + account_name grain with all useful dimensions, metrics, and lineage fields.

BUSINESS GRAIN:
  date
  + channel
  + account_name
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_gmb_bronze_locationInsights_daily` AS

WITH source AS (
  SELECT
    PARSE_DATE('%Y%m%d', date_yyyymmdd) AS date,
    date_yyyymmdd,
    'GMB' AS channel,
    account_name,

    COALESCE(SUM(business_impressions_desktop_maps), 0) AS business_impressions_desktop_maps,
    COALESCE(SUM(business_impressions_desktop_search), 0) AS business_impressions_desktop_search,
    COALESCE(SUM(business_impressions_mobile_maps), 0) AS business_impressions_mobile_maps,
    COALESCE(SUM(business_impressions_mobile_search), 0) AS business_impressions_mobile_search,

    COALESCE(SUM(call_clicks), 0) AS call_clicks,
    COALESCE(SUM(website_clicks), 0) AS website_clicks,
    COALESCE(SUM(business_direction_requests), 0) AS directions_click,

    MAX(File_Load_datetime) AS file_load_datetime,
    MAX(Filename) AS filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_business_profile_google_my_business_location_insights_tmo`
  WHERE date_yyyymmdd IS NOT NULL
    AND account_name IS NOT NULL
  GROUP BY
    1, 2, 3, 4
)

SELECT
  date,
  date_yyyymmdd,
  channel,
  account_name,

  business_impressions_desktop_maps,
  business_impressions_desktop_search,
  business_impressions_mobile_maps,
  business_impressions_mobile_search,

  business_impressions_mobile_search + business_impressions_desktop_search
    AS all_search_impressions,

  business_impressions_mobile_maps + business_impressions_desktop_maps
    AS all_maps_impressions,

  business_impressions_mobile_search + business_impressions_desktop_search
    + business_impressions_mobile_maps + business_impressions_desktop_maps
    AS all_impressions,

  call_clicks,
  website_clicks,
  directions_click,

  file_load_datetime,
  filename,
  CURRENT_TIMESTAMP() AS bronze_insert_ts
FROM source;