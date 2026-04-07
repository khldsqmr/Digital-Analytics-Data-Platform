/* =================================================================================================
FILE: 09_vw_sdi_tsd_bronze_gmb_daily.sql
LAYER: Bronze View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_bronze_gmb_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_improvado_master.google_business_profile_google_my_business_location_insights_tmo

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_gmb_daily

PURPOSE:
  Canonical Bronze GMB daily view for the Total Search Dashboard.
  This view aggregates Google Business Profile / Google My Business location insights
  to a source-close daily grain by date and account_name, preserving traceability while
  calculating the core daily GMB metrics required downstream.

BUSINESS GRAIN:
  One row per:
      event_date
      date_yyyymmdd
      account_name

OUTPUT METRICS:
  - business_impressions_desktop_maps
  - business_impressions_desktop_search
  - business_impressions_mobile_maps
  - business_impressions_mobile_search
  - gmb_search_impressions_all
  - gmb_maps_impressions_all
  - gmb_impressions_all
  - gmb_call_clicks
  - gmb_website_clicks
  - gmb_directions_clicks

KEY MODELING NOTES:
  - This Bronze object preserves account_name for downstream LOB derivation
  - No conformed channel is applied here
  - Conformed TSD LOB and channel are applied in Silver
  - Aggregation is performed directly from raw source at date + account_name level

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_gmb_daily`
AS

WITH source AS (
  SELECT
    PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING)) AS event_date,
    CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
    UPPER(TRIM(account_name)) AS account_name,

    COALESCE(SUM(business_impressions_desktop_maps), 0) AS business_impressions_desktop_maps,
    COALESCE(SUM(business_impressions_desktop_search), 0) AS business_impressions_desktop_search,
    COALESCE(SUM(business_impressions_mobile_maps), 0) AS business_impressions_mobile_maps,
    COALESCE(SUM(business_impressions_mobile_search), 0) AS business_impressions_mobile_search,

    COALESCE(SUM(call_clicks), 0) AS gmb_call_clicks,
    COALESCE(SUM(website_clicks), 0) AS gmb_website_clicks,
    COALESCE(SUM(business_direction_requests), 0) AS gmb_directions_clicks,

    MAX(TIMESTAMP(File_Load_datetime)) AS file_load_datetime,
    MAX(Filename) AS filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_business_profile_google_my_business_location_insights_tmo`
  WHERE date_yyyymmdd IS NOT NULL
    AND account_name IS NOT NULL
  GROUP BY
    1, 2, 3
)

SELECT
  event_date,
  date_yyyymmdd,
  account_name,

  business_impressions_desktop_maps,
  business_impressions_desktop_search,
  business_impressions_mobile_maps,
  business_impressions_mobile_search,

  business_impressions_mobile_search + business_impressions_desktop_search
    AS gmb_search_impressions_all,

  business_impressions_mobile_maps + business_impressions_desktop_maps
    AS gmb_maps_impressions_all,

  business_impressions_mobile_search + business_impressions_desktop_search
    + business_impressions_mobile_maps + business_impressions_desktop_maps
    AS gmb_impressions_all,

  gmb_call_clicks,
  gmb_website_clicks,
  gmb_directions_clicks,

  file_load_datetime,
  filename
FROM source;