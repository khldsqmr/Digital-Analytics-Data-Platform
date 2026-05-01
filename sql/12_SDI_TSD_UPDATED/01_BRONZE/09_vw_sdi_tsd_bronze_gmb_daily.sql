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
  This view deduplicates Google Business Profile / Google My Business location insights
  using the true business location grain before any aggregation.

BUSINESS GRAIN:
  One row per:
      event_date
      date_yyyymmdd
      account_id
      account_name
      location_id

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

DEDUPE LOGIC:
  Latest row per:
      account_id + location_id + date_yyyymmdd
  ordered by:
      File_Load_datetime DESC,
      Filename DESC,
      __insert_date DESC

KEY MODELING NOTES:
  - Bronze preserves account and location attributes for downstream debugging and conformance
  - No conformed lob or channel is applied here
  - Dedupe is applied before any aggregation to avoid snapshot double counting
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_gmb_daily`
AS

WITH ranked AS (
    SELECT
        PARSE_DATE('%Y%m%d', CAST(raw.date_yyyymmdd AS STRING)) AS event_date,
        CAST(raw.date_yyyymmdd AS STRING) AS date_yyyymmdd,

        SAFE_CAST(raw.account_id AS STRING) AS account_id,
        UPPER(TRIM(raw.account_name)) AS account_name,

        SAFE_CAST(raw.location_id AS STRING) AS location_id,
        SAFE_CAST(raw.store_code AS STRING) AS store_code,
        UPPER(TRIM(raw.location_name)) AS location_name,
        UPPER(TRIM(raw.address_line)) AS address_line,
        UPPER(TRIM(raw.location_and_address)) AS location_and_address,
        raw.location_url,

        SAFE_CAST(raw.business_impressions_desktop_maps AS FLOAT64)    AS business_impressions_desktop_maps,
        SAFE_CAST(raw.business_impressions_desktop_search AS FLOAT64)  AS business_impressions_desktop_search,
        SAFE_CAST(raw.business_impressions_mobile_maps AS FLOAT64)     AS business_impressions_mobile_maps,
        SAFE_CAST(raw.business_impressions_mobile_search AS FLOAT64)   AS business_impressions_mobile_search,

        SAFE_CAST(raw.call_clicks AS FLOAT64)                 AS gmb_call_clicks,
        SAFE_CAST(raw.website_clicks AS FLOAT64)              AS gmb_website_clicks,
        SAFE_CAST(raw.business_direction_requests AS FLOAT64) AS gmb_directions_clicks,

        SAFE_CAST(raw.business_bookings AS FLOAT64)      AS business_bookings,
        SAFE_CAST(raw.business_conversations AS FLOAT64) AS business_conversations,
        SAFE_CAST(raw.business_food_orders AS FLOAT64)   AS business_food_orders,

        SAFE_CAST(raw.__insert_date AS INT64) AS insert_date,
        TIMESTAMP(raw.File_Load_datetime) AS file_load_datetime,
        raw.Filename AS filename,

        ROW_NUMBER() OVER (
            PARTITION BY
                SAFE_CAST(raw.account_id AS STRING),
                SAFE_CAST(raw.location_id AS STRING),
                CAST(raw.date_yyyymmdd AS STRING)
            ORDER BY
                TIMESTAMP(raw.File_Load_datetime) DESC,
                raw.Filename DESC,
                SAFE_CAST(raw.__insert_date AS INT64) DESC
        ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_business_profile_google_my_business_location_insights_tmo` raw
    WHERE raw.date_yyyymmdd IS NOT NULL
      AND raw.account_id IS NOT NULL
      AND raw.account_name IS NOT NULL
      AND raw.location_id IS NOT NULL
),

deduped AS (
    SELECT
        event_date,
        date_yyyymmdd,
        account_id,
        account_name,
        location_id,
        store_code,
        location_name,
        address_line,
        location_and_address,
        location_url,

        business_impressions_desktop_maps,
        business_impressions_desktop_search,
        business_impressions_mobile_maps,
        business_impressions_mobile_search,

        gmb_call_clicks,
        gmb_website_clicks,
        gmb_directions_clicks,

        business_bookings,
        business_conversations,
        business_food_orders,

        file_load_datetime,
        filename
    FROM ranked
    WHERE rn = 1
)

SELECT
    event_date,
    date_yyyymmdd,

    account_id,
    account_name,

    location_id,
    store_code,
    location_name,
    address_line,
    location_and_address,
    location_url,

    business_impressions_desktop_maps,
    business_impressions_desktop_search,
    business_impressions_mobile_maps,
    business_impressions_mobile_search,

    COALESCE(business_impressions_mobile_search, 0)
      + COALESCE(business_impressions_desktop_search, 0) AS gmb_search_impressions_all,

    COALESCE(business_impressions_mobile_maps, 0)
      + COALESCE(business_impressions_desktop_maps, 0) AS gmb_maps_impressions_all,

    COALESCE(business_impressions_mobile_search, 0)
      + COALESCE(business_impressions_desktop_search, 0)
      + COALESCE(business_impressions_mobile_maps, 0)
      + COALESCE(business_impressions_desktop_maps, 0) AS gmb_impressions_all,

    gmb_call_clicks,
    gmb_website_clicks,
    gmb_directions_clicks,

    business_bookings,
    business_conversations,
    business_food_orders,

    file_load_datetime,
    filename
FROM deduped;