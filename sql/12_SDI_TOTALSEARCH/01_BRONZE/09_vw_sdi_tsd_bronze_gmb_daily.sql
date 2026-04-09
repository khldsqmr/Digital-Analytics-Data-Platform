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
  at the raw snapshot level first, then aggregates to a source-close daily grain by:
      event_date + account_name

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

DEDUPE LOGIC:
  Latest row per standardized raw business key using:
      account_name + location_name + date_yyyymmdd
  ordered by:
      File_Load_datetime DESC,
      Filename DESC,
      __insert_date DESC

KEY MODELING NOTES:
  - This Bronze object preserves account_name for downstream LOB derivation
  - No conformed channel is applied here
  - Conformed TSD LOB and channel are applied in Silver
  - Dedupe is applied before aggregation to avoid snapshot double counting

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_gmb_daily`
AS

WITH ranked AS (
    SELECT
        PARSE_DATE('%Y%m%d', CAST(raw.date_yyyymmdd AS STRING)) AS event_date,
        CAST(raw.date_yyyymmdd AS STRING) AS date_yyyymmdd,
        UPPER(TRIM(raw.account_name)) AS account_name,
        UPPER(TRIM(raw.location_name)) AS location_name,

        SAFE_CAST(raw.business_impressions_desktop_maps AS FLOAT64)   AS business_impressions_desktop_maps,
        SAFE_CAST(raw.business_impressions_desktop_search AS FLOAT64) AS business_impressions_desktop_search,
        SAFE_CAST(raw.business_impressions_mobile_maps AS FLOAT64)    AS business_impressions_mobile_maps,
        SAFE_CAST(raw.business_impressions_mobile_search AS FLOAT64)  AS business_impressions_mobile_search,

        SAFE_CAST(raw.call_clicks AS FLOAT64)                AS gmb_call_clicks,
        SAFE_CAST(raw.website_clicks AS FLOAT64)             AS gmb_website_clicks,
        SAFE_CAST(raw.business_direction_requests AS FLOAT64) AS gmb_directions_clicks,

        SAFE_CAST(raw.__insert_date AS INT64) AS insert_date,
        TIMESTAMP(raw.File_Load_datetime) AS file_load_datetime,
        raw.Filename AS filename,

        ROW_NUMBER() OVER (
            PARTITION BY
                UPPER(TRIM(raw.account_name)),
                UPPER(TRIM(raw.location_name)),
                CAST(raw.date_yyyymmdd AS STRING)
            ORDER BY
                TIMESTAMP(raw.File_Load_datetime) DESC,
                raw.Filename DESC,
                SAFE_CAST(raw.__insert_date AS INT64) DESC
        ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_business_profile_google_my_business_location_insights_tmo` raw
    WHERE raw.date_yyyymmdd IS NOT NULL
      AND raw.account_name IS NOT NULL
      AND raw.location_name IS NOT NULL
),

deduped AS (
    SELECT
        event_date,
        date_yyyymmdd,
        account_name,
        location_name,
        business_impressions_desktop_maps,
        business_impressions_desktop_search,
        business_impressions_mobile_maps,
        business_impressions_mobile_search,
        gmb_call_clicks,
        gmb_website_clicks,
        gmb_directions_clicks,
        insert_date,
        file_load_datetime,
        filename
    FROM ranked
    WHERE rn = 1
),

aggregated AS (
    SELECT
        event_date,
        date_yyyymmdd,
        account_name,

        SUM(COALESCE(business_impressions_desktop_maps, 0))   AS business_impressions_desktop_maps,
        SUM(COALESCE(business_impressions_desktop_search, 0)) AS business_impressions_desktop_search,
        SUM(COALESCE(business_impressions_mobile_maps, 0))    AS business_impressions_mobile_maps,
        SUM(COALESCE(business_impressions_mobile_search, 0))  AS business_impressions_mobile_search,

        SUM(COALESCE(gmb_call_clicks, 0))       AS gmb_call_clicks,
        SUM(COALESCE(gmb_website_clicks, 0))    AS gmb_website_clicks,
        SUM(COALESCE(gmb_directions_clicks, 0)) AS gmb_directions_clicks,

        MAX(file_load_datetime) AS file_load_datetime,
        MAX(filename) AS filename
    FROM deduped
    GROUP BY
        event_date,
        date_yyyymmdd,
        account_name
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

    business_impressions_mobile_search
        + business_impressions_desktop_search
        + business_impressions_mobile_maps
        + business_impressions_desktop_maps
        AS gmb_impressions_all,

    gmb_call_clicks,
    gmb_website_clicks,
    gmb_directions_clicks,

    file_load_datetime,
    filename
FROM aggregated;