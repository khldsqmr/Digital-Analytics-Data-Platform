/* =================================================================================================
FILE: 05_vw_sdi_tsd_silver_gmb_daily.sql
LAYER: Silver View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_silver_gmb_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_gmb_daily

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_gmb_daily

PURPOSE:
  Canonical Silver GMB daily source mart for the Total Search Dashboard.
  This view maps GMB daily metrics into the conformed TSD reporting grain:
      event_date + lob + channel

BUSINESS GRAIN:
  One row per:
      event_date
      lob
      channel

OUTPUT METRICS:
  - gmb_search_impressions_all
  - gmb_maps_impressions_all
  - gmb_impressions_all
  - gmb_call_clicks
  - gmb_website_clicks
  - gmb_directions_clicks

LOB DERIVATION LOGIC:
  - account_name containing T-MOBILE STORES -> POSTPAID
  - everything else -> UNMAPPED

CHANNEL LOGIC:
  - all GMB rows map to MAPS & LOCAL SEARCH

KEY MODELING NOTES:
  - account_name is used only to derive LOB and is not carried into final Silver output
  - rows with unmapped LOB are excluded from the final Silver view
  - channel is intentionally kept distinct to represent local discovery intent

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_gmb_daily`
AS

WITH classified AS (
    SELECT
        event_date,

        CASE
            WHEN REGEXP_CONTAINS(UPPER(TRIM(account_name)), r'T[- ]?MOBILE STORES') THEN 'POSTPAID'
            ELSE 'UNMAPPED'
        END AS lob,

        UPPER(TRIM('MAPS & LOCAL SEARCH')) AS channel,

        gmb_search_impressions_all,
        gmb_maps_impressions_all,
        gmb_impressions_all,
        gmb_call_clicks,
        gmb_website_clicks,
        gmb_directions_clicks

    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_gmb_daily`
),

filtered AS (
    SELECT *
    FROM classified
    WHERE UPPER(TRIM(lob)) = 'POSTPAID'
)

SELECT
    event_date,
    UPPER(TRIM(lob)) AS lob,
    UPPER(TRIM(channel)) AS channel,

    SUM(COALESCE(gmb_search_impressions_all, 0)) AS gmb_search_impressions_all,
    SUM(COALESCE(gmb_maps_impressions_all, 0)) AS gmb_maps_impressions_all,
    SUM(COALESCE(gmb_impressions_all, 0)) AS gmb_impressions_all,
    SUM(COALESCE(gmb_call_clicks, 0)) AS gmb_call_clicks,
    SUM(COALESCE(gmb_website_clicks, 0)) AS gmb_website_clicks,
    SUM(COALESCE(gmb_directions_clicks, 0)) AS gmb_directions_clicks

FROM filtered
GROUP BY
    event_date,
    lob,
    channel;