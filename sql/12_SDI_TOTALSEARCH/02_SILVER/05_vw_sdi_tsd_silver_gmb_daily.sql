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
        'MAPS & LOCAL SEARCH' AS channel,
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
    WHERE lob = 'POSTPAID'
)

SELECT
    event_date,
    lob,
    channel,
    SUM(gmb_search_impressions_all) AS gmb_search_impressions_all,
    SUM(gmb_maps_impressions_all) AS gmb_maps_impressions_all,
    SUM(gmb_impressions_all) AS gmb_impressions_all,
    SUM(gmb_call_clicks) AS gmb_call_clicks,
    SUM(gmb_website_clicks) AS gmb_website_clicks,
    SUM(gmb_directions_clicks) AS gmb_directions_clicks
FROM filtered
GROUP BY event_date, lob, channel;