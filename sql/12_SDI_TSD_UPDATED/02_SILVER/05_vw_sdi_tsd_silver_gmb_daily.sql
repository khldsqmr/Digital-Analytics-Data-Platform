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

KEY MODELING NOTES:
  - account_name is used only to derive LOB and is not carried into final Silver output
  - rows with unmapped LOB are excluded from final Silver
  - channel is intentionally MAPS & LOCAL SEARCH
  - Silver aggregates from corrected Bronze location grain, where dedupe is based on
    account_id + location_id + date_yyyymmdd
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_gmb_daily`
AS

WITH classified AS (
    SELECT
        event_date,
        CASE
            WHEN UPPER(TRIM(account_name)) = 'T-MOBILE STORES' THEN 'POSTPAID'
            WHEN UPPER(TRIM(account_name)) = 'METROPCS PLA BUSINESS ACCOUNT' THEN 'METRO'
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
    WHERE UPPER(TRIM(location_name)) NOT IN ("USCELLULAR")
),

filtered AS (
    SELECT *
    FROM classified
    WHERE lob IN ('POSTPAID', 'METRO')
)

SELECT
    event_date,
    lob,
    channel,
    SUM(COALESCE(gmb_search_impressions_all, 0)) AS gmb_search_impressions_all,
    SUM(COALESCE(gmb_maps_impressions_all, 0))   AS gmb_maps_impressions_all,
    SUM(COALESCE(gmb_impressions_all, 0))        AS gmb_impressions_all,
    SUM(COALESCE(gmb_call_clicks, 0))            AS gmb_call_clicks,
    SUM(COALESCE(gmb_website_clicks, 0))         AS gmb_website_clicks,
    SUM(COALESCE(gmb_directions_clicks, 0))      AS gmb_directions_clicks
FROM filtered
WHERE lob = 'POSTPAID'
GROUP BY 1, 2, 3
;