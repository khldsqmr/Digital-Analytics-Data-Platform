/* =================================================================================================
FILE: 06_vw_sdi_tsd_silver_sa360_daily.sql
LAYER: Silver View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_silver_sa360_daily

SOURCES:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_sa360Perf_daily
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_sa360Entity_daily

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_sa360_daily

PURPOSE:
  Canonical Silver SA360 daily source mart for the Total Search Dashboard.

BUSINESS GRAIN:
  One row per:
      event_date
      lob
      channel

BUSINESS LOGIC:
  - Only include campaigns where latest entity account_name is:
      POSTPAID GOOGLE
      POSTPAID BING
  - Only include campaigns where latest entity status is:
      ENABLED
      PAUSED
  - BRAND if resolved campaign name contains 'brand' case-insensitively
  - Otherwise NONBRAND

OUTPUT METRICS:
  - sa360_clicks_brand
  - sa360_clicks_nonbrand
  - sa360_clicks_all
  - sa360_cart_start_plus_brand
  - sa360_cart_start_plus_nonbrand
  - sa360_cart_start_plus_all

KEY MODELING NOTES:
  - SA360 only maps to PAID SEARCH
  - Uses latest available entity row per campaign
  - Nulls are preserved; no new zeroes are introduced

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_sa360_daily`
AS

WITH entity_latest AS (
    SELECT
        account_id,
        campaign_id,
        UPPER(TRIM(account_name)) AS entity_account_name,
        UPPER(TRIM(status)) AS entity_status,
        COALESCE(NULLIF(campaign_name, ''), NULLIF(latest_nonnull_campaign_name, '')) AS resolved_campaign_name
    FROM (
        SELECT
            account_id,
            campaign_id,
            account_name,
            status,
            campaign_name,
            latest_nonnull_campaign_name,
            file_load_datetime,
            filename,
            ROW_NUMBER() OVER (
                PARTITION BY account_id, campaign_id
                ORDER BY file_load_datetime DESC, filename DESC
            ) AS rn
        FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_sa360Entity_daily`
    )
    WHERE rn = 1
),

joined AS (
    SELECT
        perf.event_date,
        'POSTPAID' AS lob,
        'PAID SEARCH' AS channel,
        SAFE_CAST(perf.clicks AS FLOAT64) AS clicks,
        SAFE_CAST(perf.postpaid_cart_start AS FLOAT64) AS postpaid_cart_start,
        ent.entity_account_name,
        ent.entity_status,
        ent.resolved_campaign_name
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_sa360Perf_daily` perf
    LEFT JOIN entity_latest ent
      ON perf.account_id  = ent.account_id
     AND perf.campaign_id = ent.campaign_id
),

classified AS (
    SELECT
        event_date,
        lob,
        channel,
        clicks,
        postpaid_cart_start,
        CASE
            WHEN entity_account_name NOT IN ('POSTPAID GOOGLE', 'POSTPAID BING') THEN 'EXCLUDE'
            WHEN entity_status NOT IN ('ENABLED', 'PAUSED') THEN 'EXCLUDE'
            WHEN resolved_campaign_name IS NULL OR TRIM(resolved_campaign_name) = '' THEN 'EXCLUDE'
            WHEN REGEXP_CONTAINS(LOWER(resolved_campaign_name), r'brand') THEN 'BRAND'
            ELSE 'NONBRAND'
        END AS brand_type
    FROM joined
),

filtered AS (
    SELECT *
    FROM classified
    WHERE brand_type IN ('BRAND', 'NONBRAND')
),

aggregated AS (
    SELECT
        event_date,
        lob,
        channel,
        SUM(CASE WHEN brand_type = 'BRAND' THEN clicks END) AS sa360_clicks_brand,
        SUM(CASE WHEN brand_type = 'NONBRAND' THEN clicks END) AS sa360_clicks_nonbrand,
        SUM(CASE WHEN brand_type = 'BRAND' THEN postpaid_cart_start END) AS sa360_cart_start_plus_brand,
        SUM(CASE WHEN brand_type = 'NONBRAND' THEN postpaid_cart_start END) AS sa360_cart_start_plus_nonbrand
    FROM filtered
    GROUP BY event_date, lob, channel
)

SELECT
    event_date,
    lob,
    channel,

    sa360_clicks_brand,
    sa360_clicks_nonbrand,
    CASE
        WHEN sa360_clicks_brand IS NULL AND sa360_clicks_nonbrand IS NULL THEN NULL
        ELSE COALESCE(sa360_clicks_brand, 0) + COALESCE(sa360_clicks_nonbrand, 0)
    END AS sa360_clicks_all,

    sa360_cart_start_plus_brand,
    sa360_cart_start_plus_nonbrand,
    CASE
        WHEN sa360_cart_start_plus_brand IS NULL AND sa360_cart_start_plus_nonbrand IS NULL THEN NULL
        ELSE COALESCE(sa360_cart_start_plus_brand, 0) + COALESCE(sa360_cart_start_plus_nonbrand, 0)
    END AS sa360_cart_start_plus_all
FROM aggregated
;