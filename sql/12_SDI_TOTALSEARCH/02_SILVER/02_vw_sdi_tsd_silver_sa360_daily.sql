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

OUTPUT METRICS:
  - sa360_clicks_brand
  - sa360_clicks_nonbrand
  - sa360_clicks_all
  - sa360_cart_start_plus_brand
  - sa360_cart_start_plus_nonbrand
  - sa360_cart_start_plus_all

KEY MODELING NOTES:
  - SA360 only maps to PAID SEARCH in Silver
  - Nulls are preserved; no new zeroes are introduced

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_sa360_daily`
AS

WITH joined AS (
    SELECT
        perf.event_date,
        'POSTPAID' AS lob,
        'PAID SEARCH' AS channel,
        SAFE_CAST(perf.clicks AS FLOAT64) AS clicks,
        SAFE_CAST(perf.postpaid_cart_start AS FLOAT64) AS postpaid_cart_start,
        ent.campaign_type
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_sa360Perf_daily` perf
    LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_sa360Entity_daily` ent
      ON perf.account_id    = ent.account_id
     AND perf.campaign_id   = ent.campaign_id
     AND perf.date_yyyymmdd = ent.date_yyyymmdd
),

classified AS (
    SELECT
        event_date,
        lob,
        channel,
        clicks,
        postpaid_cart_start,
        CASE
            WHEN UPPER(TRIM(campaign_type)) = 'BRAND' THEN 'BRAND'
            WHEN UPPER(TRIM(campaign_type)) IN ('GENERIC', 'PMAX', 'SHOPPING') THEN 'NONBRAND'
            ELSE 'EXCLUDE'
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
        SUM(CASE WHEN brand_type = 'BRAND' THEN clicks END)             AS sa360_clicks_brand,
        SUM(CASE WHEN brand_type = 'NONBRAND' THEN clicks END)          AS sa360_clicks_nonbrand,
        SUM(CASE WHEN brand_type = 'BRAND' THEN postpaid_cart_start END)    AS sa360_cart_start_plus_brand,
        SUM(CASE WHEN brand_type = 'NONBRAND' THEN postpaid_cart_start END) AS sa360_cart_start_plus_nonbrand
    FROM filtered
    GROUP BY 1, 2, 3
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