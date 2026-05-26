/* =================================================================================================
FILE:         02_vw_sdi_pulseTms_silver_gsc_weekly.sql
LAYER:        Silver View  
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseTms_silver_gsc_weekly

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_bronze_gscQuery_daily

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_gsc_weekly

PURPOSE:
  Silver view for Google Search Console organic search data.
  Identical logic to vw_sdi_pulseByod_silver_gsc_weekly.
  Renamed pulseTms to support the full TMS pipeline (BYOD + Postpaid + HSI).
  One row per week_sun_to_sat. Site URL, BYOD query filters, brand/nonbrand
  classification, WoW/LY comparisons, max_data_date.

BUSINESS GRAIN: One row per week_sun_to_sat
DOWNSTREAM:
  Gold Wide : vw_sdi_pulseTms_gold_unified_wide
  Gold Long : vw_sdi_pulseTms_gold_unified_long
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_gsc_weekly`
AS

WITH classified AS (
    SELECT
        DATE_ADD(DATE_TRUNC(event_date, WEEK(SUNDAY)), INTERVAL 6 DAY)  AS week_sun_to_sat,
        impressions, clicks,
        CASE
            WHEN LOWER(TRIM(query)) IS NULL OR TRIM(LOWER(TRIM(query))) = '' THEN 'exclude'
            WHEN REGEXP_CONTAINS(LOWER(TRIM(query)), r'(business|home-internet|prepaid\.|fiber\.|careers\.|promotions\.)') THEN 'exclude'
            WHEN (
                REGEXP_CONTAINS(LOWER(TRIM(query)), r'(^t$|\. t|\.t|tmobile|t-mobile|t mobile|tmo\b|magenta|metro|sprint|simple global|sync up|syncup|t-|switch to t|on us|onus|tuesday|million|digits|jump on|go 5g|go5g|layer 3|layer3|project 10|project ten)')
                OR REGEXP_CONTAINS(LOWER(TRIM(query)), r'(tmo|t-mo|mysprin|mytmo|prepaid|metropcs|5guc|5g uc|vibe|t mobile|tmobile|www\.t)')
            ) THEN 'brand'
            ELSE 'nonbrand'
        END AS brand_type
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_bronze_gscQuery_daily`
    WHERE UPPER(TRIM(site_url)) = 'SC-DOMAIN:T-MOBILE.COM'
      AND (
          LOWER(TRIM(query)) LIKE '%bring%phone%'
          OR LOWER(TRIM(query)) LIKE '%bring%device%'
          OR LOWER(TRIM(query)) LIKE '%byod%'
      )
      AND LOWER(TRIM(query)) NOT LIKE '%near%'
      AND LOWER(TRIM(query)) NOT LIKE '%pairing%'
      AND LOWER(TRIM(query)) NOT LIKE '%starlink%'
      AND LOWER(TRIM(query)) NOT LIKE '%animation%'
      AND LOWER(TRIM(query)) NOT LIKE '%iphone setup%'
      AND LOWER(TRIM(query)) NOT LIKE '%ipad setup%'
),

aggregated AS (
    SELECT week_sun_to_sat, brand_type,
        SUM(impressions) AS impressions, SUM(clicks) AS clicks
    FROM classified
    WHERE brand_type IN ('brand', 'nonbrand')
    GROUP BY week_sun_to_sat, brand_type
),

pivoted AS (
    SELECT
        week_sun_to_sat,
        MAX(CASE WHEN brand_type = 'brand'    THEN impressions END) AS gsc_tmo_brand_impressions,
        MAX(CASE WHEN brand_type = 'brand'    THEN clicks      END) AS gsc_tmo_brand_clicks,
        MAX(CASE WHEN brand_type = 'nonbrand' THEN impressions END) AS gsc_tmo_nonbrand_impressions,
        MAX(CASE WHEN brand_type = 'nonbrand' THEN clicks      END) AS gsc_tmo_nonbrand_clicks
    FROM aggregated
    GROUP BY week_sun_to_sat
),

with_week_num AS (
    SELECT *, DATE_DIFF(DATE_SUB(week_sun_to_sat, INTERVAL 6 DAY), DATE '2023-01-01', WEEK) AS custom_week_num
    FROM pivoted
),

with_comparisons AS (
    SELECT
        c.week_sun_to_sat, c.custom_week_num,
        c.gsc_tmo_brand_impressions,    c.gsc_tmo_brand_clicks,
        c.gsc_tmo_nonbrand_impressions, c.gsc_tmo_nonbrand_clicks,
        w.gsc_tmo_brand_impressions    AS gsc_tmo_brand_impressions_wow,
        w.gsc_tmo_brand_clicks         AS gsc_tmo_brand_clicks_wow,
        w.gsc_tmo_nonbrand_impressions AS gsc_tmo_nonbrand_impressions_wow,
        w.gsc_tmo_nonbrand_clicks      AS gsc_tmo_nonbrand_clicks_wow,
        l.gsc_tmo_brand_impressions    AS gsc_tmo_brand_impressions_ly,
        l.gsc_tmo_brand_clicks         AS gsc_tmo_brand_clicks_ly,
        l.gsc_tmo_nonbrand_impressions AS gsc_tmo_nonbrand_impressions_ly,
        l.gsc_tmo_nonbrand_clicks      AS gsc_tmo_nonbrand_clicks_ly
    FROM with_week_num c
    LEFT JOIN with_week_num w ON c.week_sun_to_sat = DATE_ADD(w.week_sun_to_sat, INTERVAL 7 DAY)
    LEFT JOIN with_week_num l ON (c.custom_week_num - l.custom_week_num) = 52
),

with_pcts AS (
    SELECT
        week_sun_to_sat, custom_week_num,
        gsc_tmo_brand_impressions, gsc_tmo_brand_impressions_wow, gsc_tmo_brand_impressions_ly,
        CASE WHEN gsc_tmo_brand_impressions_wow IS NULL OR gsc_tmo_brand_impressions_wow = 0 THEN NULL ELSE ROUND((gsc_tmo_brand_impressions - gsc_tmo_brand_impressions_wow) / gsc_tmo_brand_impressions_wow, 6) END AS gsc_tmo_brand_impressions_wow_pct,
        CASE WHEN gsc_tmo_brand_impressions_ly  IS NULL OR gsc_tmo_brand_impressions_ly  = 0 THEN NULL ELSE ROUND((gsc_tmo_brand_impressions - gsc_tmo_brand_impressions_ly)  / gsc_tmo_brand_impressions_ly,  6) END AS gsc_tmo_brand_impressions_yoy_pct,
        gsc_tmo_brand_clicks, gsc_tmo_brand_clicks_wow, gsc_tmo_brand_clicks_ly,
        CASE WHEN gsc_tmo_brand_clicks_wow IS NULL OR gsc_tmo_brand_clicks_wow = 0 THEN NULL ELSE ROUND((gsc_tmo_brand_clicks - gsc_tmo_brand_clicks_wow) / gsc_tmo_brand_clicks_wow, 6) END AS gsc_tmo_brand_clicks_wow_pct,
        CASE WHEN gsc_tmo_brand_clicks_ly  IS NULL OR gsc_tmo_brand_clicks_ly  = 0 THEN NULL ELSE ROUND((gsc_tmo_brand_clicks - gsc_tmo_brand_clicks_ly)  / gsc_tmo_brand_clicks_ly,  6) END AS gsc_tmo_brand_clicks_yoy_pct,
        gsc_tmo_nonbrand_impressions, gsc_tmo_nonbrand_impressions_wow, gsc_tmo_nonbrand_impressions_ly,
        CASE WHEN gsc_tmo_nonbrand_impressions_wow IS NULL OR gsc_tmo_nonbrand_impressions_wow = 0 THEN NULL ELSE ROUND((gsc_tmo_nonbrand_impressions - gsc_tmo_nonbrand_impressions_wow) / gsc_tmo_nonbrand_impressions_wow, 6) END AS gsc_tmo_nonbrand_impressions_wow_pct,
        CASE WHEN gsc_tmo_nonbrand_impressions_ly  IS NULL OR gsc_tmo_nonbrand_impressions_ly  = 0 THEN NULL ELSE ROUND((gsc_tmo_nonbrand_impressions - gsc_tmo_nonbrand_impressions_ly)  / gsc_tmo_nonbrand_impressions_ly,  6) END AS gsc_tmo_nonbrand_impressions_yoy_pct,
        gsc_tmo_nonbrand_clicks, gsc_tmo_nonbrand_clicks_wow, gsc_tmo_nonbrand_clicks_ly,
        CASE WHEN gsc_tmo_nonbrand_clicks_wow IS NULL OR gsc_tmo_nonbrand_clicks_wow = 0 THEN NULL ELSE ROUND((gsc_tmo_nonbrand_clicks - gsc_tmo_nonbrand_clicks_wow) / gsc_tmo_nonbrand_clicks_wow, 6) END AS gsc_tmo_nonbrand_clicks_wow_pct,
        CASE WHEN gsc_tmo_nonbrand_clicks_ly  IS NULL OR gsc_tmo_nonbrand_clicks_ly  = 0 THEN NULL ELSE ROUND((gsc_tmo_nonbrand_clicks - gsc_tmo_nonbrand_clicks_ly)  / gsc_tmo_nonbrand_clicks_ly,  6) END AS gsc_tmo_nonbrand_clicks_yoy_pct
    FROM with_comparisons
),

with_max_date AS (
    SELECT *,
        MAX(CASE WHEN gsc_tmo_brand_impressions IS NOT NULL OR gsc_tmo_nonbrand_impressions IS NOT NULL THEN week_sun_to_sat END) OVER () AS max_data_date
    FROM with_pcts
)

SELECT
    week_sun_to_sat, 'GSC' AS data_source, 'ORGANIC SEARCH' AS channel, max_data_date,
    gsc_tmo_brand_impressions, gsc_tmo_brand_impressions_wow, gsc_tmo_brand_impressions_ly, gsc_tmo_brand_impressions_wow_pct, gsc_tmo_brand_impressions_yoy_pct,
    gsc_tmo_brand_clicks, gsc_tmo_brand_clicks_wow, gsc_tmo_brand_clicks_ly, gsc_tmo_brand_clicks_wow_pct, gsc_tmo_brand_clicks_yoy_pct,
    gsc_tmo_nonbrand_impressions, gsc_tmo_nonbrand_impressions_wow, gsc_tmo_nonbrand_impressions_ly, gsc_tmo_nonbrand_impressions_wow_pct, gsc_tmo_nonbrand_impressions_yoy_pct,
    gsc_tmo_nonbrand_clicks, gsc_tmo_nonbrand_clicks_wow, gsc_tmo_nonbrand_clicks_ly, gsc_tmo_nonbrand_clicks_wow_pct, gsc_tmo_nonbrand_clicks_yoy_pct
FROM with_max_date
;