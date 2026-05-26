/*===============================================================================================
FILE:         01_vw_sdi_pulseTms_silver_sa360_weekly.sql
LAYER:        Silver View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseTms_silver_sa360_weekly

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_bronze_sa360Adgroup_daily

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_sa360_weekly

PURPOSE:
  Silver view for SA360 paid search performance.
  Identical logic to vw_sdi_pulseByod_silver_sa360_weekly.
  Renamed pulseTms to support the full TMS pipeline (BYOD + Postpaid + HSI).
  Outputs a WIDE table — one row per week_sun_to_sat.
  Applies BYOD/BYOP ad group filter, brand/nonbrand classification,
  daily to weekly aggregation, WoW/LY comparisons, and max_data_date.

BUSINESS GRAIN:
  One row per: week_sun_to_sat

FILTERS APPLIED:
  - ad_group_name LIKE '%BYOD%' OR ad_group_name LIKE '%BYOP%'

BUSINESS LOGIC APPLIED:
  - data_source = 'SA360', channel = 'PAID SEARCH'
  - Brand: REGEXP_CONTAINS(LOWER(ad_group_name), r'(^|[^a-z])brand([^a-z]|$)')
  - Daily to weekly SUM aggregation
  - WoW: self-join on week_sun_to_sat - 7 days (gap-safe)
  - LY:  self-join on custom_week_num - 52 (gap-safe)
  - wow_pct and yoy_pct as decimals — NULL when prior NULL or 0
  - max_data_date: latest week with any non-null metric

DOWNSTREAM:
  Gold Wide : vw_sdi_pulseTms_gold_unified_wide
  Gold Long : vw_sdi_pulseTms_gold_unified_long
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_sa360_weekly`
AS

WITH classified AS (
    SELECT
        DATE_ADD(DATE_TRUNC(event_date, WEEK(SUNDAY)), INTERVAL 6 DAY)  AS week_sun_to_sat,
        CASE
            WHEN REGEXP_CONTAINS(LOWER(ad_group_name), r'(^|[^a-z])brand([^a-z]|$)')
            THEN 'brand'
            ELSE 'nonbrand'
        END                                                              AS brand_type,
        impressions, clicks, cost,
        postpaid_prospect_web_order, postpaid_cart_start, postpaid_pspv
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_bronze_sa360Adgroup_daily`
    WHERE ad_group_name LIKE '%BYOD%'
       OR ad_group_name LIKE '%BYOP%'
),

aggregated AS (
    SELECT
        week_sun_to_sat, brand_type,
        SUM(impressions)                 AS impressions,
        SUM(clicks)                      AS clicks,
        SUM(cost)                        AS cost,
        SUM(postpaid_prospect_web_order) AS orders,
        SUM(postpaid_cart_start)         AS cart_start,
        SUM(postpaid_pspv)               AS postpaid_pspv
    FROM classified
    GROUP BY week_sun_to_sat, brand_type
),

pivoted AS (
    SELECT
        week_sun_to_sat,
        MAX(CASE WHEN brand_type = 'brand'    THEN impressions   END) AS sa360_tmo_brand_impressions,
        MAX(CASE WHEN brand_type = 'brand'    THEN clicks        END) AS sa360_tmo_brand_clicks,
        MAX(CASE WHEN brand_type = 'brand'    THEN cost          END) AS sa360_tmo_brand_cost,
        MAX(CASE WHEN brand_type = 'brand'    THEN orders        END) AS sa360_tmo_brand_orders,
        MAX(CASE WHEN brand_type = 'brand'    THEN cart_start    END) AS sa360_tmo_brand_cart_start,
        MAX(CASE WHEN brand_type = 'brand'    THEN postpaid_pspv END) AS sa360_tmo_brand_postpaid_pspv,
        MAX(CASE WHEN brand_type = 'nonbrand' THEN impressions   END) AS sa360_tmo_nonbrand_impressions,
        MAX(CASE WHEN brand_type = 'nonbrand' THEN clicks        END) AS sa360_tmo_nonbrand_clicks,
        MAX(CASE WHEN brand_type = 'nonbrand' THEN cost          END) AS sa360_tmo_nonbrand_cost,
        MAX(CASE WHEN brand_type = 'nonbrand' THEN orders        END) AS sa360_tmo_nonbrand_orders,
        MAX(CASE WHEN brand_type = 'nonbrand' THEN cart_start    END) AS sa360_tmo_nonbrand_cart_start,
        MAX(CASE WHEN brand_type = 'nonbrand' THEN postpaid_pspv END) AS sa360_tmo_nonbrand_postpaid_pspv
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
        c.sa360_tmo_brand_impressions,    c.sa360_tmo_brand_clicks,
        c.sa360_tmo_brand_cost,           c.sa360_tmo_brand_orders,
        c.sa360_tmo_brand_cart_start,     c.sa360_tmo_brand_postpaid_pspv,
        c.sa360_tmo_nonbrand_impressions, c.sa360_tmo_nonbrand_clicks,
        c.sa360_tmo_nonbrand_cost,        c.sa360_tmo_nonbrand_orders,
        c.sa360_tmo_nonbrand_cart_start,  c.sa360_tmo_nonbrand_postpaid_pspv,
        w.sa360_tmo_brand_impressions    AS sa360_tmo_brand_impressions_wow,
        w.sa360_tmo_brand_clicks         AS sa360_tmo_brand_clicks_wow,
        w.sa360_tmo_brand_cost           AS sa360_tmo_brand_cost_wow,
        w.sa360_tmo_brand_orders         AS sa360_tmo_brand_orders_wow,
        w.sa360_tmo_brand_cart_start     AS sa360_tmo_brand_cart_start_wow,
        w.sa360_tmo_brand_postpaid_pspv  AS sa360_tmo_brand_postpaid_pspv_wow,
        w.sa360_tmo_nonbrand_impressions AS sa360_tmo_nonbrand_impressions_wow,
        w.sa360_tmo_nonbrand_clicks      AS sa360_tmo_nonbrand_clicks_wow,
        w.sa360_tmo_nonbrand_cost        AS sa360_tmo_nonbrand_cost_wow,
        w.sa360_tmo_nonbrand_orders      AS sa360_tmo_nonbrand_orders_wow,
        w.sa360_tmo_nonbrand_cart_start  AS sa360_tmo_nonbrand_cart_start_wow,
        w.sa360_tmo_nonbrand_postpaid_pspv AS sa360_tmo_nonbrand_postpaid_pspv_wow,
        l.sa360_tmo_brand_impressions    AS sa360_tmo_brand_impressions_ly,
        l.sa360_tmo_brand_clicks         AS sa360_tmo_brand_clicks_ly,
        l.sa360_tmo_brand_cost           AS sa360_tmo_brand_cost_ly,
        l.sa360_tmo_brand_orders         AS sa360_tmo_brand_orders_ly,
        l.sa360_tmo_brand_cart_start     AS sa360_tmo_brand_cart_start_ly,
        l.sa360_tmo_brand_postpaid_pspv  AS sa360_tmo_brand_postpaid_pspv_ly,
        l.sa360_tmo_nonbrand_impressions AS sa360_tmo_nonbrand_impressions_ly,
        l.sa360_tmo_nonbrand_clicks      AS sa360_tmo_nonbrand_clicks_ly,
        l.sa360_tmo_nonbrand_cost        AS sa360_tmo_nonbrand_cost_ly,
        l.sa360_tmo_nonbrand_orders      AS sa360_tmo_nonbrand_orders_ly,
        l.sa360_tmo_nonbrand_cart_start  AS sa360_tmo_nonbrand_cart_start_ly,
        l.sa360_tmo_nonbrand_postpaid_pspv AS sa360_tmo_nonbrand_postpaid_pspv_ly
    FROM with_week_num c
    LEFT JOIN with_week_num w ON c.week_sun_to_sat = DATE_ADD(w.week_sun_to_sat, INTERVAL 7 DAY)
    LEFT JOIN with_week_num l ON (c.custom_week_num - l.custom_week_num) = 52
),

with_pcts AS (
    SELECT
        week_sun_to_sat, custom_week_num,
        sa360_tmo_brand_impressions, sa360_tmo_brand_impressions_wow, sa360_tmo_brand_impressions_ly,
        CASE WHEN sa360_tmo_brand_impressions_wow IS NULL OR sa360_tmo_brand_impressions_wow = 0 THEN NULL ELSE ROUND((sa360_tmo_brand_impressions - sa360_tmo_brand_impressions_wow) / sa360_tmo_brand_impressions_wow, 6) END AS sa360_tmo_brand_impressions_wow_pct,
        CASE WHEN sa360_tmo_brand_impressions_ly  IS NULL OR sa360_tmo_brand_impressions_ly  = 0 THEN NULL ELSE ROUND((sa360_tmo_brand_impressions - sa360_tmo_brand_impressions_ly)  / sa360_tmo_brand_impressions_ly,  6) END AS sa360_tmo_brand_impressions_yoy_pct,
        sa360_tmo_brand_clicks, sa360_tmo_brand_clicks_wow, sa360_tmo_brand_clicks_ly,
        CASE WHEN sa360_tmo_brand_clicks_wow IS NULL OR sa360_tmo_brand_clicks_wow = 0 THEN NULL ELSE ROUND((sa360_tmo_brand_clicks - sa360_tmo_brand_clicks_wow) / sa360_tmo_brand_clicks_wow, 6) END AS sa360_tmo_brand_clicks_wow_pct,
        CASE WHEN sa360_tmo_brand_clicks_ly  IS NULL OR sa360_tmo_brand_clicks_ly  = 0 THEN NULL ELSE ROUND((sa360_tmo_brand_clicks - sa360_tmo_brand_clicks_ly)  / sa360_tmo_brand_clicks_ly,  6) END AS sa360_tmo_brand_clicks_yoy_pct,
        sa360_tmo_brand_cost, sa360_tmo_brand_cost_wow, sa360_tmo_brand_cost_ly,
        CASE WHEN sa360_tmo_brand_cost_wow IS NULL OR sa360_tmo_brand_cost_wow = 0 THEN NULL ELSE ROUND((sa360_tmo_brand_cost - sa360_tmo_brand_cost_wow) / sa360_tmo_brand_cost_wow, 6) END AS sa360_tmo_brand_cost_wow_pct,
        CASE WHEN sa360_tmo_brand_cost_ly  IS NULL OR sa360_tmo_brand_cost_ly  = 0 THEN NULL ELSE ROUND((sa360_tmo_brand_cost - sa360_tmo_brand_cost_ly)  / sa360_tmo_brand_cost_ly,  6) END AS sa360_tmo_brand_cost_yoy_pct,
        sa360_tmo_brand_orders, sa360_tmo_brand_orders_wow, sa360_tmo_brand_orders_ly,
        CASE WHEN sa360_tmo_brand_orders_wow IS NULL OR sa360_tmo_brand_orders_wow = 0 THEN NULL ELSE ROUND((sa360_tmo_brand_orders - sa360_tmo_brand_orders_wow) / sa360_tmo_brand_orders_wow, 6) END AS sa360_tmo_brand_orders_wow_pct,
        CASE WHEN sa360_tmo_brand_orders_ly  IS NULL OR sa360_tmo_brand_orders_ly  = 0 THEN NULL ELSE ROUND((sa360_tmo_brand_orders - sa360_tmo_brand_orders_ly)  / sa360_tmo_brand_orders_ly,  6) END AS sa360_tmo_brand_orders_yoy_pct,
        sa360_tmo_brand_cart_start, sa360_tmo_brand_cart_start_wow, sa360_tmo_brand_cart_start_ly,
        CASE WHEN sa360_tmo_brand_cart_start_wow IS NULL OR sa360_tmo_brand_cart_start_wow = 0 THEN NULL ELSE ROUND((sa360_tmo_brand_cart_start - sa360_tmo_brand_cart_start_wow) / sa360_tmo_brand_cart_start_wow, 6) END AS sa360_tmo_brand_cart_start_wow_pct,
        CASE WHEN sa360_tmo_brand_cart_start_ly  IS NULL OR sa360_tmo_brand_cart_start_ly  = 0 THEN NULL ELSE ROUND((sa360_tmo_brand_cart_start - sa360_tmo_brand_cart_start_ly)  / sa360_tmo_brand_cart_start_ly,  6) END AS sa360_tmo_brand_cart_start_yoy_pct,
        sa360_tmo_brand_postpaid_pspv, sa360_tmo_brand_postpaid_pspv_wow, sa360_tmo_brand_postpaid_pspv_ly,
        CASE WHEN sa360_tmo_brand_postpaid_pspv_wow IS NULL OR sa360_tmo_brand_postpaid_pspv_wow = 0 THEN NULL ELSE ROUND((sa360_tmo_brand_postpaid_pspv - sa360_tmo_brand_postpaid_pspv_wow) / sa360_tmo_brand_postpaid_pspv_wow, 6) END AS sa360_tmo_brand_postpaid_pspv_wow_pct,
        CASE WHEN sa360_tmo_brand_postpaid_pspv_ly  IS NULL OR sa360_tmo_brand_postpaid_pspv_ly  = 0 THEN NULL ELSE ROUND((sa360_tmo_brand_postpaid_pspv - sa360_tmo_brand_postpaid_pspv_ly)  / sa360_tmo_brand_postpaid_pspv_ly,  6) END AS sa360_tmo_brand_postpaid_pspv_yoy_pct,
        sa360_tmo_nonbrand_impressions, sa360_tmo_nonbrand_impressions_wow, sa360_tmo_nonbrand_impressions_ly,
        CASE WHEN sa360_tmo_nonbrand_impressions_wow IS NULL OR sa360_tmo_nonbrand_impressions_wow = 0 THEN NULL ELSE ROUND((sa360_tmo_nonbrand_impressions - sa360_tmo_nonbrand_impressions_wow) / sa360_tmo_nonbrand_impressions_wow, 6) END AS sa360_tmo_nonbrand_impressions_wow_pct,
        CASE WHEN sa360_tmo_nonbrand_impressions_ly  IS NULL OR sa360_tmo_nonbrand_impressions_ly  = 0 THEN NULL ELSE ROUND((sa360_tmo_nonbrand_impressions - sa360_tmo_nonbrand_impressions_ly)  / sa360_tmo_nonbrand_impressions_ly,  6) END AS sa360_tmo_nonbrand_impressions_yoy_pct,
        sa360_tmo_nonbrand_clicks, sa360_tmo_nonbrand_clicks_wow, sa360_tmo_nonbrand_clicks_ly,
        CASE WHEN sa360_tmo_nonbrand_clicks_wow IS NULL OR sa360_tmo_nonbrand_clicks_wow = 0 THEN NULL ELSE ROUND((sa360_tmo_nonbrand_clicks - sa360_tmo_nonbrand_clicks_wow) / sa360_tmo_nonbrand_clicks_wow, 6) END AS sa360_tmo_nonbrand_clicks_wow_pct,
        CASE WHEN sa360_tmo_nonbrand_clicks_ly  IS NULL OR sa360_tmo_nonbrand_clicks_ly  = 0 THEN NULL ELSE ROUND((sa360_tmo_nonbrand_clicks - sa360_tmo_nonbrand_clicks_ly)  / sa360_tmo_nonbrand_clicks_ly,  6) END AS sa360_tmo_nonbrand_clicks_yoy_pct,
        sa360_tmo_nonbrand_cost, sa360_tmo_nonbrand_cost_wow, sa360_tmo_nonbrand_cost_ly,
        CASE WHEN sa360_tmo_nonbrand_cost_wow IS NULL OR sa360_tmo_nonbrand_cost_wow = 0 THEN NULL ELSE ROUND((sa360_tmo_nonbrand_cost - sa360_tmo_nonbrand_cost_wow) / sa360_tmo_nonbrand_cost_wow, 6) END AS sa360_tmo_nonbrand_cost_wow_pct,
        CASE WHEN sa360_tmo_nonbrand_cost_ly  IS NULL OR sa360_tmo_nonbrand_cost_ly  = 0 THEN NULL ELSE ROUND((sa360_tmo_nonbrand_cost - sa360_tmo_nonbrand_cost_ly)  / sa360_tmo_nonbrand_cost_ly,  6) END AS sa360_tmo_nonbrand_cost_yoy_pct,
        sa360_tmo_nonbrand_orders, sa360_tmo_nonbrand_orders_wow, sa360_tmo_nonbrand_orders_ly,
        CASE WHEN sa360_tmo_nonbrand_orders_wow IS NULL OR sa360_tmo_nonbrand_orders_wow = 0 THEN NULL ELSE ROUND((sa360_tmo_nonbrand_orders - sa360_tmo_nonbrand_orders_wow) / sa360_tmo_nonbrand_orders_wow, 6) END AS sa360_tmo_nonbrand_orders_wow_pct,
        CASE WHEN sa360_tmo_nonbrand_orders_ly  IS NULL OR sa360_tmo_nonbrand_orders_ly  = 0 THEN NULL ELSE ROUND((sa360_tmo_nonbrand_orders - sa360_tmo_nonbrand_orders_ly)  / sa360_tmo_nonbrand_orders_ly,  6) END AS sa360_tmo_nonbrand_orders_yoy_pct,
        sa360_tmo_nonbrand_cart_start, sa360_tmo_nonbrand_cart_start_wow, sa360_tmo_nonbrand_cart_start_ly,
        CASE WHEN sa360_tmo_nonbrand_cart_start_wow IS NULL OR sa360_tmo_nonbrand_cart_start_wow = 0 THEN NULL ELSE ROUND((sa360_tmo_nonbrand_cart_start - sa360_tmo_nonbrand_cart_start_wow) / sa360_tmo_nonbrand_cart_start_wow, 6) END AS sa360_tmo_nonbrand_cart_start_wow_pct,
        CASE WHEN sa360_tmo_nonbrand_cart_start_ly  IS NULL OR sa360_tmo_nonbrand_cart_start_ly  = 0 THEN NULL ELSE ROUND((sa360_tmo_nonbrand_cart_start - sa360_tmo_nonbrand_cart_start_ly)  / sa360_tmo_nonbrand_cart_start_ly,  6) END AS sa360_tmo_nonbrand_cart_start_yoy_pct,
        sa360_tmo_nonbrand_postpaid_pspv, sa360_tmo_nonbrand_postpaid_pspv_wow, sa360_tmo_nonbrand_postpaid_pspv_ly,
        CASE WHEN sa360_tmo_nonbrand_postpaid_pspv_wow IS NULL OR sa360_tmo_nonbrand_postpaid_pspv_wow = 0 THEN NULL ELSE ROUND((sa360_tmo_nonbrand_postpaid_pspv - sa360_tmo_nonbrand_postpaid_pspv_wow) / sa360_tmo_nonbrand_postpaid_pspv_wow, 6) END AS sa360_tmo_nonbrand_postpaid_pspv_wow_pct,
        CASE WHEN sa360_tmo_nonbrand_postpaid_pspv_ly  IS NULL OR sa360_tmo_nonbrand_postpaid_pspv_ly  = 0 THEN NULL ELSE ROUND((sa360_tmo_nonbrand_postpaid_pspv - sa360_tmo_nonbrand_postpaid_pspv_ly)  / sa360_tmo_nonbrand_postpaid_pspv_ly,  6) END AS sa360_tmo_nonbrand_postpaid_pspv_yoy_pct
    FROM with_comparisons
),

with_max_date AS (
    SELECT *,
        MAX(CASE WHEN sa360_tmo_brand_impressions IS NOT NULL OR sa360_tmo_nonbrand_impressions IS NOT NULL THEN week_sun_to_sat END) OVER () AS max_data_date
    FROM with_pcts
)

SELECT
    week_sun_to_sat, 'SA360' AS data_source, 'PAID SEARCH' AS channel, max_data_date,
    sa360_tmo_brand_impressions, sa360_tmo_brand_impressions_wow, sa360_tmo_brand_impressions_ly, sa360_tmo_brand_impressions_wow_pct, sa360_tmo_brand_impressions_yoy_pct,
    sa360_tmo_brand_clicks, sa360_tmo_brand_clicks_wow, sa360_tmo_brand_clicks_ly, sa360_tmo_brand_clicks_wow_pct, sa360_tmo_brand_clicks_yoy_pct,
    sa360_tmo_brand_cost, sa360_tmo_brand_cost_wow, sa360_tmo_brand_cost_ly, sa360_tmo_brand_cost_wow_pct, sa360_tmo_brand_cost_yoy_pct,
    sa360_tmo_brand_orders, sa360_tmo_brand_orders_wow, sa360_tmo_brand_orders_ly, sa360_tmo_brand_orders_wow_pct, sa360_tmo_brand_orders_yoy_pct,
    sa360_tmo_brand_cart_start, sa360_tmo_brand_cart_start_wow, sa360_tmo_brand_cart_start_ly, sa360_tmo_brand_cart_start_wow_pct, sa360_tmo_brand_cart_start_yoy_pct,
    sa360_tmo_brand_postpaid_pspv, sa360_tmo_brand_postpaid_pspv_wow, sa360_tmo_brand_postpaid_pspv_ly, sa360_tmo_brand_postpaid_pspv_wow_pct, sa360_tmo_brand_postpaid_pspv_yoy_pct,
    sa360_tmo_nonbrand_impressions, sa360_tmo_nonbrand_impressions_wow, sa360_tmo_nonbrand_impressions_ly, sa360_tmo_nonbrand_impressions_wow_pct, sa360_tmo_nonbrand_impressions_yoy_pct,
    sa360_tmo_nonbrand_clicks, sa360_tmo_nonbrand_clicks_wow, sa360_tmo_nonbrand_clicks_ly, sa360_tmo_nonbrand_clicks_wow_pct, sa360_tmo_nonbrand_clicks_yoy_pct,
    sa360_tmo_nonbrand_cost, sa360_tmo_nonbrand_cost_wow, sa360_tmo_nonbrand_cost_ly, sa360_tmo_nonbrand_cost_wow_pct, sa360_tmo_nonbrand_cost_yoy_pct,
    sa360_tmo_nonbrand_orders, sa360_tmo_nonbrand_orders_wow, sa360_tmo_nonbrand_orders_ly, sa360_tmo_nonbrand_orders_wow_pct, sa360_tmo_nonbrand_orders_yoy_pct,
    sa360_tmo_nonbrand_cart_start, sa360_tmo_nonbrand_cart_start_wow, sa360_tmo_nonbrand_cart_start_ly, sa360_tmo_nonbrand_cart_start_wow_pct, sa360_tmo_nonbrand_cart_start_yoy_pct,
    sa360_tmo_nonbrand_postpaid_pspv, sa360_tmo_nonbrand_postpaid_pspv_wow, sa360_tmo_nonbrand_postpaid_pspv_ly, sa360_tmo_nonbrand_postpaid_pspv_wow_pct, sa360_tmo_nonbrand_postpaid_pspv_yoy_pct
FROM with_max_date
;