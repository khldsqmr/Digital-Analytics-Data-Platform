/* =================================================================================================
FILE:         03_vw_sdi_pulseByod_silver_sa360_weekly.sql
LAYER:        Silver View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseByod_silver_sa360_weekly

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_bronze_sa360Adgroup_daily

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_sa360_weekly

PURPOSE:
  Silver view for SA360 paid search performance.
  Applies BYOD/BYOP ad group filter, brand/nonbrand classification via ad_group_name,
  weekly aggregation, and week-end Saturday conversion on top of the Bronze view.
  Output is one row per week + brand_type ready for Gold unpivoting.

BUSINESS GRAIN:
  One row per:
    week_sun_to_sat + brand_type

FILTERS APPLIED:
  - ad_group_name LIKE '%BYOD%' OR ad_group_name LIKE '%BYOP%'

BUSINESS LOGIC APPLIED:
  - Brand classification via ad_group_name regex:
      BRAND    : ad_group_name contains word 'brand' (e.g. TMO_Brand_Phones_G_BYOD)
      NONBRAND : all other BYOD/BYOP ad groups  (e.g. TMO_Generic_Phones_G_BYOD)
      Regex: REGEXP_CONTAINS(LOWER(ad_group_name), r'(^|[^a-z])brand([^a-z]|$)')
      Consistent with existing Total Search Dashboard classification logic
  - Daily → weekly aggregation via SUM
  - week_sun_to_sat = DATE_ADD(DATE_TRUNC(event_date, WEEK(SUNDAY)), INTERVAL 6 DAY)

KEY MODELING NOTES:
  - SUM aggregation used for all additive metrics (impressions, clicks, cost, conversions)
  - postpaid_prospect_web_order is the primary conversion KPI for BYOD
  - postpaid_cart_start and postpaid_pspv retained as mid-funnel signals
  - cart_start retained for completeness
  - NULLs preserved — no COALESCE to zero applied here (pushed to Gold if needed)

DOWNSTREAM:
  Gold : vw_sdi_pulseByod_gold_unified_weekly
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_sa360_weekly`
AS

WITH classified AS (
    SELECT
        -- Week-end Saturday conversion
        -- Daily source rolled up to week ending Saturday
        DATE_ADD(DATE_TRUNC(event_date, WEEK(SUNDAY)), INTERVAL 6 DAY) AS week_sun_to_sat,

        -- Brand classification via ad_group_name
        -- Regex matches word boundary around 'brand' to avoid partial matches
        -- Consistent with vw_sdi_tsd_silver_sa360_daily classification logic
        CASE
            WHEN REGEXP_CONTAINS(
                LOWER(ad_group_name),
                r'(^|[^a-z])brand([^a-z]|$)'
            ) THEN 'BRAND'
            ELSE 'NONBRAND'
        END AS brand_type,

        -- Performance metrics (daily grain — aggregated to weekly in next CTE)
        impressions,
        clicks,
        cost,
        postpaid_prospect_web_order,
        postpaid_cart_start,
        postpaid_pspv,
        cart_start

    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_bronze_sa360Adgroup_daily`

    -- BYOD/BYOP ad group filter
    WHERE ad_group_name LIKE '%BYOD%'
       OR ad_group_name LIKE '%BYOP%'
),

aggregated AS (
    SELECT
        week_sun_to_sat,
        brand_type,

        -- Additive metrics aggregated from daily to weekly
        SUM(impressions)                AS impressions,
        SUM(clicks)                     AS clicks,
        SUM(cost)                       AS cost,
        SUM(postpaid_prospect_web_order) AS postpaid_prospect_web_order,
        SUM(postpaid_cart_start)        AS postpaid_cart_start,
        SUM(postpaid_pspv)              AS postpaid_pspv,
        SUM(cart_start)                 AS cart_start

    FROM classified
    GROUP BY
        week_sun_to_sat,
        brand_type
)

SELECT
    week_sun_to_sat,
    brand_type,
    impressions,
    clicks,
    cost,
    postpaid_prospect_web_order,
    postpaid_cart_start,
    postpaid_pspv,
    cart_start
FROM aggregated
ORDER BY week_sun_to_sat ASC, brand_type ASC
;