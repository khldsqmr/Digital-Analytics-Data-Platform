/* =================================================================================================
FILE:         04_vw_sdi_pulseByod_silver_gsc_weekly.sql
LAYER:        Silver View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseByod_silver_gsc_weekly

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_bronze_gscQuery_daily

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_gsc_weekly

PURPOSE:
  Silver view for Google Search Console organic search data.
  Outputs a WIDE table — one row per week_sun_to_sat.
  All metric columns prefixed with 'gsc_' for unambiguous
  identification in Gold Wide spine join and Gold Long unpivot.
  Applies site URL filter, BYOD query filters, brand/nonbrand classification,
  daily → weekly aggregation, WoW/LY comparisons, and max_data_date.

BUSINESS GRAIN:
  One row per:
    week_sun_to_sat

FILTERS APPLIED:
  - site_url = 'SC-DOMAIN:T-MOBILE.COM'
  - BYOD query inclusion:
      query LIKE '%bring%phone%' OR '%bring%device%' OR '%byod%'
  - BYOD query exclusions:
      NOT LIKE '%near%', '%pairing%', '%starlink%',
      '%animation%', '%iphone setup%', '%ipad setup%'

BUSINESS LOGIC APPLIED:
  - data_source = 'GSC'
  - channel     = 'ORGANIC SEARCH'
  - Brand classification via full T-Mobile brand regex
    Matches exactly vw_sdi_tsd_silver_gsc_daily as of 2026-05-24
    Do not modify without updating vw_sdi_tsd_silver_gsc_daily in parallel
  - Daily → weekly SUM aggregation
  - week_sun_to_sat = DATE_ADD(DATE_TRUNC(event_date, WEEK(SUNDAY)), INTERVAL 6 DAY)
  - All metric columns prefixed: gsc_tmo_{brand/nonbrand}_{metric}
  - WoW/LY self-joins on small weekly CTE
  - max_data_date per source

COLUMN NAMING CONVENTION:
  gsc_tmo_{brand_type}_{metric}
  gsc_tmo_{brand_type}_{metric}_wow
  gsc_tmo_{brand_type}_{metric}_ly
  gsc_tmo_{brand_type}_{metric}_wow_pct
  gsc_tmo_{brand_type}_{metric}_yoy_pct

  Where brand_type : brand, nonbrand
  Where metric     : impressions, clicks

KEY MODELING NOTES:
  - Query normalized for classification only — raw text not stored
  - Aggregation before pivot keeps grain clean
  - Self-joins on tiny pivoted CTE (1 row per week — extremely cheap)
  - NULLs preserved — no fake zeroes
  - No ORDER BY — applied in Gold only

DOWNSTREAM:
  Gold Wide : vw_sdi_pulseByod_gold_unified_wide
  Gold Long : vw_sdi_pulseByod_gold_unified_long
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_gsc_weekly`
AS

-- -----------------------------------------------------------------------
-- STEP 1: Normalize, filter, classify
-- -----------------------------------------------------------------------
WITH classified AS (
    SELECT
        DATE_ADD(DATE_TRUNC(event_date, WEEK(SUNDAY)), INTERVAL 6 DAY)  AS week_sun_to_sat,
        impressions,
        clicks,
        CASE
            WHEN LOWER(TRIM(query)) IS NULL OR TRIM(LOWER(TRIM(query))) = '' THEN 'EXCLUDE'
            WHEN REGEXP_CONTAINS(LOWER(TRIM(query)), r'(business|home-internet|prepaid.|fiber.|careers.|promotions.)') THEN 'EXCLUDE'
            WHEN (
                REGEXP_CONTAINS(LOWER(TRIM(query)),
                    r'(^t$|\. t|\.t|â„¢obile|aorint|apeint|aprint|atmobile|deutsche|digits|go 5g|go5g|i mobile|itmobile|jump on|jump2|layer 3|layer3|magenta|metro|mibile|mobile t|mtmobile|my tmo|myobile|mysprin|mytmo|mytobile|on us|onus|project 10|project ten|rmob|rtmobile|simple global|slrint|soeint|soribt|sorint|spei|sperint|spirint|spirit cell phone|spirnt|spprint|spri\.t|spriint|sprijt|sprimt|sprin|spritn|sprjnt|sprnt|spront|sprrint|sptint|srint|srpint|stateside international|switch to t|sync up|syncup|t-|t - mobile|t – mobile|t \.|t â€" mobile|t bision|t channel|t cision|t com|t kobile|t life|t lobile|t m9bile|t mÃ³vil|t mbile|t mboile|t metro|t mib|t minile|t mo|t- mo|t mp|t mus|t nmobile|t nobile|t obil|t remote|t television|t tv|t vibe|t vidion|t vis|t vizion|t-,obile|t\.|t\. obile|t\.com t|t\.mo|t\.obil|t\.obile|t:|t_mobil|t\+mobile|t=mobile|tâ€'mobile|tbile|t-bile|tbision|tbmobile|tbo|t-com|tdigit|te mobile|team|team mobile|teen mobile|teenmobile|temobil|temobile|temoble|ten mobile|the mobile|the vibe|t-home|tim|ti-mobile|tkobile|tlife|t-life|tlivetv|tlobile)'
                )
                OR
                REGEXP_CONTAINS(LOWER(TRIM(query)),
                    r'(tm coverage map|tm mobile|tm plans|tm tv|tm,obile|tm0bile|t-m0bile|tm9bile|tmabole|tmaobile|tmb|t-mbile|tmbilw|tmbiole|tmblie|t-mbo|tmbo|t-mbo|tmbpile|tmib|t-mib|tmlbile|tm-mobile|tmmoble|tmo|t-mo|tmpbile|t-mpbile|tmus|tnmobile|tnob|t-nob|to mobile|tobile|t-obile|toblie|tobmile|tomb|tomi|tomo|toobile|tpbile|t-phone|ttmobile|tv sion|tv vision|tviaion|tvibe channels|tvidion|tviosion|tvis|t-vis|tvivion|tvizion|tvmo|tv-mobile|tvsion|tv-t|tvusion|tvvis|tvzion activate|t-모바일|vibe|www t\.|www\.t|y mo|ymo|ytmobile|т мобил|8997|5guc|5g uc|tuesday|million)'
                )
            )
            THEN 'brand'
            ELSE 'nonbrand'
        END                                                              AS brand_type
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_bronze_gscQuery_daily`
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

-- -----------------------------------------------------------------------
-- STEP 2: Remove EXCLUDE, aggregate daily → weekly
-- -----------------------------------------------------------------------
aggregated AS (
    SELECT
        week_sun_to_sat,
        brand_type,
        SUM(impressions) AS impressions,
        SUM(clicks)      AS clicks
    FROM classified
    WHERE brand_type IN ('brand', 'nonbrand')
    GROUP BY week_sun_to_sat, brand_type
),

-- -----------------------------------------------------------------------
-- STEP 3: Pivot long → wide
-- -----------------------------------------------------------------------
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

-- -----------------------------------------------------------------------
-- STEP 4: Add custom week number
-- -----------------------------------------------------------------------
with_week_num AS (
    SELECT *,
        DATE_DIFF(DATE_SUB(week_sun_to_sat, INTERVAL 6 DAY), DATE '2023-01-01', WEEK) AS custom_week_num
    FROM pivoted
),

-- -----------------------------------------------------------------------
-- STEP 5: WoW and LY self-joins
-- -----------------------------------------------------------------------
with_comparisons AS (
    SELECT
        c.week_sun_to_sat,
        c.custom_week_num,
        c.gsc_tmo_brand_impressions,
        c.gsc_tmo_brand_clicks,
        c.gsc_tmo_nonbrand_impressions,
        c.gsc_tmo_nonbrand_clicks,
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

-- -----------------------------------------------------------------------
-- STEP 6: Compute wow_pct and yoy_pct
-- -----------------------------------------------------------------------
with_pcts AS (
    SELECT
        week_sun_to_sat,
        custom_week_num,

        gsc_tmo_brand_impressions,
        gsc_tmo_brand_impressions_wow,
        gsc_tmo_brand_impressions_ly,
        CASE WHEN gsc_tmo_brand_impressions_wow IS NULL OR gsc_tmo_brand_impressions_wow = 0 THEN NULL ELSE ROUND((gsc_tmo_brand_impressions - gsc_tmo_brand_impressions_wow) / gsc_tmo_brand_impressions_wow, 6) END AS gsc_tmo_brand_impressions_wow_pct,
        CASE WHEN gsc_tmo_brand_impressions_ly  IS NULL OR gsc_tmo_brand_impressions_ly  = 0 THEN NULL ELSE ROUND((gsc_tmo_brand_impressions - gsc_tmo_brand_impressions_ly)  / gsc_tmo_brand_impressions_ly,  6) END AS gsc_tmo_brand_impressions_yoy_pct,

        gsc_tmo_brand_clicks,
        gsc_tmo_brand_clicks_wow,
        gsc_tmo_brand_clicks_ly,
        CASE WHEN gsc_tmo_brand_clicks_wow IS NULL OR gsc_tmo_brand_clicks_wow = 0 THEN NULL ELSE ROUND((gsc_tmo_brand_clicks - gsc_tmo_brand_clicks_wow) / gsc_tmo_brand_clicks_wow, 6) END AS gsc_tmo_brand_clicks_wow_pct,
        CASE WHEN gsc_tmo_brand_clicks_ly  IS NULL OR gsc_tmo_brand_clicks_ly  = 0 THEN NULL ELSE ROUND((gsc_tmo_brand_clicks - gsc_tmo_brand_clicks_ly)  / gsc_tmo_brand_clicks_ly,  6) END AS gsc_tmo_brand_clicks_yoy_pct,

        gsc_tmo_nonbrand_impressions,
        gsc_tmo_nonbrand_impressions_wow,
        gsc_tmo_nonbrand_impressions_ly,
        CASE WHEN gsc_tmo_nonbrand_impressions_wow IS NULL OR gsc_tmo_nonbrand_impressions_wow = 0 THEN NULL ELSE ROUND((gsc_tmo_nonbrand_impressions - gsc_tmo_nonbrand_impressions_wow) / gsc_tmo_nonbrand_impressions_wow, 6) END AS gsc_tmo_nonbrand_impressions_wow_pct,
        CASE WHEN gsc_tmo_nonbrand_impressions_ly  IS NULL OR gsc_tmo_nonbrand_impressions_ly  = 0 THEN NULL ELSE ROUND((gsc_tmo_nonbrand_impressions - gsc_tmo_nonbrand_impressions_ly)  / gsc_tmo_nonbrand_impressions_ly,  6) END AS gsc_tmo_nonbrand_impressions_yoy_pct,

        gsc_tmo_nonbrand_clicks,
        gsc_tmo_nonbrand_clicks_wow,
        gsc_tmo_nonbrand_clicks_ly,
        CASE WHEN gsc_tmo_nonbrand_clicks_wow IS NULL OR gsc_tmo_nonbrand_clicks_wow = 0 THEN NULL ELSE ROUND((gsc_tmo_nonbrand_clicks - gsc_tmo_nonbrand_clicks_wow) / gsc_tmo_nonbrand_clicks_wow, 6) END AS gsc_tmo_nonbrand_clicks_wow_pct,
        CASE WHEN gsc_tmo_nonbrand_clicks_ly  IS NULL OR gsc_tmo_nonbrand_clicks_ly  = 0 THEN NULL ELSE ROUND((gsc_tmo_nonbrand_clicks - gsc_tmo_nonbrand_clicks_ly)  / gsc_tmo_nonbrand_clicks_ly,  6) END AS gsc_tmo_nonbrand_clicks_yoy_pct

    FROM with_comparisons
),

-- -----------------------------------------------------------------------
-- STEP 7: max_data_date
-- -----------------------------------------------------------------------
with_max_date AS (
    SELECT *,
        MAX(CASE
            WHEN gsc_tmo_brand_impressions    IS NOT NULL
              OR gsc_tmo_nonbrand_impressions IS NOT NULL
            THEN week_sun_to_sat END) OVER ()   AS max_data_date
    FROM with_pcts
)

SELECT
    week_sun_to_sat,
    'GSC'                                           AS data_source,
    'ORGANIC SEARCH'                                AS channel,
    max_data_date,

    gsc_tmo_brand_impressions,
    gsc_tmo_brand_impressions_wow,
    gsc_tmo_brand_impressions_ly,
    gsc_tmo_brand_impressions_wow_pct,
    gsc_tmo_brand_impressions_yoy_pct,

    gsc_tmo_brand_clicks,
    gsc_tmo_brand_clicks_wow,
    gsc_tmo_brand_clicks_ly,
    gsc_tmo_brand_clicks_wow_pct,
    gsc_tmo_brand_clicks_yoy_pct,

    gsc_tmo_nonbrand_impressions,
    gsc_tmo_nonbrand_impressions_wow,
    gsc_tmo_nonbrand_impressions_ly,
    gsc_tmo_nonbrand_impressions_wow_pct,
    gsc_tmo_nonbrand_impressions_yoy_pct,

    gsc_tmo_nonbrand_clicks,
    gsc_tmo_nonbrand_clicks_wow,
    gsc_tmo_nonbrand_clicks_ly,
    gsc_tmo_nonbrand_clicks_wow_pct,
    gsc_tmo_nonbrand_clicks_yoy_pct

FROM with_max_date
;