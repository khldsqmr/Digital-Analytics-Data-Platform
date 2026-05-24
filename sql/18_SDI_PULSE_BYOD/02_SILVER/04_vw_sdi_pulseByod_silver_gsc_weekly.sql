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
  Applies site URL filter, BYOD query filters, brand/nonbrand classification
  via the full T-Mobile brand regex, weekly aggregation, and week-end Saturday
  conversion on top of the Bronze view.
  Output is one row per week + brand_type ready for Gold unpivoting.

BUSINESS GRAIN:
  One row per:
    week_sun_to_sat + brand_type

FILTERS APPLIED:
  - site_url = 'SC-DOMAIN:T-MOBILE.COM' (T-Mobile domain only)
  - BYOD query inclusion filters:
      query LIKE '%bring%phone%'
      OR query LIKE '%bring%device%'
      OR query LIKE '%byod%'
  - BYOD query exclusion filters:
      query NOT LIKE '%near%'
      query NOT LIKE '%pairing%'
      query NOT LIKE '%starlink%'
      query NOT LIKE '%animation%'
      query NOT LIKE '%iphone setup%'
      query NOT LIKE '%ipad setup%'

BUSINESS LOGIC APPLIED:
  - Brand classification via full T-Mobile brand regex on query text
    Consistent with existing vw_sdi_tsd_silver_gsc_daily classification logic
    BRAND    : query matches T-Mobile brand regex (tmo, t-mobile, tmobile, magenta, metro, sprint etc.)
    NONBRAND : all other BYOD queries not matching brand regex
    EXCLUDE  : queries matching LOB exclusion regex — removed before aggregation
  - Daily → weekly aggregation via SUM
  - week_sun_to_sat = DATE_ADD(DATE_TRUNC(event_date, WEEK(SUNDAY)), INTERVAL 6 DAY)

KEY MODELING NOTES:
  - Query text normalized (LOWER, TRIM) for classification only — raw text not output
  - SUM aggregation used for all additive metrics (impressions, clicks)
  - NULLs preserved — no COALESCE to zero applied here (pushed to Gold if needed)
  - Brand regex sourced from vw_sdi_tsd_silver_gsc_daily for consistency
    across Total Search Dashboard and Pulse BYOD

DOWNSTREAM:
  Gold : vw_sdi_pulseByod_gold_unified_weekly
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_gsc_weekly`
AS

WITH standardized AS (
    SELECT
        -- Week-end Saturday conversion
        -- Daily source rolled up to week ending Saturday
        DATE_ADD(DATE_TRUNC(event_date, WEEK(SUNDAY)), INTERVAL 6 DAY) AS week_sun_to_sat,

        -- Metrics (daily grain — aggregated to weekly in classified CTE)
        impressions,
        clicks,

        -- Normalized query text for classification only
        -- Raw query text not carried forward — classification label used instead
        LOWER(TRIM(query))                                              AS query_normalized,

        -- Standardized site_url for filtering
        UPPER(TRIM(site_url))                                           AS site_url_standardized

    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_bronze_gscQuery_daily`
),

classified AS (
    SELECT
        week_sun_to_sat,
        impressions,
        clicks,

        -- Brand classification via full T-Mobile brand regex
        -- Sourced from vw_sdi_tsd_silver_gsc_daily for consistency
        -- BRAND    : query contains any T-Mobile brand signal
        -- NONBRAND : all other BYOD queries
        -- EXCLUDE  : queries matching LOB exclusion patterns
        CASE
            WHEN query_normalized IS NULL
              OR TRIM(query_normalized) = ''                            THEN 'EXCLUDE'
            WHEN REGEXP_CONTAINS(
                query_normalized,
                r'(business|home-internet|prepaid.|fiber.|careers.|promotions.)'
            )                                                           THEN 'EXCLUDE'
            WHEN REGEXP_CONTAINS(
                query_normalized,
                r'(^t$|\. t|\.t|tmobile|t-mobile|t mobile|aorint|apeint|aprint|atmobile|'
                r'deutsche|digits|go 5g|go5g|i mobile|itmobile|jump on|jump2|layer 3|layer3|'
                r'magenta|metro|mibile|mobile t|mtmobile|my tmo|myobile|mysprin|mytmo|'
                r'mytobile|on us|onus|project 10|project ten|rmob|rtmobile|simple global|'
                r'slrint|soeint|soribt|sorint|spei|sperint|spirint|spirit cell phone|spirnt|'
                r'spprint|spri\.t|spriint|sprijt|sprimt|sprin|spritn|sprjnt|sprnt|spront|'
                r'sprrint|sptint|srint|srpint|stateside international|switch to t|sync up|'
                r'syncup|t-|t - mobile|t – mobile|t \.|t â€" mobile|t bision|t channel|'
                r't cision|t com|t kobile|t life|t lobile|t m9bile|t mbile|t mboile|t metro|'
                r't mib|t minile|t mo|t- mo|t mp|t mus|t nmobile|t nobile|t obil|t remote|'
                r't television|t tv|t vibe|t vidion|t vis|t vizion|t-,obile|t\.|t\. obile|'
                r't\.com t|t\.mo|t\.obil|t\.obile|t:|t_mobil|t\+mobile|t=mobile|tâ€'mobile|'
                r'tbile|t-bile|tbision|tbmobile|tbo|t-com|tdigit|te mobile|team mobile|'
                r'teen mobile|teenmobile|temobil|temobile|temoble|ten mobile|the mobile|'
                r'the vibe|t-home|ti-mobile|tkobile|tlife|t-life|tlivetv|tlobile|'
                r'tm coverage map|tm mobile|tm plans|tm tv|tm,obile|tm0bile|t-m0bile|tm9bile|'
                r'tmabole|tmaobile|tmb|t-mbile|tmbilw|tmbiole|tmblie|t-mbo|tmbo|tmbpile|'
                r'tmib|t-mib|tmlbile|tm-mobile|tmmoble|tmo|t-mo|tmpbile|t-mpbile|tmus|'
                r'tnmobile|tnob|t-nob|to mobile|tobile|t-obile|toblie|tobmile|tomb|tomi|'
                r'tomo|toobile|tpbile|t-phone|ttmobile|tv sion|tv vision|tviaion|'
                r'tvibe channels|tvidion|tviosion|tvis|t-vis|tvivion|tvizion|tvmo|tv-mobile|'
                r'tvsion|tv-t|tvusion|tvvis|tvzion activate|t-모바일|vibe|www t\.|www\.t|'
                r'y mo|ymo|ytmobile|т мобил|8997|5guc|5g uc|tuesday|million)'
            )                                                           THEN 'BRAND'
            ELSE 'NONBRAND'
        END AS brand_type

    FROM standardized

    -- Site URL filter: T-Mobile domain only
    WHERE site_url_standardized = 'SC-DOMAIN:T-MOBILE.COM'

      -- BYOD query inclusion filters
      AND (
          query_normalized LIKE '%bring%phone%'
          OR query_normalized LIKE '%bring%device%'
          OR query_normalized LIKE '%byod%'
      )

      -- BYOD query exclusion filters
      AND query_normalized NOT LIKE '%near%'
      AND query_normalized NOT LIKE '%pairing%'
      AND query_normalized NOT LIKE '%starlink%'
      AND query_normalized NOT LIKE '%animation%'
      AND query_normalized NOT LIKE '%iphone setup%'
      AND query_normalized NOT LIKE '%ipad setup%'
),

filtered AS (
    -- Remove EXCLUDE rows before aggregation
    SELECT *
    FROM classified
    WHERE brand_type IN ('BRAND', 'NONBRAND')
),

aggregated AS (
    SELECT
        week_sun_to_sat,
        brand_type,

        -- Additive metrics aggregated from daily to weekly
        SUM(impressions)    AS impressions,
        SUM(clicks)         AS clicks

    FROM filtered
    GROUP BY
        week_sun_to_sat,
        brand_type
)

SELECT
    week_sun_to_sat,
    brand_type,
    impressions,
    clicks
FROM aggregated
ORDER BY week_sun_to_sat ASC, brand_type ASC
;