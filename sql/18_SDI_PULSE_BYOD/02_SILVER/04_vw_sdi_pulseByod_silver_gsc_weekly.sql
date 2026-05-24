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
      query LIKE '%bring%phone%'
      OR query LIKE '%bring%device%'
      OR query LIKE '%byod%'
  - BYOD query exclusions:
      NOT LIKE '%near%', '%pairing%', '%starlink%',
      '%animation%', '%iphone setup%', '%ipad setup%'

BUSINESS LOGIC APPLIED:
  - data_source = 'GSC'
  - channel     = 'ORGANIC SEARCH'
  - Brand classification via full T-Mobile brand regex
    Matches exactly vw_sdi_tsd_silver_gsc_daily as of 2026-05-24
    Split into two REGEXP_CONTAINS calls joined with OR to avoid
    BigQuery regex length limit
    Do not modify without updating vw_sdi_tsd_silver_gsc_daily in parallel
  - LOB exclusion regex applied before brand classification
  - Daily → weekly SUM aggregation
  - week_sun_to_sat = DATE_ADD(DATE_TRUNC(event_date, WEEK(SUNDAY)), INTERVAL 6 DAY)
  - All metric columns prefixed: gsc_tmo_{brand/nonbrand}_{metric}
  - WoW: self-join on week_sun_to_sat - 7 days (gap-safe)
  - LY : self-join on custom_week_num - 52 (gap-safe, Sun-to-Sat week)
  - wow_pct and yoy_pct as decimals (e.g. 0.051 = 5.1%)
    NULL when prior value is NULL or 0
  - max_data_date: latest week_sun_to_sat with any non-null metric value

COLUMN NAMING CONVENTION:
  gsc_tmo_{brand_type}_{metric}
  gsc_tmo_{brand_type}_{metric}_wow
  gsc_tmo_{brand_type}_{metric}_ly
  gsc_tmo_{brand_type}_{metric}_wow_pct
  gsc_tmo_{brand_type}_{metric}_yoy_pct

  Where brand_type : brand, nonbrand
  Where metric     : impressions, clicks

CUSTOM WEEK NUMBER:
  Anchored to 2023-01-01 (a Sunday) for consistent Sun-to-Sat week numbering:
    custom_week_num = DATE_DIFF(DATE_SUB(week_sun_to_sat, INTERVAL 6 DAY), DATE '2023-01-01', WEEK)
  LY match: current.custom_week_num - prior.custom_week_num = 52

KEY MODELING NOTES:
  - Query normalized (LOWER, TRIM) for classification only — raw text not stored
  - brand_type uses lowercase ('brand', 'nonbrand') throughout for consistency
    with CONCAT in metric name construction
  - LOB exclusion applied before brand regex — consistent with vw_sdi_tsd_silver_gsc_daily
  - Aggregation before pivot keeps the grain clean
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
-- STEP 1: Normalize, filter site URL and BYOD queries, classify brand
-- brand_type is lowercase throughout: 'brand', 'nonbrand', 'exclude'
-- Query normalized (LOWER, TRIM) for classification only
-- LOB exclusion applied before brand regex — consistent with
-- vw_sdi_tsd_silver_gsc_daily
-- Brand regex split into two REGEXP_CONTAINS calls joined with OR
-- to avoid BigQuery regex length limit
-- -----------------------------------------------------------------------
WITH classified AS (
    SELECT
        DATE_ADD(DATE_TRUNC(event_date, WEEK(SUNDAY)), INTERVAL 6 DAY)  AS week_sun_to_sat,
        impressions,
        clicks,
        CASE
            -- Exclude empty queries
            WHEN LOWER(TRIM(query)) IS NULL
              OR TRIM(LOWER(TRIM(query))) = ''
            THEN 'exclude'

            -- Exclude LOB patterns
            -- Consistent with vw_sdi_tsd_silver_gsc_daily
            WHEN REGEXP_CONTAINS(
                LOWER(TRIM(query)),
                r'(business|home-internet|prepaid\.|fiber\.|careers\.|promotions\.)'
            )
            THEN 'exclude'

            -- Brand classification via full T-Mobile brand regex
            -- Split into two calls to avoid BigQuery regex length limit
            -- Matches exactly vw_sdi_tsd_silver_gsc_daily as of 2026-05-24
            -- Do not modify without updating vw_sdi_tsd_silver_gsc_daily in parallel
            WHEN (
                REGEXP_CONTAINS(
                    LOWER(TRIM(query)),
                    r'(^t$|\. t|\.t|â„¢obile|aorint|apeint|aprint|atmobile|deutsche|digits|go 5g|go5g|i mobile|itmobile|jump on|jump2|layer 3|layer3|magenta|metro|mibile|mobile t|mtmobile|my tmo|myobile|mysprin|mytmo|mytobile|on us|onus|project 10|project ten|rmob|rtmobile|simple global|slrint|soeint|soribt|sorint|spei|sperint|spirint|spirit cell phone|spirnt|spprint|spri\.t|spriint|sprijt|sprimt|sprin|spritn|sprjnt|sprnt|spront|sprrint|sptint|srint|srpint|stateside international|switch to t|sync up|syncup|t-|t - mobile|t – mobile|t \.|t â€" mobile|t bision|t channel|t cision|t com|t kobile|t life|t lobile|t m9bile|t mÃ³vil|t mbile|t mboile|t metro|t mib|t minile|t mo|t- mo|t mp|t mus|t nmobile|t nobile|t obil|t remote|t television|t tv|t vibe|t vidion|t vis|t vizion|t-,obile|t\.|t\. obile|t\.com t|t\.mo|t\.obil|t\.obile|t:|t_mobil|t\+mobile|t=mobile|tâ€'mobile|tbile|t-bile|tbision|tbmobile|tbo|t-com|tdigit|te mobile|team|team mobile|teen mobile|teenmobile|temobil|temobile|temoble|ten mobile|the mobile|the vibe|t-home|tim|ti-mobile|tkobile|tlife|t-life|tlivetv|tlobile)'
                )
                OR
                REGEXP_CONTAINS(
                    LOWER(TRIM(query)),
                    r'(tm coverage map|tm mobile|tm plans|tm tv|tm,obile|tm0bile|t-m0bile|tm9bile|tmabole|tmaobile|tmb|t-mbile|tmbilw|tmbiole|tmblie|t-mbo|tmbo|t-mbo|tmbpile|tmib|t-mib|tmlbile|tm-mobile|tmmoble|tmo|t-mo|tmpbile|t-mpbile|tmus|tnmobile|tnob|t-nob|to mobile|tobile|t-obile|toblie|tobmile|tomb|tomi|tomo|toobile|tpbile|t-phone|ttmobile|tv sion|tv vision|tviaion|tvibe channels|tvidion|tviosion|tvis|t-vis|tvivion|tvizion|tvmo|tv-mobile|tvsion|tv-t|tvusion|tvvis|tvzion activate|t-모바일|vibe|www t\.|www\.t|y mo|ymo|ytmobile|т мобил|8997|5guc|5g uc|tuesday|million)'
                )
            )
            THEN 'brand'

            ELSE 'nonbrand'
        END                                                              AS brand_type

    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_bronze_gscQuery_daily`

    -- Site URL filter: T-Mobile domain only
    WHERE UPPER(TRIM(site_url)) = 'SC-DOMAIN:T-MOBILE.COM'

      -- BYOD query inclusion filters
      AND (
          LOWER(TRIM(query)) LIKE '%bring%phone%'
          OR LOWER(TRIM(query)) LIKE '%bring%device%'
          OR LOWER(TRIM(query)) LIKE '%byod%'
      )

      -- BYOD query exclusion filters
      AND LOWER(TRIM(query)) NOT LIKE '%near%'
      AND LOWER(TRIM(query)) NOT LIKE '%pairing%'
      AND LOWER(TRIM(query)) NOT LIKE '%starlink%'
      AND LOWER(TRIM(query)) NOT LIKE '%animation%'
      AND LOWER(TRIM(query)) NOT LIKE '%iphone setup%'
      AND LOWER(TRIM(query)) NOT LIKE '%ipad setup%'
),

-- -----------------------------------------------------------------------
-- STEP 2: Remove exclude rows, aggregate daily → weekly
-- SUM all additive metrics per week + brand_type
-- Aggregation before pivot keeps the grain clean
-- -----------------------------------------------------------------------
aggregated AS (
    SELECT
        week_sun_to_sat,
        brand_type,
        SUM(impressions)    AS impressions,
        SUM(clicks)         AS clicks
    FROM classified
    WHERE brand_type IN ('brand', 'nonbrand')
    GROUP BY
        week_sun_to_sat,
        brand_type
),

-- -----------------------------------------------------------------------
-- STEP 3: Pivot long → wide
-- One row per week with brand and nonbrand as separate columns
-- Prefixed with 'gsc_' for unambiguous Gold identification
-- -----------------------------------------------------------------------
pivoted AS (
    SELECT
        week_sun_to_sat,

        -- Brand
        MAX(CASE WHEN brand_type = 'brand'    THEN impressions END)     AS gsc_tmo_brand_impressions,
        MAX(CASE WHEN brand_type = 'brand'    THEN clicks      END)     AS gsc_tmo_brand_clicks,

        -- Nonbrand
        MAX(CASE WHEN brand_type = 'nonbrand' THEN impressions END)     AS gsc_tmo_nonbrand_impressions,
        MAX(CASE WHEN brand_type = 'nonbrand' THEN clicks      END)     AS gsc_tmo_nonbrand_clicks

    FROM aggregated
    GROUP BY week_sun_to_sat
),

-- -----------------------------------------------------------------------
-- STEP 4: Add custom Sun-to-Sat week number for gap-safe LY matching
-- Anchored to 2023-01-01 (a Sunday)
-- custom_week_num used in LY self-join to match same Sun-to-Sat week
-- number last year regardless of gaps in data
-- -----------------------------------------------------------------------
with_week_num AS (
    SELECT
        *,
        DATE_DIFF(
            DATE_SUB(week_sun_to_sat, INTERVAL 6 DAY),
            DATE '2023-01-01',
            WEEK
        )                                                               AS custom_week_num
    FROM pivoted
),

-- -----------------------------------------------------------------------
-- STEP 5: WoW and LY self-joins
-- Both joins on small pivoted CTE (1 row per week — extremely cheap)
-- WoW: current week = prior week + 7 days (gap-safe)
-- LY : same Sun-to-Sat week number 52 weeks prior (gap-safe)
-- NULL when prior week is missing — no fake values introduced
-- -----------------------------------------------------------------------
with_comparisons AS (
    SELECT
        c.week_sun_to_sat,
        c.custom_week_num,

        -- Current values
        c.gsc_tmo_brand_impressions,
        c.gsc_tmo_brand_clicks,
        c.gsc_tmo_nonbrand_impressions,
        c.gsc_tmo_nonbrand_clicks,

        -- WoW values (prior week)
        w.gsc_tmo_brand_impressions                 AS gsc_tmo_brand_impressions_wow,
        w.gsc_tmo_brand_clicks                      AS gsc_tmo_brand_clicks_wow,
        w.gsc_tmo_nonbrand_impressions              AS gsc_tmo_nonbrand_impressions_wow,
        w.gsc_tmo_nonbrand_clicks                   AS gsc_tmo_nonbrand_clicks_wow,

        -- LY values (same Sun-to-Sat week last year)
        l.gsc_tmo_brand_impressions                 AS gsc_tmo_brand_impressions_ly,
        l.gsc_tmo_brand_clicks                      AS gsc_tmo_brand_clicks_ly,
        l.gsc_tmo_nonbrand_impressions              AS gsc_tmo_nonbrand_impressions_ly,
        l.gsc_tmo_nonbrand_clicks                   AS gsc_tmo_nonbrand_clicks_ly

    FROM with_week_num c

    -- WoW: gap-safe join on exact 7-day prior week
    LEFT JOIN with_week_num w
      ON c.week_sun_to_sat = DATE_ADD(w.week_sun_to_sat, INTERVAL 7 DAY)

    -- LY: gap-safe join on same Sun-to-Sat week number 52 weeks prior
    LEFT JOIN with_week_num l
      ON (c.custom_week_num - l.custom_week_num) = 52
),

-- -----------------------------------------------------------------------
-- STEP 6: Compute wow_pct and yoy_pct for all metrics
-- Decimal format: 0.051 = 5.1%
-- NULL when prior value is NULL (no data) or 0 (division undefined)
-- No fake zeroes introduced
-- -----------------------------------------------------------------------
with_pcts AS (
    SELECT
        week_sun_to_sat,
        custom_week_num,

        -- ---- Brand impressions ----
        gsc_tmo_brand_impressions,
        gsc_tmo_brand_impressions_wow,
        gsc_tmo_brand_impressions_ly,
        CASE
            WHEN gsc_tmo_brand_impressions_wow IS NULL
              OR gsc_tmo_brand_impressions_wow = 0    THEN NULL
            ELSE ROUND(
                (gsc_tmo_brand_impressions - gsc_tmo_brand_impressions_wow)
                / gsc_tmo_brand_impressions_wow, 6)
        END                                                             AS gsc_tmo_brand_impressions_wow_pct,
        CASE
            WHEN gsc_tmo_brand_impressions_ly IS NULL
              OR gsc_tmo_brand_impressions_ly = 0     THEN NULL
            ELSE ROUND(
                (gsc_tmo_brand_impressions - gsc_tmo_brand_impressions_ly)
                / gsc_tmo_brand_impressions_ly, 6)
        END                                                             AS gsc_tmo_brand_impressions_yoy_pct,

        -- ---- Brand clicks ----
        gsc_tmo_brand_clicks,
        gsc_tmo_brand_clicks_wow,
        gsc_tmo_brand_clicks_ly,
        CASE
            WHEN gsc_tmo_brand_clicks_wow IS NULL
              OR gsc_tmo_brand_clicks_wow = 0         THEN NULL
            ELSE ROUND(
                (gsc_tmo_brand_clicks - gsc_tmo_brand_clicks_wow)
                / gsc_tmo_brand_clicks_wow, 6)
        END                                                             AS gsc_tmo_brand_clicks_wow_pct,
        CASE
            WHEN gsc_tmo_brand_clicks_ly IS NULL
              OR gsc_tmo_brand_clicks_ly = 0          THEN NULL
            ELSE ROUND(
                (gsc_tmo_brand_clicks - gsc_tmo_brand_clicks_ly)
                / gsc_tmo_brand_clicks_ly, 6)
        END                                                             AS gsc_tmo_brand_clicks_yoy_pct,

        -- ---- Nonbrand impressions ----
        gsc_tmo_nonbrand_impressions,
        gsc_tmo_nonbrand_impressions_wow,
        gsc_tmo_nonbrand_impressions_ly,
        CASE
            WHEN gsc_tmo_nonbrand_impressions_wow IS NULL
              OR gsc_tmo_nonbrand_impressions_wow = 0 THEN NULL
            ELSE ROUND(
                (gsc_tmo_nonbrand_impressions - gsc_tmo_nonbrand_impressions_wow)
                / gsc_tmo_nonbrand_impressions_wow, 6)
        END                                                             AS gsc_tmo_nonbrand_impressions_wow_pct,
        CASE
            WHEN gsc_tmo_nonbrand_impressions_ly IS NULL
              OR gsc_tmo_nonbrand_impressions_ly = 0  THEN NULL
            ELSE ROUND(
                (gsc_tmo_nonbrand_impressions - gsc_tmo_nonbrand_impressions_ly)
                / gsc_tmo_nonbrand_impressions_ly, 6)
        END                                                             AS gsc_tmo_nonbrand_impressions_yoy_pct,

        -- ---- Nonbrand clicks ----
        gsc_tmo_nonbrand_clicks,
        gsc_tmo_nonbrand_clicks_wow,
        gsc_tmo_nonbrand_clicks_ly,
        CASE
            WHEN gsc_tmo_nonbrand_clicks_wow IS NULL
              OR gsc_tmo_nonbrand_clicks_wow = 0      THEN NULL
            ELSE ROUND(
                (gsc_tmo_nonbrand_clicks - gsc_tmo_nonbrand_clicks_wow)
                / gsc_tmo_nonbrand_clicks_wow, 6)
        END                                                             AS gsc_tmo_nonbrand_clicks_wow_pct,
        CASE
            WHEN gsc_tmo_nonbrand_clicks_ly IS NULL
              OR gsc_tmo_nonbrand_clicks_ly = 0       THEN NULL
            ELSE ROUND(
                (gsc_tmo_nonbrand_clicks - gsc_tmo_nonbrand_clicks_ly)
                / gsc_tmo_nonbrand_clicks_ly, 6)
        END                                                             AS gsc_tmo_nonbrand_clicks_yoy_pct

    FROM with_comparisons
),

-- -----------------------------------------------------------------------
-- STEP 7: max_data_date per source
-- Latest week_sun_to_sat where any metric is non-null
-- Updates dynamically as new data arrives
-- -----------------------------------------------------------------------
with_max_date AS (
    SELECT
        *,
        MAX(CASE
            WHEN gsc_tmo_brand_impressions    IS NOT NULL
              OR gsc_tmo_nonbrand_impressions IS NOT NULL
            THEN week_sun_to_sat
        END) OVER ()                                                    AS max_data_date
    FROM with_pcts
)

-- -----------------------------------------------------------------------
-- FINAL OUTPUT
-- Wide table — one row per week_sun_to_sat
-- All columns prefixed with 'gsc_'
-- data_source and channel as static columns
-- -----------------------------------------------------------------------
SELECT
    week_sun_to_sat,
    'GSC'                                                               AS data_source,
    'ORGANIC SEARCH'                                                    AS channel,
    max_data_date,

    -- Brand impressions
    gsc_tmo_brand_impressions,
    gsc_tmo_brand_impressions_wow,
    gsc_tmo_brand_impressions_ly,
    gsc_tmo_brand_impressions_wow_pct,
    gsc_tmo_brand_impressions_yoy_pct,

    -- Brand clicks
    gsc_tmo_brand_clicks,
    gsc_tmo_brand_clicks_wow,
    gsc_tmo_brand_clicks_ly,
    gsc_tmo_brand_clicks_wow_pct,
    gsc_tmo_brand_clicks_yoy_pct,

    -- Nonbrand impressions
    gsc_tmo_nonbrand_impressions,
    gsc_tmo_nonbrand_impressions_wow,
    gsc_tmo_nonbrand_impressions_ly,
    gsc_tmo_nonbrand_impressions_wow_pct,
    gsc_tmo_nonbrand_impressions_yoy_pct,

    -- Nonbrand clicks
    gsc_tmo_nonbrand_clicks,
    gsc_tmo_nonbrand_clicks_wow,
    gsc_tmo_nonbrand_clicks_ly,
    gsc_tmo_nonbrand_clicks_wow_pct,
    gsc_tmo_nonbrand_clicks_yoy_pct

FROM with_max_date
;