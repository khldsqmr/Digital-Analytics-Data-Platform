/* =================================================================================================
FILE: 03_vw_sdi_tsd_silver_gsc_daily.sql
LAYER: Silver View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_silver_gsc_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_gscQuery_daily

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_gsc_daily

PURPOSE:
  Canonical Silver GSC daily source mart for the Total Search Dashboard.

BUSINESS GRAIN:
  One row per:
      event_date
      lob
      channel

OUTPUT METRICS:
  - gsc_clicks_brand
  - gsc_clicks_nonbrand
  - gsc_clicks_all
  - gsc_impressions_brand
  - gsc_impressions_nonbrand
  - gsc_impressions_all

KEY MODELING NOTES:
  - Query-level rows are classified first, then aggregated
  - Postpaid LOB logic excludes queries matching the provided Postpaid exclusion regex
  - Brand / non-brand logic uses the provided brand regex exactly as supplied
  - Nulls are preserved; no new zeroes are introduced
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_gsc_daily`
AS

WITH classified AS (
    SELECT
        event_date,
        'POSTPAID' AS lob,
        'ORGANIC SEARCH' AS channel,
        SAFE_CAST(clicks AS FLOAT64) AS clicks,
        SAFE_CAST(impressions AS FLOAT64) AS impressions,
        CASE
            WHEN query IS NULL OR TRIM(query) = '' THEN 'EXCLUDE'
            WHEN REGEXP_CONTAINS(
                LOWER(TRIM(query)),
                r'(business|home-internet|prepaid.|fiber.|careers.|promotions.)'
            ) THEN 'EXCLUDE'
            WHEN REGEXP_CONTAINS(
                LOWER(TRIM(query)),
                r'(^t$|\. t|\.t|â„¢obile|aorint|apeint|aprint|atmobile|deutsche|digits|go 5g|go5g|i mobile|itmobile|jump on|jump2|layer 3|layer3|magenta|metro|mibile|mobile t|mtmobile|my tmo|myobile|mysprin|mytmo|mytobile|on us|onus|project 10|project ten|rmob|rtmobile|simple global|slrint|soeint|soribt|sorint|spei|sperint|spirint|spirit cell phone|spirnt|spprint|spri\.t|spriint|sprijt|sprimt|sprin|spritn|sprjnt|sprnt|spront|sprrint|sptint|srint|srpint|stateside international|switch to t|sync up|syncup|t-|t - mobile|t – mobile|t \.|t â€“ mobile|t bision|t channel|t cision|t com|t kobile|t life|t lobile|t m9bile|t mÃ³vil|t mbile|t mboile|t metro|t mib|t minile|t mo|t- mo|t mp|t mus|t nmobile|t nobile|t obil|t remote|t television|t tv|t vibe|t vidion|t vis|t vizion|t-,obile|t\.|t\. obile|t\.com t|t\.mo|t\.obil|t\.obile|t:|t_mobil|t\+mobile|t=mobile|tâ€‘mobile|tbile|t-bile|tbision|tbmobile|tbo|t-com|tdigit|te mobile|team|team mobile|teen mobile|teenmobile|temobil|temobile|temoble|ten mobile|the mobile|the vibe|t-home|tim|ti-mobile|tkobile|tlife|t-life|tlivetv|tlobile|tm coverage map|tm mobile|tm plans|tm tv|tm,obile|tm0bile|t-m0bile|tm9bile|tmabole|tmaobile|tmb|t-mbile|tmbilw|tmbiole|tmblie|t-mbo|tmbo|t-mbo|tmbpile|tmib|t-mib|tmlbile|tm-mobile|tmmoble|tmo|t-mo|tmpbile|t-mpbile|tmus|tnmobile|tnob|t-nob|to mobile|tobile|t-obile|toblie|tobmile|tomb|tomi|tomo|toobile|tpbile|t-phone|ttmobile|tv sion|tv vision|tviaion|tvibe channels|tvidion|tviosion|tvis|t-vis|tvivion|tvizion|tvmo|tv-mobile|tvsion|tv-t|tvusion|tvvis|tvzion activate|t-모바일|vibe|www t\.|www\.t|y mo|ymo|ytmobile|т мобил|8997|5guc|5g uc|tuesday|million)'
            ) THEN 'BRAND'
            ELSE 'NONBRAND'
        END AS brand_type
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_gscQuery_daily`
    WHERE UPPER(TRIM(site_url)) = 'SC-DOMAIN:T-MOBILE.COM'
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
        SUM(CASE WHEN brand_type = 'BRAND' THEN clicks END) AS gsc_clicks_brand,
        SUM(CASE WHEN brand_type = 'NONBRAND' THEN clicks END) AS gsc_clicks_nonbrand,
        SUM(CASE WHEN brand_type = 'BRAND' THEN impressions END) AS gsc_impressions_brand,
        SUM(CASE WHEN brand_type = 'NONBRAND' THEN impressions END) AS gsc_impressions_nonbrand
    FROM filtered
    GROUP BY 1, 2, 3
)

SELECT
    event_date,
    lob,
    channel,

    gsc_clicks_brand,
    gsc_clicks_nonbrand,
    CASE
        WHEN gsc_clicks_brand IS NULL AND gsc_clicks_nonbrand IS NULL THEN NULL
        ELSE COALESCE(gsc_clicks_brand, 0) + COALESCE(gsc_clicks_nonbrand, 0)
    END AS gsc_clicks_all,

    gsc_impressions_brand,
    gsc_impressions_nonbrand,
    CASE
        WHEN gsc_impressions_brand IS NULL AND gsc_impressions_nonbrand IS NULL THEN NULL
        ELSE COALESCE(gsc_impressions_brand, 0) + COALESCE(gsc_impressions_nonbrand, 0)
    END AS gsc_impressions_all
FROM aggregated
;