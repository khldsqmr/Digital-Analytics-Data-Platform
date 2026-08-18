/* =================================================================================================
FILE:         sdi_sp_dashboardPulseByod_silver_gsc_weekly.sql
LAYER:        Silver (via Stored Procedure)
CATALOG.SCHEMA: prdrzranalytics.lab42
TABLE:        sdi_tbl_dashboardPulseByod_silver_gsc_weekly
PROCEDURE:    sdi_sp_dashboardPulseByod_silver_gsc_weekly

SOURCE:
  prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_bronze_gscQuery_daily

PURPOSE:
  Silver table for Google Search Console organic search data.
  Outputs a WIDE table — one row per week_sun_to_sat.
  All metric columns prefixed with 'gsc_' for unambiguous identification in Gold Wide
  spine join and Gold Long unpivot. Applies site URL filter, BYOD query filters,
  brand/nonbrand classification, daily -> weekly aggregation, WoW/LY comparisons,
  and max_data_date.

BUSINESS GRAIN:
  One row per week_sun_to_sat

FILTERS APPLIED:
  - site_url = 'SC-DOMAIN:T-MOBILE.COM'
  - BYOD query inclusion: query LIKE '%bring%phone%' OR '%bring%device%' OR '%byod%'
  - BYOD query exclusion: NOT LIKE '%near%', '%pairing%', '%starlink%', '%animation%',
    '%iphone setup%', '%ipad setup%'

BUSINESS LOGIC APPLIED:
  - data_source = 'GSC'
  - channel     = 'Organic Search'
  - Brand classification via full T-Mobile brand regex, matching vw_sdi_tsd_silver_gsc_daily
    exactly. Split into two RLIKE calls joined with OR — same split BQ used to stay under its
    regex length limit; kept identical here for parity, not a Databricks requirement.
    Do not modify without updating vw_sdi_tsd_silver_gsc_daily in parallel.
  - LOB exclusion regex applied before brand classification (matches vw_sdi_tsd_silver_gsc_daily)
  - Daily -> weekly SUM aggregation
  - week_sun_to_sat: BQ original used DATE_ADD(DATE_TRUNC(event_date, WEEK(SUNDAY)), INTERVAL 6 DAY).
    Databricks DATE_TRUNC('WEEK', ...) truncates to Monday (ISO), not Sunday, so computed manually:
      week_sun_to_sat = DATE_ADD(DATE_SUB(event_date, DAYOFWEEK(event_date) - 1), 6)
  - All metric columns prefixed: gsc_tmo_{brand/nonbrand}_{metric}
  - WoW: self-join on week_sun_to_sat - 7 days (gap-safe)
  - LY:  self-join on custom_week_num - 52 (gap-safe, Sun-to-Sat anchor)
  - wow_pct / yoy_pct as decimals — NULL when prior NULL or 0
  - max_data_date: latest week_sun_to_sat with any non-null metric

METRICS (2 — matches original BQ Silver exactly): impressions, clicks

COLUMN NAMING CONVENTION:
  gsc_tmo_{brand_type}_{metric}[_wow|_ly|_wow_pct|_yoy_pct]
  Where brand_type: brand, nonbrand
  Where metric: impressions, clicks

CUSTOM WEEK NUMBER:
  custom_week_num = DATEDIFF(WEEK, DATE '2023-01-01', DATE_SUB(week_sun_to_sat, 6))

DOWNSTREAM:
  Gold Wide : sdi_tbl_dashboardPulseByod_gold_unified_wide
  Gold Long : sdi_tbl_dashboardPulseByod_gold_unified_long
================================================================================================= */

CREATE OR REPLACE PROCEDURE
prdrzranalytics.lab42.sdi_sp_dashboardPulseByod_silver_gsc_weekly()
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
AS
BEGIN

  CREATE OR REPLACE TABLE
  prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_gsc_weekly
  USING DELTA
  AS

  WITH classified AS (
      SELECT
          DATE_ADD(DATE_SUB(event_date, DAYOFWEEK(event_date) - 1), 6) AS week_sun_to_sat,
          impressions,
          clicks,
          CASE
              WHEN LOWER(TRIM(query)) IS NULL OR TRIM(LOWER(TRIM(query))) = ''
              THEN 'exclude'

              WHEN LOWER(TRIM(query)) RLIKE '(business|home-internet|prepaid\\.|fiber\\.|careers\\.|promotions\\.)'
              THEN 'exclude'

              WHEN (
                  LOWER(TRIM(query)) RLIKE '(^t$|\\. t|\\.t|â„¢obile|aorint|apeint|aprint|atmobile|deutsche|digits|go 5g|go5g|i mobile|itmobile|jump on|jump2|layer 3|layer3|magenta|metro|mibile|mobile t|mtmobile|my tmo|myobile|mysprin|mytmo|mytobile|on us|onus|project 10|project ten|rmob|rtmobile|simple global|slrint|soeint|soribt|sorint|spei|sperint|spirint|spirit cell phone|spirnt|spprint|spri\\.t|spriint|sprijt|sprimt|sprin|spritn|sprjnt|sprnt|spront|sprrint|sptint|srint|srpint|stateside international|switch to t|sync up|syncup|t-|t - mobile|t – mobile|t \\.|t â€" mobile|t bision|t channel|t cision|t com|t kobile|t life|t lobile|t m9bile|t mÃ³vil|t mbile|t mboile|t metro|t mib|t minile|t mo|t- mo|t mp|t mus|t nmobile|t nobile|t obil|t remote|t television|t tv|t vibe|t vidion|t vis|t vizion|t-,obile|t\\.|t\\. obile|t\\.com t|t\\.mo|t\\.obil|t\\.obile|t:|t_mobil|t\\+mobile|t=mobile|tâ€.mobile|tbile|t-bile|tbision|tbmobile|tbo|t-com|tdigit|te mobile|team|team mobile|teen mobile|teenmobile|temobil|temobile|temoble|ten mobile|the mobile|the vibe|t-home|tim|ti-mobile|tkobile|tlife|t-life|tlivetv|tlobile)'
                  OR
                  LOWER(TRIM(query)) RLIKE '(tm coverage map|tm mobile|tm plans|tm tv|tm,obile|tm0bile|t-m0bile|tm9bile|tmabole|tmaobile|tmb|t-mbile|tmbilw|tmbiole|tmblie|t-mbo|tmbo|t-mbo|tmbpile|tmib|t-mib|tmlbile|tm-mobile|tmmoble|tmo|t-mo|tmpbile|t-mpbile|tmus|tnmobile|tnob|t-nob|to mobile|tobile|t-obile|toblie|tobmile|tomb|tomi|tomo|toobile|tpbile|t-phone|ttmobile|tv sion|tv vision|tviaion|tvibe channels|tvidion|tviosion|tvis|t-vis|tvivion|tvizion|tvmo|tv-mobile|tvsion|tv-t|tvusion|tvvis|tvzion activate|t-모바일|vibe|www t\\.|www\\.t|y mo|ymo|ytmobile|т мобил|8997|5guc|5g uc|tuesday|million)'
              )
              THEN 'brand'

              ELSE 'nonbrand'
          END AS brand_type

      FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_bronze_gscQuery_daily

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
      SELECT
          week_sun_to_sat,
          brand_type,
          SUM(impressions) AS impressions,
          SUM(clicks)      AS clicks
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
      SELECT *,
          DATEDIFF(WEEK, DATE '2023-01-01', DATE_SUB(week_sun_to_sat, 6)) AS custom_week_num
      FROM pivoted
  ),

  with_comparisons AS (
      SELECT
          c.week_sun_to_sat,
          c.custom_week_num,

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
      LEFT JOIN with_week_num w ON c.week_sun_to_sat = DATE_ADD(w.week_sun_to_sat, 7)
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
          MAX(CASE
              WHEN gsc_tmo_brand_impressions    IS NOT NULL
                OR gsc_tmo_nonbrand_impressions IS NOT NULL
              THEN week_sun_to_sat END) OVER () AS max_data_date
      FROM with_pcts
  )

  SELECT
      week_sun_to_sat,
      'GSC'                                        AS data_source,
      'Organic Search'                             AS channel,
      max_data_date,

      gsc_tmo_brand_impressions, gsc_tmo_brand_impressions_wow, gsc_tmo_brand_impressions_ly, gsc_tmo_brand_impressions_wow_pct, gsc_tmo_brand_impressions_yoy_pct,
      gsc_tmo_brand_clicks,      gsc_tmo_brand_clicks_wow,      gsc_tmo_brand_clicks_ly,      gsc_tmo_brand_clicks_wow_pct,      gsc_tmo_brand_clicks_yoy_pct,

      gsc_tmo_nonbrand_impressions, gsc_tmo_nonbrand_impressions_wow, gsc_tmo_nonbrand_impressions_ly, gsc_tmo_nonbrand_impressions_wow_pct, gsc_tmo_nonbrand_impressions_yoy_pct,
      gsc_tmo_nonbrand_clicks,      gsc_tmo_nonbrand_clicks_wow,      gsc_tmo_nonbrand_clicks_ly,      gsc_tmo_nonbrand_clicks_wow_pct,      gsc_tmo_nonbrand_clicks_yoy_pct

  FROM with_max_date
  ;

END;