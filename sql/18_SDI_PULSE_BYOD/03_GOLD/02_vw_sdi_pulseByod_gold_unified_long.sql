/* =================================================================================================
FILE:         02_vw_sdi_pulseByod_gold_unified_long.sql
LAYER:        Gold View — Long
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseByod_gold_unified_long

SOURCES:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profound_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profoundGofish_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_sa360_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_gsc_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_adobe_weekly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_long

PURPOSE:
  Gold Long view for the Pulse BYOD dashboard.
  Reads directly from Silver views — NOT from Gold Wide.
  Each Silver view is unpivoted independently and combined via UNION ALL.
  This ensures each source is scanned exactly once — fast and cheap.
  One row per metric per week.
  time_granularity = 'WEEKLY' for self-describing schema.
  Keywords unpivoted from Trends Silver with KEYWORD_RANK_1 through
  KEYWORD_RANK_5 as dimension_name for Top N Keywords visualization.
  Each keyword rank produces two rows per week:
    - metric_name = 'trends_kw_interest' : relative interest score (0-100)
    - metric_name = 'trends_kw_change'   : source-supplied WoW change from
      Google Trends (e.g. -0.03 = interest decreased 3% vs prior week).
      NOTE: this value comes pre-computed from the Trends source pipeline.
      It is NOT calculated in BigQuery. wow_pct/yoy_pct are NULL for both
      keyword metric rows — keywords change position week to week, making
      BQ-computed comparisons unreliable.
  Adobe channel derived from metric_name suffix in combined CTE.

OUTPUT SCHEMA:
  week_sun_to_sat  : DATE    — Week ending Saturday (Sun-to-Sat)
  time_granularity : STRING  — Always 'WEEKLY'
  data_source      : STRING  — PROFOUND, GOFISH, SA360, GSC, TRENDS, ADOBE
  channel          : STRING  — AI SEARCH, PAID SEARCH, ORGANIC SEARCH,
                               ALL CHANNELS, DIRECT, SOCIAL, PROGRAMMATIC, OTHER
  dimension_name   : STRING  — KEYWORD_RANK_1..5 for Trends; NULL otherwise
  dimension_value  : STRING  — Keyword text for Trends; NULL otherwise
  metric_name      : STRING  — Full prefixed metric name
  metric_value     : FLOAT64 — Current week metric value
  metric_value_wow : FLOAT64 — Prior week value
  metric_value_ly  : FLOAT64 — Same Sun-to-Sat week last year
  wow_pct          : FLOAT64 — WoW% as decimal (e.g. 0.051 = 5.1%)
  yoy_pct          : FLOAT64 — YoY% as decimal
  max_data_date    : DATE    — Latest week with non-null data per data_source

PERFORMANCE NOTES:
  - Each Silver view scanned exactly once — no re-scanning
  - No Gold Wide dependency — independent of spine join
  - Each source unpivoted in its own CTE via BigQuery UNPIVOT operator
  - UNION ALL in combined CTE assembles all sources
  - max_data_date per data_source via MAX() OVER (PARTITION BY data_source)

CHANGES:
  2026-05-28
  - trends_kw_change rows added to trends_keywords_long CTE
    These values are sourced directly from the Trends pipeline (not BQ-computed).
    Each keyword rank now produces 2 rows per week: trends_kw_interest
    and trends_kw_change. wow_pct / yoy_pct remain NULL for both keyword
    row types — keywords change position week to week.
    Filter condition matches trends_kw_interest rows: keyword text non-null only.
    Earlier rows (pre-2026-05-09) will show 0.0 for kw_change due to known
    pipeline backfill gap — consistent with existing kw_interest behavior.

ADDING NEW SOURCES:
  1. Create Bronze + Silver views
  2. Add new UNPIVOT CTE below
  3. Add to combined CTE UNION ALL
  No structural changes needed

DOWNSTREAM:
  Gold Long : this view — used directly by dashboard
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_long`
AS

WITH

-- -----------------------------------------------------------------------
-- PROFOUND: NON-BRAND AI Visibility
-- 12 metric rows per week (4 metrics × 3 assets: T-Mobile, Verizon, AT&T)
-- data_source = 'PROFOUND', channel = 'AI SEARCH' from Silver
-- -----------------------------------------------------------------------
profound_long AS (
    SELECT
        week_sun_to_sat,
        data_source,
        channel,
        max_data_date,
        metric_name,
        metric_value,
        metric_value_wow,
        metric_value_ly,
        wow_pct,
        yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profound_weekly`
    UNPIVOT (
        (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
        FOR metric_name IN (
            (profound_tmo_nonbrand_visibility_score,    profound_tmo_nonbrand_visibility_score_wow,    profound_tmo_nonbrand_visibility_score_ly,    profound_tmo_nonbrand_visibility_score_wow_pct,    profound_tmo_nonbrand_visibility_score_yoy_pct)    AS 'profound_tmo_nonbrand_visibility_score',
            (profound_tmo_nonbrand_executions,          profound_tmo_nonbrand_executions_wow,          profound_tmo_nonbrand_executions_ly,          profound_tmo_nonbrand_executions_wow_pct,          profound_tmo_nonbrand_executions_yoy_pct)          AS 'profound_tmo_nonbrand_executions',
            (profound_tmo_nonbrand_mentions_count,      profound_tmo_nonbrand_mentions_count_wow,      profound_tmo_nonbrand_mentions_count_ly,      profound_tmo_nonbrand_mentions_count_wow_pct,      profound_tmo_nonbrand_mentions_count_yoy_pct)      AS 'profound_tmo_nonbrand_mentions_count',
            (profound_tmo_nonbrand_share_of_voice,      profound_tmo_nonbrand_share_of_voice_wow,      profound_tmo_nonbrand_share_of_voice_ly,      profound_tmo_nonbrand_share_of_voice_wow_pct,      profound_tmo_nonbrand_share_of_voice_yoy_pct)      AS 'profound_tmo_nonbrand_share_of_voice',
            (profound_verizon_nonbrand_visibility_score,profound_verizon_nonbrand_visibility_score_wow,profound_verizon_nonbrand_visibility_score_ly,profound_verizon_nonbrand_visibility_score_wow_pct,profound_verizon_nonbrand_visibility_score_yoy_pct) AS 'profound_verizon_nonbrand_visibility_score',
            (profound_verizon_nonbrand_executions,      profound_verizon_nonbrand_executions_wow,      profound_verizon_nonbrand_executions_ly,      profound_verizon_nonbrand_executions_wow_pct,      profound_verizon_nonbrand_executions_yoy_pct)      AS 'profound_verizon_nonbrand_executions',
            (profound_verizon_nonbrand_mentions_count,  profound_verizon_nonbrand_mentions_count_wow,  profound_verizon_nonbrand_mentions_count_ly,  profound_verizon_nonbrand_mentions_count_wow_pct,  profound_verizon_nonbrand_mentions_count_yoy_pct)  AS 'profound_verizon_nonbrand_mentions_count',
            (profound_verizon_nonbrand_share_of_voice,  profound_verizon_nonbrand_share_of_voice_wow,  profound_verizon_nonbrand_share_of_voice_ly,  profound_verizon_nonbrand_share_of_voice_wow_pct,  profound_verizon_nonbrand_share_of_voice_yoy_pct)  AS 'profound_verizon_nonbrand_share_of_voice',
            (profound_att_nonbrand_visibility_score,    profound_att_nonbrand_visibility_score_wow,    profound_att_nonbrand_visibility_score_ly,    profound_att_nonbrand_visibility_score_wow_pct,    profound_att_nonbrand_visibility_score_yoy_pct)    AS 'profound_att_nonbrand_visibility_score',
            (profound_att_nonbrand_executions,          profound_att_nonbrand_executions_wow,          profound_att_nonbrand_executions_ly,          profound_att_nonbrand_executions_wow_pct,          profound_att_nonbrand_executions_yoy_pct)          AS 'profound_att_nonbrand_executions',
            (profound_att_nonbrand_mentions_count,      profound_att_nonbrand_mentions_count_wow,      profound_att_nonbrand_mentions_count_ly,      profound_att_nonbrand_mentions_count_wow_pct,      profound_att_nonbrand_mentions_count_yoy_pct)      AS 'profound_att_nonbrand_mentions_count',
            (profound_att_nonbrand_share_of_voice,      profound_att_nonbrand_share_of_voice_wow,      profound_att_nonbrand_share_of_voice_ly,      profound_att_nonbrand_share_of_voice_wow_pct,      profound_att_nonbrand_share_of_voice_yoy_pct)      AS 'profound_att_nonbrand_share_of_voice'
        )
    )
),

-- -----------------------------------------------------------------------
-- GOFISH: BRAND AI Visibility
-- 12 metric rows per week (4 metrics × 3 assets: T-Mobile, Verizon, AT&T)
-- data_source = 'GOFISH', channel = 'AI SEARCH' from Silver
-- -----------------------------------------------------------------------
gofish_long AS (
    SELECT
        week_sun_to_sat,
        data_source,
        channel,
        max_data_date,
        metric_name,
        metric_value,
        metric_value_wow,
        metric_value_ly,
        wow_pct,
        yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profoundGofish_weekly`
    UNPIVOT (
        (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
        FOR metric_name IN (
            (gofish_tmo_brand_visibility_score,    gofish_tmo_brand_visibility_score_wow,    gofish_tmo_brand_visibility_score_ly,    gofish_tmo_brand_visibility_score_wow_pct,    gofish_tmo_brand_visibility_score_yoy_pct)    AS 'gofish_tmo_brand_visibility_score',
            (gofish_tmo_brand_executions,          gofish_tmo_brand_executions_wow,          gofish_tmo_brand_executions_ly,          gofish_tmo_brand_executions_wow_pct,          gofish_tmo_brand_executions_yoy_pct)          AS 'gofish_tmo_brand_executions',
            (gofish_tmo_brand_mentions_count,      gofish_tmo_brand_mentions_count_wow,      gofish_tmo_brand_mentions_count_ly,      gofish_tmo_brand_mentions_count_wow_pct,      gofish_tmo_brand_mentions_count_yoy_pct)      AS 'gofish_tmo_brand_mentions_count',
            (gofish_tmo_brand_share_of_voice,      gofish_tmo_brand_share_of_voice_wow,      gofish_tmo_brand_share_of_voice_ly,      gofish_tmo_brand_share_of_voice_wow_pct,      gofish_tmo_brand_share_of_voice_yoy_pct)      AS 'gofish_tmo_brand_share_of_voice',
            (gofish_verizon_brand_visibility_score,gofish_verizon_brand_visibility_score_wow,gofish_verizon_brand_visibility_score_ly,gofish_verizon_brand_visibility_score_wow_pct,gofish_verizon_brand_visibility_score_yoy_pct) AS 'gofish_verizon_brand_visibility_score',
            (gofish_verizon_brand_executions,      gofish_verizon_brand_executions_wow,      gofish_verizon_brand_executions_ly,      gofish_verizon_brand_executions_wow_pct,      gofish_verizon_brand_executions_yoy_pct)      AS 'gofish_verizon_brand_executions',
            (gofish_verizon_brand_mentions_count,  gofish_verizon_brand_mentions_count_wow,  gofish_verizon_brand_mentions_count_ly,  gofish_verizon_brand_mentions_count_wow_pct,  gofish_verizon_brand_mentions_count_yoy_pct)  AS 'gofish_verizon_brand_mentions_count',
            (gofish_verizon_brand_share_of_voice,  gofish_verizon_brand_share_of_voice_wow,  gofish_verizon_brand_share_of_voice_ly,  gofish_verizon_brand_share_of_voice_wow_pct,  gofish_verizon_brand_share_of_voice_yoy_pct)  AS 'gofish_verizon_brand_share_of_voice',
            (gofish_att_brand_visibility_score,    gofish_att_brand_visibility_score_wow,    gofish_att_brand_visibility_score_ly,    gofish_att_brand_visibility_score_wow_pct,    gofish_att_brand_visibility_score_yoy_pct)    AS 'gofish_att_brand_visibility_score',
            (gofish_att_brand_executions,          gofish_att_brand_executions_wow,          gofish_att_brand_executions_ly,          gofish_att_brand_executions_wow_pct,          gofish_att_brand_executions_yoy_pct)          AS 'gofish_att_brand_executions',
            (gofish_att_brand_mentions_count,      gofish_att_brand_mentions_count_wow,      gofish_att_brand_mentions_count_ly,      gofish_att_brand_mentions_count_wow_pct,      gofish_att_brand_mentions_count_yoy_pct)      AS 'gofish_att_brand_mentions_count',
            (gofish_att_brand_share_of_voice,      gofish_att_brand_share_of_voice_wow,      gofish_att_brand_share_of_voice_ly,      gofish_att_brand_share_of_voice_wow_pct,      gofish_att_brand_share_of_voice_yoy_pct)      AS 'gofish_att_brand_share_of_voice'
        )
    )
),

-- -----------------------------------------------------------------------
-- SA360: Paid Search Performance
-- 12 metric rows per week (6 metrics × 2 brand types)
-- data_source = 'SA360', channel = 'PAID SEARCH' from Silver
-- -----------------------------------------------------------------------
sa360_long AS (
    SELECT
        week_sun_to_sat,
        data_source,
        channel,
        max_data_date,
        metric_name,
        metric_value,
        metric_value_wow,
        metric_value_ly,
        wow_pct,
        yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_sa360_weekly`
    UNPIVOT (
        (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
        FOR metric_name IN (
            (sa360_tmo_brand_impressions,     sa360_tmo_brand_impressions_wow,     sa360_tmo_brand_impressions_ly,     sa360_tmo_brand_impressions_wow_pct,     sa360_tmo_brand_impressions_yoy_pct)     AS 'sa360_tmo_brand_impressions',
            (sa360_tmo_brand_clicks,          sa360_tmo_brand_clicks_wow,          sa360_tmo_brand_clicks_ly,          sa360_tmo_brand_clicks_wow_pct,          sa360_tmo_brand_clicks_yoy_pct)          AS 'sa360_tmo_brand_clicks',
            (sa360_tmo_brand_cost,            sa360_tmo_brand_cost_wow,            sa360_tmo_brand_cost_ly,            sa360_tmo_brand_cost_wow_pct,            sa360_tmo_brand_cost_yoy_pct)            AS 'sa360_tmo_brand_cost',
            (sa360_tmo_brand_orders,          sa360_tmo_brand_orders_wow,          sa360_tmo_brand_orders_ly,          sa360_tmo_brand_orders_wow_pct,          sa360_tmo_brand_orders_yoy_pct)          AS 'sa360_tmo_brand_orders',
            (sa360_tmo_brand_cart_start,      sa360_tmo_brand_cart_start_wow,      sa360_tmo_brand_cart_start_ly,      sa360_tmo_brand_cart_start_wow_pct,      sa360_tmo_brand_cart_start_yoy_pct)      AS 'sa360_tmo_brand_cart_start',
            (sa360_tmo_brand_postpaid_pspv,   sa360_tmo_brand_postpaid_pspv_wow,   sa360_tmo_brand_postpaid_pspv_ly,   sa360_tmo_brand_postpaid_pspv_wow_pct,   sa360_tmo_brand_postpaid_pspv_yoy_pct)   AS 'sa360_tmo_brand_postpaid_pspv',
            (sa360_tmo_nonbrand_impressions,  sa360_tmo_nonbrand_impressions_wow,  sa360_tmo_nonbrand_impressions_ly,  sa360_tmo_nonbrand_impressions_wow_pct,  sa360_tmo_nonbrand_impressions_yoy_pct)  AS 'sa360_tmo_nonbrand_impressions',
            (sa360_tmo_nonbrand_clicks,       sa360_tmo_nonbrand_clicks_wow,       sa360_tmo_nonbrand_clicks_ly,       sa360_tmo_nonbrand_clicks_wow_pct,       sa360_tmo_nonbrand_clicks_yoy_pct)       AS 'sa360_tmo_nonbrand_clicks',
            (sa360_tmo_nonbrand_cost,         sa360_tmo_nonbrand_cost_wow,         sa360_tmo_nonbrand_cost_ly,         sa360_tmo_nonbrand_cost_wow_pct,         sa360_tmo_nonbrand_cost_yoy_pct)         AS 'sa360_tmo_nonbrand_cost',
            (sa360_tmo_nonbrand_orders,       sa360_tmo_nonbrand_orders_wow,       sa360_tmo_nonbrand_orders_ly,       sa360_tmo_nonbrand_orders_wow_pct,       sa360_tmo_nonbrand_orders_yoy_pct)       AS 'sa360_tmo_nonbrand_orders',
            (sa360_tmo_nonbrand_cart_start,   sa360_tmo_nonbrand_cart_start_wow,   sa360_tmo_nonbrand_cart_start_ly,   sa360_tmo_nonbrand_cart_start_wow_pct,   sa360_tmo_nonbrand_cart_start_yoy_pct)   AS 'sa360_tmo_nonbrand_cart_start',
            (sa360_tmo_nonbrand_postpaid_pspv,sa360_tmo_nonbrand_postpaid_pspv_wow,sa360_tmo_nonbrand_postpaid_pspv_ly,sa360_tmo_nonbrand_postpaid_pspv_wow_pct,sa360_tmo_nonbrand_postpaid_pspv_yoy_pct) AS 'sa360_tmo_nonbrand_postpaid_pspv'
        )
    )
),

-- -----------------------------------------------------------------------
-- GSC: Organic Search Performance
-- 4 metric rows per week (2 metrics × 2 brand types)
-- data_source = 'GSC', channel = 'ORGANIC SEARCH' from Silver
-- -----------------------------------------------------------------------
gsc_long AS (
    SELECT
        week_sun_to_sat,
        data_source,
        channel,
        max_data_date,
        metric_name,
        metric_value,
        metric_value_wow,
        metric_value_ly,
        wow_pct,
        yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_gsc_weekly`
    UNPIVOT (
        (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
        FOR metric_name IN (
            (gsc_tmo_brand_impressions,    gsc_tmo_brand_impressions_wow,    gsc_tmo_brand_impressions_ly,    gsc_tmo_brand_impressions_wow_pct,    gsc_tmo_brand_impressions_yoy_pct)    AS 'gsc_tmo_brand_impressions',
            (gsc_tmo_brand_clicks,         gsc_tmo_brand_clicks_wow,         gsc_tmo_brand_clicks_ly,         gsc_tmo_brand_clicks_wow_pct,         gsc_tmo_brand_clicks_yoy_pct)         AS 'gsc_tmo_brand_clicks',
            (gsc_tmo_nonbrand_impressions, gsc_tmo_nonbrand_impressions_wow, gsc_tmo_nonbrand_impressions_ly, gsc_tmo_nonbrand_impressions_wow_pct, gsc_tmo_nonbrand_impressions_yoy_pct) AS 'gsc_tmo_nonbrand_impressions',
            (gsc_tmo_nonbrand_clicks,      gsc_tmo_nonbrand_clicks_wow,      gsc_tmo_nonbrand_clicks_ly,      gsc_tmo_nonbrand_clicks_wow_pct,      gsc_tmo_nonbrand_clicks_yoy_pct)      AS 'gsc_tmo_nonbrand_clicks'
        )
    )
),

-- -----------------------------------------------------------------------
-- TRENDS: byod_index
-- 1 metric row per week
-- data_source = 'TRENDS', channel = 'ORGANIC SEARCH' from Silver
-- -----------------------------------------------------------------------
trends_index_long AS (
    SELECT
        week_sun_to_sat,
        data_source,
        channel,
        max_data_date,
        metric_name,
        metric_value,
        metric_value_wow,
        metric_value_ly,
        wow_pct,
        yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
    UNPIVOT (
        (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
        FOR metric_name IN (
            (trends_byod_index, trends_byod_index_wow, trends_byod_index_ly,
             trends_byod_index_wow_pct, trends_byod_index_yoy_pct)
            AS 'trends_byod_index'
        )
    )
),

-- -----------------------------------------------------------------------
-- TRENDS: Keywords unpivoted
-- Up to 10 rows per week (5 keywords × 2 metrics: interest + change)
-- metric_name = 'trends_kw_interest' : relative interest score (0-100)
-- metric_name = 'trends_kw_change'   : source-supplied WoW change value
--   (e.g. -0.03 = interest decreased 3% vs prior week)
--   Comes pre-computed from the Trends pipeline — NOT calculated in BQ.
-- dimension_name = 'KEYWORD_RANK_1' through 'KEYWORD_RANK_5'
-- dimension_value = keyword text
-- wow_pct, yoy_pct, metric_value_wow, metric_value_ly all NULL for
-- both metric types — keywords change position week to week, making
-- BQ-computed comparisons unreliable.
-- Row filter: keyword text must be non-null/non-empty (both metric types
-- share the same filter — if interest is missing, change will be too).
-- Earlier rows (pre-2026-05-09) will show 0.0 for kw_change due to
-- known pipeline backfill gap — consistent with kw_interest behavior.
-- -----------------------------------------------------------------------
trends_keywords_long AS (

    -- ---- KEYWORD_RANK_1 ----
    SELECT week_sun_to_sat, data_source, channel, max_data_date,
        'KEYWORD_RANK_1'     AS dimension_name,
        trends_top_kw_1      AS dimension_value,
        'trends_kw_interest' AS metric_name,
        trends_kw1_interest  AS metric_value,
        CAST(NULL AS FLOAT64) AS metric_value_wow,
        CAST(NULL AS FLOAT64) AS metric_value_ly,
        CAST(NULL AS FLOAT64) AS wow_pct,
        CAST(NULL AS FLOAT64) AS yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
    WHERE NULLIF(TRIM(trends_top_kw_1), '') IS NOT NULL

    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date,
        'KEYWORD_RANK_1',
        trends_top_kw_1,
        'trends_kw_change',
        trends_kw1_change,
        CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
    WHERE NULLIF(TRIM(trends_top_kw_1), '') IS NOT NULL

    -- ---- KEYWORD_RANK_2 ----
    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date,
        'KEYWORD_RANK_2',
        trends_top_kw_2,
        'trends_kw_interest',
        trends_kw2_interest,
        CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
    WHERE NULLIF(TRIM(trends_top_kw_2), '') IS NOT NULL

    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date,
        'KEYWORD_RANK_2',
        trends_top_kw_2,
        'trends_kw_change',
        trends_kw2_change,
        CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
    WHERE NULLIF(TRIM(trends_top_kw_2), '') IS NOT NULL

    -- ---- KEYWORD_RANK_3 ----
    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date,
        'KEYWORD_RANK_3',
        trends_top_kw_3,
        'trends_kw_interest',
        trends_kw3_interest,
        CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
    WHERE NULLIF(TRIM(trends_top_kw_3), '') IS NOT NULL

    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date,
        'KEYWORD_RANK_3',
        trends_top_kw_3,
        'trends_kw_change',
        trends_kw3_change,
        CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
    WHERE NULLIF(TRIM(trends_top_kw_3), '') IS NOT NULL

    -- ---- KEYWORD_RANK_4 ----
    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date,
        'KEYWORD_RANK_4',
        trends_top_kw_4,
        'trends_kw_interest',
        trends_kw4_interest,
        CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
    WHERE NULLIF(TRIM(trends_top_kw_4), '') IS NOT NULL

    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date,
        'KEYWORD_RANK_4',
        trends_top_kw_4,
        'trends_kw_change',
        trends_kw4_change,
        CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
    WHERE NULLIF(TRIM(trends_top_kw_4), '') IS NOT NULL

    -- ---- KEYWORD_RANK_5 ----
    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date,
        'KEYWORD_RANK_5',
        trends_top_kw_5,
        'trends_kw_interest',
        trends_kw5_interest,
        CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
    WHERE NULLIF(TRIM(trends_top_kw_5), '') IS NOT NULL

    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date,
        'KEYWORD_RANK_5',
        trends_top_kw_5,
        'trends_kw_change',
        trends_kw5_change,
        CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
    WHERE NULLIF(TRIM(trends_top_kw_5), '') IS NOT NULL
),

-- -----------------------------------------------------------------------
-- ADOBE: BYOD Analytics — multi-channel
-- 32 metric rows per week (unpivoted across all 7 channel variants)
-- data_source = 'ADOBE' from Silver
-- channel derived from metric_name suffix in combined CTE below
-- -----------------------------------------------------------------------
adobe_long AS (
    SELECT
        week_sun_to_sat,
        data_source,
        max_data_date,
        metric_name,
        metric_value,
        metric_value_wow,
        metric_value_ly,
        wow_pct,
        yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_adobe_weekly`
    UNPIVOT (
        (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
        FOR metric_name IN (
            -- ALL CHANNELS
            (adobe_uvnbByod_allChannels,              adobe_uvnbByod_allChannels_wow,              adobe_uvnbByod_allChannels_ly,              adobe_uvnbByod_allChannels_wow_pct,              adobe_uvnbByod_allChannels_yoy_pct)              AS 'adobe_uvnbByod_allChannels',
            (adobe_uvnbFlowTotal_allChannels,         adobe_uvnbFlowTotal_allChannels_wow,         adobe_uvnbFlowTotal_allChannels_ly,         adobe_uvnbFlowTotal_allChannels_wow_pct,         adobe_uvnbFlowTotal_allChannels_yoy_pct)         AS 'adobe_uvnbFlowTotal_allChannels',
            (adobe_uvnbByodPctOfUvnbFlow_allChannels, adobe_uvnbByodPctOfUvnbFlow_allChannels_wow, adobe_uvnbByodPctOfUvnbFlow_allChannels_ly, adobe_uvnbByodPctOfUvnbFlow_allChannels_wow_pct, adobe_uvnbByodPctOfUvnbFlow_allChannels_yoy_pct) AS 'adobe_uvnbByodPctOfUvnbFlow_allChannels',
            (adobe_cartStartByod_allChannels,         adobe_cartStartByod_allChannels_wow,         adobe_cartStartByod_allChannels_ly,         adobe_cartStartByod_allChannels_wow_pct,         adobe_cartStartByod_allChannels_yoy_pct)         AS 'adobe_cartStartByod_allChannels',
            (adobe_ordersUnassistedByod_allChannels,  adobe_ordersUnassistedByod_allChannels_wow,  adobe_ordersUnassistedByod_allChannels_ly,  adobe_ordersUnassistedByod_allChannels_wow_pct,  adobe_ordersUnassistedByod_allChannels_yoy_pct)  AS 'adobe_ordersUnassistedByod_allChannels',
            (adobe_ordersAssistedByod_allChannels,    adobe_ordersAssistedByod_allChannels_wow,    adobe_ordersAssistedByod_allChannels_ly,    adobe_ordersAssistedByod_allChannels_wow_pct,    adobe_ordersAssistedByod_allChannels_yoy_pct)    AS 'adobe_ordersAssistedByod_allChannels',
            (adobe_ordersTotalByod_allChannels,       adobe_ordersTotalByod_allChannels_wow,       adobe_ordersTotalByod_allChannels_ly,       adobe_ordersTotalByod_allChannels_wow_pct,       adobe_ordersTotalByod_allChannels_yoy_pct)       AS 'adobe_ordersTotalByod_allChannels',
            -- PAID SEARCH
            (adobe_uvnbByod_paidSearch,               adobe_uvnbByod_paidSearch_wow,               adobe_uvnbByod_paidSearch_ly,               adobe_uvnbByod_paidSearch_wow_pct,               adobe_uvnbByod_paidSearch_yoy_pct)               AS 'adobe_uvnbByod_paidSearch',
            (adobe_cartStartByod_paidSearch,          adobe_cartStartByod_paidSearch_wow,          adobe_cartStartByod_paidSearch_ly,          adobe_cartStartByod_paidSearch_wow_pct,          adobe_cartStartByod_paidSearch_yoy_pct)          AS 'adobe_cartStartByod_paidSearch',
            (adobe_ordersUnassistedByod_paidSearch,   adobe_ordersUnassistedByod_paidSearch_wow,   adobe_ordersUnassistedByod_paidSearch_ly,   adobe_ordersUnassistedByod_paidSearch_wow_pct,   adobe_ordersUnassistedByod_paidSearch_yoy_pct)   AS 'adobe_ordersUnassistedByod_paidSearch',
            (adobe_ordersAssistedByod_paidSearch,     adobe_ordersAssistedByod_paidSearch_wow,     adobe_ordersAssistedByod_paidSearch_ly,     adobe_ordersAssistedByod_paidSearch_wow_pct,     adobe_ordersAssistedByod_paidSearch_yoy_pct)     AS 'adobe_ordersAssistedByod_paidSearch',
            (adobe_ordersTotalByod_paidSearch,        adobe_ordersTotalByod_paidSearch_wow,        adobe_ordersTotalByod_paidSearch_ly,        adobe_ordersTotalByod_paidSearch_wow_pct,        adobe_ordersTotalByod_paidSearch_yoy_pct)        AS 'adobe_ordersTotalByod_paidSearch',
            -- ORGANIC SEARCH
            (adobe_uvnbByod_organicSearch,            adobe_uvnbByod_organicSearch_wow,            adobe_uvnbByod_organicSearch_ly,            adobe_uvnbByod_organicSearch_wow_pct,            adobe_uvnbByod_organicSearch_yoy_pct)            AS 'adobe_uvnbByod_organicSearch',
            (adobe_cartStartByod_organicSearch,       adobe_cartStartByod_organicSearch_wow,       adobe_cartStartByod_organicSearch_ly,       adobe_cartStartByod_organicSearch_wow_pct,       adobe_cartStartByod_organicSearch_yoy_pct)       AS 'adobe_cartStartByod_organicSearch',
            (adobe_ordersUnassistedByod_organicSearch,adobe_ordersUnassistedByod_organicSearch_wow,adobe_ordersUnassistedByod_organicSearch_ly,adobe_ordersUnassistedByod_organicSearch_wow_pct,adobe_ordersUnassistedByod_organicSearch_yoy_pct) AS 'adobe_ordersUnassistedByod_organicSearch',
            (adobe_ordersAssistedByod_organicSearch,  adobe_ordersAssistedByod_organicSearch_wow,  adobe_ordersAssistedByod_organicSearch_ly,  adobe_ordersAssistedByod_organicSearch_wow_pct,  adobe_ordersAssistedByod_organicSearch_yoy_pct)  AS 'adobe_ordersAssistedByod_organicSearch',
            (adobe_ordersTotalByod_organicSearch,     adobe_ordersTotalByod_organicSearch_wow,     adobe_ordersTotalByod_organicSearch_ly,     adobe_ordersTotalByod_organicSearch_wow_pct,     adobe_ordersTotalByod_organicSearch_yoy_pct)     AS 'adobe_ordersTotalByod_organicSearch',
            -- DIRECT
            (adobe_uvnbByod_direct,                   adobe_uvnbByod_direct_wow,                   adobe_uvnbByod_direct_ly,                   adobe_uvnbByod_direct_wow_pct,                   adobe_uvnbByod_direct_yoy_pct)                   AS 'adobe_uvnbByod_direct',
            (adobe_cartStartByod_direct,              adobe_cartStartByod_direct_wow,              adobe_cartStartByod_direct_ly,              adobe_cartStartByod_direct_wow_pct,              adobe_cartStartByod_direct_yoy_pct)              AS 'adobe_cartStartByod_direct',
            (adobe_ordersUnassistedByod_direct,       adobe_ordersUnassistedByod_direct_wow,       adobe_ordersUnassistedByod_direct_ly,       adobe_ordersUnassistedByod_direct_wow_pct,       adobe_ordersUnassistedByod_direct_yoy_pct)       AS 'adobe_ordersUnassistedByod_direct',
            (adobe_ordersAssistedByod_direct,         adobe_ordersAssistedByod_direct_wow,         adobe_ordersAssistedByod_direct_ly,         adobe_ordersAssistedByod_direct_wow_pct,         adobe_ordersAssistedByod_direct_yoy_pct)         AS 'adobe_ordersAssistedByod_direct',
            (adobe_ordersTotalByod_direct,            adobe_ordersTotalByod_direct_wow,            adobe_ordersTotalByod_direct_ly,            adobe_ordersTotalByod_direct_wow_pct,            adobe_ordersTotalByod_direct_yoy_pct)            AS 'adobe_ordersTotalByod_direct',
            -- SOCIAL
            (adobe_uvnbByod_social,                   adobe_uvnbByod_social_wow,                   adobe_uvnbByod_social_ly,                   adobe_uvnbByod_social_wow_pct,                   adobe_uvnbByod_social_yoy_pct)                   AS 'adobe_uvnbByod_social',
            (adobe_cartStartByod_social,              adobe_cartStartByod_social_wow,              adobe_cartStartByod_social_ly,              adobe_cartStartByod_social_wow_pct,              adobe_cartStartByod_social_yoy_pct)              AS 'adobe_cartStartByod_social',
            (adobe_ordersUnassistedByod_social,       adobe_ordersUnassistedByod_social_wow,       adobe_ordersUnassistedByod_social_ly,       adobe_ordersUnassistedByod_social_wow_pct,       adobe_ordersUnassistedByod_social_yoy_pct)       AS 'adobe_ordersUnassistedByod_social',
            (adobe_ordersAssistedByod_social,         adobe_ordersAssistedByod_social_wow,         adobe_ordersAssistedByod_social_ly,         adobe_ordersAssistedByod_social_wow_pct,         adobe_ordersAssistedByod_social_yoy_pct)         AS 'adobe_ordersAssistedByod_social',
            (adobe_ordersTotalByod_social,            adobe_ordersTotalByod_social_wow,            adobe_ordersTotalByod_social_ly,            adobe_ordersTotalByod_social_wow_pct,            adobe_ordersTotalByod_social_yoy_pct)            AS 'adobe_ordersTotalByod_social',
            -- PROGRAMMATIC
            (adobe_uvnbByod_programmatic,             adobe_uvnbByod_programmatic_wow,             adobe_uvnbByod_programmatic_ly,             adobe_uvnbByod_programmatic_wow_pct,             adobe_uvnbByod_programmatic_yoy_pct)             AS 'adobe_uvnbByod_programmatic',
            (adobe_cartStartByod_programmatic,        adobe_cartStartByod_programmatic_wow,        adobe_cartStartByod_programmatic_ly,        adobe_cartStartByod_programmatic_wow_pct,        adobe_cartStartByod_programmatic_yoy_pct)        AS 'adobe_cartStartByod_programmatic',
            (adobe_ordersUnassistedByod_programmatic, adobe_ordersUnassistedByod_programmatic_wow, adobe_ordersUnassistedByod_programmatic_ly, adobe_ordersUnassistedByod_programmatic_wow_pct, adobe_ordersUnassistedByod_programmatic_yoy_pct) AS 'adobe_ordersUnassistedByod_programmatic',
            (adobe_ordersAssistedByod_programmatic,   adobe_ordersAssistedByod_programmatic_wow,   adobe_ordersAssistedByod_programmatic_ly,   adobe_ordersAssistedByod_programmatic_wow_pct,   adobe_ordersAssistedByod_programmatic_yoy_pct)   AS 'adobe_ordersAssistedByod_programmatic',
            (adobe_ordersTotalByod_programmatic,      adobe_ordersTotalByod_programmatic_wow,      adobe_ordersTotalByod_programmatic_ly,      adobe_ordersTotalByod_programmatic_wow_pct,      adobe_ordersTotalByod_programmatic_yoy_pct)      AS 'adobe_ordersTotalByod_programmatic',
            -- OTHER
            (adobe_uvnbByod_other,                    adobe_uvnbByod_other_wow,                    adobe_uvnbByod_other_ly,                    adobe_uvnbByod_other_wow_pct,                    adobe_uvnbByod_other_yoy_pct)                    AS 'adobe_uvnbByod_other',
            (adobe_cartStartByod_other,               adobe_cartStartByod_other_wow,               adobe_cartStartByod_other_ly,               adobe_cartStartByod_other_wow_pct,               adobe_cartStartByod_other_yoy_pct)               AS 'adobe_cartStartByod_other',
            (adobe_ordersUnassistedByod_other,        adobe_ordersUnassistedByod_other_wow,        adobe_ordersUnassistedByod_other_ly,        adobe_ordersUnassistedByod_other_wow_pct,        adobe_ordersUnassistedByod_other_yoy_pct)        AS 'adobe_ordersUnassistedByod_other',
            (adobe_ordersAssistedByod_other,          adobe_ordersAssistedByod_other_wow,          adobe_ordersAssistedByod_other_ly,          adobe_ordersAssistedByod_other_wow_pct,          adobe_ordersAssistedByod_other_yoy_pct)          AS 'adobe_ordersAssistedByod_other',
            (adobe_ordersTotalByod_other,             adobe_ordersTotalByod_other_wow,             adobe_ordersTotalByod_other_ly,             adobe_ordersTotalByod_other_wow_pct,             adobe_ordersTotalByod_other_yoy_pct)             AS 'adobe_ordersTotalByod_other'
        )
    )
),

-- -----------------------------------------------------------------------
-- COMBINE: All sources into single long table
-- Each source scanned exactly once via its own CTE
-- Adobe channel derived from metric_name suffix here
-- dimension_name and dimension_value NULL for all non-keyword rows
-- -----------------------------------------------------------------------
combined AS (

    -- PROFOUND
    SELECT
        week_sun_to_sat, data_source, channel, max_data_date,
        CAST(NULL AS STRING) AS dimension_name,
        CAST(NULL AS STRING) AS dimension_value,
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM profound_long

    UNION ALL

    -- GOFISH
    SELECT
        week_sun_to_sat, data_source, channel, max_data_date,
        CAST(NULL AS STRING), CAST(NULL AS STRING),
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM gofish_long

    UNION ALL

    -- SA360
    SELECT
        week_sun_to_sat, data_source, channel, max_data_date,
        CAST(NULL AS STRING), CAST(NULL AS STRING),
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM sa360_long

    UNION ALL

    -- GSC
    SELECT
        week_sun_to_sat, data_source, channel, max_data_date,
        CAST(NULL AS STRING), CAST(NULL AS STRING),
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM gsc_long

    UNION ALL

    -- TRENDS: byod_index
    SELECT
        week_sun_to_sat, data_source, channel, max_data_date,
        CAST(NULL AS STRING), CAST(NULL AS STRING),
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM trends_index_long

    UNION ALL

    -- TRENDS: keywords (interest + source-supplied change, no BQ-computed wow)
    SELECT
        week_sun_to_sat, data_source, channel, max_data_date,
        dimension_name, dimension_value,
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM trends_keywords_long

    UNION ALL

    -- ADOBE: channel derived from metric_name suffix
    SELECT
        week_sun_to_sat,
        data_source,
        CASE
            WHEN metric_name LIKE '%_allChannels'   THEN 'ALL CHANNELS'
            WHEN metric_name LIKE '%_paidSearch'    THEN 'PAID SEARCH'
            WHEN metric_name LIKE '%_organicSearch' THEN 'ORGANIC SEARCH'
            WHEN metric_name LIKE '%_direct'        THEN 'DIRECT'
            WHEN metric_name LIKE '%_social'        THEN 'SOCIAL'
            WHEN metric_name LIKE '%_programmatic'  THEN 'PROGRAMMATIC'
            WHEN metric_name LIKE '%_other'         THEN 'OTHER'
        END                                         AS channel,
        max_data_date,
        CAST(NULL AS STRING)                        AS dimension_name,
        CAST(NULL AS STRING)                        AS dimension_value,
        metric_name,
        metric_value,
        metric_value_wow,
        metric_value_ly,
        wow_pct,
        yoy_pct
    FROM adobe_long
)

-- -----------------------------------------------------------------------
-- FINAL OUTPUT
-- Column order: time → granularity → source → channel → dimensions → metric
-- max_data_date per data_source via window function
-- -----------------------------------------------------------------------
SELECT
    week_sun_to_sat,
    'WEEKLY'                                                            AS time_granularity,
    data_source,
    channel,
    dimension_name,
    dimension_value,
    metric_name,
    metric_value,
    metric_value_wow,
    metric_value_ly,
    wow_pct,
    yoy_pct,
    MAX(max_data_date) OVER (
        PARTITION BY data_source
    )                                                                   AS max_data_date
FROM combined
ORDER BY
    week_sun_to_sat  ASC,
    data_source      ASC,
    channel          ASC,
    metric_name      ASC,
    dimension_value  ASC
;