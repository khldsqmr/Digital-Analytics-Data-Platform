/* =================================================================================================
FILE:         02_vw_sdi_pulseByod_gold_unified_long.sql
LAYER:        Gold View — Long
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseByod_gold_unified_long

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_long

PURPOSE:
  Gold Long view for the Pulse BYOD dashboard.
  Unpivots Gold Wide into a long table — one row per metric per week.
  This is the primary table consumed by the dashboard.
  data_source and channel derived from metric_name prefix.
  max_data_date per data_source computed here as a window function
  on top of the already-correct per-source max dates from Silver.
  Keywords unpivoted here for Top N Keywords dashboard visualization.

OUTPUT SCHEMA:
  week_sun_to_sat  : DATE    — Week ending Saturday (Sun-to-Sat)
  data_source      : STRING  — PROFOUND, GOFISH, SA360, GSC, TRENDS
  channel          : STRING  — AI SEARCH, PAID SEARCH, ORGANIC SEARCH
  metric_name      : STRING  — Full prefixed metric name (e.g. sa360_tmo_brand_impressions)
  metric_value     : FLOAT64 — Current week metric value
  metric_value_wow : FLOAT64 — Prior week value
  metric_value_ly  : FLOAT64 — Same Sun-to-Sat week last year
  wow_pct          : FLOAT64 — WoW% as decimal
  yoy_pct          : FLOAT64 — YoY% as decimal
  max_data_date    : DATE    — Latest week with non-null data per data_source
  dimension_name   : STRING  — KEYWORD for Trends keywords, NULL otherwise
  dimension_value  : STRING  — Keyword text for Trends keywords, NULL otherwise

UNPIVOT APPROACH:
  Standard metrics: UNION ALL of SELECT statements per metric
  Keywords: UNION ALL of SELECT statements per keyword rank (1-5)
  Each keyword row has:
    metric_name     = 'trends_kw_interest' or 'trends_kw_wow_change'
    dimension_name  = 'KEYWORD'
    dimension_value = keyword text
    metric_value    = interest or change value
    metric_value_wow/ly/wow_pct/yoy_pct = NULL (keywords change week to week)

DATA SOURCE MAPPING (from metric_name prefix):
  profound_* → data_source = PROFOUND, channel = AI SEARCH
  gofish_*   → data_source = GOFISH,   channel = AI SEARCH
  sa360_*    → data_source = SA360,    channel = PAID SEARCH
  gsc_*      → data_source = GSC,      channel = ORGANIC SEARCH
  trends_*   → data_source = TRENDS,   channel = ORGANIC SEARCH

MAX DATA DATE:
  Derived from {source}_max_data_date columns in Gold Wide
  which are already correctly computed per source in Silver.
  MAX() OVER (PARTITION BY data_source) applied here for
  consistent value across all rows of the same source.

KEY MODELING NOTES:
  - Gold Long reads from Gold Wide — no Silver re-scanning
  - All WoW/LY/pct already computed in Silver — no re-computation here
  - Keyword rows have NULL for all comparison columns
  - NULL metric_value rows included — dashboard filters as needed
  - No ORDER BY in CTEs — final ORDER BY at end

AUTHOR:       Pulse BYOD Team
CREATED:      2026-05-24
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_long`
AS

-- -----------------------------------------------------------------------
-- UNPIVOT: Each metric becomes its own row
-- Grouped by source for readability
-- Keywords unpivoted separately at the end
-- -----------------------------------------------------------------------
WITH unpivoted AS (

    -- ==============================================================
    -- PROFOUND: NON-BRAND AI Visibility
    -- ==============================================================
    SELECT week_sun_to_sat, 'PROFOUND' AS data_source, 'AI SEARCH' AS channel, profound_max_data_date AS source_max_data_date,
        'profound_tmo_nonbrand_visibility_score' AS metric_name, profound_tmo_nonbrand_visibility_score AS metric_value,
        profound_tmo_nonbrand_visibility_score_wow AS metric_value_wow, profound_tmo_nonbrand_visibility_score_ly AS metric_value_ly,
        profound_tmo_nonbrand_visibility_score_wow_pct AS wow_pct, profound_tmo_nonbrand_visibility_score_yoy_pct AS yoy_pct,
        CAST(NULL AS STRING) AS dimension_name, CAST(NULL AS STRING) AS dimension_value
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'PROFOUND', 'AI SEARCH', profound_max_data_date,
        'profound_tmo_nonbrand_executions', profound_tmo_nonbrand_executions,
        profound_tmo_nonbrand_executions_wow, profound_tmo_nonbrand_executions_ly,
        profound_tmo_nonbrand_executions_wow_pct, profound_tmo_nonbrand_executions_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'PROFOUND', 'AI SEARCH', profound_max_data_date,
        'profound_tmo_nonbrand_mentions_count', profound_tmo_nonbrand_mentions_count,
        profound_tmo_nonbrand_mentions_count_wow, profound_tmo_nonbrand_mentions_count_ly,
        profound_tmo_nonbrand_mentions_count_wow_pct, profound_tmo_nonbrand_mentions_count_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'PROFOUND', 'AI SEARCH', profound_max_data_date,
        'profound_tmo_nonbrand_share_of_voice', profound_tmo_nonbrand_share_of_voice,
        profound_tmo_nonbrand_share_of_voice_wow, profound_tmo_nonbrand_share_of_voice_ly,
        profound_tmo_nonbrand_share_of_voice_wow_pct, profound_tmo_nonbrand_share_of_voice_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'PROFOUND', 'AI SEARCH', profound_max_data_date,
        'profound_verizon_nonbrand_visibility_score', profound_verizon_nonbrand_visibility_score,
        profound_verizon_nonbrand_visibility_score_wow, profound_verizon_nonbrand_visibility_score_ly,
        profound_verizon_nonbrand_visibility_score_wow_pct, profound_verizon_nonbrand_visibility_score_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'PROFOUND', 'AI SEARCH', profound_max_data_date,
        'profound_verizon_nonbrand_executions', profound_verizon_nonbrand_executions,
        profound_verizon_nonbrand_executions_wow, profound_verizon_nonbrand_executions_ly,
        profound_verizon_nonbrand_executions_wow_pct, profound_verizon_nonbrand_executions_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'PROFOUND', 'AI SEARCH', profound_max_data_date,
        'profound_verizon_nonbrand_mentions_count', profound_verizon_nonbrand_mentions_count,
        profound_verizon_nonbrand_mentions_count_wow, profound_verizon_nonbrand_mentions_count_ly,
        profound_verizon_nonbrand_mentions_count_wow_pct, profound_verizon_nonbrand_mentions_count_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'PROFOUND', 'AI SEARCH', profound_max_data_date,
        'profound_verizon_nonbrand_share_of_voice', profound_verizon_nonbrand_share_of_voice,
        profound_verizon_nonbrand_share_of_voice_wow, profound_verizon_nonbrand_share_of_voice_ly,
        profound_verizon_nonbrand_share_of_voice_wow_pct, profound_verizon_nonbrand_share_of_voice_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'PROFOUND', 'AI SEARCH', profound_max_data_date,
        'profound_att_nonbrand_visibility_score', profound_att_nonbrand_visibility_score,
        profound_att_nonbrand_visibility_score_wow, profound_att_nonbrand_visibility_score_ly,
        profound_att_nonbrand_visibility_score_wow_pct, profound_att_nonbrand_visibility_score_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'PROFOUND', 'AI SEARCH', profound_max_data_date,
        'profound_att_nonbrand_executions', profound_att_nonbrand_executions,
        profound_att_nonbrand_executions_wow, profound_att_nonbrand_executions_ly,
        profound_att_nonbrand_executions_wow_pct, profound_att_nonbrand_executions_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'PROFOUND', 'AI SEARCH', profound_max_data_date,
        'profound_att_nonbrand_mentions_count', profound_att_nonbrand_mentions_count,
        profound_att_nonbrand_mentions_count_wow, profound_att_nonbrand_mentions_count_ly,
        profound_att_nonbrand_mentions_count_wow_pct, profound_att_nonbrand_mentions_count_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'PROFOUND', 'AI SEARCH', profound_max_data_date,
        'profound_att_nonbrand_share_of_voice', profound_att_nonbrand_share_of_voice,
        profound_att_nonbrand_share_of_voice_wow, profound_att_nonbrand_share_of_voice_ly,
        profound_att_nonbrand_share_of_voice_wow_pct, profound_att_nonbrand_share_of_voice_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    -- ==============================================================
    -- GOFISH: BRAND AI Visibility
    -- ==============================================================
    UNION ALL SELECT week_sun_to_sat, 'GOFISH', 'AI SEARCH', gofish_max_data_date,
        'gofish_tmo_brand_visibility_score', gofish_tmo_brand_visibility_score,
        gofish_tmo_brand_visibility_score_wow, gofish_tmo_brand_visibility_score_ly,
        gofish_tmo_brand_visibility_score_wow_pct, gofish_tmo_brand_visibility_score_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'GOFISH', 'AI SEARCH', gofish_max_data_date,
        'gofish_tmo_brand_executions', gofish_tmo_brand_executions,
        gofish_tmo_brand_executions_wow, gofish_tmo_brand_executions_ly,
        gofish_tmo_brand_executions_wow_pct, gofish_tmo_brand_executions_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'GOFISH', 'AI SEARCH', gofish_max_data_date,
        'gofish_tmo_brand_mentions_count', gofish_tmo_brand_mentions_count,
        gofish_tmo_brand_mentions_count_wow, gofish_tmo_brand_mentions_count_ly,
        gofish_tmo_brand_mentions_count_wow_pct, gofish_tmo_brand_mentions_count_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'GOFISH', 'AI SEARCH', gofish_max_data_date,
        'gofish_tmo_brand_share_of_voice', gofish_tmo_brand_share_of_voice,
        gofish_tmo_brand_share_of_voice_wow, gofish_tmo_brand_share_of_voice_ly,
        gofish_tmo_brand_share_of_voice_wow_pct, gofish_tmo_brand_share_of_voice_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'GOFISH', 'AI SEARCH', gofish_max_data_date,
        'gofish_verizon_brand_visibility_score', gofish_verizon_brand_visibility_score,
        gofish_verizon_brand_visibility_score_wow, gofish_verizon_brand_visibility_score_ly,
        gofish_verizon_brand_visibility_score_wow_pct, gofish_verizon_brand_visibility_score_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'GOFISH', 'AI SEARCH', gofish_max_data_date,
        'gofish_verizon_brand_executions', gofish_verizon_brand_executions,
        gofish_verizon_brand_executions_wow, gofish_verizon_brand_executions_ly,
        gofish_verizon_brand_executions_wow_pct, gofish_verizon_brand_executions_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'GOFISH', 'AI SEARCH', gofish_max_data_date,
        'gofish_verizon_brand_mentions_count', gofish_verizon_brand_mentions_count,
        gofish_verizon_brand_mentions_count_wow, gofish_verizon_brand_mentions_count_ly,
        gofish_verizon_brand_mentions_count_wow_pct, gofish_verizon_brand_mentions_count_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'GOFISH', 'AI SEARCH', gofish_max_data_date,
        'gofish_verizon_brand_share_of_voice', gofish_verizon_brand_share_of_voice,
        gofish_verizon_brand_share_of_voice_wow, gofish_verizon_brand_share_of_voice_ly,
        gofish_verizon_brand_share_of_voice_wow_pct, gofish_verizon_brand_share_of_voice_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'GOFISH', 'AI SEARCH', gofish_max_data_date,
        'gofish_att_brand_visibility_score', gofish_att_brand_visibility_score,
        gofish_att_brand_visibility_score_wow, gofish_att_brand_visibility_score_ly,
        gofish_att_brand_visibility_score_wow_pct, gofish_att_brand_visibility_score_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'GOFISH', 'AI SEARCH', gofish_max_data_date,
        'gofish_att_brand_executions', gofish_att_brand_executions,
        gofish_att_brand_executions_wow, gofish_att_brand_executions_ly,
        gofish_att_brand_executions_wow_pct, gofish_att_brand_executions_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'GOFISH', 'AI SEARCH', gofish_max_data_date,
        'gofish_att_brand_mentions_count', gofish_att_brand_mentions_count,
        gofish_att_brand_mentions_count_wow, gofish_att_brand_mentions_count_ly,
        gofish_att_brand_mentions_count_wow_pct, gofish_att_brand_mentions_count_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'GOFISH', 'AI SEARCH', gofish_max_data_date,
        'gofish_att_brand_share_of_voice', gofish_att_brand_share_of_voice,
        gofish_att_brand_share_of_voice_wow, gofish_att_brand_share_of_voice_ly,
        gofish_att_brand_share_of_voice_wow_pct, gofish_att_brand_share_of_voice_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    -- ==============================================================
    -- SA360: Paid Search Performance
    -- ==============================================================
    UNION ALL SELECT week_sun_to_sat, 'SA360', 'PAID SEARCH', sa360_max_data_date,
        'sa360_tmo_brand_impressions', sa360_tmo_brand_impressions,
        sa360_tmo_brand_impressions_wow, sa360_tmo_brand_impressions_ly,
        sa360_tmo_brand_impressions_wow_pct, sa360_tmo_brand_impressions_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'SA360', 'PAID SEARCH', sa360_max_data_date,
        'sa360_tmo_brand_clicks', sa360_tmo_brand_clicks,
        sa360_tmo_brand_clicks_wow, sa360_tmo_brand_clicks_ly,
        sa360_tmo_brand_clicks_wow_pct, sa360_tmo_brand_clicks_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'SA360', 'PAID SEARCH', sa360_max_data_date,
        'sa360_tmo_brand_cost', sa360_tmo_brand_cost,
        sa360_tmo_brand_cost_wow, sa360_tmo_brand_cost_ly,
        sa360_tmo_brand_cost_wow_pct, sa360_tmo_brand_cost_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'SA360', 'PAID SEARCH', sa360_max_data_date,
        'sa360_tmo_brand_orders', sa360_tmo_brand_orders,
        sa360_tmo_brand_orders_wow, sa360_tmo_brand_orders_ly,
        sa360_tmo_brand_orders_wow_pct, sa360_tmo_brand_orders_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'SA360', 'PAID SEARCH', sa360_max_data_date,
        'sa360_tmo_brand_cart_start', sa360_tmo_brand_cart_start,
        sa360_tmo_brand_cart_start_wow, sa360_tmo_brand_cart_start_ly,
        sa360_tmo_brand_cart_start_wow_pct, sa360_tmo_brand_cart_start_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'SA360', 'PAID SEARCH', sa360_max_data_date,
        'sa360_tmo_brand_postpaid_pspv', sa360_tmo_brand_postpaid_pspv,
        sa360_tmo_brand_postpaid_pspv_wow, sa360_tmo_brand_postpaid_pspv_ly,
        sa360_tmo_brand_postpaid_pspv_wow_pct, sa360_tmo_brand_postpaid_pspv_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'SA360', 'PAID SEARCH', sa360_max_data_date,
        'sa360_tmo_nonbrand_impressions', sa360_tmo_nonbrand_impressions,
        sa360_tmo_nonbrand_impressions_wow, sa360_tmo_nonbrand_impressions_ly,
        sa360_tmo_nonbrand_impressions_wow_pct, sa360_tmo_nonbrand_impressions_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'SA360', 'PAID SEARCH', sa360_max_data_date,
        'sa360_tmo_nonbrand_clicks', sa360_tmo_nonbrand_clicks,
        sa360_tmo_nonbrand_clicks_wow, sa360_tmo_nonbrand_clicks_ly,
        sa360_tmo_nonbrand_clicks_wow_pct, sa360_tmo_nonbrand_clicks_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'SA360', 'PAID SEARCH', sa360_max_data_date,
        'sa360_tmo_nonbrand_cost', sa360_tmo_nonbrand_cost,
        sa360_tmo_nonbrand_cost_wow, sa360_tmo_nonbrand_cost_ly,
        sa360_tmo_nonbrand_cost_wow_pct, sa360_tmo_nonbrand_cost_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'SA360', 'PAID SEARCH', sa360_max_data_date,
        'sa360_tmo_nonbrand_orders', sa360_tmo_nonbrand_orders,
        sa360_tmo_nonbrand_orders_wow, sa360_tmo_nonbrand_orders_ly,
        sa360_tmo_nonbrand_orders_wow_pct, sa360_tmo_nonbrand_orders_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'SA360', 'PAID SEARCH', sa360_max_data_date,
        'sa360_tmo_nonbrand_cart_start', sa360_tmo_nonbrand_cart_start,
        sa360_tmo_nonbrand_cart_start_wow, sa360_tmo_nonbrand_cart_start_ly,
        sa360_tmo_nonbrand_cart_start_wow_pct, sa360_tmo_nonbrand_cart_start_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'SA360', 'PAID SEARCH', sa360_max_data_date,
        'sa360_tmo_nonbrand_postpaid_pspv', sa360_tmo_nonbrand_postpaid_pspv,
        sa360_tmo_nonbrand_postpaid_pspv_wow, sa360_tmo_nonbrand_postpaid_pspv_ly,
        sa360_tmo_nonbrand_postpaid_pspv_wow_pct, sa360_tmo_nonbrand_postpaid_pspv_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    -- ==============================================================
    -- GSC: Organic Search Performance
    -- ==============================================================
    UNION ALL SELECT week_sun_to_sat, 'GSC', 'ORGANIC SEARCH', gsc_max_data_date,
        'gsc_tmo_brand_impressions', gsc_tmo_brand_impressions,
        gsc_tmo_brand_impressions_wow, gsc_tmo_brand_impressions_ly,
        gsc_tmo_brand_impressions_wow_pct, gsc_tmo_brand_impressions_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'GSC', 'ORGANIC SEARCH', gsc_max_data_date,
        'gsc_tmo_brand_clicks', gsc_tmo_brand_clicks,
        gsc_tmo_brand_clicks_wow, gsc_tmo_brand_clicks_ly,
        gsc_tmo_brand_clicks_wow_pct, gsc_tmo_brand_clicks_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'GSC', 'ORGANIC SEARCH', gsc_max_data_date,
        'gsc_tmo_nonbrand_impressions', gsc_tmo_nonbrand_impressions,
        gsc_tmo_nonbrand_impressions_wow, gsc_tmo_nonbrand_impressions_ly,
        gsc_tmo_nonbrand_impressions_wow_pct, gsc_tmo_nonbrand_impressions_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    UNION ALL SELECT week_sun_to_sat, 'GSC', 'ORGANIC SEARCH', gsc_max_data_date,
        'gsc_tmo_nonbrand_clicks', gsc_tmo_nonbrand_clicks,
        gsc_tmo_nonbrand_clicks_wow, gsc_tmo_nonbrand_clicks_ly,
        gsc_tmo_nonbrand_clicks_wow_pct, gsc_tmo_nonbrand_clicks_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    -- ==============================================================
    -- TRENDS: byod_index (with WoW/LY)
    -- ==============================================================
    UNION ALL SELECT week_sun_to_sat, 'TRENDS', 'ORGANIC SEARCH', trends_max_data_date,
        'trends_byod_index', trends_byod_index,
        trends_byod_index_wow, trends_byod_index_ly,
        trends_byod_index_wow_pct, trends_byod_index_yoy_pct,
        CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`

    -- ==============================================================
    -- TRENDS: Keywords unpivoted (no WoW/LY — keywords change weekly)
    -- dimension_name = 'KEYWORD', dimension_value = keyword text
    -- metric_name = 'trends_kw_interest' or 'trends_kw_wow_change'
    -- ==============================================================

    -- Keyword rank 1 — interest
    UNION ALL SELECT week_sun_to_sat, 'TRENDS', 'ORGANIC SEARCH', trends_max_data_date,
        'trends_kw_interest', trends_kw1_interest,
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        'KEYWORD', trends_top_kw_1
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`
    WHERE NULLIF(TRIM(trends_top_kw_1), '') IS NOT NULL

    -- Keyword rank 1 — wow_change
    UNION ALL SELECT week_sun_to_sat, 'TRENDS', 'ORGANIC SEARCH', trends_max_data_date,
        'trends_kw_wow_change', trends_kw1_change,
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        'KEYWORD', trends_top_kw_1
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`
    WHERE NULLIF(TRIM(trends_top_kw_1), '') IS NOT NULL

    -- Keyword rank 2 — interest
    UNION ALL SELECT week_sun_to_sat, 'TRENDS', 'ORGANIC SEARCH', trends_max_data_date,
        'trends_kw_interest', trends_kw2_interest,
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        'KEYWORD', trends_top_kw_2
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`
    WHERE NULLIF(TRIM(trends_top_kw_2), '') IS NOT NULL

    -- Keyword rank 2 — wow_change
    UNION ALL SELECT week_sun_to_sat, 'TRENDS', 'ORGANIC SEARCH', trends_max_data_date,
        'trends_kw_wow_change', trends_kw2_change,
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        'KEYWORD', trends_top_kw_2
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`
    WHERE NULLIF(TRIM(trends_top_kw_2), '') IS NOT NULL

    -- Keyword rank 3 — interest
    UNION ALL SELECT week_sun_to_sat, 'TRENDS', 'ORGANIC SEARCH', trends_max_data_date,
        'trends_kw_interest', trends_kw3_interest,
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        'KEYWORD', trends_top_kw_3
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`
    WHERE NULLIF(TRIM(trends_top_kw_3), '') IS NOT NULL

    -- Keyword rank 3 — wow_change
    UNION ALL SELECT week_sun_to_sat, 'TRENDS', 'ORGANIC SEARCH', trends_max_data_date,
        'trends_kw_wow_change', trends_kw3_change,
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        'KEYWORD', trends_top_kw_3
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`
    WHERE NULLIF(TRIM(trends_top_kw_3), '') IS NOT NULL

    -- Keyword rank 4 — interest
    UNION ALL SELECT week_sun_to_sat, 'TRENDS', 'ORGANIC SEARCH', trends_max_data_date,
        'trends_kw_interest', trends_kw4_interest,
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        'KEYWORD', trends_top_kw_4
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`
    WHERE NULLIF(TRIM(trends_top_kw_4), '') IS NOT NULL

    -- Keyword rank 4 — wow_change
    UNION ALL SELECT week_sun_to_sat, 'TRENDS', 'ORGANIC SEARCH', trends_max_data_date,
        'trends_kw_wow_change', trends_kw4_change,
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        'KEYWORD', trends_top_kw_4
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`
    WHERE NULLIF(TRIM(trends_top_kw_4), '') IS NOT NULL

    -- Keyword rank 5 — interest
    UNION ALL SELECT week_sun_to_sat, 'TRENDS', 'ORGANIC SEARCH', trends_max_data_date,
        'trends_kw_interest', trends_kw5_interest,
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        'KEYWORD', trends_top_kw_5
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`
    WHERE NULLIF(TRIM(trends_top_kw_5), '') IS NOT NULL

    -- Keyword rank 5 — wow_change
    UNION ALL SELECT week_sun_to_sat, 'TRENDS', 'ORGANIC SEARCH', trends_max_data_date,
        'trends_kw_wow_change', trends_kw5_change,
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        'KEYWORD', trends_top_kw_5
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`
    WHERE NULLIF(TRIM(trends_top_kw_5), '') IS NOT NULL
)

-- -----------------------------------------------------------------------
-- FINAL OUTPUT
-- max_data_date: MAX per data_source window function
-- ensures consistent value across all rows of the same source
-- -----------------------------------------------------------------------
SELECT
    week_sun_to_sat,
    data_source,
    channel,
    metric_name,
    metric_value,
    metric_value_wow,
    metric_value_ly,
    wow_pct,
    yoy_pct,
    -- max_data_date per data_source — consistent across all rows of same source
    MAX(source_max_data_date) OVER (PARTITION BY data_source) AS max_data_date,
    dimension_name,
    dimension_value
FROM unpivoted
ORDER BY
    week_sun_to_sat  ASC,
    data_source      ASC,
    metric_name      ASC,
    dimension_value  ASC
;