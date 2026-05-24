/* =================================================================================================
FILE:         02_vw_sdi_pulseByod_silver_profoundGofish_weekly.sql
LAYER:        Silver View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseByod_silver_profoundGofish_weekly

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_bronze_profoundGofish_weekly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profoundGofish_weekly

PURPOSE:
  Silver view for Profound GoFish BRANDED AI visibility data.
  Structurally identical to vw_sdi_pulseByod_silver_profound_weekly.
  Only differences: source table, data_source = 'GOFISH',
  metric prefix = 'gofish_', brand label = 'brand' in column names.

BUSINESS GRAIN:
  One row per:
    week_sun_to_sat

FILTERS APPLIED:
  - tag = 'BYOD'
  - asset_name IN ('T-Mobile', 'Verizon', 'AT&T')

BUSINESS LOGIC APPLIED:
  - data_source = 'GOFISH'
  - channel     = 'AI SEARCH'
  - brand_type  = 'BRAND' (entire source is branded by definition)
  - week_sun_to_sat = DATE_ADD(event_date_sun, INTERVAL 6 DAY)
  - All metric columns prefixed: gofish_{asset}_{metric}
  - WoW/LY/wow_pct/yoy_pct same logic as Profound Silver
  - max_data_date per source

COLUMN NAMING CONVENTION:
  gofish_{asset}_{metric}
  gofish_{asset}_{metric}_wow
  gofish_{asset}_{metric}_ly
  gofish_{asset}_{metric}_wow_pct
  gofish_{asset}_{metric}_yoy_pct

  Where asset : tmo, verizon, att
  Where metric: brand_visibility_score, brand_executions,
                brand_mentions_count, brand_share_of_voice

KEY MODELING NOTES:
  - Pivot from long → wide using MAX(CASE WHEN) per asset
  - Self-joins on small pivoted CTE — fast and cheap
  - NULLs preserved — no fake zeroes
  - No ORDER BY — applied in Gold only

DOWNSTREAM:
  Gold Wide : vw_sdi_pulseByod_gold_unified_wide
  Gold Long : vw_sdi_pulseByod_gold_unified_long
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profoundGofish_weekly`
AS

WITH filtered AS (
    SELECT
        DATE_ADD(event_date_sun, INTERVAL 6 DAY)    AS week_sun_to_sat,
        asset_name,
        visibility_score,
        executions,
        mentions_count,
        share_of_voice
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_bronze_profoundGofish_weekly`
    WHERE tag        = 'BYOD'
      AND asset_name IN ('T-Mobile', 'Verizon', 'AT&T')
),

pivoted AS (
    SELECT
        week_sun_to_sat,
        MAX(CASE WHEN asset_name = 'T-Mobile' THEN visibility_score END) AS gofish_tmo_brand_visibility_score,
        MAX(CASE WHEN asset_name = 'T-Mobile' THEN executions      END) AS gofish_tmo_brand_executions,
        MAX(CASE WHEN asset_name = 'T-Mobile' THEN mentions_count  END) AS gofish_tmo_brand_mentions_count,
        MAX(CASE WHEN asset_name = 'T-Mobile' THEN share_of_voice  END) AS gofish_tmo_brand_share_of_voice,
        MAX(CASE WHEN asset_name = 'Verizon'  THEN visibility_score END) AS gofish_verizon_brand_visibility_score,
        MAX(CASE WHEN asset_name = 'Verizon'  THEN executions      END) AS gofish_verizon_brand_executions,
        MAX(CASE WHEN asset_name = 'Verizon'  THEN mentions_count  END) AS gofish_verizon_brand_mentions_count,
        MAX(CASE WHEN asset_name = 'Verizon'  THEN share_of_voice  END) AS gofish_verizon_brand_share_of_voice,
        MAX(CASE WHEN asset_name = 'AT&T'     THEN visibility_score END) AS gofish_att_brand_visibility_score,
        MAX(CASE WHEN asset_name = 'AT&T'     THEN executions      END) AS gofish_att_brand_executions,
        MAX(CASE WHEN asset_name = 'AT&T'     THEN mentions_count  END) AS gofish_att_brand_mentions_count,
        MAX(CASE WHEN asset_name = 'AT&T'     THEN share_of_voice  END) AS gofish_att_brand_share_of_voice
    FROM filtered
    GROUP BY week_sun_to_sat
),

with_week_num AS (
    SELECT *,
        DATE_DIFF(DATE_SUB(week_sun_to_sat, INTERVAL 6 DAY), DATE '2023-01-01', WEEK) AS custom_week_num
    FROM pivoted
),

with_comparisons AS (
    SELECT
        c.week_sun_to_sat,
        c.custom_week_num,

        c.gofish_tmo_brand_visibility_score,
        c.gofish_tmo_brand_executions,
        c.gofish_tmo_brand_mentions_count,
        c.gofish_tmo_brand_share_of_voice,
        c.gofish_verizon_brand_visibility_score,
        c.gofish_verizon_brand_executions,
        c.gofish_verizon_brand_mentions_count,
        c.gofish_verizon_brand_share_of_voice,
        c.gofish_att_brand_visibility_score,
        c.gofish_att_brand_executions,
        c.gofish_att_brand_mentions_count,
        c.gofish_att_brand_share_of_voice,

        w.gofish_tmo_brand_visibility_score     AS gofish_tmo_brand_visibility_score_wow,
        w.gofish_tmo_brand_executions           AS gofish_tmo_brand_executions_wow,
        w.gofish_tmo_brand_mentions_count       AS gofish_tmo_brand_mentions_count_wow,
        w.gofish_tmo_brand_share_of_voice       AS gofish_tmo_brand_share_of_voice_wow,
        w.gofish_verizon_brand_visibility_score AS gofish_verizon_brand_visibility_score_wow,
        w.gofish_verizon_brand_executions       AS gofish_verizon_brand_executions_wow,
        w.gofish_verizon_brand_mentions_count   AS gofish_verizon_brand_mentions_count_wow,
        w.gofish_verizon_brand_share_of_voice   AS gofish_verizon_brand_share_of_voice_wow,
        w.gofish_att_brand_visibility_score     AS gofish_att_brand_visibility_score_wow,
        w.gofish_att_brand_executions           AS gofish_att_brand_executions_wow,
        w.gofish_att_brand_mentions_count       AS gofish_att_brand_mentions_count_wow,
        w.gofish_att_brand_share_of_voice       AS gofish_att_brand_share_of_voice_wow,

        l.gofish_tmo_brand_visibility_score     AS gofish_tmo_brand_visibility_score_ly,
        l.gofish_tmo_brand_executions           AS gofish_tmo_brand_executions_ly,
        l.gofish_tmo_brand_mentions_count       AS gofish_tmo_brand_mentions_count_ly,
        l.gofish_tmo_brand_share_of_voice       AS gofish_tmo_brand_share_of_voice_ly,
        l.gofish_verizon_brand_visibility_score AS gofish_verizon_brand_visibility_score_ly,
        l.gofish_verizon_brand_executions       AS gofish_verizon_brand_executions_ly,
        l.gofish_verizon_brand_mentions_count   AS gofish_verizon_brand_mentions_count_ly,
        l.gofish_verizon_brand_share_of_voice   AS gofish_verizon_brand_share_of_voice_ly,
        l.gofish_att_brand_visibility_score     AS gofish_att_brand_visibility_score_ly,
        l.gofish_att_brand_executions           AS gofish_att_brand_executions_ly,
        l.gofish_att_brand_mentions_count       AS gofish_att_brand_mentions_count_ly,
        l.gofish_att_brand_share_of_voice       AS gofish_att_brand_share_of_voice_ly

    FROM with_week_num c
    LEFT JOIN with_week_num w ON c.week_sun_to_sat = DATE_ADD(w.week_sun_to_sat, INTERVAL 7 DAY)
    LEFT JOIN with_week_num l ON (c.custom_week_num - l.custom_week_num) = 52
),

with_pcts AS (
    SELECT
        week_sun_to_sat,
        custom_week_num,

        gofish_tmo_brand_visibility_score,
        gofish_tmo_brand_visibility_score_wow,
        gofish_tmo_brand_visibility_score_ly,
        CASE WHEN gofish_tmo_brand_visibility_score_wow IS NULL OR gofish_tmo_brand_visibility_score_wow = 0 THEN NULL ELSE ROUND((gofish_tmo_brand_visibility_score - gofish_tmo_brand_visibility_score_wow) / gofish_tmo_brand_visibility_score_wow, 6) END AS gofish_tmo_brand_visibility_score_wow_pct,
        CASE WHEN gofish_tmo_brand_visibility_score_ly  IS NULL OR gofish_tmo_brand_visibility_score_ly  = 0 THEN NULL ELSE ROUND((gofish_tmo_brand_visibility_score - gofish_tmo_brand_visibility_score_ly)  / gofish_tmo_brand_visibility_score_ly,  6) END AS gofish_tmo_brand_visibility_score_yoy_pct,

        gofish_tmo_brand_executions,
        gofish_tmo_brand_executions_wow,
        gofish_tmo_brand_executions_ly,
        CASE WHEN gofish_tmo_brand_executions_wow IS NULL OR gofish_tmo_brand_executions_wow = 0 THEN NULL ELSE ROUND((gofish_tmo_brand_executions - gofish_tmo_brand_executions_wow) / gofish_tmo_brand_executions_wow, 6) END AS gofish_tmo_brand_executions_wow_pct,
        CASE WHEN gofish_tmo_brand_executions_ly  IS NULL OR gofish_tmo_brand_executions_ly  = 0 THEN NULL ELSE ROUND((gofish_tmo_brand_executions - gofish_tmo_brand_executions_ly)  / gofish_tmo_brand_executions_ly,  6) END AS gofish_tmo_brand_executions_yoy_pct,

        gofish_tmo_brand_mentions_count,
        gofish_tmo_brand_mentions_count_wow,
        gofish_tmo_brand_mentions_count_ly,
        CASE WHEN gofish_tmo_brand_mentions_count_wow IS NULL OR gofish_tmo_brand_mentions_count_wow = 0 THEN NULL ELSE ROUND((gofish_tmo_brand_mentions_count - gofish_tmo_brand_mentions_count_wow) / gofish_tmo_brand_mentions_count_wow, 6) END AS gofish_tmo_brand_mentions_count_wow_pct,
        CASE WHEN gofish_tmo_brand_mentions_count_ly  IS NULL OR gofish_tmo_brand_mentions_count_ly  = 0 THEN NULL ELSE ROUND((gofish_tmo_brand_mentions_count - gofish_tmo_brand_mentions_count_ly)  / gofish_tmo_brand_mentions_count_ly,  6) END AS gofish_tmo_brand_mentions_count_yoy_pct,

        gofish_tmo_brand_share_of_voice,
        gofish_tmo_brand_share_of_voice_wow,
        gofish_tmo_brand_share_of_voice_ly,
        CASE WHEN gofish_tmo_brand_share_of_voice_wow IS NULL OR gofish_tmo_brand_share_of_voice_wow = 0 THEN NULL ELSE ROUND((gofish_tmo_brand_share_of_voice - gofish_tmo_brand_share_of_voice_wow) / gofish_tmo_brand_share_of_voice_wow, 6) END AS gofish_tmo_brand_share_of_voice_wow_pct,
        CASE WHEN gofish_tmo_brand_share_of_voice_ly  IS NULL OR gofish_tmo_brand_share_of_voice_ly  = 0 THEN NULL ELSE ROUND((gofish_tmo_brand_share_of_voice - gofish_tmo_brand_share_of_voice_ly)  / gofish_tmo_brand_share_of_voice_ly,  6) END AS gofish_tmo_brand_share_of_voice_yoy_pct,

        gofish_verizon_brand_visibility_score,
        gofish_verizon_brand_visibility_score_wow,
        gofish_verizon_brand_visibility_score_ly,
        CASE WHEN gofish_verizon_brand_visibility_score_wow IS NULL OR gofish_verizon_brand_visibility_score_wow = 0 THEN NULL ELSE ROUND((gofish_verizon_brand_visibility_score - gofish_verizon_brand_visibility_score_wow) / gofish_verizon_brand_visibility_score_wow, 6) END AS gofish_verizon_brand_visibility_score_wow_pct,
        CASE WHEN gofish_verizon_brand_visibility_score_ly  IS NULL OR gofish_verizon_brand_visibility_score_ly  = 0 THEN NULL ELSE ROUND((gofish_verizon_brand_visibility_score - gofish_verizon_brand_visibility_score_ly)  / gofish_verizon_brand_visibility_score_ly,  6) END AS gofish_verizon_brand_visibility_score_yoy_pct,

        gofish_verizon_brand_executions,
        gofish_verizon_brand_executions_wow,
        gofish_verizon_brand_executions_ly,
        CASE WHEN gofish_verizon_brand_executions_wow IS NULL OR gofish_verizon_brand_executions_wow = 0 THEN NULL ELSE ROUND((gofish_verizon_brand_executions - gofish_verizon_brand_executions_wow) / gofish_verizon_brand_executions_wow, 6) END AS gofish_verizon_brand_executions_wow_pct,
        CASE WHEN gofish_verizon_brand_executions_ly  IS NULL OR gofish_verizon_brand_executions_ly  = 0 THEN NULL ELSE ROUND((gofish_verizon_brand_executions - gofish_verizon_brand_executions_ly)  / gofish_verizon_brand_executions_ly,  6) END AS gofish_verizon_brand_executions_yoy_pct,

        gofish_verizon_brand_mentions_count,
        gofish_verizon_brand_mentions_count_wow,
        gofish_verizon_brand_mentions_count_ly,
        CASE WHEN gofish_verizon_brand_mentions_count_wow IS NULL OR gofish_verizon_brand_mentions_count_wow = 0 THEN NULL ELSE ROUND((gofish_verizon_brand_mentions_count - gofish_verizon_brand_mentions_count_wow) / gofish_verizon_brand_mentions_count_wow, 6) END AS gofish_verizon_brand_mentions_count_wow_pct,
        CASE WHEN gofish_verizon_brand_mentions_count_ly  IS NULL OR gofish_verizon_brand_mentions_count_ly  = 0 THEN NULL ELSE ROUND((gofish_verizon_brand_mentions_count - gofish_verizon_brand_mentions_count_ly)  / gofish_verizon_brand_mentions_count_ly,  6) END AS gofish_verizon_brand_mentions_count_yoy_pct,

        gofish_verizon_brand_share_of_voice,
        gofish_verizon_brand_share_of_voice_wow,
        gofish_verizon_brand_share_of_voice_ly,
        CASE WHEN gofish_verizon_brand_share_of_voice_wow IS NULL OR gofish_verizon_brand_share_of_voice_wow = 0 THEN NULL ELSE ROUND((gofish_verizon_brand_share_of_voice - gofish_verizon_brand_share_of_voice_wow) / gofish_verizon_brand_share_of_voice_wow, 6) END AS gofish_verizon_brand_share_of_voice_wow_pct,
        CASE WHEN gofish_verizon_brand_share_of_voice_ly  IS NULL OR gofish_verizon_brand_share_of_voice_ly  = 0 THEN NULL ELSE ROUND((gofish_verizon_brand_share_of_voice - gofish_verizon_brand_share_of_voice_ly)  / gofish_verizon_brand_share_of_voice_ly,  6) END AS gofish_verizon_brand_share_of_voice_yoy_pct,

        gofish_att_brand_visibility_score,
        gofish_att_brand_visibility_score_wow,
        gofish_att_brand_visibility_score_ly,
        CASE WHEN gofish_att_brand_visibility_score_wow IS NULL OR gofish_att_brand_visibility_score_wow = 0 THEN NULL ELSE ROUND((gofish_att_brand_visibility_score - gofish_att_brand_visibility_score_wow) / gofish_att_brand_visibility_score_wow, 6) END AS gofish_att_brand_visibility_score_wow_pct,
        CASE WHEN gofish_att_brand_visibility_score_ly  IS NULL OR gofish_att_brand_visibility_score_ly  = 0 THEN NULL ELSE ROUND((gofish_att_brand_visibility_score - gofish_att_brand_visibility_score_ly)  / gofish_att_brand_visibility_score_ly,  6) END AS gofish_att_brand_visibility_score_yoy_pct,

        gofish_att_brand_executions,
        gofish_att_brand_executions_wow,
        gofish_att_brand_executions_ly,
        CASE WHEN gofish_att_brand_executions_wow IS NULL OR gofish_att_brand_executions_wow = 0 THEN NULL ELSE ROUND((gofish_att_brand_executions - gofish_att_brand_executions_wow) / gofish_att_brand_executions_wow, 6) END AS gofish_att_brand_executions_wow_pct,
        CASE WHEN gofish_att_brand_executions_ly  IS NULL OR gofish_att_brand_executions_ly  = 0 THEN NULL ELSE ROUND((gofish_att_brand_executions - gofish_att_brand_executions_ly)  / gofish_att_brand_executions_ly,  6) END AS gofish_att_brand_executions_yoy_pct,

        gofish_att_brand_mentions_count,
        gofish_att_brand_mentions_count_wow,
        gofish_att_brand_mentions_count_ly,
        CASE WHEN gofish_att_brand_mentions_count_wow IS NULL OR gofish_att_brand_mentions_count_wow = 0 THEN NULL ELSE ROUND((gofish_att_brand_mentions_count - gofish_att_brand_mentions_count_wow) / gofish_att_brand_mentions_count_wow, 6) END AS gofish_att_brand_mentions_count_wow_pct,
        CASE WHEN gofish_att_brand_mentions_count_ly  IS NULL OR gofish_att_brand_mentions_count_ly  = 0 THEN NULL ELSE ROUND((gofish_att_brand_mentions_count - gofish_att_brand_mentions_count_ly)  / gofish_att_brand_mentions_count_ly,  6) END AS gofish_att_brand_mentions_count_yoy_pct,

        gofish_att_brand_share_of_voice,
        gofish_att_brand_share_of_voice_wow,
        gofish_att_brand_share_of_voice_ly,
        CASE WHEN gofish_att_brand_share_of_voice_wow IS NULL OR gofish_att_brand_share_of_voice_wow = 0 THEN NULL ELSE ROUND((gofish_att_brand_share_of_voice - gofish_att_brand_share_of_voice_wow) / gofish_att_brand_share_of_voice_wow, 6) END AS gofish_att_brand_share_of_voice_wow_pct,
        CASE WHEN gofish_att_brand_share_of_voice_ly  IS NULL OR gofish_att_brand_share_of_voice_ly  = 0 THEN NULL ELSE ROUND((gofish_att_brand_share_of_voice - gofish_att_brand_share_of_voice_ly)  / gofish_att_brand_share_of_voice_ly,  6) END AS gofish_att_brand_share_of_voice_yoy_pct

    FROM with_comparisons
),

with_max_date AS (
    SELECT *,
        MAX(CASE
            WHEN gofish_tmo_brand_visibility_score     IS NOT NULL
              OR gofish_verizon_brand_visibility_score IS NOT NULL
              OR gofish_att_brand_visibility_score     IS NOT NULL
            THEN week_sun_to_sat END) OVER ()           AS max_data_date
    FROM with_pcts
)

SELECT
    week_sun_to_sat,
    'GOFISH'                                        AS data_source,
    'AI SEARCH'                                     AS channel,
    max_data_date,

    gofish_tmo_brand_visibility_score,
    gofish_tmo_brand_visibility_score_wow,
    gofish_tmo_brand_visibility_score_ly,
    gofish_tmo_brand_visibility_score_wow_pct,
    gofish_tmo_brand_visibility_score_yoy_pct,

    gofish_tmo_brand_executions,
    gofish_tmo_brand_executions_wow,
    gofish_tmo_brand_executions_ly,
    gofish_tmo_brand_executions_wow_pct,
    gofish_tmo_brand_executions_yoy_pct,

    gofish_tmo_brand_mentions_count,
    gofish_tmo_brand_mentions_count_wow,
    gofish_tmo_brand_mentions_count_ly,
    gofish_tmo_brand_mentions_count_wow_pct,
    gofish_tmo_brand_mentions_count_yoy_pct,

    gofish_tmo_brand_share_of_voice,
    gofish_tmo_brand_share_of_voice_wow,
    gofish_tmo_brand_share_of_voice_ly,
    gofish_tmo_brand_share_of_voice_wow_pct,
    gofish_tmo_brand_share_of_voice_yoy_pct,

    gofish_verizon_brand_visibility_score,
    gofish_verizon_brand_visibility_score_wow,
    gofish_verizon_brand_visibility_score_ly,
    gofish_verizon_brand_visibility_score_wow_pct,
    gofish_verizon_brand_visibility_score_yoy_pct,

    gofish_verizon_brand_executions,
    gofish_verizon_brand_executions_wow,
    gofish_verizon_brand_executions_ly,
    gofish_verizon_brand_executions_wow_pct,
    gofish_verizon_brand_executions_yoy_pct,

    gofish_verizon_brand_mentions_count,
    gofish_verizon_brand_mentions_count_wow,
    gofish_verizon_brand_mentions_count_ly,
    gofish_verizon_brand_mentions_count_wow_pct,
    gofish_verizon_brand_mentions_count_yoy_pct,

    gofish_verizon_brand_share_of_voice,
    gofish_verizon_brand_share_of_voice_wow,
    gofish_verizon_brand_share_of_voice_ly,
    gofish_verizon_brand_share_of_voice_wow_pct,
    gofish_verizon_brand_share_of_voice_yoy_pct,

    gofish_att_brand_visibility_score,
    gofish_att_brand_visibility_score_wow,
    gofish_att_brand_visibility_score_ly,
    gofish_att_brand_visibility_score_wow_pct,
    gofish_att_brand_visibility_score_yoy_pct,

    gofish_att_brand_executions,
    gofish_att_brand_executions_wow,
    gofish_att_brand_executions_ly,
    gofish_att_brand_executions_wow_pct,
    gofish_att_brand_executions_yoy_pct,

    gofish_att_brand_mentions_count,
    gofish_att_brand_mentions_count_wow,
    gofish_att_brand_mentions_count_ly,
    gofish_att_brand_mentions_count_wow_pct,
    gofish_att_brand_mentions_count_yoy_pct,

    gofish_att_brand_share_of_voice,
    gofish_att_brand_share_of_voice_wow,
    gofish_att_brand_share_of_voice_ly,
    gofish_att_brand_share_of_voice_wow_pct,
    gofish_att_brand_share_of_voice_yoy_pct

FROM with_max_date
;