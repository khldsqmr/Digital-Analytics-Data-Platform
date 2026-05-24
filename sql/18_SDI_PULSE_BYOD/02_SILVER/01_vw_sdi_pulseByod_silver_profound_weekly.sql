/* =================================================================================================
FILE:         01_vw_sdi_pulseByod_silver_profound_weekly.sql
LAYER:        Silver View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseByod_silver_profound_weekly

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_bronze_profound_weekly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profound_weekly

PURPOSE:
  Silver view for Profound NON-BRANDED AI visibility data.
  Outputs a WIDE table — one row per week_sun_to_sat.
  All metric columns prefixed with 'profound_' for unambiguous
  identification in Gold Wide spine join and Gold Long unpivot.
  Applies BYOD tag filter, asset name filter, week-end Saturday
  conversion, WoW/LY comparisons, and max_data_date per source.

BUSINESS GRAIN:
  One row per:
    week_sun_to_sat

FILTERS APPLIED:
  - tag = 'BYOD'
  - asset_name IN ('T-Mobile', 'Verizon', 'AT&T')

BUSINESS LOGIC APPLIED:
  - data_source = 'PROFOUND'
  - channel     = 'AI SEARCH'
  - brand_type  = 'NONBRAND' (entire source is non-branded by definition)
  - week_sun_to_sat = DATE_ADD(event_date_sun, INTERVAL 6 DAY)
  - All metric columns prefixed: profound_{asset}_{metric}
  - WoW: self-join on week_sun_to_sat - 7 days (gap-safe)
  - LY : self-join on custom_week_num - 52 (gap-safe, Sun-to-Sat week)
  - wow_pct and yoy_pct as decimals (e.g. 0.051 = 5.1%)
    NULL when prior value is NULL or 0
  - max_data_date: latest week_sun_to_sat with any non-null metric value

COLUMN NAMING CONVENTION:
  profound_{asset}_{metric}
  profound_{asset}_{metric}_wow
  profound_{asset}_{metric}_ly
  profound_{asset}_{metric}_wow_pct
  profound_{asset}_{metric}_yoy_pct

  Where asset:
    tmo     = T-Mobile
    verizon = Verizon
    att     = AT&T

  Where metric:
    nonbrand_visibility_score
    nonbrand_executions
    nonbrand_mentions_count
    nonbrand_share_of_voice

CUSTOM WEEK NUMBER:
  Anchored to 2023-01-01 (a Sunday) for consistent Sun-to-Sat week numbering:
    custom_week_num = DATE_DIFF(DATE_SUB(week_sun_to_sat, INTERVAL 6 DAY), DATE '2023-01-01', WEEK)
  LY match: current.custom_week_num - prior.custom_week_num = 52

KEY MODELING NOTES:
  - No aggregation needed — source is already weekly at asset + tag grain
  - Pivot from long (one row per asset) to wide (one row per week) using MAX(CASE WHEN)
  - Self-joins on small weekly CTE — fast and cheap
  - NULLs preserved — no fake zeroes
  - No ORDER BY — applied in Gold only

DOWNSTREAM:
  Gold Wide : vw_sdi_pulseByod_gold_unified_wide
  Gold Long : vw_sdi_pulseByod_gold_unified_long
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profound_weekly`
AS

-- -----------------------------------------------------------------------
-- STEP 1: Filter Bronze, convert to week-end Saturday
-- -----------------------------------------------------------------------
WITH filtered AS (
    SELECT
        DATE_ADD(event_date_sun, INTERVAL 6 DAY)    AS week_sun_to_sat,
        asset_name,
        visibility_score,
        executions,
        mentions_count,
        share_of_voice
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_bronze_profound_weekly`
    WHERE tag        = 'BYOD'
      AND asset_name IN ('T-Mobile', 'Verizon', 'AT&T')
),

-- -----------------------------------------------------------------------
-- STEP 2: Pivot long → wide
-- One row per week with all asset × metric combinations as columns
-- prefixed with 'profound_' for unambiguous Gold identification
-- -----------------------------------------------------------------------
pivoted AS (
    SELECT
        week_sun_to_sat,

        -- T-Mobile
        MAX(CASE WHEN asset_name = 'T-Mobile' THEN visibility_score END) AS profound_tmo_nonbrand_visibility_score,
        MAX(CASE WHEN asset_name = 'T-Mobile' THEN executions      END) AS profound_tmo_nonbrand_executions,
        MAX(CASE WHEN asset_name = 'T-Mobile' THEN mentions_count  END) AS profound_tmo_nonbrand_mentions_count,
        MAX(CASE WHEN asset_name = 'T-Mobile' THEN share_of_voice  END) AS profound_tmo_nonbrand_share_of_voice,

        -- Verizon
        MAX(CASE WHEN asset_name = 'Verizon'  THEN visibility_score END) AS profound_verizon_nonbrand_visibility_score,
        MAX(CASE WHEN asset_name = 'Verizon'  THEN executions      END) AS profound_verizon_nonbrand_executions,
        MAX(CASE WHEN asset_name = 'Verizon'  THEN mentions_count  END) AS profound_verizon_nonbrand_mentions_count,
        MAX(CASE WHEN asset_name = 'Verizon'  THEN share_of_voice  END) AS profound_verizon_nonbrand_share_of_voice,

        -- AT&T
        MAX(CASE WHEN asset_name = 'AT&T'     THEN visibility_score END) AS profound_att_nonbrand_visibility_score,
        MAX(CASE WHEN asset_name = 'AT&T'     THEN executions      END) AS profound_att_nonbrand_executions,
        MAX(CASE WHEN asset_name = 'AT&T'     THEN mentions_count  END) AS profound_att_nonbrand_mentions_count,
        MAX(CASE WHEN asset_name = 'AT&T'     THEN share_of_voice  END) AS profound_att_nonbrand_share_of_voice

    FROM filtered
    GROUP BY week_sun_to_sat
),

-- -----------------------------------------------------------------------
-- STEP 3: Add custom Sun-to-Sat week number for gap-safe LY matching
-- -----------------------------------------------------------------------
with_week_num AS (
    SELECT
        *,
        DATE_DIFF(
            DATE_SUB(week_sun_to_sat, INTERVAL 6 DAY),
            DATE '2023-01-01',
            WEEK
        )                                           AS custom_week_num
    FROM pivoted
),

-- -----------------------------------------------------------------------
-- STEP 4: WoW and LY self-joins
-- Joins on small pivoted CTE (1 row per week — very cheap)
-- WoW: current week = prior week + 7 days
-- LY : same Sun-to-Sat week number 52 weeks prior
-- -----------------------------------------------------------------------
with_comparisons AS (
    SELECT
        c.week_sun_to_sat,
        c.custom_week_num,

        -- Current values
        c.profound_tmo_nonbrand_visibility_score,
        c.profound_tmo_nonbrand_executions,
        c.profound_tmo_nonbrand_mentions_count,
        c.profound_tmo_nonbrand_share_of_voice,
        c.profound_verizon_nonbrand_visibility_score,
        c.profound_verizon_nonbrand_executions,
        c.profound_verizon_nonbrand_mentions_count,
        c.profound_verizon_nonbrand_share_of_voice,
        c.profound_att_nonbrand_visibility_score,
        c.profound_att_nonbrand_executions,
        c.profound_att_nonbrand_mentions_count,
        c.profound_att_nonbrand_share_of_voice,

        -- WoW values (prior week)
        w.profound_tmo_nonbrand_visibility_score     AS profound_tmo_nonbrand_visibility_score_wow,
        w.profound_tmo_nonbrand_executions           AS profound_tmo_nonbrand_executions_wow,
        w.profound_tmo_nonbrand_mentions_count       AS profound_tmo_nonbrand_mentions_count_wow,
        w.profound_tmo_nonbrand_share_of_voice       AS profound_tmo_nonbrand_share_of_voice_wow,
        w.profound_verizon_nonbrand_visibility_score AS profound_verizon_nonbrand_visibility_score_wow,
        w.profound_verizon_nonbrand_executions       AS profound_verizon_nonbrand_executions_wow,
        w.profound_verizon_nonbrand_mentions_count   AS profound_verizon_nonbrand_mentions_count_wow,
        w.profound_verizon_nonbrand_share_of_voice   AS profound_verizon_nonbrand_share_of_voice_wow,
        w.profound_att_nonbrand_visibility_score     AS profound_att_nonbrand_visibility_score_wow,
        w.profound_att_nonbrand_executions           AS profound_att_nonbrand_executions_wow,
        w.profound_att_nonbrand_mentions_count       AS profound_att_nonbrand_mentions_count_wow,
        w.profound_att_nonbrand_share_of_voice       AS profound_att_nonbrand_share_of_voice_wow,

        -- LY values (same week last year)
        l.profound_tmo_nonbrand_visibility_score     AS profound_tmo_nonbrand_visibility_score_ly,
        l.profound_tmo_nonbrand_executions           AS profound_tmo_nonbrand_executions_ly,
        l.profound_tmo_nonbrand_mentions_count       AS profound_tmo_nonbrand_mentions_count_ly,
        l.profound_tmo_nonbrand_share_of_voice       AS profound_tmo_nonbrand_share_of_voice_ly,
        l.profound_verizon_nonbrand_visibility_score AS profound_verizon_nonbrand_visibility_score_ly,
        l.profound_verizon_nonbrand_executions       AS profound_verizon_nonbrand_executions_ly,
        l.profound_verizon_nonbrand_mentions_count   AS profound_verizon_nonbrand_mentions_count_ly,
        l.profound_verizon_nonbrand_share_of_voice   AS profound_verizon_nonbrand_share_of_voice_ly,
        l.profound_att_nonbrand_visibility_score     AS profound_att_nonbrand_visibility_score_ly,
        l.profound_att_nonbrand_executions           AS profound_att_nonbrand_executions_ly,
        l.profound_att_nonbrand_mentions_count       AS profound_att_nonbrand_mentions_count_ly,
        l.profound_att_nonbrand_share_of_voice       AS profound_att_nonbrand_share_of_voice_ly

    FROM with_week_num c
    LEFT JOIN with_week_num w
      ON c.week_sun_to_sat = DATE_ADD(w.week_sun_to_sat, INTERVAL 7 DAY)
    LEFT JOIN with_week_num l
      ON (c.custom_week_num - l.custom_week_num) = 52
),

-- -----------------------------------------------------------------------
-- STEP 5: Compute wow_pct and yoy_pct for all metrics
-- NULL when prior value is NULL or 0 — no fake zeroes
-- -----------------------------------------------------------------------
with_pcts AS (
    SELECT
        week_sun_to_sat,
        custom_week_num,

        -- ---- T-Mobile visibility_score ----
        profound_tmo_nonbrand_visibility_score,
        profound_tmo_nonbrand_visibility_score_wow,
        profound_tmo_nonbrand_visibility_score_ly,
        CASE WHEN profound_tmo_nonbrand_visibility_score_wow IS NULL OR profound_tmo_nonbrand_visibility_score_wow = 0 THEN NULL
             ELSE ROUND((profound_tmo_nonbrand_visibility_score - profound_tmo_nonbrand_visibility_score_wow) / profound_tmo_nonbrand_visibility_score_wow, 6) END AS profound_tmo_nonbrand_visibility_score_wow_pct,
        CASE WHEN profound_tmo_nonbrand_visibility_score_ly  IS NULL OR profound_tmo_nonbrand_visibility_score_ly  = 0 THEN NULL
             ELSE ROUND((profound_tmo_nonbrand_visibility_score - profound_tmo_nonbrand_visibility_score_ly)  / profound_tmo_nonbrand_visibility_score_ly,  6) END AS profound_tmo_nonbrand_visibility_score_yoy_pct,

        -- ---- T-Mobile executions ----
        profound_tmo_nonbrand_executions,
        profound_tmo_nonbrand_executions_wow,
        profound_tmo_nonbrand_executions_ly,
        CASE WHEN profound_tmo_nonbrand_executions_wow IS NULL OR profound_tmo_nonbrand_executions_wow = 0 THEN NULL
             ELSE ROUND((profound_tmo_nonbrand_executions - profound_tmo_nonbrand_executions_wow) / profound_tmo_nonbrand_executions_wow, 6) END AS profound_tmo_nonbrand_executions_wow_pct,
        CASE WHEN profound_tmo_nonbrand_executions_ly  IS NULL OR profound_tmo_nonbrand_executions_ly  = 0 THEN NULL
             ELSE ROUND((profound_tmo_nonbrand_executions - profound_tmo_nonbrand_executions_ly)  / profound_tmo_nonbrand_executions_ly,  6) END AS profound_tmo_nonbrand_executions_yoy_pct,

        -- ---- T-Mobile mentions_count ----
        profound_tmo_nonbrand_mentions_count,
        profound_tmo_nonbrand_mentions_count_wow,
        profound_tmo_nonbrand_mentions_count_ly,
        CASE WHEN profound_tmo_nonbrand_mentions_count_wow IS NULL OR profound_tmo_nonbrand_mentions_count_wow = 0 THEN NULL
             ELSE ROUND((profound_tmo_nonbrand_mentions_count - profound_tmo_nonbrand_mentions_count_wow) / profound_tmo_nonbrand_mentions_count_wow, 6) END AS profound_tmo_nonbrand_mentions_count_wow_pct,
        CASE WHEN profound_tmo_nonbrand_mentions_count_ly  IS NULL OR profound_tmo_nonbrand_mentions_count_ly  = 0 THEN NULL
             ELSE ROUND((profound_tmo_nonbrand_mentions_count - profound_tmo_nonbrand_mentions_count_ly)  / profound_tmo_nonbrand_mentions_count_ly,  6) END AS profound_tmo_nonbrand_mentions_count_yoy_pct,

        -- ---- T-Mobile share_of_voice ----
        profound_tmo_nonbrand_share_of_voice,
        profound_tmo_nonbrand_share_of_voice_wow,
        profound_tmo_nonbrand_share_of_voice_ly,
        CASE WHEN profound_tmo_nonbrand_share_of_voice_wow IS NULL OR profound_tmo_nonbrand_share_of_voice_wow = 0 THEN NULL
             ELSE ROUND((profound_tmo_nonbrand_share_of_voice - profound_tmo_nonbrand_share_of_voice_wow) / profound_tmo_nonbrand_share_of_voice_wow, 6) END AS profound_tmo_nonbrand_share_of_voice_wow_pct,
        CASE WHEN profound_tmo_nonbrand_share_of_voice_ly  IS NULL OR profound_tmo_nonbrand_share_of_voice_ly  = 0 THEN NULL
             ELSE ROUND((profound_tmo_nonbrand_share_of_voice - profound_tmo_nonbrand_share_of_voice_ly)  / profound_tmo_nonbrand_share_of_voice_ly,  6) END AS profound_tmo_nonbrand_share_of_voice_yoy_pct,

        -- ---- Verizon visibility_score ----
        profound_verizon_nonbrand_visibility_score,
        profound_verizon_nonbrand_visibility_score_wow,
        profound_verizon_nonbrand_visibility_score_ly,
        CASE WHEN profound_verizon_nonbrand_visibility_score_wow IS NULL OR profound_verizon_nonbrand_visibility_score_wow = 0 THEN NULL
             ELSE ROUND((profound_verizon_nonbrand_visibility_score - profound_verizon_nonbrand_visibility_score_wow) / profound_verizon_nonbrand_visibility_score_wow, 6) END AS profound_verizon_nonbrand_visibility_score_wow_pct,
        CASE WHEN profound_verizon_nonbrand_visibility_score_ly  IS NULL OR profound_verizon_nonbrand_visibility_score_ly  = 0 THEN NULL
             ELSE ROUND((profound_verizon_nonbrand_visibility_score - profound_verizon_nonbrand_visibility_score_ly)  / profound_verizon_nonbrand_visibility_score_ly,  6) END AS profound_verizon_nonbrand_visibility_score_yoy_pct,

        -- ---- Verizon executions ----
        profound_verizon_nonbrand_executions,
        profound_verizon_nonbrand_executions_wow,
        profound_verizon_nonbrand_executions_ly,
        CASE WHEN profound_verizon_nonbrand_executions_wow IS NULL OR profound_verizon_nonbrand_executions_wow = 0 THEN NULL
             ELSE ROUND((profound_verizon_nonbrand_executions - profound_verizon_nonbrand_executions_wow) / profound_verizon_nonbrand_executions_wow, 6) END AS profound_verizon_nonbrand_executions_wow_pct,
        CASE WHEN profound_verizon_nonbrand_executions_ly  IS NULL OR profound_verizon_nonbrand_executions_ly  = 0 THEN NULL
             ELSE ROUND((profound_verizon_nonbrand_executions - profound_verizon_nonbrand_executions_ly)  / profound_verizon_nonbrand_executions_ly,  6) END AS profound_verizon_nonbrand_executions_yoy_pct,

        -- ---- Verizon mentions_count ----
        profound_verizon_nonbrand_mentions_count,
        profound_verizon_nonbrand_mentions_count_wow,
        profound_verizon_nonbrand_mentions_count_ly,
        CASE WHEN profound_verizon_nonbrand_mentions_count_wow IS NULL OR profound_verizon_nonbrand_mentions_count_wow = 0 THEN NULL
             ELSE ROUND((profound_verizon_nonbrand_mentions_count - profound_verizon_nonbrand_mentions_count_wow) / profound_verizon_nonbrand_mentions_count_wow, 6) END AS profound_verizon_nonbrand_mentions_count_wow_pct,
        CASE WHEN profound_verizon_nonbrand_mentions_count_ly  IS NULL OR profound_verizon_nonbrand_mentions_count_ly  = 0 THEN NULL
             ELSE ROUND((profound_verizon_nonbrand_mentions_count - profound_verizon_nonbrand_mentions_count_ly)  / profound_verizon_nonbrand_mentions_count_ly,  6) END AS profound_verizon_nonbrand_mentions_count_yoy_pct,

        -- ---- Verizon share_of_voice ----
        profound_verizon_nonbrand_share_of_voice,
        profound_verizon_nonbrand_share_of_voice_wow,
        profound_verizon_nonbrand_share_of_voice_ly,
        CASE WHEN profound_verizon_nonbrand_share_of_voice_wow IS NULL OR profound_verizon_nonbrand_share_of_voice_wow = 0 THEN NULL
             ELSE ROUND((profound_verizon_nonbrand_share_of_voice - profound_verizon_nonbrand_share_of_voice_wow) / profound_verizon_nonbrand_share_of_voice_wow, 6) END AS profound_verizon_nonbrand_share_of_voice_wow_pct,
        CASE WHEN profound_verizon_nonbrand_share_of_voice_ly  IS NULL OR profound_verizon_nonbrand_share_of_voice_ly  = 0 THEN NULL
             ELSE ROUND((profound_verizon_nonbrand_share_of_voice - profound_verizon_nonbrand_share_of_voice_ly)  / profound_verizon_nonbrand_share_of_voice_ly,  6) END AS profound_verizon_nonbrand_share_of_voice_yoy_pct,

        -- ---- AT&T visibility_score ----
        profound_att_nonbrand_visibility_score,
        profound_att_nonbrand_visibility_score_wow,
        profound_att_nonbrand_visibility_score_ly,
        CASE WHEN profound_att_nonbrand_visibility_score_wow IS NULL OR profound_att_nonbrand_visibility_score_wow = 0 THEN NULL
             ELSE ROUND((profound_att_nonbrand_visibility_score - profound_att_nonbrand_visibility_score_wow) / profound_att_nonbrand_visibility_score_wow, 6) END AS profound_att_nonbrand_visibility_score_wow_pct,
        CASE WHEN profound_att_nonbrand_visibility_score_ly  IS NULL OR profound_att_nonbrand_visibility_score_ly  = 0 THEN NULL
             ELSE ROUND((profound_att_nonbrand_visibility_score - profound_att_nonbrand_visibility_score_ly)  / profound_att_nonbrand_visibility_score_ly,  6) END AS profound_att_nonbrand_visibility_score_yoy_pct,

        -- ---- AT&T executions ----
        profound_att_nonbrand_executions,
        profound_att_nonbrand_executions_wow,
        profound_att_nonbrand_executions_ly,
        CASE WHEN profound_att_nonbrand_executions_wow IS NULL OR profound_att_nonbrand_executions_wow = 0 THEN NULL
             ELSE ROUND((profound_att_nonbrand_executions - profound_att_nonbrand_executions_wow) / profound_att_nonbrand_executions_wow, 6) END AS profound_att_nonbrand_executions_wow_pct,
        CASE WHEN profound_att_nonbrand_executions_ly  IS NULL OR profound_att_nonbrand_executions_ly  = 0 THEN NULL
             ELSE ROUND((profound_att_nonbrand_executions - profound_att_nonbrand_executions_ly)  / profound_att_nonbrand_executions_ly,  6) END AS profound_att_nonbrand_executions_yoy_pct,

        -- ---- AT&T mentions_count ----
        profound_att_nonbrand_mentions_count,
        profound_att_nonbrand_mentions_count_wow,
        profound_att_nonbrand_mentions_count_ly,
        CASE WHEN profound_att_nonbrand_mentions_count_wow IS NULL OR profound_att_nonbrand_mentions_count_wow = 0 THEN NULL
             ELSE ROUND((profound_att_nonbrand_mentions_count - profound_att_nonbrand_mentions_count_wow) / profound_att_nonbrand_mentions_count_wow, 6) END AS profound_att_nonbrand_mentions_count_wow_pct,
        CASE WHEN profound_att_nonbrand_mentions_count_ly  IS NULL OR profound_att_nonbrand_mentions_count_ly  = 0 THEN NULL
             ELSE ROUND((profound_att_nonbrand_mentions_count - profound_att_nonbrand_mentions_count_ly)  / profound_att_nonbrand_mentions_count_ly,  6) END AS profound_att_nonbrand_mentions_count_yoy_pct,

        -- ---- AT&T share_of_voice ----
        profound_att_nonbrand_share_of_voice,
        profound_att_nonbrand_share_of_voice_wow,
        profound_att_nonbrand_share_of_voice_ly,
        CASE WHEN profound_att_nonbrand_share_of_voice_wow IS NULL OR profound_att_nonbrand_share_of_voice_wow = 0 THEN NULL
             ELSE ROUND((profound_att_nonbrand_share_of_voice - profound_att_nonbrand_share_of_voice_wow) / profound_att_nonbrand_share_of_voice_wow, 6) END AS profound_att_nonbrand_share_of_voice_wow_pct,
        CASE WHEN profound_att_nonbrand_share_of_voice_ly  IS NULL OR profound_att_nonbrand_share_of_voice_ly  = 0 THEN NULL
             ELSE ROUND((profound_att_nonbrand_share_of_voice - profound_att_nonbrand_share_of_voice_ly)  / profound_att_nonbrand_share_of_voice_ly,  6) END AS profound_att_nonbrand_share_of_voice_yoy_pct

    FROM with_comparisons
),

-- -----------------------------------------------------------------------
-- STEP 6: max_data_date per source
-- Latest week_sun_to_sat where any metric is non-null
-- -----------------------------------------------------------------------
with_max_date AS (
    SELECT
        *,
        MAX(CASE
            WHEN profound_tmo_nonbrand_visibility_score     IS NOT NULL
              OR profound_verizon_nonbrand_visibility_score IS NOT NULL
              OR profound_att_nonbrand_visibility_score     IS NOT NULL
            THEN week_sun_to_sat
        END) OVER ()                                AS max_data_date
    FROM with_pcts
)

-- -----------------------------------------------------------------------
-- FINAL OUTPUT
-- Wide table — one row per week_sun_to_sat
-- All columns prefixed with 'profound_'
-- data_source and channel as static columns
-- -----------------------------------------------------------------------
SELECT
    week_sun_to_sat,
    'PROFOUND'                                      AS data_source,
    'AI SEARCH'                                     AS channel,
    max_data_date,

    -- T-Mobile
    profound_tmo_nonbrand_visibility_score,
    profound_tmo_nonbrand_visibility_score_wow,
    profound_tmo_nonbrand_visibility_score_ly,
    profound_tmo_nonbrand_visibility_score_wow_pct,
    profound_tmo_nonbrand_visibility_score_yoy_pct,

    profound_tmo_nonbrand_executions,
    profound_tmo_nonbrand_executions_wow,
    profound_tmo_nonbrand_executions_ly,
    profound_tmo_nonbrand_executions_wow_pct,
    profound_tmo_nonbrand_executions_yoy_pct,

    profound_tmo_nonbrand_mentions_count,
    profound_tmo_nonbrand_mentions_count_wow,
    profound_tmo_nonbrand_mentions_count_ly,
    profound_tmo_nonbrand_mentions_count_wow_pct,
    profound_tmo_nonbrand_mentions_count_yoy_pct,

    profound_tmo_nonbrand_share_of_voice,
    profound_tmo_nonbrand_share_of_voice_wow,
    profound_tmo_nonbrand_share_of_voice_ly,
    profound_tmo_nonbrand_share_of_voice_wow_pct,
    profound_tmo_nonbrand_share_of_voice_yoy_pct,

    -- Verizon
    profound_verizon_nonbrand_visibility_score,
    profound_verizon_nonbrand_visibility_score_wow,
    profound_verizon_nonbrand_visibility_score_ly,
    profound_verizon_nonbrand_visibility_score_wow_pct,
    profound_verizon_nonbrand_visibility_score_yoy_pct,

    profound_verizon_nonbrand_executions,
    profound_verizon_nonbrand_executions_wow,
    profound_verizon_nonbrand_executions_ly,
    profound_verizon_nonbrand_executions_wow_pct,
    profound_verizon_nonbrand_executions_yoy_pct,

    profound_verizon_nonbrand_mentions_count,
    profound_verizon_nonbrand_mentions_count_wow,
    profound_verizon_nonbrand_mentions_count_ly,
    profound_verizon_nonbrand_mentions_count_wow_pct,
    profound_verizon_nonbrand_mentions_count_yoy_pct,

    profound_verizon_nonbrand_share_of_voice,
    profound_verizon_nonbrand_share_of_voice_wow,
    profound_verizon_nonbrand_share_of_voice_ly,
    profound_verizon_nonbrand_share_of_voice_wow_pct,
    profound_verizon_nonbrand_share_of_voice_yoy_pct,

    -- AT&T
    profound_att_nonbrand_visibility_score,
    profound_att_nonbrand_visibility_score_wow,
    profound_att_nonbrand_visibility_score_ly,
    profound_att_nonbrand_visibility_score_wow_pct,
    profound_att_nonbrand_visibility_score_yoy_pct,

    profound_att_nonbrand_executions,
    profound_att_nonbrand_executions_wow,
    profound_att_nonbrand_executions_ly,
    profound_att_nonbrand_executions_wow_pct,
    profound_att_nonbrand_executions_yoy_pct,

    profound_att_nonbrand_mentions_count,
    profound_att_nonbrand_mentions_count_wow,
    profound_att_nonbrand_mentions_count_ly,
    profound_att_nonbrand_mentions_count_wow_pct,
    profound_att_nonbrand_mentions_count_yoy_pct,

    profound_att_nonbrand_share_of_voice,
    profound_att_nonbrand_share_of_voice_wow,
    profound_att_nonbrand_share_of_voice_ly,
    profound_att_nonbrand_share_of_voice_wow_pct,
    profound_att_nonbrand_share_of_voice_yoy_pct

FROM with_max_date
;