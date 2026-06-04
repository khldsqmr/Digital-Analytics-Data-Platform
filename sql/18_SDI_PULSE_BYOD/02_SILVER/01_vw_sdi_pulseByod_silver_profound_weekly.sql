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
  Applies BYOD tag filter, asset/domain filter, week-end Saturday
  conversion, WoW/LY comparisons, and max_data_date per source.

  Handles two source types from Bronze:
    VIS : brand name visibility metrics (visibility_score, executions,
          mentions_count, share_of_voice) — pivoted on asset_name
    CIT : domain citation share of voice (share_of_voice only)
          — resolved from root_domain to friendly asset name via
            resolved_asset CTE before pivoting

BUSINESS GRAIN:
  One row per:
    week_sun_to_sat

FILTERS APPLIED:
  - tag = 'BYOD'
  - VIS: asset_name IN ('T-Mobile', 'Verizon', 'AT&T')
  - CIT: root_domain IN ('t-mobile.com', 'verizon.com', 'att.com')

BUSINESS LOGIC APPLIED:
  - data_source = 'PROFOUND'
  - channel     = 'AI SEARCH'
  - brand_type  = 'NONBRAND' (entire source is non-branded by definition)
  - week_sun_to_sat = DATE_ADD(event_date_sun, INTERVAL 6 DAY)
  - All VIS metric columns prefixed : profoundVis_{asset}_nonbrand_{metric}
  - All CIT metric columns prefixed : profoundCit_{asset}_nonbrand_share_of_voice
  - WoW: self-join on week_sun_to_sat - 7 days (gap-safe)
  - LY : self-join on custom_week_num - 52 (gap-safe, Sun-to-Sat week)
  - wow_pct and yoy_pct as decimals (e.g. 0.051 = 5.1%)
    NULL when prior value is NULL or 0
  - max_data_date: latest week_sun_to_sat with any non-null metric value
    across both VIS and CIT metrics

COLUMN NAMING CONVENTION:
  VIS metrics:
    profoundVis_{asset}_nonbrand_{metric}
    profoundVis_{asset}_nonbrand_{metric}_wow
    profoundVis_{asset}_nonbrand_{metric}_ly
    profoundVis_{asset}_nonbrand_{metric}_wow_pct
    profoundVis_{asset}_nonbrand_{metric}_yoy_pct

  CIT metrics:
    profoundCit_{asset}_nonbrand_share_of_voice
    profoundCit_{asset}_nonbrand_share_of_voice_wow
    profoundCit_{asset}_nonbrand_share_of_voice_ly
    profoundCit_{asset}_nonbrand_share_of_voice_wow_pct
    profoundCit_{asset}_nonbrand_share_of_voice_yoy_pct

  Where asset:
    tmo     = T-Mobile
    verizon = Verizon
    att     = AT&T

  VIS metrics:
    nonbrand_visibility_score
    nonbrand_executions
    nonbrand_mentions_count
    nonbrand_share_of_voice

  CIT metric:
    cit_share_of_voice

CUSTOM WEEK NUMBER:
  Anchored to 2023-01-01 (a Sunday) for consistent Sun-to-Sat week numbering:
    custom_week_num = DATE_DIFF(DATE_SUB(week_sun_to_sat, INTERVAL 6 DAY), DATE '2023-01-01', WEEK)
  LY match: current.custom_week_num - prior.custom_week_num = 52

KEY MODELING NOTES:
  - No aggregation needed — source is already weekly at asset/domain + tag grain
  - CIT root_domain mapped to friendly asset name in resolved_asset CTE before pivot
  - Pivot from long (one row per asset) to wide (one row per week) using MAX(CASE WHEN)
  - VIS and CIT filtered separately then joined on week_sun_to_sat before self-joins
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
-- STEP 1a: Filter Bronze VIS rows — asset name filter + week-end Saturday
-- -----------------------------------------------------------------------
WITH vis_filtered AS (
    SELECT
        DATE_ADD(event_date_sun, INTERVAL 6 DAY)    AS week_sun_to_sat,
        asset_name,
        visibility_score,
        executions,
        mentions_count,
        share_of_voice
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_bronze_profound_weekly`
    WHERE source_type = 'VIS'
      AND tag         = 'BYOD'
      AND asset_name IN ('T-Mobile', 'Verizon', 'AT&T')
),

-- -----------------------------------------------------------------------
-- STEP 1b: Filter Bronze CIT rows — domain filter + resolve to asset name
-- root_domain mapped to same friendly names used by VIS pivot
-- -----------------------------------------------------------------------
cit_filtered AS (
    SELECT
        DATE_ADD(event_date_sun, INTERVAL 6 DAY)    AS week_sun_to_sat,
        CASE root_domain
            WHEN 't-mobile.com' THEN 'T-Mobile'
            WHEN 'verizon.com'  THEN 'Verizon'
            WHEN 'att.com'      THEN 'AT&T'
        END                                         AS asset_name,
        share_of_voice
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_bronze_profound_weekly`
    WHERE source_type  = 'CIT'
      AND tag          = 'BYOD'
      AND root_domain IN ('t-mobile.com', 'verizon.com', 'att.com')
),

-- -----------------------------------------------------------------------
-- STEP 2: Pivot VIS long → wide
-- One row per week, all asset × VIS metric combinations as columns
-- -----------------------------------------------------------------------
vis_pivoted AS (
    SELECT
        week_sun_to_sat,

        -- T-Mobile
        MAX(CASE WHEN asset_name = 'T-Mobile' THEN visibility_score END) AS profoundVis_tmo_nonbrand_visibility_score,
        MAX(CASE WHEN asset_name = 'T-Mobile' THEN executions      END) AS profoundVis_tmo_nonbrand_executions,
        MAX(CASE WHEN asset_name = 'T-Mobile' THEN mentions_count  END) AS profoundVis_tmo_nonbrand_mentions_count,
        MAX(CASE WHEN asset_name = 'T-Mobile' THEN share_of_voice  END) AS profoundVis_tmo_nonbrand_share_of_voice,

        -- Verizon
        MAX(CASE WHEN asset_name = 'Verizon'  THEN visibility_score END) AS profoundVis_verizon_nonbrand_visibility_score,
        MAX(CASE WHEN asset_name = 'Verizon'  THEN executions      END) AS profoundVis_verizon_nonbrand_executions,
        MAX(CASE WHEN asset_name = 'Verizon'  THEN mentions_count  END) AS profoundVis_verizon_nonbrand_mentions_count,
        MAX(CASE WHEN asset_name = 'Verizon'  THEN share_of_voice  END) AS profoundVis_verizon_nonbrand_share_of_voice,

        -- AT&T
        MAX(CASE WHEN asset_name = 'AT&T'     THEN visibility_score END) AS profoundVis_att_nonbrand_visibility_score,
        MAX(CASE WHEN asset_name = 'AT&T'     THEN executions      END) AS profoundVis_att_nonbrand_executions,
        MAX(CASE WHEN asset_name = 'AT&T'     THEN mentions_count  END) AS profoundVis_att_nonbrand_mentions_count,
        MAX(CASE WHEN asset_name = 'AT&T'     THEN share_of_voice  END) AS profoundVis_att_nonbrand_share_of_voice

    FROM vis_filtered
    GROUP BY week_sun_to_sat
),

-- -----------------------------------------------------------------------
-- STEP 3: Pivot CIT long → wide
-- One row per week, one SOV column per asset
-- -----------------------------------------------------------------------
cit_pivoted AS (
    SELECT
        week_sun_to_sat,

        -- T-Mobile
        MAX(CASE WHEN asset_name = 'T-Mobile' THEN share_of_voice END) AS profoundCit_tmo_nonbrand_share_of_voice,

        -- Verizon
        MAX(CASE WHEN asset_name = 'Verizon'  THEN share_of_voice END) AS profoundCit_verizon_nonbrand_share_of_voice,

        -- AT&T
        MAX(CASE WHEN asset_name = 'AT&T'     THEN share_of_voice END) AS profoundCit_att_nonbrand_share_of_voice

    FROM cit_filtered
    GROUP BY week_sun_to_sat
),

-- -----------------------------------------------------------------------
-- STEP 4: Join VIS and CIT pivots on week_sun_to_sat
-- FULL OUTER JOIN preserves weeks that exist in one source but not the other
-- -----------------------------------------------------------------------
pivoted AS (
    SELECT
        COALESCE(v.week_sun_to_sat, c.week_sun_to_sat)  AS week_sun_to_sat,

        -- VIS metrics
        v.profoundVis_tmo_nonbrand_visibility_score,
        v.profoundVis_tmo_nonbrand_executions,
        v.profoundVis_tmo_nonbrand_mentions_count,
        v.profoundVis_tmo_nonbrand_share_of_voice,
        v.profoundVis_verizon_nonbrand_visibility_score,
        v.profoundVis_verizon_nonbrand_executions,
        v.profoundVis_verizon_nonbrand_mentions_count,
        v.profoundVis_verizon_nonbrand_share_of_voice,
        v.profoundVis_att_nonbrand_visibility_score,
        v.profoundVis_att_nonbrand_executions,
        v.profoundVis_att_nonbrand_mentions_count,
        v.profoundVis_att_nonbrand_share_of_voice,

        -- CIT metrics
        c.profoundCit_tmo_nonbrand_share_of_voice,
        c.profoundCit_verizon_nonbrand_share_of_voice,
        c.profoundCit_att_nonbrand_share_of_voice

    FROM vis_pivoted v
    FULL OUTER JOIN cit_pivoted c
      ON v.week_sun_to_sat = c.week_sun_to_sat
),

-- -----------------------------------------------------------------------
-- STEP 5: Add custom Sun-to-Sat week number for gap-safe LY matching
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
-- STEP 6: WoW and LY self-joins
-- WoW: current week = prior week + 7 days
-- LY : same Sun-to-Sat week number 52 weeks prior
-- -----------------------------------------------------------------------
with_comparisons AS (
    SELECT
        c.week_sun_to_sat,
        c.custom_week_num,

        -- ---- Current VIS values ----
        c.profoundVis_tmo_nonbrand_visibility_score,
        c.profoundVis_tmo_nonbrand_executions,
        c.profoundVis_tmo_nonbrand_mentions_count,
        c.profoundVis_tmo_nonbrand_share_of_voice,
        c.profoundVis_verizon_nonbrand_visibility_score,
        c.profoundVis_verizon_nonbrand_executions,
        c.profoundVis_verizon_nonbrand_mentions_count,
        c.profoundVis_verizon_nonbrand_share_of_voice,
        c.profoundVis_att_nonbrand_visibility_score,
        c.profoundVis_att_nonbrand_executions,
        c.profoundVis_att_nonbrand_mentions_count,
        c.profoundVis_att_nonbrand_share_of_voice,

        -- ---- Current CIT values ----
        c.profoundCit_tmo_nonbrand_share_of_voice,
        c.profoundCit_verizon_nonbrand_share_of_voice,
        c.profoundCit_att_nonbrand_share_of_voice,

        -- ---- WoW VIS values ----
        w.profoundVis_tmo_nonbrand_visibility_score     AS profoundVis_tmo_nonbrand_visibility_score_wow,
        w.profoundVis_tmo_nonbrand_executions           AS profoundVis_tmo_nonbrand_executions_wow,
        w.profoundVis_tmo_nonbrand_mentions_count       AS profoundVis_tmo_nonbrand_mentions_count_wow,
        w.profoundVis_tmo_nonbrand_share_of_voice       AS profoundVis_tmo_nonbrand_share_of_voice_wow,
        w.profoundVis_verizon_nonbrand_visibility_score AS profoundVis_verizon_nonbrand_visibility_score_wow,
        w.profoundVis_verizon_nonbrand_executions       AS profoundVis_verizon_nonbrand_executions_wow,
        w.profoundVis_verizon_nonbrand_mentions_count   AS profoundVis_verizon_nonbrand_mentions_count_wow,
        w.profoundVis_verizon_nonbrand_share_of_voice   AS profoundVis_verizon_nonbrand_share_of_voice_wow,
        w.profoundVis_att_nonbrand_visibility_score     AS profoundVis_att_nonbrand_visibility_score_wow,
        w.profoundVis_att_nonbrand_executions           AS profoundVis_att_nonbrand_executions_wow,
        w.profoundVis_att_nonbrand_mentions_count       AS profoundVis_att_nonbrand_mentions_count_wow,
        w.profoundVis_att_nonbrand_share_of_voice       AS profoundVis_att_nonbrand_share_of_voice_wow,

        -- ---- WoW CIT values ----
        w.profoundCit_tmo_nonbrand_share_of_voice                       AS profoundCit_tmo_nonbrand_share_of_voice_wow,
        w.profoundCit_verizon_nonbrand_share_of_voice                   AS profoundCit_verizon_nonbrand_share_of_voice_wow,
        w.profoundCit_att_nonbrand_share_of_voice                       AS profoundCit_att_nonbrand_share_of_voice_wow,

        -- ---- LY VIS values ----
        l.profoundVis_tmo_nonbrand_visibility_score     AS profoundVis_tmo_nonbrand_visibility_score_ly,
        l.profoundVis_tmo_nonbrand_executions           AS profoundVis_tmo_nonbrand_executions_ly,
        l.profoundVis_tmo_nonbrand_mentions_count       AS profoundVis_tmo_nonbrand_mentions_count_ly,
        l.profoundVis_tmo_nonbrand_share_of_voice       AS profoundVis_tmo_nonbrand_share_of_voice_ly,
        l.profoundVis_verizon_nonbrand_visibility_score AS profoundVis_verizon_nonbrand_visibility_score_ly,
        l.profoundVis_verizon_nonbrand_executions       AS profoundVis_verizon_nonbrand_executions_ly,
        l.profoundVis_verizon_nonbrand_mentions_count   AS profoundVis_verizon_nonbrand_mentions_count_ly,
        l.profoundVis_verizon_nonbrand_share_of_voice   AS profoundVis_verizon_nonbrand_share_of_voice_ly,
        l.profoundVis_att_nonbrand_visibility_score     AS profoundVis_att_nonbrand_visibility_score_ly,
        l.profoundVis_att_nonbrand_executions           AS profoundVis_att_nonbrand_executions_ly,
        l.profoundVis_att_nonbrand_mentions_count       AS profoundVis_att_nonbrand_mentions_count_ly,
        l.profoundVis_att_nonbrand_share_of_voice       AS profoundVis_att_nonbrand_share_of_voice_ly,

        -- ---- LY CIT values ----
        l.profoundCit_tmo_nonbrand_share_of_voice                       AS profoundCit_tmo_nonbrand_share_of_voice_ly,
        l.profoundCit_verizon_nonbrand_share_of_voice                   AS profoundCit_verizon_nonbrand_share_of_voice_ly,
        l.profoundCit_att_nonbrand_share_of_voice                       AS profoundCit_att_nonbrand_share_of_voice_ly

    FROM with_week_num c
    LEFT JOIN with_week_num w
      ON c.week_sun_to_sat = DATE_ADD(w.week_sun_to_sat, INTERVAL 7 DAY)
    LEFT JOIN with_week_num l
      ON (c.custom_week_num - l.custom_week_num) = 52
),

-- -----------------------------------------------------------------------
-- STEP 7: Compute wow_pct and yoy_pct for all metrics
-- NULL when prior value is NULL or 0 — no fake zeroes
-- -----------------------------------------------------------------------
with_pcts AS (
    SELECT
        week_sun_to_sat,
        custom_week_num,

        -- ---- T-Mobile visibility_score ----
        profoundVis_tmo_nonbrand_visibility_score,
        profoundVis_tmo_nonbrand_visibility_score_wow,
        profoundVis_tmo_nonbrand_visibility_score_ly,
        CASE WHEN profoundVis_tmo_nonbrand_visibility_score_wow IS NULL OR profoundVis_tmo_nonbrand_visibility_score_wow = 0 THEN NULL
             ELSE ROUND((profoundVis_tmo_nonbrand_visibility_score - profoundVis_tmo_nonbrand_visibility_score_wow) / profoundVis_tmo_nonbrand_visibility_score_wow, 6) END AS profoundVis_tmo_nonbrand_visibility_score_wow_pct,
        CASE WHEN profoundVis_tmo_nonbrand_visibility_score_ly  IS NULL OR profoundVis_tmo_nonbrand_visibility_score_ly  = 0 THEN NULL
             ELSE ROUND((profoundVis_tmo_nonbrand_visibility_score - profoundVis_tmo_nonbrand_visibility_score_ly)  / profoundVis_tmo_nonbrand_visibility_score_ly,  6) END AS profoundVis_tmo_nonbrand_visibility_score_yoy_pct,

        -- ---- T-Mobile executions ----
        profoundVis_tmo_nonbrand_executions,
        profoundVis_tmo_nonbrand_executions_wow,
        profoundVis_tmo_nonbrand_executions_ly,
        CASE WHEN profoundVis_tmo_nonbrand_executions_wow IS NULL OR profoundVis_tmo_nonbrand_executions_wow = 0 THEN NULL
             ELSE ROUND((profoundVis_tmo_nonbrand_executions - profoundVis_tmo_nonbrand_executions_wow) / profoundVis_tmo_nonbrand_executions_wow, 6) END AS profoundVis_tmo_nonbrand_executions_wow_pct,
        CASE WHEN profoundVis_tmo_nonbrand_executions_ly  IS NULL OR profoundVis_tmo_nonbrand_executions_ly  = 0 THEN NULL
             ELSE ROUND((profoundVis_tmo_nonbrand_executions - profoundVis_tmo_nonbrand_executions_ly)  / profoundVis_tmo_nonbrand_executions_ly,  6) END AS profoundVis_tmo_nonbrand_executions_yoy_pct,

        -- ---- T-Mobile mentions_count ----
        profoundVis_tmo_nonbrand_mentions_count,
        profoundVis_tmo_nonbrand_mentions_count_wow,
        profoundVis_tmo_nonbrand_mentions_count_ly,
        CASE WHEN profoundVis_tmo_nonbrand_mentions_count_wow IS NULL OR profoundVis_tmo_nonbrand_mentions_count_wow = 0 THEN NULL
             ELSE ROUND((profoundVis_tmo_nonbrand_mentions_count - profoundVis_tmo_nonbrand_mentions_count_wow) / profoundVis_tmo_nonbrand_mentions_count_wow, 6) END AS profoundVis_tmo_nonbrand_mentions_count_wow_pct,
        CASE WHEN profoundVis_tmo_nonbrand_mentions_count_ly  IS NULL OR profoundVis_tmo_nonbrand_mentions_count_ly  = 0 THEN NULL
             ELSE ROUND((profoundVis_tmo_nonbrand_mentions_count - profoundVis_tmo_nonbrand_mentions_count_ly)  / profoundVis_tmo_nonbrand_mentions_count_ly,  6) END AS profoundVis_tmo_nonbrand_mentions_count_yoy_pct,

        -- ---- T-Mobile share_of_voice ----
        profoundVis_tmo_nonbrand_share_of_voice,
        profoundVis_tmo_nonbrand_share_of_voice_wow,
        profoundVis_tmo_nonbrand_share_of_voice_ly,
        CASE WHEN profoundVis_tmo_nonbrand_share_of_voice_wow IS NULL OR profoundVis_tmo_nonbrand_share_of_voice_wow = 0 THEN NULL
             ELSE ROUND((profoundVis_tmo_nonbrand_share_of_voice - profoundVis_tmo_nonbrand_share_of_voice_wow) / profoundVis_tmo_nonbrand_share_of_voice_wow, 6) END AS profoundVis_tmo_nonbrand_share_of_voice_wow_pct,
        CASE WHEN profoundVis_tmo_nonbrand_share_of_voice_ly  IS NULL OR profoundVis_tmo_nonbrand_share_of_voice_ly  = 0 THEN NULL
             ELSE ROUND((profoundVis_tmo_nonbrand_share_of_voice - profoundVis_tmo_nonbrand_share_of_voice_ly)  / profoundVis_tmo_nonbrand_share_of_voice_ly,  6) END AS profoundVis_tmo_nonbrand_share_of_voice_yoy_pct,

        -- ---- T-Mobile cit_share_of_voice ----
        profoundCit_tmo_nonbrand_share_of_voice,
        profoundCit_tmo_nonbrand_share_of_voice_wow,
        profoundCit_tmo_nonbrand_share_of_voice_ly,
        CASE WHEN profoundCit_tmo_nonbrand_share_of_voice_wow IS NULL OR profoundCit_tmo_nonbrand_share_of_voice_wow = 0 THEN NULL
             ELSE ROUND((profoundCit_tmo_nonbrand_share_of_voice - profoundCit_tmo_nonbrand_share_of_voice_wow) / profoundCit_tmo_nonbrand_share_of_voice_wow, 6) END AS profoundCit_tmo_nonbrand_share_of_voice_wow_pct,
        CASE WHEN profoundCit_tmo_nonbrand_share_of_voice_ly  IS NULL OR profoundCit_tmo_nonbrand_share_of_voice_ly  = 0 THEN NULL
             ELSE ROUND((profoundCit_tmo_nonbrand_share_of_voice - profoundCit_tmo_nonbrand_share_of_voice_ly)  / profoundCit_tmo_nonbrand_share_of_voice_ly,  6) END AS profoundCit_tmo_nonbrand_share_of_voice_yoy_pct,

        -- ---- Verizon visibility_score ----
        profoundVis_verizon_nonbrand_visibility_score,
        profoundVis_verizon_nonbrand_visibility_score_wow,
        profoundVis_verizon_nonbrand_visibility_score_ly,
        CASE WHEN profoundVis_verizon_nonbrand_visibility_score_wow IS NULL OR profoundVis_verizon_nonbrand_visibility_score_wow = 0 THEN NULL
             ELSE ROUND((profoundVis_verizon_nonbrand_visibility_score - profoundVis_verizon_nonbrand_visibility_score_wow) / profoundVis_verizon_nonbrand_visibility_score_wow, 6) END AS profoundVis_verizon_nonbrand_visibility_score_wow_pct,
        CASE WHEN profoundVis_verizon_nonbrand_visibility_score_ly  IS NULL OR profoundVis_verizon_nonbrand_visibility_score_ly  = 0 THEN NULL
             ELSE ROUND((profoundVis_verizon_nonbrand_visibility_score - profoundVis_verizon_nonbrand_visibility_score_ly)  / profoundVis_verizon_nonbrand_visibility_score_ly,  6) END AS profoundVis_verizon_nonbrand_visibility_score_yoy_pct,

        -- ---- Verizon executions ----
        profoundVis_verizon_nonbrand_executions,
        profoundVis_verizon_nonbrand_executions_wow,
        profoundVis_verizon_nonbrand_executions_ly,
        CASE WHEN profoundVis_verizon_nonbrand_executions_wow IS NULL OR profoundVis_verizon_nonbrand_executions_wow = 0 THEN NULL
             ELSE ROUND((profoundVis_verizon_nonbrand_executions - profoundVis_verizon_nonbrand_executions_wow) / profoundVis_verizon_nonbrand_executions_wow, 6) END AS profoundVis_verizon_nonbrand_executions_wow_pct,
        CASE WHEN profoundVis_verizon_nonbrand_executions_ly  IS NULL OR profoundVis_verizon_nonbrand_executions_ly  = 0 THEN NULL
             ELSE ROUND((profoundVis_verizon_nonbrand_executions - profoundVis_verizon_nonbrand_executions_ly)  / profoundVis_verizon_nonbrand_executions_ly,  6) END AS profoundVis_verizon_nonbrand_executions_yoy_pct,

        -- ---- Verizon mentions_count ----
        profoundVis_verizon_nonbrand_mentions_count,
        profoundVis_verizon_nonbrand_mentions_count_wow,
        profoundVis_verizon_nonbrand_mentions_count_ly,
        CASE WHEN profoundVis_verizon_nonbrand_mentions_count_wow IS NULL OR profoundVis_verizon_nonbrand_mentions_count_wow = 0 THEN NULL
             ELSE ROUND((profoundVis_verizon_nonbrand_mentions_count - profoundVis_verizon_nonbrand_mentions_count_wow) / profoundVis_verizon_nonbrand_mentions_count_wow, 6) END AS profoundVis_verizon_nonbrand_mentions_count_wow_pct,
        CASE WHEN profoundVis_verizon_nonbrand_mentions_count_ly  IS NULL OR profoundVis_verizon_nonbrand_mentions_count_ly  = 0 THEN NULL
             ELSE ROUND((profoundVis_verizon_nonbrand_mentions_count - profoundVis_verizon_nonbrand_mentions_count_ly)  / profoundVis_verizon_nonbrand_mentions_count_ly,  6) END AS profoundVis_verizon_nonbrand_mentions_count_yoy_pct,

        -- ---- Verizon share_of_voice ----
        profoundVis_verizon_nonbrand_share_of_voice,
        profoundVis_verizon_nonbrand_share_of_voice_wow,
        profoundVis_verizon_nonbrand_share_of_voice_ly,
        CASE WHEN profoundVis_verizon_nonbrand_share_of_voice_wow IS NULL OR profoundVis_verizon_nonbrand_share_of_voice_wow = 0 THEN NULL
             ELSE ROUND((profoundVis_verizon_nonbrand_share_of_voice - profoundVis_verizon_nonbrand_share_of_voice_wow) / profoundVis_verizon_nonbrand_share_of_voice_wow, 6) END AS profoundVis_verizon_nonbrand_share_of_voice_wow_pct,
        CASE WHEN profoundVis_verizon_nonbrand_share_of_voice_ly  IS NULL OR profoundVis_verizon_nonbrand_share_of_voice_ly  = 0 THEN NULL
             ELSE ROUND((profoundVis_verizon_nonbrand_share_of_voice - profoundVis_verizon_nonbrand_share_of_voice_ly)  / profoundVis_verizon_nonbrand_share_of_voice_ly,  6) END AS profoundVis_verizon_nonbrand_share_of_voice_yoy_pct,

        -- ---- Verizon cit_share_of_voice ----
        profoundCit_verizon_nonbrand_share_of_voice,
        profoundCit_verizon_nonbrand_share_of_voice_wow,
        profoundCit_verizon_nonbrand_share_of_voice_ly,
        CASE WHEN profoundCit_verizon_nonbrand_share_of_voice_wow IS NULL OR profoundCit_verizon_nonbrand_share_of_voice_wow = 0 THEN NULL
             ELSE ROUND((profoundCit_verizon_nonbrand_share_of_voice - profoundCit_verizon_nonbrand_share_of_voice_wow) / profoundCit_verizon_nonbrand_share_of_voice_wow, 6) END AS profoundCit_verizon_nonbrand_share_of_voice_wow_pct,
        CASE WHEN profoundCit_verizon_nonbrand_share_of_voice_ly  IS NULL OR profoundCit_verizon_nonbrand_share_of_voice_ly  = 0 THEN NULL
             ELSE ROUND((profoundCit_verizon_nonbrand_share_of_voice - profoundCit_verizon_nonbrand_share_of_voice_ly)  / profoundCit_verizon_nonbrand_share_of_voice_ly,  6) END AS profoundCit_verizon_nonbrand_share_of_voice_yoy_pct,

        -- ---- AT&T visibility_score ----
        profoundVis_att_nonbrand_visibility_score,
        profoundVis_att_nonbrand_visibility_score_wow,
        profoundVis_att_nonbrand_visibility_score_ly,
        CASE WHEN profoundVis_att_nonbrand_visibility_score_wow IS NULL OR profoundVis_att_nonbrand_visibility_score_wow = 0 THEN NULL
             ELSE ROUND((profoundVis_att_nonbrand_visibility_score - profoundVis_att_nonbrand_visibility_score_wow) / profoundVis_att_nonbrand_visibility_score_wow, 6) END AS profoundVis_att_nonbrand_visibility_score_wow_pct,
        CASE WHEN profoundVis_att_nonbrand_visibility_score_ly  IS NULL OR profoundVis_att_nonbrand_visibility_score_ly  = 0 THEN NULL
             ELSE ROUND((profoundVis_att_nonbrand_visibility_score - profoundVis_att_nonbrand_visibility_score_ly)  / profoundVis_att_nonbrand_visibility_score_ly,  6) END AS profoundVis_att_nonbrand_visibility_score_yoy_pct,

        -- ---- AT&T executions ----
        profoundVis_att_nonbrand_executions,
        profoundVis_att_nonbrand_executions_wow,
        profoundVis_att_nonbrand_executions_ly,
        CASE WHEN profoundVis_att_nonbrand_executions_wow IS NULL OR profoundVis_att_nonbrand_executions_wow = 0 THEN NULL
             ELSE ROUND((profoundVis_att_nonbrand_executions - profoundVis_att_nonbrand_executions_wow) / profoundVis_att_nonbrand_executions_wow, 6) END AS profoundVis_att_nonbrand_executions_wow_pct,
        CASE WHEN profoundVis_att_nonbrand_executions_ly  IS NULL OR profoundVis_att_nonbrand_executions_ly  = 0 THEN NULL
             ELSE ROUND((profoundVis_att_nonbrand_executions - profoundVis_att_nonbrand_executions_ly)  / profoundVis_att_nonbrand_executions_ly,  6) END AS profoundVis_att_nonbrand_executions_yoy_pct,

        -- ---- AT&T mentions_count ----
        profoundVis_att_nonbrand_mentions_count,
        profoundVis_att_nonbrand_mentions_count_wow,
        profoundVis_att_nonbrand_mentions_count_ly,
        CASE WHEN profoundVis_att_nonbrand_mentions_count_wow IS NULL OR profoundVis_att_nonbrand_mentions_count_wow = 0 THEN NULL
             ELSE ROUND((profoundVis_att_nonbrand_mentions_count - profoundVis_att_nonbrand_mentions_count_wow) / profoundVis_att_nonbrand_mentions_count_wow, 6) END AS profoundVis_att_nonbrand_mentions_count_wow_pct,
        CASE WHEN profoundVis_att_nonbrand_mentions_count_ly  IS NULL OR profoundVis_att_nonbrand_mentions_count_ly  = 0 THEN NULL
             ELSE ROUND((profoundVis_att_nonbrand_mentions_count - profoundVis_att_nonbrand_mentions_count_ly)  / profoundVis_att_nonbrand_mentions_count_ly,  6) END AS profoundVis_att_nonbrand_mentions_count_yoy_pct,

        -- ---- AT&T share_of_voice ----
        profoundVis_att_nonbrand_share_of_voice,
        profoundVis_att_nonbrand_share_of_voice_wow,
        profoundVis_att_nonbrand_share_of_voice_ly,
        CASE WHEN profoundVis_att_nonbrand_share_of_voice_wow IS NULL OR profoundVis_att_nonbrand_share_of_voice_wow = 0 THEN NULL
             ELSE ROUND((profoundVis_att_nonbrand_share_of_voice - profoundVis_att_nonbrand_share_of_voice_wow) / profoundVis_att_nonbrand_share_of_voice_wow, 6) END AS profoundVis_att_nonbrand_share_of_voice_wow_pct,
        CASE WHEN profoundVis_att_nonbrand_share_of_voice_ly  IS NULL OR profoundVis_att_nonbrand_share_of_voice_ly  = 0 THEN NULL
             ELSE ROUND((profoundVis_att_nonbrand_share_of_voice - profoundVis_att_nonbrand_share_of_voice_ly)  / profoundVis_att_nonbrand_share_of_voice_ly,  6) END AS profoundVis_att_nonbrand_share_of_voice_yoy_pct,

        -- ---- AT&T cit_share_of_voice ----
        profoundCit_att_nonbrand_share_of_voice,
        profoundCit_att_nonbrand_share_of_voice_wow,
        profoundCit_att_nonbrand_share_of_voice_ly,
        CASE WHEN profoundCit_att_nonbrand_share_of_voice_wow IS NULL OR profoundCit_att_nonbrand_share_of_voice_wow = 0 THEN NULL
             ELSE ROUND((profoundCit_att_nonbrand_share_of_voice - profoundCit_att_nonbrand_share_of_voice_wow) / profoundCit_att_nonbrand_share_of_voice_wow, 6) END AS profoundCit_att_nonbrand_share_of_voice_wow_pct,
        CASE WHEN profoundCit_att_nonbrand_share_of_voice_ly  IS NULL OR profoundCit_att_nonbrand_share_of_voice_ly  = 0 THEN NULL
             ELSE ROUND((profoundCit_att_nonbrand_share_of_voice - profoundCit_att_nonbrand_share_of_voice_ly)  / profoundCit_att_nonbrand_share_of_voice_ly,  6) END AS profoundCit_att_nonbrand_share_of_voice_yoy_pct

    FROM with_comparisons
),

-- -----------------------------------------------------------------------
-- STEP 8: max_data_date per source
-- Latest week_sun_to_sat where any metric (VIS or CIT) is non-null
-- -----------------------------------------------------------------------
with_max_date AS (
    SELECT
        *,
        MAX(CASE
            WHEN profoundVis_tmo_nonbrand_visibility_score     IS NOT NULL
              OR profoundVis_verizon_nonbrand_visibility_score IS NOT NULL
              OR profoundVis_att_nonbrand_visibility_score     IS NOT NULL
              OR profoundCit_tmo_nonbrand_share_of_voice                       IS NOT NULL
              OR profoundCit_verizon_nonbrand_share_of_voice                   IS NOT NULL
              OR profoundCit_att_nonbrand_share_of_voice                       IS NOT NULL
            THEN week_sun_to_sat
        END) OVER ()                                AS max_data_date
    FROM with_pcts
)

-- -----------------------------------------------------------------------
-- FINAL OUTPUT
-- Wide table — one row per week_sun_to_sat
-- All columns prefixed with 'profound_'
-- VIS metrics: profound_{asset}_nonbrand_{metric}
-- CIT metrics: profoundCit_{asset}_nonbrand_share_of_voice
-- -----------------------------------------------------------------------
SELECT
    week_sun_to_sat,
    'PROFOUND'                                      AS data_source,
    'AI SEARCH'                                     AS channel,
    max_data_date,

    -- ---- T-Mobile VIS ----
    profoundVis_tmo_nonbrand_visibility_score,
    profoundVis_tmo_nonbrand_visibility_score_wow,
    profoundVis_tmo_nonbrand_visibility_score_ly,
    profoundVis_tmo_nonbrand_visibility_score_wow_pct,
    profoundVis_tmo_nonbrand_visibility_score_yoy_pct,

    profoundVis_tmo_nonbrand_executions,
    profoundVis_tmo_nonbrand_executions_wow,
    profoundVis_tmo_nonbrand_executions_ly,
    profoundVis_tmo_nonbrand_executions_wow_pct,
    profoundVis_tmo_nonbrand_executions_yoy_pct,

    profoundVis_tmo_nonbrand_mentions_count,
    profoundVis_tmo_nonbrand_mentions_count_wow,
    profoundVis_tmo_nonbrand_mentions_count_ly,
    profoundVis_tmo_nonbrand_mentions_count_wow_pct,
    profoundVis_tmo_nonbrand_mentions_count_yoy_pct,

    profoundVis_tmo_nonbrand_share_of_voice,
    profoundVis_tmo_nonbrand_share_of_voice_wow,
    profoundVis_tmo_nonbrand_share_of_voice_ly,
    profoundVis_tmo_nonbrand_share_of_voice_wow_pct,
    profoundVis_tmo_nonbrand_share_of_voice_yoy_pct,

    -- ---- T-Mobile CIT ----
    profoundCit_tmo_nonbrand_share_of_voice,
    profoundCit_tmo_nonbrand_share_of_voice_wow,
    profoundCit_tmo_nonbrand_share_of_voice_ly,
    profoundCit_tmo_nonbrand_share_of_voice_wow_pct,
    profoundCit_tmo_nonbrand_share_of_voice_yoy_pct,

    -- ---- Verizon VIS ----
    profoundVis_verizon_nonbrand_visibility_score,
    profoundVis_verizon_nonbrand_visibility_score_wow,
    profoundVis_verizon_nonbrand_visibility_score_ly,
    profoundVis_verizon_nonbrand_visibility_score_wow_pct,
    profoundVis_verizon_nonbrand_visibility_score_yoy_pct,

    profoundVis_verizon_nonbrand_executions,
    profoundVis_verizon_nonbrand_executions_wow,
    profoundVis_verizon_nonbrand_executions_ly,
    profoundVis_verizon_nonbrand_executions_wow_pct,
    profoundVis_verizon_nonbrand_executions_yoy_pct,

    profoundVis_verizon_nonbrand_mentions_count,
    profoundVis_verizon_nonbrand_mentions_count_wow,
    profoundVis_verizon_nonbrand_mentions_count_ly,
    profoundVis_verizon_nonbrand_mentions_count_wow_pct,
    profoundVis_verizon_nonbrand_mentions_count_yoy_pct,

    profoundVis_verizon_nonbrand_share_of_voice,
    profoundVis_verizon_nonbrand_share_of_voice_wow,
    profoundVis_verizon_nonbrand_share_of_voice_ly,
    profoundVis_verizon_nonbrand_share_of_voice_wow_pct,
    profoundVis_verizon_nonbrand_share_of_voice_yoy_pct,

    -- ---- Verizon CIT ----
    profoundCit_verizon_nonbrand_share_of_voice,
    profoundCit_verizon_nonbrand_share_of_voice_wow,
    profoundCit_verizon_nonbrand_share_of_voice_ly,
    profoundCit_verizon_nonbrand_share_of_voice_wow_pct,
    profoundCit_verizon_nonbrand_share_of_voice_yoy_pct,

    -- ---- AT&T VIS ----
    profoundVis_att_nonbrand_visibility_score,
    profoundVis_att_nonbrand_visibility_score_wow,
    profoundVis_att_nonbrand_visibility_score_ly,
    profoundVis_att_nonbrand_visibility_score_wow_pct,
    profoundVis_att_nonbrand_visibility_score_yoy_pct,

    profoundVis_att_nonbrand_executions,
    profoundVis_att_nonbrand_executions_wow,
    profoundVis_att_nonbrand_executions_ly,
    profoundVis_att_nonbrand_executions_wow_pct,
    profoundVis_att_nonbrand_executions_yoy_pct,

    profoundVis_att_nonbrand_mentions_count,
    profoundVis_att_nonbrand_mentions_count_wow,
    profoundVis_att_nonbrand_mentions_count_ly,
    profoundVis_att_nonbrand_mentions_count_wow_pct,
    profoundVis_att_nonbrand_mentions_count_yoy_pct,

    profoundVis_att_nonbrand_share_of_voice,
    profoundVis_att_nonbrand_share_of_voice_wow,
    profoundVis_att_nonbrand_share_of_voice_ly,
    profoundVis_att_nonbrand_share_of_voice_wow_pct,
    profoundVis_att_nonbrand_share_of_voice_yoy_pct,

    -- ---- AT&T CIT ----
    profoundCit_att_nonbrand_share_of_voice,
    profoundCit_att_nonbrand_share_of_voice_wow,
    profoundCit_att_nonbrand_share_of_voice_ly,
    profoundCit_att_nonbrand_share_of_voice_wow_pct,
    profoundCit_att_nonbrand_share_of_voice_yoy_pct

FROM with_max_date
;