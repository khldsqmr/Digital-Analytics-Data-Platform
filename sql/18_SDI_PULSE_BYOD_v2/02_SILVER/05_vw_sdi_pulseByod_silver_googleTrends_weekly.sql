/* =================================================================================================
FILE:         05_vw_sdi_pulseByod_silver_googleTrends_weekly.sql
LAYER:        Silver View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseByod_silver_googleTrends_weekly

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_bronze_googleTrends_weekly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly

PURPOSE:
  Silver view for Google Trends BYOD weekly search interest data.
  Outputs a WIDE table — one row per week_sun_to_sat.
  All metric columns prefixed with 'trends_' for unambiguous
  identification in Gold Wide spine join and Gold Long unpivot.
  Applies week-end Saturday conversion, WoW/LY for byod_index only,
  and preserves keyword columns wide (top_kw_1 through top_kw_5).
  Keywords are NOT unpivoted here — unpivoting happens in Gold Long.

BUSINESS GRAIN:
  One row per:
    week_sun_to_sat

BUSINESS LOGIC APPLIED:
  - data_source = 'TRENDS'
  - channel     = 'ORGANIC SEARCH'
  - week_sun_to_sat = DATE_ADD(event_date_sun, INTERVAL 6 DAY)
  - byod_index: WoW and LY computed (stable metric)
  - Keywords: kept wide as trends_top_kw_1 through trends_top_kw_5
    with trends_kw{n}_interest and trends_kw{n}_change
    No WoW/LY for keywords — they change week to week
  - max_data_date per source

COLUMN NAMING CONVENTION:
  trends_byod_index
  trends_byod_index_wow
  trends_byod_index_ly
  trends_byod_index_wow_pct
  trends_byod_index_yoy_pct
  trends_top_kw_{1-5}
  trends_kw{n}_interest
  trends_kw{n}_change

KEY MODELING NOTES:
  - byod_index WoW/LY self-join on 1-row-per-week CTE — essentially free
  - Keyword columns kept wide — unpivot in Gold Long for Top N visualization
  - Keywords only populated from 2026-05-09 onward (pipeline backfill issue)
    NULL/empty values preserved as-is — not a Silver concern
  - NULLs preserved — no fake zeroes
  - No ORDER BY — applied in Gold only

DOWNSTREAM:
  Gold Wide : vw_sdi_pulseByod_gold_unified_wide
  Gold Long : vw_sdi_pulseByod_gold_unified_long
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
AS

-- -----------------------------------------------------------------------
-- STEP 1: Convert dates, prefix all columns with 'trends_'
-- -----------------------------------------------------------------------
WITH base AS (
    SELECT
        DATE_ADD(event_date_sun, INTERVAL 6 DAY)    AS week_sun_to_sat,
        byod_index                                  AS trends_byod_index,
        top_kw_1                                    AS trends_top_kw_1,
        kw1_interest                                AS trends_kw1_interest,
        kw1_change                                  AS trends_kw1_change,
        top_kw_2                                    AS trends_top_kw_2,
        kw2_interest                                AS trends_kw2_interest,
        kw2_change                                  AS trends_kw2_change,
        top_kw_3                                    AS trends_top_kw_3,
        kw3_interest                                AS trends_kw3_interest,
        kw3_change                                  AS trends_kw3_change,
        top_kw_4                                    AS trends_top_kw_4,
        kw4_interest                                AS trends_kw4_interest,
        kw4_change                                  AS trends_kw4_change,
        top_kw_5                                    AS trends_top_kw_5,
        kw5_interest                                AS trends_kw5_interest,
        kw5_change                                  AS trends_kw5_change
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_bronze_googleTrends_weekly`
),

-- -----------------------------------------------------------------------
-- STEP 2: Add custom week number for gap-safe LY matching
-- -----------------------------------------------------------------------
with_week_num AS (
    SELECT *,
        DATE_DIFF(DATE_SUB(week_sun_to_sat, INTERVAL 6 DAY), DATE '2023-01-01', WEEK) AS custom_week_num
    FROM base
),

-- -----------------------------------------------------------------------
-- STEP 3: WoW and LY self-joins for byod_index only
-- Joins on 1-row-per-week CTE — essentially free
-- Keywords excluded — they change week to week
-- -----------------------------------------------------------------------
with_comparisons AS (
    SELECT
        c.week_sun_to_sat,
        c.custom_week_num,
        c.trends_byod_index,
        c.trends_top_kw_1, c.trends_kw1_interest, c.trends_kw1_change,
        c.trends_top_kw_2, c.trends_kw2_interest, c.trends_kw2_change,
        c.trends_top_kw_3, c.trends_kw3_interest, c.trends_kw3_change,
        c.trends_top_kw_4, c.trends_kw4_interest, c.trends_kw4_change,
        c.trends_top_kw_5, c.trends_kw5_interest, c.trends_kw5_change,

        -- WoW and LY for byod_index only
        w.trends_byod_index                         AS trends_byod_index_wow,
        l.trends_byod_index                         AS trends_byod_index_ly

    FROM with_week_num c
    LEFT JOIN with_week_num w ON c.week_sun_to_sat = DATE_ADD(w.week_sun_to_sat, INTERVAL 7 DAY)
    LEFT JOIN with_week_num l ON (c.custom_week_num - l.custom_week_num) = 52
),

-- -----------------------------------------------------------------------
-- STEP 4: Compute wow_pct and yoy_pct for byod_index
-- -----------------------------------------------------------------------
with_pcts AS (
    SELECT
        week_sun_to_sat,
        custom_week_num,

        trends_byod_index,
        trends_byod_index_wow,
        trends_byod_index_ly,
        CASE WHEN trends_byod_index_wow IS NULL OR trends_byod_index_wow = 0 THEN NULL
             ELSE ROUND((trends_byod_index - trends_byod_index_wow) / trends_byod_index_wow, 6)
        END                                         AS trends_byod_index_wow_pct,
        CASE WHEN trends_byod_index_ly  IS NULL OR trends_byod_index_ly  = 0 THEN NULL
             ELSE ROUND((trends_byod_index - trends_byod_index_ly)  / trends_byod_index_ly,  6)
        END                                         AS trends_byod_index_yoy_pct,

        -- Keywords kept wide — no WoW/LY
        trends_top_kw_1, trends_kw1_interest, trends_kw1_change,
        trends_top_kw_2, trends_kw2_interest, trends_kw2_change,
        trends_top_kw_3, trends_kw3_interest, trends_kw3_change,
        trends_top_kw_4, trends_kw4_interest, trends_kw4_change,
        trends_top_kw_5, trends_kw5_interest, trends_kw5_change

    FROM with_comparisons
),

-- -----------------------------------------------------------------------
-- STEP 5: max_data_date per source
-- -----------------------------------------------------------------------
with_max_date AS (
    SELECT *,
        MAX(CASE WHEN trends_byod_index IS NOT NULL THEN week_sun_to_sat END)
            OVER ()                                 AS max_data_date
    FROM with_pcts
)

SELECT
    week_sun_to_sat,
    'TRENDS'                                        AS data_source,
    'ORGANIC SEARCH'                                AS channel,
    max_data_date,

    -- byod_index with WoW/LY
    trends_byod_index,
    trends_byod_index_wow,
    trends_byod_index_ly,
    trends_byod_index_wow_pct,
    trends_byod_index_yoy_pct,

    -- Keywords wide — unpivoted in Gold Long
    trends_top_kw_1,
    trends_kw1_interest,
    trends_kw1_change,
    trends_top_kw_2,
    trends_kw2_interest,
    trends_kw2_change,
    trends_top_kw_3,
    trends_kw3_interest,
    trends_kw3_change,
    trends_top_kw_4,
    trends_kw4_interest,
    trends_kw4_change,
    trends_top_kw_5,
    trends_kw5_interest,
    trends_kw5_change

FROM with_max_date
;