/* =================================================================================================
FILE:         03_vw_sdi_pulseTms_silver_googleTrends_weekly.sql
LAYER:        Silver View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseTms_silver_googleTrends_weekly

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_bronze_googleTrends_weekly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_googleTrends_weekly

PURPOSE:
  Silver view for Google Trends BYOD weekly search interest data.
  Identical logic to vw_sdi_pulseByod_silver_googleTrends_weekly.
  Renamed pulseTms to support the full TMS pipeline.
  One row per week_sun_to_sat. WoW/LY for byod_index only.
  Keywords kept wide (top_kw_1 through top_kw_5) — unpivoted in Gold Long.

BUSINESS GRAIN: One row per week_sun_to_sat
DOWNSTREAM:
  Gold Wide : vw_sdi_pulseTms_gold_unified_wide
  Gold Long : vw_sdi_pulseTms_gold_unified_long
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_googleTrends_weekly`
AS

WITH base AS (
    SELECT
        DATE_ADD(event_date_sun, INTERVAL 6 DAY)    AS week_sun_to_sat,
        byod_index                                  AS trends_byod_index,
        top_kw_1 AS trends_top_kw_1, kw1_interest AS trends_kw1_interest, kw1_change AS trends_kw1_change,
        top_kw_2 AS trends_top_kw_2, kw2_interest AS trends_kw2_interest, kw2_change AS trends_kw2_change,
        top_kw_3 AS trends_top_kw_3, kw3_interest AS trends_kw3_interest, kw3_change AS trends_kw3_change,
        top_kw_4 AS trends_top_kw_4, kw4_interest AS trends_kw4_interest, kw4_change AS trends_kw4_change,
        top_kw_5 AS trends_top_kw_5, kw5_interest AS trends_kw5_interest, kw5_change AS trends_kw5_change
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_bronze_googleTrends_weekly`
),

with_week_num AS (
    SELECT *, DATE_DIFF(DATE_SUB(week_sun_to_sat, INTERVAL 6 DAY), DATE '2023-01-01', WEEK) AS custom_week_num
    FROM base
),

with_comparisons AS (
    SELECT
        c.week_sun_to_sat, c.custom_week_num,
        c.trends_byod_index,
        c.trends_top_kw_1, c.trends_kw1_interest, c.trends_kw1_change,
        c.trends_top_kw_2, c.trends_kw2_interest, c.trends_kw2_change,
        c.trends_top_kw_3, c.trends_kw3_interest, c.trends_kw3_change,
        c.trends_top_kw_4, c.trends_kw4_interest, c.trends_kw4_change,
        c.trends_top_kw_5, c.trends_kw5_interest, c.trends_kw5_change,
        w.trends_byod_index AS trends_byod_index_wow,
        l.trends_byod_index AS trends_byod_index_ly
    FROM with_week_num c
    LEFT JOIN with_week_num w ON c.week_sun_to_sat = DATE_ADD(w.week_sun_to_sat, INTERVAL 7 DAY)
    LEFT JOIN with_week_num l ON (c.custom_week_num - l.custom_week_num) = 52
),

with_pcts AS (
    SELECT
        week_sun_to_sat, custom_week_num,
        trends_byod_index, trends_byod_index_wow, trends_byod_index_ly,
        CASE WHEN trends_byod_index_wow IS NULL OR trends_byod_index_wow = 0 THEN NULL
             ELSE ROUND((trends_byod_index - trends_byod_index_wow) / trends_byod_index_wow, 6) END AS trends_byod_index_wow_pct,
        CASE WHEN trends_byod_index_ly  IS NULL OR trends_byod_index_ly  = 0 THEN NULL
             ELSE ROUND((trends_byod_index - trends_byod_index_ly)  / trends_byod_index_ly,  6) END AS trends_byod_index_yoy_pct,
        trends_top_kw_1, trends_kw1_interest, trends_kw1_change,
        trends_top_kw_2, trends_kw2_interest, trends_kw2_change,
        trends_top_kw_3, trends_kw3_interest, trends_kw3_change,
        trends_top_kw_4, trends_kw4_interest, trends_kw4_change,
        trends_top_kw_5, trends_kw5_interest, trends_kw5_change
    FROM with_comparisons
),

with_max_date AS (
    SELECT *,
        MAX(CASE WHEN trends_byod_index IS NOT NULL THEN week_sun_to_sat END) OVER () AS max_data_date
    FROM with_pcts
)

SELECT
    week_sun_to_sat, 'TRENDS' AS data_source, 'ORGANIC SEARCH' AS channel, max_data_date,
    trends_byod_index, trends_byod_index_wow, trends_byod_index_ly,
    trends_byod_index_wow_pct, trends_byod_index_yoy_pct,
    trends_top_kw_1, trends_kw1_interest, trends_kw1_change,
    trends_top_kw_2, trends_kw2_interest, trends_kw2_change,
    trends_top_kw_3, trends_kw3_interest, trends_kw3_change,
    trends_top_kw_4, trends_kw4_interest, trends_kw4_change,
    trends_top_kw_5, trends_kw5_interest, trends_kw5_change
FROM with_max_date
;