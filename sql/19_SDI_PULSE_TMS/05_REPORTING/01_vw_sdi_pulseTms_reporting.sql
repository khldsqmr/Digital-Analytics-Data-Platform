/* =================================================================================================
FILE:         01_vw_sdi_pulseTms_reporting.sql
LAYER:        Reporting View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseTms_reporting

SOURCES:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_gold_unified_long
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_date

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_reporting

PURPOSE:
  Single unified reporting layer for the pulseTms dashboard.
  Joins Gold Long (all sources including MFC) to dim_date.
  Produces two sections via section_name:

  ── section_name = 'Table' ─────────────────────────────────────────────
  Driven from the quarter spine — all weeks in any started quarter appear
  including future weeks (NULL metric values).
  Join type: dim_date LEFT JOIN Gold Long.
  Partial weight applied to metric_value_display for boundary weeks
  EXCEPT for MFC rows (is_mfc_pre_apportioned = TRUE) where values are
  already apportioned at source — used as-is, no further adjustment.

  ── section_name = 'Trend' ─────────────────────────────────────────────
  Driven from actual data — only weeks in Gold Long appear.
  No future phantom weeks on trend lines.
  Join type: Gold Long INNER JOIN dim_date.
  metric_value_display = raw metric_value (partial_weight = 1.0 always).
  MFC rows in Trend section: aggregated across tactic and agency
  (dimension_name/value_2 become NULL) — channel is the only filter.
  All other sources: one row per metric per week per channel as-is.
  Trend line stops at max_data_date per data_source.

IN-PROGRESS WEEK SUPPRESSION:
  For all non-MFC sources:
    week_sun_to_sat > max_data_date → is_data_complete = FALSE
    metric_value_display and all comparison columns suppressed to NULL
    Row still exists in Table section (column header shows, value blank)
  For MFC:
    All existing rows are actual data → is_data_complete = TRUE
    Future weeks (no Gold Long row) → is_data_complete = FALSE

PARTIAL WEIGHT LOGIC:
  Non-MFC Table rows:
    metric_value_display = ROUND(metric_value * partial_weight, 2)
    Full weeks: partial_weight = 1.0 → no change
    Boundary weeks: partial_weight = n/7 → apportioned
  MFC Table rows (is_mfc_pre_apportioned = TRUE):
    metric_value_display = metric_value (no partial_weight multiplication)
    Values already apportioned at source for BOUNDARY_WEEK rows
  All Trend rows:
    metric_value_display = metric_value (partial_weight = 1.0 always)

WoW RULES:
  All sources: wow_pct always full week vs full week, never adjusted by partial_weight
  MFC BOUNDARY_WEEK quarter_end_date rows: wow_pct = NULL (from Silver)
  In-progress weeks: wow_pct suppressed to NULL
  Trend section: wow_pct as-is from Gold Long

ADDING NEW SOURCES (Platform Spend, etc.):
  No changes needed to this view.
  New sources flow through automatically via Gold Long.

OUTPUT SCHEMA:
  week_sun_to_sat       : DATE
  display_date          : DATE    — dashboard column label
  time_granularity      : STRING  — 'WEEKLY'
  year_quarter          : STRING  — '2026-Q2'; NULL for Trend
  quarter_start         : DATE    — NULL for Trend
  quarter_end           : DATE    — NULL for Trend
  week_of_quarter       : INT64   — NULL for Trend
  week_label            : STRING  — NULL for Trend
  is_partial_week       : BOOL
  partial_weight        : FLOAT64
  days_in_quarter       : INT64
  section_name          : STRING  — 'Table' or 'Trend'
  data_source           : STRING
  channel               : STRING
  lob_supported         : STRING  — MFC only; NULL otherwise
  dimension_name        : STRING  — 'KEYWORD_RANK_1..5' / 'TACTIC' / NULL
  dimension_value       : STRING  — keyword / tactic / NULL
  dimension_name_2      : STRING  — 'AGENCY' for MFC Table; NULL otherwise
  dimension_value_2     : STRING  — agency for MFC Table; NULL otherwise
  metric_name           : STRING
  is_data_complete      : BOOL    — FALSE for future/in-progress weeks
  is_mfc_pre_apportioned: BOOL    — TRUE for MFC BOUNDARY_WEEK rows
  metric_value_display  : FLOAT64 — the single display value for dashboard
  metric_value_wow      : FLOAT64 — NULL for future/in-progress
  metric_value_ly       : FLOAT64 — NULL for future/in-progress
  wow_pct               : FLOAT64 — NULL for future/in-progress
  yoy_pct               : FLOAT64 — NULL for future/in-progress
  act_vs_fcst_pct       : FLOAT64 — MFC only; NULL otherwise
  act_vs_fcst_delta     : FLOAT64 — MFC only; NULL otherwise
  max_data_date         : DATE

DASHBOARD QUERY PATTERNS:

  -- Trend chart dual axis (all sources, channel filter only)
  SELECT display_date, data_source, metric_name, metric_value_display, wow_pct
  FROM vw_sdi_pulseTms_reporting
  WHERE section_name = 'Trend'
  AND   channel      = 'PAID SEARCH'
  AND   metric_name IN ('adobe_uvnbByod_allChannels', 'mfc_spend_display')
  ORDER BY display_date

  -- Non-MFC table (e.g. Adobe UVNB for Q2)
  SELECT display_date, week_label, is_data_complete, metric_value_display, wow_pct
  FROM vw_sdi_pulseTms_reporting
  WHERE section_name = 'Table'
  AND   year_quarter = '2026-Q2'
  AND   metric_name  = 'adobe_uvnbByod_allChannels'
  ORDER BY display_date

  -- MFC table filtered by channel/tactic/agency
  SELECT display_date, week_label, is_data_complete,
         metric_value_display, wow_pct, act_vs_fcst_pct
  FROM vw_sdi_pulseTms_reporting
  WHERE section_name      = 'Table'
  AND   year_quarter      = '2026-Q2'
  AND   data_source       = 'MFC'
  AND   metric_name       = 'mfc_spend_display'
  AND   channel           = 'PAID SEARCH'
  AND   dimension_value   = 'PAID SEARCH'       -- tactic filter
  AND   dimension_value_2 = 'IN-HOUSE (TMO)'    -- agency filter
  ORDER BY display_date

  -- MFC table by message type (Table 2)
  SELECT dimension_name, dimension_value, SUM(metric_value_display) AS total_spend
  FROM vw_sdi_pulseTms_reporting
  WHERE section_name      = 'Table'
  AND   year_quarter      = '2026-Q2'
  AND   data_source       = 'MFC'
  AND   metric_name       = 'mfc_spend_display'
  AND   channel           = 'PAID SEARCH'
  AND   is_data_complete  = TRUE
  GROUP BY dimension_name, dimension_value
  ORDER BY total_spend DESC

  -- QTD sum non-MFC (partial weight already in metric_value_display)
  SELECT year_quarter, SUM(metric_value_display) AS qtd_total
  FROM vw_sdi_pulseTms_reporting
  WHERE section_name     = 'Table'
  AND   year_quarter     = '2026-Q2'
  AND   metric_name      = 'adobe_uvnbByod_allChannels'
  AND   is_data_complete = TRUE
  GROUP BY year_quarter

  -- QTD sum MFC (values already apportioned, no partial_weight needed)
  SELECT year_quarter, SUM(metric_value_display) AS qtd_spend
  FROM vw_sdi_pulseTms_reporting
  WHERE section_name     = 'Table'
  AND   year_quarter     = '2026-Q2'
  AND   data_source      = 'MFC'
  AND   metric_name      = 'mfc_spend_display'
  AND   channel          = 'PAID SEARCH'
  AND   is_data_complete = TRUE
  GROUP BY year_quarter

BUSINESS GRAIN:
  Table: week_sun_to_sat × year_quarter × data_source × channel
         × lob_supported × dimension_value × dimension_value_2 × metric_name
  Trend: week_sun_to_sat × data_source × channel × metric_name
         (MFC aggregated across tactic/agency for trend)

DOWNSTREAM:
  Dashboard (Looker / Tableau / Power BI / Sigma / custom)
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_reporting`
AS

WITH

-- -----------------------------------------------------------------------
-- TABLE SECTION
-- dim_date LEFT JOIN Gold Long
-- All quarter weeks appear — future weeks have NULL metric values
-- Non-MFC: partial_weight applied to metric_value_display for boundary weeks
-- MFC:     metric_value used as-is (already apportioned, is_mfc_pre_apportioned=TRUE)
-- In-progress weeks: metric_value_display suppressed to NULL
-- -----------------------------------------------------------------------
table_section AS (
    SELECT

        -- ---- Time dimensions ----
        d.week_sun_to_sat,
        d.display_date,
        'WEEKLY'                                                        AS time_granularity,

        -- ---- Quarter dimensions ----
        d.year_quarter,
        d.quarter_start,
        d.quarter_end,
        d.week_of_quarter,
        d.week_label,

        -- ---- Partial week info ----
        d.is_partial_week,
        d.partial_weight,
        d.days_in_quarter,

        -- ---- Section ----
        d.section_name,

        -- ---- Source dimensions ----
        l.data_source,
        l.channel,
        l.lob_supported,
        l.dimension_name,
        l.dimension_value,
        l.dimension_name_2,
        l.dimension_value_2,
        l.metric_name,
        l.is_mfc_pre_apportioned,

        -- ---- Data completeness ----
        -- Future week (no Gold Long row): FALSE
        -- In-progress non-MFC week (week > max_data_date): FALSE
        -- All existing MFC rows: TRUE (MFC has no future rows in Gold Long)
        CASE
            WHEN l.week_sun_to_sat IS NULL                         THEN FALSE
            WHEN l.is_mfc_pre_apportioned IS NOT NULL              THEN TRUE  -- MFC row exists = complete
            WHEN l.week_sun_to_sat <= l.max_data_date              THEN TRUE
            ELSE FALSE
        END                                                             AS is_data_complete,

        -- ---- metric_value_display ----
        -- NULL:  future week (no Gold Long row) or in-progress non-MFC week
        -- MFC:   metric_value as-is (already apportioned at source)
        -- Other: metric_value × partial_weight
        --        full weeks: partial_weight = 1.0 → no change
        --        boundary weeks: partial_weight = n/7 → apportioned
        CASE
            WHEN l.week_sun_to_sat IS NULL
                THEN NULL                                               -- future, no data
            WHEN l.is_mfc_pre_apportioned IS NOT NULL
                -- MFC row: use as-is regardless of partial_weight
                -- is_mfc_pre_apportioned covers both TRUE (BOUNDARY_WEEK)
                -- and FALSE (NORMAL) MFC rows
                THEN ROUND(l.metric_value, 2)
            WHEN l.week_sun_to_sat > l.max_data_date
                THEN NULL                                               -- in-progress non-MFC
            ELSE
                ROUND(l.metric_value * d.partial_weight, 2)            -- complete non-MFC week
        END                                                             AS metric_value_display,

        -- ---- WoW and YoY ----
        -- NULL for future and in-progress weeks
        -- MFC: as-is from Gold Long (NULL for boundary_quarter_end rows from Silver)
        -- Others: suppressed for in-progress weeks
        CASE
            WHEN l.week_sun_to_sat IS NULL                             THEN NULL
            WHEN l.is_mfc_pre_apportioned IS NULL
             AND l.week_sun_to_sat > l.max_data_date                   THEN NULL
            ELSE l.metric_value_wow
        END                                                             AS metric_value_wow,

        CASE
            WHEN l.week_sun_to_sat IS NULL                             THEN NULL
            WHEN l.is_mfc_pre_apportioned IS NULL
             AND l.week_sun_to_sat > l.max_data_date                   THEN NULL
            ELSE l.metric_value_ly
        END                                                             AS metric_value_ly,

        CASE
            WHEN l.week_sun_to_sat IS NULL                             THEN NULL
            WHEN l.is_mfc_pre_apportioned IS NULL
             AND l.week_sun_to_sat > l.max_data_date                   THEN NULL
            ELSE l.wow_pct
        END                                                             AS wow_pct,

        CASE
            WHEN l.week_sun_to_sat IS NULL                             THEN NULL
            WHEN l.is_mfc_pre_apportioned IS NULL
             AND l.week_sun_to_sat > l.max_data_date                   THEN NULL
            ELSE l.yoy_pct
        END                                                             AS yoy_pct,

        -- ---- Act vs Fcst (MFC only) ----
        CASE
            WHEN l.week_sun_to_sat IS NULL                             THEN NULL
            WHEN l.is_mfc_pre_apportioned IS NULL
             AND l.week_sun_to_sat > l.max_data_date                   THEN NULL
            ELSE l.act_vs_fcst_pct
        END                                                             AS act_vs_fcst_pct,

        CASE
            WHEN l.week_sun_to_sat IS NULL                             THEN NULL
            WHEN l.is_mfc_pre_apportioned IS NULL
             AND l.week_sun_to_sat > l.max_data_date                   THEN NULL
            ELSE l.act_vs_fcst_delta
        END                                                             AS act_vs_fcst_delta,

        -- ---- Data freshness ----
        l.max_data_date

    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_date`              d
    -- LEFT JOIN: all quarter weeks appear even with no data
    LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_gold_unified_long` l
        ON  d.week_sun_to_sat = l.week_sun_to_sat
    WHERE d.section_name = 'Table'
),

-- -----------------------------------------------------------------------
-- TREND SECTION
-- Gold Long INNER JOIN dim_date
-- Only weeks with actual data — no future phantom rows
-- metric_value_display = raw metric_value (partial_weight = 1.0 always)
-- Non-MFC: Trend stops at max_data_date per data_source
-- MFC: aggregated across tactic and agency for trend chart
--      channel is the only filter dimension in Trend
--      dimension_name_2 and dimension_value_2 set to NULL (aggregated away)
--      metric_value_display = SUM across tactic/agency per week/channel/metric
-- -----------------------------------------------------------------------
trend_non_mfc AS (
    SELECT
        l.week_sun_to_sat,
        d.display_date,
        'WEEKLY'                                                        AS time_granularity,
        d.year_quarter,
        d.quarter_start,
        d.quarter_end,
        d.week_of_quarter,
        d.week_label,
        d.is_partial_week,
        d.partial_weight,
        d.days_in_quarter,
        d.section_name,
        l.data_source,
        l.channel,
        l.lob_supported,
        l.dimension_name,
        l.dimension_value,
        CAST(NULL AS STRING)                                            AS dimension_name_2,
        CAST(NULL AS STRING)                                            AS dimension_value_2,
        l.metric_name,
        l.is_mfc_pre_apportioned,
        TRUE                                                            AS is_data_complete,
        -- Raw full week value — never partial adjusted for trend
        ROUND(l.metric_value, 2)                                        AS metric_value_display,
        l.metric_value_wow,
        l.metric_value_ly,
        l.wow_pct,
        l.yoy_pct,
        l.act_vs_fcst_pct,
        l.act_vs_fcst_delta,
        l.max_data_date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_gold_unified_long`     l
    JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_date`              d
        ON  l.week_sun_to_sat = d.week_sun_to_sat
        AND d.section_name    = 'Trend'
    -- Non-MFC only — MFC handled separately below
    WHERE l.data_source != 'MFC'
    -- Trend stops at last complete week per source
    AND   l.week_sun_to_sat <= l.max_data_date
),

-- MFC Trend: aggregated across tactic and agency
-- channel is the only remaining filter
-- SUM spend across all tactic/agency combinations per week/channel/lob/metric
trend_mfc AS (
    SELECT
        l.week_sun_to_sat,
        d.display_date,
        'WEEKLY'                                                        AS time_granularity,
        d.year_quarter,
        d.quarter_start,
        d.quarter_end,
        d.week_of_quarter,
        d.week_label,
        d.is_partial_week,
        d.partial_weight,
        d.days_in_quarter,
        d.section_name,
        l.data_source,
        l.channel,
        l.lob_supported,
        CAST(NULL AS STRING)                                            AS dimension_name,
        CAST(NULL AS STRING)                                            AS dimension_value,
        CAST(NULL AS STRING)                                            AS dimension_name_2,
        CAST(NULL AS STRING)                                            AS dimension_value_2,
        l.metric_name,
        -- BOUNDARY_WEEK rows may exist in Trend but values are pre-apportioned
        -- For Trend we use full-week Saturdays only (NORMAL rows)
        -- BOUNDARY_WEEK quarter_end_date rows excluded from Trend aggregation
        -- to avoid double-counting straddling week spend
        FALSE                                                           AS is_mfc_pre_apportioned,
        TRUE                                                            AS is_data_complete,
        -- SUM across tactic/agency — channel is the only remaining dimension
        ROUND(SUM(l.metric_value), 2)                                   AS metric_value_display,
        CAST(NULL AS FLOAT64)                                           AS metric_value_wow,
        CAST(NULL AS FLOAT64)                                           AS metric_value_ly,
        CAST(NULL AS FLOAT64)                                           AS wow_pct,
        CAST(NULL AS FLOAT64)                                           AS yoy_pct,
        CAST(NULL AS FLOAT64)                                           AS act_vs_fcst_pct,
        CAST(NULL AS FLOAT64)                                           AS act_vs_fcst_delta,
        MAX(l.max_data_date)                                            AS max_data_date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_gold_unified_long`     l
    JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_date`              d
        ON  l.week_sun_to_sat = d.week_sun_to_sat
        AND d.section_name    = 'Trend'
    WHERE l.data_source = 'MFC'
    -- NORMAL weeks only for trend — exclude BOUNDARY_WEEK quarter_end_date rows
    -- to avoid double-counting (the Saturday row already has the full week value)
    AND   l.is_mfc_pre_apportioned = FALSE
    AND   l.week_sun_to_sat <= l.max_data_date
    GROUP BY
        l.week_sun_to_sat,
        d.display_date,
        d.year_quarter,
        d.quarter_start,
        d.quarter_end,
        d.week_of_quarter,
        d.week_label,
        d.is_partial_week,
        d.partial_weight,
        d.days_in_quarter,
        d.section_name,
        l.data_source,
        l.channel,
        l.lob_supported,
        l.metric_name
)

-- -----------------------------------------------------------------------
-- FINAL OUTPUT
-- UNION ALL of Table + Trend (non-MFC) + Trend (MFC)
-- -----------------------------------------------------------------------
SELECT
    week_sun_to_sat,
    display_date,
    time_granularity,
    year_quarter,
    quarter_start,
    quarter_end,
    week_of_quarter,
    week_label,
    is_partial_week,
    partial_weight,
    days_in_quarter,
    section_name,
    data_source,
    channel,
    lob_supported,
    dimension_name,
    dimension_value,
    dimension_name_2,
    dimension_value_2,
    metric_name,
    is_data_complete,
    is_mfc_pre_apportioned,
    metric_value_display,
    metric_value_wow,
    metric_value_ly,
    wow_pct,
    yoy_pct,
    act_vs_fcst_pct,
    act_vs_fcst_delta,
    max_data_date
FROM table_section

UNION ALL

SELECT
    week_sun_to_sat,
    display_date,
    time_granularity,
    year_quarter,
    quarter_start,
    quarter_end,
    week_of_quarter,
    week_label,
    is_partial_week,
    partial_weight,
    days_in_quarter,
    section_name,
    data_source,
    channel,
    lob_supported,
    dimension_name,
    dimension_value,
    dimension_name_2,
    dimension_value_2,
    metric_name,
    is_data_complete,
    is_mfc_pre_apportioned,
    metric_value_display,
    metric_value_wow,
    metric_value_ly,
    wow_pct,
    yoy_pct,
    act_vs_fcst_pct,
    act_vs_fcst_delta,
    max_data_date
FROM trend_non_mfc

UNION ALL

SELECT
    week_sun_to_sat,
    display_date,
    time_granularity,
    year_quarter,
    quarter_start,
    quarter_end,
    week_of_quarter,
    week_label,
    is_partial_week,
    partial_weight,
    days_in_quarter,
    section_name,
    data_source,
    channel,
    lob_supported,
    dimension_name,
    dimension_value,
    dimension_name_2,
    dimension_value_2,
    metric_name,
    is_data_complete,
    is_mfc_pre_apportioned,
    metric_value_display,
    metric_value_wow,
    metric_value_ly,
    wow_pct,
    yoy_pct,
    act_vs_fcst_pct,
    act_vs_fcst_delta,
    max_data_date
FROM trend_mfc

ORDER BY
    week_sun_to_sat         ASC,
    section_name            ASC,
    year_quarter            ASC,
    data_source             ASC,
    channel                 ASC,
    metric_name             ASC,
    dimension_value         ASC,
    dimension_value_2       ASC
;