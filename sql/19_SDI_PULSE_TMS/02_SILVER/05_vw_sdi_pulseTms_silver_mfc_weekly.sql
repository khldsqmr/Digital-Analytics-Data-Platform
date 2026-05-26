/* =================================================================================================
FILE:         05_vw_sdi_pulseTms_silver_mfc_weekly.sql
LAYER:        Silver View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseTms_silver_mfc_weekly

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_bronze_mfc_weekly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_mfc_weekly

PURPOSE:
  Silver view for MFC weekly spend data.
  Adds WoW/LY comparisons, act_vs_fcst metrics, and max_data_date.
  Standardizes quarter format from '2026 Q1' → '2026-Q1'.
  Preserves full dimension grain for Gold Long unpivot.

BUSINESS GRAIN:
  One row per:
    week_sun_to_sat × lob_supported × channel × tactic × message_type × agency

  channel = standardized channel from Bronze (PAID SEARCH, SOCIAL,
            PROGRAMMATIC, OTHER) — consistent with Adobe/SA360/GSC

  This grain produces hundreds of rows per week — the Gold Long view
  unpivots spend metrics while preserving all dimension columns.

DATE CONVENTION:
  week_sun_to_sat = qgp_week from Bronze
  Saturdays for NORMAL weeks, quarter-end dates for BOUNDARY_WEEK rows
  Directly compatible with dim_date join in reporting view

WoW LOGIC:
  NORMAL weeks:
    wow_pct = (current_spend - prior_spend) / prior_spend
    Joins to prior week at same grain (week_sun_to_sat - 7 days)

  BOUNDARY_WEEK where week_sun_to_sat = quarter_end_date (e.g. 2026-03-31):
    wow_pct = NULL
    This row represents only the partial tail days of the prior quarter
    Comparing partial days to a full week is meaningless
    WoW suppressed entirely for these rows

  BOUNDARY_WEEK where week_sun_to_sat = actual Saturday (e.g. 2026-04-04):
    wow_pct = current row vs prior Saturday's row
    Even though this row contains only apportioned days, the WoW compares
    the apportioned values — consistent, comparable, meaningful
    The dashboard shows this WoW% on the Apr-04 column

LY LOGIC:
  Same grain, custom_week_num - 52 (gap-safe Sun-to-Sat week matching)
  BOUNDARY_WEEK quarter_end_date rows: yoy_pct = NULL (same reason as WoW)
  All other rows: yoy_pct populated where prior year data exists

ACT VS FCST:
  act_vs_fcst_pct   = (spend_actual - spend_forecast) / NULLIF(spend_forecast, 0)
  act_vs_fcst_delta = spend_actual - spend_forecast
  NULL when spend_actual IS NULL (line item not yet executed)

PARTIAL WEIGHT NOTE:
  MFC spend values for BOUNDARY_WEEK rows are ALREADY apportioned at source.
  This Silver view does NOT compute or store partial_weight.
  The reporting view must NOT multiply MFC values by dim_date.partial_weight.
  MFC metric_value_display = metric_value as-is (no further adjustment).

CUSTOM WEEK NUMBER:
  Anchored to 2023-01-01 (a Sunday):
    custom_week_num = DATE_DIFF(DATE_SUB(week_sun_to_sat, INTERVAL 6 DAY), DATE '2023-01-01', WEEK)
  LY match: current.custom_week_num - prior.custom_week_num = 52
  For BOUNDARY_WEEK quarter_end_date rows (e.g. Mar-31 which is a Tuesday):
    DATE_SUB(2026-03-31, INTERVAL 6 DAY) = 2026-03-25 (Wednesday)
    This is used only for LY matching — suppressed to NULL anyway for these rows

KEY MODELING NOTES:
  - NULLs preserved — no fake zeroes
  - wow_pct and yoy_pct as decimals (0.051 = 5.1%)
  - NULL when prior value is NULL or 0
  - No ORDER BY — applied in Gold only
  - Self-joins on the full-grain CTE — more expensive than Adobe/SA360
    (hundreds of rows per week vs one row per week) but manageable at weekly grain

DOWNSTREAM:
  Gold Long : vw_sdi_pulseTms_gold_mfc_long
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_mfc_weekly`
AS

WITH

-- -----------------------------------------------------------------------
-- STEP 1: Standardize dates and quarter format
-- week_sun_to_sat = qgp_week (already Saturdays + quarter-end dates)
-- quarter standardized: '2026 Q1' → '2026-Q1'
-- is_boundary_quarter_end: TRUE for the partial rows displayed at quarter_end_date
--   These are the rows where wow_pct and yoy_pct must be NULL
-- -----------------------------------------------------------------------
base AS (
    SELECT
        qgp_week                                                        AS week_sun_to_sat,
        period_start,
        period_end,
        week_type,
        quarter_end_date,

        -- Standardize quarter format from '2026 Q1' to '2026-Q1'
        REPLACE(quarter_raw, ' ', '-')                                  AS year_quarter,

        -- Flag for boundary week rows displayed at quarter_end_date
        -- These rows represent the partial tail days of the prior quarter
        -- WoW and YoY are suppressed for these rows
        CASE
            WHEN week_type = 'BOUNDARY_WEEK'
             AND qgp_week  = quarter_end_date
                THEN TRUE
            ELSE FALSE
        END                                                             AS is_boundary_quarter_end,

        -- Dimension columns
        lob_supported,
        channel,
        tactic,
        message_type,
        agency,

        -- Spend metrics
        spend_actual,
        spend_forecast,
        spend_display,

        -- Act vs Fcst — computed before self-joins for clarity
        -- NULL when spend_actual IS NULL (unexecuted line item)
        CASE
            WHEN spend_actual IS NULL                    THEN NULL
            WHEN spend_forecast IS NULL OR spend_forecast = 0 THEN NULL
            ELSE ROUND((spend_actual - spend_forecast) / spend_forecast, 6)
        END                                                             AS act_vs_fcst_pct,

        CASE
            WHEN spend_actual IS NULL THEN NULL
            ELSE spend_actual - spend_forecast
        END                                                             AS act_vs_fcst_delta,

        -- Custom week number for gap-safe LY matching
        -- Anchored to 2023-01-01 (a Sunday)
        DATE_DIFF(
            DATE_SUB(qgp_week, INTERVAL 6 DAY),
            DATE '2023-01-01',
            WEEK
        )                                                               AS custom_week_num

    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_bronze_mfc_weekly`
),

-- -----------------------------------------------------------------------
-- STEP 2: WoW self-join
-- Join on same grain (all dimension columns) + week_sun_to_sat - 7 days
-- For BOUNDARY_WEEK quarter_end_date rows: WoW join will find nothing
--   because prior week Saturday is a NORMAL row 7 days earlier
--   The join succeeds but wow_pct is suppressed via is_boundary_quarter_end flag
-- -----------------------------------------------------------------------
with_wow AS (
    SELECT
        c.week_sun_to_sat,
        c.period_start,
        c.period_end,
        c.week_type,
        c.quarter_end_date,
        c.year_quarter,
        c.is_boundary_quarter_end,
        c.lob_supported,
        c.channel,
        c.tactic,
        c.message_type,
        c.agency,
        c.custom_week_num,

        -- Current week metrics
        c.spend_actual,
        c.spend_forecast,
        c.spend_display,
        c.act_vs_fcst_pct,
        c.act_vs_fcst_delta,

        -- WoW prior values
        -- Join on full grain + exact 7-day prior week (gap-safe)
        w.spend_actual                                                  AS spend_actual_wow,
        w.spend_forecast                                                AS spend_forecast_wow,
        w.spend_display                                                 AS spend_display_wow

    FROM base c
    LEFT JOIN base w
        ON  c.week_sun_to_sat = DATE_ADD(w.week_sun_to_sat, INTERVAL 7 DAY)
        AND c.lob_supported   = w.lob_supported
        AND c.channel         = w.channel
        AND c.tactic          = w.tactic
        AND c.message_type    = w.message_type
        AND c.agency          = w.agency
),

-- -----------------------------------------------------------------------
-- STEP 3: LY self-join
-- Same grain + custom_week_num - 52 (gap-safe same Sun-to-Sat week LY)
-- -----------------------------------------------------------------------
with_ly AS (
    SELECT
        c.week_sun_to_sat,
        c.period_start,
        c.period_end,
        c.week_type,
        c.quarter_end_date,
        c.year_quarter,
        c.is_boundary_quarter_end,
        c.lob_supported,
        c.channel,
        c.tactic,
        c.message_type,
        c.agency,
        c.custom_week_num,

        c.spend_actual,
        c.spend_forecast,
        c.spend_display,
        c.act_vs_fcst_pct,
        c.act_vs_fcst_delta,

        c.spend_actual_wow,
        c.spend_forecast_wow,
        c.spend_display_wow,

        -- LY prior values
        l.spend_actual                                                  AS spend_actual_ly,
        l.spend_forecast                                                AS spend_forecast_ly,
        l.spend_display                                                 AS spend_display_ly

    FROM with_wow c
    LEFT JOIN base l
        ON  (c.custom_week_num - l.custom_week_num) = 52
        AND c.lob_supported   = l.lob_supported
        AND c.channel         = l.channel
        AND c.tactic          = l.tactic
        AND c.message_type    = l.message_type
        AND c.agency          = l.agency
),

-- -----------------------------------------------------------------------
-- STEP 4: Compute wow_pct and yoy_pct
--
-- WoW rules:
--   is_boundary_quarter_end = TRUE  → NULL (partial tail days, no WoW)
--   Prior value NULL or 0           → NULL
--   Otherwise                       → (current - prior) / prior
--
-- Same rules apply to yoy_pct
-- -----------------------------------------------------------------------
with_pcts AS (
    SELECT
        week_sun_to_sat,
        period_start,
        period_end,
        week_type,
        quarter_end_date,
        year_quarter,
        is_boundary_quarter_end,
        lob_supported,
        channel,
        tactic,
        message_type,
        agency,
        custom_week_num,

        spend_actual,
        spend_forecast,
        spend_display,
        act_vs_fcst_pct,
        act_vs_fcst_delta,

        spend_actual_wow,
        spend_forecast_wow,
        spend_display_wow,
        spend_actual_ly,
        spend_forecast_ly,
        spend_display_ly,

        -- ---- spend_actual WoW / YoY ----
        CASE
            WHEN is_boundary_quarter_end                              THEN NULL
            WHEN spend_actual_wow IS NULL OR spend_actual_wow = 0    THEN NULL
            ELSE ROUND((spend_actual - spend_actual_wow) / spend_actual_wow, 6)
        END                                                             AS spend_actual_wow_pct,

        CASE
            WHEN is_boundary_quarter_end                              THEN NULL
            WHEN spend_actual_ly  IS NULL OR spend_actual_ly  = 0    THEN NULL
            ELSE ROUND((spend_actual - spend_actual_ly)  / spend_actual_ly,  6)
        END                                                             AS spend_actual_yoy_pct,

        -- ---- spend_forecast WoW / YoY ----
        CASE
            WHEN is_boundary_quarter_end                                  THEN NULL
            WHEN spend_forecast_wow IS NULL OR spend_forecast_wow = 0    THEN NULL
            ELSE ROUND((spend_forecast - spend_forecast_wow) / spend_forecast_wow, 6)
        END                                                             AS spend_forecast_wow_pct,

        CASE
            WHEN is_boundary_quarter_end                                  THEN NULL
            WHEN spend_forecast_ly  IS NULL OR spend_forecast_ly  = 0    THEN NULL
            ELSE ROUND((spend_forecast - spend_forecast_ly)  / spend_forecast_ly,  6)
        END                                                             AS spend_forecast_yoy_pct,

        -- ---- spend_display WoW / YoY (primary metric) ----
        CASE
            WHEN is_boundary_quarter_end                                  THEN NULL
            WHEN spend_display_wow IS NULL OR spend_display_wow = 0      THEN NULL
            ELSE ROUND((spend_display - spend_display_wow) / spend_display_wow, 6)
        END                                                             AS spend_display_wow_pct,

        CASE
            WHEN is_boundary_quarter_end                                  THEN NULL
            WHEN spend_display_ly  IS NULL OR spend_display_ly  = 0      THEN NULL
            ELSE ROUND((spend_display - spend_display_ly)  / spend_display_ly,  6)
        END                                                             AS spend_display_yoy_pct

    FROM with_ly
),

-- -----------------------------------------------------------------------
-- STEP 5: max_data_date
-- Latest week_sun_to_sat with any non-null spend_actual or spend_display
-- Includes BOUNDARY_WEEK rows — they are actual data, just apportioned
-- -----------------------------------------------------------------------
with_max_date AS (
    SELECT
        *,
        MAX(CASE
            WHEN spend_actual  IS NOT NULL
              OR spend_display IS NOT NULL
            THEN week_sun_to_sat
        END) OVER ()                                                    AS max_data_date
    FROM with_pcts
)

-- -----------------------------------------------------------------------
-- FINAL OUTPUT
-- Full grain: one row per week × LOB × channel × tactic × message_type × agency
-- -----------------------------------------------------------------------
SELECT
    week_sun_to_sat,
    'MFC'                                                               AS data_source,
    max_data_date,
    year_quarter,
    quarter_end_date,
    week_type,
    is_boundary_quarter_end,
    lob_supported,
    channel,
    tactic,
    message_type,
    agency,

    -- Spend actuals
    spend_actual,
    spend_actual_wow,
    spend_actual_ly,
    spend_actual_wow_pct,
    spend_actual_yoy_pct,

    -- Spend forecast
    spend_forecast,
    spend_forecast_wow,
    spend_forecast_ly,
    spend_forecast_wow_pct,
    spend_forecast_yoy_pct,

    -- Spend display (primary — actual when available, forecast otherwise)
    spend_display,
    spend_display_wow,
    spend_display_ly,
    spend_display_wow_pct,
    spend_display_yoy_pct,

    -- Actual vs Forecast
    act_vs_fcst_pct,
    act_vs_fcst_delta

FROM with_max_date
;