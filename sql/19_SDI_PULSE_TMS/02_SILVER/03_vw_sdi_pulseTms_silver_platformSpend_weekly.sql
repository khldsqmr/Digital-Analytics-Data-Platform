/* =================================================================================================
FILE:         03_vw_sdi_pulseTms_silver_platformSpend_weekly.sql
LAYER:        Silver View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseTms_silver_platformSpend_weekly

RAW SOURCES (via Bronze):
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_bronze_platformSpend_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_platformSpend_weekly

PURPOSE:
  Silver view for Platform spend data in the PulseTMS pipeline.
  All heavy lifting happens here — Bronze is source-close only.

  This view:
    1. Joins Bronze Platform to QGP calendar dim on week_sun_sat = qgp_date.
       Platform spend is daily-aggregated to week-ending Saturday in Bronze,
       aligning to NORMAL QGP dates. No pre-split boundary rows exist in the
       platform source — BOUNDARY_STUB calendar rows have NULL platform values.
    2. Filters to lob = 'POSTPAID' to match MFC_SPEND_CHANNEL grain (CONSUMER POSTPAID).
    3. Produces LONG format at qgp_date × lob × channel_group × metric_name.
    4. Computes 'All Channels' rollup per LOB via UNION inside this Silver.
    5. Computes WoW / YoY numerators, denominators, wow_pct, yoy_pct.
    6. Enforces completeness: metric_value = NULL for incomplete/future periods.
    7. Computes max_date per metric_name.

IMPORTANT NOTE ON PLATFORM BOUNDARY WEEKS:
  Unlike MFC, platform source is daily and does NOT pre-apportion spend across
  quarter boundaries. The Bronze aggregates to week-ending Saturday, so a
  boundary week's spend is fully contained in the Saturday row.
  Therefore BOUNDARY_FIRST wow_numerator = metric_value only (no stub to add).
  BOUNDARY_STUB rows from the calendar dim have NULL platform metric values.

LOB SCOPE:
  lob = 'POSTPAID' only — matches MFC_SPEND_CHANNEL grain (CONSUMER POSTPAID).

BUSINESS GRAIN:
  One row per:
    qgp_date × lob × channel_group × metric_name
  (includes 'All Channels' row per lob)

METRIC NAMES (camelCase):
  platformSpend — weekly aggregated platform spend

WoW / YoY LOGIC:
  NORMAL / BOUNDARY_FIRST: numerator = current value (no stub for platform)
  BOUNDARY_STUB           : all WoW/YoY fields = NULL
  wow_pct / yoy_pct       : NULL when denominator is NULL or zero

DOWNSTREAM:
  08_vw_sdi_pulseTms_gold_unified_long
================================================================================================= */

CREATE OR REPLACE VIEW
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_platformSpend_weekly`
AS

WITH

-- ---------------------------------------------------------------------------
-- STEP 1: Join Bronze Platform to QGP calendar
--         Filter to POSTPAID only to match channel-group grain
-- ---------------------------------------------------------------------------
BronzeWithCalendar AS (
  SELECT
    cal.qgp_date,
    cal.week_type,
    cal.quarter,
    cal.days_in_period,
    cal.is_complete_period,
    cal.is_current_quarter,
    cal.wow_prior_qgp_date,
    cal.prior_year_qgp_date,
    cal.boundary_stub_date,
    cal.iso_week_number,
    cal.iso_year,

    b.lob,
    b.channel_group,

    -- Apply completeness rule
    IF(cal.is_complete_period, b.spend, NULL)                           AS spend

  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar` cal
  LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_bronze_platformSpend_weekly` b
    ON  b.week_sun_sat = cal.qgp_date
    AND b.lob = 'POSTPAID'  -- POSTPAID only to match MFC_SPEND_CHANNEL grain
  WHERE cal.is_current_quarter = TRUE
     OR cal.qgp_date < DATE_TRUNC(CURRENT_DATE(), QUARTER)
),

-- ---------------------------------------------------------------------------
-- STEP 2: Long format — individual channel_groups
-- ---------------------------------------------------------------------------
UnpivotedBase AS (
  SELECT
    qgp_date, week_type, quarter, days_in_period, is_complete_period,
    is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
    boundary_stub_date, iso_week_number, iso_year,
    lob,
    channel_group,
    'platformSpend'                                                     AS metric_name,
    spend                                                               AS metric_value
  FROM BronzeWithCalendar
  WHERE lob IS NOT NULL
),

-- ---------------------------------------------------------------------------
-- STEP 3: All Channels rollup per LOB
-- ---------------------------------------------------------------------------
AllChannelsRollup AS (
  SELECT
    qgp_date, week_type, quarter, days_in_period, is_complete_period,
    is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
    boundary_stub_date, iso_week_number, iso_year,
    lob,
    'All Channels'                                                      AS channel_group,
    metric_name,
    SUM(metric_value)                                                   AS metric_value
  FROM UnpivotedBase
  GROUP BY
    qgp_date, week_type, quarter, days_in_period, is_complete_period,
    is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
    boundary_stub_date, iso_week_number, iso_year,
    lob, metric_name
),

-- Combined: individual channel_groups + All Channels per LOB
Unpivoted AS (
  SELECT * FROM UnpivotedBase
  UNION ALL
  SELECT * FROM AllChannelsRollup
),

-- ---------------------------------------------------------------------------
-- STEP 4: Metric lookup for WoW / YoY joins
-- ---------------------------------------------------------------------------
MetricLookup AS (
  SELECT qgp_date, lob, channel_group, metric_name, metric_value
  FROM Unpivoted
),

-- ---------------------------------------------------------------------------
-- STEP 5: WoW / YoY computation
--         Platform has no BOUNDARY_STUB rows in source — Saturday aggregation
--         captures full week including cross-quarter days.
--         wow_numerator for BOUNDARY_FIRST = metric_value only (no stub).
-- ---------------------------------------------------------------------------
WithWowYoy AS (
  SELECT
    u.qgp_date,
    u.week_type,
    u.quarter,
    u.days_in_period,
    u.lob,
    u.channel_group,
    u.metric_name,
    u.metric_value,

    ly_lookup.metric_value                                              AS metric_value_ly,

    -- WoW numerator: no stub for platform
    CASE u.week_type
      WHEN 'BOUNDARY_STUB' THEN NULL
      ELSE                      u.metric_value
    END                                                                 AS wow_numerator,

    -- WoW denominator
    CASE u.week_type
      WHEN 'BOUNDARY_STUB' THEN NULL
      ELSE                      wow_prior_lookup.metric_value
    END                                                                 AS wow_denominator,

    -- YoY numerator: no stub for platform
    CASE u.week_type
      WHEN 'BOUNDARY_STUB' THEN NULL
      ELSE                      u.metric_value
    END                                                                 AS yoy_numerator,

    -- YoY denominator
    CASE u.week_type
      WHEN 'BOUNDARY_STUB' THEN NULL
      ELSE                      ly_lookup.metric_value
    END                                                                 AS yoy_denominator

  FROM Unpivoted u

  LEFT JOIN MetricLookup wow_prior_lookup
    ON  wow_prior_lookup.qgp_date      = u.wow_prior_qgp_date
    AND wow_prior_lookup.lob           = u.lob
    AND wow_prior_lookup.channel_group = u.channel_group
    AND wow_prior_lookup.metric_name   = u.metric_name

  LEFT JOIN MetricLookup ly_lookup
    ON  ly_lookup.qgp_date      = u.prior_year_qgp_date
    AND ly_lookup.lob           = u.lob
    AND ly_lookup.channel_group = u.channel_group
    AND ly_lookup.metric_name   = u.metric_name
)

-- ---------------------------------------------------------------------------
-- STEP 6: Final output — add wow_pct, yoy_pct, max_date
-- ---------------------------------------------------------------------------
SELECT
  qgp_date,
  week_type,
  quarter,
  days_in_period,
  lob,
  channel_group,
  metric_name,
  metric_value,
  metric_value_ly,
  wow_numerator,
  wow_denominator,

  CASE
    WHEN wow_denominator IS NULL OR wow_denominator = 0 THEN NULL
    ELSE wow_numerator / wow_denominator - 1
  END                                                                   AS wow_pct,

  yoy_numerator,
  yoy_denominator,

  CASE
    WHEN yoy_denominator IS NULL OR yoy_denominator = 0 THEN NULL
    ELSE yoy_numerator / yoy_denominator - 1
  END                                                                   AS yoy_pct,

  -- max_date per metric_name
  MAX(CASE WHEN metric_value IS NOT NULL THEN qgp_date END)
    OVER (PARTITION BY metric_name)                                     AS max_date

FROM WithWowYoy
;