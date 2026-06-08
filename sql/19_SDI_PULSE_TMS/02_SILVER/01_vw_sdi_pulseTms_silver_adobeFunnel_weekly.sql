/* =================================================================================================
FILE:         01_vw_sdi_pulseTms_silver_adobeFunnel_weekly.sql
LAYER:        Silver View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseTms_silver_adobeFunnel_weekly

RAW SOURCES (via Bronze):
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_bronze_adobeFunnel_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_adobeFunnel_weekly

PURPOSE:
  Silver view for Adobe UPV funnel metrics in the PulseTMS pipeline.
  All heavy lifting happens here — Bronze is source-close only.

  This view:
    1. Joins Bronze Adobe to QGP calendar dim on week_sun_sat = qgp_date.
       Adobe raw tables use week-ending Saturdays → align to NORMAL QGP dates.
       BOUNDARY_STUB and BOUNDARY_FIRST dates have no Adobe source rows and
       appear as NULL metric rows via the calendar spine.
    2. Renames channel_group 'ALL' → 'All Channels'.
       The ALL row comes directly from Adobe's own all-channel source tables
       and is NOT recomputed as a sum of individual channel groups.
    3. Pivots wide Bronze format → LONG format with camelCase metric_name values.
    4. Computes derived totals (cartstartTotal, ordersTotal etc).
    5. Attaches metric_value_ly via join to prior_year_qgp_date.
    6. Computes WoW and YoY numerators and denominators:
         NORMAL         : numerator = current value; denominator = prior week value
         BOUNDARY_FIRST : numerator = current + stub value; denominator = last full week
         BOUNDARY_STUB  : all WoW/YoY fields = NULL
    7. Computes wow_pct and yoy_pct:
         = numerator / denominator - 1
         NULL when denominator is NULL or zero (BOUNDARY_STUB, missing data)
    8. Enforces completeness: metric_value = NULL for incomplete/future periods.
    9. Computes max_date per metric_name:
         MAX(qgp_date WHERE metric_value IS NOT NULL) OVER (PARTITION BY metric_name)
         Represents the most recent date with actual data for each metric.

BUSINESS GRAIN:
  One row per:
    qgp_date × channel_group × metric_name

METRIC NAMES (camelCase):
  upvPostpaid, upvHsi, upvByod,
  upvFlowTotal, upvTotalAdobe,
  cartstartPostpaid, cartstartHsi, cartstartByod, cartstartTotal,
  ordersUnassistedPostpaid, ordersUnassistedHsi, ordersUnassistedByod, ordersUnassistedTotal,
  ordersAssistedPostpaid, ordersAssistedHsi, ordersAssistedByod, ordersAssistedTotal,
  ordersTotal

WoW / YoY BOUNDARY LOGIC:
  NORMAL (e.g. Apr 11):
    wow_numerator   = metric_value (Apr 11)
    wow_denominator = metric_value at wow_prior_qgp_date (Apr 4)
    wow_pct         = wow_numerator / wow_denominator - 1

  BOUNDARY_FIRST (e.g. Apr 4, stub = Mar 31):
    wow_numerator   = metric_value (Apr 4) + metric_value (Mar 31 stub)
    wow_denominator = metric_value at wow_prior_qgp_date (Mar 28)
    wow_pct         = wow_numerator / wow_denominator - 1
    yoy_numerator   = current year combined (Apr 4 + Mar 31)
    yoy_denominator = prior year BOUNDARY_FIRST + prior year stub

  BOUNDARY_STUB (e.g. Mar 31):
    All WoW/YoY fields = NULL — stubs never shown as standalone comparison points

COMPLETENESS RULE:
  metric_value = NULL when is_complete_period = FALSE.
  QGP spine extends through end of current quarter for Tableau placeholder rows.

DOWNSTREAM:
  08_vw_sdi_pulseTms_gold_unified_long
================================================================================================= */

CREATE OR REPLACE VIEW
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_adobeFunnel_weekly`
AS

WITH

-- ---------------------------------------------------------------------------
-- STEP 1: Join Bronze Adobe to QGP calendar
--         Rename 'ALL' → 'All Channels'
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

    -- Rename ALL → All Channels
    CASE b.channel_group
      WHEN 'ALL' THEN 'All Channels'
      ELSE b.channel_group
    END                                                                 AS channel_group,

    -- Apply completeness rule inline
    IF(cal.is_complete_period, b.upvPostpaid,               NULL)     AS upvPostpaid,
    IF(cal.is_complete_period, b.upvHsi,                    NULL)     AS upvHsi,
    IF(cal.is_complete_period, b.upvByod,                   NULL)     AS upvByod,
    IF(cal.is_complete_period, b.upvFlowTotal,             NULL)     AS upvFlowTotal,
    IF(cal.is_complete_period, b.upvTotalAdobe,            NULL)     AS upvTotalAdobe,
    IF(cal.is_complete_period, b.cartstartPostpaid,         NULL)     AS cartstartPostpaid,
    IF(cal.is_complete_period, b.cartstartHsi,              NULL)     AS cartstartHsi,
    IF(cal.is_complete_period, b.cartstartByod,             NULL)     AS cartstartByod,
    IF(cal.is_complete_period, b.ordersUnassistedPostpaid, NULL)     AS ordersUnassistedPostpaid,
    IF(cal.is_complete_period, b.ordersUnassistedHsi,      NULL)     AS ordersUnassistedHsi,
    IF(cal.is_complete_period, b.ordersUnassistedByod,     NULL)     AS ordersUnassistedByod,
    IF(cal.is_complete_period, b.ordersAssistedPostpaid,   NULL)     AS ordersAssistedPostpaid,
    IF(cal.is_complete_period, b.ordersAssistedHsi,        NULL)     AS ordersAssistedHsi,
    IF(cal.is_complete_period, b.ordersAssistedByod,       NULL)     AS ordersAssistedByod,

    -- Derived totals (NULL if any component is NULL — no COALESCE)
    IF(cal.is_complete_period,
       b.cartstartPostpaid + b.cartstartHsi + b.cartstartByod,
       NULL)                                                            AS cartstartTotal,
    IF(cal.is_complete_period,
       b.ordersUnassistedPostpaid + b.ordersUnassistedHsi + b.ordersUnassistedByod,
       NULL)                                                            AS ordersUnassistedTotal,
    IF(cal.is_complete_period,
       b.ordersAssistedPostpaid + b.ordersAssistedHsi + b.ordersAssistedByod,
       NULL)                                                            AS ordersAssistedTotal,
    IF(cal.is_complete_period,
       (b.ordersUnassistedPostpaid + b.ordersUnassistedHsi + b.ordersUnassistedByod)
       + (b.ordersAssistedPostpaid + b.ordersAssistedHsi + b.ordersAssistedByod),
       NULL)                                                            AS ordersTotal

  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar` cal
  LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_bronze_adobeFunnel_weekly` b
    ON b.week_sun_sat = cal.qgp_date
  WHERE cal.is_current_quarter = TRUE
     OR cal.qgp_date < DATE_TRUNC(CURRENT_DATE(), QUARTER)
),

-- ---------------------------------------------------------------------------
-- STEP 2: Unpivot wide → long format with camelCase metric names
-- ---------------------------------------------------------------------------
Unpivoted AS (

  SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period,
         is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
         boundary_stub_date, iso_week_number, iso_year, channel_group,
         'upvPostpaid'                AS metric_name, upvPostpaid               AS metric_value
  FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

  UNION ALL
  SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period,
         is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
         boundary_stub_date, iso_week_number, iso_year, channel_group,
         'upvHsi',                    upvHsi
  FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

  UNION ALL
  SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period,
         is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
         boundary_stub_date, iso_week_number, iso_year, channel_group,
         'upvByod',                   upvByod
  FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

  UNION ALL
  SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period,
         is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
         boundary_stub_date, iso_week_number, iso_year, channel_group,
         'upvFlowTotal',              upvFlowTotal
  FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

  UNION ALL
  SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period,
         is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
         boundary_stub_date, iso_week_number, iso_year, channel_group,
         'upvTotalAdobe',             upvTotalAdobe
  FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

  UNION ALL
  SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period,
         is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
         boundary_stub_date, iso_week_number, iso_year, channel_group,
         'cartstartPostpaid',         cartstartPostpaid
  FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

  UNION ALL
  SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period,
         is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
         boundary_stub_date, iso_week_number, iso_year, channel_group,
         'cartstartHsi',              cartstartHsi
  FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

  UNION ALL
  SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period,
         is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
         boundary_stub_date, iso_week_number, iso_year, channel_group,
         'cartstartByod',             cartstartByod
  FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

  UNION ALL
  SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period,
         is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
         boundary_stub_date, iso_week_number, iso_year, channel_group,
         'cartstartTotal',            cartstartTotal
  FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

  UNION ALL
  SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period,
         is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
         boundary_stub_date, iso_week_number, iso_year, channel_group,
         'ordersUnassistedPostpaid',  ordersUnassistedPostpaid
  FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

  UNION ALL
  SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period,
         is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
         boundary_stub_date, iso_week_number, iso_year, channel_group,
         'ordersUnassistedHsi',       ordersUnassistedHsi
  FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

  UNION ALL
  SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period,
         is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
         boundary_stub_date, iso_week_number, iso_year, channel_group,
         'ordersUnassistedByod',      ordersUnassistedByod
  FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

  UNION ALL
  SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period,
         is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
         boundary_stub_date, iso_week_number, iso_year, channel_group,
         'ordersUnassistedTotal',     ordersUnassistedTotal
  FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

  UNION ALL
  SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period,
         is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
         boundary_stub_date, iso_week_number, iso_year, channel_group,
         'ordersAssistedPostpaid',    ordersAssistedPostpaid
  FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

  UNION ALL
  SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period,
         is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
         boundary_stub_date, iso_week_number, iso_year, channel_group,
         'ordersAssistedHsi',         ordersAssistedHsi
  FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

  UNION ALL
  SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period,
         is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
         boundary_stub_date, iso_week_number, iso_year, channel_group,
         'ordersAssistedByod',        ordersAssistedByod
  FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

  UNION ALL
  SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period,
         is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
         boundary_stub_date, iso_week_number, iso_year, channel_group,
         'ordersAssistedTotal',       ordersAssistedTotal
  FROM BronzeWithCalendar WHERE channel_group IS NOT NULL

  UNION ALL
  SELECT qgp_date, week_type, quarter, days_in_period, is_complete_period,
         is_current_quarter, wow_prior_qgp_date, prior_year_qgp_date,
         boundary_stub_date, iso_week_number, iso_year, channel_group,
         'ordersTotal',               ordersTotal
  FROM BronzeWithCalendar WHERE channel_group IS NOT NULL
),

-- ---------------------------------------------------------------------------
-- STEP 3: Metric lookup for WoW / YoY self-joins
-- ---------------------------------------------------------------------------
MetricLookup AS (
  SELECT qgp_date, channel_group, metric_name, metric_value
  FROM Unpivoted
),

-- ---------------------------------------------------------------------------
-- STEP 4: Compute WoW / YoY numerators, denominators, and percentages
-- ---------------------------------------------------------------------------
WithWowYoy AS (
  SELECT
    u.qgp_date,
    u.week_type,
    u.quarter,
    u.days_in_period,
    u.is_complete_period,
    u.is_current_quarter,
    u.channel_group,
    u.metric_name,
    u.metric_value,

    -- metric_value_ly: value at same ISO week prior year
    ly_lookup.metric_value                                              AS metric_value_ly,

    -- -----------------------------------------------------------------------
    -- WoW numerator
    -- BOUNDARY_STUB  → NULL
    -- BOUNDARY_FIRST → current + stub (reconstructed full 7-day week)
    -- NORMAL         → current value
    -- -----------------------------------------------------------------------
    CASE u.week_type
      WHEN 'BOUNDARY_STUB'  THEN NULL
      WHEN 'BOUNDARY_FIRST' THEN u.metric_value + stub_lookup.metric_value
      ELSE                       u.metric_value
    END                                                                 AS wow_numerator,

    -- WoW denominator: value at wow_prior_qgp_date
    CASE u.week_type
      WHEN 'BOUNDARY_STUB' THEN NULL
      ELSE                      wow_prior_lookup.metric_value
    END                                                                 AS wow_denominator,

    -- -----------------------------------------------------------------------
    -- YoY numerator
    -- -----------------------------------------------------------------------
    CASE u.week_type
      WHEN 'BOUNDARY_STUB'  THEN NULL
      WHEN 'BOUNDARY_FIRST' THEN u.metric_value + stub_lookup.metric_value
      ELSE                       u.metric_value
    END                                                                 AS yoy_numerator,

    -- -----------------------------------------------------------------------
    -- YoY denominator
    -- BOUNDARY_FIRST → prior year BOUNDARY_FIRST + prior year stub
    -- NORMAL         → value at prior_year_qgp_date
    -- -----------------------------------------------------------------------
    CASE u.week_type
      WHEN 'BOUNDARY_STUB'  THEN NULL
      WHEN 'BOUNDARY_FIRST' THEN yoy_bf_lookup.metric_value + yoy_stub_lookup.metric_value
      ELSE                       ly_lookup.metric_value
    END                                                                 AS yoy_denominator

  FROM Unpivoted u

  -- WoW denominator
  LEFT JOIN MetricLookup wow_prior_lookup
    ON  wow_prior_lookup.qgp_date      = u.wow_prior_qgp_date
    AND wow_prior_lookup.channel_group = u.channel_group
    AND wow_prior_lookup.metric_name   = u.metric_name

  -- LY lookup
  LEFT JOIN MetricLookup ly_lookup
    ON  ly_lookup.qgp_date      = u.prior_year_qgp_date
    AND ly_lookup.channel_group = u.channel_group
    AND ly_lookup.metric_name   = u.metric_name

  -- Stub lookup: preceding stub value for BOUNDARY_FIRST rows
  LEFT JOIN MetricLookup stub_lookup
    ON  stub_lookup.qgp_date      = u.boundary_stub_date
    AND stub_lookup.channel_group = u.channel_group
    AND stub_lookup.metric_name   = u.metric_name

  -- YoY BOUNDARY_FIRST: prior year BOUNDARY_FIRST value
  LEFT JOIN MetricLookup yoy_bf_lookup
    ON  yoy_bf_lookup.qgp_date      = u.prior_year_qgp_date
    AND yoy_bf_lookup.channel_group = u.channel_group
    AND yoy_bf_lookup.metric_name   = u.metric_name

  -- YoY stub: prior year stub via calendar dim
  LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_dim_qgp_calendar` ly_cal
    ON  ly_cal.qgp_date = u.prior_year_qgp_date
  LEFT JOIN MetricLookup yoy_stub_lookup
    ON  yoy_stub_lookup.qgp_date      = ly_cal.boundary_stub_date
    AND yoy_stub_lookup.channel_group = u.channel_group
    AND yoy_stub_lookup.metric_name   = u.metric_name
)

-- ---------------------------------------------------------------------------
-- STEP 5: Final output — add wow_pct, yoy_pct, max_date
--         max_date: MAX(qgp_date WHERE metric_value IS NOT NULL)
--                   partitioned by metric_name
--                   represents most recent date with actual data per metric
-- ---------------------------------------------------------------------------
SELECT
  qgp_date,
  week_type,
  quarter,
  days_in_period,
  channel_group,
  metric_name,
  metric_value,
  metric_value_ly,
  wow_numerator,
  wow_denominator,

  -- wow_pct: NULL when denominator is NULL or zero
  CASE
    WHEN wow_denominator IS NULL OR wow_denominator = 0 THEN NULL
    ELSE wow_numerator / wow_denominator - 1
  END                                                                   AS wow_pct,

  yoy_numerator,
  yoy_denominator,

  -- yoy_pct: NULL when denominator is NULL or zero
  CASE
    WHEN yoy_denominator IS NULL OR yoy_denominator = 0 THEN NULL
    ELSE yoy_numerator / yoy_denominator - 1
  END                                                                   AS yoy_pct,

  -- max_date per metric_name: most recent qgp_date with actual data
  MAX(CASE WHEN metric_value IS NOT NULL THEN qgp_date END)
    OVER (PARTITION BY metric_name)                                     AS max_date

FROM WithWowYoy
;