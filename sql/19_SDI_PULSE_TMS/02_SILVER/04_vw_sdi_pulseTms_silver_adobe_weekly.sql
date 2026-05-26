/* =================================================================================================
FILE:         04_vw_sdi_pulseTms_silver_adobe_weekly.sql
LAYER:        Silver View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseTms_silver_adobe_weekly

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_flowPerformanceByChannelGroupsPlusAll_Weekly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_adobe_weekly

PURPOSE:
  Silver view for Adobe Analytics TMS metrics (BYOD + Postpaid + HSI + Totals).
  Outputs a WIDE table — one row per week_sun_to_sat.
  Pivots ChannelGroup rows into columns so each channel becomes its own
  set of prefixed metric columns.
  Applies WoW/LY comparisons and max_data_date.
  All product lines retained: Postpaid, HSI, BYOD, and computed totals.

BUSINESS GRAIN:
  One row per:
    week_sun_to_sat

SOURCE GRAIN:
  One row per WeekSunSat + ChannelGroup
  ChannelGroup values: ALL, PAID SEARCH, ORGANIC SEARCH,
                       DIRECT, SOCIAL, PROGRAMMATIC, OTHER

BUSINESS LOGIC APPLIED:
  - data_source = 'ADOBE'
  - week_sun_to_sat sourced directly from WeekSunSat
    (source already uses Sun-to-Sat convention)
  - UvnbTrackedFlowSum, CartstartTotal, OrdersUnassistedTotal,
    OrdersAssistedTotal, OrdersTotal all pre-computed in source Silver
    No COALESCE — NULL if any component is NULL (consistent with source)
  - uvnbByodPctOfUvnbFlow computed before pivot for allChannels only
  - Pivot: MAX(CASE WHEN ChannelGroup = ...) per week
  - WoW: self-join on week_sun_to_sat - 7 days (gap-safe)
  - LY : self-join on custom_week_num - 52 (gap-safe, Sun-to-Sat)
  - wow_pct and yoy_pct as decimals — NULL when prior NULL or 0
  - max_data_date: latest week_sun_to_sat with any non-null metric

COLUMN NAMING CONVENTION:
  adobe_{metric}_{channel}
  adobe_{metric}_{channel}_wow
  adobe_{metric}_{channel}_ly
  adobe_{metric}_{channel}_wow_pct
  adobe_{metric}_{channel}_yoy_pct

  Channel suffixes:
    ALL            → allChannels
    PAID SEARCH    → paidSearch
    ORGANIC SEARCH → organicSearch
    DIRECT         → direct
    SOCIAL         → social
    PROGRAMMATIC   → programmatic
    OTHER          → other

  Metrics (allChannels only):
    uvnbTotalAdobe
    uvnbFlowTotal
    uvnbByodPctOfUvnbFlow  (derived)

  Metrics (all channels):
    uvnbPostpaid, uvnbHsi, uvnbByod, uvnbTrackedFlowSum
    cartStartTotal, cartStartPostpaid, cartStartHsi, cartStartByod
    ordersTotal
    ordersUnassistedTotal, ordersUnassistedPostpaid, ordersUnassistedHsi, ordersUnassistedByod
    ordersAssistedTotal, ordersAssistedPostpaid, ordersAssistedHsi, ordersAssistedByod

CUSTOM WEEK NUMBER:
  Anchored to 2023-01-01 (a Sunday):
    custom_week_num = DATE_DIFF(DATE_SUB(week_sun_to_sat, INTERVAL 6 DAY), DATE '2023-01-01', WEEK)
  LY match: current.custom_week_num - prior.custom_week_num = 52

KEY MODELING NOTES:
  - Source WeekSunSat already follows Sun-to-Sat convention — no conversion needed
  - Derived metrics computed before pivot so NULL propagation is correct
  - Self-joins on small pivoted CTE (1 row per week — very cheap)
  - NULLs preserved — no fake zeroes
  - No ORDER BY — applied in Gold only

DOWNSTREAM:
  Gold Wide : vw_sdi_pulseTms_gold_unified_wide
  Gold Long : vw_sdi_pulseTms_gold_unified_long
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_adobe_weekly`
AS

-- -----------------------------------------------------------------------
-- STEP 1: Extract all metrics from source, compute uvnbByodPctOfUvnbFlow
-- All derived totals (UvnbTrackedFlowSum, CartstartTotal etc.) already
-- computed in the source Silver view — no re-derivation needed here
-- uvnbByodPctOfUvnbFlow computed before pivot for correct NULL propagation
-- -----------------------------------------------------------------------
WITH base AS (
    SELECT
        WeekSunSat                                                      AS week_sun_to_sat,
        ChannelGroup,

        -- UVNB: allChannels-only metrics
        UvnbTotalAdobe,
        UvnbFlowTotal,

        -- UVNB BYOD as % of UVNB Flow Total (allChannels only)
        -- NULL if UvnbFlowTotal is NULL or 0
        CASE
            WHEN UvnbFlowTotal IS NULL OR UvnbFlowTotal = 0 THEN NULL
            ELSE ROUND(UvnbByod / UvnbFlowTotal, 6)
        END                                                             AS uvnbByodPctOfUvnbFlow,

        -- UVNB: all channels
        UvnbPostpaid,
        UvnbHsi,
        UvnbByod,
        UvnbTrackedFlowSum,

        -- CartStart: all channels
        CartstartTotal,
        CartstartPostpaid,
        CartstartHsi,
        CartstartByod,

        -- Orders: all channels
        OrdersTotal,
        OrdersUnassistedTotal,
        OrdersUnassistedPostpaid,
        OrdersUnassistedHsi,
        OrdersUnassistedByod,
        OrdersAssistedTotal,
        OrdersAssistedPostpaid,
        OrdersAssistedHsi,
        OrdersAssistedByod

    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_flowPerformanceByChannelGroupsPlusAll_Weekly`
),

-- -----------------------------------------------------------------------
-- STEP 2: Pivot long → wide
-- One row per week with each ChannelGroup as separate columns
-- allChannels-only metrics: UvnbTotalAdobe, UvnbFlowTotal, uvnbByodPctOfUvnbFlow
-- All other metrics: pivoted across all 7 channels
-- -----------------------------------------------------------------------
pivoted AS (
    SELECT
        week_sun_to_sat,

        -- ==== ALL CHANNELS ONLY ====
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN UvnbTotalAdobe        END) AS adobe_uvnbTotalAdobe_allChannels,
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN UvnbFlowTotal         END) AS adobe_uvnbFlowTotal_allChannels,
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN uvnbByodPctOfUvnbFlow END) AS adobe_uvnbByodPctOfUvnbFlow_allChannels,

        -- ---- adobe_uvnbPostpaid ----
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN UvnbPostpaid END) AS adobe_uvnbPostpaid_allChannels,
        MAX(CASE WHEN ChannelGroup = 'PAID SEARCH' THEN UvnbPostpaid END) AS adobe_uvnbPostpaid_paidSearch,
        MAX(CASE WHEN ChannelGroup = 'ORGANIC SEARCH' THEN UvnbPostpaid END) AS adobe_uvnbPostpaid_organicSearch,
        MAX(CASE WHEN ChannelGroup = 'DIRECT' THEN UvnbPostpaid END) AS adobe_uvnbPostpaid_direct,
        MAX(CASE WHEN ChannelGroup = 'SOCIAL' THEN UvnbPostpaid END) AS adobe_uvnbPostpaid_social,
        MAX(CASE WHEN ChannelGroup = 'PROGRAMMATIC' THEN UvnbPostpaid END) AS adobe_uvnbPostpaid_programmatic,
        MAX(CASE WHEN ChannelGroup = 'OTHER' THEN UvnbPostpaid END) AS adobe_uvnbPostpaid_other,

        -- ---- adobe_uvnbHsi ----
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN UvnbHsi END) AS adobe_uvnbHsi_allChannels,
        MAX(CASE WHEN ChannelGroup = 'PAID SEARCH' THEN UvnbHsi END) AS adobe_uvnbHsi_paidSearch,
        MAX(CASE WHEN ChannelGroup = 'ORGANIC SEARCH' THEN UvnbHsi END) AS adobe_uvnbHsi_organicSearch,
        MAX(CASE WHEN ChannelGroup = 'DIRECT' THEN UvnbHsi END) AS adobe_uvnbHsi_direct,
        MAX(CASE WHEN ChannelGroup = 'SOCIAL' THEN UvnbHsi END) AS adobe_uvnbHsi_social,
        MAX(CASE WHEN ChannelGroup = 'PROGRAMMATIC' THEN UvnbHsi END) AS adobe_uvnbHsi_programmatic,
        MAX(CASE WHEN ChannelGroup = 'OTHER' THEN UvnbHsi END) AS adobe_uvnbHsi_other,

        -- ---- adobe_uvnbByod ----
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN UvnbByod END) AS adobe_uvnbByod_allChannels,
        MAX(CASE WHEN ChannelGroup = 'PAID SEARCH' THEN UvnbByod END) AS adobe_uvnbByod_paidSearch,
        MAX(CASE WHEN ChannelGroup = 'ORGANIC SEARCH' THEN UvnbByod END) AS adobe_uvnbByod_organicSearch,
        MAX(CASE WHEN ChannelGroup = 'DIRECT' THEN UvnbByod END) AS adobe_uvnbByod_direct,
        MAX(CASE WHEN ChannelGroup = 'SOCIAL' THEN UvnbByod END) AS adobe_uvnbByod_social,
        MAX(CASE WHEN ChannelGroup = 'PROGRAMMATIC' THEN UvnbByod END) AS adobe_uvnbByod_programmatic,
        MAX(CASE WHEN ChannelGroup = 'OTHER' THEN UvnbByod END) AS adobe_uvnbByod_other,

        -- ---- adobe_uvnbTrackedFlowSum ----
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN UvnbTrackedFlowSum END) AS adobe_uvnbTrackedFlowSum_allChannels,
        MAX(CASE WHEN ChannelGroup = 'PAID SEARCH' THEN UvnbTrackedFlowSum END) AS adobe_uvnbTrackedFlowSum_paidSearch,
        MAX(CASE WHEN ChannelGroup = 'ORGANIC SEARCH' THEN UvnbTrackedFlowSum END) AS adobe_uvnbTrackedFlowSum_organicSearch,
        MAX(CASE WHEN ChannelGroup = 'DIRECT' THEN UvnbTrackedFlowSum END) AS adobe_uvnbTrackedFlowSum_direct,
        MAX(CASE WHEN ChannelGroup = 'SOCIAL' THEN UvnbTrackedFlowSum END) AS adobe_uvnbTrackedFlowSum_social,
        MAX(CASE WHEN ChannelGroup = 'PROGRAMMATIC' THEN UvnbTrackedFlowSum END) AS adobe_uvnbTrackedFlowSum_programmatic,
        MAX(CASE WHEN ChannelGroup = 'OTHER' THEN UvnbTrackedFlowSum END) AS adobe_uvnbTrackedFlowSum_other,

        -- ---- adobe_cartStartTotal ----
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN CartstartTotal END) AS adobe_cartStartTotal_allChannels,
        MAX(CASE WHEN ChannelGroup = 'PAID SEARCH' THEN CartstartTotal END) AS adobe_cartStartTotal_paidSearch,
        MAX(CASE WHEN ChannelGroup = 'ORGANIC SEARCH' THEN CartstartTotal END) AS adobe_cartStartTotal_organicSearch,
        MAX(CASE WHEN ChannelGroup = 'DIRECT' THEN CartstartTotal END) AS adobe_cartStartTotal_direct,
        MAX(CASE WHEN ChannelGroup = 'SOCIAL' THEN CartstartTotal END) AS adobe_cartStartTotal_social,
        MAX(CASE WHEN ChannelGroup = 'PROGRAMMATIC' THEN CartstartTotal END) AS adobe_cartStartTotal_programmatic,
        MAX(CASE WHEN ChannelGroup = 'OTHER' THEN CartstartTotal END) AS adobe_cartStartTotal_other,

        -- ---- adobe_cartStartPostpaid ----
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN CartstartPostpaid END) AS adobe_cartStartPostpaid_allChannels,
        MAX(CASE WHEN ChannelGroup = 'PAID SEARCH' THEN CartstartPostpaid END) AS adobe_cartStartPostpaid_paidSearch,
        MAX(CASE WHEN ChannelGroup = 'ORGANIC SEARCH' THEN CartstartPostpaid END) AS adobe_cartStartPostpaid_organicSearch,
        MAX(CASE WHEN ChannelGroup = 'DIRECT' THEN CartstartPostpaid END) AS adobe_cartStartPostpaid_direct,
        MAX(CASE WHEN ChannelGroup = 'SOCIAL' THEN CartstartPostpaid END) AS adobe_cartStartPostpaid_social,
        MAX(CASE WHEN ChannelGroup = 'PROGRAMMATIC' THEN CartstartPostpaid END) AS adobe_cartStartPostpaid_programmatic,
        MAX(CASE WHEN ChannelGroup = 'OTHER' THEN CartstartPostpaid END) AS adobe_cartStartPostpaid_other,

        -- ---- adobe_cartStartHsi ----
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN CartstartHsi END) AS adobe_cartStartHsi_allChannels,
        MAX(CASE WHEN ChannelGroup = 'PAID SEARCH' THEN CartstartHsi END) AS adobe_cartStartHsi_paidSearch,
        MAX(CASE WHEN ChannelGroup = 'ORGANIC SEARCH' THEN CartstartHsi END) AS adobe_cartStartHsi_organicSearch,
        MAX(CASE WHEN ChannelGroup = 'DIRECT' THEN CartstartHsi END) AS adobe_cartStartHsi_direct,
        MAX(CASE WHEN ChannelGroup = 'SOCIAL' THEN CartstartHsi END) AS adobe_cartStartHsi_social,
        MAX(CASE WHEN ChannelGroup = 'PROGRAMMATIC' THEN CartstartHsi END) AS adobe_cartStartHsi_programmatic,
        MAX(CASE WHEN ChannelGroup = 'OTHER' THEN CartstartHsi END) AS adobe_cartStartHsi_other,

        -- ---- adobe_cartStartByod ----
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN CartstartByod END) AS adobe_cartStartByod_allChannels,
        MAX(CASE WHEN ChannelGroup = 'PAID SEARCH' THEN CartstartByod END) AS adobe_cartStartByod_paidSearch,
        MAX(CASE WHEN ChannelGroup = 'ORGANIC SEARCH' THEN CartstartByod END) AS adobe_cartStartByod_organicSearch,
        MAX(CASE WHEN ChannelGroup = 'DIRECT' THEN CartstartByod END) AS adobe_cartStartByod_direct,
        MAX(CASE WHEN ChannelGroup = 'SOCIAL' THEN CartstartByod END) AS adobe_cartStartByod_social,
        MAX(CASE WHEN ChannelGroup = 'PROGRAMMATIC' THEN CartstartByod END) AS adobe_cartStartByod_programmatic,
        MAX(CASE WHEN ChannelGroup = 'OTHER' THEN CartstartByod END) AS adobe_cartStartByod_other,

        -- ---- adobe_ordersTotal ----
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN OrdersTotal END) AS adobe_ordersTotal_allChannels,
        MAX(CASE WHEN ChannelGroup = 'PAID SEARCH' THEN OrdersTotal END) AS adobe_ordersTotal_paidSearch,
        MAX(CASE WHEN ChannelGroup = 'ORGANIC SEARCH' THEN OrdersTotal END) AS adobe_ordersTotal_organicSearch,
        MAX(CASE WHEN ChannelGroup = 'DIRECT' THEN OrdersTotal END) AS adobe_ordersTotal_direct,
        MAX(CASE WHEN ChannelGroup = 'SOCIAL' THEN OrdersTotal END) AS adobe_ordersTotal_social,
        MAX(CASE WHEN ChannelGroup = 'PROGRAMMATIC' THEN OrdersTotal END) AS adobe_ordersTotal_programmatic,
        MAX(CASE WHEN ChannelGroup = 'OTHER' THEN OrdersTotal END) AS adobe_ordersTotal_other,

        -- ---- adobe_ordersUnassistedTotal ----
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN OrdersUnassistedTotal END) AS adobe_ordersUnassistedTotal_allChannels,
        MAX(CASE WHEN ChannelGroup = 'PAID SEARCH' THEN OrdersUnassistedTotal END) AS adobe_ordersUnassistedTotal_paidSearch,
        MAX(CASE WHEN ChannelGroup = 'ORGANIC SEARCH' THEN OrdersUnassistedTotal END) AS adobe_ordersUnassistedTotal_organicSearch,
        MAX(CASE WHEN ChannelGroup = 'DIRECT' THEN OrdersUnassistedTotal END) AS adobe_ordersUnassistedTotal_direct,
        MAX(CASE WHEN ChannelGroup = 'SOCIAL' THEN OrdersUnassistedTotal END) AS adobe_ordersUnassistedTotal_social,
        MAX(CASE WHEN ChannelGroup = 'PROGRAMMATIC' THEN OrdersUnassistedTotal END) AS adobe_ordersUnassistedTotal_programmatic,
        MAX(CASE WHEN ChannelGroup = 'OTHER' THEN OrdersUnassistedTotal END) AS adobe_ordersUnassistedTotal_other,

        -- ---- adobe_ordersUnassistedPostpaid ----
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN OrdersUnassistedPostpaid END) AS adobe_ordersUnassistedPostpaid_allChannels,
        MAX(CASE WHEN ChannelGroup = 'PAID SEARCH' THEN OrdersUnassistedPostpaid END) AS adobe_ordersUnassistedPostpaid_paidSearch,
        MAX(CASE WHEN ChannelGroup = 'ORGANIC SEARCH' THEN OrdersUnassistedPostpaid END) AS adobe_ordersUnassistedPostpaid_organicSearch,
        MAX(CASE WHEN ChannelGroup = 'DIRECT' THEN OrdersUnassistedPostpaid END) AS adobe_ordersUnassistedPostpaid_direct,
        MAX(CASE WHEN ChannelGroup = 'SOCIAL' THEN OrdersUnassistedPostpaid END) AS adobe_ordersUnassistedPostpaid_social,
        MAX(CASE WHEN ChannelGroup = 'PROGRAMMATIC' THEN OrdersUnassistedPostpaid END) AS adobe_ordersUnassistedPostpaid_programmatic,
        MAX(CASE WHEN ChannelGroup = 'OTHER' THEN OrdersUnassistedPostpaid END) AS adobe_ordersUnassistedPostpaid_other,

        -- ---- adobe_ordersUnassistedHsi ----
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN OrdersUnassistedHsi END) AS adobe_ordersUnassistedHsi_allChannels,
        MAX(CASE WHEN ChannelGroup = 'PAID SEARCH' THEN OrdersUnassistedHsi END) AS adobe_ordersUnassistedHsi_paidSearch,
        MAX(CASE WHEN ChannelGroup = 'ORGANIC SEARCH' THEN OrdersUnassistedHsi END) AS adobe_ordersUnassistedHsi_organicSearch,
        MAX(CASE WHEN ChannelGroup = 'DIRECT' THEN OrdersUnassistedHsi END) AS adobe_ordersUnassistedHsi_direct,
        MAX(CASE WHEN ChannelGroup = 'SOCIAL' THEN OrdersUnassistedHsi END) AS adobe_ordersUnassistedHsi_social,
        MAX(CASE WHEN ChannelGroup = 'PROGRAMMATIC' THEN OrdersUnassistedHsi END) AS adobe_ordersUnassistedHsi_programmatic,
        MAX(CASE WHEN ChannelGroup = 'OTHER' THEN OrdersUnassistedHsi END) AS adobe_ordersUnassistedHsi_other,

        -- ---- adobe_ordersUnassistedByod ----
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN OrdersUnassistedByod END) AS adobe_ordersUnassistedByod_allChannels,
        MAX(CASE WHEN ChannelGroup = 'PAID SEARCH' THEN OrdersUnassistedByod END) AS adobe_ordersUnassistedByod_paidSearch,
        MAX(CASE WHEN ChannelGroup = 'ORGANIC SEARCH' THEN OrdersUnassistedByod END) AS adobe_ordersUnassistedByod_organicSearch,
        MAX(CASE WHEN ChannelGroup = 'DIRECT' THEN OrdersUnassistedByod END) AS adobe_ordersUnassistedByod_direct,
        MAX(CASE WHEN ChannelGroup = 'SOCIAL' THEN OrdersUnassistedByod END) AS adobe_ordersUnassistedByod_social,
        MAX(CASE WHEN ChannelGroup = 'PROGRAMMATIC' THEN OrdersUnassistedByod END) AS adobe_ordersUnassistedByod_programmatic,
        MAX(CASE WHEN ChannelGroup = 'OTHER' THEN OrdersUnassistedByod END) AS adobe_ordersUnassistedByod_other,

        -- ---- adobe_ordersAssistedTotal ----
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN OrdersAssistedTotal END) AS adobe_ordersAssistedTotal_allChannels,
        MAX(CASE WHEN ChannelGroup = 'PAID SEARCH' THEN OrdersAssistedTotal END) AS adobe_ordersAssistedTotal_paidSearch,
        MAX(CASE WHEN ChannelGroup = 'ORGANIC SEARCH' THEN OrdersAssistedTotal END) AS adobe_ordersAssistedTotal_organicSearch,
        MAX(CASE WHEN ChannelGroup = 'DIRECT' THEN OrdersAssistedTotal END) AS adobe_ordersAssistedTotal_direct,
        MAX(CASE WHEN ChannelGroup = 'SOCIAL' THEN OrdersAssistedTotal END) AS adobe_ordersAssistedTotal_social,
        MAX(CASE WHEN ChannelGroup = 'PROGRAMMATIC' THEN OrdersAssistedTotal END) AS adobe_ordersAssistedTotal_programmatic,
        MAX(CASE WHEN ChannelGroup = 'OTHER' THEN OrdersAssistedTotal END) AS adobe_ordersAssistedTotal_other,

        -- ---- adobe_ordersAssistedPostpaid ----
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN OrdersAssistedPostpaid END) AS adobe_ordersAssistedPostpaid_allChannels,
        MAX(CASE WHEN ChannelGroup = 'PAID SEARCH' THEN OrdersAssistedPostpaid END) AS adobe_ordersAssistedPostpaid_paidSearch,
        MAX(CASE WHEN ChannelGroup = 'ORGANIC SEARCH' THEN OrdersAssistedPostpaid END) AS adobe_ordersAssistedPostpaid_organicSearch,
        MAX(CASE WHEN ChannelGroup = 'DIRECT' THEN OrdersAssistedPostpaid END) AS adobe_ordersAssistedPostpaid_direct,
        MAX(CASE WHEN ChannelGroup = 'SOCIAL' THEN OrdersAssistedPostpaid END) AS adobe_ordersAssistedPostpaid_social,
        MAX(CASE WHEN ChannelGroup = 'PROGRAMMATIC' THEN OrdersAssistedPostpaid END) AS adobe_ordersAssistedPostpaid_programmatic,
        MAX(CASE WHEN ChannelGroup = 'OTHER' THEN OrdersAssistedPostpaid END) AS adobe_ordersAssistedPostpaid_other,

        -- ---- adobe_ordersAssistedHsi ----
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN OrdersAssistedHsi END) AS adobe_ordersAssistedHsi_allChannels,
        MAX(CASE WHEN ChannelGroup = 'PAID SEARCH' THEN OrdersAssistedHsi END) AS adobe_ordersAssistedHsi_paidSearch,
        MAX(CASE WHEN ChannelGroup = 'ORGANIC SEARCH' THEN OrdersAssistedHsi END) AS adobe_ordersAssistedHsi_organicSearch,
        MAX(CASE WHEN ChannelGroup = 'DIRECT' THEN OrdersAssistedHsi END) AS adobe_ordersAssistedHsi_direct,
        MAX(CASE WHEN ChannelGroup = 'SOCIAL' THEN OrdersAssistedHsi END) AS adobe_ordersAssistedHsi_social,
        MAX(CASE WHEN ChannelGroup = 'PROGRAMMATIC' THEN OrdersAssistedHsi END) AS adobe_ordersAssistedHsi_programmatic,
        MAX(CASE WHEN ChannelGroup = 'OTHER' THEN OrdersAssistedHsi END) AS adobe_ordersAssistedHsi_other,

        -- ---- adobe_ordersAssistedByod ----
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN OrdersAssistedByod END) AS adobe_ordersAssistedByod_allChannels,
        MAX(CASE WHEN ChannelGroup = 'PAID SEARCH' THEN OrdersAssistedByod END) AS adobe_ordersAssistedByod_paidSearch,
        MAX(CASE WHEN ChannelGroup = 'ORGANIC SEARCH' THEN OrdersAssistedByod END) AS adobe_ordersAssistedByod_organicSearch,
        MAX(CASE WHEN ChannelGroup = 'DIRECT' THEN OrdersAssistedByod END) AS adobe_ordersAssistedByod_direct,
        MAX(CASE WHEN ChannelGroup = 'SOCIAL' THEN OrdersAssistedByod END) AS adobe_ordersAssistedByod_social,
        MAX(CASE WHEN ChannelGroup = 'PROGRAMMATIC' THEN OrdersAssistedByod END) AS adobe_ordersAssistedByod_programmatic,
        MAX(CASE WHEN ChannelGroup = 'OTHER' THEN OrdersAssistedByod END) AS adobe_ordersAssistedByod_other

    FROM base
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
        )                                                               AS custom_week_num
    FROM pivoted
),

-- -----------------------------------------------------------------------
-- STEP 4: WoW and LY self-joins
-- -----------------------------------------------------------------------
with_comparisons AS (
    SELECT
        c.week_sun_to_sat,
        c.custom_week_num,

        -- adobe_uvnbTotalAdobe_allChannels
        c.adobe_uvnbTotalAdobe_allChannels,
        w.adobe_uvnbTotalAdobe_allChannels                AS adobe_uvnbTotalAdobe_allChannels_wow,
        l.adobe_uvnbTotalAdobe_allChannels                AS adobe_uvnbTotalAdobe_allChannels_ly,

        -- adobe_uvnbFlowTotal_allChannels
        c.adobe_uvnbFlowTotal_allChannels,
        w.adobe_uvnbFlowTotal_allChannels                AS adobe_uvnbFlowTotal_allChannels_wow,
        l.adobe_uvnbFlowTotal_allChannels                AS adobe_uvnbFlowTotal_allChannels_ly,

        -- adobe_uvnbByodPctOfUvnbFlow_allChannels
        c.adobe_uvnbByodPctOfUvnbFlow_allChannels,
        w.adobe_uvnbByodPctOfUvnbFlow_allChannels                AS adobe_uvnbByodPctOfUvnbFlow_allChannels_wow,
        l.adobe_uvnbByodPctOfUvnbFlow_allChannels                AS adobe_uvnbByodPctOfUvnbFlow_allChannels_ly,

        -- adobe_uvnbPostpaid_allChannels
        c.adobe_uvnbPostpaid_allChannels,
        w.adobe_uvnbPostpaid_allChannels                AS adobe_uvnbPostpaid_allChannels_wow,
        l.adobe_uvnbPostpaid_allChannels                AS adobe_uvnbPostpaid_allChannels_ly,

        -- adobe_uvnbPostpaid_paidSearch
        c.adobe_uvnbPostpaid_paidSearch,
        w.adobe_uvnbPostpaid_paidSearch                AS adobe_uvnbPostpaid_paidSearch_wow,
        l.adobe_uvnbPostpaid_paidSearch                AS adobe_uvnbPostpaid_paidSearch_ly,

        -- adobe_uvnbPostpaid_organicSearch
        c.adobe_uvnbPostpaid_organicSearch,
        w.adobe_uvnbPostpaid_organicSearch                AS adobe_uvnbPostpaid_organicSearch_wow,
        l.adobe_uvnbPostpaid_organicSearch                AS adobe_uvnbPostpaid_organicSearch_ly,

        -- adobe_uvnbPostpaid_direct
        c.adobe_uvnbPostpaid_direct,
        w.adobe_uvnbPostpaid_direct                AS adobe_uvnbPostpaid_direct_wow,
        l.adobe_uvnbPostpaid_direct                AS adobe_uvnbPostpaid_direct_ly,

        -- adobe_uvnbPostpaid_social
        c.adobe_uvnbPostpaid_social,
        w.adobe_uvnbPostpaid_social                AS adobe_uvnbPostpaid_social_wow,
        l.adobe_uvnbPostpaid_social                AS adobe_uvnbPostpaid_social_ly,

        -- adobe_uvnbPostpaid_programmatic
        c.adobe_uvnbPostpaid_programmatic,
        w.adobe_uvnbPostpaid_programmatic                AS adobe_uvnbPostpaid_programmatic_wow,
        l.adobe_uvnbPostpaid_programmatic                AS adobe_uvnbPostpaid_programmatic_ly,

        -- adobe_uvnbPostpaid_other
        c.adobe_uvnbPostpaid_other,
        w.adobe_uvnbPostpaid_other                AS adobe_uvnbPostpaid_other_wow,
        l.adobe_uvnbPostpaid_other                AS adobe_uvnbPostpaid_other_ly,

        -- adobe_uvnbHsi_allChannels
        c.adobe_uvnbHsi_allChannels,
        w.adobe_uvnbHsi_allChannels                AS adobe_uvnbHsi_allChannels_wow,
        l.adobe_uvnbHsi_allChannels                AS adobe_uvnbHsi_allChannels_ly,

        -- adobe_uvnbHsi_paidSearch
        c.adobe_uvnbHsi_paidSearch,
        w.adobe_uvnbHsi_paidSearch                AS adobe_uvnbHsi_paidSearch_wow,
        l.adobe_uvnbHsi_paidSearch                AS adobe_uvnbHsi_paidSearch_ly,

        -- adobe_uvnbHsi_organicSearch
        c.adobe_uvnbHsi_organicSearch,
        w.adobe_uvnbHsi_organicSearch                AS adobe_uvnbHsi_organicSearch_wow,
        l.adobe_uvnbHsi_organicSearch                AS adobe_uvnbHsi_organicSearch_ly,

        -- adobe_uvnbHsi_direct
        c.adobe_uvnbHsi_direct,
        w.adobe_uvnbHsi_direct                AS adobe_uvnbHsi_direct_wow,
        l.adobe_uvnbHsi_direct                AS adobe_uvnbHsi_direct_ly,

        -- adobe_uvnbHsi_social
        c.adobe_uvnbHsi_social,
        w.adobe_uvnbHsi_social                AS adobe_uvnbHsi_social_wow,
        l.adobe_uvnbHsi_social                AS adobe_uvnbHsi_social_ly,

        -- adobe_uvnbHsi_programmatic
        c.adobe_uvnbHsi_programmatic,
        w.adobe_uvnbHsi_programmatic                AS adobe_uvnbHsi_programmatic_wow,
        l.adobe_uvnbHsi_programmatic                AS adobe_uvnbHsi_programmatic_ly,

        -- adobe_uvnbHsi_other
        c.adobe_uvnbHsi_other,
        w.adobe_uvnbHsi_other                AS adobe_uvnbHsi_other_wow,
        l.adobe_uvnbHsi_other                AS adobe_uvnbHsi_other_ly,

        -- adobe_uvnbByod_allChannels
        c.adobe_uvnbByod_allChannels,
        w.adobe_uvnbByod_allChannels                AS adobe_uvnbByod_allChannels_wow,
        l.adobe_uvnbByod_allChannels                AS adobe_uvnbByod_allChannels_ly,

        -- adobe_uvnbByod_paidSearch
        c.adobe_uvnbByod_paidSearch,
        w.adobe_uvnbByod_paidSearch                AS adobe_uvnbByod_paidSearch_wow,
        l.adobe_uvnbByod_paidSearch                AS adobe_uvnbByod_paidSearch_ly,

        -- adobe_uvnbByod_organicSearch
        c.adobe_uvnbByod_organicSearch,
        w.adobe_uvnbByod_organicSearch                AS adobe_uvnbByod_organicSearch_wow,
        l.adobe_uvnbByod_organicSearch                AS adobe_uvnbByod_organicSearch_ly,

        -- adobe_uvnbByod_direct
        c.adobe_uvnbByod_direct,
        w.adobe_uvnbByod_direct                AS adobe_uvnbByod_direct_wow,
        l.adobe_uvnbByod_direct                AS adobe_uvnbByod_direct_ly,

        -- adobe_uvnbByod_social
        c.adobe_uvnbByod_social,
        w.adobe_uvnbByod_social                AS adobe_uvnbByod_social_wow,
        l.adobe_uvnbByod_social                AS adobe_uvnbByod_social_ly,

        -- adobe_uvnbByod_programmatic
        c.adobe_uvnbByod_programmatic,
        w.adobe_uvnbByod_programmatic                AS adobe_uvnbByod_programmatic_wow,
        l.adobe_uvnbByod_programmatic                AS adobe_uvnbByod_programmatic_ly,

        -- adobe_uvnbByod_other
        c.adobe_uvnbByod_other,
        w.adobe_uvnbByod_other                AS adobe_uvnbByod_other_wow,
        l.adobe_uvnbByod_other                AS adobe_uvnbByod_other_ly,

        -- adobe_uvnbTrackedFlowSum_allChannels
        c.adobe_uvnbTrackedFlowSum_allChannels,
        w.adobe_uvnbTrackedFlowSum_allChannels                AS adobe_uvnbTrackedFlowSum_allChannels_wow,
        l.adobe_uvnbTrackedFlowSum_allChannels                AS adobe_uvnbTrackedFlowSum_allChannels_ly,

        -- adobe_uvnbTrackedFlowSum_paidSearch
        c.adobe_uvnbTrackedFlowSum_paidSearch,
        w.adobe_uvnbTrackedFlowSum_paidSearch                AS adobe_uvnbTrackedFlowSum_paidSearch_wow,
        l.adobe_uvnbTrackedFlowSum_paidSearch                AS adobe_uvnbTrackedFlowSum_paidSearch_ly,

        -- adobe_uvnbTrackedFlowSum_organicSearch
        c.adobe_uvnbTrackedFlowSum_organicSearch,
        w.adobe_uvnbTrackedFlowSum_organicSearch                AS adobe_uvnbTrackedFlowSum_organicSearch_wow,
        l.adobe_uvnbTrackedFlowSum_organicSearch                AS adobe_uvnbTrackedFlowSum_organicSearch_ly,

        -- adobe_uvnbTrackedFlowSum_direct
        c.adobe_uvnbTrackedFlowSum_direct,
        w.adobe_uvnbTrackedFlowSum_direct                AS adobe_uvnbTrackedFlowSum_direct_wow,
        l.adobe_uvnbTrackedFlowSum_direct                AS adobe_uvnbTrackedFlowSum_direct_ly,

        -- adobe_uvnbTrackedFlowSum_social
        c.adobe_uvnbTrackedFlowSum_social,
        w.adobe_uvnbTrackedFlowSum_social                AS adobe_uvnbTrackedFlowSum_social_wow,
        l.adobe_uvnbTrackedFlowSum_social                AS adobe_uvnbTrackedFlowSum_social_ly,

        -- adobe_uvnbTrackedFlowSum_programmatic
        c.adobe_uvnbTrackedFlowSum_programmatic,
        w.adobe_uvnbTrackedFlowSum_programmatic                AS adobe_uvnbTrackedFlowSum_programmatic_wow,
        l.adobe_uvnbTrackedFlowSum_programmatic                AS adobe_uvnbTrackedFlowSum_programmatic_ly,

        -- adobe_uvnbTrackedFlowSum_other
        c.adobe_uvnbTrackedFlowSum_other,
        w.adobe_uvnbTrackedFlowSum_other                AS adobe_uvnbTrackedFlowSum_other_wow,
        l.adobe_uvnbTrackedFlowSum_other                AS adobe_uvnbTrackedFlowSum_other_ly,

        -- adobe_cartStartTotal_allChannels
        c.adobe_cartStartTotal_allChannels,
        w.adobe_cartStartTotal_allChannels                AS adobe_cartStartTotal_allChannels_wow,
        l.adobe_cartStartTotal_allChannels                AS adobe_cartStartTotal_allChannels_ly,

        -- adobe_cartStartTotal_paidSearch
        c.adobe_cartStartTotal_paidSearch,
        w.adobe_cartStartTotal_paidSearch                AS adobe_cartStartTotal_paidSearch_wow,
        l.adobe_cartStartTotal_paidSearch                AS adobe_cartStartTotal_paidSearch_ly,

        -- adobe_cartStartTotal_organicSearch
        c.adobe_cartStartTotal_organicSearch,
        w.adobe_cartStartTotal_organicSearch                AS adobe_cartStartTotal_organicSearch_wow,
        l.adobe_cartStartTotal_organicSearch                AS adobe_cartStartTotal_organicSearch_ly,

        -- adobe_cartStartTotal_direct
        c.adobe_cartStartTotal_direct,
        w.adobe_cartStartTotal_direct                AS adobe_cartStartTotal_direct_wow,
        l.adobe_cartStartTotal_direct                AS adobe_cartStartTotal_direct_ly,

        -- adobe_cartStartTotal_social
        c.adobe_cartStartTotal_social,
        w.adobe_cartStartTotal_social                AS adobe_cartStartTotal_social_wow,
        l.adobe_cartStartTotal_social                AS adobe_cartStartTotal_social_ly,

        -- adobe_cartStartTotal_programmatic
        c.adobe_cartStartTotal_programmatic,
        w.adobe_cartStartTotal_programmatic                AS adobe_cartStartTotal_programmatic_wow,
        l.adobe_cartStartTotal_programmatic                AS adobe_cartStartTotal_programmatic_ly,

        -- adobe_cartStartTotal_other
        c.adobe_cartStartTotal_other,
        w.adobe_cartStartTotal_other                AS adobe_cartStartTotal_other_wow,
        l.adobe_cartStartTotal_other                AS adobe_cartStartTotal_other_ly,

        -- adobe_cartStartPostpaid_allChannels
        c.adobe_cartStartPostpaid_allChannels,
        w.adobe_cartStartPostpaid_allChannels                AS adobe_cartStartPostpaid_allChannels_wow,
        l.adobe_cartStartPostpaid_allChannels                AS adobe_cartStartPostpaid_allChannels_ly,

        -- adobe_cartStartPostpaid_paidSearch
        c.adobe_cartStartPostpaid_paidSearch,
        w.adobe_cartStartPostpaid_paidSearch                AS adobe_cartStartPostpaid_paidSearch_wow,
        l.adobe_cartStartPostpaid_paidSearch                AS adobe_cartStartPostpaid_paidSearch_ly,

        -- adobe_cartStartPostpaid_organicSearch
        c.adobe_cartStartPostpaid_organicSearch,
        w.adobe_cartStartPostpaid_organicSearch                AS adobe_cartStartPostpaid_organicSearch_wow,
        l.adobe_cartStartPostpaid_organicSearch                AS adobe_cartStartPostpaid_organicSearch_ly,

        -- adobe_cartStartPostpaid_direct
        c.adobe_cartStartPostpaid_direct,
        w.adobe_cartStartPostpaid_direct                AS adobe_cartStartPostpaid_direct_wow,
        l.adobe_cartStartPostpaid_direct                AS adobe_cartStartPostpaid_direct_ly,

        -- adobe_cartStartPostpaid_social
        c.adobe_cartStartPostpaid_social,
        w.adobe_cartStartPostpaid_social                AS adobe_cartStartPostpaid_social_wow,
        l.adobe_cartStartPostpaid_social                AS adobe_cartStartPostpaid_social_ly,

        -- adobe_cartStartPostpaid_programmatic
        c.adobe_cartStartPostpaid_programmatic,
        w.adobe_cartStartPostpaid_programmatic                AS adobe_cartStartPostpaid_programmatic_wow,
        l.adobe_cartStartPostpaid_programmatic                AS adobe_cartStartPostpaid_programmatic_ly,

        -- adobe_cartStartPostpaid_other
        c.adobe_cartStartPostpaid_other,
        w.adobe_cartStartPostpaid_other                AS adobe_cartStartPostpaid_other_wow,
        l.adobe_cartStartPostpaid_other                AS adobe_cartStartPostpaid_other_ly,

        -- adobe_cartStartHsi_allChannels
        c.adobe_cartStartHsi_allChannels,
        w.adobe_cartStartHsi_allChannels                AS adobe_cartStartHsi_allChannels_wow,
        l.adobe_cartStartHsi_allChannels                AS adobe_cartStartHsi_allChannels_ly,

        -- adobe_cartStartHsi_paidSearch
        c.adobe_cartStartHsi_paidSearch,
        w.adobe_cartStartHsi_paidSearch                AS adobe_cartStartHsi_paidSearch_wow,
        l.adobe_cartStartHsi_paidSearch                AS adobe_cartStartHsi_paidSearch_ly,

        -- adobe_cartStartHsi_organicSearch
        c.adobe_cartStartHsi_organicSearch,
        w.adobe_cartStartHsi_organicSearch                AS adobe_cartStartHsi_organicSearch_wow,
        l.adobe_cartStartHsi_organicSearch                AS adobe_cartStartHsi_organicSearch_ly,

        -- adobe_cartStartHsi_direct
        c.adobe_cartStartHsi_direct,
        w.adobe_cartStartHsi_direct                AS adobe_cartStartHsi_direct_wow,
        l.adobe_cartStartHsi_direct                AS adobe_cartStartHsi_direct_ly,

        -- adobe_cartStartHsi_social
        c.adobe_cartStartHsi_social,
        w.adobe_cartStartHsi_social                AS adobe_cartStartHsi_social_wow,
        l.adobe_cartStartHsi_social                AS adobe_cartStartHsi_social_ly,

        -- adobe_cartStartHsi_programmatic
        c.adobe_cartStartHsi_programmatic,
        w.adobe_cartStartHsi_programmatic                AS adobe_cartStartHsi_programmatic_wow,
        l.adobe_cartStartHsi_programmatic                AS adobe_cartStartHsi_programmatic_ly,

        -- adobe_cartStartHsi_other
        c.adobe_cartStartHsi_other,
        w.adobe_cartStartHsi_other                AS adobe_cartStartHsi_other_wow,
        l.adobe_cartStartHsi_other                AS adobe_cartStartHsi_other_ly,

        -- adobe_cartStartByod_allChannels
        c.adobe_cartStartByod_allChannels,
        w.adobe_cartStartByod_allChannels                AS adobe_cartStartByod_allChannels_wow,
        l.adobe_cartStartByod_allChannels                AS adobe_cartStartByod_allChannels_ly,

        -- adobe_cartStartByod_paidSearch
        c.adobe_cartStartByod_paidSearch,
        w.adobe_cartStartByod_paidSearch                AS adobe_cartStartByod_paidSearch_wow,
        l.adobe_cartStartByod_paidSearch                AS adobe_cartStartByod_paidSearch_ly,

        -- adobe_cartStartByod_organicSearch
        c.adobe_cartStartByod_organicSearch,
        w.adobe_cartStartByod_organicSearch                AS adobe_cartStartByod_organicSearch_wow,
        l.adobe_cartStartByod_organicSearch                AS adobe_cartStartByod_organicSearch_ly,

        -- adobe_cartStartByod_direct
        c.adobe_cartStartByod_direct,
        w.adobe_cartStartByod_direct                AS adobe_cartStartByod_direct_wow,
        l.adobe_cartStartByod_direct                AS adobe_cartStartByod_direct_ly,

        -- adobe_cartStartByod_social
        c.adobe_cartStartByod_social,
        w.adobe_cartStartByod_social                AS adobe_cartStartByod_social_wow,
        l.adobe_cartStartByod_social                AS adobe_cartStartByod_social_ly,

        -- adobe_cartStartByod_programmatic
        c.adobe_cartStartByod_programmatic,
        w.adobe_cartStartByod_programmatic                AS adobe_cartStartByod_programmatic_wow,
        l.adobe_cartStartByod_programmatic                AS adobe_cartStartByod_programmatic_ly,

        -- adobe_cartStartByod_other
        c.adobe_cartStartByod_other,
        w.adobe_cartStartByod_other                AS adobe_cartStartByod_other_wow,
        l.adobe_cartStartByod_other                AS adobe_cartStartByod_other_ly,

        -- adobe_ordersTotal_allChannels
        c.adobe_ordersTotal_allChannels,
        w.adobe_ordersTotal_allChannels                AS adobe_ordersTotal_allChannels_wow,
        l.adobe_ordersTotal_allChannels                AS adobe_ordersTotal_allChannels_ly,

        -- adobe_ordersTotal_paidSearch
        c.adobe_ordersTotal_paidSearch,
        w.adobe_ordersTotal_paidSearch                AS adobe_ordersTotal_paidSearch_wow,
        l.adobe_ordersTotal_paidSearch                AS adobe_ordersTotal_paidSearch_ly,

        -- adobe_ordersTotal_organicSearch
        c.adobe_ordersTotal_organicSearch,
        w.adobe_ordersTotal_organicSearch                AS adobe_ordersTotal_organicSearch_wow,
        l.adobe_ordersTotal_organicSearch                AS adobe_ordersTotal_organicSearch_ly,

        -- adobe_ordersTotal_direct
        c.adobe_ordersTotal_direct,
        w.adobe_ordersTotal_direct                AS adobe_ordersTotal_direct_wow,
        l.adobe_ordersTotal_direct                AS adobe_ordersTotal_direct_ly,

        -- adobe_ordersTotal_social
        c.adobe_ordersTotal_social,
        w.adobe_ordersTotal_social                AS adobe_ordersTotal_social_wow,
        l.adobe_ordersTotal_social                AS adobe_ordersTotal_social_ly,

        -- adobe_ordersTotal_programmatic
        c.adobe_ordersTotal_programmatic,
        w.adobe_ordersTotal_programmatic                AS adobe_ordersTotal_programmatic_wow,
        l.adobe_ordersTotal_programmatic                AS adobe_ordersTotal_programmatic_ly,

        -- adobe_ordersTotal_other
        c.adobe_ordersTotal_other,
        w.adobe_ordersTotal_other                AS adobe_ordersTotal_other_wow,
        l.adobe_ordersTotal_other                AS adobe_ordersTotal_other_ly,

        -- adobe_ordersUnassistedTotal_allChannels
        c.adobe_ordersUnassistedTotal_allChannels,
        w.adobe_ordersUnassistedTotal_allChannels                AS adobe_ordersUnassistedTotal_allChannels_wow,
        l.adobe_ordersUnassistedTotal_allChannels                AS adobe_ordersUnassistedTotal_allChannels_ly,

        -- adobe_ordersUnassistedTotal_paidSearch
        c.adobe_ordersUnassistedTotal_paidSearch,
        w.adobe_ordersUnassistedTotal_paidSearch                AS adobe_ordersUnassistedTotal_paidSearch_wow,
        l.adobe_ordersUnassistedTotal_paidSearch                AS adobe_ordersUnassistedTotal_paidSearch_ly,

        -- adobe_ordersUnassistedTotal_organicSearch
        c.adobe_ordersUnassistedTotal_organicSearch,
        w.adobe_ordersUnassistedTotal_organicSearch                AS adobe_ordersUnassistedTotal_organicSearch_wow,
        l.adobe_ordersUnassistedTotal_organicSearch                AS adobe_ordersUnassistedTotal_organicSearch_ly,

        -- adobe_ordersUnassistedTotal_direct
        c.adobe_ordersUnassistedTotal_direct,
        w.adobe_ordersUnassistedTotal_direct                AS adobe_ordersUnassistedTotal_direct_wow,
        l.adobe_ordersUnassistedTotal_direct                AS adobe_ordersUnassistedTotal_direct_ly,

        -- adobe_ordersUnassistedTotal_social
        c.adobe_ordersUnassistedTotal_social,
        w.adobe_ordersUnassistedTotal_social                AS adobe_ordersUnassistedTotal_social_wow,
        l.adobe_ordersUnassistedTotal_social                AS adobe_ordersUnassistedTotal_social_ly,

        -- adobe_ordersUnassistedTotal_programmatic
        c.adobe_ordersUnassistedTotal_programmatic,
        w.adobe_ordersUnassistedTotal_programmatic                AS adobe_ordersUnassistedTotal_programmatic_wow,
        l.adobe_ordersUnassistedTotal_programmatic                AS adobe_ordersUnassistedTotal_programmatic_ly,

        -- adobe_ordersUnassistedTotal_other
        c.adobe_ordersUnassistedTotal_other,
        w.adobe_ordersUnassistedTotal_other                AS adobe_ordersUnassistedTotal_other_wow,
        l.adobe_ordersUnassistedTotal_other                AS adobe_ordersUnassistedTotal_other_ly,

        -- adobe_ordersUnassistedPostpaid_allChannels
        c.adobe_ordersUnassistedPostpaid_allChannels,
        w.adobe_ordersUnassistedPostpaid_allChannels                AS adobe_ordersUnassistedPostpaid_allChannels_wow,
        l.adobe_ordersUnassistedPostpaid_allChannels                AS adobe_ordersUnassistedPostpaid_allChannels_ly,

        -- adobe_ordersUnassistedPostpaid_paidSearch
        c.adobe_ordersUnassistedPostpaid_paidSearch,
        w.adobe_ordersUnassistedPostpaid_paidSearch                AS adobe_ordersUnassistedPostpaid_paidSearch_wow,
        l.adobe_ordersUnassistedPostpaid_paidSearch                AS adobe_ordersUnassistedPostpaid_paidSearch_ly,

        -- adobe_ordersUnassistedPostpaid_organicSearch
        c.adobe_ordersUnassistedPostpaid_organicSearch,
        w.adobe_ordersUnassistedPostpaid_organicSearch                AS adobe_ordersUnassistedPostpaid_organicSearch_wow,
        l.adobe_ordersUnassistedPostpaid_organicSearch                AS adobe_ordersUnassistedPostpaid_organicSearch_ly,

        -- adobe_ordersUnassistedPostpaid_direct
        c.adobe_ordersUnassistedPostpaid_direct,
        w.adobe_ordersUnassistedPostpaid_direct                AS adobe_ordersUnassistedPostpaid_direct_wow,
        l.adobe_ordersUnassistedPostpaid_direct                AS adobe_ordersUnassistedPostpaid_direct_ly,

        -- adobe_ordersUnassistedPostpaid_social
        c.adobe_ordersUnassistedPostpaid_social,
        w.adobe_ordersUnassistedPostpaid_social                AS adobe_ordersUnassistedPostpaid_social_wow,
        l.adobe_ordersUnassistedPostpaid_social                AS adobe_ordersUnassistedPostpaid_social_ly,

        -- adobe_ordersUnassistedPostpaid_programmatic
        c.adobe_ordersUnassistedPostpaid_programmatic,
        w.adobe_ordersUnassistedPostpaid_programmatic                AS adobe_ordersUnassistedPostpaid_programmatic_wow,
        l.adobe_ordersUnassistedPostpaid_programmatic                AS adobe_ordersUnassistedPostpaid_programmatic_ly,

        -- adobe_ordersUnassistedPostpaid_other
        c.adobe_ordersUnassistedPostpaid_other,
        w.adobe_ordersUnassistedPostpaid_other                AS adobe_ordersUnassistedPostpaid_other_wow,
        l.adobe_ordersUnassistedPostpaid_other                AS adobe_ordersUnassistedPostpaid_other_ly,

        -- adobe_ordersUnassistedHsi_allChannels
        c.adobe_ordersUnassistedHsi_allChannels,
        w.adobe_ordersUnassistedHsi_allChannels                AS adobe_ordersUnassistedHsi_allChannels_wow,
        l.adobe_ordersUnassistedHsi_allChannels                AS adobe_ordersUnassistedHsi_allChannels_ly,

        -- adobe_ordersUnassistedHsi_paidSearch
        c.adobe_ordersUnassistedHsi_paidSearch,
        w.adobe_ordersUnassistedHsi_paidSearch                AS adobe_ordersUnassistedHsi_paidSearch_wow,
        l.adobe_ordersUnassistedHsi_paidSearch                AS adobe_ordersUnassistedHsi_paidSearch_ly,

        -- adobe_ordersUnassistedHsi_organicSearch
        c.adobe_ordersUnassistedHsi_organicSearch,
        w.adobe_ordersUnassistedHsi_organicSearch                AS adobe_ordersUnassistedHsi_organicSearch_wow,
        l.adobe_ordersUnassistedHsi_organicSearch                AS adobe_ordersUnassistedHsi_organicSearch_ly,

        -- adobe_ordersUnassistedHsi_direct
        c.adobe_ordersUnassistedHsi_direct,
        w.adobe_ordersUnassistedHsi_direct                AS adobe_ordersUnassistedHsi_direct_wow,
        l.adobe_ordersUnassistedHsi_direct                AS adobe_ordersUnassistedHsi_direct_ly,

        -- adobe_ordersUnassistedHsi_social
        c.adobe_ordersUnassistedHsi_social,
        w.adobe_ordersUnassistedHsi_social                AS adobe_ordersUnassistedHsi_social_wow,
        l.adobe_ordersUnassistedHsi_social                AS adobe_ordersUnassistedHsi_social_ly,

        -- adobe_ordersUnassistedHsi_programmatic
        c.adobe_ordersUnassistedHsi_programmatic,
        w.adobe_ordersUnassistedHsi_programmatic                AS adobe_ordersUnassistedHsi_programmatic_wow,
        l.adobe_ordersUnassistedHsi_programmatic                AS adobe_ordersUnassistedHsi_programmatic_ly,

        -- adobe_ordersUnassistedHsi_other
        c.adobe_ordersUnassistedHsi_other,
        w.adobe_ordersUnassistedHsi_other                AS adobe_ordersUnassistedHsi_other_wow,
        l.adobe_ordersUnassistedHsi_other                AS adobe_ordersUnassistedHsi_other_ly,

        -- adobe_ordersUnassistedByod_allChannels
        c.adobe_ordersUnassistedByod_allChannels,
        w.adobe_ordersUnassistedByod_allChannels                AS adobe_ordersUnassistedByod_allChannels_wow,
        l.adobe_ordersUnassistedByod_allChannels                AS adobe_ordersUnassistedByod_allChannels_ly,

        -- adobe_ordersUnassistedByod_paidSearch
        c.adobe_ordersUnassistedByod_paidSearch,
        w.adobe_ordersUnassistedByod_paidSearch                AS adobe_ordersUnassistedByod_paidSearch_wow,
        l.adobe_ordersUnassistedByod_paidSearch                AS adobe_ordersUnassistedByod_paidSearch_ly,

        -- adobe_ordersUnassistedByod_organicSearch
        c.adobe_ordersUnassistedByod_organicSearch,
        w.adobe_ordersUnassistedByod_organicSearch                AS adobe_ordersUnassistedByod_organicSearch_wow,
        l.adobe_ordersUnassistedByod_organicSearch                AS adobe_ordersUnassistedByod_organicSearch_ly,

        -- adobe_ordersUnassistedByod_direct
        c.adobe_ordersUnassistedByod_direct,
        w.adobe_ordersUnassistedByod_direct                AS adobe_ordersUnassistedByod_direct_wow,
        l.adobe_ordersUnassistedByod_direct                AS adobe_ordersUnassistedByod_direct_ly,

        -- adobe_ordersUnassistedByod_social
        c.adobe_ordersUnassistedByod_social,
        w.adobe_ordersUnassistedByod_social                AS adobe_ordersUnassistedByod_social_wow,
        l.adobe_ordersUnassistedByod_social                AS adobe_ordersUnassistedByod_social_ly,

        -- adobe_ordersUnassistedByod_programmatic
        c.adobe_ordersUnassistedByod_programmatic,
        w.adobe_ordersUnassistedByod_programmatic                AS adobe_ordersUnassistedByod_programmatic_wow,
        l.adobe_ordersUnassistedByod_programmatic                AS adobe_ordersUnassistedByod_programmatic_ly,

        -- adobe_ordersUnassistedByod_other
        c.adobe_ordersUnassistedByod_other,
        w.adobe_ordersUnassistedByod_other                AS adobe_ordersUnassistedByod_other_wow,
        l.adobe_ordersUnassistedByod_other                AS adobe_ordersUnassistedByod_other_ly,

        -- adobe_ordersAssistedTotal_allChannels
        c.adobe_ordersAssistedTotal_allChannels,
        w.adobe_ordersAssistedTotal_allChannels                AS adobe_ordersAssistedTotal_allChannels_wow,
        l.adobe_ordersAssistedTotal_allChannels                AS adobe_ordersAssistedTotal_allChannels_ly,

        -- adobe_ordersAssistedTotal_paidSearch
        c.adobe_ordersAssistedTotal_paidSearch,
        w.adobe_ordersAssistedTotal_paidSearch                AS adobe_ordersAssistedTotal_paidSearch_wow,
        l.adobe_ordersAssistedTotal_paidSearch                AS adobe_ordersAssistedTotal_paidSearch_ly,

        -- adobe_ordersAssistedTotal_organicSearch
        c.adobe_ordersAssistedTotal_organicSearch,
        w.adobe_ordersAssistedTotal_organicSearch                AS adobe_ordersAssistedTotal_organicSearch_wow,
        l.adobe_ordersAssistedTotal_organicSearch                AS adobe_ordersAssistedTotal_organicSearch_ly,

        -- adobe_ordersAssistedTotal_direct
        c.adobe_ordersAssistedTotal_direct,
        w.adobe_ordersAssistedTotal_direct                AS adobe_ordersAssistedTotal_direct_wow,
        l.adobe_ordersAssistedTotal_direct                AS adobe_ordersAssistedTotal_direct_ly,

        -- adobe_ordersAssistedTotal_social
        c.adobe_ordersAssistedTotal_social,
        w.adobe_ordersAssistedTotal_social                AS adobe_ordersAssistedTotal_social_wow,
        l.adobe_ordersAssistedTotal_social                AS adobe_ordersAssistedTotal_social_ly,

        -- adobe_ordersAssistedTotal_programmatic
        c.adobe_ordersAssistedTotal_programmatic,
        w.adobe_ordersAssistedTotal_programmatic                AS adobe_ordersAssistedTotal_programmatic_wow,
        l.adobe_ordersAssistedTotal_programmatic                AS adobe_ordersAssistedTotal_programmatic_ly,

        -- adobe_ordersAssistedTotal_other
        c.adobe_ordersAssistedTotal_other,
        w.adobe_ordersAssistedTotal_other                AS adobe_ordersAssistedTotal_other_wow,
        l.adobe_ordersAssistedTotal_other                AS adobe_ordersAssistedTotal_other_ly,

        -- adobe_ordersAssistedPostpaid_allChannels
        c.adobe_ordersAssistedPostpaid_allChannels,
        w.adobe_ordersAssistedPostpaid_allChannels                AS adobe_ordersAssistedPostpaid_allChannels_wow,
        l.adobe_ordersAssistedPostpaid_allChannels                AS adobe_ordersAssistedPostpaid_allChannels_ly,

        -- adobe_ordersAssistedPostpaid_paidSearch
        c.adobe_ordersAssistedPostpaid_paidSearch,
        w.adobe_ordersAssistedPostpaid_paidSearch                AS adobe_ordersAssistedPostpaid_paidSearch_wow,
        l.adobe_ordersAssistedPostpaid_paidSearch                AS adobe_ordersAssistedPostpaid_paidSearch_ly,

        -- adobe_ordersAssistedPostpaid_organicSearch
        c.adobe_ordersAssistedPostpaid_organicSearch,
        w.adobe_ordersAssistedPostpaid_organicSearch                AS adobe_ordersAssistedPostpaid_organicSearch_wow,
        l.adobe_ordersAssistedPostpaid_organicSearch                AS adobe_ordersAssistedPostpaid_organicSearch_ly,

        -- adobe_ordersAssistedPostpaid_direct
        c.adobe_ordersAssistedPostpaid_direct,
        w.adobe_ordersAssistedPostpaid_direct                AS adobe_ordersAssistedPostpaid_direct_wow,
        l.adobe_ordersAssistedPostpaid_direct                AS adobe_ordersAssistedPostpaid_direct_ly,

        -- adobe_ordersAssistedPostpaid_social
        c.adobe_ordersAssistedPostpaid_social,
        w.adobe_ordersAssistedPostpaid_social                AS adobe_ordersAssistedPostpaid_social_wow,
        l.adobe_ordersAssistedPostpaid_social                AS adobe_ordersAssistedPostpaid_social_ly,

        -- adobe_ordersAssistedPostpaid_programmatic
        c.adobe_ordersAssistedPostpaid_programmatic,
        w.adobe_ordersAssistedPostpaid_programmatic                AS adobe_ordersAssistedPostpaid_programmatic_wow,
        l.adobe_ordersAssistedPostpaid_programmatic                AS adobe_ordersAssistedPostpaid_programmatic_ly,

        -- adobe_ordersAssistedPostpaid_other
        c.adobe_ordersAssistedPostpaid_other,
        w.adobe_ordersAssistedPostpaid_other                AS adobe_ordersAssistedPostpaid_other_wow,
        l.adobe_ordersAssistedPostpaid_other                AS adobe_ordersAssistedPostpaid_other_ly,

        -- adobe_ordersAssistedHsi_allChannels
        c.adobe_ordersAssistedHsi_allChannels,
        w.adobe_ordersAssistedHsi_allChannels                AS adobe_ordersAssistedHsi_allChannels_wow,
        l.adobe_ordersAssistedHsi_allChannels                AS adobe_ordersAssistedHsi_allChannels_ly,

        -- adobe_ordersAssistedHsi_paidSearch
        c.adobe_ordersAssistedHsi_paidSearch,
        w.adobe_ordersAssistedHsi_paidSearch                AS adobe_ordersAssistedHsi_paidSearch_wow,
        l.adobe_ordersAssistedHsi_paidSearch                AS adobe_ordersAssistedHsi_paidSearch_ly,

        -- adobe_ordersAssistedHsi_organicSearch
        c.adobe_ordersAssistedHsi_organicSearch,
        w.adobe_ordersAssistedHsi_organicSearch                AS adobe_ordersAssistedHsi_organicSearch_wow,
        l.adobe_ordersAssistedHsi_organicSearch                AS adobe_ordersAssistedHsi_organicSearch_ly,

        -- adobe_ordersAssistedHsi_direct
        c.adobe_ordersAssistedHsi_direct,
        w.adobe_ordersAssistedHsi_direct                AS adobe_ordersAssistedHsi_direct_wow,
        l.adobe_ordersAssistedHsi_direct                AS adobe_ordersAssistedHsi_direct_ly,

        -- adobe_ordersAssistedHsi_social
        c.adobe_ordersAssistedHsi_social,
        w.adobe_ordersAssistedHsi_social                AS adobe_ordersAssistedHsi_social_wow,
        l.adobe_ordersAssistedHsi_social                AS adobe_ordersAssistedHsi_social_ly,

        -- adobe_ordersAssistedHsi_programmatic
        c.adobe_ordersAssistedHsi_programmatic,
        w.adobe_ordersAssistedHsi_programmatic                AS adobe_ordersAssistedHsi_programmatic_wow,
        l.adobe_ordersAssistedHsi_programmatic                AS adobe_ordersAssistedHsi_programmatic_ly,

        -- adobe_ordersAssistedHsi_other
        c.adobe_ordersAssistedHsi_other,
        w.adobe_ordersAssistedHsi_other                AS adobe_ordersAssistedHsi_other_wow,
        l.adobe_ordersAssistedHsi_other                AS adobe_ordersAssistedHsi_other_ly,

        -- adobe_ordersAssistedByod_allChannels
        c.adobe_ordersAssistedByod_allChannels,
        w.adobe_ordersAssistedByod_allChannels                AS adobe_ordersAssistedByod_allChannels_wow,
        l.adobe_ordersAssistedByod_allChannels                AS adobe_ordersAssistedByod_allChannels_ly,

        -- adobe_ordersAssistedByod_paidSearch
        c.adobe_ordersAssistedByod_paidSearch,
        w.adobe_ordersAssistedByod_paidSearch                AS adobe_ordersAssistedByod_paidSearch_wow,
        l.adobe_ordersAssistedByod_paidSearch                AS adobe_ordersAssistedByod_paidSearch_ly,

        -- adobe_ordersAssistedByod_organicSearch
        c.adobe_ordersAssistedByod_organicSearch,
        w.adobe_ordersAssistedByod_organicSearch                AS adobe_ordersAssistedByod_organicSearch_wow,
        l.adobe_ordersAssistedByod_organicSearch                AS adobe_ordersAssistedByod_organicSearch_ly,

        -- adobe_ordersAssistedByod_direct
        c.adobe_ordersAssistedByod_direct,
        w.adobe_ordersAssistedByod_direct                AS adobe_ordersAssistedByod_direct_wow,
        l.adobe_ordersAssistedByod_direct                AS adobe_ordersAssistedByod_direct_ly,

        -- adobe_ordersAssistedByod_social
        c.adobe_ordersAssistedByod_social,
        w.adobe_ordersAssistedByod_social                AS adobe_ordersAssistedByod_social_wow,
        l.adobe_ordersAssistedByod_social                AS adobe_ordersAssistedByod_social_ly,

        -- adobe_ordersAssistedByod_programmatic
        c.adobe_ordersAssistedByod_programmatic,
        w.adobe_ordersAssistedByod_programmatic                AS adobe_ordersAssistedByod_programmatic_wow,
        l.adobe_ordersAssistedByod_programmatic                AS adobe_ordersAssistedByod_programmatic_ly,

        -- adobe_ordersAssistedByod_other
        c.adobe_ordersAssistedByod_other
        w.adobe_ordersAssistedByod_other                AS adobe_ordersAssistedByod_other_wow
        l.adobe_ordersAssistedByod_other                AS adobe_ordersAssistedByod_other_ly

    FROM with_week_num c
    LEFT JOIN with_week_num w
        ON c.week_sun_to_sat = DATE_ADD(w.week_sun_to_sat, INTERVAL 7 DAY)
    LEFT JOIN with_week_num l
        ON (c.custom_week_num - l.custom_week_num) = 52
),

-- -----------------------------------------------------------------------
-- STEP 5: Compute wow_pct and yoy_pct for all metrics
-- NULL when prior value is NULL or 0
-- -----------------------------------------------------------------------
with_pcts AS (
    SELECT
        week_sun_to_sat,
        custom_week_num,

        adobe_uvnbTotalAdobe_allChannels,
        adobe_uvnbTotalAdobe_allChannels_wow,
        adobe_uvnbTotalAdobe_allChannels_ly,
        CASE WHEN adobe_uvnbTotalAdobe_allChannels_wow IS NULL OR adobe_uvnbTotalAdobe_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbTotalAdobe_allChannels - adobe_uvnbTotalAdobe_allChannels_wow) / adobe_uvnbTotalAdobe_allChannels_wow, 6) END AS adobe_uvnbTotalAdobe_allChannels_wow_pct,
        CASE WHEN adobe_uvnbTotalAdobe_allChannels_ly IS NULL OR adobe_uvnbTotalAdobe_allChannels_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbTotalAdobe_allChannels - adobe_uvnbTotalAdobe_allChannels_ly) / adobe_uvnbTotalAdobe_allChannels_ly, 6) END AS adobe_uvnbTotalAdobe_allChannels_yoy_pct,

        adobe_uvnbFlowTotal_allChannels,
        adobe_uvnbFlowTotal_allChannels_wow,
        adobe_uvnbFlowTotal_allChannels_ly,
        CASE WHEN adobe_uvnbFlowTotal_allChannels_wow IS NULL OR adobe_uvnbFlowTotal_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbFlowTotal_allChannels - adobe_uvnbFlowTotal_allChannels_wow) / adobe_uvnbFlowTotal_allChannels_wow, 6) END AS adobe_uvnbFlowTotal_allChannels_wow_pct,
        CASE WHEN adobe_uvnbFlowTotal_allChannels_ly IS NULL OR adobe_uvnbFlowTotal_allChannels_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbFlowTotal_allChannels - adobe_uvnbFlowTotal_allChannels_ly) / adobe_uvnbFlowTotal_allChannels_ly, 6) END AS adobe_uvnbFlowTotal_allChannels_yoy_pct,

        adobe_uvnbByodPctOfUvnbFlow_allChannels,
        adobe_uvnbByodPctOfUvnbFlow_allChannels_wow,
        adobe_uvnbByodPctOfUvnbFlow_allChannels_ly,
        CASE WHEN adobe_uvnbByodPctOfUvnbFlow_allChannels_wow IS NULL OR adobe_uvnbByodPctOfUvnbFlow_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbByodPctOfUvnbFlow_allChannels - adobe_uvnbByodPctOfUvnbFlow_allChannels_wow) / adobe_uvnbByodPctOfUvnbFlow_allChannels_wow, 6) END AS adobe_uvnbByodPctOfUvnbFlow_allChannels_wow_pct,
        CASE WHEN adobe_uvnbByodPctOfUvnbFlow_allChannels_ly IS NULL OR adobe_uvnbByodPctOfUvnbFlow_allChannels_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbByodPctOfUvnbFlow_allChannels - adobe_uvnbByodPctOfUvnbFlow_allChannels_ly) / adobe_uvnbByodPctOfUvnbFlow_allChannels_ly, 6) END AS adobe_uvnbByodPctOfUvnbFlow_allChannels_yoy_pct,

        adobe_uvnbPostpaid_allChannels,
        adobe_uvnbPostpaid_allChannels_wow,
        adobe_uvnbPostpaid_allChannels_ly,
        CASE WHEN adobe_uvnbPostpaid_allChannels_wow IS NULL OR adobe_uvnbPostpaid_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbPostpaid_allChannels - adobe_uvnbPostpaid_allChannels_wow) / adobe_uvnbPostpaid_allChannels_wow, 6) END AS adobe_uvnbPostpaid_allChannels_wow_pct,
        CASE WHEN adobe_uvnbPostpaid_allChannels_ly IS NULL OR adobe_uvnbPostpaid_allChannels_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbPostpaid_allChannels - adobe_uvnbPostpaid_allChannels_ly) / adobe_uvnbPostpaid_allChannels_ly, 6) END AS adobe_uvnbPostpaid_allChannels_yoy_pct,

        adobe_uvnbPostpaid_paidSearch,
        adobe_uvnbPostpaid_paidSearch_wow,
        adobe_uvnbPostpaid_paidSearch_ly,
        CASE WHEN adobe_uvnbPostpaid_paidSearch_wow IS NULL OR adobe_uvnbPostpaid_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbPostpaid_paidSearch - adobe_uvnbPostpaid_paidSearch_wow) / adobe_uvnbPostpaid_paidSearch_wow, 6) END AS adobe_uvnbPostpaid_paidSearch_wow_pct,
        CASE WHEN adobe_uvnbPostpaid_paidSearch_ly IS NULL OR adobe_uvnbPostpaid_paidSearch_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbPostpaid_paidSearch - adobe_uvnbPostpaid_paidSearch_ly) / adobe_uvnbPostpaid_paidSearch_ly, 6) END AS adobe_uvnbPostpaid_paidSearch_yoy_pct,

        adobe_uvnbPostpaid_organicSearch,
        adobe_uvnbPostpaid_organicSearch_wow,
        adobe_uvnbPostpaid_organicSearch_ly,
        CASE WHEN adobe_uvnbPostpaid_organicSearch_wow IS NULL OR adobe_uvnbPostpaid_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbPostpaid_organicSearch - adobe_uvnbPostpaid_organicSearch_wow) / adobe_uvnbPostpaid_organicSearch_wow, 6) END AS adobe_uvnbPostpaid_organicSearch_wow_pct,
        CASE WHEN adobe_uvnbPostpaid_organicSearch_ly IS NULL OR adobe_uvnbPostpaid_organicSearch_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbPostpaid_organicSearch - adobe_uvnbPostpaid_organicSearch_ly) / adobe_uvnbPostpaid_organicSearch_ly, 6) END AS adobe_uvnbPostpaid_organicSearch_yoy_pct,

        adobe_uvnbPostpaid_direct,
        adobe_uvnbPostpaid_direct_wow,
        adobe_uvnbPostpaid_direct_ly,
        CASE WHEN adobe_uvnbPostpaid_direct_wow IS NULL OR adobe_uvnbPostpaid_direct_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbPostpaid_direct - adobe_uvnbPostpaid_direct_wow) / adobe_uvnbPostpaid_direct_wow, 6) END AS adobe_uvnbPostpaid_direct_wow_pct,
        CASE WHEN adobe_uvnbPostpaid_direct_ly IS NULL OR adobe_uvnbPostpaid_direct_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbPostpaid_direct - adobe_uvnbPostpaid_direct_ly) / adobe_uvnbPostpaid_direct_ly, 6) END AS adobe_uvnbPostpaid_direct_yoy_pct,

        adobe_uvnbPostpaid_social,
        adobe_uvnbPostpaid_social_wow,
        adobe_uvnbPostpaid_social_ly,
        CASE WHEN adobe_uvnbPostpaid_social_wow IS NULL OR adobe_uvnbPostpaid_social_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbPostpaid_social - adobe_uvnbPostpaid_social_wow) / adobe_uvnbPostpaid_social_wow, 6) END AS adobe_uvnbPostpaid_social_wow_pct,
        CASE WHEN adobe_uvnbPostpaid_social_ly IS NULL OR adobe_uvnbPostpaid_social_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbPostpaid_social - adobe_uvnbPostpaid_social_ly) / adobe_uvnbPostpaid_social_ly, 6) END AS adobe_uvnbPostpaid_social_yoy_pct,

        adobe_uvnbPostpaid_programmatic,
        adobe_uvnbPostpaid_programmatic_wow,
        adobe_uvnbPostpaid_programmatic_ly,
        CASE WHEN adobe_uvnbPostpaid_programmatic_wow IS NULL OR adobe_uvnbPostpaid_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbPostpaid_programmatic - adobe_uvnbPostpaid_programmatic_wow) / adobe_uvnbPostpaid_programmatic_wow, 6) END AS adobe_uvnbPostpaid_programmatic_wow_pct,
        CASE WHEN adobe_uvnbPostpaid_programmatic_ly IS NULL OR adobe_uvnbPostpaid_programmatic_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbPostpaid_programmatic - adobe_uvnbPostpaid_programmatic_ly) / adobe_uvnbPostpaid_programmatic_ly, 6) END AS adobe_uvnbPostpaid_programmatic_yoy_pct,

        adobe_uvnbPostpaid_other,
        adobe_uvnbPostpaid_other_wow,
        adobe_uvnbPostpaid_other_ly,
        CASE WHEN adobe_uvnbPostpaid_other_wow IS NULL OR adobe_uvnbPostpaid_other_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbPostpaid_other - adobe_uvnbPostpaid_other_wow) / adobe_uvnbPostpaid_other_wow, 6) END AS adobe_uvnbPostpaid_other_wow_pct,
        CASE WHEN adobe_uvnbPostpaid_other_ly IS NULL OR adobe_uvnbPostpaid_other_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbPostpaid_other - adobe_uvnbPostpaid_other_ly) / adobe_uvnbPostpaid_other_ly, 6) END AS adobe_uvnbPostpaid_other_yoy_pct,

        adobe_uvnbHsi_allChannels,
        adobe_uvnbHsi_allChannels_wow,
        adobe_uvnbHsi_allChannels_ly,
        CASE WHEN adobe_uvnbHsi_allChannels_wow IS NULL OR adobe_uvnbHsi_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbHsi_allChannels - adobe_uvnbHsi_allChannels_wow) / adobe_uvnbHsi_allChannels_wow, 6) END AS adobe_uvnbHsi_allChannels_wow_pct,
        CASE WHEN adobe_uvnbHsi_allChannels_ly IS NULL OR adobe_uvnbHsi_allChannels_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbHsi_allChannels - adobe_uvnbHsi_allChannels_ly) / adobe_uvnbHsi_allChannels_ly, 6) END AS adobe_uvnbHsi_allChannels_yoy_pct,

        adobe_uvnbHsi_paidSearch,
        adobe_uvnbHsi_paidSearch_wow,
        adobe_uvnbHsi_paidSearch_ly,
        CASE WHEN adobe_uvnbHsi_paidSearch_wow IS NULL OR adobe_uvnbHsi_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbHsi_paidSearch - adobe_uvnbHsi_paidSearch_wow) / adobe_uvnbHsi_paidSearch_wow, 6) END AS adobe_uvnbHsi_paidSearch_wow_pct,
        CASE WHEN adobe_uvnbHsi_paidSearch_ly IS NULL OR adobe_uvnbHsi_paidSearch_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbHsi_paidSearch - adobe_uvnbHsi_paidSearch_ly) / adobe_uvnbHsi_paidSearch_ly, 6) END AS adobe_uvnbHsi_paidSearch_yoy_pct,

        adobe_uvnbHsi_organicSearch,
        adobe_uvnbHsi_organicSearch_wow,
        adobe_uvnbHsi_organicSearch_ly,
        CASE WHEN adobe_uvnbHsi_organicSearch_wow IS NULL OR adobe_uvnbHsi_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbHsi_organicSearch - adobe_uvnbHsi_organicSearch_wow) / adobe_uvnbHsi_organicSearch_wow, 6) END AS adobe_uvnbHsi_organicSearch_wow_pct,
        CASE WHEN adobe_uvnbHsi_organicSearch_ly IS NULL OR adobe_uvnbHsi_organicSearch_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbHsi_organicSearch - adobe_uvnbHsi_organicSearch_ly) / adobe_uvnbHsi_organicSearch_ly, 6) END AS adobe_uvnbHsi_organicSearch_yoy_pct,

        adobe_uvnbHsi_direct,
        adobe_uvnbHsi_direct_wow,
        adobe_uvnbHsi_direct_ly,
        CASE WHEN adobe_uvnbHsi_direct_wow IS NULL OR adobe_uvnbHsi_direct_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbHsi_direct - adobe_uvnbHsi_direct_wow) / adobe_uvnbHsi_direct_wow, 6) END AS adobe_uvnbHsi_direct_wow_pct,
        CASE WHEN adobe_uvnbHsi_direct_ly IS NULL OR adobe_uvnbHsi_direct_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbHsi_direct - adobe_uvnbHsi_direct_ly) / adobe_uvnbHsi_direct_ly, 6) END AS adobe_uvnbHsi_direct_yoy_pct,

        adobe_uvnbHsi_social,
        adobe_uvnbHsi_social_wow,
        adobe_uvnbHsi_social_ly,
        CASE WHEN adobe_uvnbHsi_social_wow IS NULL OR adobe_uvnbHsi_social_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbHsi_social - adobe_uvnbHsi_social_wow) / adobe_uvnbHsi_social_wow, 6) END AS adobe_uvnbHsi_social_wow_pct,
        CASE WHEN adobe_uvnbHsi_social_ly IS NULL OR adobe_uvnbHsi_social_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbHsi_social - adobe_uvnbHsi_social_ly) / adobe_uvnbHsi_social_ly, 6) END AS adobe_uvnbHsi_social_yoy_pct,

        adobe_uvnbHsi_programmatic,
        adobe_uvnbHsi_programmatic_wow,
        adobe_uvnbHsi_programmatic_ly,
        CASE WHEN adobe_uvnbHsi_programmatic_wow IS NULL OR adobe_uvnbHsi_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbHsi_programmatic - adobe_uvnbHsi_programmatic_wow) / adobe_uvnbHsi_programmatic_wow, 6) END AS adobe_uvnbHsi_programmatic_wow_pct,
        CASE WHEN adobe_uvnbHsi_programmatic_ly IS NULL OR adobe_uvnbHsi_programmatic_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbHsi_programmatic - adobe_uvnbHsi_programmatic_ly) / adobe_uvnbHsi_programmatic_ly, 6) END AS adobe_uvnbHsi_programmatic_yoy_pct,

        adobe_uvnbHsi_other,
        adobe_uvnbHsi_other_wow,
        adobe_uvnbHsi_other_ly,
        CASE WHEN adobe_uvnbHsi_other_wow IS NULL OR adobe_uvnbHsi_other_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbHsi_other - adobe_uvnbHsi_other_wow) / adobe_uvnbHsi_other_wow, 6) END AS adobe_uvnbHsi_other_wow_pct,
        CASE WHEN adobe_uvnbHsi_other_ly IS NULL OR adobe_uvnbHsi_other_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbHsi_other - adobe_uvnbHsi_other_ly) / adobe_uvnbHsi_other_ly, 6) END AS adobe_uvnbHsi_other_yoy_pct,

        adobe_uvnbByod_allChannels,
        adobe_uvnbByod_allChannels_wow,
        adobe_uvnbByod_allChannels_ly,
        CASE WHEN adobe_uvnbByod_allChannels_wow IS NULL OR adobe_uvnbByod_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_allChannels - adobe_uvnbByod_allChannels_wow) / adobe_uvnbByod_allChannels_wow, 6) END AS adobe_uvnbByod_allChannels_wow_pct,
        CASE WHEN adobe_uvnbByod_allChannels_ly IS NULL OR adobe_uvnbByod_allChannels_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_allChannels - adobe_uvnbByod_allChannels_ly) / adobe_uvnbByod_allChannels_ly, 6) END AS adobe_uvnbByod_allChannels_yoy_pct,

        adobe_uvnbByod_paidSearch,
        adobe_uvnbByod_paidSearch_wow,
        adobe_uvnbByod_paidSearch_ly,
        CASE WHEN adobe_uvnbByod_paidSearch_wow IS NULL OR adobe_uvnbByod_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_paidSearch - adobe_uvnbByod_paidSearch_wow) / adobe_uvnbByod_paidSearch_wow, 6) END AS adobe_uvnbByod_paidSearch_wow_pct,
        CASE WHEN adobe_uvnbByod_paidSearch_ly IS NULL OR adobe_uvnbByod_paidSearch_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_paidSearch - adobe_uvnbByod_paidSearch_ly) / adobe_uvnbByod_paidSearch_ly, 6) END AS adobe_uvnbByod_paidSearch_yoy_pct,

        adobe_uvnbByod_organicSearch,
        adobe_uvnbByod_organicSearch_wow,
        adobe_uvnbByod_organicSearch_ly,
        CASE WHEN adobe_uvnbByod_organicSearch_wow IS NULL OR adobe_uvnbByod_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_organicSearch - adobe_uvnbByod_organicSearch_wow) / adobe_uvnbByod_organicSearch_wow, 6) END AS adobe_uvnbByod_organicSearch_wow_pct,
        CASE WHEN adobe_uvnbByod_organicSearch_ly IS NULL OR adobe_uvnbByod_organicSearch_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_organicSearch - adobe_uvnbByod_organicSearch_ly) / adobe_uvnbByod_organicSearch_ly, 6) END AS adobe_uvnbByod_organicSearch_yoy_pct,

        adobe_uvnbByod_direct,
        adobe_uvnbByod_direct_wow,
        adobe_uvnbByod_direct_ly,
        CASE WHEN adobe_uvnbByod_direct_wow IS NULL OR adobe_uvnbByod_direct_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_direct - adobe_uvnbByod_direct_wow) / adobe_uvnbByod_direct_wow, 6) END AS adobe_uvnbByod_direct_wow_pct,
        CASE WHEN adobe_uvnbByod_direct_ly IS NULL OR adobe_uvnbByod_direct_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_direct - adobe_uvnbByod_direct_ly) / adobe_uvnbByod_direct_ly, 6) END AS adobe_uvnbByod_direct_yoy_pct,

        adobe_uvnbByod_social,
        adobe_uvnbByod_social_wow,
        adobe_uvnbByod_social_ly,
        CASE WHEN adobe_uvnbByod_social_wow IS NULL OR adobe_uvnbByod_social_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_social - adobe_uvnbByod_social_wow) / adobe_uvnbByod_social_wow, 6) END AS adobe_uvnbByod_social_wow_pct,
        CASE WHEN adobe_uvnbByod_social_ly IS NULL OR adobe_uvnbByod_social_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_social - adobe_uvnbByod_social_ly) / adobe_uvnbByod_social_ly, 6) END AS adobe_uvnbByod_social_yoy_pct,

        adobe_uvnbByod_programmatic,
        adobe_uvnbByod_programmatic_wow,
        adobe_uvnbByod_programmatic_ly,
        CASE WHEN adobe_uvnbByod_programmatic_wow IS NULL OR adobe_uvnbByod_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_programmatic - adobe_uvnbByod_programmatic_wow) / adobe_uvnbByod_programmatic_wow, 6) END AS adobe_uvnbByod_programmatic_wow_pct,
        CASE WHEN adobe_uvnbByod_programmatic_ly IS NULL OR adobe_uvnbByod_programmatic_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_programmatic - adobe_uvnbByod_programmatic_ly) / adobe_uvnbByod_programmatic_ly, 6) END AS adobe_uvnbByod_programmatic_yoy_pct,

        adobe_uvnbByod_other,
        adobe_uvnbByod_other_wow,
        adobe_uvnbByod_other_ly,
        CASE WHEN adobe_uvnbByod_other_wow IS NULL OR adobe_uvnbByod_other_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_other - adobe_uvnbByod_other_wow) / adobe_uvnbByod_other_wow, 6) END AS adobe_uvnbByod_other_wow_pct,
        CASE WHEN adobe_uvnbByod_other_ly IS NULL OR adobe_uvnbByod_other_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_other - adobe_uvnbByod_other_ly) / adobe_uvnbByod_other_ly, 6) END AS adobe_uvnbByod_other_yoy_pct,

        adobe_uvnbTrackedFlowSum_allChannels,
        adobe_uvnbTrackedFlowSum_allChannels_wow,
        adobe_uvnbTrackedFlowSum_allChannels_ly,
        CASE WHEN adobe_uvnbTrackedFlowSum_allChannels_wow IS NULL OR adobe_uvnbTrackedFlowSum_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbTrackedFlowSum_allChannels - adobe_uvnbTrackedFlowSum_allChannels_wow) / adobe_uvnbTrackedFlowSum_allChannels_wow, 6) END AS adobe_uvnbTrackedFlowSum_allChannels_wow_pct,
        CASE WHEN adobe_uvnbTrackedFlowSum_allChannels_ly IS NULL OR adobe_uvnbTrackedFlowSum_allChannels_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbTrackedFlowSum_allChannels - adobe_uvnbTrackedFlowSum_allChannels_ly) / adobe_uvnbTrackedFlowSum_allChannels_ly, 6) END AS adobe_uvnbTrackedFlowSum_allChannels_yoy_pct,

        adobe_uvnbTrackedFlowSum_paidSearch,
        adobe_uvnbTrackedFlowSum_paidSearch_wow,
        adobe_uvnbTrackedFlowSum_paidSearch_ly,
        CASE WHEN adobe_uvnbTrackedFlowSum_paidSearch_wow IS NULL OR adobe_uvnbTrackedFlowSum_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbTrackedFlowSum_paidSearch - adobe_uvnbTrackedFlowSum_paidSearch_wow) / adobe_uvnbTrackedFlowSum_paidSearch_wow, 6) END AS adobe_uvnbTrackedFlowSum_paidSearch_wow_pct,
        CASE WHEN adobe_uvnbTrackedFlowSum_paidSearch_ly IS NULL OR adobe_uvnbTrackedFlowSum_paidSearch_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbTrackedFlowSum_paidSearch - adobe_uvnbTrackedFlowSum_paidSearch_ly) / adobe_uvnbTrackedFlowSum_paidSearch_ly, 6) END AS adobe_uvnbTrackedFlowSum_paidSearch_yoy_pct,

        adobe_uvnbTrackedFlowSum_organicSearch,
        adobe_uvnbTrackedFlowSum_organicSearch_wow,
        adobe_uvnbTrackedFlowSum_organicSearch_ly,
        CASE WHEN adobe_uvnbTrackedFlowSum_organicSearch_wow IS NULL OR adobe_uvnbTrackedFlowSum_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbTrackedFlowSum_organicSearch - adobe_uvnbTrackedFlowSum_organicSearch_wow) / adobe_uvnbTrackedFlowSum_organicSearch_wow, 6) END AS adobe_uvnbTrackedFlowSum_organicSearch_wow_pct,
        CASE WHEN adobe_uvnbTrackedFlowSum_organicSearch_ly IS NULL OR adobe_uvnbTrackedFlowSum_organicSearch_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbTrackedFlowSum_organicSearch - adobe_uvnbTrackedFlowSum_organicSearch_ly) / adobe_uvnbTrackedFlowSum_organicSearch_ly, 6) END AS adobe_uvnbTrackedFlowSum_organicSearch_yoy_pct,

        adobe_uvnbTrackedFlowSum_direct,
        adobe_uvnbTrackedFlowSum_direct_wow,
        adobe_uvnbTrackedFlowSum_direct_ly,
        CASE WHEN adobe_uvnbTrackedFlowSum_direct_wow IS NULL OR adobe_uvnbTrackedFlowSum_direct_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbTrackedFlowSum_direct - adobe_uvnbTrackedFlowSum_direct_wow) / adobe_uvnbTrackedFlowSum_direct_wow, 6) END AS adobe_uvnbTrackedFlowSum_direct_wow_pct,
        CASE WHEN adobe_uvnbTrackedFlowSum_direct_ly IS NULL OR adobe_uvnbTrackedFlowSum_direct_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbTrackedFlowSum_direct - adobe_uvnbTrackedFlowSum_direct_ly) / adobe_uvnbTrackedFlowSum_direct_ly, 6) END AS adobe_uvnbTrackedFlowSum_direct_yoy_pct,

        adobe_uvnbTrackedFlowSum_social,
        adobe_uvnbTrackedFlowSum_social_wow,
        adobe_uvnbTrackedFlowSum_social_ly,
        CASE WHEN adobe_uvnbTrackedFlowSum_social_wow IS NULL OR adobe_uvnbTrackedFlowSum_social_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbTrackedFlowSum_social - adobe_uvnbTrackedFlowSum_social_wow) / adobe_uvnbTrackedFlowSum_social_wow, 6) END AS adobe_uvnbTrackedFlowSum_social_wow_pct,
        CASE WHEN adobe_uvnbTrackedFlowSum_social_ly IS NULL OR adobe_uvnbTrackedFlowSum_social_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbTrackedFlowSum_social - adobe_uvnbTrackedFlowSum_social_ly) / adobe_uvnbTrackedFlowSum_social_ly, 6) END AS adobe_uvnbTrackedFlowSum_social_yoy_pct,

        adobe_uvnbTrackedFlowSum_programmatic,
        adobe_uvnbTrackedFlowSum_programmatic_wow,
        adobe_uvnbTrackedFlowSum_programmatic_ly,
        CASE WHEN adobe_uvnbTrackedFlowSum_programmatic_wow IS NULL OR adobe_uvnbTrackedFlowSum_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbTrackedFlowSum_programmatic - adobe_uvnbTrackedFlowSum_programmatic_wow) / adobe_uvnbTrackedFlowSum_programmatic_wow, 6) END AS adobe_uvnbTrackedFlowSum_programmatic_wow_pct,
        CASE WHEN adobe_uvnbTrackedFlowSum_programmatic_ly IS NULL OR adobe_uvnbTrackedFlowSum_programmatic_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbTrackedFlowSum_programmatic - adobe_uvnbTrackedFlowSum_programmatic_ly) / adobe_uvnbTrackedFlowSum_programmatic_ly, 6) END AS adobe_uvnbTrackedFlowSum_programmatic_yoy_pct,

        adobe_uvnbTrackedFlowSum_other,
        adobe_uvnbTrackedFlowSum_other_wow,
        adobe_uvnbTrackedFlowSum_other_ly,
        CASE WHEN adobe_uvnbTrackedFlowSum_other_wow IS NULL OR adobe_uvnbTrackedFlowSum_other_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbTrackedFlowSum_other - adobe_uvnbTrackedFlowSum_other_wow) / adobe_uvnbTrackedFlowSum_other_wow, 6) END AS adobe_uvnbTrackedFlowSum_other_wow_pct,
        CASE WHEN adobe_uvnbTrackedFlowSum_other_ly IS NULL OR adobe_uvnbTrackedFlowSum_other_ly = 0 THEN NULL ELSE ROUND((adobe_uvnbTrackedFlowSum_other - adobe_uvnbTrackedFlowSum_other_ly) / adobe_uvnbTrackedFlowSum_other_ly, 6) END AS adobe_uvnbTrackedFlowSum_other_yoy_pct,

        adobe_cartStartTotal_allChannels,
        adobe_cartStartTotal_allChannels_wow,
        adobe_cartStartTotal_allChannels_ly,
        CASE WHEN adobe_cartStartTotal_allChannels_wow IS NULL OR adobe_cartStartTotal_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartTotal_allChannels - adobe_cartStartTotal_allChannels_wow) / adobe_cartStartTotal_allChannels_wow, 6) END AS adobe_cartStartTotal_allChannels_wow_pct,
        CASE WHEN adobe_cartStartTotal_allChannels_ly IS NULL OR adobe_cartStartTotal_allChannels_ly = 0 THEN NULL ELSE ROUND((adobe_cartStartTotal_allChannels - adobe_cartStartTotal_allChannels_ly) / adobe_cartStartTotal_allChannels_ly, 6) END AS adobe_cartStartTotal_allChannels_yoy_pct,

        adobe_cartStartTotal_paidSearch,
        adobe_cartStartTotal_paidSearch_wow,
        adobe_cartStartTotal_paidSearch_ly,
        CASE WHEN adobe_cartStartTotal_paidSearch_wow IS NULL OR adobe_cartStartTotal_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartTotal_paidSearch - adobe_cartStartTotal_paidSearch_wow) / adobe_cartStartTotal_paidSearch_wow, 6) END AS adobe_cartStartTotal_paidSearch_wow_pct,
        CASE WHEN adobe_cartStartTotal_paidSearch_ly IS NULL OR adobe_cartStartTotal_paidSearch_ly = 0 THEN NULL ELSE ROUND((adobe_cartStartTotal_paidSearch - adobe_cartStartTotal_paidSearch_ly) / adobe_cartStartTotal_paidSearch_ly, 6) END AS adobe_cartStartTotal_paidSearch_yoy_pct,

        adobe_cartStartTotal_organicSearch,
        adobe_cartStartTotal_organicSearch_wow,
        adobe_cartStartTotal_organicSearch_ly,
        CASE WHEN adobe_cartStartTotal_organicSearch_wow IS NULL OR adobe_cartStartTotal_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartTotal_organicSearch - adobe_cartStartTotal_organicSearch_wow) / adobe_cartStartTotal_organicSearch_wow, 6) END AS adobe_cartStartTotal_organicSearch_wow_pct,
        CASE WHEN adobe_cartStartTotal_organicSearch_ly IS NULL OR adobe_cartStartTotal_organicSearch_ly = 0 THEN NULL ELSE ROUND((adobe_cartStartTotal_organicSearch - adobe_cartStartTotal_organicSearch_ly) / adobe_cartStartTotal_organicSearch_ly, 6) END AS adobe_cartStartTotal_organicSearch_yoy_pct,

        adobe_cartStartTotal_direct,
        adobe_cartStartTotal_direct_wow,
        adobe_cartStartTotal_direct_ly,
        CASE WHEN adobe_cartStartTotal_direct_wow IS NULL OR adobe_cartStartTotal_direct_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartTotal_direct - adobe_cartStartTotal_direct_wow) / adobe_cartStartTotal_direct_wow, 6) END AS adobe_cartStartTotal_direct_wow_pct,
        CASE WHEN adobe_cartStartTotal_direct_ly IS NULL OR adobe_cartStartTotal_direct_ly = 0 THEN NULL ELSE ROUND((adobe_cartStartTotal_direct - adobe_cartStartTotal_direct_ly) / adobe_cartStartTotal_direct_ly, 6) END AS adobe_cartStartTotal_direct_yoy_pct,

        adobe_cartStartTotal_social,
        adobe_cartStartTotal_social_wow,
        adobe_cartStartTotal_social_ly,
        CASE WHEN adobe_cartStartTotal_social_wow IS NULL OR adobe_cartStartTotal_social_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartTotal_social - adobe_cartStartTotal_social_wow) / adobe_cartStartTotal_social_wow, 6) END AS adobe_cartStartTotal_social_wow_pct,
        CASE WHEN adobe_cartStartTotal_social_ly IS NULL OR adobe_cartStartTotal_social_ly = 0 THEN NULL ELSE ROUND((adobe_cartStartTotal_social - adobe_cartStartTotal_social_ly) / adobe_cartStartTotal_social_ly, 6) END AS adobe_cartStartTotal_social_yoy_pct,

        adobe_cartStartTotal_programmatic,
        adobe_cartStartTotal_programmatic_wow,
        adobe_cartStartTotal_programmatic_ly,
        CASE WHEN adobe_cartStartTotal_programmatic_wow IS NULL OR adobe_cartStartTotal_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartTotal_programmatic - adobe_cartStartTotal_programmatic_wow) / adobe_cartStartTotal_programmatic_wow, 6) END AS adobe_cartStartTotal_programmatic_wow_pct,
        CASE WHEN adobe_cartStartTotal_programmatic_ly IS NULL OR adobe_cartStartTotal_programmatic_ly = 0 THEN NULL ELSE ROUND((adobe_cartStartTotal_programmatic - adobe_cartStartTotal_programmatic_ly) / adobe_cartStartTotal_programmatic_ly, 6) END AS adobe_cartStartTotal_programmatic_yoy_pct,

        adobe_cartStartTotal_other,
        adobe_cartStartTotal_other_wow,
        adobe_cartStartTotal_other_ly,
        CASE WHEN adobe_cartStartTotal_other_wow IS NULL OR adobe_cartStartTotal_other_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartTotal_other - adobe_cartStartTotal_other_wow) / adobe_cartStartTotal_other_wow, 6) END AS adobe_cartStartTotal_other_wow_pct,
        CASE WHEN adobe_cartStartTotal_other_ly IS NULL OR adobe_cartStartTotal_other_ly = 0 THEN NULL ELSE ROUND((adobe_cartStartTotal_other - adobe_cartStartTotal_other_ly) / adobe_cartStartTotal_other_ly, 6) END AS adobe_cartStartTotal_other_yoy_pct,

        adobe_cartStartPostpaid_allChannels,
        adobe_cartStartPostpaid_allChannels_wow,
        adobe_cartStartPostpaid_allChannels_ly,
        CASE WHEN adobe_cartStartPostpaid_allChannels_wow IS NULL OR adobe_cartStartPostpaid_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartPostpaid_allChannels - adobe_cartStartPostpaid_allChannels_wow) / adobe_cartStartPostpaid_allChannels_wow, 6) END AS adobe_cartStartPostpaid_allChannels_wow_pct,
        CASE WHEN adobe_cartStartPostpaid_allChannels_ly IS NULL OR adobe_cartStartPostpaid_allChannels_ly = 0 THEN NULL ELSE ROUND((adobe_cartStartPostpaid_allChannels - adobe_cartStartPostpaid_allChannels_ly) / adobe_cartStartPostpaid_allChannels_ly, 6) END AS adobe_cartStartPostpaid_allChannels_yoy_pct,

        adobe_cartStartPostpaid_paidSearch,
        adobe_cartStartPostpaid_paidSearch_wow,
        adobe_cartStartPostpaid_paidSearch_ly,
        CASE WHEN adobe_cartStartPostpaid_paidSearch_wow IS NULL OR adobe_cartStartPostpaid_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartPostpaid_paidSearch - adobe_cartStartPostpaid_paidSearch_wow) / adobe_cartStartPostpaid_paidSearch_wow, 6) END AS adobe_cartStartPostpaid_paidSearch_wow_pct,
        CASE WHEN adobe_cartStartPostpaid_paidSearch_ly IS NULL OR adobe_cartStartPostpaid_paidSearch_ly = 0 THEN NULL ELSE ROUND((adobe_cartStartPostpaid_paidSearch - adobe_cartStartPostpaid_paidSearch_ly) / adobe_cartStartPostpaid_paidSearch_ly, 6) END AS adobe_cartStartPostpaid_paidSearch_yoy_pct,

        adobe_cartStartPostpaid_organicSearch,
        adobe_cartStartPostpaid_organicSearch_wow,
        adobe_cartStartPostpaid_organicSearch_ly,
        CASE WHEN adobe_cartStartPostpaid_organicSearch_wow IS NULL OR adobe_cartStartPostpaid_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartPostpaid_organicSearch - adobe_cartStartPostpaid_organicSearch_wow) / adobe_cartStartPostpaid_organicSearch_wow, 6) END AS adobe_cartStartPostpaid_organicSearch_wow_pct,
        CASE WHEN adobe_cartStartPostpaid_organicSearch_ly IS NULL OR adobe_cartStartPostpaid_organicSearch_ly = 0 THEN NULL ELSE ROUND((adobe_cartStartPostpaid_organicSearch - adobe_cartStartPostpaid_organicSearch_ly) / adobe_cartStartPostpaid_organicSearch_ly, 6) END AS adobe_cartStartPostpaid_organicSearch_yoy_pct,

        adobe_cartStartPostpaid_direct,
        adobe_cartStartPostpaid_direct_wow,
        adobe_cartStartPostpaid_direct_ly,
        CASE WHEN adobe_cartStartPostpaid_direct_wow IS NULL OR adobe_cartStartPostpaid_direct_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartPostpaid_direct - adobe_cartStartPostpaid_direct_wow) / adobe_cartStartPostpaid_direct_wow, 6) END AS adobe_cartStartPostpaid_direct_wow_pct,
        CASE WHEN adobe_cartStartPostpaid_direct_ly IS NULL OR adobe_cartStartPostpaid_direct_ly = 0 THEN NULL ELSE ROUND((adobe_cartStartPostpaid_direct - adobe_cartStartPostpaid_direct_ly) / adobe_cartStartPostpaid_direct_ly, 6) END AS adobe_cartStartPostpaid_direct_yoy_pct,

        adobe_cartStartPostpaid_social,
        adobe_cartStartPostpaid_social_wow,
        adobe_cartStartPostpaid_social_ly,
        CASE WHEN adobe_cartStartPostpaid_social_wow IS NULL OR adobe_cartStartPostpaid_social_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartPostpaid_social - adobe_cartStartPostpaid_social_wow) / adobe_cartStartPostpaid_social_wow, 6) END AS adobe_cartStartPostpaid_social_wow_pct,
        CASE WHEN adobe_cartStartPostpaid_social_ly IS NULL OR adobe_cartStartPostpaid_social_ly = 0 THEN NULL ELSE ROUND((adobe_cartStartPostpaid_social - adobe_cartStartPostpaid_social_ly) / adobe_cartStartPostpaid_social_ly, 6) END AS adobe_cartStartPostpaid_social_yoy_pct,

        adobe_cartStartPostpaid_programmatic,
        adobe_cartStartPostpaid_programmatic_wow,
        adobe_cartStartPostpaid_programmatic_ly,
        CASE WHEN adobe_cartStartPostpaid_programmatic_wow IS NULL OR adobe_cartStartPostpaid_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartPostpaid_programmatic - adobe_cartStartPostpaid_programmatic_wow) / adobe_cartStartPostpaid_programmatic_wow, 6) END AS adobe_cartStartPostpaid_programmatic_wow_pct,
        CASE WHEN adobe_cartStartPostpaid_programmatic_ly IS NULL OR adobe_cartStartPostpaid_programmatic_ly = 0 THEN NULL ELSE ROUND((adobe_cartStartPostpaid_programmatic - adobe_cartStartPostpaid_programmatic_ly) / adobe_cartStartPostpaid_programmatic_ly, 6) END AS adobe_cartStartPostpaid_programmatic_yoy_pct,

        adobe_cartStartPostpaid_other,
        adobe_cartStartPostpaid_other_wow,
        adobe_cartStartPostpaid_other_ly,
        CASE WHEN adobe_cartStartPostpaid_other_wow IS NULL OR adobe_cartStartPostpaid_other_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartPostpaid_other - adobe_cartStartPostpaid_other_wow) / adobe_cartStartPostpaid_other_wow, 6) END AS adobe_cartStartPostpaid_other_wow_pct,
        CASE WHEN adobe_cartStartPostpaid_other_ly IS NULL OR adobe_cartStartPostpaid_other_ly = 0 THEN NULL ELSE ROUND((adobe_cartStartPostpaid_other - adobe_cartStartPostpaid_other_ly) / adobe_cartStartPostpaid_other_ly, 6) END AS adobe_cartStartPostpaid_other_yoy_pct,

        adobe_cartStartHsi_allChannels,
        adobe_cartStartHsi_allChannels_wow,
        adobe_cartStartHsi_allChannels_ly,
        CASE WHEN adobe_cartStartHsi_allChannels_wow IS NULL OR adobe_cartStartHsi_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartHsi_allChannels - adobe_cartStartHsi_allChannels_wow) / adobe_cartStartHsi_allChannels_wow, 6) END AS adobe_cartStartHsi_allChannels_wow_pct,
        CASE WHEN adobe_cartStartHsi_allChannels_ly IS NULL OR adobe_cartStartHsi_allChannels_ly = 0 THEN NULL ELSE ROUND((adobe_cartStartHsi_allChannels - adobe_cartStartHsi_allChannels_ly) / adobe_cartStartHsi_allChannels_ly, 6) END AS adobe_cartStartHsi_allChannels_yoy_pct,

        adobe_cartStartHsi_paidSearch,
        adobe_cartStartHsi_paidSearch_wow,
        adobe_cartStartHsi_paidSearch_ly,
        CASE WHEN adobe_cartStartHsi_paidSearch_wow IS NULL OR adobe_cartStartHsi_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartHsi_paidSearch - adobe_cartStartHsi_paidSearch_wow) / adobe_cartStartHsi_paidSearch_wow, 6) END AS adobe_cartStartHsi_paidSearch_wow_pct,
        CASE WHEN adobe_cartStartHsi_paidSearch_ly IS NULL OR adobe_cartStartHsi_paidSearch_ly = 0 THEN NULL ELSE ROUND((adobe_cartStartHsi_paidSearch - adobe_cartStartHsi_paidSearch_ly) / adobe_cartStartHsi_paidSearch_ly, 6) END AS adobe_cartStartHsi_paidSearch_yoy_pct,

        adobe_cartStartHsi_organicSearch,
        adobe_cartStartHsi_organicSearch_wow,
        adobe_cartStartHsi_organicSearch_ly,
        CASE WHEN adobe_cartStartHsi_organicSearch_wow IS NULL OR adobe_cartStartHsi_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartHsi_organicSearch - adobe_cartStartHsi_organicSearch_wow) / adobe_cartStartHsi_organicSearch_wow, 6) END AS adobe_cartStartHsi_organicSearch_wow_pct,
        CASE WHEN adobe_cartStartHsi_organicSearch_ly IS NULL OR adobe_cartStartHsi_organicSearch_ly = 0 THEN NULL ELSE ROUND((adobe_cartStartHsi_organicSearch - adobe_cartStartHsi_organicSearch_ly) / adobe_cartStartHsi_organicSearch_ly, 6) END AS adobe_cartStartHsi_organicSearch_yoy_pct,

        adobe_cartStartHsi_direct,
        adobe_cartStartHsi_direct_wow,
        adobe_cartStartHsi_direct_ly,
        CASE WHEN adobe_cartStartHsi_direct_wow IS NULL OR adobe_cartStartHsi_direct_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartHsi_direct - adobe_cartStartHsi_direct_wow) / adobe_cartStartHsi_direct_wow, 6) END AS adobe_cartStartHsi_direct_wow_pct,
        CASE WHEN adobe_cartStartHsi_direct_ly IS NULL OR adobe_cartStartHsi_direct_ly = 0 THEN NULL ELSE ROUND((adobe_cartStartHsi_direct - adobe_cartStartHsi_direct_ly) / adobe_cartStartHsi_direct_ly, 6) END AS adobe_cartStartHsi_direct_yoy_pct,

        adobe_cartStartHsi_social,
        adobe_cartStartHsi_social_wow,
        adobe_cartStartHsi_social_ly,
        CASE WHEN adobe_cartStartHsi_social_wow IS NULL OR adobe_cartStartHsi_social_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartHsi_social - adobe_cartStartHsi_social_wow) / adobe_cartStartHsi_social_wow, 6) END AS adobe_cartStartHsi_social_wow_pct,
        CASE WHEN adobe_cartStartHsi_social_ly IS NULL OR adobe_cartStartHsi_social_ly = 0 THEN NULL ELSE ROUND((adobe_cartStartHsi_social - adobe_cartStartHsi_social_ly) / adobe_cartStartHsi_social_ly, 6) END AS adobe_cartStartHsi_social_yoy_pct,

        adobe_cartStartHsi_programmatic,
        adobe_cartStartHsi_programmatic_wow,
        adobe_cartStartHsi_programmatic_ly,
        CASE WHEN adobe_cartStartHsi_programmatic_wow IS NULL OR adobe_cartStartHsi_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartHsi_programmatic - adobe_cartStartHsi_programmatic_wow) / adobe_cartStartHsi_programmatic_wow, 6) END AS adobe_cartStartHsi_programmatic_wow_pct,
        CASE WHEN adobe_cartStartHsi_programmatic_ly IS NULL OR adobe_cartStartHsi_programmatic_ly = 0 THEN NULL ELSE ROUND((adobe_cartStartHsi_programmatic - adobe_cartStartHsi_programmatic_ly) / adobe_cartStartHsi_programmatic_ly, 6) END AS adobe_cartStartHsi_programmatic_yoy_pct,

        adobe_cartStartHsi_other,
        adobe_cartStartHsi_other_wow,
        adobe_cartStartHsi_other_ly,
        CASE WHEN adobe_cartStartHsi_other_wow IS NULL OR adobe_cartStartHsi_other_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartHsi_other - adobe_cartStartHsi_other_wow) / adobe_cartStartHsi_other_wow, 6) END AS adobe_cartStartHsi_other_wow_pct,
        CASE WHEN adobe_cartStartHsi_other_ly IS NULL OR adobe_cartStartHsi_other_ly = 0 THEN NULL ELSE ROUND((adobe_cartStartHsi_other - adobe_cartStartHsi_other_ly) / adobe_cartStartHsi_other_ly, 6) END AS adobe_cartStartHsi_other_yoy_pct,

        adobe_cartStartByod_allChannels,
        adobe_cartStartByod_allChannels_wow,
        adobe_cartStartByod_allChannels_ly,
        CASE WHEN adobe_cartStartByod_allChannels_wow IS NULL OR adobe_cartStartByod_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_allChannels - adobe_cartStartByod_allChannels_wow) / adobe_cartStartByod_allChannels_wow, 6) END AS adobe_cartStartByod_allChannels_wow_pct,
        CASE WHEN adobe_cartStartByod_allChannels_ly IS NULL OR adobe_cartStartByod_allChannels_ly = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_allChannels - adobe_cartStartByod_allChannels_ly) / adobe_cartStartByod_allChannels_ly, 6) END AS adobe_cartStartByod_allChannels_yoy_pct,

        adobe_cartStartByod_paidSearch,
        adobe_cartStartByod_paidSearch_wow,
        adobe_cartStartByod_paidSearch_ly,
        CASE WHEN adobe_cartStartByod_paidSearch_wow IS NULL OR adobe_cartStartByod_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_paidSearch - adobe_cartStartByod_paidSearch_wow) / adobe_cartStartByod_paidSearch_wow, 6) END AS adobe_cartStartByod_paidSearch_wow_pct,
        CASE WHEN adobe_cartStartByod_paidSearch_ly IS NULL OR adobe_cartStartByod_paidSearch_ly = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_paidSearch - adobe_cartStartByod_paidSearch_ly) / adobe_cartStartByod_paidSearch_ly, 6) END AS adobe_cartStartByod_paidSearch_yoy_pct,

        adobe_cartStartByod_organicSearch,
        adobe_cartStartByod_organicSearch_wow,
        adobe_cartStartByod_organicSearch_ly,
        CASE WHEN adobe_cartStartByod_organicSearch_wow IS NULL OR adobe_cartStartByod_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_organicSearch - adobe_cartStartByod_organicSearch_wow) / adobe_cartStartByod_organicSearch_wow, 6) END AS adobe_cartStartByod_organicSearch_wow_pct,
        CASE WHEN adobe_cartStartByod_organicSearch_ly IS NULL OR adobe_cartStartByod_organicSearch_ly = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_organicSearch - adobe_cartStartByod_organicSearch_ly) / adobe_cartStartByod_organicSearch_ly, 6) END AS adobe_cartStartByod_organicSearch_yoy_pct,

        adobe_cartStartByod_direct,
        adobe_cartStartByod_direct_wow,
        adobe_cartStartByod_direct_ly,
        CASE WHEN adobe_cartStartByod_direct_wow IS NULL OR adobe_cartStartByod_direct_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_direct - adobe_cartStartByod_direct_wow) / adobe_cartStartByod_direct_wow, 6) END AS adobe_cartStartByod_direct_wow_pct,
        CASE WHEN adobe_cartStartByod_direct_ly IS NULL OR adobe_cartStartByod_direct_ly = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_direct - adobe_cartStartByod_direct_ly) / adobe_cartStartByod_direct_ly, 6) END AS adobe_cartStartByod_direct_yoy_pct,

        adobe_cartStartByod_social,
        adobe_cartStartByod_social_wow,
        adobe_cartStartByod_social_ly,
        CASE WHEN adobe_cartStartByod_social_wow IS NULL OR adobe_cartStartByod_social_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_social - adobe_cartStartByod_social_wow) / adobe_cartStartByod_social_wow, 6) END AS adobe_cartStartByod_social_wow_pct,
        CASE WHEN adobe_cartStartByod_social_ly IS NULL OR adobe_cartStartByod_social_ly = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_social - adobe_cartStartByod_social_ly) / adobe_cartStartByod_social_ly, 6) END AS adobe_cartStartByod_social_yoy_pct,

        adobe_cartStartByod_programmatic,
        adobe_cartStartByod_programmatic_wow,
        adobe_cartStartByod_programmatic_ly,
        CASE WHEN adobe_cartStartByod_programmatic_wow IS NULL OR adobe_cartStartByod_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_programmatic - adobe_cartStartByod_programmatic_wow) / adobe_cartStartByod_programmatic_wow, 6) END AS adobe_cartStartByod_programmatic_wow_pct,
        CASE WHEN adobe_cartStartByod_programmatic_ly IS NULL OR adobe_cartStartByod_programmatic_ly = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_programmatic - adobe_cartStartByod_programmatic_ly) / adobe_cartStartByod_programmatic_ly, 6) END AS adobe_cartStartByod_programmatic_yoy_pct,

        adobe_cartStartByod_other,
        adobe_cartStartByod_other_wow,
        adobe_cartStartByod_other_ly,
        CASE WHEN adobe_cartStartByod_other_wow IS NULL OR adobe_cartStartByod_other_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_other - adobe_cartStartByod_other_wow) / adobe_cartStartByod_other_wow, 6) END AS adobe_cartStartByod_other_wow_pct,
        CASE WHEN adobe_cartStartByod_other_ly IS NULL OR adobe_cartStartByod_other_ly = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_other - adobe_cartStartByod_other_ly) / adobe_cartStartByod_other_ly, 6) END AS adobe_cartStartByod_other_yoy_pct,

        adobe_ordersTotal_allChannels,
        adobe_ordersTotal_allChannels_wow,
        adobe_ordersTotal_allChannels_ly,
        CASE WHEN adobe_ordersTotal_allChannels_wow IS NULL OR adobe_ordersTotal_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_ordersTotal_allChannels - adobe_ordersTotal_allChannels_wow) / adobe_ordersTotal_allChannels_wow, 6) END AS adobe_ordersTotal_allChannels_wow_pct,
        CASE WHEN adobe_ordersTotal_allChannels_ly IS NULL OR adobe_ordersTotal_allChannels_ly = 0 THEN NULL ELSE ROUND((adobe_ordersTotal_allChannels - adobe_ordersTotal_allChannels_ly) / adobe_ordersTotal_allChannels_ly, 6) END AS adobe_ordersTotal_allChannels_yoy_pct,

        adobe_ordersTotal_paidSearch,
        adobe_ordersTotal_paidSearch_wow,
        adobe_ordersTotal_paidSearch_ly,
        CASE WHEN adobe_ordersTotal_paidSearch_wow IS NULL OR adobe_ordersTotal_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersTotal_paidSearch - adobe_ordersTotal_paidSearch_wow) / adobe_ordersTotal_paidSearch_wow, 6) END AS adobe_ordersTotal_paidSearch_wow_pct,
        CASE WHEN adobe_ordersTotal_paidSearch_ly IS NULL OR adobe_ordersTotal_paidSearch_ly = 0 THEN NULL ELSE ROUND((adobe_ordersTotal_paidSearch - adobe_ordersTotal_paidSearch_ly) / adobe_ordersTotal_paidSearch_ly, 6) END AS adobe_ordersTotal_paidSearch_yoy_pct,

        adobe_ordersTotal_organicSearch,
        adobe_ordersTotal_organicSearch_wow,
        adobe_ordersTotal_organicSearch_ly,
        CASE WHEN adobe_ordersTotal_organicSearch_wow IS NULL OR adobe_ordersTotal_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersTotal_organicSearch - adobe_ordersTotal_organicSearch_wow) / adobe_ordersTotal_organicSearch_wow, 6) END AS adobe_ordersTotal_organicSearch_wow_pct,
        CASE WHEN adobe_ordersTotal_organicSearch_ly IS NULL OR adobe_ordersTotal_organicSearch_ly = 0 THEN NULL ELSE ROUND((adobe_ordersTotal_organicSearch - adobe_ordersTotal_organicSearch_ly) / adobe_ordersTotal_organicSearch_ly, 6) END AS adobe_ordersTotal_organicSearch_yoy_pct,

        adobe_ordersTotal_direct,
        adobe_ordersTotal_direct_wow,
        adobe_ordersTotal_direct_ly,
        CASE WHEN adobe_ordersTotal_direct_wow IS NULL OR adobe_ordersTotal_direct_wow = 0 THEN NULL ELSE ROUND((adobe_ordersTotal_direct - adobe_ordersTotal_direct_wow) / adobe_ordersTotal_direct_wow, 6) END AS adobe_ordersTotal_direct_wow_pct,
        CASE WHEN adobe_ordersTotal_direct_ly IS NULL OR adobe_ordersTotal_direct_ly = 0 THEN NULL ELSE ROUND((adobe_ordersTotal_direct - adobe_ordersTotal_direct_ly) / adobe_ordersTotal_direct_ly, 6) END AS adobe_ordersTotal_direct_yoy_pct,

        adobe_ordersTotal_social,
        adobe_ordersTotal_social_wow,
        adobe_ordersTotal_social_ly,
        CASE WHEN adobe_ordersTotal_social_wow IS NULL OR adobe_ordersTotal_social_wow = 0 THEN NULL ELSE ROUND((adobe_ordersTotal_social - adobe_ordersTotal_social_wow) / adobe_ordersTotal_social_wow, 6) END AS adobe_ordersTotal_social_wow_pct,
        CASE WHEN adobe_ordersTotal_social_ly IS NULL OR adobe_ordersTotal_social_ly = 0 THEN NULL ELSE ROUND((adobe_ordersTotal_social - adobe_ordersTotal_social_ly) / adobe_ordersTotal_social_ly, 6) END AS adobe_ordersTotal_social_yoy_pct,

        adobe_ordersTotal_programmatic,
        adobe_ordersTotal_programmatic_wow,
        adobe_ordersTotal_programmatic_ly,
        CASE WHEN adobe_ordersTotal_programmatic_wow IS NULL OR adobe_ordersTotal_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_ordersTotal_programmatic - adobe_ordersTotal_programmatic_wow) / adobe_ordersTotal_programmatic_wow, 6) END AS adobe_ordersTotal_programmatic_wow_pct,
        CASE WHEN adobe_ordersTotal_programmatic_ly IS NULL OR adobe_ordersTotal_programmatic_ly = 0 THEN NULL ELSE ROUND((adobe_ordersTotal_programmatic - adobe_ordersTotal_programmatic_ly) / adobe_ordersTotal_programmatic_ly, 6) END AS adobe_ordersTotal_programmatic_yoy_pct,

        adobe_ordersTotal_other,
        adobe_ordersTotal_other_wow,
        adobe_ordersTotal_other_ly,
        CASE WHEN adobe_ordersTotal_other_wow IS NULL OR adobe_ordersTotal_other_wow = 0 THEN NULL ELSE ROUND((adobe_ordersTotal_other - adobe_ordersTotal_other_wow) / adobe_ordersTotal_other_wow, 6) END AS adobe_ordersTotal_other_wow_pct,
        CASE WHEN adobe_ordersTotal_other_ly IS NULL OR adobe_ordersTotal_other_ly = 0 THEN NULL ELSE ROUND((adobe_ordersTotal_other - adobe_ordersTotal_other_ly) / adobe_ordersTotal_other_ly, 6) END AS adobe_ordersTotal_other_yoy_pct,

        adobe_ordersUnassistedTotal_allChannels,
        adobe_ordersUnassistedTotal_allChannels_wow,
        adobe_ordersUnassistedTotal_allChannels_ly,
        CASE WHEN adobe_ordersUnassistedTotal_allChannels_wow IS NULL OR adobe_ordersUnassistedTotal_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedTotal_allChannels - adobe_ordersUnassistedTotal_allChannels_wow) / adobe_ordersUnassistedTotal_allChannels_wow, 6) END AS adobe_ordersUnassistedTotal_allChannels_wow_pct,
        CASE WHEN adobe_ordersUnassistedTotal_allChannels_ly IS NULL OR adobe_ordersUnassistedTotal_allChannels_ly = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedTotal_allChannels - adobe_ordersUnassistedTotal_allChannels_ly) / adobe_ordersUnassistedTotal_allChannels_ly, 6) END AS adobe_ordersUnassistedTotal_allChannels_yoy_pct,

        adobe_ordersUnassistedTotal_paidSearch,
        adobe_ordersUnassistedTotal_paidSearch_wow,
        adobe_ordersUnassistedTotal_paidSearch_ly,
        CASE WHEN adobe_ordersUnassistedTotal_paidSearch_wow IS NULL OR adobe_ordersUnassistedTotal_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedTotal_paidSearch - adobe_ordersUnassistedTotal_paidSearch_wow) / adobe_ordersUnassistedTotal_paidSearch_wow, 6) END AS adobe_ordersUnassistedTotal_paidSearch_wow_pct,
        CASE WHEN adobe_ordersUnassistedTotal_paidSearch_ly IS NULL OR adobe_ordersUnassistedTotal_paidSearch_ly = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedTotal_paidSearch - adobe_ordersUnassistedTotal_paidSearch_ly) / adobe_ordersUnassistedTotal_paidSearch_ly, 6) END AS adobe_ordersUnassistedTotal_paidSearch_yoy_pct,

        adobe_ordersUnassistedTotal_organicSearch,
        adobe_ordersUnassistedTotal_organicSearch_wow,
        adobe_ordersUnassistedTotal_organicSearch_ly,
        CASE WHEN adobe_ordersUnassistedTotal_organicSearch_wow IS NULL OR adobe_ordersUnassistedTotal_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedTotal_organicSearch - adobe_ordersUnassistedTotal_organicSearch_wow) / adobe_ordersUnassistedTotal_organicSearch_wow, 6) END AS adobe_ordersUnassistedTotal_organicSearch_wow_pct,
        CASE WHEN adobe_ordersUnassistedTotal_organicSearch_ly IS NULL OR adobe_ordersUnassistedTotal_organicSearch_ly = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedTotal_organicSearch - adobe_ordersUnassistedTotal_organicSearch_ly) / adobe_ordersUnassistedTotal_organicSearch_ly, 6) END AS adobe_ordersUnassistedTotal_organicSearch_yoy_pct,

        adobe_ordersUnassistedTotal_direct,
        adobe_ordersUnassistedTotal_direct_wow,
        adobe_ordersUnassistedTotal_direct_ly,
        CASE WHEN adobe_ordersUnassistedTotal_direct_wow IS NULL OR adobe_ordersUnassistedTotal_direct_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedTotal_direct - adobe_ordersUnassistedTotal_direct_wow) / adobe_ordersUnassistedTotal_direct_wow, 6) END AS adobe_ordersUnassistedTotal_direct_wow_pct,
        CASE WHEN adobe_ordersUnassistedTotal_direct_ly IS NULL OR adobe_ordersUnassistedTotal_direct_ly = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedTotal_direct - adobe_ordersUnassistedTotal_direct_ly) / adobe_ordersUnassistedTotal_direct_ly, 6) END AS adobe_ordersUnassistedTotal_direct_yoy_pct,

        adobe_ordersUnassistedTotal_social,
        adobe_ordersUnassistedTotal_social_wow,
        adobe_ordersUnassistedTotal_social_ly,
        CASE WHEN adobe_ordersUnassistedTotal_social_wow IS NULL OR adobe_ordersUnassistedTotal_social_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedTotal_social - adobe_ordersUnassistedTotal_social_wow) / adobe_ordersUnassistedTotal_social_wow, 6) END AS adobe_ordersUnassistedTotal_social_wow_pct,
        CASE WHEN adobe_ordersUnassistedTotal_social_ly IS NULL OR adobe_ordersUnassistedTotal_social_ly = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedTotal_social - adobe_ordersUnassistedTotal_social_ly) / adobe_ordersUnassistedTotal_social_ly, 6) END AS adobe_ordersUnassistedTotal_social_yoy_pct,

        adobe_ordersUnassistedTotal_programmatic,
        adobe_ordersUnassistedTotal_programmatic_wow,
        adobe_ordersUnassistedTotal_programmatic_ly,
        CASE WHEN adobe_ordersUnassistedTotal_programmatic_wow IS NULL OR adobe_ordersUnassistedTotal_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedTotal_programmatic - adobe_ordersUnassistedTotal_programmatic_wow) / adobe_ordersUnassistedTotal_programmatic_wow, 6) END AS adobe_ordersUnassistedTotal_programmatic_wow_pct,
        CASE WHEN adobe_ordersUnassistedTotal_programmatic_ly IS NULL OR adobe_ordersUnassistedTotal_programmatic_ly = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedTotal_programmatic - adobe_ordersUnassistedTotal_programmatic_ly) / adobe_ordersUnassistedTotal_programmatic_ly, 6) END AS adobe_ordersUnassistedTotal_programmatic_yoy_pct,

        adobe_ordersUnassistedTotal_other,
        adobe_ordersUnassistedTotal_other_wow,
        adobe_ordersUnassistedTotal_other_ly,
        CASE WHEN adobe_ordersUnassistedTotal_other_wow IS NULL OR adobe_ordersUnassistedTotal_other_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedTotal_other - adobe_ordersUnassistedTotal_other_wow) / adobe_ordersUnassistedTotal_other_wow, 6) END AS adobe_ordersUnassistedTotal_other_wow_pct,
        CASE WHEN adobe_ordersUnassistedTotal_other_ly IS NULL OR adobe_ordersUnassistedTotal_other_ly = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedTotal_other - adobe_ordersUnassistedTotal_other_ly) / adobe_ordersUnassistedTotal_other_ly, 6) END AS adobe_ordersUnassistedTotal_other_yoy_pct,

        adobe_ordersUnassistedPostpaid_allChannels,
        adobe_ordersUnassistedPostpaid_allChannels_wow,
        adobe_ordersUnassistedPostpaid_allChannels_ly,
        CASE WHEN adobe_ordersUnassistedPostpaid_allChannels_wow IS NULL OR adobe_ordersUnassistedPostpaid_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedPostpaid_allChannels - adobe_ordersUnassistedPostpaid_allChannels_wow) / adobe_ordersUnassistedPostpaid_allChannels_wow, 6) END AS adobe_ordersUnassistedPostpaid_allChannels_wow_pct,
        CASE WHEN adobe_ordersUnassistedPostpaid_allChannels_ly IS NULL OR adobe_ordersUnassistedPostpaid_allChannels_ly = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedPostpaid_allChannels - adobe_ordersUnassistedPostpaid_allChannels_ly) / adobe_ordersUnassistedPostpaid_allChannels_ly, 6) END AS adobe_ordersUnassistedPostpaid_allChannels_yoy_pct,

        adobe_ordersUnassistedPostpaid_paidSearch,
        adobe_ordersUnassistedPostpaid_paidSearch_wow,
        adobe_ordersUnassistedPostpaid_paidSearch_ly,
        CASE WHEN adobe_ordersUnassistedPostpaid_paidSearch_wow IS NULL OR adobe_ordersUnassistedPostpaid_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedPostpaid_paidSearch - adobe_ordersUnassistedPostpaid_paidSearch_wow) / adobe_ordersUnassistedPostpaid_paidSearch_wow, 6) END AS adobe_ordersUnassistedPostpaid_paidSearch_wow_pct,
        CASE WHEN adobe_ordersUnassistedPostpaid_paidSearch_ly IS NULL OR adobe_ordersUnassistedPostpaid_paidSearch_ly = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedPostpaid_paidSearch - adobe_ordersUnassistedPostpaid_paidSearch_ly) / adobe_ordersUnassistedPostpaid_paidSearch_ly, 6) END AS adobe_ordersUnassistedPostpaid_paidSearch_yoy_pct,

        adobe_ordersUnassistedPostpaid_organicSearch,
        adobe_ordersUnassistedPostpaid_organicSearch_wow,
        adobe_ordersUnassistedPostpaid_organicSearch_ly,
        CASE WHEN adobe_ordersUnassistedPostpaid_organicSearch_wow IS NULL OR adobe_ordersUnassistedPostpaid_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedPostpaid_organicSearch - adobe_ordersUnassistedPostpaid_organicSearch_wow) / adobe_ordersUnassistedPostpaid_organicSearch_wow, 6) END AS adobe_ordersUnassistedPostpaid_organicSearch_wow_pct,
        CASE WHEN adobe_ordersUnassistedPostpaid_organicSearch_ly IS NULL OR adobe_ordersUnassistedPostpaid_organicSearch_ly = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedPostpaid_organicSearch - adobe_ordersUnassistedPostpaid_organicSearch_ly) / adobe_ordersUnassistedPostpaid_organicSearch_ly, 6) END AS adobe_ordersUnassistedPostpaid_organicSearch_yoy_pct,

        adobe_ordersUnassistedPostpaid_direct,
        adobe_ordersUnassistedPostpaid_direct_wow,
        adobe_ordersUnassistedPostpaid_direct_ly,
        CASE WHEN adobe_ordersUnassistedPostpaid_direct_wow IS NULL OR adobe_ordersUnassistedPostpaid_direct_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedPostpaid_direct - adobe_ordersUnassistedPostpaid_direct_wow) / adobe_ordersUnassistedPostpaid_direct_wow, 6) END AS adobe_ordersUnassistedPostpaid_direct_wow_pct,
        CASE WHEN adobe_ordersUnassistedPostpaid_direct_ly IS NULL OR adobe_ordersUnassistedPostpaid_direct_ly = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedPostpaid_direct - adobe_ordersUnassistedPostpaid_direct_ly) / adobe_ordersUnassistedPostpaid_direct_ly, 6) END AS adobe_ordersUnassistedPostpaid_direct_yoy_pct,

        adobe_ordersUnassistedPostpaid_social,
        adobe_ordersUnassistedPostpaid_social_wow,
        adobe_ordersUnassistedPostpaid_social_ly,
        CASE WHEN adobe_ordersUnassistedPostpaid_social_wow IS NULL OR adobe_ordersUnassistedPostpaid_social_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedPostpaid_social - adobe_ordersUnassistedPostpaid_social_wow) / adobe_ordersUnassistedPostpaid_social_wow, 6) END AS adobe_ordersUnassistedPostpaid_social_wow_pct,
        CASE WHEN adobe_ordersUnassistedPostpaid_social_ly IS NULL OR adobe_ordersUnassistedPostpaid_social_ly = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedPostpaid_social - adobe_ordersUnassistedPostpaid_social_ly) / adobe_ordersUnassistedPostpaid_social_ly, 6) END AS adobe_ordersUnassistedPostpaid_social_yoy_pct,

        adobe_ordersUnassistedPostpaid_programmatic,
        adobe_ordersUnassistedPostpaid_programmatic_wow,
        adobe_ordersUnassistedPostpaid_programmatic_ly,
        CASE WHEN adobe_ordersUnassistedPostpaid_programmatic_wow IS NULL OR adobe_ordersUnassistedPostpaid_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedPostpaid_programmatic - adobe_ordersUnassistedPostpaid_programmatic_wow) / adobe_ordersUnassistedPostpaid_programmatic_wow, 6) END AS adobe_ordersUnassistedPostpaid_programmatic_wow_pct,
        CASE WHEN adobe_ordersUnassistedPostpaid_programmatic_ly IS NULL OR adobe_ordersUnassistedPostpaid_programmatic_ly = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedPostpaid_programmatic - adobe_ordersUnassistedPostpaid_programmatic_ly) / adobe_ordersUnassistedPostpaid_programmatic_ly, 6) END AS adobe_ordersUnassistedPostpaid_programmatic_yoy_pct,

        adobe_ordersUnassistedPostpaid_other,
        adobe_ordersUnassistedPostpaid_other_wow,
        adobe_ordersUnassistedPostpaid_other_ly,
        CASE WHEN adobe_ordersUnassistedPostpaid_other_wow IS NULL OR adobe_ordersUnassistedPostpaid_other_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedPostpaid_other - adobe_ordersUnassistedPostpaid_other_wow) / adobe_ordersUnassistedPostpaid_other_wow, 6) END AS adobe_ordersUnassistedPostpaid_other_wow_pct,
        CASE WHEN adobe_ordersUnassistedPostpaid_other_ly IS NULL OR adobe_ordersUnassistedPostpaid_other_ly = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedPostpaid_other - adobe_ordersUnassistedPostpaid_other_ly) / adobe_ordersUnassistedPostpaid_other_ly, 6) END AS adobe_ordersUnassistedPostpaid_other_yoy_pct,

        adobe_ordersUnassistedHsi_allChannels,
        adobe_ordersUnassistedHsi_allChannels_wow,
        adobe_ordersUnassistedHsi_allChannels_ly,
        CASE WHEN adobe_ordersUnassistedHsi_allChannels_wow IS NULL OR adobe_ordersUnassistedHsi_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedHsi_allChannels - adobe_ordersUnassistedHsi_allChannels_wow) / adobe_ordersUnassistedHsi_allChannels_wow, 6) END AS adobe_ordersUnassistedHsi_allChannels_wow_pct,
        CASE WHEN adobe_ordersUnassistedHsi_allChannels_ly IS NULL OR adobe_ordersUnassistedHsi_allChannels_ly = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedHsi_allChannels - adobe_ordersUnassistedHsi_allChannels_ly) / adobe_ordersUnassistedHsi_allChannels_ly, 6) END AS adobe_ordersUnassistedHsi_allChannels_yoy_pct,

        adobe_ordersUnassistedHsi_paidSearch,
        adobe_ordersUnassistedHsi_paidSearch_wow,
        adobe_ordersUnassistedHsi_paidSearch_ly,
        CASE WHEN adobe_ordersUnassistedHsi_paidSearch_wow IS NULL OR adobe_ordersUnassistedHsi_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedHsi_paidSearch - adobe_ordersUnassistedHsi_paidSearch_wow) / adobe_ordersUnassistedHsi_paidSearch_wow, 6) END AS adobe_ordersUnassistedHsi_paidSearch_wow_pct,
        CASE WHEN adobe_ordersUnassistedHsi_paidSearch_ly IS NULL OR adobe_ordersUnassistedHsi_paidSearch_ly = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedHsi_paidSearch - adobe_ordersUnassistedHsi_paidSearch_ly) / adobe_ordersUnassistedHsi_paidSearch_ly, 6) END AS adobe_ordersUnassistedHsi_paidSearch_yoy_pct,

        adobe_ordersUnassistedHsi_organicSearch,
        adobe_ordersUnassistedHsi_organicSearch_wow,
        adobe_ordersUnassistedHsi_organicSearch_ly,
        CASE WHEN adobe_ordersUnassistedHsi_organicSearch_wow IS NULL OR adobe_ordersUnassistedHsi_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedHsi_organicSearch - adobe_ordersUnassistedHsi_organicSearch_wow) / adobe_ordersUnassistedHsi_organicSearch_wow, 6) END AS adobe_ordersUnassistedHsi_organicSearch_wow_pct,
        CASE WHEN adobe_ordersUnassistedHsi_organicSearch_ly IS NULL OR adobe_ordersUnassistedHsi_organicSearch_ly = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedHsi_organicSearch - adobe_ordersUnassistedHsi_organicSearch_ly) / adobe_ordersUnassistedHsi_organicSearch_ly, 6) END AS adobe_ordersUnassistedHsi_organicSearch_yoy_pct,

        adobe_ordersUnassistedHsi_direct,
        adobe_ordersUnassistedHsi_direct_wow,
        adobe_ordersUnassistedHsi_direct_ly,
        CASE WHEN adobe_ordersUnassistedHsi_direct_wow IS NULL OR adobe_ordersUnassistedHsi_direct_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedHsi_direct - adobe_ordersUnassistedHsi_direct_wow) / adobe_ordersUnassistedHsi_direct_wow, 6) END AS adobe_ordersUnassistedHsi_direct_wow_pct,
        CASE WHEN adobe_ordersUnassistedHsi_direct_ly IS NULL OR adobe_ordersUnassistedHsi_direct_ly = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedHsi_direct - adobe_ordersUnassistedHsi_direct_ly) / adobe_ordersUnassistedHsi_direct_ly, 6) END AS adobe_ordersUnassistedHsi_direct_yoy_pct,

        adobe_ordersUnassistedHsi_social,
        adobe_ordersUnassistedHsi_social_wow,
        adobe_ordersUnassistedHsi_social_ly,
        CASE WHEN adobe_ordersUnassistedHsi_social_wow IS NULL OR adobe_ordersUnassistedHsi_social_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedHsi_social - adobe_ordersUnassistedHsi_social_wow) / adobe_ordersUnassistedHsi_social_wow, 6) END AS adobe_ordersUnassistedHsi_social_wow_pct,
        CASE WHEN adobe_ordersUnassistedHsi_social_ly IS NULL OR adobe_ordersUnassistedHsi_social_ly = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedHsi_social - adobe_ordersUnassistedHsi_social_ly) / adobe_ordersUnassistedHsi_social_ly, 6) END AS adobe_ordersUnassistedHsi_social_yoy_pct,

        adobe_ordersUnassistedHsi_programmatic,
        adobe_ordersUnassistedHsi_programmatic_wow,
        adobe_ordersUnassistedHsi_programmatic_ly,
        CASE WHEN adobe_ordersUnassistedHsi_programmatic_wow IS NULL OR adobe_ordersUnassistedHsi_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedHsi_programmatic - adobe_ordersUnassistedHsi_programmatic_wow) / adobe_ordersUnassistedHsi_programmatic_wow, 6) END AS adobe_ordersUnassistedHsi_programmatic_wow_pct,
        CASE WHEN adobe_ordersUnassistedHsi_programmatic_ly IS NULL OR adobe_ordersUnassistedHsi_programmatic_ly = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedHsi_programmatic - adobe_ordersUnassistedHsi_programmatic_ly) / adobe_ordersUnassistedHsi_programmatic_ly, 6) END AS adobe_ordersUnassistedHsi_programmatic_yoy_pct,

        adobe_ordersUnassistedHsi_other,
        adobe_ordersUnassistedHsi_other_wow,
        adobe_ordersUnassistedHsi_other_ly,
        CASE WHEN adobe_ordersUnassistedHsi_other_wow IS NULL OR adobe_ordersUnassistedHsi_other_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedHsi_other - adobe_ordersUnassistedHsi_other_wow) / adobe_ordersUnassistedHsi_other_wow, 6) END AS adobe_ordersUnassistedHsi_other_wow_pct,
        CASE WHEN adobe_ordersUnassistedHsi_other_ly IS NULL OR adobe_ordersUnassistedHsi_other_ly = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedHsi_other - adobe_ordersUnassistedHsi_other_ly) / adobe_ordersUnassistedHsi_other_ly, 6) END AS adobe_ordersUnassistedHsi_other_yoy_pct,

        adobe_ordersUnassistedByod_allChannels,
        adobe_ordersUnassistedByod_allChannels_wow,
        adobe_ordersUnassistedByod_allChannels_ly,
        CASE WHEN adobe_ordersUnassistedByod_allChannels_wow IS NULL OR adobe_ordersUnassistedByod_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_allChannels - adobe_ordersUnassistedByod_allChannels_wow) / adobe_ordersUnassistedByod_allChannels_wow, 6) END AS adobe_ordersUnassistedByod_allChannels_wow_pct,
        CASE WHEN adobe_ordersUnassistedByod_allChannels_ly IS NULL OR adobe_ordersUnassistedByod_allChannels_ly = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_allChannels - adobe_ordersUnassistedByod_allChannels_ly) / adobe_ordersUnassistedByod_allChannels_ly, 6) END AS adobe_ordersUnassistedByod_allChannels_yoy_pct,

        adobe_ordersUnassistedByod_paidSearch,
        adobe_ordersUnassistedByod_paidSearch_wow,
        adobe_ordersUnassistedByod_paidSearch_ly,
        CASE WHEN adobe_ordersUnassistedByod_paidSearch_wow IS NULL OR adobe_ordersUnassistedByod_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_paidSearch - adobe_ordersUnassistedByod_paidSearch_wow) / adobe_ordersUnassistedByod_paidSearch_wow, 6) END AS adobe_ordersUnassistedByod_paidSearch_wow_pct,
        CASE WHEN adobe_ordersUnassistedByod_paidSearch_ly IS NULL OR adobe_ordersUnassistedByod_paidSearch_ly = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_paidSearch - adobe_ordersUnassistedByod_paidSearch_ly) / adobe_ordersUnassistedByod_paidSearch_ly, 6) END AS adobe_ordersUnassistedByod_paidSearch_yoy_pct,

        adobe_ordersUnassistedByod_organicSearch,
        adobe_ordersUnassistedByod_organicSearch_wow,
        adobe_ordersUnassistedByod_organicSearch_ly,
        CASE WHEN adobe_ordersUnassistedByod_organicSearch_wow IS NULL OR adobe_ordersUnassistedByod_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_organicSearch - adobe_ordersUnassistedByod_organicSearch_wow) / adobe_ordersUnassistedByod_organicSearch_wow, 6) END AS adobe_ordersUnassistedByod_organicSearch_wow_pct,
        CASE WHEN adobe_ordersUnassistedByod_organicSearch_ly IS NULL OR adobe_ordersUnassistedByod_organicSearch_ly = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_organicSearch - adobe_ordersUnassistedByod_organicSearch_ly) / adobe_ordersUnassistedByod_organicSearch_ly, 6) END AS adobe_ordersUnassistedByod_organicSearch_yoy_pct,

        adobe_ordersUnassistedByod_direct,
        adobe_ordersUnassistedByod_direct_wow,
        adobe_ordersUnassistedByod_direct_ly,
        CASE WHEN adobe_ordersUnassistedByod_direct_wow IS NULL OR adobe_ordersUnassistedByod_direct_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_direct - adobe_ordersUnassistedByod_direct_wow) / adobe_ordersUnassistedByod_direct_wow, 6) END AS adobe_ordersUnassistedByod_direct_wow_pct,
        CASE WHEN adobe_ordersUnassistedByod_direct_ly IS NULL OR adobe_ordersUnassistedByod_direct_ly = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_direct - adobe_ordersUnassistedByod_direct_ly) / adobe_ordersUnassistedByod_direct_ly, 6) END AS adobe_ordersUnassistedByod_direct_yoy_pct,

        adobe_ordersUnassistedByod_social,
        adobe_ordersUnassistedByod_social_wow,
        adobe_ordersUnassistedByod_social_ly,
        CASE WHEN adobe_ordersUnassistedByod_social_wow IS NULL OR adobe_ordersUnassistedByod_social_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_social - adobe_ordersUnassistedByod_social_wow) / adobe_ordersUnassistedByod_social_wow, 6) END AS adobe_ordersUnassistedByod_social_wow_pct,
        CASE WHEN adobe_ordersUnassistedByod_social_ly IS NULL OR adobe_ordersUnassistedByod_social_ly = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_social - adobe_ordersUnassistedByod_social_ly) / adobe_ordersUnassistedByod_social_ly, 6) END AS adobe_ordersUnassistedByod_social_yoy_pct,

        adobe_ordersUnassistedByod_programmatic,
        adobe_ordersUnassistedByod_programmatic_wow,
        adobe_ordersUnassistedByod_programmatic_ly,
        CASE WHEN adobe_ordersUnassistedByod_programmatic_wow IS NULL OR adobe_ordersUnassistedByod_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_programmatic - adobe_ordersUnassistedByod_programmatic_wow) / adobe_ordersUnassistedByod_programmatic_wow, 6) END AS adobe_ordersUnassistedByod_programmatic_wow_pct,
        CASE WHEN adobe_ordersUnassistedByod_programmatic_ly IS NULL OR adobe_ordersUnassistedByod_programmatic_ly = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_programmatic - adobe_ordersUnassistedByod_programmatic_ly) / adobe_ordersUnassistedByod_programmatic_ly, 6) END AS adobe_ordersUnassistedByod_programmatic_yoy_pct,

        adobe_ordersUnassistedByod_other,
        adobe_ordersUnassistedByod_other_wow,
        adobe_ordersUnassistedByod_other_ly,
        CASE WHEN adobe_ordersUnassistedByod_other_wow IS NULL OR adobe_ordersUnassistedByod_other_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_other - adobe_ordersUnassistedByod_other_wow) / adobe_ordersUnassistedByod_other_wow, 6) END AS adobe_ordersUnassistedByod_other_wow_pct,
        CASE WHEN adobe_ordersUnassistedByod_other_ly IS NULL OR adobe_ordersUnassistedByod_other_ly = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_other - adobe_ordersUnassistedByod_other_ly) / adobe_ordersUnassistedByod_other_ly, 6) END AS adobe_ordersUnassistedByod_other_yoy_pct,

        adobe_ordersAssistedTotal_allChannels,
        adobe_ordersAssistedTotal_allChannels_wow,
        adobe_ordersAssistedTotal_allChannels_ly,
        CASE WHEN adobe_ordersAssistedTotal_allChannels_wow IS NULL OR adobe_ordersAssistedTotal_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedTotal_allChannels - adobe_ordersAssistedTotal_allChannels_wow) / adobe_ordersAssistedTotal_allChannels_wow, 6) END AS adobe_ordersAssistedTotal_allChannels_wow_pct,
        CASE WHEN adobe_ordersAssistedTotal_allChannels_ly IS NULL OR adobe_ordersAssistedTotal_allChannels_ly = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedTotal_allChannels - adobe_ordersAssistedTotal_allChannels_ly) / adobe_ordersAssistedTotal_allChannels_ly, 6) END AS adobe_ordersAssistedTotal_allChannels_yoy_pct,

        adobe_ordersAssistedTotal_paidSearch,
        adobe_ordersAssistedTotal_paidSearch_wow,
        adobe_ordersAssistedTotal_paidSearch_ly,
        CASE WHEN adobe_ordersAssistedTotal_paidSearch_wow IS NULL OR adobe_ordersAssistedTotal_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedTotal_paidSearch - adobe_ordersAssistedTotal_paidSearch_wow) / adobe_ordersAssistedTotal_paidSearch_wow, 6) END AS adobe_ordersAssistedTotal_paidSearch_wow_pct,
        CASE WHEN adobe_ordersAssistedTotal_paidSearch_ly IS NULL OR adobe_ordersAssistedTotal_paidSearch_ly = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedTotal_paidSearch - adobe_ordersAssistedTotal_paidSearch_ly) / adobe_ordersAssistedTotal_paidSearch_ly, 6) END AS adobe_ordersAssistedTotal_paidSearch_yoy_pct,

        adobe_ordersAssistedTotal_organicSearch,
        adobe_ordersAssistedTotal_organicSearch_wow,
        adobe_ordersAssistedTotal_organicSearch_ly,
        CASE WHEN adobe_ordersAssistedTotal_organicSearch_wow IS NULL OR adobe_ordersAssistedTotal_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedTotal_organicSearch - adobe_ordersAssistedTotal_organicSearch_wow) / adobe_ordersAssistedTotal_organicSearch_wow, 6) END AS adobe_ordersAssistedTotal_organicSearch_wow_pct,
        CASE WHEN adobe_ordersAssistedTotal_organicSearch_ly IS NULL OR adobe_ordersAssistedTotal_organicSearch_ly = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedTotal_organicSearch - adobe_ordersAssistedTotal_organicSearch_ly) / adobe_ordersAssistedTotal_organicSearch_ly, 6) END AS adobe_ordersAssistedTotal_organicSearch_yoy_pct,

        adobe_ordersAssistedTotal_direct,
        adobe_ordersAssistedTotal_direct_wow,
        adobe_ordersAssistedTotal_direct_ly,
        CASE WHEN adobe_ordersAssistedTotal_direct_wow IS NULL OR adobe_ordersAssistedTotal_direct_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedTotal_direct - adobe_ordersAssistedTotal_direct_wow) / adobe_ordersAssistedTotal_direct_wow, 6) END AS adobe_ordersAssistedTotal_direct_wow_pct,
        CASE WHEN adobe_ordersAssistedTotal_direct_ly IS NULL OR adobe_ordersAssistedTotal_direct_ly = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedTotal_direct - adobe_ordersAssistedTotal_direct_ly) / adobe_ordersAssistedTotal_direct_ly, 6) END AS adobe_ordersAssistedTotal_direct_yoy_pct,

        adobe_ordersAssistedTotal_social,
        adobe_ordersAssistedTotal_social_wow,
        adobe_ordersAssistedTotal_social_ly,
        CASE WHEN adobe_ordersAssistedTotal_social_wow IS NULL OR adobe_ordersAssistedTotal_social_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedTotal_social - adobe_ordersAssistedTotal_social_wow) / adobe_ordersAssistedTotal_social_wow, 6) END AS adobe_ordersAssistedTotal_social_wow_pct,
        CASE WHEN adobe_ordersAssistedTotal_social_ly IS NULL OR adobe_ordersAssistedTotal_social_ly = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedTotal_social - adobe_ordersAssistedTotal_social_ly) / adobe_ordersAssistedTotal_social_ly, 6) END AS adobe_ordersAssistedTotal_social_yoy_pct,

        adobe_ordersAssistedTotal_programmatic,
        adobe_ordersAssistedTotal_programmatic_wow,
        adobe_ordersAssistedTotal_programmatic_ly,
        CASE WHEN adobe_ordersAssistedTotal_programmatic_wow IS NULL OR adobe_ordersAssistedTotal_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedTotal_programmatic - adobe_ordersAssistedTotal_programmatic_wow) / adobe_ordersAssistedTotal_programmatic_wow, 6) END AS adobe_ordersAssistedTotal_programmatic_wow_pct,
        CASE WHEN adobe_ordersAssistedTotal_programmatic_ly IS NULL OR adobe_ordersAssistedTotal_programmatic_ly = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedTotal_programmatic - adobe_ordersAssistedTotal_programmatic_ly) / adobe_ordersAssistedTotal_programmatic_ly, 6) END AS adobe_ordersAssistedTotal_programmatic_yoy_pct,

        adobe_ordersAssistedTotal_other,
        adobe_ordersAssistedTotal_other_wow,
        adobe_ordersAssistedTotal_other_ly,
        CASE WHEN adobe_ordersAssistedTotal_other_wow IS NULL OR adobe_ordersAssistedTotal_other_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedTotal_other - adobe_ordersAssistedTotal_other_wow) / adobe_ordersAssistedTotal_other_wow, 6) END AS adobe_ordersAssistedTotal_other_wow_pct,
        CASE WHEN adobe_ordersAssistedTotal_other_ly IS NULL OR adobe_ordersAssistedTotal_other_ly = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedTotal_other - adobe_ordersAssistedTotal_other_ly) / adobe_ordersAssistedTotal_other_ly, 6) END AS adobe_ordersAssistedTotal_other_yoy_pct,

        adobe_ordersAssistedPostpaid_allChannels,
        adobe_ordersAssistedPostpaid_allChannels_wow,
        adobe_ordersAssistedPostpaid_allChannels_ly,
        CASE WHEN adobe_ordersAssistedPostpaid_allChannels_wow IS NULL OR adobe_ordersAssistedPostpaid_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedPostpaid_allChannels - adobe_ordersAssistedPostpaid_allChannels_wow) / adobe_ordersAssistedPostpaid_allChannels_wow, 6) END AS adobe_ordersAssistedPostpaid_allChannels_wow_pct,
        CASE WHEN adobe_ordersAssistedPostpaid_allChannels_ly IS NULL OR adobe_ordersAssistedPostpaid_allChannels_ly = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedPostpaid_allChannels - adobe_ordersAssistedPostpaid_allChannels_ly) / adobe_ordersAssistedPostpaid_allChannels_ly, 6) END AS adobe_ordersAssistedPostpaid_allChannels_yoy_pct,

        adobe_ordersAssistedPostpaid_paidSearch,
        adobe_ordersAssistedPostpaid_paidSearch_wow,
        adobe_ordersAssistedPostpaid_paidSearch_ly,
        CASE WHEN adobe_ordersAssistedPostpaid_paidSearch_wow IS NULL OR adobe_ordersAssistedPostpaid_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedPostpaid_paidSearch - adobe_ordersAssistedPostpaid_paidSearch_wow) / adobe_ordersAssistedPostpaid_paidSearch_wow, 6) END AS adobe_ordersAssistedPostpaid_paidSearch_wow_pct,
        CASE WHEN adobe_ordersAssistedPostpaid_paidSearch_ly IS NULL OR adobe_ordersAssistedPostpaid_paidSearch_ly = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedPostpaid_paidSearch - adobe_ordersAssistedPostpaid_paidSearch_ly) / adobe_ordersAssistedPostpaid_paidSearch_ly, 6) END AS adobe_ordersAssistedPostpaid_paidSearch_yoy_pct,

        adobe_ordersAssistedPostpaid_organicSearch,
        adobe_ordersAssistedPostpaid_organicSearch_wow,
        adobe_ordersAssistedPostpaid_organicSearch_ly,
        CASE WHEN adobe_ordersAssistedPostpaid_organicSearch_wow IS NULL OR adobe_ordersAssistedPostpaid_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedPostpaid_organicSearch - adobe_ordersAssistedPostpaid_organicSearch_wow) / adobe_ordersAssistedPostpaid_organicSearch_wow, 6) END AS adobe_ordersAssistedPostpaid_organicSearch_wow_pct,
        CASE WHEN adobe_ordersAssistedPostpaid_organicSearch_ly IS NULL OR adobe_ordersAssistedPostpaid_organicSearch_ly = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedPostpaid_organicSearch - adobe_ordersAssistedPostpaid_organicSearch_ly) / adobe_ordersAssistedPostpaid_organicSearch_ly, 6) END AS adobe_ordersAssistedPostpaid_organicSearch_yoy_pct,

        adobe_ordersAssistedPostpaid_direct,
        adobe_ordersAssistedPostpaid_direct_wow,
        adobe_ordersAssistedPostpaid_direct_ly,
        CASE WHEN adobe_ordersAssistedPostpaid_direct_wow IS NULL OR adobe_ordersAssistedPostpaid_direct_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedPostpaid_direct - adobe_ordersAssistedPostpaid_direct_wow) / adobe_ordersAssistedPostpaid_direct_wow, 6) END AS adobe_ordersAssistedPostpaid_direct_wow_pct,
        CASE WHEN adobe_ordersAssistedPostpaid_direct_ly IS NULL OR adobe_ordersAssistedPostpaid_direct_ly = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedPostpaid_direct - adobe_ordersAssistedPostpaid_direct_ly) / adobe_ordersAssistedPostpaid_direct_ly, 6) END AS adobe_ordersAssistedPostpaid_direct_yoy_pct,

        adobe_ordersAssistedPostpaid_social,
        adobe_ordersAssistedPostpaid_social_wow,
        adobe_ordersAssistedPostpaid_social_ly,
        CASE WHEN adobe_ordersAssistedPostpaid_social_wow IS NULL OR adobe_ordersAssistedPostpaid_social_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedPostpaid_social - adobe_ordersAssistedPostpaid_social_wow) / adobe_ordersAssistedPostpaid_social_wow, 6) END AS adobe_ordersAssistedPostpaid_social_wow_pct,
        CASE WHEN adobe_ordersAssistedPostpaid_social_ly IS NULL OR adobe_ordersAssistedPostpaid_social_ly = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedPostpaid_social - adobe_ordersAssistedPostpaid_social_ly) / adobe_ordersAssistedPostpaid_social_ly, 6) END AS adobe_ordersAssistedPostpaid_social_yoy_pct,

        adobe_ordersAssistedPostpaid_programmatic,
        adobe_ordersAssistedPostpaid_programmatic_wow,
        adobe_ordersAssistedPostpaid_programmatic_ly,
        CASE WHEN adobe_ordersAssistedPostpaid_programmatic_wow IS NULL OR adobe_ordersAssistedPostpaid_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedPostpaid_programmatic - adobe_ordersAssistedPostpaid_programmatic_wow) / adobe_ordersAssistedPostpaid_programmatic_wow, 6) END AS adobe_ordersAssistedPostpaid_programmatic_wow_pct,
        CASE WHEN adobe_ordersAssistedPostpaid_programmatic_ly IS NULL OR adobe_ordersAssistedPostpaid_programmatic_ly = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedPostpaid_programmatic - adobe_ordersAssistedPostpaid_programmatic_ly) / adobe_ordersAssistedPostpaid_programmatic_ly, 6) END AS adobe_ordersAssistedPostpaid_programmatic_yoy_pct,

        adobe_ordersAssistedPostpaid_other,
        adobe_ordersAssistedPostpaid_other_wow,
        adobe_ordersAssistedPostpaid_other_ly,
        CASE WHEN adobe_ordersAssistedPostpaid_other_wow IS NULL OR adobe_ordersAssistedPostpaid_other_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedPostpaid_other - adobe_ordersAssistedPostpaid_other_wow) / adobe_ordersAssistedPostpaid_other_wow, 6) END AS adobe_ordersAssistedPostpaid_other_wow_pct,
        CASE WHEN adobe_ordersAssistedPostpaid_other_ly IS NULL OR adobe_ordersAssistedPostpaid_other_ly = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedPostpaid_other - adobe_ordersAssistedPostpaid_other_ly) / adobe_ordersAssistedPostpaid_other_ly, 6) END AS adobe_ordersAssistedPostpaid_other_yoy_pct,

        adobe_ordersAssistedHsi_allChannels,
        adobe_ordersAssistedHsi_allChannels_wow,
        adobe_ordersAssistedHsi_allChannels_ly,
        CASE WHEN adobe_ordersAssistedHsi_allChannels_wow IS NULL OR adobe_ordersAssistedHsi_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedHsi_allChannels - adobe_ordersAssistedHsi_allChannels_wow) / adobe_ordersAssistedHsi_allChannels_wow, 6) END AS adobe_ordersAssistedHsi_allChannels_wow_pct,
        CASE WHEN adobe_ordersAssistedHsi_allChannels_ly IS NULL OR adobe_ordersAssistedHsi_allChannels_ly = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedHsi_allChannels - adobe_ordersAssistedHsi_allChannels_ly) / adobe_ordersAssistedHsi_allChannels_ly, 6) END AS adobe_ordersAssistedHsi_allChannels_yoy_pct,

        adobe_ordersAssistedHsi_paidSearch,
        adobe_ordersAssistedHsi_paidSearch_wow,
        adobe_ordersAssistedHsi_paidSearch_ly,
        CASE WHEN adobe_ordersAssistedHsi_paidSearch_wow IS NULL OR adobe_ordersAssistedHsi_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedHsi_paidSearch - adobe_ordersAssistedHsi_paidSearch_wow) / adobe_ordersAssistedHsi_paidSearch_wow, 6) END AS adobe_ordersAssistedHsi_paidSearch_wow_pct,
        CASE WHEN adobe_ordersAssistedHsi_paidSearch_ly IS NULL OR adobe_ordersAssistedHsi_paidSearch_ly = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedHsi_paidSearch - adobe_ordersAssistedHsi_paidSearch_ly) / adobe_ordersAssistedHsi_paidSearch_ly, 6) END AS adobe_ordersAssistedHsi_paidSearch_yoy_pct,

        adobe_ordersAssistedHsi_organicSearch,
        adobe_ordersAssistedHsi_organicSearch_wow,
        adobe_ordersAssistedHsi_organicSearch_ly,
        CASE WHEN adobe_ordersAssistedHsi_organicSearch_wow IS NULL OR adobe_ordersAssistedHsi_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedHsi_organicSearch - adobe_ordersAssistedHsi_organicSearch_wow) / adobe_ordersAssistedHsi_organicSearch_wow, 6) END AS adobe_ordersAssistedHsi_organicSearch_wow_pct,
        CASE WHEN adobe_ordersAssistedHsi_organicSearch_ly IS NULL OR adobe_ordersAssistedHsi_organicSearch_ly = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedHsi_organicSearch - adobe_ordersAssistedHsi_organicSearch_ly) / adobe_ordersAssistedHsi_organicSearch_ly, 6) END AS adobe_ordersAssistedHsi_organicSearch_yoy_pct,

        adobe_ordersAssistedHsi_direct,
        adobe_ordersAssistedHsi_direct_wow,
        adobe_ordersAssistedHsi_direct_ly,
        CASE WHEN adobe_ordersAssistedHsi_direct_wow IS NULL OR adobe_ordersAssistedHsi_direct_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedHsi_direct - adobe_ordersAssistedHsi_direct_wow) / adobe_ordersAssistedHsi_direct_wow, 6) END AS adobe_ordersAssistedHsi_direct_wow_pct,
        CASE WHEN adobe_ordersAssistedHsi_direct_ly IS NULL OR adobe_ordersAssistedHsi_direct_ly = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedHsi_direct - adobe_ordersAssistedHsi_direct_ly) / adobe_ordersAssistedHsi_direct_ly, 6) END AS adobe_ordersAssistedHsi_direct_yoy_pct,

        adobe_ordersAssistedHsi_social,
        adobe_ordersAssistedHsi_social_wow,
        adobe_ordersAssistedHsi_social_ly,
        CASE WHEN adobe_ordersAssistedHsi_social_wow IS NULL OR adobe_ordersAssistedHsi_social_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedHsi_social - adobe_ordersAssistedHsi_social_wow) / adobe_ordersAssistedHsi_social_wow, 6) END AS adobe_ordersAssistedHsi_social_wow_pct,
        CASE WHEN adobe_ordersAssistedHsi_social_ly IS NULL OR adobe_ordersAssistedHsi_social_ly = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedHsi_social - adobe_ordersAssistedHsi_social_ly) / adobe_ordersAssistedHsi_social_ly, 6) END AS adobe_ordersAssistedHsi_social_yoy_pct,

        adobe_ordersAssistedHsi_programmatic,
        adobe_ordersAssistedHsi_programmatic_wow,
        adobe_ordersAssistedHsi_programmatic_ly,
        CASE WHEN adobe_ordersAssistedHsi_programmatic_wow IS NULL OR adobe_ordersAssistedHsi_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedHsi_programmatic - adobe_ordersAssistedHsi_programmatic_wow) / adobe_ordersAssistedHsi_programmatic_wow, 6) END AS adobe_ordersAssistedHsi_programmatic_wow_pct,
        CASE WHEN adobe_ordersAssistedHsi_programmatic_ly IS NULL OR adobe_ordersAssistedHsi_programmatic_ly = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedHsi_programmatic - adobe_ordersAssistedHsi_programmatic_ly) / adobe_ordersAssistedHsi_programmatic_ly, 6) END AS adobe_ordersAssistedHsi_programmatic_yoy_pct,

        adobe_ordersAssistedHsi_other,
        adobe_ordersAssistedHsi_other_wow,
        adobe_ordersAssistedHsi_other_ly,
        CASE WHEN adobe_ordersAssistedHsi_other_wow IS NULL OR adobe_ordersAssistedHsi_other_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedHsi_other - adobe_ordersAssistedHsi_other_wow) / adobe_ordersAssistedHsi_other_wow, 6) END AS adobe_ordersAssistedHsi_other_wow_pct,
        CASE WHEN adobe_ordersAssistedHsi_other_ly IS NULL OR adobe_ordersAssistedHsi_other_ly = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedHsi_other - adobe_ordersAssistedHsi_other_ly) / adobe_ordersAssistedHsi_other_ly, 6) END AS adobe_ordersAssistedHsi_other_yoy_pct,

        adobe_ordersAssistedByod_allChannels,
        adobe_ordersAssistedByod_allChannels_wow,
        adobe_ordersAssistedByod_allChannels_ly,
        CASE WHEN adobe_ordersAssistedByod_allChannels_wow IS NULL OR adobe_ordersAssistedByod_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_allChannels - adobe_ordersAssistedByod_allChannels_wow) / adobe_ordersAssistedByod_allChannels_wow, 6) END AS adobe_ordersAssistedByod_allChannels_wow_pct,
        CASE WHEN adobe_ordersAssistedByod_allChannels_ly IS NULL OR adobe_ordersAssistedByod_allChannels_ly = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_allChannels - adobe_ordersAssistedByod_allChannels_ly) / adobe_ordersAssistedByod_allChannels_ly, 6) END AS adobe_ordersAssistedByod_allChannels_yoy_pct,

        adobe_ordersAssistedByod_paidSearch,
        adobe_ordersAssistedByod_paidSearch_wow,
        adobe_ordersAssistedByod_paidSearch_ly,
        CASE WHEN adobe_ordersAssistedByod_paidSearch_wow IS NULL OR adobe_ordersAssistedByod_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_paidSearch - adobe_ordersAssistedByod_paidSearch_wow) / adobe_ordersAssistedByod_paidSearch_wow, 6) END AS adobe_ordersAssistedByod_paidSearch_wow_pct,
        CASE WHEN adobe_ordersAssistedByod_paidSearch_ly IS NULL OR adobe_ordersAssistedByod_paidSearch_ly = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_paidSearch - adobe_ordersAssistedByod_paidSearch_ly) / adobe_ordersAssistedByod_paidSearch_ly, 6) END AS adobe_ordersAssistedByod_paidSearch_yoy_pct,

        adobe_ordersAssistedByod_organicSearch,
        adobe_ordersAssistedByod_organicSearch_wow,
        adobe_ordersAssistedByod_organicSearch_ly,
        CASE WHEN adobe_ordersAssistedByod_organicSearch_wow IS NULL OR adobe_ordersAssistedByod_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_organicSearch - adobe_ordersAssistedByod_organicSearch_wow) / adobe_ordersAssistedByod_organicSearch_wow, 6) END AS adobe_ordersAssistedByod_organicSearch_wow_pct,
        CASE WHEN adobe_ordersAssistedByod_organicSearch_ly IS NULL OR adobe_ordersAssistedByod_organicSearch_ly = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_organicSearch - adobe_ordersAssistedByod_organicSearch_ly) / adobe_ordersAssistedByod_organicSearch_ly, 6) END AS adobe_ordersAssistedByod_organicSearch_yoy_pct,

        adobe_ordersAssistedByod_direct,
        adobe_ordersAssistedByod_direct_wow,
        adobe_ordersAssistedByod_direct_ly,
        CASE WHEN adobe_ordersAssistedByod_direct_wow IS NULL OR adobe_ordersAssistedByod_direct_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_direct - adobe_ordersAssistedByod_direct_wow) / adobe_ordersAssistedByod_direct_wow, 6) END AS adobe_ordersAssistedByod_direct_wow_pct,
        CASE WHEN adobe_ordersAssistedByod_direct_ly IS NULL OR adobe_ordersAssistedByod_direct_ly = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_direct - adobe_ordersAssistedByod_direct_ly) / adobe_ordersAssistedByod_direct_ly, 6) END AS adobe_ordersAssistedByod_direct_yoy_pct,

        adobe_ordersAssistedByod_social,
        adobe_ordersAssistedByod_social_wow,
        adobe_ordersAssistedByod_social_ly,
        CASE WHEN adobe_ordersAssistedByod_social_wow IS NULL OR adobe_ordersAssistedByod_social_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_social - adobe_ordersAssistedByod_social_wow) / adobe_ordersAssistedByod_social_wow, 6) END AS adobe_ordersAssistedByod_social_wow_pct,
        CASE WHEN adobe_ordersAssistedByod_social_ly IS NULL OR adobe_ordersAssistedByod_social_ly = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_social - adobe_ordersAssistedByod_social_ly) / adobe_ordersAssistedByod_social_ly, 6) END AS adobe_ordersAssistedByod_social_yoy_pct,

        adobe_ordersAssistedByod_programmatic,
        adobe_ordersAssistedByod_programmatic_wow,
        adobe_ordersAssistedByod_programmatic_ly,
        CASE WHEN adobe_ordersAssistedByod_programmatic_wow IS NULL OR adobe_ordersAssistedByod_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_programmatic - adobe_ordersAssistedByod_programmatic_wow) / adobe_ordersAssistedByod_programmatic_wow, 6) END AS adobe_ordersAssistedByod_programmatic_wow_pct,
        CASE WHEN adobe_ordersAssistedByod_programmatic_ly IS NULL OR adobe_ordersAssistedByod_programmatic_ly = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_programmatic - adobe_ordersAssistedByod_programmatic_ly) / adobe_ordersAssistedByod_programmatic_ly, 6) END AS adobe_ordersAssistedByod_programmatic_yoy_pct,

        adobe_ordersAssistedByod_other,
        adobe_ordersAssistedByod_other_wow,
        adobe_ordersAssistedByod_other_ly,
        CASE WHEN adobe_ordersAssistedByod_other_wow IS NULL OR adobe_ordersAssistedByod_other_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_other - adobe_ordersAssistedByod_other_wow) / adobe_ordersAssistedByod_other_wow, 6) END AS adobe_ordersAssistedByod_other_wow_pct,
        CASE WHEN adobe_ordersAssistedByod_other_ly IS NULL OR adobe_ordersAssistedByod_other_ly = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_other - adobe_ordersAssistedByod_other_ly) / adobe_ordersAssistedByod_other_ly, 6) END AS adobe_ordersAssistedByod_other_yoy_pct

    FROM with_comparisons
),

-- -----------------------------------------------------------------------
-- STEP 6: max_data_date
-- -----------------------------------------------------------------------
with_max_date AS (
    SELECT
        *,
        MAX(CASE
            WHEN adobe_uvnbByod_allChannels IS NOT NULL
              OR adobe_ordersTotal_allChannels IS NOT NULL
            THEN week_sun_to_sat
        END) OVER ()                                                    AS max_data_date
    FROM with_pcts
)

-- -----------------------------------------------------------------------
-- FINAL OUTPUT
-- Wide table — one row per week_sun_to_sat
-- -----------------------------------------------------------------------
SELECT
    week_sun_to_sat,
    'ADOBE'                                                             AS data_source,
    max_data_date,

    adobe_uvnbTotalAdobe_allChannels,
    adobe_uvnbTotalAdobe_allChannels_wow,
    adobe_uvnbTotalAdobe_allChannels_ly,
    adobe_uvnbTotalAdobe_allChannels_wow_pct,
    adobe_uvnbTotalAdobe_allChannels_yoy_pct,

    adobe_uvnbFlowTotal_allChannels,
    adobe_uvnbFlowTotal_allChannels_wow,
    adobe_uvnbFlowTotal_allChannels_ly,
    adobe_uvnbFlowTotal_allChannels_wow_pct,
    adobe_uvnbFlowTotal_allChannels_yoy_pct,

    adobe_uvnbByodPctOfUvnbFlow_allChannels,
    adobe_uvnbByodPctOfUvnbFlow_allChannels_wow,
    adobe_uvnbByodPctOfUvnbFlow_allChannels_ly,
    adobe_uvnbByodPctOfUvnbFlow_allChannels_wow_pct,
    adobe_uvnbByodPctOfUvnbFlow_allChannels_yoy_pct,

    adobe_uvnbPostpaid_allChannels,
    adobe_uvnbPostpaid_allChannels_wow,
    adobe_uvnbPostpaid_allChannels_ly,
    adobe_uvnbPostpaid_allChannels_wow_pct,
    adobe_uvnbPostpaid_allChannels_yoy_pct,

    adobe_uvnbPostpaid_paidSearch,
    adobe_uvnbPostpaid_paidSearch_wow,
    adobe_uvnbPostpaid_paidSearch_ly,
    adobe_uvnbPostpaid_paidSearch_wow_pct,
    adobe_uvnbPostpaid_paidSearch_yoy_pct,

    adobe_uvnbPostpaid_organicSearch,
    adobe_uvnbPostpaid_organicSearch_wow,
    adobe_uvnbPostpaid_organicSearch_ly,
    adobe_uvnbPostpaid_organicSearch_wow_pct,
    adobe_uvnbPostpaid_organicSearch_yoy_pct,

    adobe_uvnbPostpaid_direct,
    adobe_uvnbPostpaid_direct_wow,
    adobe_uvnbPostpaid_direct_ly,
    adobe_uvnbPostpaid_direct_wow_pct,
    adobe_uvnbPostpaid_direct_yoy_pct,

    adobe_uvnbPostpaid_social,
    adobe_uvnbPostpaid_social_wow,
    adobe_uvnbPostpaid_social_ly,
    adobe_uvnbPostpaid_social_wow_pct,
    adobe_uvnbPostpaid_social_yoy_pct,

    adobe_uvnbPostpaid_programmatic,
    adobe_uvnbPostpaid_programmatic_wow,
    adobe_uvnbPostpaid_programmatic_ly,
    adobe_uvnbPostpaid_programmatic_wow_pct,
    adobe_uvnbPostpaid_programmatic_yoy_pct,

    adobe_uvnbPostpaid_other,
    adobe_uvnbPostpaid_other_wow,
    adobe_uvnbPostpaid_other_ly,
    adobe_uvnbPostpaid_other_wow_pct,
    adobe_uvnbPostpaid_other_yoy_pct,

    adobe_uvnbHsi_allChannels,
    adobe_uvnbHsi_allChannels_wow,
    adobe_uvnbHsi_allChannels_ly,
    adobe_uvnbHsi_allChannels_wow_pct,
    adobe_uvnbHsi_allChannels_yoy_pct,

    adobe_uvnbHsi_paidSearch,
    adobe_uvnbHsi_paidSearch_wow,
    adobe_uvnbHsi_paidSearch_ly,
    adobe_uvnbHsi_paidSearch_wow_pct,
    adobe_uvnbHsi_paidSearch_yoy_pct,

    adobe_uvnbHsi_organicSearch,
    adobe_uvnbHsi_organicSearch_wow,
    adobe_uvnbHsi_organicSearch_ly,
    adobe_uvnbHsi_organicSearch_wow_pct,
    adobe_uvnbHsi_organicSearch_yoy_pct,

    adobe_uvnbHsi_direct,
    adobe_uvnbHsi_direct_wow,
    adobe_uvnbHsi_direct_ly,
    adobe_uvnbHsi_direct_wow_pct,
    adobe_uvnbHsi_direct_yoy_pct,

    adobe_uvnbHsi_social,
    adobe_uvnbHsi_social_wow,
    adobe_uvnbHsi_social_ly,
    adobe_uvnbHsi_social_wow_pct,
    adobe_uvnbHsi_social_yoy_pct,

    adobe_uvnbHsi_programmatic,
    adobe_uvnbHsi_programmatic_wow,
    adobe_uvnbHsi_programmatic_ly,
    adobe_uvnbHsi_programmatic_wow_pct,
    adobe_uvnbHsi_programmatic_yoy_pct,

    adobe_uvnbHsi_other,
    adobe_uvnbHsi_other_wow,
    adobe_uvnbHsi_other_ly,
    adobe_uvnbHsi_other_wow_pct,
    adobe_uvnbHsi_other_yoy_pct,

    adobe_uvnbByod_allChannels,
    adobe_uvnbByod_allChannels_wow,
    adobe_uvnbByod_allChannels_ly,
    adobe_uvnbByod_allChannels_wow_pct,
    adobe_uvnbByod_allChannels_yoy_pct,

    adobe_uvnbByod_paidSearch,
    adobe_uvnbByod_paidSearch_wow,
    adobe_uvnbByod_paidSearch_ly,
    adobe_uvnbByod_paidSearch_wow_pct,
    adobe_uvnbByod_paidSearch_yoy_pct,

    adobe_uvnbByod_organicSearch,
    adobe_uvnbByod_organicSearch_wow,
    adobe_uvnbByod_organicSearch_ly,
    adobe_uvnbByod_organicSearch_wow_pct,
    adobe_uvnbByod_organicSearch_yoy_pct,

    adobe_uvnbByod_direct,
    adobe_uvnbByod_direct_wow,
    adobe_uvnbByod_direct_ly,
    adobe_uvnbByod_direct_wow_pct,
    adobe_uvnbByod_direct_yoy_pct,

    adobe_uvnbByod_social,
    adobe_uvnbByod_social_wow,
    adobe_uvnbByod_social_ly,
    adobe_uvnbByod_social_wow_pct,
    adobe_uvnbByod_social_yoy_pct,

    adobe_uvnbByod_programmatic,
    adobe_uvnbByod_programmatic_wow,
    adobe_uvnbByod_programmatic_ly,
    adobe_uvnbByod_programmatic_wow_pct,
    adobe_uvnbByod_programmatic_yoy_pct,

    adobe_uvnbByod_other,
    adobe_uvnbByod_other_wow,
    adobe_uvnbByod_other_ly,
    adobe_uvnbByod_other_wow_pct,
    adobe_uvnbByod_other_yoy_pct,

    adobe_uvnbTrackedFlowSum_allChannels,
    adobe_uvnbTrackedFlowSum_allChannels_wow,
    adobe_uvnbTrackedFlowSum_allChannels_ly,
    adobe_uvnbTrackedFlowSum_allChannels_wow_pct,
    adobe_uvnbTrackedFlowSum_allChannels_yoy_pct,

    adobe_uvnbTrackedFlowSum_paidSearch,
    adobe_uvnbTrackedFlowSum_paidSearch_wow,
    adobe_uvnbTrackedFlowSum_paidSearch_ly,
    adobe_uvnbTrackedFlowSum_paidSearch_wow_pct,
    adobe_uvnbTrackedFlowSum_paidSearch_yoy_pct,

    adobe_uvnbTrackedFlowSum_organicSearch,
    adobe_uvnbTrackedFlowSum_organicSearch_wow,
    adobe_uvnbTrackedFlowSum_organicSearch_ly,
    adobe_uvnbTrackedFlowSum_organicSearch_wow_pct,
    adobe_uvnbTrackedFlowSum_organicSearch_yoy_pct,

    adobe_uvnbTrackedFlowSum_direct,
    adobe_uvnbTrackedFlowSum_direct_wow,
    adobe_uvnbTrackedFlowSum_direct_ly,
    adobe_uvnbTrackedFlowSum_direct_wow_pct,
    adobe_uvnbTrackedFlowSum_direct_yoy_pct,

    adobe_uvnbTrackedFlowSum_social,
    adobe_uvnbTrackedFlowSum_social_wow,
    adobe_uvnbTrackedFlowSum_social_ly,
    adobe_uvnbTrackedFlowSum_social_wow_pct,
    adobe_uvnbTrackedFlowSum_social_yoy_pct,

    adobe_uvnbTrackedFlowSum_programmatic,
    adobe_uvnbTrackedFlowSum_programmatic_wow,
    adobe_uvnbTrackedFlowSum_programmatic_ly,
    adobe_uvnbTrackedFlowSum_programmatic_wow_pct,
    adobe_uvnbTrackedFlowSum_programmatic_yoy_pct,

    adobe_uvnbTrackedFlowSum_other,
    adobe_uvnbTrackedFlowSum_other_wow,
    adobe_uvnbTrackedFlowSum_other_ly,
    adobe_uvnbTrackedFlowSum_other_wow_pct,
    adobe_uvnbTrackedFlowSum_other_yoy_pct,

    adobe_cartStartTotal_allChannels,
    adobe_cartStartTotal_allChannels_wow,
    adobe_cartStartTotal_allChannels_ly,
    adobe_cartStartTotal_allChannels_wow_pct,
    adobe_cartStartTotal_allChannels_yoy_pct,

    adobe_cartStartTotal_paidSearch,
    adobe_cartStartTotal_paidSearch_wow,
    adobe_cartStartTotal_paidSearch_ly,
    adobe_cartStartTotal_paidSearch_wow_pct,
    adobe_cartStartTotal_paidSearch_yoy_pct,

    adobe_cartStartTotal_organicSearch,
    adobe_cartStartTotal_organicSearch_wow,
    adobe_cartStartTotal_organicSearch_ly,
    adobe_cartStartTotal_organicSearch_wow_pct,
    adobe_cartStartTotal_organicSearch_yoy_pct,

    adobe_cartStartTotal_direct,
    adobe_cartStartTotal_direct_wow,
    adobe_cartStartTotal_direct_ly,
    adobe_cartStartTotal_direct_wow_pct,
    adobe_cartStartTotal_direct_yoy_pct,

    adobe_cartStartTotal_social,
    adobe_cartStartTotal_social_wow,
    adobe_cartStartTotal_social_ly,
    adobe_cartStartTotal_social_wow_pct,
    adobe_cartStartTotal_social_yoy_pct,

    adobe_cartStartTotal_programmatic,
    adobe_cartStartTotal_programmatic_wow,
    adobe_cartStartTotal_programmatic_ly,
    adobe_cartStartTotal_programmatic_wow_pct,
    adobe_cartStartTotal_programmatic_yoy_pct,

    adobe_cartStartTotal_other,
    adobe_cartStartTotal_other_wow,
    adobe_cartStartTotal_other_ly,
    adobe_cartStartTotal_other_wow_pct,
    adobe_cartStartTotal_other_yoy_pct,

    adobe_cartStartPostpaid_allChannels,
    adobe_cartStartPostpaid_allChannels_wow,
    adobe_cartStartPostpaid_allChannels_ly,
    adobe_cartStartPostpaid_allChannels_wow_pct,
    adobe_cartStartPostpaid_allChannels_yoy_pct,

    adobe_cartStartPostpaid_paidSearch,
    adobe_cartStartPostpaid_paidSearch_wow,
    adobe_cartStartPostpaid_paidSearch_ly,
    adobe_cartStartPostpaid_paidSearch_wow_pct,
    adobe_cartStartPostpaid_paidSearch_yoy_pct,

    adobe_cartStartPostpaid_organicSearch,
    adobe_cartStartPostpaid_organicSearch_wow,
    adobe_cartStartPostpaid_organicSearch_ly,
    adobe_cartStartPostpaid_organicSearch_wow_pct,
    adobe_cartStartPostpaid_organicSearch_yoy_pct,

    adobe_cartStartPostpaid_direct,
    adobe_cartStartPostpaid_direct_wow,
    adobe_cartStartPostpaid_direct_ly,
    adobe_cartStartPostpaid_direct_wow_pct,
    adobe_cartStartPostpaid_direct_yoy_pct,

    adobe_cartStartPostpaid_social,
    adobe_cartStartPostpaid_social_wow,
    adobe_cartStartPostpaid_social_ly,
    adobe_cartStartPostpaid_social_wow_pct,
    adobe_cartStartPostpaid_social_yoy_pct,

    adobe_cartStartPostpaid_programmatic,
    adobe_cartStartPostpaid_programmatic_wow,
    adobe_cartStartPostpaid_programmatic_ly,
    adobe_cartStartPostpaid_programmatic_wow_pct,
    adobe_cartStartPostpaid_programmatic_yoy_pct,

    adobe_cartStartPostpaid_other,
    adobe_cartStartPostpaid_other_wow,
    adobe_cartStartPostpaid_other_ly,
    adobe_cartStartPostpaid_other_wow_pct,
    adobe_cartStartPostpaid_other_yoy_pct,

    adobe_cartStartHsi_allChannels,
    adobe_cartStartHsi_allChannels_wow,
    adobe_cartStartHsi_allChannels_ly,
    adobe_cartStartHsi_allChannels_wow_pct,
    adobe_cartStartHsi_allChannels_yoy_pct,

    adobe_cartStartHsi_paidSearch,
    adobe_cartStartHsi_paidSearch_wow,
    adobe_cartStartHsi_paidSearch_ly,
    adobe_cartStartHsi_paidSearch_wow_pct,
    adobe_cartStartHsi_paidSearch_yoy_pct,

    adobe_cartStartHsi_organicSearch,
    adobe_cartStartHsi_organicSearch_wow,
    adobe_cartStartHsi_organicSearch_ly,
    adobe_cartStartHsi_organicSearch_wow_pct,
    adobe_cartStartHsi_organicSearch_yoy_pct,

    adobe_cartStartHsi_direct,
    adobe_cartStartHsi_direct_wow,
    adobe_cartStartHsi_direct_ly,
    adobe_cartStartHsi_direct_wow_pct,
    adobe_cartStartHsi_direct_yoy_pct,

    adobe_cartStartHsi_social,
    adobe_cartStartHsi_social_wow,
    adobe_cartStartHsi_social_ly,
    adobe_cartStartHsi_social_wow_pct,
    adobe_cartStartHsi_social_yoy_pct,

    adobe_cartStartHsi_programmatic,
    adobe_cartStartHsi_programmatic_wow,
    adobe_cartStartHsi_programmatic_ly,
    adobe_cartStartHsi_programmatic_wow_pct,
    adobe_cartStartHsi_programmatic_yoy_pct,

    adobe_cartStartHsi_other,
    adobe_cartStartHsi_other_wow,
    adobe_cartStartHsi_other_ly,
    adobe_cartStartHsi_other_wow_pct,
    adobe_cartStartHsi_other_yoy_pct,

    adobe_cartStartByod_allChannels,
    adobe_cartStartByod_allChannels_wow,
    adobe_cartStartByod_allChannels_ly,
    adobe_cartStartByod_allChannels_wow_pct,
    adobe_cartStartByod_allChannels_yoy_pct,

    adobe_cartStartByod_paidSearch,
    adobe_cartStartByod_paidSearch_wow,
    adobe_cartStartByod_paidSearch_ly,
    adobe_cartStartByod_paidSearch_wow_pct,
    adobe_cartStartByod_paidSearch_yoy_pct,

    adobe_cartStartByod_organicSearch,
    adobe_cartStartByod_organicSearch_wow,
    adobe_cartStartByod_organicSearch_ly,
    adobe_cartStartByod_organicSearch_wow_pct,
    adobe_cartStartByod_organicSearch_yoy_pct,

    adobe_cartStartByod_direct,
    adobe_cartStartByod_direct_wow,
    adobe_cartStartByod_direct_ly,
    adobe_cartStartByod_direct_wow_pct,
    adobe_cartStartByod_direct_yoy_pct,

    adobe_cartStartByod_social,
    adobe_cartStartByod_social_wow,
    adobe_cartStartByod_social_ly,
    adobe_cartStartByod_social_wow_pct,
    adobe_cartStartByod_social_yoy_pct,

    adobe_cartStartByod_programmatic,
    adobe_cartStartByod_programmatic_wow,
    adobe_cartStartByod_programmatic_ly,
    adobe_cartStartByod_programmatic_wow_pct,
    adobe_cartStartByod_programmatic_yoy_pct,

    adobe_cartStartByod_other,
    adobe_cartStartByod_other_wow,
    adobe_cartStartByod_other_ly,
    adobe_cartStartByod_other_wow_pct,
    adobe_cartStartByod_other_yoy_pct,

    adobe_ordersTotal_allChannels,
    adobe_ordersTotal_allChannels_wow,
    adobe_ordersTotal_allChannels_ly,
    adobe_ordersTotal_allChannels_wow_pct,
    adobe_ordersTotal_allChannels_yoy_pct,

    adobe_ordersTotal_paidSearch,
    adobe_ordersTotal_paidSearch_wow,
    adobe_ordersTotal_paidSearch_ly,
    adobe_ordersTotal_paidSearch_wow_pct,
    adobe_ordersTotal_paidSearch_yoy_pct,

    adobe_ordersTotal_organicSearch,
    adobe_ordersTotal_organicSearch_wow,
    adobe_ordersTotal_organicSearch_ly,
    adobe_ordersTotal_organicSearch_wow_pct,
    adobe_ordersTotal_organicSearch_yoy_pct,

    adobe_ordersTotal_direct,
    adobe_ordersTotal_direct_wow,
    adobe_ordersTotal_direct_ly,
    adobe_ordersTotal_direct_wow_pct,
    adobe_ordersTotal_direct_yoy_pct,

    adobe_ordersTotal_social,
    adobe_ordersTotal_social_wow,
    adobe_ordersTotal_social_ly,
    adobe_ordersTotal_social_wow_pct,
    adobe_ordersTotal_social_yoy_pct,

    adobe_ordersTotal_programmatic,
    adobe_ordersTotal_programmatic_wow,
    adobe_ordersTotal_programmatic_ly,
    adobe_ordersTotal_programmatic_wow_pct,
    adobe_ordersTotal_programmatic_yoy_pct,

    adobe_ordersTotal_other,
    adobe_ordersTotal_other_wow,
    adobe_ordersTotal_other_ly,
    adobe_ordersTotal_other_wow_pct,
    adobe_ordersTotal_other_yoy_pct,

    adobe_ordersUnassistedTotal_allChannels,
    adobe_ordersUnassistedTotal_allChannels_wow,
    adobe_ordersUnassistedTotal_allChannels_ly,
    adobe_ordersUnassistedTotal_allChannels_wow_pct,
    adobe_ordersUnassistedTotal_allChannels_yoy_pct,

    adobe_ordersUnassistedTotal_paidSearch,
    adobe_ordersUnassistedTotal_paidSearch_wow,
    adobe_ordersUnassistedTotal_paidSearch_ly,
    adobe_ordersUnassistedTotal_paidSearch_wow_pct,
    adobe_ordersUnassistedTotal_paidSearch_yoy_pct,

    adobe_ordersUnassistedTotal_organicSearch,
    adobe_ordersUnassistedTotal_organicSearch_wow,
    adobe_ordersUnassistedTotal_organicSearch_ly,
    adobe_ordersUnassistedTotal_organicSearch_wow_pct,
    adobe_ordersUnassistedTotal_organicSearch_yoy_pct,

    adobe_ordersUnassistedTotal_direct,
    adobe_ordersUnassistedTotal_direct_wow,
    adobe_ordersUnassistedTotal_direct_ly,
    adobe_ordersUnassistedTotal_direct_wow_pct,
    adobe_ordersUnassistedTotal_direct_yoy_pct,

    adobe_ordersUnassistedTotal_social,
    adobe_ordersUnassistedTotal_social_wow,
    adobe_ordersUnassistedTotal_social_ly,
    adobe_ordersUnassistedTotal_social_wow_pct,
    adobe_ordersUnassistedTotal_social_yoy_pct,

    adobe_ordersUnassistedTotal_programmatic,
    adobe_ordersUnassistedTotal_programmatic_wow,
    adobe_ordersUnassistedTotal_programmatic_ly,
    adobe_ordersUnassistedTotal_programmatic_wow_pct,
    adobe_ordersUnassistedTotal_programmatic_yoy_pct,

    adobe_ordersUnassistedTotal_other,
    adobe_ordersUnassistedTotal_other_wow,
    adobe_ordersUnassistedTotal_other_ly,
    adobe_ordersUnassistedTotal_other_wow_pct,
    adobe_ordersUnassistedTotal_other_yoy_pct,

    adobe_ordersUnassistedPostpaid_allChannels,
    adobe_ordersUnassistedPostpaid_allChannels_wow,
    adobe_ordersUnassistedPostpaid_allChannels_ly,
    adobe_ordersUnassistedPostpaid_allChannels_wow_pct,
    adobe_ordersUnassistedPostpaid_allChannels_yoy_pct,

    adobe_ordersUnassistedPostpaid_paidSearch,
    adobe_ordersUnassistedPostpaid_paidSearch_wow,
    adobe_ordersUnassistedPostpaid_paidSearch_ly,
    adobe_ordersUnassistedPostpaid_paidSearch_wow_pct,
    adobe_ordersUnassistedPostpaid_paidSearch_yoy_pct,

    adobe_ordersUnassistedPostpaid_organicSearch,
    adobe_ordersUnassistedPostpaid_organicSearch_wow,
    adobe_ordersUnassistedPostpaid_organicSearch_ly,
    adobe_ordersUnassistedPostpaid_organicSearch_wow_pct,
    adobe_ordersUnassistedPostpaid_organicSearch_yoy_pct,

    adobe_ordersUnassistedPostpaid_direct,
    adobe_ordersUnassistedPostpaid_direct_wow,
    adobe_ordersUnassistedPostpaid_direct_ly,
    adobe_ordersUnassistedPostpaid_direct_wow_pct,
    adobe_ordersUnassistedPostpaid_direct_yoy_pct,

    adobe_ordersUnassistedPostpaid_social,
    adobe_ordersUnassistedPostpaid_social_wow,
    adobe_ordersUnassistedPostpaid_social_ly,
    adobe_ordersUnassistedPostpaid_social_wow_pct,
    adobe_ordersUnassistedPostpaid_social_yoy_pct,

    adobe_ordersUnassistedPostpaid_programmatic,
    adobe_ordersUnassistedPostpaid_programmatic_wow,
    adobe_ordersUnassistedPostpaid_programmatic_ly,
    adobe_ordersUnassistedPostpaid_programmatic_wow_pct,
    adobe_ordersUnassistedPostpaid_programmatic_yoy_pct,

    adobe_ordersUnassistedPostpaid_other,
    adobe_ordersUnassistedPostpaid_other_wow,
    adobe_ordersUnassistedPostpaid_other_ly,
    adobe_ordersUnassistedPostpaid_other_wow_pct,
    adobe_ordersUnassistedPostpaid_other_yoy_pct,

    adobe_ordersUnassistedHsi_allChannels,
    adobe_ordersUnassistedHsi_allChannels_wow,
    adobe_ordersUnassistedHsi_allChannels_ly,
    adobe_ordersUnassistedHsi_allChannels_wow_pct,
    adobe_ordersUnassistedHsi_allChannels_yoy_pct,

    adobe_ordersUnassistedHsi_paidSearch,
    adobe_ordersUnassistedHsi_paidSearch_wow,
    adobe_ordersUnassistedHsi_paidSearch_ly,
    adobe_ordersUnassistedHsi_paidSearch_wow_pct,
    adobe_ordersUnassistedHsi_paidSearch_yoy_pct,

    adobe_ordersUnassistedHsi_organicSearch,
    adobe_ordersUnassistedHsi_organicSearch_wow,
    adobe_ordersUnassistedHsi_organicSearch_ly,
    adobe_ordersUnassistedHsi_organicSearch_wow_pct,
    adobe_ordersUnassistedHsi_organicSearch_yoy_pct,

    adobe_ordersUnassistedHsi_direct,
    adobe_ordersUnassistedHsi_direct_wow,
    adobe_ordersUnassistedHsi_direct_ly,
    adobe_ordersUnassistedHsi_direct_wow_pct,
    adobe_ordersUnassistedHsi_direct_yoy_pct,

    adobe_ordersUnassistedHsi_social,
    adobe_ordersUnassistedHsi_social_wow,
    adobe_ordersUnassistedHsi_social_ly,
    adobe_ordersUnassistedHsi_social_wow_pct,
    adobe_ordersUnassistedHsi_social_yoy_pct,

    adobe_ordersUnassistedHsi_programmatic,
    adobe_ordersUnassistedHsi_programmatic_wow,
    adobe_ordersUnassistedHsi_programmatic_ly,
    adobe_ordersUnassistedHsi_programmatic_wow_pct,
    adobe_ordersUnassistedHsi_programmatic_yoy_pct,

    adobe_ordersUnassistedHsi_other,
    adobe_ordersUnassistedHsi_other_wow,
    adobe_ordersUnassistedHsi_other_ly,
    adobe_ordersUnassistedHsi_other_wow_pct,
    adobe_ordersUnassistedHsi_other_yoy_pct,

    adobe_ordersUnassistedByod_allChannels,
    adobe_ordersUnassistedByod_allChannels_wow,
    adobe_ordersUnassistedByod_allChannels_ly,
    adobe_ordersUnassistedByod_allChannels_wow_pct,
    adobe_ordersUnassistedByod_allChannels_yoy_pct,

    adobe_ordersUnassistedByod_paidSearch,
    adobe_ordersUnassistedByod_paidSearch_wow,
    adobe_ordersUnassistedByod_paidSearch_ly,
    adobe_ordersUnassistedByod_paidSearch_wow_pct,
    adobe_ordersUnassistedByod_paidSearch_yoy_pct,

    adobe_ordersUnassistedByod_organicSearch,
    adobe_ordersUnassistedByod_organicSearch_wow,
    adobe_ordersUnassistedByod_organicSearch_ly,
    adobe_ordersUnassistedByod_organicSearch_wow_pct,
    adobe_ordersUnassistedByod_organicSearch_yoy_pct,

    adobe_ordersUnassistedByod_direct,
    adobe_ordersUnassistedByod_direct_wow,
    adobe_ordersUnassistedByod_direct_ly,
    adobe_ordersUnassistedByod_direct_wow_pct,
    adobe_ordersUnassistedByod_direct_yoy_pct,

    adobe_ordersUnassistedByod_social,
    adobe_ordersUnassistedByod_social_wow,
    adobe_ordersUnassistedByod_social_ly,
    adobe_ordersUnassistedByod_social_wow_pct,
    adobe_ordersUnassistedByod_social_yoy_pct,

    adobe_ordersUnassistedByod_programmatic,
    adobe_ordersUnassistedByod_programmatic_wow,
    adobe_ordersUnassistedByod_programmatic_ly,
    adobe_ordersUnassistedByod_programmatic_wow_pct,
    adobe_ordersUnassistedByod_programmatic_yoy_pct,

    adobe_ordersUnassistedByod_other,
    adobe_ordersUnassistedByod_other_wow,
    adobe_ordersUnassistedByod_other_ly,
    adobe_ordersUnassistedByod_other_wow_pct,
    adobe_ordersUnassistedByod_other_yoy_pct,

    adobe_ordersAssistedTotal_allChannels,
    adobe_ordersAssistedTotal_allChannels_wow,
    adobe_ordersAssistedTotal_allChannels_ly,
    adobe_ordersAssistedTotal_allChannels_wow_pct,
    adobe_ordersAssistedTotal_allChannels_yoy_pct,

    adobe_ordersAssistedTotal_paidSearch,
    adobe_ordersAssistedTotal_paidSearch_wow,
    adobe_ordersAssistedTotal_paidSearch_ly,
    adobe_ordersAssistedTotal_paidSearch_wow_pct,
    adobe_ordersAssistedTotal_paidSearch_yoy_pct,

    adobe_ordersAssistedTotal_organicSearch,
    adobe_ordersAssistedTotal_organicSearch_wow,
    adobe_ordersAssistedTotal_organicSearch_ly,
    adobe_ordersAssistedTotal_organicSearch_wow_pct,
    adobe_ordersAssistedTotal_organicSearch_yoy_pct,

    adobe_ordersAssistedTotal_direct,
    adobe_ordersAssistedTotal_direct_wow,
    adobe_ordersAssistedTotal_direct_ly,
    adobe_ordersAssistedTotal_direct_wow_pct,
    adobe_ordersAssistedTotal_direct_yoy_pct,

    adobe_ordersAssistedTotal_social,
    adobe_ordersAssistedTotal_social_wow,
    adobe_ordersAssistedTotal_social_ly,
    adobe_ordersAssistedTotal_social_wow_pct,
    adobe_ordersAssistedTotal_social_yoy_pct,

    adobe_ordersAssistedTotal_programmatic,
    adobe_ordersAssistedTotal_programmatic_wow,
    adobe_ordersAssistedTotal_programmatic_ly,
    adobe_ordersAssistedTotal_programmatic_wow_pct,
    adobe_ordersAssistedTotal_programmatic_yoy_pct,

    adobe_ordersAssistedTotal_other,
    adobe_ordersAssistedTotal_other_wow,
    adobe_ordersAssistedTotal_other_ly,
    adobe_ordersAssistedTotal_other_wow_pct,
    adobe_ordersAssistedTotal_other_yoy_pct,

    adobe_ordersAssistedPostpaid_allChannels,
    adobe_ordersAssistedPostpaid_allChannels_wow,
    adobe_ordersAssistedPostpaid_allChannels_ly,
    adobe_ordersAssistedPostpaid_allChannels_wow_pct,
    adobe_ordersAssistedPostpaid_allChannels_yoy_pct,

    adobe_ordersAssistedPostpaid_paidSearch,
    adobe_ordersAssistedPostpaid_paidSearch_wow,
    adobe_ordersAssistedPostpaid_paidSearch_ly,
    adobe_ordersAssistedPostpaid_paidSearch_wow_pct,
    adobe_ordersAssistedPostpaid_paidSearch_yoy_pct,

    adobe_ordersAssistedPostpaid_organicSearch,
    adobe_ordersAssistedPostpaid_organicSearch_wow,
    adobe_ordersAssistedPostpaid_organicSearch_ly,
    adobe_ordersAssistedPostpaid_organicSearch_wow_pct,
    adobe_ordersAssistedPostpaid_organicSearch_yoy_pct,

    adobe_ordersAssistedPostpaid_direct,
    adobe_ordersAssistedPostpaid_direct_wow,
    adobe_ordersAssistedPostpaid_direct_ly,
    adobe_ordersAssistedPostpaid_direct_wow_pct,
    adobe_ordersAssistedPostpaid_direct_yoy_pct,

    adobe_ordersAssistedPostpaid_social,
    adobe_ordersAssistedPostpaid_social_wow,
    adobe_ordersAssistedPostpaid_social_ly,
    adobe_ordersAssistedPostpaid_social_wow_pct,
    adobe_ordersAssistedPostpaid_social_yoy_pct,

    adobe_ordersAssistedPostpaid_programmatic,
    adobe_ordersAssistedPostpaid_programmatic_wow,
    adobe_ordersAssistedPostpaid_programmatic_ly,
    adobe_ordersAssistedPostpaid_programmatic_wow_pct,
    adobe_ordersAssistedPostpaid_programmatic_yoy_pct,

    adobe_ordersAssistedPostpaid_other,
    adobe_ordersAssistedPostpaid_other_wow,
    adobe_ordersAssistedPostpaid_other_ly,
    adobe_ordersAssistedPostpaid_other_wow_pct,
    adobe_ordersAssistedPostpaid_other_yoy_pct,

    adobe_ordersAssistedHsi_allChannels,
    adobe_ordersAssistedHsi_allChannels_wow,
    adobe_ordersAssistedHsi_allChannels_ly,
    adobe_ordersAssistedHsi_allChannels_wow_pct,
    adobe_ordersAssistedHsi_allChannels_yoy_pct,

    adobe_ordersAssistedHsi_paidSearch,
    adobe_ordersAssistedHsi_paidSearch_wow,
    adobe_ordersAssistedHsi_paidSearch_ly,
    adobe_ordersAssistedHsi_paidSearch_wow_pct,
    adobe_ordersAssistedHsi_paidSearch_yoy_pct,

    adobe_ordersAssistedHsi_organicSearch,
    adobe_ordersAssistedHsi_organicSearch_wow,
    adobe_ordersAssistedHsi_organicSearch_ly,
    adobe_ordersAssistedHsi_organicSearch_wow_pct,
    adobe_ordersAssistedHsi_organicSearch_yoy_pct,

    adobe_ordersAssistedHsi_direct,
    adobe_ordersAssistedHsi_direct_wow,
    adobe_ordersAssistedHsi_direct_ly,
    adobe_ordersAssistedHsi_direct_wow_pct,
    adobe_ordersAssistedHsi_direct_yoy_pct,

    adobe_ordersAssistedHsi_social,
    adobe_ordersAssistedHsi_social_wow,
    adobe_ordersAssistedHsi_social_ly,
    adobe_ordersAssistedHsi_social_wow_pct,
    adobe_ordersAssistedHsi_social_yoy_pct,

    adobe_ordersAssistedHsi_programmatic,
    adobe_ordersAssistedHsi_programmatic_wow,
    adobe_ordersAssistedHsi_programmatic_ly,
    adobe_ordersAssistedHsi_programmatic_wow_pct,
    adobe_ordersAssistedHsi_programmatic_yoy_pct,

    adobe_ordersAssistedHsi_other,
    adobe_ordersAssistedHsi_other_wow,
    adobe_ordersAssistedHsi_other_ly,
    adobe_ordersAssistedHsi_other_wow_pct,
    adobe_ordersAssistedHsi_other_yoy_pct,

    adobe_ordersAssistedByod_allChannels,
    adobe_ordersAssistedByod_allChannels_wow,
    adobe_ordersAssistedByod_allChannels_ly,
    adobe_ordersAssistedByod_allChannels_wow_pct,
    adobe_ordersAssistedByod_allChannels_yoy_pct,

    adobe_ordersAssistedByod_paidSearch,
    adobe_ordersAssistedByod_paidSearch_wow,
    adobe_ordersAssistedByod_paidSearch_ly,
    adobe_ordersAssistedByod_paidSearch_wow_pct,
    adobe_ordersAssistedByod_paidSearch_yoy_pct,

    adobe_ordersAssistedByod_organicSearch,
    adobe_ordersAssistedByod_organicSearch_wow,
    adobe_ordersAssistedByod_organicSearch_ly,
    adobe_ordersAssistedByod_organicSearch_wow_pct,
    adobe_ordersAssistedByod_organicSearch_yoy_pct,

    adobe_ordersAssistedByod_direct,
    adobe_ordersAssistedByod_direct_wow,
    adobe_ordersAssistedByod_direct_ly,
    adobe_ordersAssistedByod_direct_wow_pct,
    adobe_ordersAssistedByod_direct_yoy_pct,

    adobe_ordersAssistedByod_social,
    adobe_ordersAssistedByod_social_wow,
    adobe_ordersAssistedByod_social_ly,
    adobe_ordersAssistedByod_social_wow_pct,
    adobe_ordersAssistedByod_social_yoy_pct,

    adobe_ordersAssistedByod_programmatic,
    adobe_ordersAssistedByod_programmatic_wow,
    adobe_ordersAssistedByod_programmatic_ly,
    adobe_ordersAssistedByod_programmatic_wow_pct,
    adobe_ordersAssistedByod_programmatic_yoy_pct,

    adobe_ordersAssistedByod_other,
    adobe_ordersAssistedByod_other_wow,
    adobe_ordersAssistedByod_other_ly,
    adobe_ordersAssistedByod_other_wow_pct,
    adobe_ordersAssistedByod_other_yoy_pct

FROM with_max_date
;