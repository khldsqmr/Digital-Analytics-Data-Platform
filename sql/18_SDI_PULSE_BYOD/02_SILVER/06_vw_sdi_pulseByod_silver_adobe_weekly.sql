/* =================================================================================================
FILE:         06_vw_sdi_pulseByod_silver_adobe_weekly.sql
LAYER:        Silver View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseByod_silver_adobe_weekly

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_flowPerformanceByChannelGroupsPlusAll_Weekly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_adobe_weekly

PURPOSE:
  Silver view for Adobe Analytics BYOD metrics.
  Outputs a WIDE table — one row per week_sun_to_sat.
  Pivots ChannelGroup rows into columns so each channel
  becomes its own set of prefixed metric columns.
  Applies WoW/LY comparisons and max_data_date.
  Only BYOD-specific metrics retained — Postpaid and HSI excluded.
  uvnbByodPctOfUvnbFlow computed for allChannels only.

BUSINESS GRAIN:
  One row per:
    week_sun_to_sat

SOURCE GRAIN:
  One row per WeekSunSat + ChannelGroup
  ChannelGroup values: ALL, PAID SEARCH, ORGANIC SEARCH,
                       DIRECT, SOCIAL, PROGRAMMATIC, OTHER

FILTERS APPLIED:
  - All 7 ChannelGroup values included
  - Only BYOD metrics retained (UvnbByod, UvnbFlowTotal,
    CartstartByod, OrdersUnassistedByod, OrdersAssistedByod)

BUSINESS LOGIC APPLIED:
  - data_source = 'ADOBE'
  - week_sun_to_sat sourced directly from WeekSunSat
    (source already uses Sun-to-Sat convention)
  - ordersTotalByod = OrdersUnassistedByod + OrdersAssistedByod
    NULL if either component is NULL (no COALESCE — consistent
    with source business rules)
  - uvnbByodPctOfUvnbFlow = UvnbByod / NULLIF(UvnbFlowTotal, 0)
    allChannels only — NULL if UvnbFlowTotal is NULL or 0
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
    ALL           → allChannels
    PAID SEARCH   → paidSearch
    ORGANIC SEARCH→ organicSearch
    DIRECT        → direct
    SOCIAL        → social
    PROGRAMMATIC  → programmatic
    OTHER         → other

  Metrics:
    uvnbByod
    uvnbFlowTotal          (allChannels only)
    uvnbByodPctOfUvnbFlow  (allChannels only)
    cartStartByod
    ordersUnassistedByod
    ordersAssistedByod
    ordersTotalByod

CUSTOM WEEK NUMBER:
  Anchored to 2023-01-01 (a Sunday):
    custom_week_num = DATE_DIFF(DATE_SUB(week_sun_to_sat, INTERVAL 6 DAY), DATE '2023-01-01', WEEK)
  LY match: current.custom_week_num - prior.custom_week_num = 52

KEY MODELING NOTES:
  - Source WeekSunSat already follows Sun-to-Sat convention
    matching our pipeline standard — no conversion needed
  - ordersTotalByod computed before pivot so NULL propagation
    is consistent with source business rules
  - uvnbByodPctOfUvnbFlow computed before pivot for same reason
  - Self-joins on small pivoted CTE (1 row per week — very cheap)
  - NULLs preserved — no fake zeroes
  - No ORDER BY — applied in Gold only

DOWNSTREAM:
  Gold Wide : vw_sdi_pulseByod_gold_unified_wide
  Gold Long : vw_sdi_pulseByod_gold_unified_long
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_adobe_weekly`
AS

-- -----------------------------------------------------------------------
-- STEP 1: Extract BYOD metrics per channel, compute derived metrics
-- ordersTotalByod and uvnbByodPctOfUvnbFlow computed here
-- before pivot so NULL propagation is correct
-- -----------------------------------------------------------------------
WITH base AS (
    SELECT
        -- week_sun_to_sat: source already uses Sun-to-Sat convention
        WeekSunSat                                                      AS week_sun_to_sat,
        ChannelGroup,

        -- BYOD UVNB
        UvnbByod,

        -- UVNB Flow Total (allChannels only — kept for all, filtered in pivot)
        UvnbFlowTotal,

        -- UVNB BYOD as % of UVNB Flow Total
        -- Computed here for allChannels — NULL if UvnbFlowTotal is NULL or 0
        -- For non-ALL channels this will be available but only used for allChannels
        CASE
            WHEN UvnbFlowTotal IS NULL OR UvnbFlowTotal = 0 THEN NULL
            ELSE ROUND(UvnbByod / UvnbFlowTotal, 6)
        END                                                             AS uvnbByodPctOfUvnbFlow,

        -- BYOD Cart Start
        CartstartByod,

        -- BYOD Orders
        OrdersUnassistedByod,
        OrdersAssistedByod,

        -- BYOD Orders Total: Unassisted + Assisted
        -- NULL if either component is NULL — consistent with source business rules
        OrdersUnassistedByod + OrdersAssistedByod                      AS ordersTotalByod

    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_flowPerformanceByChannelGroupsPlusAll_Weekly`
),

-- -----------------------------------------------------------------------
-- STEP 2: Pivot long → wide
-- One row per week with each ChannelGroup as separate columns
-- Prefixed with 'adobe_' for unambiguous Gold identification
-- uvnbFlowTotal and uvnbByodPctOfUvnbFlow: allChannels only
-- -----------------------------------------------------------------------
pivoted AS (
    SELECT
        week_sun_to_sat,

        -- ---- ALL CHANNELS ----
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN UvnbByod              END) AS adobe_uvnbByod_allChannels,
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN UvnbFlowTotal         END) AS adobe_uvnbFlowTotal_allChannels,
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN uvnbByodPctOfUvnbFlow END) AS adobe_uvnbByodPctOfUvnbFlow_allChannels,
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN CartstartByod         END) AS adobe_cartStartByod_allChannels,
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN OrdersUnassistedByod  END) AS adobe_ordersUnassistedByod_allChannels,
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN OrdersAssistedByod    END) AS adobe_ordersAssistedByod_allChannels,
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN ordersTotalByod       END) AS adobe_ordersTotalByod_allChannels,

        -- ---- PAID SEARCH ----
        MAX(CASE WHEN ChannelGroup = 'PAID SEARCH' THEN UvnbByod             END) AS adobe_uvnbByod_paidSearch,
        MAX(CASE WHEN ChannelGroup = 'PAID SEARCH' THEN CartstartByod        END) AS adobe_cartStartByod_paidSearch,
        MAX(CASE WHEN ChannelGroup = 'PAID SEARCH' THEN OrdersUnassistedByod END) AS adobe_ordersUnassistedByod_paidSearch,
        MAX(CASE WHEN ChannelGroup = 'PAID SEARCH' THEN OrdersAssistedByod   END) AS adobe_ordersAssistedByod_paidSearch,
        MAX(CASE WHEN ChannelGroup = 'PAID SEARCH' THEN ordersTotalByod      END) AS adobe_ordersTotalByod_paidSearch,

        -- ---- ORGANIC SEARCH ----
        MAX(CASE WHEN ChannelGroup = 'ORGANIC SEARCH' THEN UvnbByod             END) AS adobe_uvnbByod_organicSearch,
        MAX(CASE WHEN ChannelGroup = 'ORGANIC SEARCH' THEN CartstartByod        END) AS adobe_cartStartByod_organicSearch,
        MAX(CASE WHEN ChannelGroup = 'ORGANIC SEARCH' THEN OrdersUnassistedByod END) AS adobe_ordersUnassistedByod_organicSearch,
        MAX(CASE WHEN ChannelGroup = 'ORGANIC SEARCH' THEN OrdersAssistedByod   END) AS adobe_ordersAssistedByod_organicSearch,
        MAX(CASE WHEN ChannelGroup = 'ORGANIC SEARCH' THEN ordersTotalByod      END) AS adobe_ordersTotalByod_organicSearch,

        -- ---- DIRECT ----
        MAX(CASE WHEN ChannelGroup = 'DIRECT' THEN UvnbByod             END) AS adobe_uvnbByod_direct,
        MAX(CASE WHEN ChannelGroup = 'DIRECT' THEN CartstartByod        END) AS adobe_cartStartByod_direct,
        MAX(CASE WHEN ChannelGroup = 'DIRECT' THEN OrdersUnassistedByod END) AS adobe_ordersUnassistedByod_direct,
        MAX(CASE WHEN ChannelGroup = 'DIRECT' THEN OrdersAssistedByod   END) AS adobe_ordersAssistedByod_direct,
        MAX(CASE WHEN ChannelGroup = 'DIRECT' THEN ordersTotalByod      END) AS adobe_ordersTotalByod_direct,

        -- ---- SOCIAL ----
        MAX(CASE WHEN ChannelGroup = 'SOCIAL' THEN UvnbByod             END) AS adobe_uvnbByod_social,
        MAX(CASE WHEN ChannelGroup = 'SOCIAL' THEN CartstartByod        END) AS adobe_cartStartByod_social,
        MAX(CASE WHEN ChannelGroup = 'SOCIAL' THEN OrdersUnassistedByod END) AS adobe_ordersUnassistedByod_social,
        MAX(CASE WHEN ChannelGroup = 'SOCIAL' THEN OrdersAssistedByod   END) AS adobe_ordersAssistedByod_social,
        MAX(CASE WHEN ChannelGroup = 'SOCIAL' THEN ordersTotalByod      END) AS adobe_ordersTotalByod_social,

        -- ---- PROGRAMMATIC ----
        MAX(CASE WHEN ChannelGroup = 'PROGRAMMATIC' THEN UvnbByod             END) AS adobe_uvnbByod_programmatic,
        MAX(CASE WHEN ChannelGroup = 'PROGRAMMATIC' THEN CartstartByod        END) AS adobe_cartStartByod_programmatic,
        MAX(CASE WHEN ChannelGroup = 'PROGRAMMATIC' THEN OrdersUnassistedByod END) AS adobe_ordersUnassistedByod_programmatic,
        MAX(CASE WHEN ChannelGroup = 'PROGRAMMATIC' THEN OrdersAssistedByod   END) AS adobe_ordersAssistedByod_programmatic,
        MAX(CASE WHEN ChannelGroup = 'PROGRAMMATIC' THEN ordersTotalByod      END) AS adobe_ordersTotalByod_programmatic,

        -- ---- OTHER ----
        MAX(CASE WHEN ChannelGroup = 'OTHER' THEN UvnbByod             END) AS adobe_uvnbByod_other,
        MAX(CASE WHEN ChannelGroup = 'OTHER' THEN CartstartByod        END) AS adobe_cartStartByod_other,
        MAX(CASE WHEN ChannelGroup = 'OTHER' THEN OrdersUnassistedByod END) AS adobe_ordersUnassistedByod_other,
        MAX(CASE WHEN ChannelGroup = 'OTHER' THEN OrdersAssistedByod   END) AS adobe_ordersAssistedByod_other,
        MAX(CASE WHEN ChannelGroup = 'OTHER' THEN ordersTotalByod      END) AS adobe_ordersTotalByod_other

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
-- Joins on small pivoted CTE (1 row per week — very cheap)
-- -----------------------------------------------------------------------
with_comparisons AS (
    SELECT
        c.week_sun_to_sat,
        c.custom_week_num,

        -- ALL CHANNELS current
        c.adobe_uvnbByod_allChannels,
        c.adobe_uvnbFlowTotal_allChannels,
        c.adobe_uvnbByodPctOfUvnbFlow_allChannels,
        c.adobe_cartStartByod_allChannels,
        c.adobe_ordersUnassistedByod_allChannels,
        c.adobe_ordersAssistedByod_allChannels,
        c.adobe_ordersTotalByod_allChannels,

        -- ALL CHANNELS WoW
        w.adobe_uvnbByod_allChannels                AS adobe_uvnbByod_allChannels_wow,
        w.adobe_uvnbFlowTotal_allChannels           AS adobe_uvnbFlowTotal_allChannels_wow,
        w.adobe_uvnbByodPctOfUvnbFlow_allChannels   AS adobe_uvnbByodPctOfUvnbFlow_allChannels_wow,
        w.adobe_cartStartByod_allChannels           AS adobe_cartStartByod_allChannels_wow,
        w.adobe_ordersUnassistedByod_allChannels    AS adobe_ordersUnassistedByod_allChannels_wow,
        w.adobe_ordersAssistedByod_allChannels      AS adobe_ordersAssistedByod_allChannels_wow,
        w.adobe_ordersTotalByod_allChannels         AS adobe_ordersTotalByod_allChannels_wow,

        -- ALL CHANNELS LY
        l.adobe_uvnbByod_allChannels                AS adobe_uvnbByod_allChannels_ly,
        l.adobe_uvnbFlowTotal_allChannels           AS adobe_uvnbFlowTotal_allChannels_ly,
        l.adobe_uvnbByodPctOfUvnbFlow_allChannels   AS adobe_uvnbByodPctOfUvnbFlow_allChannels_ly,
        l.adobe_cartStartByod_allChannels           AS adobe_cartStartByod_allChannels_ly,
        l.adobe_ordersUnassistedByod_allChannels    AS adobe_ordersUnassistedByod_allChannels_ly,
        l.adobe_ordersAssistedByod_allChannels      AS adobe_ordersAssistedByod_allChannels_ly,
        l.adobe_ordersTotalByod_allChannels         AS adobe_ordersTotalByod_allChannels_ly,

        -- PAID SEARCH current
        c.adobe_uvnbByod_paidSearch,
        c.adobe_cartStartByod_paidSearch,
        c.adobe_ordersUnassistedByod_paidSearch,
        c.adobe_ordersAssistedByod_paidSearch,
        c.adobe_ordersTotalByod_paidSearch,

        -- PAID SEARCH WoW
        w.adobe_uvnbByod_paidSearch                 AS adobe_uvnbByod_paidSearch_wow,
        w.adobe_cartStartByod_paidSearch            AS adobe_cartStartByod_paidSearch_wow,
        w.adobe_ordersUnassistedByod_paidSearch     AS adobe_ordersUnassistedByod_paidSearch_wow,
        w.adobe_ordersAssistedByod_paidSearch       AS adobe_ordersAssistedByod_paidSearch_wow,
        w.adobe_ordersTotalByod_paidSearch          AS adobe_ordersTotalByod_paidSearch_wow,

        -- PAID SEARCH LY
        l.adobe_uvnbByod_paidSearch                 AS adobe_uvnbByod_paidSearch_ly,
        l.adobe_cartStartByod_paidSearch            AS adobe_cartStartByod_paidSearch_ly,
        l.adobe_ordersUnassistedByod_paidSearch     AS adobe_ordersUnassistedByod_paidSearch_ly,
        l.adobe_ordersAssistedByod_paidSearch       AS adobe_ordersAssistedByod_paidSearch_ly,
        l.adobe_ordersTotalByod_paidSearch          AS adobe_ordersTotalByod_paidSearch_ly,

        -- ORGANIC SEARCH current
        c.adobe_uvnbByod_organicSearch,
        c.adobe_cartStartByod_organicSearch,
        c.adobe_ordersUnassistedByod_organicSearch,
        c.adobe_ordersAssistedByod_organicSearch,
        c.adobe_ordersTotalByod_organicSearch,

        -- ORGANIC SEARCH WoW
        w.adobe_uvnbByod_organicSearch              AS adobe_uvnbByod_organicSearch_wow,
        w.adobe_cartStartByod_organicSearch         AS adobe_cartStartByod_organicSearch_wow,
        w.adobe_ordersUnassistedByod_organicSearch  AS adobe_ordersUnassistedByod_organicSearch_wow,
        w.adobe_ordersAssistedByod_organicSearch    AS adobe_ordersAssistedByod_organicSearch_wow,
        w.adobe_ordersTotalByod_organicSearch       AS adobe_ordersTotalByod_organicSearch_wow,

        -- ORGANIC SEARCH LY
        l.adobe_uvnbByod_organicSearch              AS adobe_uvnbByod_organicSearch_ly,
        l.adobe_cartStartByod_organicSearch         AS adobe_cartStartByod_organicSearch_ly,
        l.adobe_ordersUnassistedByod_organicSearch  AS adobe_ordersUnassistedByod_organicSearch_ly,
        l.adobe_ordersAssistedByod_organicSearch    AS adobe_ordersAssistedByod_organicSearch_ly,
        l.adobe_ordersTotalByod_organicSearch       AS adobe_ordersTotalByod_organicSearch_ly,

        -- DIRECT current
        c.adobe_uvnbByod_direct,
        c.adobe_cartStartByod_direct,
        c.adobe_ordersUnassistedByod_direct,
        c.adobe_ordersAssistedByod_direct,
        c.adobe_ordersTotalByod_direct,

        -- DIRECT WoW
        w.adobe_uvnbByod_direct                     AS adobe_uvnbByod_direct_wow,
        w.adobe_cartStartByod_direct                AS adobe_cartStartByod_direct_wow,
        w.adobe_ordersUnassistedByod_direct         AS adobe_ordersUnassistedByod_direct_wow,
        w.adobe_ordersAssistedByod_direct           AS adobe_ordersAssistedByod_direct_wow,
        w.adobe_ordersTotalByod_direct              AS adobe_ordersTotalByod_direct_wow,

        -- DIRECT LY
        l.adobe_uvnbByod_direct                     AS adobe_uvnbByod_direct_ly,
        l.adobe_cartStartByod_direct                AS adobe_cartStartByod_direct_ly,
        l.adobe_ordersUnassistedByod_direct         AS adobe_ordersUnassistedByod_direct_ly,
        l.adobe_ordersAssistedByod_direct           AS adobe_ordersAssistedByod_direct_ly,
        l.adobe_ordersTotalByod_direct              AS adobe_ordersTotalByod_direct_ly,

        -- SOCIAL current
        c.adobe_uvnbByod_social,
        c.adobe_cartStartByod_social,
        c.adobe_ordersUnassistedByod_social,
        c.adobe_ordersAssistedByod_social,
        c.adobe_ordersTotalByod_social,

        -- SOCIAL WoW
        w.adobe_uvnbByod_social                     AS adobe_uvnbByod_social_wow,
        w.adobe_cartStartByod_social                AS adobe_cartStartByod_social_wow,
        w.adobe_ordersUnassistedByod_social         AS adobe_ordersUnassistedByod_social_wow,
        w.adobe_ordersAssistedByod_social           AS adobe_ordersAssistedByod_social_wow,
        w.adobe_ordersTotalByod_social              AS adobe_ordersTotalByod_social_wow,

        -- SOCIAL LY
        l.adobe_uvnbByod_social                     AS adobe_uvnbByod_social_ly,
        l.adobe_cartStartByod_social                AS adobe_cartStartByod_social_ly,
        l.adobe_ordersUnassistedByod_social         AS adobe_ordersUnassistedByod_social_ly,
        l.adobe_ordersAssistedByod_social           AS adobe_ordersAssistedByod_social_ly,
        l.adobe_ordersTotalByod_social              AS adobe_ordersTotalByod_social_ly,

        -- PROGRAMMATIC current
        c.adobe_uvnbByod_programmatic,
        c.adobe_cartStartByod_programmatic,
        c.adobe_ordersUnassistedByod_programmatic,
        c.adobe_ordersAssistedByod_programmatic,
        c.adobe_ordersTotalByod_programmatic,

        -- PROGRAMMATIC WoW
        w.adobe_uvnbByod_programmatic               AS adobe_uvnbByod_programmatic_wow,
        w.adobe_cartStartByod_programmatic          AS adobe_cartStartByod_programmatic_wow,
        w.adobe_ordersUnassistedByod_programmatic   AS adobe_ordersUnassistedByod_programmatic_wow,
        w.adobe_ordersAssistedByod_programmatic     AS adobe_ordersAssistedByod_programmatic_wow,
        w.adobe_ordersTotalByod_programmatic        AS adobe_ordersTotalByod_programmatic_wow,

        -- PROGRAMMATIC LY
        l.adobe_uvnbByod_programmatic               AS adobe_uvnbByod_programmatic_ly,
        l.adobe_cartStartByod_programmatic          AS adobe_cartStartByod_programmatic_ly,
        l.adobe_ordersUnassistedByod_programmatic   AS adobe_ordersUnassistedByod_programmatic_ly,
        l.adobe_ordersAssistedByod_programmatic     AS adobe_ordersAssistedByod_programmatic_ly,
        l.adobe_ordersTotalByod_programmatic        AS adobe_ordersTotalByod_programmatic_ly,

        -- OTHER current
        c.adobe_uvnbByod_other,
        c.adobe_cartStartByod_other,
        c.adobe_ordersUnassistedByod_other,
        c.adobe_ordersAssistedByod_other,
        c.adobe_ordersTotalByod_other,

        -- OTHER WoW
        w.adobe_uvnbByod_other                      AS adobe_uvnbByod_other_wow,
        w.adobe_cartStartByod_other                 AS adobe_cartStartByod_other_wow,
        w.adobe_ordersUnassistedByod_other          AS adobe_ordersUnassistedByod_other_wow,
        w.adobe_ordersAssistedByod_other            AS adobe_ordersAssistedByod_other_wow,
        w.adobe_ordersTotalByod_other               AS adobe_ordersTotalByod_other_wow,

        -- OTHER LY
        l.adobe_uvnbByod_other                      AS adobe_uvnbByod_other_ly,
        l.adobe_cartStartByod_other                 AS adobe_cartStartByod_other_ly,
        l.adobe_ordersUnassistedByod_other          AS adobe_ordersUnassistedByod_other_ly,
        l.adobe_ordersAssistedByod_other            AS adobe_ordersAssistedByod_other_ly,
        l.adobe_ordersTotalByod_other               AS adobe_ordersTotalByod_other_ly

    FROM with_week_num c
    LEFT JOIN with_week_num w
        ON c.week_sun_to_sat = DATE_ADD(w.week_sun_to_sat, INTERVAL 7 DAY)
    LEFT JOIN with_week_num l
        ON (c.custom_week_num - l.custom_week_num) = 52
),

-- -----------------------------------------------------------------------
-- STEP 5: Compute wow_pct and yoy_pct for all metrics
-- Reusable macro pattern: NULL when prior NULL or 0
-- -----------------------------------------------------------------------
with_pcts AS (
    SELECT
        week_sun_to_sat,
        custom_week_num,

        -- ================================================================
        -- ALL CHANNELS
        -- ================================================================
        adobe_uvnbByod_allChannels,
        adobe_uvnbByod_allChannels_wow,
        adobe_uvnbByod_allChannels_ly,
        CASE WHEN adobe_uvnbByod_allChannels_wow IS NULL OR adobe_uvnbByod_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_allChannels - adobe_uvnbByod_allChannels_wow) / adobe_uvnbByod_allChannels_wow, 6) END AS adobe_uvnbByod_allChannels_wow_pct,
        CASE WHEN adobe_uvnbByod_allChannels_ly  IS NULL OR adobe_uvnbByod_allChannels_ly  = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_allChannels - adobe_uvnbByod_allChannels_ly)  / adobe_uvnbByod_allChannels_ly,  6) END AS adobe_uvnbByod_allChannels_yoy_pct,

        adobe_uvnbFlowTotal_allChannels,
        adobe_uvnbFlowTotal_allChannels_wow,
        adobe_uvnbFlowTotal_allChannels_ly,
        CASE WHEN adobe_uvnbFlowTotal_allChannels_wow IS NULL OR adobe_uvnbFlowTotal_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbFlowTotal_allChannels - adobe_uvnbFlowTotal_allChannels_wow) / adobe_uvnbFlowTotal_allChannels_wow, 6) END AS adobe_uvnbFlowTotal_allChannels_wow_pct,
        CASE WHEN adobe_uvnbFlowTotal_allChannels_ly  IS NULL OR adobe_uvnbFlowTotal_allChannels_ly  = 0 THEN NULL ELSE ROUND((adobe_uvnbFlowTotal_allChannels - adobe_uvnbFlowTotal_allChannels_ly)  / adobe_uvnbFlowTotal_allChannels_ly,  6) END AS adobe_uvnbFlowTotal_allChannels_yoy_pct,

        adobe_uvnbByodPctOfUvnbFlow_allChannels,
        adobe_uvnbByodPctOfUvnbFlow_allChannels_wow,
        adobe_uvnbByodPctOfUvnbFlow_allChannels_ly,
        CASE WHEN adobe_uvnbByodPctOfUvnbFlow_allChannels_wow IS NULL OR adobe_uvnbByodPctOfUvnbFlow_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbByodPctOfUvnbFlow_allChannels - adobe_uvnbByodPctOfUvnbFlow_allChannels_wow) / adobe_uvnbByodPctOfUvnbFlow_allChannels_wow, 6) END AS adobe_uvnbByodPctOfUvnbFlow_allChannels_wow_pct,
        CASE WHEN adobe_uvnbByodPctOfUvnbFlow_allChannels_ly  IS NULL OR adobe_uvnbByodPctOfUvnbFlow_allChannels_ly  = 0 THEN NULL ELSE ROUND((adobe_uvnbByodPctOfUvnbFlow_allChannels - adobe_uvnbByodPctOfUvnbFlow_allChannels_ly)  / adobe_uvnbByodPctOfUvnbFlow_allChannels_ly,  6) END AS adobe_uvnbByodPctOfUvnbFlow_allChannels_yoy_pct,

        adobe_cartStartByod_allChannels,
        adobe_cartStartByod_allChannels_wow,
        adobe_cartStartByod_allChannels_ly,
        CASE WHEN adobe_cartStartByod_allChannels_wow IS NULL OR adobe_cartStartByod_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_allChannels - adobe_cartStartByod_allChannels_wow) / adobe_cartStartByod_allChannels_wow, 6) END AS adobe_cartStartByod_allChannels_wow_pct,
        CASE WHEN adobe_cartStartByod_allChannels_ly  IS NULL OR adobe_cartStartByod_allChannels_ly  = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_allChannels - adobe_cartStartByod_allChannels_ly)  / adobe_cartStartByod_allChannels_ly,  6) END AS adobe_cartStartByod_allChannels_yoy_pct,

        adobe_ordersUnassistedByod_allChannels,
        adobe_ordersUnassistedByod_allChannels_wow,
        adobe_ordersUnassistedByod_allChannels_ly,
        CASE WHEN adobe_ordersUnassistedByod_allChannels_wow IS NULL OR adobe_ordersUnassistedByod_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_allChannels - adobe_ordersUnassistedByod_allChannels_wow) / adobe_ordersUnassistedByod_allChannels_wow, 6) END AS adobe_ordersUnassistedByod_allChannels_wow_pct,
        CASE WHEN adobe_ordersUnassistedByod_allChannels_ly  IS NULL OR adobe_ordersUnassistedByod_allChannels_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_allChannels - adobe_ordersUnassistedByod_allChannels_ly)  / adobe_ordersUnassistedByod_allChannels_ly,  6) END AS adobe_ordersUnassistedByod_allChannels_yoy_pct,

        adobe_ordersAssistedByod_allChannels,
        adobe_ordersAssistedByod_allChannels_wow,
        adobe_ordersAssistedByod_allChannels_ly,
        CASE WHEN adobe_ordersAssistedByod_allChannels_wow IS NULL OR adobe_ordersAssistedByod_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_allChannels - adobe_ordersAssistedByod_allChannels_wow) / adobe_ordersAssistedByod_allChannels_wow, 6) END AS adobe_ordersAssistedByod_allChannels_wow_pct,
        CASE WHEN adobe_ordersAssistedByod_allChannels_ly  IS NULL OR adobe_ordersAssistedByod_allChannels_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_allChannels - adobe_ordersAssistedByod_allChannels_ly)  / adobe_ordersAssistedByod_allChannels_ly,  6) END AS adobe_ordersAssistedByod_allChannels_yoy_pct,

        adobe_ordersTotalByod_allChannels,
        adobe_ordersTotalByod_allChannels_wow,
        adobe_ordersTotalByod_allChannels_ly,
        CASE WHEN adobe_ordersTotalByod_allChannels_wow IS NULL OR adobe_ordersTotalByod_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_ordersTotalByod_allChannels - adobe_ordersTotalByod_allChannels_wow) / adobe_ordersTotalByod_allChannels_wow, 6) END AS adobe_ordersTotalByod_allChannels_wow_pct,
        CASE WHEN adobe_ordersTotalByod_allChannels_ly  IS NULL OR adobe_ordersTotalByod_allChannels_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersTotalByod_allChannels - adobe_ordersTotalByod_allChannels_ly)  / adobe_ordersTotalByod_allChannels_ly,  6) END AS adobe_ordersTotalByod_allChannels_yoy_pct,

        -- ================================================================
        -- PAID SEARCH
        -- ================================================================
        adobe_uvnbByod_paidSearch,
        adobe_uvnbByod_paidSearch_wow,
        adobe_uvnbByod_paidSearch_ly,
        CASE WHEN adobe_uvnbByod_paidSearch_wow IS NULL OR adobe_uvnbByod_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_paidSearch - adobe_uvnbByod_paidSearch_wow) / adobe_uvnbByod_paidSearch_wow, 6) END AS adobe_uvnbByod_paidSearch_wow_pct,
        CASE WHEN adobe_uvnbByod_paidSearch_ly  IS NULL OR adobe_uvnbByod_paidSearch_ly  = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_paidSearch - adobe_uvnbByod_paidSearch_ly)  / adobe_uvnbByod_paidSearch_ly,  6) END AS adobe_uvnbByod_paidSearch_yoy_pct,

        adobe_cartStartByod_paidSearch,
        adobe_cartStartByod_paidSearch_wow,
        adobe_cartStartByod_paidSearch_ly,
        CASE WHEN adobe_cartStartByod_paidSearch_wow IS NULL OR adobe_cartStartByod_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_paidSearch - adobe_cartStartByod_paidSearch_wow) / adobe_cartStartByod_paidSearch_wow, 6) END AS adobe_cartStartByod_paidSearch_wow_pct,
        CASE WHEN adobe_cartStartByod_paidSearch_ly  IS NULL OR adobe_cartStartByod_paidSearch_ly  = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_paidSearch - adobe_cartStartByod_paidSearch_ly)  / adobe_cartStartByod_paidSearch_ly,  6) END AS adobe_cartStartByod_paidSearch_yoy_pct,

        adobe_ordersUnassistedByod_paidSearch,
        adobe_ordersUnassistedByod_paidSearch_wow,
        adobe_ordersUnassistedByod_paidSearch_ly,
        CASE WHEN adobe_ordersUnassistedByod_paidSearch_wow IS NULL OR adobe_ordersUnassistedByod_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_paidSearch - adobe_ordersUnassistedByod_paidSearch_wow) / adobe_ordersUnassistedByod_paidSearch_wow, 6) END AS adobe_ordersUnassistedByod_paidSearch_wow_pct,
        CASE WHEN adobe_ordersUnassistedByod_paidSearch_ly  IS NULL OR adobe_ordersUnassistedByod_paidSearch_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_paidSearch - adobe_ordersUnassistedByod_paidSearch_ly)  / adobe_ordersUnassistedByod_paidSearch_ly,  6) END AS adobe_ordersUnassistedByod_paidSearch_yoy_pct,

        adobe_ordersAssistedByod_paidSearch,
        adobe_ordersAssistedByod_paidSearch_wow,
        adobe_ordersAssistedByod_paidSearch_ly,
        CASE WHEN adobe_ordersAssistedByod_paidSearch_wow IS NULL OR adobe_ordersAssistedByod_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_paidSearch - adobe_ordersAssistedByod_paidSearch_wow) / adobe_ordersAssistedByod_paidSearch_wow, 6) END AS adobe_ordersAssistedByod_paidSearch_wow_pct,
        CASE WHEN adobe_ordersAssistedByod_paidSearch_ly  IS NULL OR adobe_ordersAssistedByod_paidSearch_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_paidSearch - adobe_ordersAssistedByod_paidSearch_ly)  / adobe_ordersAssistedByod_paidSearch_ly,  6) END AS adobe_ordersAssistedByod_paidSearch_yoy_pct,

        adobe_ordersTotalByod_paidSearch,
        adobe_ordersTotalByod_paidSearch_wow,
        adobe_ordersTotalByod_paidSearch_ly,
        CASE WHEN adobe_ordersTotalByod_paidSearch_wow IS NULL OR adobe_ordersTotalByod_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersTotalByod_paidSearch - adobe_ordersTotalByod_paidSearch_wow) / adobe_ordersTotalByod_paidSearch_wow, 6) END AS adobe_ordersTotalByod_paidSearch_wow_pct,
        CASE WHEN adobe_ordersTotalByod_paidSearch_ly  IS NULL OR adobe_ordersTotalByod_paidSearch_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersTotalByod_paidSearch - adobe_ordersTotalByod_paidSearch_ly)  / adobe_ordersTotalByod_paidSearch_ly,  6) END AS adobe_ordersTotalByod_paidSearch_yoy_pct,

        -- ================================================================
        -- ORGANIC SEARCH
        -- ================================================================
        adobe_uvnbByod_organicSearch,
        adobe_uvnbByod_organicSearch_wow,
        adobe_uvnbByod_organicSearch_ly,
        CASE WHEN adobe_uvnbByod_organicSearch_wow IS NULL OR adobe_uvnbByod_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_organicSearch - adobe_uvnbByod_organicSearch_wow) / adobe_uvnbByod_organicSearch_wow, 6) END AS adobe_uvnbByod_organicSearch_wow_pct,
        CASE WHEN adobe_uvnbByod_organicSearch_ly  IS NULL OR adobe_uvnbByod_organicSearch_ly  = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_organicSearch - adobe_uvnbByod_organicSearch_ly)  / adobe_uvnbByod_organicSearch_ly,  6) END AS adobe_uvnbByod_organicSearch_yoy_pct,

        adobe_cartStartByod_organicSearch,
        adobe_cartStartByod_organicSearch_wow,
        adobe_cartStartByod_organicSearch_ly,
        CASE WHEN adobe_cartStartByod_organicSearch_wow IS NULL OR adobe_cartStartByod_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_organicSearch - adobe_cartStartByod_organicSearch_wow) / adobe_cartStartByod_organicSearch_wow, 6) END AS adobe_cartStartByod_organicSearch_wow_pct,
        CASE WHEN adobe_cartStartByod_organicSearch_ly  IS NULL OR adobe_cartStartByod_organicSearch_ly  = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_organicSearch - adobe_cartStartByod_organicSearch_ly)  / adobe_cartStartByod_organicSearch_ly,  6) END AS adobe_cartStartByod_organicSearch_yoy_pct,

        adobe_ordersUnassistedByod_organicSearch,
        adobe_ordersUnassistedByod_organicSearch_wow,
        adobe_ordersUnassistedByod_organicSearch_ly,
        CASE WHEN adobe_ordersUnassistedByod_organicSearch_wow IS NULL OR adobe_ordersUnassistedByod_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_organicSearch - adobe_ordersUnassistedByod_organicSearch_wow) / adobe_ordersUnassistedByod_organicSearch_wow, 6) END AS adobe_ordersUnassistedByod_organicSearch_wow_pct,
        CASE WHEN adobe_ordersUnassistedByod_organicSearch_ly  IS NULL OR adobe_ordersUnassistedByod_organicSearch_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_organicSearch - adobe_ordersUnassistedByod_organicSearch_ly)  / adobe_ordersUnassistedByod_organicSearch_ly,  6) END AS adobe_ordersUnassistedByod_organicSearch_yoy_pct,

        adobe_ordersAssistedByod_organicSearch,
        adobe_ordersAssistedByod_organicSearch_wow,
        adobe_ordersAssistedByod_organicSearch_ly,
        CASE WHEN adobe_ordersAssistedByod_organicSearch_wow IS NULL OR adobe_ordersAssistedByod_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_organicSearch - adobe_ordersAssistedByod_organicSearch_wow) / adobe_ordersAssistedByod_organicSearch_wow, 6) END AS adobe_ordersAssistedByod_organicSearch_wow_pct,
        CASE WHEN adobe_ordersAssistedByod_organicSearch_ly  IS NULL OR adobe_ordersAssistedByod_organicSearch_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_organicSearch - adobe_ordersAssistedByod_organicSearch_ly)  / adobe_ordersAssistedByod_organicSearch_ly,  6) END AS adobe_ordersAssistedByod_organicSearch_yoy_pct,

        adobe_ordersTotalByod_organicSearch,
        adobe_ordersTotalByod_organicSearch_wow,
        adobe_ordersTotalByod_organicSearch_ly,
        CASE WHEN adobe_ordersTotalByod_organicSearch_wow IS NULL OR adobe_ordersTotalByod_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersTotalByod_organicSearch - adobe_ordersTotalByod_organicSearch_wow) / adobe_ordersTotalByod_organicSearch_wow, 6) END AS adobe_ordersTotalByod_organicSearch_wow_pct,
        CASE WHEN adobe_ordersTotalByod_organicSearch_ly  IS NULL OR adobe_ordersTotalByod_organicSearch_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersTotalByod_organicSearch - adobe_ordersTotalByod_organicSearch_ly)  / adobe_ordersTotalByod_organicSearch_ly,  6) END AS adobe_ordersTotalByod_organicSearch_yoy_pct,

        -- ================================================================
        -- DIRECT
        -- ================================================================
        adobe_uvnbByod_direct,
        adobe_uvnbByod_direct_wow,
        adobe_uvnbByod_direct_ly,
        CASE WHEN adobe_uvnbByod_direct_wow IS NULL OR adobe_uvnbByod_direct_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_direct - adobe_uvnbByod_direct_wow) / adobe_uvnbByod_direct_wow, 6) END AS adobe_uvnbByod_direct_wow_pct,
        CASE WHEN adobe_uvnbByod_direct_ly  IS NULL OR adobe_uvnbByod_direct_ly  = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_direct - adobe_uvnbByod_direct_ly)  / adobe_uvnbByod_direct_ly,  6) END AS adobe_uvnbByod_direct_yoy_pct,

        adobe_cartStartByod_direct,
        adobe_cartStartByod_direct_wow,
        adobe_cartStartByod_direct_ly,
        CASE WHEN adobe_cartStartByod_direct_wow IS NULL OR adobe_cartStartByod_direct_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_direct - adobe_cartStartByod_direct_wow) / adobe_cartStartByod_direct_wow, 6) END AS adobe_cartStartByod_direct_wow_pct,
        CASE WHEN adobe_cartStartByod_direct_ly  IS NULL OR adobe_cartStartByod_direct_ly  = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_direct - adobe_cartStartByod_direct_ly)  / adobe_cartStartByod_direct_ly,  6) END AS adobe_cartStartByod_direct_yoy_pct,

        adobe_ordersUnassistedByod_direct,
        adobe_ordersUnassistedByod_direct_wow,
        adobe_ordersUnassistedByod_direct_ly,
        CASE WHEN adobe_ordersUnassistedByod_direct_wow IS NULL OR adobe_ordersUnassistedByod_direct_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_direct - adobe_ordersUnassistedByod_direct_wow) / adobe_ordersUnassistedByod_direct_wow, 6) END AS adobe_ordersUnassistedByod_direct_wow_pct,
        CASE WHEN adobe_ordersUnassistedByod_direct_ly  IS NULL OR adobe_ordersUnassistedByod_direct_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_direct - adobe_ordersUnassistedByod_direct_ly)  / adobe_ordersUnassistedByod_direct_ly,  6) END AS adobe_ordersUnassistedByod_direct_yoy_pct,

        adobe_ordersAssistedByod_direct,
        adobe_ordersAssistedByod_direct_wow,
        adobe_ordersAssistedByod_direct_ly,
        CASE WHEN adobe_ordersAssistedByod_direct_wow IS NULL OR adobe_ordersAssistedByod_direct_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_direct - adobe_ordersAssistedByod_direct_wow) / adobe_ordersAssistedByod_direct_wow, 6) END AS adobe_ordersAssistedByod_direct_wow_pct,
        CASE WHEN adobe_ordersAssistedByod_direct_ly  IS NULL OR adobe_ordersAssistedByod_direct_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_direct - adobe_ordersAssistedByod_direct_ly)  / adobe_ordersAssistedByod_direct_ly,  6) END AS adobe_ordersAssistedByod_direct_yoy_pct,

        adobe_ordersTotalByod_direct,
        adobe_ordersTotalByod_direct_wow,
        adobe_ordersTotalByod_direct_ly,
        CASE WHEN adobe_ordersTotalByod_direct_wow IS NULL OR adobe_ordersTotalByod_direct_wow = 0 THEN NULL ELSE ROUND((adobe_ordersTotalByod_direct - adobe_ordersTotalByod_direct_wow) / adobe_ordersTotalByod_direct_wow, 6) END AS adobe_ordersTotalByod_direct_wow_pct,
        CASE WHEN adobe_ordersTotalByod_direct_ly  IS NULL OR adobe_ordersTotalByod_direct_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersTotalByod_direct - adobe_ordersTotalByod_direct_ly)  / adobe_ordersTotalByod_direct_ly,  6) END AS adobe_ordersTotalByod_direct_yoy_pct,

        -- ================================================================
        -- SOCIAL
        -- ================================================================
        adobe_uvnbByod_social,
        adobe_uvnbByod_social_wow,
        adobe_uvnbByod_social_ly,
        CASE WHEN adobe_uvnbByod_social_wow IS NULL OR adobe_uvnbByod_social_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_social - adobe_uvnbByod_social_wow) / adobe_uvnbByod_social_wow, 6) END AS adobe_uvnbByod_social_wow_pct,
        CASE WHEN adobe_uvnbByod_social_ly  IS NULL OR adobe_uvnbByod_social_ly  = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_social - adobe_uvnbByod_social_ly)  / adobe_uvnbByod_social_ly,  6) END AS adobe_uvnbByod_social_yoy_pct,

        adobe_cartStartByod_social,
        adobe_cartStartByod_social_wow,
        adobe_cartStartByod_social_ly,
        CASE WHEN adobe_cartStartByod_social_wow IS NULL OR adobe_cartStartByod_social_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_social - adobe_cartStartByod_social_wow) / adobe_cartStartByod_social_wow, 6) END AS adobe_cartStartByod_social_wow_pct,
        CASE WHEN adobe_cartStartByod_social_ly  IS NULL OR adobe_cartStartByod_social_ly  = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_social - adobe_cartStartByod_social_ly)  / adobe_cartStartByod_social_ly,  6) END AS adobe_cartStartByod_social_yoy_pct,

        adobe_ordersUnassistedByod_social,
        adobe_ordersUnassistedByod_social_wow,
        adobe_ordersUnassistedByod_social_ly,
        CASE WHEN adobe_ordersUnassistedByod_social_wow IS NULL OR adobe_ordersUnassistedByod_social_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_social - adobe_ordersUnassistedByod_social_wow) / adobe_ordersUnassistedByod_social_wow, 6) END AS adobe_ordersUnassistedByod_social_wow_pct,
        CASE WHEN adobe_ordersUnassistedByod_social_ly  IS NULL OR adobe_ordersUnassistedByod_social_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_social - adobe_ordersUnassistedByod_social_ly)  / adobe_ordersUnassistedByod_social_ly,  6) END AS adobe_ordersUnassistedByod_social_yoy_pct,

        adobe_ordersAssistedByod_social,
        adobe_ordersAssistedByod_social_wow,
        adobe_ordersAssistedByod_social_ly,
        CASE WHEN adobe_ordersAssistedByod_social_wow IS NULL OR adobe_ordersAssistedByod_social_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_social - adobe_ordersAssistedByod_social_wow) / adobe_ordersAssistedByod_social_wow, 6) END AS adobe_ordersAssistedByod_social_wow_pct,
        CASE WHEN adobe_ordersAssistedByod_social_ly  IS NULL OR adobe_ordersAssistedByod_social_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_social - adobe_ordersAssistedByod_social_ly)  / adobe_ordersAssistedByod_social_ly,  6) END AS adobe_ordersAssistedByod_social_yoy_pct,

        adobe_ordersTotalByod_social,
        adobe_ordersTotalByod_social_wow,
        adobe_ordersTotalByod_social_ly,
        CASE WHEN adobe_ordersTotalByod_social_wow IS NULL OR adobe_ordersTotalByod_social_wow = 0 THEN NULL ELSE ROUND((adobe_ordersTotalByod_social - adobe_ordersTotalByod_social_wow) / adobe_ordersTotalByod_social_wow, 6) END AS adobe_ordersTotalByod_social_wow_pct,
        CASE WHEN adobe_ordersTotalByod_social_ly  IS NULL OR adobe_ordersTotalByod_social_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersTotalByod_social - adobe_ordersTotalByod_social_ly)  / adobe_ordersTotalByod_social_ly,  6) END AS adobe_ordersTotalByod_social_yoy_pct,

        -- ================================================================
        -- PROGRAMMATIC
        -- ================================================================
        adobe_uvnbByod_programmatic,
        adobe_uvnbByod_programmatic_wow,
        adobe_uvnbByod_programmatic_ly,
        CASE WHEN adobe_uvnbByod_programmatic_wow IS NULL OR adobe_uvnbByod_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_programmatic - adobe_uvnbByod_programmatic_wow) / adobe_uvnbByod_programmatic_wow, 6) END AS adobe_uvnbByod_programmatic_wow_pct,
        CASE WHEN adobe_uvnbByod_programmatic_ly  IS NULL OR adobe_uvnbByod_programmatic_ly  = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_programmatic - adobe_uvnbByod_programmatic_ly)  / adobe_uvnbByod_programmatic_ly,  6) END AS adobe_uvnbByod_programmatic_yoy_pct,

        adobe_cartStartByod_programmatic,
        adobe_cartStartByod_programmatic_wow,
        adobe_cartStartByod_programmatic_ly,
        CASE WHEN adobe_cartStartByod_programmatic_wow IS NULL OR adobe_cartStartByod_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_programmatic - adobe_cartStartByod_programmatic_wow) / adobe_cartStartByod_programmatic_wow, 6) END AS adobe_cartStartByod_programmatic_wow_pct,
        CASE WHEN adobe_cartStartByod_programmatic_ly  IS NULL OR adobe_cartStartByod_programmatic_ly  = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_programmatic - adobe_cartStartByod_programmatic_ly)  / adobe_cartStartByod_programmatic_ly,  6) END AS adobe_cartStartByod_programmatic_yoy_pct,

        adobe_ordersUnassistedByod_programmatic,
        adobe_ordersUnassistedByod_programmatic_wow,
        adobe_ordersUnassistedByod_programmatic_ly,
        CASE WHEN adobe_ordersUnassistedByod_programmatic_wow IS NULL OR adobe_ordersUnassistedByod_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_programmatic - adobe_ordersUnassistedByod_programmatic_wow) / adobe_ordersUnassistedByod_programmatic_wow, 6) END AS adobe_ordersUnassistedByod_programmatic_wow_pct,
        CASE WHEN adobe_ordersUnassistedByod_programmatic_ly  IS NULL OR adobe_ordersUnassistedByod_programmatic_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_programmatic - adobe_ordersUnassistedByod_programmatic_ly)  / adobe_ordersUnassistedByod_programmatic_ly,  6) END AS adobe_ordersUnassistedByod_programmatic_yoy_pct,

        adobe_ordersAssistedByod_programmatic,
        adobe_ordersAssistedByod_programmatic_wow,
        adobe_ordersAssistedByod_programmatic_ly,
        CASE WHEN adobe_ordersAssistedByod_programmatic_wow IS NULL OR adobe_ordersAssistedByod_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_programmatic - adobe_ordersAssistedByod_programmatic_wow) / adobe_ordersAssistedByod_programmatic_wow, 6) END AS adobe_ordersAssistedByod_programmatic_wow_pct,
        CASE WHEN adobe_ordersAssistedByod_programmatic_ly  IS NULL OR adobe_ordersAssistedByod_programmatic_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_programmatic - adobe_ordersAssistedByod_programmatic_ly)  / adobe_ordersAssistedByod_programmatic_ly,  6) END AS adobe_ordersAssistedByod_programmatic_yoy_pct,

        adobe_ordersTotalByod_programmatic,
        adobe_ordersTotalByod_programmatic_wow,
        adobe_ordersTotalByod_programmatic_ly,
        CASE WHEN adobe_ordersTotalByod_programmatic_wow IS NULL OR adobe_ordersTotalByod_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_ordersTotalByod_programmatic - adobe_ordersTotalByod_programmatic_wow) / adobe_ordersTotalByod_programmatic_wow, 6) END AS adobe_ordersTotalByod_programmatic_wow_pct,
        CASE WHEN adobe_ordersTotalByod_programmatic_ly  IS NULL OR adobe_ordersTotalByod_programmatic_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersTotalByod_programmatic - adobe_ordersTotalByod_programmatic_ly)  / adobe_ordersTotalByod_programmatic_ly,  6) END AS adobe_ordersTotalByod_programmatic_yoy_pct,

        -- ================================================================
        -- OTHER
        -- ================================================================
        adobe_uvnbByod_other,
        adobe_uvnbByod_other_wow,
        adobe_uvnbByod_other_ly,
        CASE WHEN adobe_uvnbByod_other_wow IS NULL OR adobe_uvnbByod_other_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_other - adobe_uvnbByod_other_wow) / adobe_uvnbByod_other_wow, 6) END AS adobe_uvnbByod_other_wow_pct,
        CASE WHEN adobe_uvnbByod_other_ly  IS NULL OR adobe_uvnbByod_other_ly  = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_other - adobe_uvnbByod_other_ly)  / adobe_uvnbByod_other_ly,  6) END AS adobe_uvnbByod_other_yoy_pct,

        adobe_cartStartByod_other,
        adobe_cartStartByod_other_wow,
        adobe_cartStartByod_other_ly,
        CASE WHEN adobe_cartStartByod_other_wow IS NULL OR adobe_cartStartByod_other_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_other - adobe_cartStartByod_other_wow) / adobe_cartStartByod_other_wow, 6) END AS adobe_cartStartByod_other_wow_pct,
        CASE WHEN adobe_cartStartByod_other_ly  IS NULL OR adobe_cartStartByod_other_ly  = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_other - adobe_cartStartByod_other_ly)  / adobe_cartStartByod_other_ly,  6) END AS adobe_cartStartByod_other_yoy_pct,

        adobe_ordersUnassistedByod_other,
        adobe_ordersUnassistedByod_other_wow,
        adobe_ordersUnassistedByod_other_ly,
        CASE WHEN adobe_ordersUnassistedByod_other_wow IS NULL OR adobe_ordersUnassistedByod_other_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_other - adobe_ordersUnassistedByod_other_wow) / adobe_ordersUnassistedByod_other_wow, 6) END AS adobe_ordersUnassistedByod_other_wow_pct,
        CASE WHEN adobe_ordersUnassistedByod_other_ly  IS NULL OR adobe_ordersUnassistedByod_other_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_other - adobe_ordersUnassistedByod_other_ly)  / adobe_ordersUnassistedByod_other_ly,  6) END AS adobe_ordersUnassistedByod_other_yoy_pct,

        adobe_ordersAssistedByod_other,
        adobe_ordersAssistedByod_other_wow,
        adobe_ordersAssistedByod_other_ly,
        CASE WHEN adobe_ordersAssistedByod_other_wow IS NULL OR adobe_ordersAssistedByod_other_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_other - adobe_ordersAssistedByod_other_wow) / adobe_ordersAssistedByod_other_wow, 6) END AS adobe_ordersAssistedByod_other_wow_pct,
        CASE WHEN adobe_ordersAssistedByod_other_ly  IS NULL OR adobe_ordersAssistedByod_other_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_other - adobe_ordersAssistedByod_other_ly)  / adobe_ordersAssistedByod_other_ly,  6) END AS adobe_ordersAssistedByod_other_yoy_pct,

        adobe_ordersTotalByod_other,
        adobe_ordersTotalByod_other_wow,
        adobe_ordersTotalByod_other_ly,
        CASE WHEN adobe_ordersTotalByod_other_wow IS NULL OR adobe_ordersTotalByod_other_wow = 0 THEN NULL ELSE ROUND((adobe_ordersTotalByod_other - adobe_ordersTotalByod_other_wow) / adobe_ordersTotalByod_other_wow, 6) END AS adobe_ordersTotalByod_other_wow_pct,
        CASE WHEN adobe_ordersTotalByod_other_ly  IS NULL OR adobe_ordersTotalByod_other_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersTotalByod_other - adobe_ordersTotalByod_other_ly)  / adobe_ordersTotalByod_other_ly,  6) END AS adobe_ordersTotalByod_other_yoy_pct

    FROM with_comparisons
),

-- -----------------------------------------------------------------------
-- STEP 6: max_data_date per source
-- -----------------------------------------------------------------------
with_max_date AS (
    SELECT
        *,
        MAX(CASE
            WHEN adobe_uvnbByod_allChannels IS NOT NULL
              OR adobe_ordersTotalByod_allChannels IS NOT NULL
            THEN week_sun_to_sat
        END) OVER ()                                                    AS max_data_date
    FROM with_pcts
)

-- -----------------------------------------------------------------------
-- FINAL OUTPUT
-- Wide table — one row per week_sun_to_sat
-- All columns prefixed with 'adobe_'
-- data_source as static column
-- -----------------------------------------------------------------------
SELECT
    week_sun_to_sat,
    'ADOBE'                                                             AS data_source,
    max_data_date,

    -- ================================================================
    -- ALL CHANNELS
    -- ================================================================
    adobe_uvnbByod_allChannels,
    adobe_uvnbByod_allChannels_wow,
    adobe_uvnbByod_allChannels_ly,
    adobe_uvnbByod_allChannels_wow_pct,
    adobe_uvnbByod_allChannels_yoy_pct,

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

    adobe_cartStartByod_allChannels,
    adobe_cartStartByod_allChannels_wow,
    adobe_cartStartByod_allChannels_ly,
    adobe_cartStartByod_allChannels_wow_pct,
    adobe_cartStartByod_allChannels_yoy_pct,

    adobe_ordersUnassistedByod_allChannels,
    adobe_ordersUnassistedByod_allChannels_wow,
    adobe_ordersUnassistedByod_allChannels_ly,
    adobe_ordersUnassistedByod_allChannels_wow_pct,
    adobe_ordersUnassistedByod_allChannels_yoy_pct,

    adobe_ordersAssistedByod_allChannels,
    adobe_ordersAssistedByod_allChannels_wow,
    adobe_ordersAssistedByod_allChannels_ly,
    adobe_ordersAssistedByod_allChannels_wow_pct,
    adobe_ordersAssistedByod_allChannels_yoy_pct,

    adobe_ordersTotalByod_allChannels,
    adobe_ordersTotalByod_allChannels_wow,
    adobe_ordersTotalByod_allChannels_ly,
    adobe_ordersTotalByod_allChannels_wow_pct,
    adobe_ordersTotalByod_allChannels_yoy_pct,

    -- ================================================================
    -- PAID SEARCH
    -- ================================================================
    adobe_uvnbByod_paidSearch,
    adobe_uvnbByod_paidSearch_wow,
    adobe_uvnbByod_paidSearch_ly,
    adobe_uvnbByod_paidSearch_wow_pct,
    adobe_uvnbByod_paidSearch_yoy_pct,

    adobe_cartStartByod_paidSearch,
    adobe_cartStartByod_paidSearch_wow,
    adobe_cartStartByod_paidSearch_ly,
    adobe_cartStartByod_paidSearch_wow_pct,
    adobe_cartStartByod_paidSearch_yoy_pct,

    adobe_ordersUnassistedByod_paidSearch,
    adobe_ordersUnassistedByod_paidSearch_wow,
    adobe_ordersUnassistedByod_paidSearch_ly,
    adobe_ordersUnassistedByod_paidSearch_wow_pct,
    adobe_ordersUnassistedByod_paidSearch_yoy_pct,

    adobe_ordersAssistedByod_paidSearch,
    adobe_ordersAssistedByod_paidSearch_wow,
    adobe_ordersAssistedByod_paidSearch_ly,
    adobe_ordersAssistedByod_paidSearch_wow_pct,
    adobe_ordersAssistedByod_paidSearch_yoy_pct,

    adobe_ordersTotalByod_paidSearch,
    adobe_ordersTotalByod_paidSearch_wow,
    adobe_ordersTotalByod_paidSearch_ly,
    adobe_ordersTotalByod_paidSearch_wow_pct,
    adobe_ordersTotalByod_paidSearch_yoy_pct,

    -- ================================================================
    -- ORGANIC SEARCH
    -- ================================================================
    adobe_uvnbByod_organicSearch,
    adobe_uvnbByod_organicSearch_wow,
    adobe_uvnbByod_organicSearch_ly,
    adobe_uvnbByod_organicSearch_wow_pct,
    adobe_uvnbByod_organicSearch_yoy_pct,

    adobe_cartStartByod_organicSearch,
    adobe_cartStartByod_organicSearch_wow,
    adobe_cartStartByod_organicSearch_ly,
    adobe_cartStartByod_organicSearch_wow_pct,
    adobe_cartStartByod_organicSearch_yoy_pct,

    adobe_ordersUnassistedByod_organicSearch,
    adobe_ordersUnassistedByod_organicSearch_wow,
    adobe_ordersUnassistedByod_organicSearch_ly,
    adobe_ordersUnassistedByod_organicSearch_wow_pct,
    adobe_ordersUnassistedByod_organicSearch_yoy_pct,

    adobe_ordersAssistedByod_organicSearch,
    adobe_ordersAssistedByod_organicSearch_wow,
    adobe_ordersAssistedByod_organicSearch_ly,
    adobe_ordersAssistedByod_organicSearch_wow_pct,
    adobe_ordersAssistedByod_organicSearch_yoy_pct,

    adobe_ordersTotalByod_organicSearch,
    adobe_ordersTotalByod_organicSearch_wow,
    adobe_ordersTotalByod_organicSearch_ly,
    adobe_ordersTotalByod_organicSearch_wow_pct,
    adobe_ordersTotalByod_organicSearch_yoy_pct,

    -- ================================================================
    -- DIRECT
    -- ================================================================
    adobe_uvnbByod_direct,
    adobe_uvnbByod_direct_wow,
    adobe_uvnbByod_direct_ly,
    adobe_uvnbByod_direct_wow_pct,
    adobe_uvnbByod_direct_yoy_pct,

    adobe_cartStartByod_direct,
    adobe_cartStartByod_direct_wow,
    adobe_cartStartByod_direct_ly,
    adobe_cartStartByod_direct_wow_pct,
    adobe_cartStartByod_direct_yoy_pct,

    adobe_ordersUnassistedByod_direct,
    adobe_ordersUnassistedByod_direct_wow,
    adobe_ordersUnassistedByod_direct_ly,
    adobe_ordersUnassistedByod_direct_wow_pct,
    adobe_ordersUnassistedByod_direct_yoy_pct,

    adobe_ordersAssistedByod_direct,
    adobe_ordersAssistedByod_direct_wow,
    adobe_ordersAssistedByod_direct_ly,
    adobe_ordersAssistedByod_direct_wow_pct,
    adobe_ordersAssistedByod_direct_yoy_pct,

    adobe_ordersTotalByod_direct,
    adobe_ordersTotalByod_direct_wow,
    adobe_ordersTotalByod_direct_ly,
    adobe_ordersTotalByod_direct_wow_pct,
    adobe_ordersTotalByod_direct_yoy_pct,

    -- ================================================================
    -- SOCIAL
    -- ================================================================
    adobe_uvnbByod_social,
    adobe_uvnbByod_social_wow,
    adobe_uvnbByod_social_ly,
    adobe_uvnbByod_social_wow_pct,
    adobe_uvnbByod_social_yoy_pct,

    adobe_cartStartByod_social,
    adobe_cartStartByod_social_wow,
    adobe_cartStartByod_social_ly,
    adobe_cartStartByod_social_wow_pct,
    adobe_cartStartByod_social_yoy_pct,

    adobe_ordersUnassistedByod_social,
    adobe_ordersUnassistedByod_social_wow,
    adobe_ordersUnassistedByod_social_ly,
    adobe_ordersUnassistedByod_social_wow_pct,
    adobe_ordersUnassistedByod_social_yoy_pct,

    adobe_ordersAssistedByod_social,
    adobe_ordersAssistedByod_social_wow,
    adobe_ordersAssistedByod_social_ly,
    adobe_ordersAssistedByod_social_wow_pct,
    adobe_ordersAssistedByod_social_yoy_pct,

    adobe_ordersTotalByod_social,
    adobe_ordersTotalByod_social_wow,
    adobe_ordersTotalByod_social_ly,
    adobe_ordersTotalByod_social_wow_pct,
    adobe_ordersTotalByod_social_yoy_pct,

    -- ================================================================
    -- PROGRAMMATIC
    -- ================================================================
    adobe_uvnbByod_programmatic,
    adobe_uvnbByod_programmatic_wow,
    adobe_uvnbByod_programmatic_ly,
    adobe_uvnbByod_programmatic_wow_pct,
    adobe_uvnbByod_programmatic_yoy_pct,

    adobe_cartStartByod_programmatic,
    adobe_cartStartByod_programmatic_wow,
    adobe_cartStartByod_programmatic_ly,
    adobe_cartStartByod_programmatic_wow_pct,
    adobe_cartStartByod_programmatic_yoy_pct,

    adobe_ordersUnassistedByod_programmatic,
    adobe_ordersUnassistedByod_programmatic_wow,
    adobe_ordersUnassistedByod_programmatic_ly,
    adobe_ordersUnassistedByod_programmatic_wow_pct,
    adobe_ordersUnassistedByod_programmatic_yoy_pct,

    adobe_ordersAssistedByod_programmatic,
    adobe_ordersAssistedByod_programmatic_wow,
    adobe_ordersAssistedByod_programmatic_ly,
    adobe_ordersAssistedByod_programmatic_wow_pct,
    adobe_ordersAssistedByod_programmatic_yoy_pct,

    adobe_ordersTotalByod_programmatic,
    adobe_ordersTotalByod_programmatic_wow,
    adobe_ordersTotalByod_programmatic_ly,
    adobe_ordersTotalByod_programmatic_wow_pct,
    adobe_ordersTotalByod_programmatic_yoy_pct,

    -- ================================================================
    -- OTHER
    -- ================================================================
    adobe_uvnbByod_other,
    adobe_uvnbByod_other_wow,
    adobe_uvnbByod_other_ly,
    adobe_uvnbByod_other_wow_pct,
    adobe_uvnbByod_other_yoy_pct,

    adobe_cartStartByod_other,
    adobe_cartStartByod_other_wow,
    adobe_cartStartByod_other_ly,
    adobe_cartStartByod_other_wow_pct,
    adobe_cartStartByod_other_yoy_pct,

    adobe_ordersUnassistedByod_other,
    adobe_ordersUnassistedByod_other_wow,
    adobe_ordersUnassistedByod_other_ly,
    adobe_ordersUnassistedByod_other_wow_pct,
    adobe_ordersUnassistedByod_other_yoy_pct,

    adobe_ordersAssistedByod_other,
    adobe_ordersAssistedByod_other_wow,
    adobe_ordersAssistedByod_other_ly,
    adobe_ordersAssistedByod_other_wow_pct,
    adobe_ordersAssistedByod_other_yoy_pct,

    adobe_ordersTotalByod_other,
    adobe_ordersTotalByod_other_wow,
    adobe_ordersTotalByod_other_ly,
    adobe_ordersTotalByod_other_wow_pct,
    adobe_ordersTotalByod_other_yoy_pct

FROM with_max_date
;