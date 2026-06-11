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

COLUMN NAMING CONVENTION:
  adobe_{metric}_{channel}
  Pct/CVR metrics prefixed with type:
    adobe_pct{subject}Of{denominator}_{channel}
    adobe_cvr{scope}_{channel}

  Raw metrics:
    uvnbByod               (all channels)
    uvnbTotal              (allChannels only)
    uvnbFlowTotal          (allChannels only)
    cartStartByod          (all channels)
    ordersUnassistedByod   (all channels)
    ordersAssistedByod     (all channels)
    ordersTotalByod        (all channels)
    ordersTotal            (allChannels only)

  Derived metrics (allChannels only):
    pctUvnbByodOfUvnbFlow          — uvnbByod / uvnbFlowTotal
    pctOrdersByodOfOrdersTotal     — ordersTotalByod / ordersTotal
    cvrByod                        — ordersTotalByod / uvnbByod
    cvrSite                        — ordersTotal / uvnbTotal
    cvrPostpaid                    — (OrdersUnassistedPostpaid + OrdersAssistedPostpaid) / UvnbPostpaid
    cvrHsi                         — (OrdersUnassistedHsi + OrdersAssistedHsi) / UvnbHsi

  Derived metrics (per non-ALL channel):
    pctUvnbByodOfTotal             — channel share of BYOD UVNB
    Denominator = SUM of the 6 individual channel UVNBs (not allChannels).

BUSINESS LOGIC:
  - ordersTotalByod = OrdersUnassistedByod + OrdersAssistedByod
  - cvrPostpaid and cvrHsi are allChannels only — computed from internal
    aggregates in pivoted (_int_ prefix) that do not appear in final output
  - All pct/cvr metrics: NULL if denominator is NULL or 0
  - WoW: self-join on week_sun_to_sat - 7 days (gap-safe)
  - LY:  self-join on custom_week_num - 52
  - wow_pct / yoy_pct as decimals — NULL when prior NULL or 0
  - max_data_date: latest week_sun_to_sat with any non-null metric

CUSTOM WEEK NUMBER:
  custom_week_num = DATE_DIFF(DATE_SUB(week_sun_to_sat, INTERVAL 6 DAY), DATE '2023-01-01', WEEK)

DOWNSTREAM:
  Gold Wide : vw_sdi_pulseByod_gold_unified_wide
  Gold Long : vw_sdi_pulseByod_gold_unified_long
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_adobe_weekly`
AS

-- -----------------------------------------------------------------------
-- STEP 1: Extract metrics per channel row
-- -----------------------------------------------------------------------
WITH base AS (
    SELECT
        WeekSunSat                                                      AS week_sun_to_sat,
        ChannelGroup,
        UvnbByod,
        UvnbTotalAdobe,
        UvnbFlowTotal,
        CartstartByod,
        OrdersUnassistedByod,
        OrdersAssistedByod,
        OrdersUnassistedByod + OrdersAssistedByod                      AS ordersTotalByod,
        OrdersTotal,
        -- Postpaid and HSI — used to compute cvrPostpaid / cvrHsi (allChannels only)
        UvnbPostpaid,
        UvnbHsi,
        OrdersUnassistedPostpaid + OrdersAssistedPostpaid              AS ordersTotalPostpaid,
        OrdersUnassistedHsi      + OrdersAssistedHsi                   AS ordersTotalHsi
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_flowPerformanceByChannelGroupsPlusAll_Weekly`
),

-- -----------------------------------------------------------------------
-- STEP 2: Pivot long → wide (one row per week)
-- _int_ columns are internal-only — used by with_channel_mix to compute
-- cvrPostpaid and cvrHsi, then excluded from all downstream CTEs.
-- -----------------------------------------------------------------------
pivoted AS (
    SELECT
        week_sun_to_sat,

        -- ---- ALL CHANNELS ----
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN UvnbByod       END)    AS adobe_uvnbByod_allChannels,
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN UvnbTotalAdobe END)    AS adobe_uvnbTotal_allChannels,
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN UvnbFlowTotal  END)    AS adobe_uvnbFlowTotal_allChannels,
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN CartstartByod  END)    AS adobe_cartStartByod_allChannels,
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN OrdersUnassistedByod END) AS adobe_ordersUnassistedByod_allChannels,
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN OrdersAssistedByod   END) AS adobe_ordersAssistedByod_allChannels,
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN ordersTotalByod      END) AS adobe_ordersTotalByod_allChannels,
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN OrdersTotal          END) AS adobe_ordersTotal_allChannels,

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
        MAX(CASE WHEN ChannelGroup = 'OTHER' THEN ordersTotalByod      END) AS adobe_ordersTotalByod_other,

        -- ---- CHANNEL SUM — denominator for pctUvnbByodOfTotal ----
        COALESCE(MAX(CASE WHEN ChannelGroup = 'PAID SEARCH'    THEN UvnbByod END), 0)
      + COALESCE(MAX(CASE WHEN ChannelGroup = 'ORGANIC SEARCH' THEN UvnbByod END), 0)
      + COALESCE(MAX(CASE WHEN ChannelGroup = 'DIRECT'         THEN UvnbByod END), 0)
      + COALESCE(MAX(CASE WHEN ChannelGroup = 'SOCIAL'         THEN UvnbByod END), 0)
      + COALESCE(MAX(CASE WHEN ChannelGroup = 'PROGRAMMATIC'   THEN UvnbByod END), 0)
      + COALESCE(MAX(CASE WHEN ChannelGroup = 'OTHER'          THEN UvnbByod END), 0)
                                                                        AS uvnb_byod_channel_sum,

        -- ---- INTERNAL: Postpaid + HSI for CVR computation only ----
        -- These columns do not appear in the final SELECT output.
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN UvnbPostpaid       END) AS _int_uvnb_postpaid,
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN ordersTotalPostpaid END) AS _int_orders_postpaid,
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN UvnbHsi            END) AS _int_uvnb_hsi,
        MAX(CASE WHEN ChannelGroup = 'ALL' THEN ordersTotalHsi     END) AS _int_orders_hsi

    FROM base
    GROUP BY week_sun_to_sat
),

-- -----------------------------------------------------------------------
-- STEP 3: Post-pivot derived metrics
-- -----------------------------------------------------------------------
with_channel_mix AS (
    SELECT
        *,

        -- ---- allChannels BYOD/site derived ----
        CASE WHEN adobe_uvnbFlowTotal_allChannels IS NULL OR adobe_uvnbFlowTotal_allChannels = 0 THEN NULL
             ELSE ROUND(adobe_uvnbByod_allChannels / adobe_uvnbFlowTotal_allChannels, 6)
        END                                                             AS adobe_pctUvnbByodOfUvnbFlow_allChannels,

        CASE WHEN adobe_ordersTotal_allChannels IS NULL OR adobe_ordersTotal_allChannels = 0 THEN NULL
             ELSE ROUND(adobe_ordersTotalByod_allChannels / adobe_ordersTotal_allChannels, 6)
        END                                                             AS adobe_pctOrdersByodOfOrdersTotal_allChannels,

        CASE WHEN adobe_uvnbByod_allChannels IS NULL OR adobe_uvnbByod_allChannels = 0 THEN NULL
             ELSE ROUND(adobe_ordersTotalByod_allChannels / adobe_uvnbByod_allChannels, 6)
        END                                                             AS adobe_cvrByod_allChannels,

        CASE WHEN adobe_uvnbTotal_allChannels IS NULL OR adobe_uvnbTotal_allChannels = 0 THEN NULL
             ELSE ROUND(adobe_ordersTotal_allChannels / adobe_uvnbTotal_allChannels, 6)
        END                                                             AS adobe_cvrSite_allChannels,

        -- ---- allChannels Postpaid + HSI CVR ----
        CASE WHEN _int_uvnb_postpaid IS NULL OR _int_uvnb_postpaid = 0 THEN NULL
             ELSE ROUND(_int_orders_postpaid / _int_uvnb_postpaid, 6)
        END                                                             AS adobe_cvrPostpaid_allChannels,

        CASE WHEN _int_uvnb_hsi IS NULL OR _int_uvnb_hsi = 0 THEN NULL
             ELSE ROUND(_int_orders_hsi / _int_uvnb_hsi, 6)
        END                                                             AS adobe_cvrHsi_allChannels,

        -- ---- UVNB BYOD channel mix ----
        CASE WHEN uvnb_byod_channel_sum = 0 THEN NULL
             ELSE ROUND(adobe_uvnbByod_paidSearch    / uvnb_byod_channel_sum, 6)
        END                                                             AS adobe_pctUvnbByodOfTotal_paidSearch,

        CASE WHEN uvnb_byod_channel_sum = 0 THEN NULL
             ELSE ROUND(adobe_uvnbByod_organicSearch / uvnb_byod_channel_sum, 6)
        END                                                             AS adobe_pctUvnbByodOfTotal_organicSearch,

        CASE WHEN uvnb_byod_channel_sum = 0 THEN NULL
             ELSE ROUND(adobe_uvnbByod_direct        / uvnb_byod_channel_sum, 6)
        END                                                             AS adobe_pctUvnbByodOfTotal_direct,

        CASE WHEN uvnb_byod_channel_sum = 0 THEN NULL
             ELSE ROUND(adobe_uvnbByod_social        / uvnb_byod_channel_sum, 6)
        END                                                             AS adobe_pctUvnbByodOfTotal_social,

        CASE WHEN uvnb_byod_channel_sum = 0 THEN NULL
             ELSE ROUND(adobe_uvnbByod_programmatic  / uvnb_byod_channel_sum, 6)
        END                                                             AS adobe_pctUvnbByodOfTotal_programmatic,

        CASE WHEN uvnb_byod_channel_sum = 0 THEN NULL
             ELSE ROUND(adobe_uvnbByod_other         / uvnb_byod_channel_sum, 6)
        END                                                             AS adobe_pctUvnbByodOfTotal_other

    FROM pivoted
),

-- -----------------------------------------------------------------------
-- STEP 4: Custom week number
-- -----------------------------------------------------------------------
with_week_num AS (
    SELECT
        *,
        DATE_DIFF(DATE_SUB(week_sun_to_sat, INTERVAL 6 DAY), DATE '2023-01-01', WEEK) AS custom_week_num
    FROM with_channel_mix
),

-- -----------------------------------------------------------------------
-- STEP 5: WoW and LY self-joins
-- -----------------------------------------------------------------------
with_comparisons AS (
    SELECT
        c.week_sun_to_sat,
        c.custom_week_num,

        -- ================================================================ ALL CHANNELS current
        c.adobe_uvnbByod_allChannels,
        c.adobe_uvnbTotal_allChannels,
        c.adobe_uvnbFlowTotal_allChannels,
        c.adobe_pctUvnbByodOfUvnbFlow_allChannels,
        c.adobe_cartStartByod_allChannels,
        c.adobe_ordersUnassistedByod_allChannels,
        c.adobe_ordersAssistedByod_allChannels,
        c.adobe_ordersTotalByod_allChannels,
        c.adobe_ordersTotal_allChannels,
        c.adobe_pctOrdersByodOfOrdersTotal_allChannels,
        c.adobe_cvrByod_allChannels,
        c.adobe_cvrSite_allChannels,
        c.adobe_cvrPostpaid_allChannels,
        c.adobe_cvrHsi_allChannels,
        -- ALL CHANNELS WoW
        w.adobe_uvnbByod_allChannels                   AS adobe_uvnbByod_allChannels_wow,
        w.adobe_uvnbTotal_allChannels                  AS adobe_uvnbTotal_allChannels_wow,
        w.adobe_uvnbFlowTotal_allChannels              AS adobe_uvnbFlowTotal_allChannels_wow,
        w.adobe_pctUvnbByodOfUvnbFlow_allChannels      AS adobe_pctUvnbByodOfUvnbFlow_allChannels_wow,
        w.adobe_cartStartByod_allChannels              AS adobe_cartStartByod_allChannels_wow,
        w.adobe_ordersUnassistedByod_allChannels       AS adobe_ordersUnassistedByod_allChannels_wow,
        w.adobe_ordersAssistedByod_allChannels         AS adobe_ordersAssistedByod_allChannels_wow,
        w.adobe_ordersTotalByod_allChannels            AS adobe_ordersTotalByod_allChannels_wow,
        w.adobe_ordersTotal_allChannels                AS adobe_ordersTotal_allChannels_wow,
        w.adobe_pctOrdersByodOfOrdersTotal_allChannels AS adobe_pctOrdersByodOfOrdersTotal_allChannels_wow,
        w.adobe_cvrByod_allChannels                    AS adobe_cvrByod_allChannels_wow,
        w.adobe_cvrSite_allChannels                    AS adobe_cvrSite_allChannels_wow,
        w.adobe_cvrPostpaid_allChannels                AS adobe_cvrPostpaid_allChannels_wow,
        w.adobe_cvrHsi_allChannels                     AS adobe_cvrHsi_allChannels_wow,
        -- ALL CHANNELS LY
        l.adobe_uvnbByod_allChannels                   AS adobe_uvnbByod_allChannels_ly,
        l.adobe_uvnbTotal_allChannels                  AS adobe_uvnbTotal_allChannels_ly,
        l.adobe_uvnbFlowTotal_allChannels              AS adobe_uvnbFlowTotal_allChannels_ly,
        l.adobe_pctUvnbByodOfUvnbFlow_allChannels      AS adobe_pctUvnbByodOfUvnbFlow_allChannels_ly,
        l.adobe_cartStartByod_allChannels              AS adobe_cartStartByod_allChannels_ly,
        l.adobe_ordersUnassistedByod_allChannels       AS adobe_ordersUnassistedByod_allChannels_ly,
        l.adobe_ordersAssistedByod_allChannels         AS adobe_ordersAssistedByod_allChannels_ly,
        l.adobe_ordersTotalByod_allChannels            AS adobe_ordersTotalByod_allChannels_ly,
        l.adobe_ordersTotal_allChannels                AS adobe_ordersTotal_allChannels_ly,
        l.adobe_pctOrdersByodOfOrdersTotal_allChannels AS adobe_pctOrdersByodOfOrdersTotal_allChannels_ly,
        l.adobe_cvrByod_allChannels                    AS adobe_cvrByod_allChannels_ly,
        l.adobe_cvrSite_allChannels                    AS adobe_cvrSite_allChannels_ly,
        l.adobe_cvrPostpaid_allChannels                AS adobe_cvrPostpaid_allChannels_ly,
        l.adobe_cvrHsi_allChannels                     AS adobe_cvrHsi_allChannels_ly,

        -- ================================================================ PAID SEARCH current
        c.adobe_uvnbByod_paidSearch,
        c.adobe_pctUvnbByodOfTotal_paidSearch,
        c.adobe_cartStartByod_paidSearch,
        c.adobe_ordersUnassistedByod_paidSearch,
        c.adobe_ordersAssistedByod_paidSearch,
        c.adobe_ordersTotalByod_paidSearch,
        w.adobe_uvnbByod_paidSearch                    AS adobe_uvnbByod_paidSearch_wow,
        w.adobe_pctUvnbByodOfTotal_paidSearch          AS adobe_pctUvnbByodOfTotal_paidSearch_wow,
        w.adobe_cartStartByod_paidSearch               AS adobe_cartStartByod_paidSearch_wow,
        w.adobe_ordersUnassistedByod_paidSearch        AS adobe_ordersUnassistedByod_paidSearch_wow,
        w.adobe_ordersAssistedByod_paidSearch          AS adobe_ordersAssistedByod_paidSearch_wow,
        w.adobe_ordersTotalByod_paidSearch             AS adobe_ordersTotalByod_paidSearch_wow,
        l.adobe_uvnbByod_paidSearch                    AS adobe_uvnbByod_paidSearch_ly,
        l.adobe_pctUvnbByodOfTotal_paidSearch          AS adobe_pctUvnbByodOfTotal_paidSearch_ly,
        l.adobe_cartStartByod_paidSearch               AS adobe_cartStartByod_paidSearch_ly,
        l.adobe_ordersUnassistedByod_paidSearch        AS adobe_ordersUnassistedByod_paidSearch_ly,
        l.adobe_ordersAssistedByod_paidSearch          AS adobe_ordersAssistedByod_paidSearch_ly,
        l.adobe_ordersTotalByod_paidSearch             AS adobe_ordersTotalByod_paidSearch_ly,

        -- ================================================================ ORGANIC SEARCH current
        c.adobe_uvnbByod_organicSearch,
        c.adobe_pctUvnbByodOfTotal_organicSearch,
        c.adobe_cartStartByod_organicSearch,
        c.adobe_ordersUnassistedByod_organicSearch,
        c.adobe_ordersAssistedByod_organicSearch,
        c.adobe_ordersTotalByod_organicSearch,
        w.adobe_uvnbByod_organicSearch                 AS adobe_uvnbByod_organicSearch_wow,
        w.adobe_pctUvnbByodOfTotal_organicSearch       AS adobe_pctUvnbByodOfTotal_organicSearch_wow,
        w.adobe_cartStartByod_organicSearch            AS adobe_cartStartByod_organicSearch_wow,
        w.adobe_ordersUnassistedByod_organicSearch     AS adobe_ordersUnassistedByod_organicSearch_wow,
        w.adobe_ordersAssistedByod_organicSearch       AS adobe_ordersAssistedByod_organicSearch_wow,
        w.adobe_ordersTotalByod_organicSearch          AS adobe_ordersTotalByod_organicSearch_wow,
        l.adobe_uvnbByod_organicSearch                 AS adobe_uvnbByod_organicSearch_ly,
        l.adobe_pctUvnbByodOfTotal_organicSearch       AS adobe_pctUvnbByodOfTotal_organicSearch_ly,
        l.adobe_cartStartByod_organicSearch            AS adobe_cartStartByod_organicSearch_ly,
        l.adobe_ordersUnassistedByod_organicSearch     AS adobe_ordersUnassistedByod_organicSearch_ly,
        l.adobe_ordersAssistedByod_organicSearch       AS adobe_ordersAssistedByod_organicSearch_ly,
        l.adobe_ordersTotalByod_organicSearch          AS adobe_ordersTotalByod_organicSearch_ly,

        -- ================================================================ DIRECT current
        c.adobe_uvnbByod_direct,
        c.adobe_pctUvnbByodOfTotal_direct,
        c.adobe_cartStartByod_direct,
        c.adobe_ordersUnassistedByod_direct,
        c.adobe_ordersAssistedByod_direct,
        c.adobe_ordersTotalByod_direct,
        w.adobe_uvnbByod_direct                        AS adobe_uvnbByod_direct_wow,
        w.adobe_pctUvnbByodOfTotal_direct              AS adobe_pctUvnbByodOfTotal_direct_wow,
        w.adobe_cartStartByod_direct                   AS adobe_cartStartByod_direct_wow,
        w.adobe_ordersUnassistedByod_direct            AS adobe_ordersUnassistedByod_direct_wow,
        w.adobe_ordersAssistedByod_direct              AS adobe_ordersAssistedByod_direct_wow,
        w.adobe_ordersTotalByod_direct                 AS adobe_ordersTotalByod_direct_wow,
        l.adobe_uvnbByod_direct                        AS adobe_uvnbByod_direct_ly,
        l.adobe_pctUvnbByodOfTotal_direct              AS adobe_pctUvnbByodOfTotal_direct_ly,
        l.adobe_cartStartByod_direct                   AS adobe_cartStartByod_direct_ly,
        l.adobe_ordersUnassistedByod_direct            AS adobe_ordersUnassistedByod_direct_ly,
        l.adobe_ordersAssistedByod_direct              AS adobe_ordersAssistedByod_direct_ly,
        l.adobe_ordersTotalByod_direct                 AS adobe_ordersTotalByod_direct_ly,

        -- ================================================================ SOCIAL current
        c.adobe_uvnbByod_social,
        c.adobe_pctUvnbByodOfTotal_social,
        c.adobe_cartStartByod_social,
        c.adobe_ordersUnassistedByod_social,
        c.adobe_ordersAssistedByod_social,
        c.adobe_ordersTotalByod_social,
        w.adobe_uvnbByod_social                        AS adobe_uvnbByod_social_wow,
        w.adobe_pctUvnbByodOfTotal_social              AS adobe_pctUvnbByodOfTotal_social_wow,
        w.adobe_cartStartByod_social                   AS adobe_cartStartByod_social_wow,
        w.adobe_ordersUnassistedByod_social            AS adobe_ordersUnassistedByod_social_wow,
        w.adobe_ordersAssistedByod_social              AS adobe_ordersAssistedByod_social_wow,
        w.adobe_ordersTotalByod_social                 AS adobe_ordersTotalByod_social_wow,
        l.adobe_uvnbByod_social                        AS adobe_uvnbByod_social_ly,
        l.adobe_pctUvnbByodOfTotal_social              AS adobe_pctUvnbByodOfTotal_social_ly,
        l.adobe_cartStartByod_social                   AS adobe_cartStartByod_social_ly,
        l.adobe_ordersUnassistedByod_social            AS adobe_ordersUnassistedByod_social_ly,
        l.adobe_ordersAssistedByod_social              AS adobe_ordersAssistedByod_social_ly,
        l.adobe_ordersTotalByod_social                 AS adobe_ordersTotalByod_social_ly,

        -- ================================================================ PROGRAMMATIC current
        c.adobe_uvnbByod_programmatic,
        c.adobe_pctUvnbByodOfTotal_programmatic,
        c.adobe_cartStartByod_programmatic,
        c.adobe_ordersUnassistedByod_programmatic,
        c.adobe_ordersAssistedByod_programmatic,
        c.adobe_ordersTotalByod_programmatic,
        w.adobe_uvnbByod_programmatic                  AS adobe_uvnbByod_programmatic_wow,
        w.adobe_pctUvnbByodOfTotal_programmatic        AS adobe_pctUvnbByodOfTotal_programmatic_wow,
        w.adobe_cartStartByod_programmatic             AS adobe_cartStartByod_programmatic_wow,
        w.adobe_ordersUnassistedByod_programmatic      AS adobe_ordersUnassistedByod_programmatic_wow,
        w.adobe_ordersAssistedByod_programmatic        AS adobe_ordersAssistedByod_programmatic_wow,
        w.adobe_ordersTotalByod_programmatic           AS adobe_ordersTotalByod_programmatic_wow,
        l.adobe_uvnbByod_programmatic                  AS adobe_uvnbByod_programmatic_ly,
        l.adobe_pctUvnbByodOfTotal_programmatic        AS adobe_pctUvnbByodOfTotal_programmatic_ly,
        l.adobe_cartStartByod_programmatic             AS adobe_cartStartByod_programmatic_ly,
        l.adobe_ordersUnassistedByod_programmatic      AS adobe_ordersUnassistedByod_programmatic_ly,
        l.adobe_ordersAssistedByod_programmatic        AS adobe_ordersAssistedByod_programmatic_ly,
        l.adobe_ordersTotalByod_programmatic           AS adobe_ordersTotalByod_programmatic_ly,

        -- ================================================================ OTHER current
        c.adobe_uvnbByod_other,
        c.adobe_pctUvnbByodOfTotal_other,
        c.adobe_cartStartByod_other,
        c.adobe_ordersUnassistedByod_other,
        c.adobe_ordersAssistedByod_other,
        c.adobe_ordersTotalByod_other,
        w.adobe_uvnbByod_other                         AS adobe_uvnbByod_other_wow,
        w.adobe_pctUvnbByodOfTotal_other               AS adobe_pctUvnbByodOfTotal_other_wow,
        w.adobe_cartStartByod_other                    AS adobe_cartStartByod_other_wow,
        w.adobe_ordersUnassistedByod_other             AS adobe_ordersUnassistedByod_other_wow,
        w.adobe_ordersAssistedByod_other               AS adobe_ordersAssistedByod_other_wow,
        w.adobe_ordersTotalByod_other                  AS adobe_ordersTotalByod_other_wow,
        l.adobe_uvnbByod_other                         AS adobe_uvnbByod_other_ly,
        l.adobe_pctUvnbByodOfTotal_other               AS adobe_pctUvnbByodOfTotal_other_ly,
        l.adobe_cartStartByod_other                    AS adobe_cartStartByod_other_ly,
        l.adobe_ordersUnassistedByod_other             AS adobe_ordersUnassistedByod_other_ly,
        l.adobe_ordersAssistedByod_other               AS adobe_ordersAssistedByod_other_ly,
        l.adobe_ordersTotalByod_other                  AS adobe_ordersTotalByod_other_ly

    FROM with_week_num c
    LEFT JOIN with_week_num w ON c.week_sun_to_sat = DATE_ADD(w.week_sun_to_sat, INTERVAL 7 DAY)
    LEFT JOIN with_week_num l ON (c.custom_week_num - l.custom_week_num) = 52
),

-- -----------------------------------------------------------------------
-- STEP 6: wow_pct and yoy_pct
-- -----------------------------------------------------------------------
with_pcts AS (
    SELECT
        week_sun_to_sat,
        custom_week_num,

        -- ================================================================ ALL CHANNELS
        adobe_uvnbByod_allChannels, adobe_uvnbByod_allChannels_wow, adobe_uvnbByod_allChannels_ly,
        CASE WHEN adobe_uvnbByod_allChannels_wow IS NULL OR adobe_uvnbByod_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_allChannels - adobe_uvnbByod_allChannels_wow) / adobe_uvnbByod_allChannels_wow, 6) END AS adobe_uvnbByod_allChannels_wow_pct,
        CASE WHEN adobe_uvnbByod_allChannels_ly  IS NULL OR adobe_uvnbByod_allChannels_ly  = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_allChannels - adobe_uvnbByod_allChannels_ly)  / adobe_uvnbByod_allChannels_ly,  6) END AS adobe_uvnbByod_allChannels_yoy_pct,

        adobe_uvnbTotal_allChannels, adobe_uvnbTotal_allChannels_wow, adobe_uvnbTotal_allChannels_ly,
        CASE WHEN adobe_uvnbTotal_allChannels_wow IS NULL OR adobe_uvnbTotal_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbTotal_allChannels - adobe_uvnbTotal_allChannels_wow) / adobe_uvnbTotal_allChannels_wow, 6) END AS adobe_uvnbTotal_allChannels_wow_pct,
        CASE WHEN adobe_uvnbTotal_allChannels_ly  IS NULL OR adobe_uvnbTotal_allChannels_ly  = 0 THEN NULL ELSE ROUND((adobe_uvnbTotal_allChannels - adobe_uvnbTotal_allChannels_ly)  / adobe_uvnbTotal_allChannels_ly,  6) END AS adobe_uvnbTotal_allChannels_yoy_pct,

        adobe_uvnbFlowTotal_allChannels, adobe_uvnbFlowTotal_allChannels_wow, adobe_uvnbFlowTotal_allChannels_ly,
        CASE WHEN adobe_uvnbFlowTotal_allChannels_wow IS NULL OR adobe_uvnbFlowTotal_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbFlowTotal_allChannels - adobe_uvnbFlowTotal_allChannels_wow) / adobe_uvnbFlowTotal_allChannels_wow, 6) END AS adobe_uvnbFlowTotal_allChannels_wow_pct,
        CASE WHEN adobe_uvnbFlowTotal_allChannels_ly  IS NULL OR adobe_uvnbFlowTotal_allChannels_ly  = 0 THEN NULL ELSE ROUND((adobe_uvnbFlowTotal_allChannels - adobe_uvnbFlowTotal_allChannels_ly)  / adobe_uvnbFlowTotal_allChannels_ly,  6) END AS adobe_uvnbFlowTotal_allChannels_yoy_pct,

        adobe_pctUvnbByodOfUvnbFlow_allChannels, adobe_pctUvnbByodOfUvnbFlow_allChannels_wow, adobe_pctUvnbByodOfUvnbFlow_allChannels_ly,
        CASE WHEN adobe_pctUvnbByodOfUvnbFlow_allChannels_wow IS NULL OR adobe_pctUvnbByodOfUvnbFlow_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_pctUvnbByodOfUvnbFlow_allChannels - adobe_pctUvnbByodOfUvnbFlow_allChannels_wow) / adobe_pctUvnbByodOfUvnbFlow_allChannels_wow, 6) END AS adobe_pctUvnbByodOfUvnbFlow_allChannels_wow_pct,
        CASE WHEN adobe_pctUvnbByodOfUvnbFlow_allChannels_ly  IS NULL OR adobe_pctUvnbByodOfUvnbFlow_allChannels_ly  = 0 THEN NULL ELSE ROUND((adobe_pctUvnbByodOfUvnbFlow_allChannels - adobe_pctUvnbByodOfUvnbFlow_allChannels_ly)  / adobe_pctUvnbByodOfUvnbFlow_allChannels_ly,  6) END AS adobe_pctUvnbByodOfUvnbFlow_allChannels_yoy_pct,

        adobe_cartStartByod_allChannels, adobe_cartStartByod_allChannels_wow, adobe_cartStartByod_allChannels_ly,
        CASE WHEN adobe_cartStartByod_allChannels_wow IS NULL OR adobe_cartStartByod_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_allChannels - adobe_cartStartByod_allChannels_wow) / adobe_cartStartByod_allChannels_wow, 6) END AS adobe_cartStartByod_allChannels_wow_pct,
        CASE WHEN adobe_cartStartByod_allChannels_ly  IS NULL OR adobe_cartStartByod_allChannels_ly  = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_allChannels - adobe_cartStartByod_allChannels_ly)  / adobe_cartStartByod_allChannels_ly,  6) END AS adobe_cartStartByod_allChannels_yoy_pct,

        adobe_ordersUnassistedByod_allChannels, adobe_ordersUnassistedByod_allChannels_wow, adobe_ordersUnassistedByod_allChannels_ly,
        CASE WHEN adobe_ordersUnassistedByod_allChannels_wow IS NULL OR adobe_ordersUnassistedByod_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_allChannels - adobe_ordersUnassistedByod_allChannels_wow) / adobe_ordersUnassistedByod_allChannels_wow, 6) END AS adobe_ordersUnassistedByod_allChannels_wow_pct,
        CASE WHEN adobe_ordersUnassistedByod_allChannels_ly  IS NULL OR adobe_ordersUnassistedByod_allChannels_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_allChannels - adobe_ordersUnassistedByod_allChannels_ly)  / adobe_ordersUnassistedByod_allChannels_ly,  6) END AS adobe_ordersUnassistedByod_allChannels_yoy_pct,

        adobe_ordersAssistedByod_allChannels, adobe_ordersAssistedByod_allChannels_wow, adobe_ordersAssistedByod_allChannels_ly,
        CASE WHEN adobe_ordersAssistedByod_allChannels_wow IS NULL OR adobe_ordersAssistedByod_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_allChannels - adobe_ordersAssistedByod_allChannels_wow) / adobe_ordersAssistedByod_allChannels_wow, 6) END AS adobe_ordersAssistedByod_allChannels_wow_pct,
        CASE WHEN adobe_ordersAssistedByod_allChannels_ly  IS NULL OR adobe_ordersAssistedByod_allChannels_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_allChannels - adobe_ordersAssistedByod_allChannels_ly)  / adobe_ordersAssistedByod_allChannels_ly,  6) END AS adobe_ordersAssistedByod_allChannels_yoy_pct,

        adobe_ordersTotalByod_allChannels, adobe_ordersTotalByod_allChannels_wow, adobe_ordersTotalByod_allChannels_ly,
        CASE WHEN adobe_ordersTotalByod_allChannels_wow IS NULL OR adobe_ordersTotalByod_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_ordersTotalByod_allChannels - adobe_ordersTotalByod_allChannels_wow) / adobe_ordersTotalByod_allChannels_wow, 6) END AS adobe_ordersTotalByod_allChannels_wow_pct,
        CASE WHEN adobe_ordersTotalByod_allChannels_ly  IS NULL OR adobe_ordersTotalByod_allChannels_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersTotalByod_allChannels - adobe_ordersTotalByod_allChannels_ly)  / adobe_ordersTotalByod_allChannels_ly,  6) END AS adobe_ordersTotalByod_allChannels_yoy_pct,

        adobe_ordersTotal_allChannels, adobe_ordersTotal_allChannels_wow, adobe_ordersTotal_allChannels_ly,
        CASE WHEN adobe_ordersTotal_allChannels_wow IS NULL OR adobe_ordersTotal_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_ordersTotal_allChannels - adobe_ordersTotal_allChannels_wow) / adobe_ordersTotal_allChannels_wow, 6) END AS adobe_ordersTotal_allChannels_wow_pct,
        CASE WHEN adobe_ordersTotal_allChannels_ly  IS NULL OR adobe_ordersTotal_allChannels_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersTotal_allChannels - adobe_ordersTotal_allChannels_ly)  / adobe_ordersTotal_allChannels_ly,  6) END AS adobe_ordersTotal_allChannels_yoy_pct,

        adobe_pctOrdersByodOfOrdersTotal_allChannels, adobe_pctOrdersByodOfOrdersTotal_allChannels_wow, adobe_pctOrdersByodOfOrdersTotal_allChannels_ly,
        CASE WHEN adobe_pctOrdersByodOfOrdersTotal_allChannels_wow IS NULL OR adobe_pctOrdersByodOfOrdersTotal_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_pctOrdersByodOfOrdersTotal_allChannels - adobe_pctOrdersByodOfOrdersTotal_allChannels_wow) / adobe_pctOrdersByodOfOrdersTotal_allChannels_wow, 6) END AS adobe_pctOrdersByodOfOrdersTotal_allChannels_wow_pct,
        CASE WHEN adobe_pctOrdersByodOfOrdersTotal_allChannels_ly  IS NULL OR adobe_pctOrdersByodOfOrdersTotal_allChannels_ly  = 0 THEN NULL ELSE ROUND((adobe_pctOrdersByodOfOrdersTotal_allChannels - adobe_pctOrdersByodOfOrdersTotal_allChannels_ly)  / adobe_pctOrdersByodOfOrdersTotal_allChannels_ly,  6) END AS adobe_pctOrdersByodOfOrdersTotal_allChannels_yoy_pct,

        adobe_cvrByod_allChannels, adobe_cvrByod_allChannels_wow, adobe_cvrByod_allChannels_ly,
        CASE WHEN adobe_cvrByod_allChannels_wow IS NULL OR adobe_cvrByod_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_cvrByod_allChannels - adobe_cvrByod_allChannels_wow) / adobe_cvrByod_allChannels_wow, 6) END AS adobe_cvrByod_allChannels_wow_pct,
        CASE WHEN adobe_cvrByod_allChannels_ly  IS NULL OR adobe_cvrByod_allChannels_ly  = 0 THEN NULL ELSE ROUND((adobe_cvrByod_allChannels - adobe_cvrByod_allChannels_ly)  / adobe_cvrByod_allChannels_ly,  6) END AS adobe_cvrByod_allChannels_yoy_pct,

        adobe_cvrSite_allChannels, adobe_cvrSite_allChannels_wow, adobe_cvrSite_allChannels_ly,
        CASE WHEN adobe_cvrSite_allChannels_wow IS NULL OR adobe_cvrSite_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_cvrSite_allChannels - adobe_cvrSite_allChannels_wow) / adobe_cvrSite_allChannels_wow, 6) END AS adobe_cvrSite_allChannels_wow_pct,
        CASE WHEN adobe_cvrSite_allChannels_ly  IS NULL OR adobe_cvrSite_allChannels_ly  = 0 THEN NULL ELSE ROUND((adobe_cvrSite_allChannels - adobe_cvrSite_allChannels_ly)  / adobe_cvrSite_allChannels_ly,  6) END AS adobe_cvrSite_allChannels_yoy_pct,

        -- NEW: Postpaid + HSI CVR
        adobe_cvrPostpaid_allChannels, adobe_cvrPostpaid_allChannels_wow, adobe_cvrPostpaid_allChannels_ly,
        CASE WHEN adobe_cvrPostpaid_allChannels_wow IS NULL OR adobe_cvrPostpaid_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_cvrPostpaid_allChannels - adobe_cvrPostpaid_allChannels_wow) / adobe_cvrPostpaid_allChannels_wow, 6) END AS adobe_cvrPostpaid_allChannels_wow_pct,
        CASE WHEN adobe_cvrPostpaid_allChannels_ly  IS NULL OR adobe_cvrPostpaid_allChannels_ly  = 0 THEN NULL ELSE ROUND((adobe_cvrPostpaid_allChannels - adobe_cvrPostpaid_allChannels_ly)  / adobe_cvrPostpaid_allChannels_ly,  6) END AS adobe_cvrPostpaid_allChannels_yoy_pct,

        adobe_cvrHsi_allChannels, adobe_cvrHsi_allChannels_wow, adobe_cvrHsi_allChannels_ly,
        CASE WHEN adobe_cvrHsi_allChannels_wow IS NULL OR adobe_cvrHsi_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_cvrHsi_allChannels - adobe_cvrHsi_allChannels_wow) / adobe_cvrHsi_allChannels_wow, 6) END AS adobe_cvrHsi_allChannels_wow_pct,
        CASE WHEN adobe_cvrHsi_allChannels_ly  IS NULL OR adobe_cvrHsi_allChannels_ly  = 0 THEN NULL ELSE ROUND((adobe_cvrHsi_allChannels - adobe_cvrHsi_allChannels_ly)  / adobe_cvrHsi_allChannels_ly,  6) END AS adobe_cvrHsi_allChannels_yoy_pct,

        -- ================================================================ PAID SEARCH
        adobe_uvnbByod_paidSearch, adobe_uvnbByod_paidSearch_wow, adobe_uvnbByod_paidSearch_ly,
        CASE WHEN adobe_uvnbByod_paidSearch_wow IS NULL OR adobe_uvnbByod_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_paidSearch - adobe_uvnbByod_paidSearch_wow) / adobe_uvnbByod_paidSearch_wow, 6) END AS adobe_uvnbByod_paidSearch_wow_pct,
        CASE WHEN adobe_uvnbByod_paidSearch_ly  IS NULL OR adobe_uvnbByod_paidSearch_ly  = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_paidSearch - adobe_uvnbByod_paidSearch_ly)  / adobe_uvnbByod_paidSearch_ly,  6) END AS adobe_uvnbByod_paidSearch_yoy_pct,

        adobe_pctUvnbByodOfTotal_paidSearch, adobe_pctUvnbByodOfTotal_paidSearch_wow, adobe_pctUvnbByodOfTotal_paidSearch_ly,
        CASE WHEN adobe_pctUvnbByodOfTotal_paidSearch_wow IS NULL OR adobe_pctUvnbByodOfTotal_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_pctUvnbByodOfTotal_paidSearch - adobe_pctUvnbByodOfTotal_paidSearch_wow) / adobe_pctUvnbByodOfTotal_paidSearch_wow, 6) END AS adobe_pctUvnbByodOfTotal_paidSearch_wow_pct,
        CASE WHEN adobe_pctUvnbByodOfTotal_paidSearch_ly  IS NULL OR adobe_pctUvnbByodOfTotal_paidSearch_ly  = 0 THEN NULL ELSE ROUND((adobe_pctUvnbByodOfTotal_paidSearch - adobe_pctUvnbByodOfTotal_paidSearch_ly)  / adobe_pctUvnbByodOfTotal_paidSearch_ly,  6) END AS adobe_pctUvnbByodOfTotal_paidSearch_yoy_pct,

        adobe_cartStartByod_paidSearch, adobe_cartStartByod_paidSearch_wow, adobe_cartStartByod_paidSearch_ly,
        CASE WHEN adobe_cartStartByod_paidSearch_wow IS NULL OR adobe_cartStartByod_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_paidSearch - adobe_cartStartByod_paidSearch_wow) / adobe_cartStartByod_paidSearch_wow, 6) END AS adobe_cartStartByod_paidSearch_wow_pct,
        CASE WHEN adobe_cartStartByod_paidSearch_ly  IS NULL OR adobe_cartStartByod_paidSearch_ly  = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_paidSearch - adobe_cartStartByod_paidSearch_ly)  / adobe_cartStartByod_paidSearch_ly,  6) END AS adobe_cartStartByod_paidSearch_yoy_pct,

        adobe_ordersUnassistedByod_paidSearch, adobe_ordersUnassistedByod_paidSearch_wow, adobe_ordersUnassistedByod_paidSearch_ly,
        CASE WHEN adobe_ordersUnassistedByod_paidSearch_wow IS NULL OR adobe_ordersUnassistedByod_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_paidSearch - adobe_ordersUnassistedByod_paidSearch_wow) / adobe_ordersUnassistedByod_paidSearch_wow, 6) END AS adobe_ordersUnassistedByod_paidSearch_wow_pct,
        CASE WHEN adobe_ordersUnassistedByod_paidSearch_ly  IS NULL OR adobe_ordersUnassistedByod_paidSearch_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_paidSearch - adobe_ordersUnassistedByod_paidSearch_ly)  / adobe_ordersUnassistedByod_paidSearch_ly,  6) END AS adobe_ordersUnassistedByod_paidSearch_yoy_pct,

        adobe_ordersAssistedByod_paidSearch, adobe_ordersAssistedByod_paidSearch_wow, adobe_ordersAssistedByod_paidSearch_ly,
        CASE WHEN adobe_ordersAssistedByod_paidSearch_wow IS NULL OR adobe_ordersAssistedByod_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_paidSearch - adobe_ordersAssistedByod_paidSearch_wow) / adobe_ordersAssistedByod_paidSearch_wow, 6) END AS adobe_ordersAssistedByod_paidSearch_wow_pct,
        CASE WHEN adobe_ordersAssistedByod_paidSearch_ly  IS NULL OR adobe_ordersAssistedByod_paidSearch_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_paidSearch - adobe_ordersAssistedByod_paidSearch_ly)  / adobe_ordersAssistedByod_paidSearch_ly,  6) END AS adobe_ordersAssistedByod_paidSearch_yoy_pct,

        adobe_ordersTotalByod_paidSearch, adobe_ordersTotalByod_paidSearch_wow, adobe_ordersTotalByod_paidSearch_ly,
        CASE WHEN adobe_ordersTotalByod_paidSearch_wow IS NULL OR adobe_ordersTotalByod_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersTotalByod_paidSearch - adobe_ordersTotalByod_paidSearch_wow) / adobe_ordersTotalByod_paidSearch_wow, 6) END AS adobe_ordersTotalByod_paidSearch_wow_pct,
        CASE WHEN adobe_ordersTotalByod_paidSearch_ly  IS NULL OR adobe_ordersTotalByod_paidSearch_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersTotalByod_paidSearch - adobe_ordersTotalByod_paidSearch_ly)  / adobe_ordersTotalByod_paidSearch_ly,  6) END AS adobe_ordersTotalByod_paidSearch_yoy_pct,

        -- ================================================================ ORGANIC SEARCH
        adobe_uvnbByod_organicSearch, adobe_uvnbByod_organicSearch_wow, adobe_uvnbByod_organicSearch_ly,
        CASE WHEN adobe_uvnbByod_organicSearch_wow IS NULL OR adobe_uvnbByod_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_organicSearch - adobe_uvnbByod_organicSearch_wow) / adobe_uvnbByod_organicSearch_wow, 6) END AS adobe_uvnbByod_organicSearch_wow_pct,
        CASE WHEN adobe_uvnbByod_organicSearch_ly  IS NULL OR adobe_uvnbByod_organicSearch_ly  = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_organicSearch - adobe_uvnbByod_organicSearch_ly)  / adobe_uvnbByod_organicSearch_ly,  6) END AS adobe_uvnbByod_organicSearch_yoy_pct,

        adobe_pctUvnbByodOfTotal_organicSearch, adobe_pctUvnbByodOfTotal_organicSearch_wow, adobe_pctUvnbByodOfTotal_organicSearch_ly,
        CASE WHEN adobe_pctUvnbByodOfTotal_organicSearch_wow IS NULL OR adobe_pctUvnbByodOfTotal_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_pctUvnbByodOfTotal_organicSearch - adobe_pctUvnbByodOfTotal_organicSearch_wow) / adobe_pctUvnbByodOfTotal_organicSearch_wow, 6) END AS adobe_pctUvnbByodOfTotal_organicSearch_wow_pct,
        CASE WHEN adobe_pctUvnbByodOfTotal_organicSearch_ly  IS NULL OR adobe_pctUvnbByodOfTotal_organicSearch_ly  = 0 THEN NULL ELSE ROUND((adobe_pctUvnbByodOfTotal_organicSearch - adobe_pctUvnbByodOfTotal_organicSearch_ly)  / adobe_pctUvnbByodOfTotal_organicSearch_ly,  6) END AS adobe_pctUvnbByodOfTotal_organicSearch_yoy_pct,

        adobe_cartStartByod_organicSearch, adobe_cartStartByod_organicSearch_wow, adobe_cartStartByod_organicSearch_ly,
        CASE WHEN adobe_cartStartByod_organicSearch_wow IS NULL OR adobe_cartStartByod_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_organicSearch - adobe_cartStartByod_organicSearch_wow) / adobe_cartStartByod_organicSearch_wow, 6) END AS adobe_cartStartByod_organicSearch_wow_pct,
        CASE WHEN adobe_cartStartByod_organicSearch_ly  IS NULL OR adobe_cartStartByod_organicSearch_ly  = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_organicSearch - adobe_cartStartByod_organicSearch_ly)  / adobe_cartStartByod_organicSearch_ly,  6) END AS adobe_cartStartByod_organicSearch_yoy_pct,

        adobe_ordersUnassistedByod_organicSearch, adobe_ordersUnassistedByod_organicSearch_wow, adobe_ordersUnassistedByod_organicSearch_ly,
        CASE WHEN adobe_ordersUnassistedByod_organicSearch_wow IS NULL OR adobe_ordersUnassistedByod_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_organicSearch - adobe_ordersUnassistedByod_organicSearch_wow) / adobe_ordersUnassistedByod_organicSearch_wow, 6) END AS adobe_ordersUnassistedByod_organicSearch_wow_pct,
        CASE WHEN adobe_ordersUnassistedByod_organicSearch_ly  IS NULL OR adobe_ordersUnassistedByod_organicSearch_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_organicSearch - adobe_ordersUnassistedByod_organicSearch_ly)  / adobe_ordersUnassistedByod_organicSearch_ly,  6) END AS adobe_ordersUnassistedByod_organicSearch_yoy_pct,

        adobe_ordersAssistedByod_organicSearch, adobe_ordersAssistedByod_organicSearch_wow, adobe_ordersAssistedByod_organicSearch_ly,
        CASE WHEN adobe_ordersAssistedByod_organicSearch_wow IS NULL OR adobe_ordersAssistedByod_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_organicSearch - adobe_ordersAssistedByod_organicSearch_wow) / adobe_ordersAssistedByod_organicSearch_wow, 6) END AS adobe_ordersAssistedByod_organicSearch_wow_pct,
        CASE WHEN adobe_ordersAssistedByod_organicSearch_ly  IS NULL OR adobe_ordersAssistedByod_organicSearch_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_organicSearch - adobe_ordersAssistedByod_organicSearch_ly)  / adobe_ordersAssistedByod_organicSearch_ly,  6) END AS adobe_ordersAssistedByod_organicSearch_yoy_pct,

        adobe_ordersTotalByod_organicSearch, adobe_ordersTotalByod_organicSearch_wow, adobe_ordersTotalByod_organicSearch_ly,
        CASE WHEN adobe_ordersTotalByod_organicSearch_wow IS NULL OR adobe_ordersTotalByod_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_ordersTotalByod_organicSearch - adobe_ordersTotalByod_organicSearch_wow) / adobe_ordersTotalByod_organicSearch_wow, 6) END AS adobe_ordersTotalByod_organicSearch_wow_pct,
        CASE WHEN adobe_ordersTotalByod_organicSearch_ly  IS NULL OR adobe_ordersTotalByod_organicSearch_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersTotalByod_organicSearch - adobe_ordersTotalByod_organicSearch_ly)  / adobe_ordersTotalByod_organicSearch_ly,  6) END AS adobe_ordersTotalByod_organicSearch_yoy_pct,

        -- ================================================================ DIRECT
        adobe_uvnbByod_direct, adobe_uvnbByod_direct_wow, adobe_uvnbByod_direct_ly,
        CASE WHEN adobe_uvnbByod_direct_wow IS NULL OR adobe_uvnbByod_direct_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_direct - adobe_uvnbByod_direct_wow) / adobe_uvnbByod_direct_wow, 6) END AS adobe_uvnbByod_direct_wow_pct,
        CASE WHEN adobe_uvnbByod_direct_ly  IS NULL OR adobe_uvnbByod_direct_ly  = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_direct - adobe_uvnbByod_direct_ly)  / adobe_uvnbByod_direct_ly,  6) END AS adobe_uvnbByod_direct_yoy_pct,

        adobe_pctUvnbByodOfTotal_direct, adobe_pctUvnbByodOfTotal_direct_wow, adobe_pctUvnbByodOfTotal_direct_ly,
        CASE WHEN adobe_pctUvnbByodOfTotal_direct_wow IS NULL OR adobe_pctUvnbByodOfTotal_direct_wow = 0 THEN NULL ELSE ROUND((adobe_pctUvnbByodOfTotal_direct - adobe_pctUvnbByodOfTotal_direct_wow) / adobe_pctUvnbByodOfTotal_direct_wow, 6) END AS adobe_pctUvnbByodOfTotal_direct_wow_pct,
        CASE WHEN adobe_pctUvnbByodOfTotal_direct_ly  IS NULL OR adobe_pctUvnbByodOfTotal_direct_ly  = 0 THEN NULL ELSE ROUND((adobe_pctUvnbByodOfTotal_direct - adobe_pctUvnbByodOfTotal_direct_ly)  / adobe_pctUvnbByodOfTotal_direct_ly,  6) END AS adobe_pctUvnbByodOfTotal_direct_yoy_pct,

        adobe_cartStartByod_direct, adobe_cartStartByod_direct_wow, adobe_cartStartByod_direct_ly,
        CASE WHEN adobe_cartStartByod_direct_wow IS NULL OR adobe_cartStartByod_direct_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_direct - adobe_cartStartByod_direct_wow) / adobe_cartStartByod_direct_wow, 6) END AS adobe_cartStartByod_direct_wow_pct,
        CASE WHEN adobe_cartStartByod_direct_ly  IS NULL OR adobe_cartStartByod_direct_ly  = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_direct - adobe_cartStartByod_direct_ly)  / adobe_cartStartByod_direct_ly,  6) END AS adobe_cartStartByod_direct_yoy_pct,

        adobe_ordersUnassistedByod_direct, adobe_ordersUnassistedByod_direct_wow, adobe_ordersUnassistedByod_direct_ly,
        CASE WHEN adobe_ordersUnassistedByod_direct_wow IS NULL OR adobe_ordersUnassistedByod_direct_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_direct - adobe_ordersUnassistedByod_direct_wow) / adobe_ordersUnassistedByod_direct_wow, 6) END AS adobe_ordersUnassistedByod_direct_wow_pct,
        CASE WHEN adobe_ordersUnassistedByod_direct_ly  IS NULL OR adobe_ordersUnassistedByod_direct_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_direct - adobe_ordersUnassistedByod_direct_ly)  / adobe_ordersUnassistedByod_direct_ly,  6) END AS adobe_ordersUnassistedByod_direct_yoy_pct,

        adobe_ordersAssistedByod_direct, adobe_ordersAssistedByod_direct_wow, adobe_ordersAssistedByod_direct_ly,
        CASE WHEN adobe_ordersAssistedByod_direct_wow IS NULL OR adobe_ordersAssistedByod_direct_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_direct - adobe_ordersAssistedByod_direct_wow) / adobe_ordersAssistedByod_direct_wow, 6) END AS adobe_ordersAssistedByod_direct_wow_pct,
        CASE WHEN adobe_ordersAssistedByod_direct_ly  IS NULL OR adobe_ordersAssistedByod_direct_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_direct - adobe_ordersAssistedByod_direct_ly)  / adobe_ordersAssistedByod_direct_ly,  6) END AS adobe_ordersAssistedByod_direct_yoy_pct,

        adobe_ordersTotalByod_direct, adobe_ordersTotalByod_direct_wow, adobe_ordersTotalByod_direct_ly,
        CASE WHEN adobe_ordersTotalByod_direct_wow IS NULL OR adobe_ordersTotalByod_direct_wow = 0 THEN NULL ELSE ROUND((adobe_ordersTotalByod_direct - adobe_ordersTotalByod_direct_wow) / adobe_ordersTotalByod_direct_wow, 6) END AS adobe_ordersTotalByod_direct_wow_pct,
        CASE WHEN adobe_ordersTotalByod_direct_ly  IS NULL OR adobe_ordersTotalByod_direct_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersTotalByod_direct - adobe_ordersTotalByod_direct_ly)  / adobe_ordersTotalByod_direct_ly,  6) END AS adobe_ordersTotalByod_direct_yoy_pct,

        -- ================================================================ SOCIAL
        adobe_uvnbByod_social, adobe_uvnbByod_social_wow, adobe_uvnbByod_social_ly,
        CASE WHEN adobe_uvnbByod_social_wow IS NULL OR adobe_uvnbByod_social_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_social - adobe_uvnbByod_social_wow) / adobe_uvnbByod_social_wow, 6) END AS adobe_uvnbByod_social_wow_pct,
        CASE WHEN adobe_uvnbByod_social_ly  IS NULL OR adobe_uvnbByod_social_ly  = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_social - adobe_uvnbByod_social_ly)  / adobe_uvnbByod_social_ly,  6) END AS adobe_uvnbByod_social_yoy_pct,

        adobe_pctUvnbByodOfTotal_social, adobe_pctUvnbByodOfTotal_social_wow, adobe_pctUvnbByodOfTotal_social_ly,
        CASE WHEN adobe_pctUvnbByodOfTotal_social_wow IS NULL OR adobe_pctUvnbByodOfTotal_social_wow = 0 THEN NULL ELSE ROUND((adobe_pctUvnbByodOfTotal_social - adobe_pctUvnbByodOfTotal_social_wow) / adobe_pctUvnbByodOfTotal_social_wow, 6) END AS adobe_pctUvnbByodOfTotal_social_wow_pct,
        CASE WHEN adobe_pctUvnbByodOfTotal_social_ly  IS NULL OR adobe_pctUvnbByodOfTotal_social_ly  = 0 THEN NULL ELSE ROUND((adobe_pctUvnbByodOfTotal_social - adobe_pctUvnbByodOfTotal_social_ly)  / adobe_pctUvnbByodOfTotal_social_ly,  6) END AS adobe_pctUvnbByodOfTotal_social_yoy_pct,

        adobe_cartStartByod_social, adobe_cartStartByod_social_wow, adobe_cartStartByod_social_ly,
        CASE WHEN adobe_cartStartByod_social_wow IS NULL OR adobe_cartStartByod_social_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_social - adobe_cartStartByod_social_wow) / adobe_cartStartByod_social_wow, 6) END AS adobe_cartStartByod_social_wow_pct,
        CASE WHEN adobe_cartStartByod_social_ly  IS NULL OR adobe_cartStartByod_social_ly  = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_social - adobe_cartStartByod_social_ly)  / adobe_cartStartByod_social_ly,  6) END AS adobe_cartStartByod_social_yoy_pct,

        adobe_ordersUnassistedByod_social, adobe_ordersUnassistedByod_social_wow, adobe_ordersUnassistedByod_social_ly,
        CASE WHEN adobe_ordersUnassistedByod_social_wow IS NULL OR adobe_ordersUnassistedByod_social_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_social - adobe_ordersUnassistedByod_social_wow) / adobe_ordersUnassistedByod_social_wow, 6) END AS adobe_ordersUnassistedByod_social_wow_pct,
        CASE WHEN adobe_ordersUnassistedByod_social_ly  IS NULL OR adobe_ordersUnassistedByod_social_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_social - adobe_ordersUnassistedByod_social_ly)  / adobe_ordersUnassistedByod_social_ly,  6) END AS adobe_ordersUnassistedByod_social_yoy_pct,

        adobe_ordersAssistedByod_social, adobe_ordersAssistedByod_social_wow, adobe_ordersAssistedByod_social_ly,
        CASE WHEN adobe_ordersAssistedByod_social_wow IS NULL OR adobe_ordersAssistedByod_social_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_social - adobe_ordersAssistedByod_social_wow) / adobe_ordersAssistedByod_social_wow, 6) END AS adobe_ordersAssistedByod_social_wow_pct,
        CASE WHEN adobe_ordersAssistedByod_social_ly  IS NULL OR adobe_ordersAssistedByod_social_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_social - adobe_ordersAssistedByod_social_ly)  / adobe_ordersAssistedByod_social_ly,  6) END AS adobe_ordersAssistedByod_social_yoy_pct,

        adobe_ordersTotalByod_social, adobe_ordersTotalByod_social_wow, adobe_ordersTotalByod_social_ly,
        CASE WHEN adobe_ordersTotalByod_social_wow IS NULL OR adobe_ordersTotalByod_social_wow = 0 THEN NULL ELSE ROUND((adobe_ordersTotalByod_social - adobe_ordersTotalByod_social_wow) / adobe_ordersTotalByod_social_wow, 6) END AS adobe_ordersTotalByod_social_wow_pct,
        CASE WHEN adobe_ordersTotalByod_social_ly  IS NULL OR adobe_ordersTotalByod_social_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersTotalByod_social - adobe_ordersTotalByod_social_ly)  / adobe_ordersTotalByod_social_ly,  6) END AS adobe_ordersTotalByod_social_yoy_pct,

        -- ================================================================ PROGRAMMATIC
        adobe_uvnbByod_programmatic, adobe_uvnbByod_programmatic_wow, adobe_uvnbByod_programmatic_ly,
        CASE WHEN adobe_uvnbByod_programmatic_wow IS NULL OR adobe_uvnbByod_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_programmatic - adobe_uvnbByod_programmatic_wow) / adobe_uvnbByod_programmatic_wow, 6) END AS adobe_uvnbByod_programmatic_wow_pct,
        CASE WHEN adobe_uvnbByod_programmatic_ly  IS NULL OR adobe_uvnbByod_programmatic_ly  = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_programmatic - adobe_uvnbByod_programmatic_ly)  / adobe_uvnbByod_programmatic_ly,  6) END AS adobe_uvnbByod_programmatic_yoy_pct,

        adobe_pctUvnbByodOfTotal_programmatic, adobe_pctUvnbByodOfTotal_programmatic_wow, adobe_pctUvnbByodOfTotal_programmatic_ly,
        CASE WHEN adobe_pctUvnbByodOfTotal_programmatic_wow IS NULL OR adobe_pctUvnbByodOfTotal_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_pctUvnbByodOfTotal_programmatic - adobe_pctUvnbByodOfTotal_programmatic_wow) / adobe_pctUvnbByodOfTotal_programmatic_wow, 6) END AS adobe_pctUvnbByodOfTotal_programmatic_wow_pct,
        CASE WHEN adobe_pctUvnbByodOfTotal_programmatic_ly  IS NULL OR adobe_pctUvnbByodOfTotal_programmatic_ly  = 0 THEN NULL ELSE ROUND((adobe_pctUvnbByodOfTotal_programmatic - adobe_pctUvnbByodOfTotal_programmatic_ly)  / adobe_pctUvnbByodOfTotal_programmatic_ly,  6) END AS adobe_pctUvnbByodOfTotal_programmatic_yoy_pct,

        adobe_cartStartByod_programmatic, adobe_cartStartByod_programmatic_wow, adobe_cartStartByod_programmatic_ly,
        CASE WHEN adobe_cartStartByod_programmatic_wow IS NULL OR adobe_cartStartByod_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_programmatic - adobe_cartStartByod_programmatic_wow) / adobe_cartStartByod_programmatic_wow, 6) END AS adobe_cartStartByod_programmatic_wow_pct,
        CASE WHEN adobe_cartStartByod_programmatic_ly  IS NULL OR adobe_cartStartByod_programmatic_ly  = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_programmatic - adobe_cartStartByod_programmatic_ly)  / adobe_cartStartByod_programmatic_ly,  6) END AS adobe_cartStartByod_programmatic_yoy_pct,

        adobe_ordersUnassistedByod_programmatic, adobe_ordersUnassistedByod_programmatic_wow, adobe_ordersUnassistedByod_programmatic_ly,
        CASE WHEN adobe_ordersUnassistedByod_programmatic_wow IS NULL OR adobe_ordersUnassistedByod_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_programmatic - adobe_ordersUnassistedByod_programmatic_wow) / adobe_ordersUnassistedByod_programmatic_wow, 6) END AS adobe_ordersUnassistedByod_programmatic_wow_pct,
        CASE WHEN adobe_ordersUnassistedByod_programmatic_ly  IS NULL OR adobe_ordersUnassistedByod_programmatic_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_programmatic - adobe_ordersUnassistedByod_programmatic_ly)  / adobe_ordersUnassistedByod_programmatic_ly,  6) END AS adobe_ordersUnassistedByod_programmatic_yoy_pct,

        adobe_ordersAssistedByod_programmatic, adobe_ordersAssistedByod_programmatic_wow, adobe_ordersAssistedByod_programmatic_ly,
        CASE WHEN adobe_ordersAssistedByod_programmatic_wow IS NULL OR adobe_ordersAssistedByod_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_programmatic - adobe_ordersAssistedByod_programmatic_wow) / adobe_ordersAssistedByod_programmatic_wow, 6) END AS adobe_ordersAssistedByod_programmatic_wow_pct,
        CASE WHEN adobe_ordersAssistedByod_programmatic_ly  IS NULL OR adobe_ordersAssistedByod_programmatic_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_programmatic - adobe_ordersAssistedByod_programmatic_ly)  / adobe_ordersAssistedByod_programmatic_ly,  6) END AS adobe_ordersAssistedByod_programmatic_yoy_pct,

        adobe_ordersTotalByod_programmatic, adobe_ordersTotalByod_programmatic_wow, adobe_ordersTotalByod_programmatic_ly,
        CASE WHEN adobe_ordersTotalByod_programmatic_wow IS NULL OR adobe_ordersTotalByod_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_ordersTotalByod_programmatic - adobe_ordersTotalByod_programmatic_wow) / adobe_ordersTotalByod_programmatic_wow, 6) END AS adobe_ordersTotalByod_programmatic_wow_pct,
        CASE WHEN adobe_ordersTotalByod_programmatic_ly  IS NULL OR adobe_ordersTotalByod_programmatic_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersTotalByod_programmatic - adobe_ordersTotalByod_programmatic_ly)  / adobe_ordersTotalByod_programmatic_ly,  6) END AS adobe_ordersTotalByod_programmatic_yoy_pct,

        -- ================================================================ OTHER
        adobe_uvnbByod_other, adobe_uvnbByod_other_wow, adobe_uvnbByod_other_ly,
        CASE WHEN adobe_uvnbByod_other_wow IS NULL OR adobe_uvnbByod_other_wow = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_other - adobe_uvnbByod_other_wow) / adobe_uvnbByod_other_wow, 6) END AS adobe_uvnbByod_other_wow_pct,
        CASE WHEN adobe_uvnbByod_other_ly  IS NULL OR adobe_uvnbByod_other_ly  = 0 THEN NULL ELSE ROUND((adobe_uvnbByod_other - adobe_uvnbByod_other_ly)  / adobe_uvnbByod_other_ly,  6) END AS adobe_uvnbByod_other_yoy_pct,

        adobe_pctUvnbByodOfTotal_other, adobe_pctUvnbByodOfTotal_other_wow, adobe_pctUvnbByodOfTotal_other_ly,
        CASE WHEN adobe_pctUvnbByodOfTotal_other_wow IS NULL OR adobe_pctUvnbByodOfTotal_other_wow = 0 THEN NULL ELSE ROUND((adobe_pctUvnbByodOfTotal_other - adobe_pctUvnbByodOfTotal_other_wow) / adobe_pctUvnbByodOfTotal_other_wow, 6) END AS adobe_pctUvnbByodOfTotal_other_wow_pct,
        CASE WHEN adobe_pctUvnbByodOfTotal_other_ly  IS NULL OR adobe_pctUvnbByodOfTotal_other_ly  = 0 THEN NULL ELSE ROUND((adobe_pctUvnbByodOfTotal_other - adobe_pctUvnbByodOfTotal_other_ly)  / adobe_pctUvnbByodOfTotal_other_ly,  6) END AS adobe_pctUvnbByodOfTotal_other_yoy_pct,

        adobe_cartStartByod_other, adobe_cartStartByod_other_wow, adobe_cartStartByod_other_ly,
        CASE WHEN adobe_cartStartByod_other_wow IS NULL OR adobe_cartStartByod_other_wow = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_other - adobe_cartStartByod_other_wow) / adobe_cartStartByod_other_wow, 6) END AS adobe_cartStartByod_other_wow_pct,
        CASE WHEN adobe_cartStartByod_other_ly  IS NULL OR adobe_cartStartByod_other_ly  = 0 THEN NULL ELSE ROUND((adobe_cartStartByod_other - adobe_cartStartByod_other_ly)  / adobe_cartStartByod_other_ly,  6) END AS adobe_cartStartByod_other_yoy_pct,

        adobe_ordersUnassistedByod_other, adobe_ordersUnassistedByod_other_wow, adobe_ordersUnassistedByod_other_ly,
        CASE WHEN adobe_ordersUnassistedByod_other_wow IS NULL OR adobe_ordersUnassistedByod_other_wow = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_other - adobe_ordersUnassistedByod_other_wow) / adobe_ordersUnassistedByod_other_wow, 6) END AS adobe_ordersUnassistedByod_other_wow_pct,
        CASE WHEN adobe_ordersUnassistedByod_other_ly  IS NULL OR adobe_ordersUnassistedByod_other_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersUnassistedByod_other - adobe_ordersUnassistedByod_other_ly)  / adobe_ordersUnassistedByod_other_ly,  6) END AS adobe_ordersUnassistedByod_other_yoy_pct,

        adobe_ordersAssistedByod_other, adobe_ordersAssistedByod_other_wow, adobe_ordersAssistedByod_other_ly,
        CASE WHEN adobe_ordersAssistedByod_other_wow IS NULL OR adobe_ordersAssistedByod_other_wow = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_other - adobe_ordersAssistedByod_other_wow) / adobe_ordersAssistedByod_other_wow, 6) END AS adobe_ordersAssistedByod_other_wow_pct,
        CASE WHEN adobe_ordersAssistedByod_other_ly  IS NULL OR adobe_ordersAssistedByod_other_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersAssistedByod_other - adobe_ordersAssistedByod_other_ly)  / adobe_ordersAssistedByod_other_ly,  6) END AS adobe_ordersAssistedByod_other_yoy_pct,

        adobe_ordersTotalByod_other, adobe_ordersTotalByod_other_wow, adobe_ordersTotalByod_other_ly,
        CASE WHEN adobe_ordersTotalByod_other_wow IS NULL OR adobe_ordersTotalByod_other_wow = 0 THEN NULL ELSE ROUND((adobe_ordersTotalByod_other - adobe_ordersTotalByod_other_wow) / adobe_ordersTotalByod_other_wow, 6) END AS adobe_ordersTotalByod_other_wow_pct,
        CASE WHEN adobe_ordersTotalByod_other_ly  IS NULL OR adobe_ordersTotalByod_other_ly  = 0 THEN NULL ELSE ROUND((adobe_ordersTotalByod_other - adobe_ordersTotalByod_other_ly)  / adobe_ordersTotalByod_other_ly,  6) END AS adobe_ordersTotalByod_other_yoy_pct

    FROM with_comparisons
),

-- -----------------------------------------------------------------------
-- STEP 7: max_data_date
-- -----------------------------------------------------------------------
with_max_date AS (
    SELECT
        *,
        MAX(CASE
            WHEN adobe_uvnbByod_allChannels       IS NOT NULL
              OR adobe_ordersTotalByod_allChannels IS NOT NULL
            THEN week_sun_to_sat
        END) OVER ()                                                    AS max_data_date
    FROM with_pcts
)

SELECT
    week_sun_to_sat,
    'ADOBE'                                                             AS data_source,
    max_data_date,

    -- ================================================================ ALL CHANNELS
    adobe_uvnbByod_allChannels,              adobe_uvnbByod_allChannels_wow,              adobe_uvnbByod_allChannels_ly,              adobe_uvnbByod_allChannels_wow_pct,              adobe_uvnbByod_allChannels_yoy_pct,
    adobe_uvnbTotal_allChannels,             adobe_uvnbTotal_allChannels_wow,             adobe_uvnbTotal_allChannels_ly,             adobe_uvnbTotal_allChannels_wow_pct,             adobe_uvnbTotal_allChannels_yoy_pct,
    adobe_uvnbFlowTotal_allChannels,         adobe_uvnbFlowTotal_allChannels_wow,         adobe_uvnbFlowTotal_allChannels_ly,         adobe_uvnbFlowTotal_allChannels_wow_pct,         adobe_uvnbFlowTotal_allChannels_yoy_pct,
    adobe_pctUvnbByodOfUvnbFlow_allChannels, adobe_pctUvnbByodOfUvnbFlow_allChannels_wow, adobe_pctUvnbByodOfUvnbFlow_allChannels_ly, adobe_pctUvnbByodOfUvnbFlow_allChannels_wow_pct, adobe_pctUvnbByodOfUvnbFlow_allChannels_yoy_pct,
    adobe_cartStartByod_allChannels,         adobe_cartStartByod_allChannels_wow,         adobe_cartStartByod_allChannels_ly,         adobe_cartStartByod_allChannels_wow_pct,         adobe_cartStartByod_allChannels_yoy_pct,
    adobe_ordersUnassistedByod_allChannels,  adobe_ordersUnassistedByod_allChannels_wow,  adobe_ordersUnassistedByod_allChannels_ly,  adobe_ordersUnassistedByod_allChannels_wow_pct,  adobe_ordersUnassistedByod_allChannels_yoy_pct,
    adobe_ordersAssistedByod_allChannels,    adobe_ordersAssistedByod_allChannels_wow,    adobe_ordersAssistedByod_allChannels_ly,    adobe_ordersAssistedByod_allChannels_wow_pct,    adobe_ordersAssistedByod_allChannels_yoy_pct,
    adobe_ordersTotalByod_allChannels,       adobe_ordersTotalByod_allChannels_wow,       adobe_ordersTotalByod_allChannels_ly,       adobe_ordersTotalByod_allChannels_wow_pct,       adobe_ordersTotalByod_allChannels_yoy_pct,
    adobe_ordersTotal_allChannels,           adobe_ordersTotal_allChannels_wow,           adobe_ordersTotal_allChannels_ly,           adobe_ordersTotal_allChannels_wow_pct,           adobe_ordersTotal_allChannels_yoy_pct,
    adobe_pctOrdersByodOfOrdersTotal_allChannels, adobe_pctOrdersByodOfOrdersTotal_allChannels_wow, adobe_pctOrdersByodOfOrdersTotal_allChannels_ly, adobe_pctOrdersByodOfOrdersTotal_allChannels_wow_pct, adobe_pctOrdersByodOfOrdersTotal_allChannels_yoy_pct,
    adobe_cvrByod_allChannels,               adobe_cvrByod_allChannels_wow,               adobe_cvrByod_allChannels_ly,               adobe_cvrByod_allChannels_wow_pct,               adobe_cvrByod_allChannels_yoy_pct,
    adobe_cvrSite_allChannels,               adobe_cvrSite_allChannels_wow,               adobe_cvrSite_allChannels_ly,               adobe_cvrSite_allChannels_wow_pct,               adobe_cvrSite_allChannels_yoy_pct,
    -- NEW
    adobe_cvrPostpaid_allChannels,           adobe_cvrPostpaid_allChannels_wow,           adobe_cvrPostpaid_allChannels_ly,           adobe_cvrPostpaid_allChannels_wow_pct,           adobe_cvrPostpaid_allChannels_yoy_pct,
    adobe_cvrHsi_allChannels,                adobe_cvrHsi_allChannels_wow,                adobe_cvrHsi_allChannels_ly,                adobe_cvrHsi_allChannels_wow_pct,                adobe_cvrHsi_allChannels_yoy_pct,

    -- ================================================================ PAID SEARCH
    adobe_uvnbByod_paidSearch,               adobe_uvnbByod_paidSearch_wow,               adobe_uvnbByod_paidSearch_ly,               adobe_uvnbByod_paidSearch_wow_pct,               adobe_uvnbByod_paidSearch_yoy_pct,
    adobe_pctUvnbByodOfTotal_paidSearch,     adobe_pctUvnbByodOfTotal_paidSearch_wow,     adobe_pctUvnbByodOfTotal_paidSearch_ly,     adobe_pctUvnbByodOfTotal_paidSearch_wow_pct,     adobe_pctUvnbByodOfTotal_paidSearch_yoy_pct,
    adobe_cartStartByod_paidSearch,          adobe_cartStartByod_paidSearch_wow,          adobe_cartStartByod_paidSearch_ly,          adobe_cartStartByod_paidSearch_wow_pct,          adobe_cartStartByod_paidSearch_yoy_pct,
    adobe_ordersUnassistedByod_paidSearch,   adobe_ordersUnassistedByod_paidSearch_wow,   adobe_ordersUnassistedByod_paidSearch_ly,   adobe_ordersUnassistedByod_paidSearch_wow_pct,   adobe_ordersUnassistedByod_paidSearch_yoy_pct,
    adobe_ordersAssistedByod_paidSearch,     adobe_ordersAssistedByod_paidSearch_wow,     adobe_ordersAssistedByod_paidSearch_ly,     adobe_ordersAssistedByod_paidSearch_wow_pct,     adobe_ordersAssistedByod_paidSearch_yoy_pct,
    adobe_ordersTotalByod_paidSearch,        adobe_ordersTotalByod_paidSearch_wow,        adobe_ordersTotalByod_paidSearch_ly,        adobe_ordersTotalByod_paidSearch_wow_pct,        adobe_ordersTotalByod_paidSearch_yoy_pct,

    -- ================================================================ ORGANIC SEARCH
    adobe_uvnbByod_organicSearch,            adobe_uvnbByod_organicSearch_wow,            adobe_uvnbByod_organicSearch_ly,            adobe_uvnbByod_organicSearch_wow_pct,            adobe_uvnbByod_organicSearch_yoy_pct,
    adobe_pctUvnbByodOfTotal_organicSearch,  adobe_pctUvnbByodOfTotal_organicSearch_wow,  adobe_pctUvnbByodOfTotal_organicSearch_ly,  adobe_pctUvnbByodOfTotal_organicSearch_wow_pct,  adobe_pctUvnbByodOfTotal_organicSearch_yoy_pct,
    adobe_cartStartByod_organicSearch,       adobe_cartStartByod_organicSearch_wow,       adobe_cartStartByod_organicSearch_ly,       adobe_cartStartByod_organicSearch_wow_pct,       adobe_cartStartByod_organicSearch_yoy_pct,
    adobe_ordersUnassistedByod_organicSearch,adobe_ordersUnassistedByod_organicSearch_wow,adobe_ordersUnassistedByod_organicSearch_ly,adobe_ordersUnassistedByod_organicSearch_wow_pct,adobe_ordersUnassistedByod_organicSearch_yoy_pct,
    adobe_ordersAssistedByod_organicSearch,  adobe_ordersAssistedByod_organicSearch_wow,  adobe_ordersAssistedByod_organicSearch_ly,  adobe_ordersAssistedByod_organicSearch_wow_pct,  adobe_ordersAssistedByod_organicSearch_yoy_pct,
    adobe_ordersTotalByod_organicSearch,     adobe_ordersTotalByod_organicSearch_wow,     adobe_ordersTotalByod_organicSearch_ly,     adobe_ordersTotalByod_organicSearch_wow_pct,     adobe_ordersTotalByod_organicSearch_yoy_pct,

    -- ================================================================ DIRECT
    adobe_uvnbByod_direct,                   adobe_uvnbByod_direct_wow,                   adobe_uvnbByod_direct_ly,                   adobe_uvnbByod_direct_wow_pct,                   adobe_uvnbByod_direct_yoy_pct,
    adobe_pctUvnbByodOfTotal_direct,         adobe_pctUvnbByodOfTotal_direct_wow,         adobe_pctUvnbByodOfTotal_direct_ly,         adobe_pctUvnbByodOfTotal_direct_wow_pct,         adobe_pctUvnbByodOfTotal_direct_yoy_pct,
    adobe_cartStartByod_direct,              adobe_cartStartByod_direct_wow,              adobe_cartStartByod_direct_ly,              adobe_cartStartByod_direct_wow_pct,              adobe_cartStartByod_direct_yoy_pct,
    adobe_ordersUnassistedByod_direct,       adobe_ordersUnassistedByod_direct_wow,       adobe_ordersUnassistedByod_direct_ly,       adobe_ordersUnassistedByod_direct_wow_pct,       adobe_ordersUnassistedByod_direct_yoy_pct,
    adobe_ordersAssistedByod_direct,         adobe_ordersAssistedByod_direct_wow,         adobe_ordersAssistedByod_direct_ly,         adobe_ordersAssistedByod_direct_wow_pct,         adobe_ordersAssistedByod_direct_yoy_pct,
    adobe_ordersTotalByod_direct,            adobe_ordersTotalByod_direct_wow,            adobe_ordersTotalByod_direct_ly,            adobe_ordersTotalByod_direct_wow_pct,            adobe_ordersTotalByod_direct_yoy_pct,

    -- ================================================================ SOCIAL
    adobe_uvnbByod_social,                   adobe_uvnbByod_social_wow,                   adobe_uvnbByod_social_ly,                   adobe_uvnbByod_social_wow_pct,                   adobe_uvnbByod_social_yoy_pct,
    adobe_pctUvnbByodOfTotal_social,         adobe_pctUvnbByodOfTotal_social_wow,         adobe_pctUvnbByodOfTotal_social_ly,         adobe_pctUvnbByodOfTotal_social_wow_pct,         adobe_pctUvnbByodOfTotal_social_yoy_pct,
    adobe_cartStartByod_social,              adobe_cartStartByod_social_wow,              adobe_cartStartByod_social_ly,              adobe_cartStartByod_social_wow_pct,              adobe_cartStartByod_social_yoy_pct,
    adobe_ordersUnassistedByod_social,       adobe_ordersUnassistedByod_social_wow,       adobe_ordersUnassistedByod_social_ly,       adobe_ordersUnassistedByod_social_wow_pct,       adobe_ordersUnassistedByod_social_yoy_pct,
    adobe_ordersAssistedByod_social,         adobe_ordersAssistedByod_social_wow,         adobe_ordersAssistedByod_social_ly,         adobe_ordersAssistedByod_social_wow_pct,         adobe_ordersAssistedByod_social_yoy_pct,
    adobe_ordersTotalByod_social,            adobe_ordersTotalByod_social_wow,            adobe_ordersTotalByod_social_ly,            adobe_ordersTotalByod_social_wow_pct,            adobe_ordersTotalByod_social_yoy_pct,

    -- ================================================================ PROGRAMMATIC
    adobe_uvnbByod_programmatic,             adobe_uvnbByod_programmatic_wow,             adobe_uvnbByod_programmatic_ly,             adobe_uvnbByod_programmatic_wow_pct,             adobe_uvnbByod_programmatic_yoy_pct,
    adobe_pctUvnbByodOfTotal_programmatic,   adobe_pctUvnbByodOfTotal_programmatic_wow,   adobe_pctUvnbByodOfTotal_programmatic_ly,   adobe_pctUvnbByodOfTotal_programmatic_wow_pct,   adobe_pctUvnbByodOfTotal_programmatic_yoy_pct,
    adobe_cartStartByod_programmatic,        adobe_cartStartByod_programmatic_wow,        adobe_cartStartByod_programmatic_ly,        adobe_cartStartByod_programmatic_wow_pct,        adobe_cartStartByod_programmatic_yoy_pct,
    adobe_ordersUnassistedByod_programmatic, adobe_ordersUnassistedByod_programmatic_wow, adobe_ordersUnassistedByod_programmatic_ly, adobe_ordersUnassistedByod_programmatic_wow_pct, adobe_ordersUnassistedByod_programmatic_yoy_pct,
    adobe_ordersAssistedByod_programmatic,   adobe_ordersAssistedByod_programmatic_wow,   adobe_ordersAssistedByod_programmatic_ly,   adobe_ordersAssistedByod_programmatic_wow_pct,   adobe_ordersAssistedByod_programmatic_yoy_pct,
    adobe_ordersTotalByod_programmatic,      adobe_ordersTotalByod_programmatic_wow,      adobe_ordersTotalByod_programmatic_ly,      adobe_ordersTotalByod_programmatic_wow_pct,      adobe_ordersTotalByod_programmatic_yoy_pct,

    -- ================================================================ OTHER
    adobe_uvnbByod_other,                    adobe_uvnbByod_other_wow,                    adobe_uvnbByod_other_ly,                    adobe_uvnbByod_other_wow_pct,                    adobe_uvnbByod_other_yoy_pct,
    adobe_pctUvnbByodOfTotal_other,          adobe_pctUvnbByodOfTotal_other_wow,          adobe_pctUvnbByodOfTotal_other_ly,          adobe_pctUvnbByodOfTotal_other_wow_pct,          adobe_pctUvnbByodOfTotal_other_yoy_pct,
    adobe_cartStartByod_other,               adobe_cartStartByod_other_wow,               adobe_cartStartByod_other_ly,               adobe_cartStartByod_other_wow_pct,               adobe_cartStartByod_other_yoy_pct,
    adobe_ordersUnassistedByod_other,        adobe_ordersUnassistedByod_other_wow,        adobe_ordersUnassistedByod_other_ly,        adobe_ordersUnassistedByod_other_wow_pct,        adobe_ordersUnassistedByod_other_yoy_pct,
    adobe_ordersAssistedByod_other,          adobe_ordersAssistedByod_other_wow,          adobe_ordersAssistedByod_other_ly,          adobe_ordersAssistedByod_other_wow_pct,          adobe_ordersAssistedByod_other_yoy_pct,
    adobe_ordersTotalByod_other,             adobe_ordersTotalByod_other_wow,             adobe_ordersTotalByod_other_ly,             adobe_ordersTotalByod_other_wow_pct,             adobe_ordersTotalByod_other_yoy_pct

FROM with_max_date
;