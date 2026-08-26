/* =================================================================================================
FILE:         sdi_sp_dashboardPulseByod_silver_adobe_weekly.sql
LAYER:        Silver View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseByod_silver_adobe_weekly

SOURCE:
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_flowPerformanceByLtcGroupsPlusAllChannels_weekly

DESTINATION:
  prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly

PURPOSE:
  Silver view for Adobe Analytics BYOD metrics.
  Outputs a WIDE table — one row per week_sun_to_sat.

COLUMN NAMING CONVENTION:
  adobe_{metric}_{channel}
  Pct/CVR metrics prefixed with type:
    adobe_pct{subject}Of{denominator}_{channel}
    adobe_cvr{scope}_{channel}

  Raw metrics:
    upvByod               (all channels)
    upvTotal              (allChannels only)
    upvFlowTotal          (allChannels only)
    cartStartByod          (all channels)
    ordersUnassistedByod   (all channels)
    ordersAssistedByod     (all channels)
    ordersTotalByod        (all channels)
    ordersTotal            (allChannels only)

  Derived metrics (allChannels only):
    pctUpvByodOfUpvFlow          — upvByod / upvFlowTotal
    pctOrdersByodOfOrdersTotal     — ordersTotalByod / ordersTotal
    cvrByod                        — ordersTotalByod / upvByod
    cvrSite                        — ordersTotal / upvTotal
    cvrPostpaid                    — (OrdersUnassistedPostpaid + OrdersAssistedPostpaid) / UpvPostpaid
    cvrHsi                         — (OrdersUnassistedHsi + OrdersAssistedHsi) / UpvHsi

  Derived metrics (per non-ALL channel):
    pctUpvByodOfTotal             — channel share of BYOD UPV
    Denominator = SUM of the 6 individual channel UPVs (not allChannels).

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
  custom_week_num = DATEDIFF(WEEK, DATE '2023-01-01', DATE_SUB(week_sun_to_sat, 6))

DOWNSTREAM:
  Gold Wide : vw_sdi_pulseByod_gold_unified_wide
  Gold Long : vw_sdi_pulseByod_gold_unified_long
================================================================================================= */

CREATE OR REPLACE PROCEDURE
prdrzranalytics.lab42.sdi_sp_dashboardPulseByod_silver_adobe_weekly()
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
AS
BEGIN

  CREATE OR REPLACE TABLE
  prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
  USING DELTA
  AS

-- -----------------------------------------------------------------------
-- STEP 1: Extract metrics per channel row
-- -----------------------------------------------------------------------
WITH base AS (
    SELECT
        WeekSunSat                                                      AS week_sun_to_sat,
        ChannelGroup,
        UpvByod,
        UpvTotalAdobe,
        UpvFlowTotal,
        CartstartByod,
        OrdersUnassistedByod,
        OrdersAssistedByod,
        OrdersUnassistedByod + OrdersAssistedByod                      AS ordersTotalByod,
        OrdersTotal,
        -- Postpaid and HSI — used to compute cvrPostpaid / cvrHsi (allChannels only)
        UpvPostpaid,
        UpvHsi,
        OrdersUnassistedPostpaid + OrdersAssistedPostpaid              AS ordersTotalPostpaid,
        OrdersUnassistedHsi      + OrdersAssistedHsi                   AS ordersTotalHsi
    FROM prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_flowPerformanceByLtcGroupsPlusAllChannels_weekly
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
        MAX(CASE WHEN ChannelGroup = 'All Channels' THEN UpvByod       END)    AS adobe_upvByod_allChannels,
        MAX(CASE WHEN ChannelGroup = 'All Channels' THEN UpvTotalAdobe END)    AS adobe_upvTotal_allChannels,
        MAX(CASE WHEN ChannelGroup = 'All Channels' THEN UpvFlowTotal  END)    AS adobe_upvFlowTotal_allChannels,
        MAX(CASE WHEN ChannelGroup = 'All Channels' THEN CartstartByod  END)    AS adobe_cartStartByod_allChannels,
        MAX(CASE WHEN ChannelGroup = 'All Channels' THEN OrdersUnassistedByod END) AS adobe_ordersUnassistedByod_allChannels,
        MAX(CASE WHEN ChannelGroup = 'All Channels' THEN OrdersAssistedByod   END) AS adobe_ordersAssistedByod_allChannels,
        MAX(CASE WHEN ChannelGroup = 'All Channels' THEN ordersTotalByod      END) AS adobe_ordersTotalByod_allChannels,
        MAX(CASE WHEN ChannelGroup = 'All Channels' THEN OrdersTotal          END) AS adobe_ordersTotal_allChannels,

        -- ---- PAID SEARCH ----
        MAX(CASE WHEN ChannelGroup = 'Paid Search' THEN UpvByod             END) AS adobe_upvByod_paidSearch,
        MAX(CASE WHEN ChannelGroup = 'Paid Search' THEN CartstartByod        END) AS adobe_cartStartByod_paidSearch,
        MAX(CASE WHEN ChannelGroup = 'Paid Search' THEN OrdersUnassistedByod END) AS adobe_ordersUnassistedByod_paidSearch,
        MAX(CASE WHEN ChannelGroup = 'Paid Search' THEN OrdersAssistedByod   END) AS adobe_ordersAssistedByod_paidSearch,
        MAX(CASE WHEN ChannelGroup = 'Paid Search' THEN ordersTotalByod      END) AS adobe_ordersTotalByod_paidSearch,

        -- ---- ORGANIC SEARCH ----
        MAX(CASE WHEN ChannelGroup = 'Organic Search' THEN UpvByod             END) AS adobe_upvByod_organicSearch,
        MAX(CASE WHEN ChannelGroup = 'Organic Search' THEN CartstartByod        END) AS adobe_cartStartByod_organicSearch,
        MAX(CASE WHEN ChannelGroup = 'Organic Search' THEN OrdersUnassistedByod END) AS adobe_ordersUnassistedByod_organicSearch,
        MAX(CASE WHEN ChannelGroup = 'Organic Search' THEN OrdersAssistedByod   END) AS adobe_ordersAssistedByod_organicSearch,
        MAX(CASE WHEN ChannelGroup = 'Organic Search' THEN ordersTotalByod      END) AS adobe_ordersTotalByod_organicSearch,

        -- ---- DIRECT ----
        MAX(CASE WHEN ChannelGroup = 'Direct' THEN UpvByod             END) AS adobe_upvByod_direct,
        MAX(CASE WHEN ChannelGroup = 'Direct' THEN CartstartByod        END) AS adobe_cartStartByod_direct,
        MAX(CASE WHEN ChannelGroup = 'Direct' THEN OrdersUnassistedByod END) AS adobe_ordersUnassistedByod_direct,
        MAX(CASE WHEN ChannelGroup = 'Direct' THEN OrdersAssistedByod   END) AS adobe_ordersAssistedByod_direct,
        MAX(CASE WHEN ChannelGroup = 'Direct' THEN ordersTotalByod      END) AS adobe_ordersTotalByod_direct,

        -- ---- SOCIAL ----
        MAX(CASE WHEN ChannelGroup = 'Paid Social' THEN UpvByod             END) AS adobe_upvByod_social,
        MAX(CASE WHEN ChannelGroup = 'Paid Social' THEN CartstartByod        END) AS adobe_cartStartByod_social,
        MAX(CASE WHEN ChannelGroup = 'Paid Social' THEN OrdersUnassistedByod END) AS adobe_ordersUnassistedByod_social,
        MAX(CASE WHEN ChannelGroup = 'Paid Social' THEN OrdersAssistedByod   END) AS adobe_ordersAssistedByod_social,
        MAX(CASE WHEN ChannelGroup = 'Paid Social' THEN ordersTotalByod      END) AS adobe_ordersTotalByod_social,

        -- ---- PROGRAMMATIC ----
        MAX(CASE WHEN ChannelGroup = 'Programmatic' THEN UpvByod             END) AS adobe_upvByod_programmatic,
        MAX(CASE WHEN ChannelGroup = 'Programmatic' THEN CartstartByod        END) AS adobe_cartStartByod_programmatic,
        MAX(CASE WHEN ChannelGroup = 'Programmatic' THEN OrdersUnassistedByod END) AS adobe_ordersUnassistedByod_programmatic,
        MAX(CASE WHEN ChannelGroup = 'Programmatic' THEN OrdersAssistedByod   END) AS adobe_ordersAssistedByod_programmatic,
        MAX(CASE WHEN ChannelGroup = 'Programmatic' THEN ordersTotalByod      END) AS adobe_ordersTotalByod_programmatic,

        -- ---- OTHER ----
        MAX(CASE WHEN ChannelGroup = 'Other' THEN UpvByod             END) AS adobe_upvByod_other,
        MAX(CASE WHEN ChannelGroup = 'Other' THEN CartstartByod        END) AS adobe_cartStartByod_other,
        MAX(CASE WHEN ChannelGroup = 'Other' THEN OrdersUnassistedByod END) AS adobe_ordersUnassistedByod_other,
        MAX(CASE WHEN ChannelGroup = 'Other' THEN OrdersAssistedByod   END) AS adobe_ordersAssistedByod_other,
        MAX(CASE WHEN ChannelGroup = 'Other' THEN ordersTotalByod      END) AS adobe_ordersTotalByod_other,

        -- ---- CHANNEL SUM — denominator for pctUpvByodOfTotal ----
        COALESCE(MAX(CASE WHEN ChannelGroup = 'Paid Search'    THEN UpvByod END), 0)
      + COALESCE(MAX(CASE WHEN ChannelGroup = 'Organic Search' THEN UpvByod END), 0)
      + COALESCE(MAX(CASE WHEN ChannelGroup = 'Direct'         THEN UpvByod END), 0)
      + COALESCE(MAX(CASE WHEN ChannelGroup = 'Paid Social'         THEN UpvByod END), 0)
      + COALESCE(MAX(CASE WHEN ChannelGroup = 'Programmatic'   THEN UpvByod END), 0)
      + COALESCE(MAX(CASE WHEN ChannelGroup = 'Other'          THEN UpvByod END), 0)
                                                                        AS upv_byod_channel_sum,

        -- ---- INTERNAL: Postpaid + HSI for CVR computation only ----
        -- These columns do not appear in the final SELECT output.
        MAX(CASE WHEN ChannelGroup = 'All Channels' THEN UpvPostpaid       END) AS _int_upv_postpaid,
        MAX(CASE WHEN ChannelGroup = 'All Channels' THEN ordersTotalPostpaid END) AS _int_orders_postpaid,
        MAX(CASE WHEN ChannelGroup = 'All Channels' THEN UpvHsi            END) AS _int_upv_hsi,
        MAX(CASE WHEN ChannelGroup = 'All Channels' THEN ordersTotalHsi     END) AS _int_orders_hsi

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
        CASE WHEN adobe_upvFlowTotal_allChannels IS NULL OR adobe_upvFlowTotal_allChannels = 0 THEN NULL
             ELSE ROUND(adobe_upvByod_allChannels / adobe_upvFlowTotal_allChannels, 6)
        END                                                             AS adobe_pctUpvByodOfUpvFlow_allChannels,

        CASE WHEN adobe_ordersTotal_allChannels IS NULL OR adobe_ordersTotal_allChannels = 0 THEN NULL
             ELSE ROUND(adobe_ordersTotalByod_allChannels / adobe_ordersTotal_allChannels, 6)
        END                                                             AS adobe_pctOrdersByodOfOrdersTotal_allChannels,

        CASE WHEN adobe_upvByod_allChannels IS NULL OR adobe_upvByod_allChannels = 0 THEN NULL
             ELSE ROUND(adobe_ordersTotalByod_allChannels / adobe_upvByod_allChannels, 6)
        END                                                             AS adobe_cvrByod_allChannels,

        CASE WHEN adobe_upvTotal_allChannels IS NULL OR adobe_upvTotal_allChannels = 0 THEN NULL
             ELSE ROUND(adobe_ordersTotal_allChannels / adobe_upvTotal_allChannels, 6)
        END                                                             AS adobe_cvrSite_allChannels,

        -- ---- allChannels Postpaid + HSI CVR ----
        CASE WHEN _int_upv_postpaid IS NULL OR _int_upv_postpaid = 0 THEN NULL
             ELSE ROUND(_int_orders_postpaid / _int_upv_postpaid, 6)
        END                                                             AS adobe_cvrPostpaid_allChannels,

        CASE WHEN _int_upv_hsi IS NULL OR _int_upv_hsi = 0 THEN NULL
             ELSE ROUND(_int_orders_hsi / _int_upv_hsi, 6)
        END                                                             AS adobe_cvrHsi_allChannels,

        -- ---- UPV BYOD channel mix ----
        CASE WHEN upv_byod_channel_sum = 0 THEN NULL
             ELSE ROUND(adobe_upvByod_paidSearch    / upv_byod_channel_sum, 6)
        END                                                             AS adobe_pctUpvByodOfTotal_paidSearch,

        CASE WHEN upv_byod_channel_sum = 0 THEN NULL
             ELSE ROUND(adobe_upvByod_organicSearch / upv_byod_channel_sum, 6)
        END                                                             AS adobe_pctUpvByodOfTotal_organicSearch,

        CASE WHEN upv_byod_channel_sum = 0 THEN NULL
             ELSE ROUND(adobe_upvByod_direct        / upv_byod_channel_sum, 6)
        END                                                             AS adobe_pctUpvByodOfTotal_direct,

        CASE WHEN upv_byod_channel_sum = 0 THEN NULL
             ELSE ROUND(adobe_upvByod_social        / upv_byod_channel_sum, 6)
        END                                                             AS adobe_pctUpvByodOfTotal_social,

        CASE WHEN upv_byod_channel_sum = 0 THEN NULL
             ELSE ROUND(adobe_upvByod_programmatic  / upv_byod_channel_sum, 6)
        END                                                             AS adobe_pctUpvByodOfTotal_programmatic,

        CASE WHEN upv_byod_channel_sum = 0 THEN NULL
             ELSE ROUND(adobe_upvByod_other         / upv_byod_channel_sum, 6)
        END                                                             AS adobe_pctUpvByodOfTotal_other

    FROM pivoted
),

-- -----------------------------------------------------------------------
-- STEP 4: Custom week number
-- -----------------------------------------------------------------------
with_week_num AS (
    SELECT
        *,
        DATEDIFF(WEEK, DATE '2023-01-01', DATE_SUB(week_sun_to_sat, 6)) AS custom_week_num
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
        c.adobe_upvByod_allChannels,
        c.adobe_upvTotal_allChannels,
        c.adobe_upvFlowTotal_allChannels,
        c.adobe_pctUpvByodOfUpvFlow_allChannels,
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
        w.adobe_upvByod_allChannels                   AS adobe_upvByod_allChannels_wow,
        w.adobe_upvTotal_allChannels                  AS adobe_upvTotal_allChannels_wow,
        w.adobe_upvFlowTotal_allChannels              AS adobe_upvFlowTotal_allChannels_wow,
        w.adobe_pctUpvByodOfUpvFlow_allChannels      AS adobe_pctUpvByodOfUpvFlow_allChannels_wow,
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
        l.adobe_upvByod_allChannels                   AS adobe_upvByod_allChannels_ly,
        l.adobe_upvTotal_allChannels                  AS adobe_upvTotal_allChannels_ly,
        l.adobe_upvFlowTotal_allChannels              AS adobe_upvFlowTotal_allChannels_ly,
        l.adobe_pctUpvByodOfUpvFlow_allChannels      AS adobe_pctUpvByodOfUpvFlow_allChannels_ly,
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
        c.adobe_upvByod_paidSearch,
        c.adobe_pctUpvByodOfTotal_paidSearch,
        c.adobe_cartStartByod_paidSearch,
        c.adobe_ordersUnassistedByod_paidSearch,
        c.adobe_ordersAssistedByod_paidSearch,
        c.adobe_ordersTotalByod_paidSearch,
        w.adobe_upvByod_paidSearch                    AS adobe_upvByod_paidSearch_wow,
        w.adobe_pctUpvByodOfTotal_paidSearch          AS adobe_pctUpvByodOfTotal_paidSearch_wow,
        w.adobe_cartStartByod_paidSearch               AS adobe_cartStartByod_paidSearch_wow,
        w.adobe_ordersUnassistedByod_paidSearch        AS adobe_ordersUnassistedByod_paidSearch_wow,
        w.adobe_ordersAssistedByod_paidSearch          AS adobe_ordersAssistedByod_paidSearch_wow,
        w.adobe_ordersTotalByod_paidSearch             AS adobe_ordersTotalByod_paidSearch_wow,
        l.adobe_upvByod_paidSearch                    AS adobe_upvByod_paidSearch_ly,
        l.adobe_pctUpvByodOfTotal_paidSearch          AS adobe_pctUpvByodOfTotal_paidSearch_ly,
        l.adobe_cartStartByod_paidSearch               AS adobe_cartStartByod_paidSearch_ly,
        l.adobe_ordersUnassistedByod_paidSearch        AS adobe_ordersUnassistedByod_paidSearch_ly,
        l.adobe_ordersAssistedByod_paidSearch          AS adobe_ordersAssistedByod_paidSearch_ly,
        l.adobe_ordersTotalByod_paidSearch             AS adobe_ordersTotalByod_paidSearch_ly,

        -- ================================================================ ORGANIC SEARCH current
        c.adobe_upvByod_organicSearch,
        c.adobe_pctUpvByodOfTotal_organicSearch,
        c.adobe_cartStartByod_organicSearch,
        c.adobe_ordersUnassistedByod_organicSearch,
        c.adobe_ordersAssistedByod_organicSearch,
        c.adobe_ordersTotalByod_organicSearch,
        w.adobe_upvByod_organicSearch                 AS adobe_upvByod_organicSearch_wow,
        w.adobe_pctUpvByodOfTotal_organicSearch       AS adobe_pctUpvByodOfTotal_organicSearch_wow,
        w.adobe_cartStartByod_organicSearch            AS adobe_cartStartByod_organicSearch_wow,
        w.adobe_ordersUnassistedByod_organicSearch     AS adobe_ordersUnassistedByod_organicSearch_wow,
        w.adobe_ordersAssistedByod_organicSearch       AS adobe_ordersAssistedByod_organicSearch_wow,
        w.adobe_ordersTotalByod_organicSearch          AS adobe_ordersTotalByod_organicSearch_wow,
        l.adobe_upvByod_organicSearch                 AS adobe_upvByod_organicSearch_ly,
        l.adobe_pctUpvByodOfTotal_organicSearch       AS adobe_pctUpvByodOfTotal_organicSearch_ly,
        l.adobe_cartStartByod_organicSearch            AS adobe_cartStartByod_organicSearch_ly,
        l.adobe_ordersUnassistedByod_organicSearch     AS adobe_ordersUnassistedByod_organicSearch_ly,
        l.adobe_ordersAssistedByod_organicSearch       AS adobe_ordersAssistedByod_organicSearch_ly,
        l.adobe_ordersTotalByod_organicSearch          AS adobe_ordersTotalByod_organicSearch_ly,

        -- ================================================================ DIRECT current
        c.adobe_upvByod_direct,
        c.adobe_pctUpvByodOfTotal_direct,
        c.adobe_cartStartByod_direct,
        c.adobe_ordersUnassistedByod_direct,
        c.adobe_ordersAssistedByod_direct,
        c.adobe_ordersTotalByod_direct,
        w.adobe_upvByod_direct                        AS adobe_upvByod_direct_wow,
        w.adobe_pctUpvByodOfTotal_direct              AS adobe_pctUpvByodOfTotal_direct_wow,
        w.adobe_cartStartByod_direct                   AS adobe_cartStartByod_direct_wow,
        w.adobe_ordersUnassistedByod_direct            AS adobe_ordersUnassistedByod_direct_wow,
        w.adobe_ordersAssistedByod_direct              AS adobe_ordersAssistedByod_direct_wow,
        w.adobe_ordersTotalByod_direct                 AS adobe_ordersTotalByod_direct_wow,
        l.adobe_upvByod_direct                        AS adobe_upvByod_direct_ly,
        l.adobe_pctUpvByodOfTotal_direct              AS adobe_pctUpvByodOfTotal_direct_ly,
        l.adobe_cartStartByod_direct                   AS adobe_cartStartByod_direct_ly,
        l.adobe_ordersUnassistedByod_direct            AS adobe_ordersUnassistedByod_direct_ly,
        l.adobe_ordersAssistedByod_direct              AS adobe_ordersAssistedByod_direct_ly,
        l.adobe_ordersTotalByod_direct                 AS adobe_ordersTotalByod_direct_ly,

        -- ================================================================ SOCIAL current
        c.adobe_upvByod_social,
        c.adobe_pctUpvByodOfTotal_social,
        c.adobe_cartStartByod_social,
        c.adobe_ordersUnassistedByod_social,
        c.adobe_ordersAssistedByod_social,
        c.adobe_ordersTotalByod_social,
        w.adobe_upvByod_social                        AS adobe_upvByod_social_wow,
        w.adobe_pctUpvByodOfTotal_social              AS adobe_pctUpvByodOfTotal_social_wow,
        w.adobe_cartStartByod_social                   AS adobe_cartStartByod_social_wow,
        w.adobe_ordersUnassistedByod_social            AS adobe_ordersUnassistedByod_social_wow,
        w.adobe_ordersAssistedByod_social              AS adobe_ordersAssistedByod_social_wow,
        w.adobe_ordersTotalByod_social                 AS adobe_ordersTotalByod_social_wow,
        l.adobe_upvByod_social                        AS adobe_upvByod_social_ly,
        l.adobe_pctUpvByodOfTotal_social              AS adobe_pctUpvByodOfTotal_social_ly,
        l.adobe_cartStartByod_social                   AS adobe_cartStartByod_social_ly,
        l.adobe_ordersUnassistedByod_social            AS adobe_ordersUnassistedByod_social_ly,
        l.adobe_ordersAssistedByod_social              AS adobe_ordersAssistedByod_social_ly,
        l.adobe_ordersTotalByod_social                 AS adobe_ordersTotalByod_social_ly,

        -- ================================================================ PROGRAMMATIC current
        c.adobe_upvByod_programmatic,
        c.adobe_pctUpvByodOfTotal_programmatic,
        c.adobe_cartStartByod_programmatic,
        c.adobe_ordersUnassistedByod_programmatic,
        c.adobe_ordersAssistedByod_programmatic,
        c.adobe_ordersTotalByod_programmatic,
        w.adobe_upvByod_programmatic                  AS adobe_upvByod_programmatic_wow,
        w.adobe_pctUpvByodOfTotal_programmatic        AS adobe_pctUpvByodOfTotal_programmatic_wow,
        w.adobe_cartStartByod_programmatic             AS adobe_cartStartByod_programmatic_wow,
        w.adobe_ordersUnassistedByod_programmatic      AS adobe_ordersUnassistedByod_programmatic_wow,
        w.adobe_ordersAssistedByod_programmatic        AS adobe_ordersAssistedByod_programmatic_wow,
        w.adobe_ordersTotalByod_programmatic           AS adobe_ordersTotalByod_programmatic_wow,
        l.adobe_upvByod_programmatic                  AS adobe_upvByod_programmatic_ly,
        l.adobe_pctUpvByodOfTotal_programmatic        AS adobe_pctUpvByodOfTotal_programmatic_ly,
        l.adobe_cartStartByod_programmatic             AS adobe_cartStartByod_programmatic_ly,
        l.adobe_ordersUnassistedByod_programmatic      AS adobe_ordersUnassistedByod_programmatic_ly,
        l.adobe_ordersAssistedByod_programmatic        AS adobe_ordersAssistedByod_programmatic_ly,
        l.adobe_ordersTotalByod_programmatic           AS adobe_ordersTotalByod_programmatic_ly,

        -- ================================================================ OTHER current
        c.adobe_upvByod_other,
        c.adobe_pctUpvByodOfTotal_other,
        c.adobe_cartStartByod_other,
        c.adobe_ordersUnassistedByod_other,
        c.adobe_ordersAssistedByod_other,
        c.adobe_ordersTotalByod_other,
        w.adobe_upvByod_other                         AS adobe_upvByod_other_wow,
        w.adobe_pctUpvByodOfTotal_other               AS adobe_pctUpvByodOfTotal_other_wow,
        w.adobe_cartStartByod_other                    AS adobe_cartStartByod_other_wow,
        w.adobe_ordersUnassistedByod_other             AS adobe_ordersUnassistedByod_other_wow,
        w.adobe_ordersAssistedByod_other               AS adobe_ordersAssistedByod_other_wow,
        w.adobe_ordersTotalByod_other                  AS adobe_ordersTotalByod_other_wow,
        l.adobe_upvByod_other                         AS adobe_upvByod_other_ly,
        l.adobe_pctUpvByodOfTotal_other               AS adobe_pctUpvByodOfTotal_other_ly,
        l.adobe_cartStartByod_other                    AS adobe_cartStartByod_other_ly,
        l.adobe_ordersUnassistedByod_other             AS adobe_ordersUnassistedByod_other_ly,
        l.adobe_ordersAssistedByod_other               AS adobe_ordersAssistedByod_other_ly,
        l.adobe_ordersTotalByod_other                  AS adobe_ordersTotalByod_other_ly

    FROM with_week_num c
    LEFT JOIN with_week_num w ON c.week_sun_to_sat = DATE_ADD(w.week_sun_to_sat, 7)
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
        adobe_upvByod_allChannels, adobe_upvByod_allChannels_wow, adobe_upvByod_allChannels_ly,
        CASE WHEN adobe_upvByod_allChannels_wow IS NULL OR adobe_upvByod_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_upvByod_allChannels - adobe_upvByod_allChannels_wow) / adobe_upvByod_allChannels_wow, 6) END AS adobe_upvByod_allChannels_wow_pct,
        CASE WHEN adobe_upvByod_allChannels_ly  IS NULL OR adobe_upvByod_allChannels_ly  = 0 THEN NULL ELSE ROUND((adobe_upvByod_allChannels - adobe_upvByod_allChannels_ly)  / adobe_upvByod_allChannels_ly,  6) END AS adobe_upvByod_allChannels_yoy_pct,

        adobe_upvTotal_allChannels, adobe_upvTotal_allChannels_wow, adobe_upvTotal_allChannels_ly,
        CASE WHEN adobe_upvTotal_allChannels_wow IS NULL OR adobe_upvTotal_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_upvTotal_allChannels - adobe_upvTotal_allChannels_wow) / adobe_upvTotal_allChannels_wow, 6) END AS adobe_upvTotal_allChannels_wow_pct,
        CASE WHEN adobe_upvTotal_allChannels_ly  IS NULL OR adobe_upvTotal_allChannels_ly  = 0 THEN NULL ELSE ROUND((adobe_upvTotal_allChannels - adobe_upvTotal_allChannels_ly)  / adobe_upvTotal_allChannels_ly,  6) END AS adobe_upvTotal_allChannels_yoy_pct,

        adobe_upvFlowTotal_allChannels, adobe_upvFlowTotal_allChannels_wow, adobe_upvFlowTotal_allChannels_ly,
        CASE WHEN adobe_upvFlowTotal_allChannels_wow IS NULL OR adobe_upvFlowTotal_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_upvFlowTotal_allChannels - adobe_upvFlowTotal_allChannels_wow) / adobe_upvFlowTotal_allChannels_wow, 6) END AS adobe_upvFlowTotal_allChannels_wow_pct,
        CASE WHEN adobe_upvFlowTotal_allChannels_ly  IS NULL OR adobe_upvFlowTotal_allChannels_ly  = 0 THEN NULL ELSE ROUND((adobe_upvFlowTotal_allChannels - adobe_upvFlowTotal_allChannels_ly)  / adobe_upvFlowTotal_allChannels_ly,  6) END AS adobe_upvFlowTotal_allChannels_yoy_pct,

        adobe_pctUpvByodOfUpvFlow_allChannels, adobe_pctUpvByodOfUpvFlow_allChannels_wow, adobe_pctUpvByodOfUpvFlow_allChannels_ly,
        CASE WHEN adobe_pctUpvByodOfUpvFlow_allChannels_wow IS NULL OR adobe_pctUpvByodOfUpvFlow_allChannels_wow = 0 THEN NULL ELSE ROUND((adobe_pctUpvByodOfUpvFlow_allChannels - adobe_pctUpvByodOfUpvFlow_allChannels_wow) / adobe_pctUpvByodOfUpvFlow_allChannels_wow, 6) END AS adobe_pctUpvByodOfUpvFlow_allChannels_wow_pct,
        CASE WHEN adobe_pctUpvByodOfUpvFlow_allChannels_ly  IS NULL OR adobe_pctUpvByodOfUpvFlow_allChannels_ly  = 0 THEN NULL ELSE ROUND((adobe_pctUpvByodOfUpvFlow_allChannels - adobe_pctUpvByodOfUpvFlow_allChannels_ly)  / adobe_pctUpvByodOfUpvFlow_allChannels_ly,  6) END AS adobe_pctUpvByodOfUpvFlow_allChannels_yoy_pct,

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
        adobe_upvByod_paidSearch, adobe_upvByod_paidSearch_wow, adobe_upvByod_paidSearch_ly,
        CASE WHEN adobe_upvByod_paidSearch_wow IS NULL OR adobe_upvByod_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_upvByod_paidSearch - adobe_upvByod_paidSearch_wow) / adobe_upvByod_paidSearch_wow, 6) END AS adobe_upvByod_paidSearch_wow_pct,
        CASE WHEN adobe_upvByod_paidSearch_ly  IS NULL OR adobe_upvByod_paidSearch_ly  = 0 THEN NULL ELSE ROUND((adobe_upvByod_paidSearch - adobe_upvByod_paidSearch_ly)  / adobe_upvByod_paidSearch_ly,  6) END AS adobe_upvByod_paidSearch_yoy_pct,

        adobe_pctUpvByodOfTotal_paidSearch, adobe_pctUpvByodOfTotal_paidSearch_wow, adobe_pctUpvByodOfTotal_paidSearch_ly,
        CASE WHEN adobe_pctUpvByodOfTotal_paidSearch_wow IS NULL OR adobe_pctUpvByodOfTotal_paidSearch_wow = 0 THEN NULL ELSE ROUND((adobe_pctUpvByodOfTotal_paidSearch - adobe_pctUpvByodOfTotal_paidSearch_wow) / adobe_pctUpvByodOfTotal_paidSearch_wow, 6) END AS adobe_pctUpvByodOfTotal_paidSearch_wow_pct,
        CASE WHEN adobe_pctUpvByodOfTotal_paidSearch_ly  IS NULL OR adobe_pctUpvByodOfTotal_paidSearch_ly  = 0 THEN NULL ELSE ROUND((adobe_pctUpvByodOfTotal_paidSearch - adobe_pctUpvByodOfTotal_paidSearch_ly)  / adobe_pctUpvByodOfTotal_paidSearch_ly,  6) END AS adobe_pctUpvByodOfTotal_paidSearch_yoy_pct,

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
        adobe_upvByod_organicSearch, adobe_upvByod_organicSearch_wow, adobe_upvByod_organicSearch_ly,
        CASE WHEN adobe_upvByod_organicSearch_wow IS NULL OR adobe_upvByod_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_upvByod_organicSearch - adobe_upvByod_organicSearch_wow) / adobe_upvByod_organicSearch_wow, 6) END AS adobe_upvByod_organicSearch_wow_pct,
        CASE WHEN adobe_upvByod_organicSearch_ly  IS NULL OR adobe_upvByod_organicSearch_ly  = 0 THEN NULL ELSE ROUND((adobe_upvByod_organicSearch - adobe_upvByod_organicSearch_ly)  / adobe_upvByod_organicSearch_ly,  6) END AS adobe_upvByod_organicSearch_yoy_pct,

        adobe_pctUpvByodOfTotal_organicSearch, adobe_pctUpvByodOfTotal_organicSearch_wow, adobe_pctUpvByodOfTotal_organicSearch_ly,
        CASE WHEN adobe_pctUpvByodOfTotal_organicSearch_wow IS NULL OR adobe_pctUpvByodOfTotal_organicSearch_wow = 0 THEN NULL ELSE ROUND((adobe_pctUpvByodOfTotal_organicSearch - adobe_pctUpvByodOfTotal_organicSearch_wow) / adobe_pctUpvByodOfTotal_organicSearch_wow, 6) END AS adobe_pctUpvByodOfTotal_organicSearch_wow_pct,
        CASE WHEN adobe_pctUpvByodOfTotal_organicSearch_ly  IS NULL OR adobe_pctUpvByodOfTotal_organicSearch_ly  = 0 THEN NULL ELSE ROUND((adobe_pctUpvByodOfTotal_organicSearch - adobe_pctUpvByodOfTotal_organicSearch_ly)  / adobe_pctUpvByodOfTotal_organicSearch_ly,  6) END AS adobe_pctUpvByodOfTotal_organicSearch_yoy_pct,

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
        adobe_upvByod_direct, adobe_upvByod_direct_wow, adobe_upvByod_direct_ly,
        CASE WHEN adobe_upvByod_direct_wow IS NULL OR adobe_upvByod_direct_wow = 0 THEN NULL ELSE ROUND((adobe_upvByod_direct - adobe_upvByod_direct_wow) / adobe_upvByod_direct_wow, 6) END AS adobe_upvByod_direct_wow_pct,
        CASE WHEN adobe_upvByod_direct_ly  IS NULL OR adobe_upvByod_direct_ly  = 0 THEN NULL ELSE ROUND((adobe_upvByod_direct - adobe_upvByod_direct_ly)  / adobe_upvByod_direct_ly,  6) END AS adobe_upvByod_direct_yoy_pct,

        adobe_pctUpvByodOfTotal_direct, adobe_pctUpvByodOfTotal_direct_wow, adobe_pctUpvByodOfTotal_direct_ly,
        CASE WHEN adobe_pctUpvByodOfTotal_direct_wow IS NULL OR adobe_pctUpvByodOfTotal_direct_wow = 0 THEN NULL ELSE ROUND((adobe_pctUpvByodOfTotal_direct - adobe_pctUpvByodOfTotal_direct_wow) / adobe_pctUpvByodOfTotal_direct_wow, 6) END AS adobe_pctUpvByodOfTotal_direct_wow_pct,
        CASE WHEN adobe_pctUpvByodOfTotal_direct_ly  IS NULL OR adobe_pctUpvByodOfTotal_direct_ly  = 0 THEN NULL ELSE ROUND((adobe_pctUpvByodOfTotal_direct - adobe_pctUpvByodOfTotal_direct_ly)  / adobe_pctUpvByodOfTotal_direct_ly,  6) END AS adobe_pctUpvByodOfTotal_direct_yoy_pct,

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
        adobe_upvByod_social, adobe_upvByod_social_wow, adobe_upvByod_social_ly,
        CASE WHEN adobe_upvByod_social_wow IS NULL OR adobe_upvByod_social_wow = 0 THEN NULL ELSE ROUND((adobe_upvByod_social - adobe_upvByod_social_wow) / adobe_upvByod_social_wow, 6) END AS adobe_upvByod_social_wow_pct,
        CASE WHEN adobe_upvByod_social_ly  IS NULL OR adobe_upvByod_social_ly  = 0 THEN NULL ELSE ROUND((adobe_upvByod_social - adobe_upvByod_social_ly)  / adobe_upvByod_social_ly,  6) END AS adobe_upvByod_social_yoy_pct,

        adobe_pctUpvByodOfTotal_social, adobe_pctUpvByodOfTotal_social_wow, adobe_pctUpvByodOfTotal_social_ly,
        CASE WHEN adobe_pctUpvByodOfTotal_social_wow IS NULL OR adobe_pctUpvByodOfTotal_social_wow = 0 THEN NULL ELSE ROUND((adobe_pctUpvByodOfTotal_social - adobe_pctUpvByodOfTotal_social_wow) / adobe_pctUpvByodOfTotal_social_wow, 6) END AS adobe_pctUpvByodOfTotal_social_wow_pct,
        CASE WHEN adobe_pctUpvByodOfTotal_social_ly  IS NULL OR adobe_pctUpvByodOfTotal_social_ly  = 0 THEN NULL ELSE ROUND((adobe_pctUpvByodOfTotal_social - adobe_pctUpvByodOfTotal_social_ly)  / adobe_pctUpvByodOfTotal_social_ly,  6) END AS adobe_pctUpvByodOfTotal_social_yoy_pct,

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
        adobe_upvByod_programmatic, adobe_upvByod_programmatic_wow, adobe_upvByod_programmatic_ly,
        CASE WHEN adobe_upvByod_programmatic_wow IS NULL OR adobe_upvByod_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_upvByod_programmatic - adobe_upvByod_programmatic_wow) / adobe_upvByod_programmatic_wow, 6) END AS adobe_upvByod_programmatic_wow_pct,
        CASE WHEN adobe_upvByod_programmatic_ly  IS NULL OR adobe_upvByod_programmatic_ly  = 0 THEN NULL ELSE ROUND((adobe_upvByod_programmatic - adobe_upvByod_programmatic_ly)  / adobe_upvByod_programmatic_ly,  6) END AS adobe_upvByod_programmatic_yoy_pct,

        adobe_pctUpvByodOfTotal_programmatic, adobe_pctUpvByodOfTotal_programmatic_wow, adobe_pctUpvByodOfTotal_programmatic_ly,
        CASE WHEN adobe_pctUpvByodOfTotal_programmatic_wow IS NULL OR adobe_pctUpvByodOfTotal_programmatic_wow = 0 THEN NULL ELSE ROUND((adobe_pctUpvByodOfTotal_programmatic - adobe_pctUpvByodOfTotal_programmatic_wow) / adobe_pctUpvByodOfTotal_programmatic_wow, 6) END AS adobe_pctUpvByodOfTotal_programmatic_wow_pct,
        CASE WHEN adobe_pctUpvByodOfTotal_programmatic_ly  IS NULL OR adobe_pctUpvByodOfTotal_programmatic_ly  = 0 THEN NULL ELSE ROUND((adobe_pctUpvByodOfTotal_programmatic - adobe_pctUpvByodOfTotal_programmatic_ly)  / adobe_pctUpvByodOfTotal_programmatic_ly,  6) END AS adobe_pctUpvByodOfTotal_programmatic_yoy_pct,

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
        adobe_upvByod_other, adobe_upvByod_other_wow, adobe_upvByod_other_ly,
        CASE WHEN adobe_upvByod_other_wow IS NULL OR adobe_upvByod_other_wow = 0 THEN NULL ELSE ROUND((adobe_upvByod_other - adobe_upvByod_other_wow) / adobe_upvByod_other_wow, 6) END AS adobe_upvByod_other_wow_pct,
        CASE WHEN adobe_upvByod_other_ly  IS NULL OR adobe_upvByod_other_ly  = 0 THEN NULL ELSE ROUND((adobe_upvByod_other - adobe_upvByod_other_ly)  / adobe_upvByod_other_ly,  6) END AS adobe_upvByod_other_yoy_pct,

        adobe_pctUpvByodOfTotal_other, adobe_pctUpvByodOfTotal_other_wow, adobe_pctUpvByodOfTotal_other_ly,
        CASE WHEN adobe_pctUpvByodOfTotal_other_wow IS NULL OR adobe_pctUpvByodOfTotal_other_wow = 0 THEN NULL ELSE ROUND((adobe_pctUpvByodOfTotal_other - adobe_pctUpvByodOfTotal_other_wow) / adobe_pctUpvByodOfTotal_other_wow, 6) END AS adobe_pctUpvByodOfTotal_other_wow_pct,
        CASE WHEN adobe_pctUpvByodOfTotal_other_ly  IS NULL OR adobe_pctUpvByodOfTotal_other_ly  = 0 THEN NULL ELSE ROUND((adobe_pctUpvByodOfTotal_other - adobe_pctUpvByodOfTotal_other_ly)  / adobe_pctUpvByodOfTotal_other_ly,  6) END AS adobe_pctUpvByodOfTotal_other_yoy_pct,

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
            WHEN adobe_upvByod_allChannels       IS NOT NULL
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
    adobe_upvByod_allChannels,              adobe_upvByod_allChannels_wow,              adobe_upvByod_allChannels_ly,              adobe_upvByod_allChannels_wow_pct,              adobe_upvByod_allChannels_yoy_pct,
    adobe_upvTotal_allChannels,             adobe_upvTotal_allChannels_wow,             adobe_upvTotal_allChannels_ly,             adobe_upvTotal_allChannels_wow_pct,             adobe_upvTotal_allChannels_yoy_pct,
    adobe_upvFlowTotal_allChannels,         adobe_upvFlowTotal_allChannels_wow,         adobe_upvFlowTotal_allChannels_ly,         adobe_upvFlowTotal_allChannels_wow_pct,         adobe_upvFlowTotal_allChannels_yoy_pct,
    adobe_pctUpvByodOfUpvFlow_allChannels, adobe_pctUpvByodOfUpvFlow_allChannels_wow, adobe_pctUpvByodOfUpvFlow_allChannels_ly, adobe_pctUpvByodOfUpvFlow_allChannels_wow_pct, adobe_pctUpvByodOfUpvFlow_allChannels_yoy_pct,
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
    adobe_upvByod_paidSearch,               adobe_upvByod_paidSearch_wow,               adobe_upvByod_paidSearch_ly,               adobe_upvByod_paidSearch_wow_pct,               adobe_upvByod_paidSearch_yoy_pct,
    adobe_pctUpvByodOfTotal_paidSearch,     adobe_pctUpvByodOfTotal_paidSearch_wow,     adobe_pctUpvByodOfTotal_paidSearch_ly,     adobe_pctUpvByodOfTotal_paidSearch_wow_pct,     adobe_pctUpvByodOfTotal_paidSearch_yoy_pct,
    adobe_cartStartByod_paidSearch,          adobe_cartStartByod_paidSearch_wow,          adobe_cartStartByod_paidSearch_ly,          adobe_cartStartByod_paidSearch_wow_pct,          adobe_cartStartByod_paidSearch_yoy_pct,
    adobe_ordersUnassistedByod_paidSearch,   adobe_ordersUnassistedByod_paidSearch_wow,   adobe_ordersUnassistedByod_paidSearch_ly,   adobe_ordersUnassistedByod_paidSearch_wow_pct,   adobe_ordersUnassistedByod_paidSearch_yoy_pct,
    adobe_ordersAssistedByod_paidSearch,     adobe_ordersAssistedByod_paidSearch_wow,     adobe_ordersAssistedByod_paidSearch_ly,     adobe_ordersAssistedByod_paidSearch_wow_pct,     adobe_ordersAssistedByod_paidSearch_yoy_pct,
    adobe_ordersTotalByod_paidSearch,        adobe_ordersTotalByod_paidSearch_wow,        adobe_ordersTotalByod_paidSearch_ly,        adobe_ordersTotalByod_paidSearch_wow_pct,        adobe_ordersTotalByod_paidSearch_yoy_pct,

    -- ================================================================ ORGANIC SEARCH
    adobe_upvByod_organicSearch,            adobe_upvByod_organicSearch_wow,            adobe_upvByod_organicSearch_ly,            adobe_upvByod_organicSearch_wow_pct,            adobe_upvByod_organicSearch_yoy_pct,
    adobe_pctUpvByodOfTotal_organicSearch,  adobe_pctUpvByodOfTotal_organicSearch_wow,  adobe_pctUpvByodOfTotal_organicSearch_ly,  adobe_pctUpvByodOfTotal_organicSearch_wow_pct,  adobe_pctUpvByodOfTotal_organicSearch_yoy_pct,
    adobe_cartStartByod_organicSearch,       adobe_cartStartByod_organicSearch_wow,       adobe_cartStartByod_organicSearch_ly,       adobe_cartStartByod_organicSearch_wow_pct,       adobe_cartStartByod_organicSearch_yoy_pct,
    adobe_ordersUnassistedByod_organicSearch,adobe_ordersUnassistedByod_organicSearch_wow,adobe_ordersUnassistedByod_organicSearch_ly,adobe_ordersUnassistedByod_organicSearch_wow_pct,adobe_ordersUnassistedByod_organicSearch_yoy_pct,
    adobe_ordersAssistedByod_organicSearch,  adobe_ordersAssistedByod_organicSearch_wow,  adobe_ordersAssistedByod_organicSearch_ly,  adobe_ordersAssistedByod_organicSearch_wow_pct,  adobe_ordersAssistedByod_organicSearch_yoy_pct,
    adobe_ordersTotalByod_organicSearch,     adobe_ordersTotalByod_organicSearch_wow,     adobe_ordersTotalByod_organicSearch_ly,     adobe_ordersTotalByod_organicSearch_wow_pct,     adobe_ordersTotalByod_organicSearch_yoy_pct,

    -- ================================================================ DIRECT
    adobe_upvByod_direct,                   adobe_upvByod_direct_wow,                   adobe_upvByod_direct_ly,                   adobe_upvByod_direct_wow_pct,                   adobe_upvByod_direct_yoy_pct,
    adobe_pctUpvByodOfTotal_direct,         adobe_pctUpvByodOfTotal_direct_wow,         adobe_pctUpvByodOfTotal_direct_ly,         adobe_pctUpvByodOfTotal_direct_wow_pct,         adobe_pctUpvByodOfTotal_direct_yoy_pct,
    adobe_cartStartByod_direct,              adobe_cartStartByod_direct_wow,              adobe_cartStartByod_direct_ly,              adobe_cartStartByod_direct_wow_pct,              adobe_cartStartByod_direct_yoy_pct,
    adobe_ordersUnassistedByod_direct,       adobe_ordersUnassistedByod_direct_wow,       adobe_ordersUnassistedByod_direct_ly,       adobe_ordersUnassistedByod_direct_wow_pct,       adobe_ordersUnassistedByod_direct_yoy_pct,
    adobe_ordersAssistedByod_direct,         adobe_ordersAssistedByod_direct_wow,         adobe_ordersAssistedByod_direct_ly,         adobe_ordersAssistedByod_direct_wow_pct,         adobe_ordersAssistedByod_direct_yoy_pct,
    adobe_ordersTotalByod_direct,            adobe_ordersTotalByod_direct_wow,            adobe_ordersTotalByod_direct_ly,            adobe_ordersTotalByod_direct_wow_pct,            adobe_ordersTotalByod_direct_yoy_pct,

    -- ================================================================ SOCIAL
    adobe_upvByod_social,                   adobe_upvByod_social_wow,                   adobe_upvByod_social_ly,                   adobe_upvByod_social_wow_pct,                   adobe_upvByod_social_yoy_pct,
    adobe_pctUpvByodOfTotal_social,         adobe_pctUpvByodOfTotal_social_wow,         adobe_pctUpvByodOfTotal_social_ly,         adobe_pctUpvByodOfTotal_social_wow_pct,         adobe_pctUpvByodOfTotal_social_yoy_pct,
    adobe_cartStartByod_social,              adobe_cartStartByod_social_wow,              adobe_cartStartByod_social_ly,              adobe_cartStartByod_social_wow_pct,              adobe_cartStartByod_social_yoy_pct,
    adobe_ordersUnassistedByod_social,       adobe_ordersUnassistedByod_social_wow,       adobe_ordersUnassistedByod_social_ly,       adobe_ordersUnassistedByod_social_wow_pct,       adobe_ordersUnassistedByod_social_yoy_pct,
    adobe_ordersAssistedByod_social,         adobe_ordersAssistedByod_social_wow,         adobe_ordersAssistedByod_social_ly,         adobe_ordersAssistedByod_social_wow_pct,         adobe_ordersAssistedByod_social_yoy_pct,
    adobe_ordersTotalByod_social,            adobe_ordersTotalByod_social_wow,            adobe_ordersTotalByod_social_ly,            adobe_ordersTotalByod_social_wow_pct,            adobe_ordersTotalByod_social_yoy_pct,

    -- ================================================================ PROGRAMMATIC
    adobe_upvByod_programmatic,             adobe_upvByod_programmatic_wow,             adobe_upvByod_programmatic_ly,             adobe_upvByod_programmatic_wow_pct,             adobe_upvByod_programmatic_yoy_pct,
    adobe_pctUpvByodOfTotal_programmatic,   adobe_pctUpvByodOfTotal_programmatic_wow,   adobe_pctUpvByodOfTotal_programmatic_ly,   adobe_pctUpvByodOfTotal_programmatic_wow_pct,   adobe_pctUpvByodOfTotal_programmatic_yoy_pct,
    adobe_cartStartByod_programmatic,        adobe_cartStartByod_programmatic_wow,        adobe_cartStartByod_programmatic_ly,        adobe_cartStartByod_programmatic_wow_pct,        adobe_cartStartByod_programmatic_yoy_pct,
    adobe_ordersUnassistedByod_programmatic, adobe_ordersUnassistedByod_programmatic_wow, adobe_ordersUnassistedByod_programmatic_ly, adobe_ordersUnassistedByod_programmatic_wow_pct, adobe_ordersUnassistedByod_programmatic_yoy_pct,
    adobe_ordersAssistedByod_programmatic,   adobe_ordersAssistedByod_programmatic_wow,   adobe_ordersAssistedByod_programmatic_ly,   adobe_ordersAssistedByod_programmatic_wow_pct,   adobe_ordersAssistedByod_programmatic_yoy_pct,
    adobe_ordersTotalByod_programmatic,      adobe_ordersTotalByod_programmatic_wow,      adobe_ordersTotalByod_programmatic_ly,      adobe_ordersTotalByod_programmatic_wow_pct,      adobe_ordersTotalByod_programmatic_yoy_pct,

    -- ================================================================ OTHER
    adobe_upvByod_other,                    adobe_upvByod_other_wow,                    adobe_upvByod_other_ly,                    adobe_upvByod_other_wow_pct,                    adobe_upvByod_other_yoy_pct,
    adobe_pctUpvByodOfTotal_other,          adobe_pctUpvByodOfTotal_other_wow,          adobe_pctUpvByodOfTotal_other_ly,          adobe_pctUpvByodOfTotal_other_wow_pct,          adobe_pctUpvByodOfTotal_other_yoy_pct,
    adobe_cartStartByod_other,               adobe_cartStartByod_other_wow,               adobe_cartStartByod_other_ly,               adobe_cartStartByod_other_wow_pct,               adobe_cartStartByod_other_yoy_pct,
    adobe_ordersUnassistedByod_other,        adobe_ordersUnassistedByod_other_wow,        adobe_ordersUnassistedByod_other_ly,        adobe_ordersUnassistedByod_other_wow_pct,        adobe_ordersUnassistedByod_other_yoy_pct,
    adobe_ordersAssistedByod_other,          adobe_ordersAssistedByod_other_wow,          adobe_ordersAssistedByod_other_ly,          adobe_ordersAssistedByod_other_wow_pct,          adobe_ordersAssistedByod_other_yoy_pct,
    adobe_ordersTotalByod_other,             adobe_ordersTotalByod_other_wow,             adobe_ordersTotalByod_other_ly,             adobe_ordersTotalByod_other_wow_pct,             adobe_ordersTotalByod_other_yoy_pct

FROM with_max_date
; 

END;