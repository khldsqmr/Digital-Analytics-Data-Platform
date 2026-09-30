-- ============================================================================
-- FILE  : 10_sdi_mv_mip_serving_exploreBase_wide.sql
-- LAYER : SERVING / MATERIALIZED
-- TAB   : Explore
-- SECTION: Explore base
-- PURPOSE:
--   Flexible wide base for dynamic Explore API queries at week × session × page-category grain.
--   The backend chooses approved dimensions/metrics and performs dynamic grouping; the browser
--   receives already-aggregated results. Calendar comparison anchors are included for comparator-aware queries.
--
-- REFRESH:
--   TRIGGER ON UPDATE keeps this object independent of browser/API refreshes.
--   AT MOST EVERY INTERVAL 1 MINUTE prevents refresh storms while remaining
--   event-driven. REFRESH POLICY AUTO lets Databricks choose incremental vs full.
--
-- NAMING:
--   sdi_mv_mip_serving_<tab><Section>_<shape>
--   Refresh cadence is intentionally not encoded in the object name.
-- ============================================================================

CREATE OR REPLACE MATERIALIZED VIEW prdrzranalytics.lab42.sdi_mv_mip_serving_exploreBase_wide
COMMENT 'MIP Explore wide base. Dynamic API aggregation source; browser should not aggregate raw rows.'
CLUSTER BY (weekStartDate, platform, pageCategory)
REFRESH POLICY AUTO
TRIGGER ON UPDATE AT MOST EVERY INTERVAL 1 MINUTE
AS
SELECT
    g.weekStartDate,g.weekEndDate,
    c.fiscalYear,c.fiscalQuarter,c.fiscalQuarterLabel,c.fiscalWeekOfQuarter,c.fiscalWeekCode,c.weekLabel,c.weekEndingLabel,
    c.priorWeekStartDate,c.fourWeekAvgStartDate,c.fourWeekAvgEndDate,c.sameWeekLastYearStartDate,
    g.sessionId,g.visitorId,
    g.lobList,g.platform,g.prospectVsBase,g.authState,g.channel,g.campaign,g.entryPage,g.pageCategory,g.device,g.region,
    g.utmSource,g.utmMedium,g.utmCampaign,g.buyFlowStep,
    g.pageViews,g.orderCount,g.vrCallEvents,g.vrChatEvents,g.storeLocatorEvents,g.assistedOrderEvents,
    g.hasBuyFlow,g.configureEvents,g.checkoutStartEvents,
    CASE WHEN coalesce(g.orderCount,0)>0 THEN 1 ELSE 0 END AS hasOrder,
    1 AS sessionPageCategoryRow,
    g.goldProcessedAt
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_exploreSessionPageCategoryByWeek_wide g
LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c
  ON c.weekStartDate=g.weekStartDate;
