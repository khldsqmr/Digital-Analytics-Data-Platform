-- ============================================================================
-- FILE  : 11_sdi_vw_mip_serving_exploreResults_wide.sql
-- LAYER : SERVING
-- TAB   : Explore
-- SECTION: Results
-- PURPOSE:
--   Flexible API-facing Explore base at week × session × page category grain.
--   API applies approved dynamic dimension/metric grouping on top of this view.
--
-- IMPORTANT:
--   lobList remains an ARRAY so filtering can use array_contains(lobList, :lob)
--   without exploding and duplicating session/page-category measures.
--
-- NAMING:
--   Object name expresses app tab/section + physical shape only.
--   Current reporting grain is week-based, but cadence/grain is intentionally
--   not encoded in the serving-view object name.
-- ============================================================================

CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_mip_serving_exploreResults_wide AS
SELECT
    g.weekStartDate,
    g.weekEndDate,
    c.fiscalQuarterLabel,
    c.fiscalWeekCode,
    c.weekLabel,
    c.weekEndingLabel,

    g.sessionId,
    g.visitorId,

    g.lobList,
    g.platform,
    g.prospectVsBase,
    g.authState,
    g.channel,
    g.campaign,
    g.entryPage,
    g.pageCategory,
    g.device,
    g.region,
    g.utmSource,
    g.utmMedium,
    g.utmCampaign,
    g.buyFlowStep,

    g.pageViews,
    g.orderCount,
    g.vrCallEvents,
    g.vrChatEvents,
    g.storeLocatorEvents,
    g.assistedOrderEvents,
    g.hasBuyFlow,
    g.configureEvents,
    g.checkoutStartEvents,

    g.goldProcessedAt

FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_exploreSessionPageCategoryByWeek_wide g

LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c
  ON c.weekStartDate = g.weekStartDate;
