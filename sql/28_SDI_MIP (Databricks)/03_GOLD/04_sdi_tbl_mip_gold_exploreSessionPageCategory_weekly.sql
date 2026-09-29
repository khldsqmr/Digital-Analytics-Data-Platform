
-- ###########################################################################
-- BEGIN gold/04_sdi_tbl_mip_gold_exploreSessionPageCategory_weekly.sql
-- ###########################################################################

-- ============================================================================
-- FILE  : 04_sdi_tbl_mip_gold_exploreSessionPageCategory_weekly.sql
-- LAYER : GOLD
-- PURPOSE:
--   Flexible session × page-category serving base for Explore.
-- ============================================================================

USE CATALOG prdrzranalytics;
USE SCHEMA lab42;
-- GOLD PRINCIPLE
-- Persist comparison INGREDIENTS, not precomputed percentages.
-- WoW / 4-week / LY comparisons join the appropriate weekly aggregates at the same
-- dimensional grain; do not cross-join raw visitors across weeks.
--
-- ----------------------------------------------------------------------------
-- EXPLORE
-- Flexible serving base. No comparison percentages are persisted here.
-- Later Explore SQL filters first, groups next, then calculates comparison math.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sdi_tbl_mip_gold_exploreSessionPageCategory_weekly (
  weekStartDate         DATE,
  weekEndDate           DATE,

  sessionId             STRING,
  visitorId             STRING,

  lobList               ARRAY<STRING>,
  platform              STRING,
  prospectVsBase        STRING,
  authState             STRING,
  channel               STRING,
  campaign              STRING,
  entryPage             STRING,
  pageCategory          STRING,
  device                STRING,
  region                STRING,
  utmSource             STRING,
  utmMedium             STRING,
  utmCampaign           STRING,
  buyFlowStep           STRING,

  pageViews             BIGINT,
  orderCount            BIGINT,
  vrCallEvents          BIGINT,
  vrChatEvents          BIGINT,
  storeLocatorEvents    BIGINT,
  assistedOrderEvents   BIGINT,
  hasBuyFlow            INT,
  configureEvents       BIGINT,
  checkoutStartEvents   BIGINT,

  _runId                STRING,
  goldProcessedAt       TIMESTAMP
)
USING DELTA
CLUSTER BY (weekStartDate)
COMMENT 'Gold Explore: non-bounced session × page-category serving base for arbitrary filter/cross operations.';

CREATE OR REPLACE PROCEDURE sdi_sp_mip_gold_exploreSessionPageCategory_weekly(
  p_runId          STRING,
  p_asOfDate       DATE DEFAULT NULL,
  p_weeksToRebuild INT DEFAULT 1
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  DECLARE v_asOfDate    DATE DEFAULT coalesce(
    p_asOfDate,
    to_date(from_utc_timestamp(current_timestamp(), 'America/Los_Angeles'))
  );
  DECLARE v_weekTo      DATE DEFAULT date_add(v_asOfDate, 1 - dayofweek(v_asOfDate));
  DECLARE v_weekFrom    DATE DEFAULT date_add(v_weekTo, -7 * (p_weeksToRebuild - 1));
  DECLARE v_processedAt TIMESTAMP DEFAULT current_timestamp();

  INSERT INTO sdi_tbl_mip_gold_exploreSessionPageCategory_weekly
  REPLACE WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
  SELECT
    s.weekStartDate,
    s.weekEndDate,

    s.sessionId,
    s.visitorId,

    s.lobList,
    s.platform,
    s.prospectVsBase,
    s.authState,
    s.channel,
    CASE
      WHEN s.campaignCode IS NULL THEN '(not set)'
      WHEN s.campaignName IS NOT NULL THEN concat(s.campaignCode, ' · ', s.campaignName)
      ELSE s.campaignCode
    END AS campaign,
    s.entryPage,
    a.pageCategory,
    s.device,
    '(not available)' AS region,
    s.utmSource,
    s.utmMedium,
    s.utmCampaign,

    CASE
      WHEN a.buyFlowStep IS NOT NULL THEN a.buyFlowStep
      WHEN s.hasBuyFlow = 0 THEN 'Did not enter buy flow'
      ELSE '(buy flow - step not mapped)'
    END AS buyFlowStep,

    a.pageViews,
    a.orderCount,
    a.vrCallEvents,
    a.vrChatEvents,
    a.storeLocatorEvents,
    a.assistedOrderEvents,
    a.hasBuyFlow,
    a.configureEvents,
    a.checkoutStartEvents,

    p_runId AS _runId,
    v_processedAt AS goldProcessedAt
  FROM sdi_tbl_mip_silver_attributesPerSession_daily s
  JOIN sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily a
    ON a.sessionId = s.sessionId
  WHERE s.weekStartDate BETWEEN v_weekFrom AND v_weekTo
    AND s.visitorId IS NOT NULL;
END;

-- ----------------------------------------------------------------------------


-- ###########################################################################
-- END gold/04_sdi_tbl_mip_gold_exploreSessionPageCategory_weekly.sql
-- ###########################################################################

