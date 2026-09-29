
-- ###########################################################################
-- BEGIN silver/05_sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly.sql
-- ###########################################################################

-- ============================================================================
-- FILE  : 05_sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly.sql
-- LAYER : SILVER
-- PURPOSE:
--   One metric row per visitor/week with 0/1 visitor flags plus additive counts.
-- ============================================================================

USE CATALOG prdrzranalytics;
USE SCHEMA lab42;
-- ----------------------------------------------------------------------------
-- One row per visitor × week: metric ingredients
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly (
  weekStartDate      DATE,
  weekEndDate        DATE,
  visitorId          STRING,

  nbv                INT,
  sessionCount       BIGINT,
  pageViews          BIGINT,

  nbvBuyFlow         INT,
  nbvConfigure       INT,
  nbvCheckoutStart   INT,

  orders             INT,
  ordersAcquisition  INT,
  ordersBase         INT,
  ordersUnassisted   INT,
  ordersAssisted     INT,

  vrCalls            INT,
  vrChats            INT,
  storeLocator       INT,
  orderCount         BIGINT,

  _runId             STRING,
  silverProcessedAt  TIMESTAMP
)
USING DELTA
CLUSTER BY (weekStartDate)
COMMENT 'Silver: one metric row per visitor per week; 0/1 unique-visitor flags plus additive event/session counts.';

CREATE OR REPLACE PROCEDURE sdi_sp_mip_silver_actionsPerVisitorWeek_weekly(
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

  IF p_weeksToRebuild < 1 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'p_weeksToRebuild must be >= 1';
  END IF;

  INSERT INTO sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly
  REPLACE WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
  WITH agg AS (
    SELECT
      visitorId,
      weekStartDate,
      max(weekEndDate) AS weekEndDate,

      count(*) AS sessionCount,
      sum(pageViews) AS pageViews,

      max(hasBuyFlow) AS nbvBuyFlow,
      max(hasConfigure) AS nbvConfigure,
      max(hasCheckoutStart) AS nbvCheckoutStart,

      max(hasOrder) AS orders,
      max(hasAcquisitionOrder) AS ordersAcquisition,
      max(hasAssistedOrder) AS ordersAssisted,

      max(hasVrCall) AS vrCalls,
      max(hasVrChat) AS vrChats,
      max(hasStoreLocator) AS storeLocator,
      sum(orderCount) AS orderCount
    FROM sdi_tbl_mip_silver_attributesPerSession_daily
    WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
      AND visitorId IS NOT NULL
    GROUP BY visitorId, weekStartDate
  )
  SELECT
    weekStartDate,
    weekEndDate,
    visitorId,

    1 AS nbv,
    sessionCount,
    pageViews,

    nbvBuyFlow,
    nbvConfigure,
    nbvCheckoutStart,

    orders,
    ordersAcquisition,
    greatest(orders - ordersAcquisition, 0) AS ordersBase,
    greatest(orders - ordersAssisted, 0) AS ordersUnassisted,
    ordersAssisted,

    vrCalls,
    vrChats,
    storeLocator,
    orderCount,

    p_runId AS _runId,
    v_processedAt AS silverProcessedAt
  FROM agg;
END;


-- ###########################################################################
-- END silver/05_sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly.sql
-- ###########################################################################

