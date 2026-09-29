
-- ###########################################################################
-- BEGIN silver/03_sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily.sql
-- ###########################################################################

-- ============================================================================
-- FILE  : 03_sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily.sql
-- LAYER : SILVER
-- PURPOSE:
--   One row per non-bounced session × page category with action metrics.
-- ============================================================================

USE CATALOG prdrzranalytics;
USE SCHEMA lab42;
-- ----------------------------------------------------------------------------
-- Manager-aligned session x page category actions: NON-BOUNCED ONLY.
-- Core manager fields are preserved; three funnel fields are MIP extensions.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily (
  sessionId             STRING,
  canonicalUserId       STRING,
  visitorId             STRING,
  sessionStartDatePst   DATE,
  weekStartDate         DATE,
  weekEndDate           DATE,

  pageCategory          STRING,
  buyFlowStep           STRING,

  pageViews             BIGINT,
  orderCount            BIGINT,
  orderCustomerType     STRING,
  vrCallEvents          BIGINT,
  vrChatEvents          BIGINT,
  storeLocatorEvents    BIGINT,
  assistedOrderEvents   BIGINT,

  hasBuyFlow            INT COMMENT 'MIP extension for Explore/funnel',
  configureEvents       BIGINT COMMENT 'MIP extension',
  checkoutStartEvents   BIGINT COMMENT 'MIP extension',

  _runId                STRING,
  silverProcessedAt     TIMESTAMP
)
USING DELTA
CLUSTER BY (sessionStartDatePst)
COMMENT 'Silver: one row per non-bounced session × page category with manager action metrics plus MIP funnel extensions.';

CREATE OR REPLACE PROCEDURE sdi_sp_mip_silver_actionsPerSessionPageCategory_daily(
  p_runId           STRING,
  p_asOfDate        DATE DEFAULT NULL,
  p_eventWindowDays INT DEFAULT 1
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  DECLARE v_asOfDate    DATE DEFAULT coalesce(
    p_asOfDate,
    to_date(from_utc_timestamp(current_timestamp(), 'America/Los_Angeles'))
  );
  DECLARE v_windowEnd   DATE DEFAULT v_asOfDate;
  DECLARE v_windowStart DATE DEFAULT date_add(v_asOfDate, -(p_eventWindowDays - 1));
  DECLARE v_processedAt TIMESTAMP DEFAULT current_timestamp();

  INSERT INTO sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily
  REPLACE WHERE sessionStartDatePst BETWEEN v_windowStart AND v_windowEnd
  WITH windowSessions AS (
    SELECT
      sessionId,
      canonicalUserId,
      visitorId,
      sessionStartDatePst,
      weekStartDate,
      weekEndDate
    FROM sdi_tbl_mip_silver_attributesPerSession_daily
    WHERE sessionStartDatePst BETWEEN v_windowStart AND v_windowEnd
  ),
  sessionHits AS (
    SELECT
      s.sessionId,
      s.canonicalUserId,
      s.visitorId,
      s.sessionStartDatePst,
      s.weekStartDate,
      s.weekEndDate,
      h.*
    FROM windowSessions s
    JOIN sdi_tbl_mip_silver_detailsPerHit_daily h
      ON h.sessionId = s.sessionId
    WHERE h.eventDate BETWEEN date_add(v_windowStart, -1) AND date_add(v_windowEnd, 2)
  )
  SELECT
    sessionId,
    max(canonicalUserId) AS canonicalUserId,
    max(visitorId) AS visitorId,
    max(sessionStartDatePst) AS sessionStartDatePst,
    max(weekStartDate) AS weekStartDate,
    max(weekEndDate) AS weekEndDate,

    coalesce(pageCategory, '(not set)') AS pageCategory,
    max_by(buyFlowStep, coalesce(buyFlowStepOrder, -1)) AS buyFlowStep,

    sum(isPageView) AS pageViews,
    sum(isOrder) AS orderCount,

    CASE
      WHEN max(CASE WHEN isOrder = 1 AND customerType = 'Prospect' THEN 1 ELSE 0 END) = 1 THEN 'Prospect'
      WHEN max(CASE WHEN isOrder = 1 AND customerType = 'Care' THEN 1 ELSE 0 END) = 1 THEN 'Care'
      WHEN max(CASE WHEN isOrder = 1 AND customerType = 'Customer' THEN 1 ELSE 0 END) = 1 THEN 'Customer'
      ELSE NULL
    END AS orderCustomerType,

    sum(isVrCall) AS vrCallEvents,
    sum(isVrChat) AS vrChatEvents,
    sum(isStoreLocator) AS storeLocatorEvents,
    sum(isAssistedOrder) AS assistedOrderEvents,

    max(isBuyFlow) AS hasBuyFlow,
    sum(isConfigure) AS configureEvents,
    sum(isCheckoutStart) AS checkoutStartEvents,

    p_runId AS _runId,
    v_processedAt AS silverProcessedAt
  FROM sessionHits
  GROUP BY sessionId, coalesce(pageCategory, '(not set)')
  HAVING
       sum(isPageView) > 0
    OR sum(isOrder) > 0
    OR sum(isVrCall) > 0
    OR sum(isVrChat) > 0
    OR sum(isStoreLocator) > 0
    OR sum(isAssistedOrder) > 0
    OR max(isBuyFlow) > 0
    OR sum(isConfigure) > 0
    OR sum(isCheckoutStart) > 0;
END;


-- ###########################################################################
-- END silver/03_sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily.sql
-- ###########################################################################

