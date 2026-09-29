-- ============================================================================
-- FILE  : 05_sdi_sp_mip_silver_actionsPerVisitorWeek_weekly.sql
-- LAYER : SILVER
-- PURPOSE:
--   One metric row per visitor/week with 0/1 visitor flags plus additive counts.
--
-- NOTE:
--   p_asOfDate determines the Sunday-starting week containing that date.
--   During development this can intentionally be a partial week.
-- ============================================================================

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_silver_actionsPerVisitorWeek_weekly(
    IN p_asOfDate       DATE    DEFAULT NULL,
    IN p_weeksToRebuild INT     DEFAULT 1,
    IN p_validateOnly   BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Silver weekly visitor metric ingredients: one row per visitor per Sunday-Saturday reporting week.'
AS
BEGIN
    DECLARE v_asOfDate DATE DEFAULT coalesce(
        p_asOfDate,
        date_add(
            to_date(from_utc_timestamp(current_timestamp(), 'America/Los_Angeles')),
            -1
        )
    );
    DECLARE v_weekStartTo DATE;
    DECLARE v_weekStartFrom DATE;
    DECLARE v_weekEndTo DATE;
    DECLARE v_processedAt TIMESTAMP DEFAULT current_timestamp();

    IF p_weeksToRebuild IS NULL OR p_weeksToRebuild < 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'p_weeksToRebuild must be >= 1.';
    END IF;

    SET v_weekStartTo = date_add(v_asOfDate, 1 - dayofweek(v_asOfDate));
    SET v_weekStartFrom = date_add(v_weekStartTo, -7 * (p_weeksToRebuild - 1));
    SET v_weekEndTo = date_add(v_weekStartTo, 6);

    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily
        WHERE weekStartDate BETWEEN v_weekStartFrom AND v_weekStartTo
          AND visitorId IS NOT NULL
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Silver attributesPerSession returned no visitor/week rows for the requested weekly rebuild.';
    END IF;

    IF p_validateOnly THEN
        SELECT
            'VALIDATION_ONLY' AS status,
            v_weekStartFrom AS rebuildWeekStartFrom,
            v_weekStartTo AS rebuildWeekStartTo,
            v_weekEndTo AS latestWeekEndDate,
            CASE WHEN v_asOfDate < v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,
            'No Silver weekly table was created or modified.' AS message;
    ELSE
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly (
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
            silverProcessedAt  TIMESTAMP
        )
        USING DELTA
        CLUSTER BY (weekStartDate)
        COMMENT 'Silver: one metric row per visitor per week; 0/1 unique-visitor flags plus additive event/session counts.';

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
            FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily
            WHERE weekStartDate BETWEEN v_weekStartFrom AND v_weekStartTo
              AND visitorId IS NOT NULL
            GROUP BY visitorId, weekStartDate
        )
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly
        REPLACE WHERE weekStartDate BETWEEN v_weekStartFrom AND v_weekStartTo
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
            v_processedAt AS silverProcessedAt
        FROM agg;

        SELECT
            'SUCCESS' AS status,
            v_weekStartFrom AS rebuiltWeekStartFrom,
            v_weekStartTo AS rebuiltWeekStartTo,
            v_weekEndTo AS latestWeekEndDate,
            CASE WHEN v_asOfDate < v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,
            'prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly' AS targetObject;
    END IF;
END;

-- Test:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_silver_actionsPerVisitorWeek_weekly(
--   p_asOfDate => DATE '2026-09-28', p_weeksToRebuild => 1, p_validateOnly => TRUE);
-- CALL prdrzranalytics.lab42.sdi_sp_mip_silver_actionsPerVisitorWeek_weekly(
--   p_asOfDate => DATE '2026-09-28', p_weeksToRebuild => 1, p_validateOnly => FALSE);
