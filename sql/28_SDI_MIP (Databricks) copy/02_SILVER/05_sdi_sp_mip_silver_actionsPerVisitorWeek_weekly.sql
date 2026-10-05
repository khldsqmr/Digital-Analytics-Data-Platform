-- ============================================================================

-- FILE  : 05_sdi_sp_mip_silver_actionsPerVisitorWeek_weekly.sql

-- LAYER : SILVER

-- PURPOSE:

--   One metric ingredient row per NBV visitor/week.

--

-- NOTE:

--   Unique-visitor metrics are 0/1 flags at visitor/week grain.

--   Page Views, Order Count and Session Count remain additive counts.
--
-- PREFLIGHT / VALIDATION CONTRACT:
--   p_validateOnly=TRUE verifies that attributesPerSession contains every
--   requested reporting week and writes nothing.
--
-- PEER / IMPACT CONTRACT:
--   This table stores reusable metric ingredients only. Impact-on-topline is a
--   Gold/App-Gold comparison calculation. True peer membership will later use a
--   separate overlapping-membership Silver; do not alter this 1-row-per-visitor
--   weekly metric contract for peer-set support.

-- ============================================================================

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_silver_actionsPerVisitorWeek_weekly(

    IN p_asOfDate DATE DEFAULT NULL,

    IN p_weeksToRebuild INT DEFAULT 1,

    IN p_validateOnly BOOLEAN DEFAULT FALSE

)

LANGUAGE SQL

SQL SECURITY INVOKER

MODIFIES SQL DATA

COMMENT 'Silver weekly NBV visitor metric ingredients: one row per visitor per Sunday-Saturday reporting week.'

AS

BEGIN

    DECLARE v_asOfDate DATE DEFAULT coalesce(

        p_asOfDate,

        date_add(to_date(from_utc_timestamp(current_timestamp(),'America/Los_Angeles')),-1)

    );

    DECLARE v_weekStartTo DATE;

    DECLARE v_weekStartFrom DATE;

    DECLARE v_weekEndTo DATE;

    DECLARE v_processedAt TIMESTAMP DEFAULT current_timestamp();

    DECLARE v_sourceWeekCount BIGINT DEFAULT 0;

    IF p_weeksToRebuild IS NULL OR p_weeksToRebuild<1 THEN

        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='p_weeksToRebuild must be >= 1.';

    END IF;

    SET v_weekStartTo=date_add(v_asOfDate,1-dayofweek(v_asOfDate));

    SET v_weekStartFrom=date_add(v_weekStartTo,-7*(p_weeksToRebuild-1));

    SET v_weekEndTo=date_add(v_weekStartTo,6);

    SET v_sourceWeekCount=(

        SELECT COUNT(DISTINCT weekStartDate)

        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily

        WHERE weekStartDate BETWEEN v_weekStartFrom AND v_weekStartTo

          AND visitorId IS NOT NULL

    );

    IF v_sourceWeekCount<>p_weeksToRebuild THEN

        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Silver attributesPerSession does not contain every requested reporting week.';

    END IF;

    IF p_validateOnly THEN

        SELECT

            'VALIDATION_ONLY' AS status,

            v_weekStartFrom AS rebuildWeekStartFrom,

            v_weekStartTo AS rebuildWeekStartTo,

            v_weekEndTo AS latestWeekEndDate,

            v_sourceWeekCount AS sourceWeekCount,

            CASE WHEN v_asOfDate<v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,

            'No Silver weekly table was created or modified.' AS message;

    ELSE

        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly (

            weekStartDate DATE,

            weekEndDate DATE,

            visitorId STRING,

            nbv INT,

            sessionCount BIGINT,

            pageViews BIGINT,

            nbvBuyFlow INT,

            nbvConfigure INT,

            nbvCheckoutStart INT,

            orders INT,

            ordersAcquisition INT,

            ordersBase INT,

            ordersUnassisted INT,

            ordersAssisted INT,

            vrCalls INT,

            vrChats INT,

            storeLocator INT,

            orderCount BIGINT,

            silverProcessedAt TIMESTAMP

        )

        USING DELTA

        CLUSTER BY (weekStartDate)

        COMMENT 'Silver: one NBV metric ingredient row per visitor/week; 0/1 visitor flags plus additive event/session counts.';

        WITH agg AS (

            SELECT

                visitorId,

                weekStartDate,

                max(weekEndDate) AS weekEndDate,

                count(*) AS sessionCount,

                SUM(pageViews) AS pageViews,

                max(hasBuyFlow) AS nbvBuyFlow,

                max(hasConfigure) AS nbvConfigure,

                max(hasCheckoutStart) AS nbvCheckoutStart,

                max(hasOrder) AS orders,

                max(hasAcquisitionOrder) AS ordersAcquisition,

                max(hasAssistedOrder) AS ordersAssisted,

                max(hasVrCall) AS vrCalls,

                max(hasVrChat) AS vrChats,

                max(hasStoreLocator) AS storeLocator,

                SUM(orderCount) AS orderCount

            FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily

            WHERE weekStartDate BETWEEN v_weekStartFrom AND v_weekStartTo

              AND visitorId IS NOT NULL

            GROUP BY visitorId,weekStartDate

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

            greatest(orders-ordersAcquisition,0) AS ordersBase,

            greatest(orders-ordersAssisted,0) AS ordersUnassisted,

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

            CASE WHEN v_asOfDate<v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,

            'prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly' AS targetObject;

    END IF;

END;

-- ============================================================================

-- DEVELOPMENT / TEST EXAMPLES

-- Run these statements separately after deploying the procedure.

-- ============================================================================

-- --------------------------------------------------------------------------

-- A. PREFLIGHT ONLY

-- --------------------------------------------------------------------------

-- CALL prdrzranalytics.lab42.sdi_sp_mip_silver_actionsPerVisitorWeek_weekly(

--     p_asOfDate       => DATE '2026-09-28',

--     p_weeksToRebuild => 1,

--     p_validateOnly   => TRUE

-- );

-- --------------------------------------------------------------------------

-- B. EXECUTE / REBUILD ONE REPORTING WEEK

-- --------------------------------------------------------------------------

-- CALL prdrzranalytics.lab42.sdi_sp_mip_silver_actionsPerVisitorWeek_weekly(

--     p_asOfDate       => DATE '2026-09-28',

--     p_weeksToRebuild => 1,

--     p_validateOnly   => FALSE

-- );

-- --------------------------------------------------------------------------

-- C. VALIDATION 1: VISITOR/WEEK GRAIN + FLAG CONTRACT

-- Expected:

--   duplicateRows = 0

--   invalidNbvFlags = 0

--   invalidMetricFlags = 0

-- --------------------------------------------------------------------------

-- WITH grain AS (

--     SELECT weekStartDate,visitorId,COUNT(*) AS rowCount

--     FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly

--     WHERE weekStartDate = DATE '2026-09-27'

--     GROUP BY weekStartDate,visitorId

-- )

-- SELECT

--     (SELECT COUNT(*) FROM grain WHERE rowCount>1) AS duplicateRows,

--     COUNT_IF(nbv<>1) AS invalidNbvFlags,

--     COUNT_IF(

--         nbvBuyFlow NOT IN (0,1)

--         OR nbvConfigure NOT IN (0,1)

--         OR nbvCheckoutStart NOT IN (0,1)

--         OR orders NOT IN (0,1)

--         OR ordersAcquisition NOT IN (0,1)

--         OR ordersBase NOT IN (0,1)

--         OR ordersUnassisted NOT IN (0,1)

--         OR ordersAssisted NOT IN (0,1)

--         OR vrCalls NOT IN (0,1)

--         OR vrChats NOT IN (0,1)

--         OR storeLocator NOT IN (0,1)

--     ) AS invalidMetricFlags

-- FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly

-- WHERE weekStartDate = DATE '2026-09-27';

-- --------------------------------------------------------------------------

-- D. VALIDATION 2: LOGICAL FLAG RELATIONSHIPS

-- Expected: all counts below = 0.

-- --------------------------------------------------------------------------

-- SELECT

--     COUNT_IF(ordersAcquisition>orders) AS acquisitionGreaterThanOrders,

--     COUNT_IF(ordersAssisted>orders) AS assistedGreaterThanOrders,

--     COUNT_IF(ordersBase>orders) AS baseGreaterThanOrders,

--     COUNT_IF(ordersUnassisted>orders) AS unassistedGreaterThanOrders,

--     COUNT_IF(orderCount<orders) AS rawOrderCountBelowOrderVisitorFlag

-- FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly

-- WHERE weekStartDate = DATE '2026-09-27';

-- --------------------------------------------------------------------------

-- E. VALIDATION 3: WEEKLY ADDITIVE RECONCILIATION TO SESSION SILVER

-- Expected:

--   sessionCountDiff = 0

--   pageViewDiff = 0

--   orderCountDiff = 0

-- --------------------------------------------------------------------------

-- WITH expected AS (

--     SELECT

--         weekStartDate,

--         COUNT(*) AS sessionCount,

--         SUM(pageViews) AS pageViews,

--         SUM(orderCount) AS orderCount

--     FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily

--     WHERE weekStartDate = DATE '2026-09-27'

--       AND visitorId IS NOT NULL

--     GROUP BY weekStartDate

-- ),

-- actual AS (

--     SELECT

--         weekStartDate,

--         SUM(sessionCount) AS sessionCount,

--         SUM(pageViews) AS pageViews,

--         SUM(orderCount) AS orderCount

--     FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly

--     WHERE weekStartDate = DATE '2026-09-27'

--     GROUP BY weekStartDate

-- )

-- SELECT

--     a.weekStartDate,

--     a.sessionCount-e.sessionCount AS sessionCountDiff,

--     a.pageViews-e.pageViews AS pageViewDiff,

--     a.orderCount-e.orderCount AS orderCountDiff

-- FROM actual a

-- JOIN expected e USING (weekStartDate);
