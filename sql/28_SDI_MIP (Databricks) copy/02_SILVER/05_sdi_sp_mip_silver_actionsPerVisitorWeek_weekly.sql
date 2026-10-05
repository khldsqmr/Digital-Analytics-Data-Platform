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
--
--   This table remains ONE row per NBV visitor/week.
--
--   channelMetricMemberships is a compact overlapping helper built from
--   attributesPerSession, not from hit-level data. It stores one nested entry per
--   distinct resolved session channel touched by the visitor/week, ordered by the
--   channel's first sessionStartTsUtc. Null resolved channels are represented as
--   '(not set)'; existing values such as 'Session Refresh' are preserved rather
--   than silently excluded in Silver.
--
--   Each nested channel entry carries the primitive weekly metric ingredients
--   required for metric-specific peer-set calculations: NBV membership, session
--   count, pageViews, buy-flow/configure/checkout flags, order/acquisition/
--   assisted flags, VR call/chat, store locator and raw orderCount.
--
--   ordersBase and ordersUnassisted are carried in each channel helper entry as
--   visitor/week/channel flags derived from the same primitive order flags. This
--   keeps downstream peer-set logic aligned with the canonical weekly definitions:
--       ordersBase       = greatest(orders-ordersAcquisition,0)
--       ordersUnassisted = greatest(orders-ordersAssisted,0)
--
--   Impact-on-topline remains a Gold/App-Gold comparison calculation:
--       (slice current - slice baseline) / topline baseline
--
--   Peer-set comparison basis in Gold is always the four-week trend.
--
--   channelMetricMemberships is a cost-saving helper for Channel. It is NOT used
--   to invent crosstab pairs by crossing independent arrays; Crosstab Gold uses
--   session/page-category Silver so row/column values are known to have co-occurred.
--
-- SCHEMA CHANGE NOTE:
--   channelMetricMemberships is a new nested column. If the existing target table
--   was created from the previous schema, rebuild/drop it before the first load of
--   this procedure, or explicitly evolve the table schema. This procedure does
--   not silently ALTER it.
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
            channelMetricMemberships ARRAY<STRUCT<
                channel:STRING,
                firstTouchTs:TIMESTAMP,
                nbv:INT,
                sessionCount:BIGINT,
                pageViews:BIGINT,
                nbvBuyFlow:INT,
                nbvConfigure:INT,
                nbvCheckoutStart:INT,
                orders:INT,
                ordersAcquisition:INT,
                ordersBase:INT,
                ordersUnassisted:INT,
                ordersAssisted:INT,
                vrCalls:INT,
                vrChats:INT,
                storeLocator:INT,
                orderCount:BIGINT
            >> COMMENT 'Per-channel visitor/week metric ingredients for peer-set calculations; ordered by first resolved session-channel touch',
            silverProcessedAt TIMESTAMP
        )
        USING DELTA
        CLUSTER BY (weekStartDate)
        COMMENT 'Silver: one NBV metric ingredient row per visitor/week; 0/1 visitor flags plus additive event/session counts.';
        WITH channelAgg AS (
            -- One row per visitor/week/resolved session channel.
            -- This is the only additional aggregation required for the helper.
            -- It reads session-grain Silver 02 once; no hit-grain rescan occurs.
            SELECT
                visitorId,
                weekStartDate,
                coalesce(channel,'(not set)') AS channel,
                MIN(sessionStartTsUtc) AS firstTouchTs,
                COUNT(*) AS sessionCount,
                SUM(pageViews) AS pageViews,
                MAX(hasBuyFlow) AS nbvBuyFlow,
                MAX(hasConfigure) AS nbvConfigure,
                MAX(hasCheckoutStart) AS nbvCheckoutStart,
                MAX(hasOrder) AS orders,
                MAX(hasAcquisitionOrder) AS ordersAcquisition,
                MAX(hasAssistedOrder) AS ordersAssisted,
                MAX(hasVrCall) AS vrCalls,
                MAX(hasVrChat) AS vrChats,
                MAX(hasStoreLocator) AS storeLocator,
                SUM(orderCount) AS orderCount
            FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily
            WHERE weekStartDate BETWEEN v_weekStartFrom AND v_weekStartTo
              AND visitorId IS NOT NULL
            GROUP BY visitorId,weekStartDate,coalesce(channel,'(not set)')
        ),
        agg AS (
            -- Preserve the existing one-row-per-visitor/week metric contract.
            -- Derive weekly visitor flags from the already-collapsed channel rows.
            SELECT
                visitorId,
                weekStartDate,
                date_add(weekStartDate,6) AS weekEndDate,
                SUM(sessionCount) AS sessionCount,
                SUM(pageViews) AS pageViews,
                MAX(nbvBuyFlow) AS nbvBuyFlow,
                MAX(nbvConfigure) AS nbvConfigure,
                MAX(nbvCheckoutStart) AS nbvCheckoutStart,
                MAX(orders) AS orders,
                MAX(ordersAcquisition) AS ordersAcquisition,
                MAX(ordersAssisted) AS ordersAssisted,
                MAX(vrCalls) AS vrCalls,
                MAX(vrChats) AS vrChats,
                MAX(storeLocator) AS storeLocator,
                SUM(orderCount) AS orderCount
            FROM channelAgg
            GROUP BY visitorId,weekStartDate
        ),
        channelMembershipResolved AS (
            -- Keep channel and its qualifying metrics together in the same STRUCT.
            -- This avoids fragile parallel arrays and supports exact metric-specific
            -- channel peer-set qualification downstream.
            SELECT
                visitorId,
                weekStartDate,
                transform(
                    array_sort(
                        collect_list(
                            named_struct(
                                'sortTs',firstTouchTs,
                                'sortChannel',channel,
                                'membership',named_struct(
                                    'channel',channel,
                                    'firstTouchTs',firstTouchTs,
                                    'nbv',1,
                                    'sessionCount',sessionCount,
                                    'pageViews',pageViews,
                                    'nbvBuyFlow',nbvBuyFlow,
                                    'nbvConfigure',nbvConfigure,
                                    'nbvCheckoutStart',nbvCheckoutStart,
                                    'orders',orders,
                                    'ordersAcquisition',ordersAcquisition,
                                    'ordersBase',greatest(orders-ordersAcquisition,0),
                                    'ordersUnassisted',greatest(orders-ordersAssisted,0),
                                    'ordersAssisted',ordersAssisted,
                                    'vrCalls',vrCalls,
                                    'vrChats',vrChats,
                                    'storeLocator',storeLocator,
                                    'orderCount',orderCount
                                )
                            )
                        )
                    ),
                    x -> x.membership
                ) AS channelMetricMemberships
            FROM channelAgg
            GROUP BY visitorId,weekStartDate
        )
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly
        REPLACE WHERE weekStartDate BETWEEN v_weekStartFrom AND v_weekStartTo
        SELECT
            a.weekStartDate,
            a.weekEndDate,
            a.visitorId,
            1 AS nbv,
            a.sessionCount,
            a.pageViews,
            a.nbvBuyFlow,
            a.nbvConfigure,
            a.nbvCheckoutStart,
            a.orders,
            a.ordersAcquisition,
            greatest(a.orders-a.ordersAcquisition,0) AS ordersBase,
            greatest(a.orders-a.ordersAssisted,0) AS ordersUnassisted,
            a.ordersAssisted,
            a.vrCalls,
            a.vrChats,
            a.storeLocator,
            a.orderCount,
            c.channelMetricMemberships,
            v_processedAt AS silverProcessedAt
        FROM agg a
        INNER JOIN channelMembershipResolved c
          ON c.visitorId=a.visitorId
         AND c.weekStartDate=a.weekStartDate;
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
--     ) AS invalidMetricFlags,
--     COUNT_IF(channelMetricMemberships IS NULL OR size(channelMetricMemberships)=0) AS emptyChannelMetricMemberships,
--     COUNT_IF(
--         size(transform(channelMetricMemberships,x -> x.channel))
--         <> size(array_distinct(transform(channelMetricMemberships,x -> x.channel)))
--     ) AS duplicateChannelMemberships,
--     COUNT_IF(
--         exists(
--             channelMetricMemberships,
--             x -> x.nbv<>1
--               OR x.nbvBuyFlow NOT IN (0,1)
--               OR x.nbvConfigure NOT IN (0,1)
--               OR x.nbvCheckoutStart NOT IN (0,1)
--               OR x.orders NOT IN (0,1)
--               OR x.ordersAcquisition NOT IN (0,1)
--               OR x.ordersAssisted NOT IN (0,1)
--               OR x.vrCalls NOT IN (0,1)
--               OR x.vrChats NOT IN (0,1)
--               OR x.storeLocator NOT IN (0,1)
--         )
--     ) AS invalidNestedMetricFlags
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
-- --------------------------------------------------------------------------
-- F. VALIDATION 4: CHANNEL-METRIC HELPER RECONCILIATION
-- Rebuild the expected visitor/week/channel helper from attributesPerSession.
-- Expected:
--   missingOrExtraChannelMemberships = 0
--   metricMismatchRows = 0
-- --------------------------------------------------------------------------
-- WITH expected AS (
--     SELECT
--         visitorId,
--         weekStartDate,
--         coalesce(channel,'(not set)') AS channel,
--         MIN(sessionStartTsUtc) AS firstTouchTs,
--         COUNT(*) AS sessionCount,
--         SUM(pageViews) AS pageViews,
--         max(hasBuyFlow) AS nbvBuyFlow,
--         max(hasConfigure) AS nbvConfigure,
--         max(hasCheckoutStart) AS nbvCheckoutStart,
--         max(hasOrder) AS orders,
--         max(hasAcquisitionOrder) AS ordersAcquisition,
--         max(hasAssistedOrder) AS ordersAssisted,
--         max(hasVrCall) AS vrCalls,
--         max(hasVrChat) AS vrChats,
--         max(hasStoreLocator) AS storeLocator,
--         SUM(orderCount) AS orderCount
--     FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily
--     WHERE weekStartDate = DATE '2026-09-27'
--       AND visitorId IS NOT NULL
--     GROUP BY visitorId,weekStartDate,coalesce(channel,'(not set)')
-- ),
-- actual AS (
--     SELECT
--         a.visitorId,
--         a.weekStartDate,
--         m.channel,
--         m.firstTouchTs,
--         m.sessionCount,
--         m.pageViews,
--         m.nbvBuyFlow,
--         m.nbvConfigure,
--         m.nbvCheckoutStart,
--         m.orders,
--         m.ordersAcquisition,
--         m.ordersAssisted,
--         m.vrCalls,
--         m.vrChats,
--         m.storeLocator,
--         m.orderCount
--     FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly a
--     LATERAL VIEW explode(a.channelMetricMemberships) x AS m
--     WHERE a.weekStartDate = DATE '2026-09-27'
-- ),
-- compared AS (
--     SELECT
--         coalesce(e.visitorId,a.visitorId) AS visitorId,
--         coalesce(e.weekStartDate,a.weekStartDate) AS weekStartDate,
--         coalesce(e.channel,a.channel) AS channel,
--         CASE WHEN e.visitorId IS NULL OR a.visitorId IS NULL THEN 1 ELSE 0 END AS missingOrExtra,
--         CASE
--             WHEN e.visitorId IS NULL OR a.visitorId IS NULL THEN 0
--             WHEN NOT (
--                 e.firstTouchTs <=> a.firstTouchTs
--                 AND e.sessionCount <=> a.sessionCount
--                 AND e.pageViews <=> a.pageViews
--                 AND e.nbvBuyFlow <=> a.nbvBuyFlow
--                 AND e.nbvConfigure <=> a.nbvConfigure
--                 AND e.nbvCheckoutStart <=> a.nbvCheckoutStart
--                 AND e.orders <=> a.orders
--                 AND e.ordersAcquisition <=> a.ordersAcquisition
--                 AND e.ordersAssisted <=> a.ordersAssisted
--                 AND e.vrCalls <=> a.vrCalls
--                 AND e.vrChats <=> a.vrChats
--                 AND e.storeLocator <=> a.storeLocator
--                 AND e.orderCount <=> a.orderCount
--             ) THEN 1 ELSE 0
--         END AS metricMismatch
--     FROM expected e
--     FULL OUTER JOIN actual a
--       ON a.visitorId=e.visitorId
--      AND a.weekStartDate=e.weekStartDate
--      AND a.channel=e.channel
-- )
-- SELECT
--     SUM(missingOrExtra) AS missingOrExtraChannelMemberships,
--     SUM(metricMismatch) AS metricMismatchRows
-- FROM compared;
