-- ============================================================================
-- FILE  : 03_sdi_sp_mip_silver_actionsPerSessionPageCategory_daily.sql
-- LAYER : SILVER
-- PURPOSE:
--   One row per NBV session x page category with measurable action metrics.
--
-- PERFORMANCE:
--   - NBV session set comes from attributesPerSession.
--   - No arbitrary event-date widening.
--   - detailsPerHit is pruned directly by persisted sessionStartDatePst.
-- ============================================================================
CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_silver_actionsPerSessionPageCategory_daily(
    IN p_asOfDate DATE DEFAULT NULL,
    IN p_eventWindowDays INT DEFAULT 1,
    IN p_validateOnly BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Silver NBV session x page-category action layer. Zero-signal rows are excluded.'
AS
BEGIN
    DECLARE v_asOfDate DATE DEFAULT coalesce(
        p_asOfDate,
        date_add(to_date(from_utc_timestamp(current_timestamp(),'America/Los_Angeles')),-1)
    );
    DECLARE v_windowEnd DATE DEFAULT v_asOfDate;
    DECLARE v_windowStart DATE DEFAULT date_add(v_asOfDate,-(p_eventWindowDays-1));
    DECLARE v_processedAt TIMESTAMP DEFAULT current_timestamp();
    IF p_eventWindowDays IS NULL OR p_eventWindowDays<1 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='p_eventWindowDays must be >= 1.';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily
        WHERE sessionStartDatePst BETWEEN v_windowStart AND v_windowEnd
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Silver attributesPerSession returned no NBV sessions for the requested window.';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily
        WHERE sessionStartDatePst BETWEEN v_windowStart AND v_windowEnd
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Silver detailsPerHit returned no session hits for the requested window.';
    END IF;
    IF p_validateOnly THEN
        SELECT
            'VALIDATION_ONLY' AS status,
            v_windowStart AS requestedSessionStartDate,
            v_windowEnd AS requestedSessionEndDate,
            'attributesPerSession + detailsPerHit' AS sourceObjects,
            'No table was created or modified.' AS message;
    ELSE
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily (
            sessionId STRING,
            canonicalUserId STRING,
            visitorId STRING,
            sessionStartDatePst DATE,
            weekStartDate DATE,
            weekEndDate DATE,
            pageCategory STRING,
            buyFlowStep STRING,
            pageViews BIGINT,
            orderCount BIGINT,
            orderCustomerType STRING,
            vrCallEvents BIGINT,
            vrChatEvents BIGINT,
            storeLocatorEvents BIGINT,
            assistedOrderEvents BIGINT,
            hasBuyFlow INT COMMENT 'MIP extension for Explore/funnel',
            configureEvents BIGINT COMMENT 'MIP extension',
            checkoutStartEvents BIGINT COMMENT 'MIP extension',
            silverProcessedAt TIMESTAMP
        )
        USING DELTA
        CLUSTER BY (sessionStartDatePst,pageCategory)
        COMMENT 'Silver: one row per NBV session x page category with at least one measurable MIP signal.';
        WITH windowSessions AS (
            SELECT
                sessionId,
                canonicalUserId,
                visitorId,
                sessionStartDatePst,
                weekStartDate,
                weekEndDate
            FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily
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
                h.eventTimestampUtc,
                coalesce(h.pageCategory,'(not set)') AS pageCategory,
                h.buyFlowStep,
                h.buyFlowStepOrder,
                h.customerType,
                h.isPageView,
                h.isOrder,
                h.isVrCall,
                h.isVrChat,
                h.isStoreLocator,
                h.isAssistedOrder,
                h.isBuyFlow,
                h.isConfigure,
                h.isCheckoutStart
            FROM windowSessions s
            INNER JOIN prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily h
              ON h.sessionId=s.sessionId
             AND h.sessionStartDatePst=s.sessionStartDatePst
            WHERE h.sessionStartDatePst BETWEEN v_windowStart AND v_windowEnd
        )
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily
        REPLACE WHERE sessionStartDatePst BETWEEN v_windowStart AND v_windowEnd
        SELECT
            sessionId,
            max(canonicalUserId) AS canonicalUserId,
            max(visitorId) AS visitorId,
            max(sessionStartDatePst) AS sessionStartDatePst,
            max(weekStartDate) AS weekStartDate,
            max(weekEndDate) AS weekEndDate,
            pageCategory,
            max_by(
                buyFlowStep,
                struct(coalesce(buyFlowStepOrder,-1),eventTimestampUtc,coalesce(buyFlowStep,''))
            ) FILTER (WHERE buyFlowStep IS NOT NULL) AS buyFlowStep,
            SUM(CAST(isPageView AS BIGINT)) AS pageViews,
            SUM(CAST(isOrder AS BIGINT)) AS orderCount,
            CASE
                WHEN max(CASE WHEN isOrder=1 AND customerType='Prospect' THEN 1 ELSE 0 END)=1 THEN 'Prospect'
                WHEN max(CASE WHEN isOrder=1 AND customerType='Care' THEN 1 ELSE 0 END)=1 THEN 'Care'
                WHEN max(CASE WHEN isOrder=1 AND customerType='Customer' THEN 1 ELSE 0 END)=1 THEN 'Customer'
                ELSE NULL
            END AS orderCustomerType,
            SUM(CAST(isVrCall AS BIGINT)) AS vrCallEvents,
            SUM(CAST(isVrChat AS BIGINT)) AS vrChatEvents,
            SUM(CAST(isStoreLocator AS BIGINT)) AS storeLocatorEvents,
            SUM(CAST(isAssistedOrder AS BIGINT)) AS assistedOrderEvents,
            max(isBuyFlow) AS hasBuyFlow,
            SUM(CAST(isConfigure AS BIGINT)) AS configureEvents,
            SUM(CAST(isCheckoutStart AS BIGINT)) AS checkoutStartEvents,
            v_processedAt AS silverProcessedAt
        FROM sessionHits
        GROUP BY sessionId,pageCategory
        HAVING
               SUM(CAST(isPageView AS BIGINT))>0
            OR SUM(CAST(isOrder AS BIGINT))>0
            OR SUM(CAST(isVrCall AS BIGINT))>0
            OR SUM(CAST(isVrChat AS BIGINT))>0
            OR SUM(CAST(isStoreLocator AS BIGINT))>0
            OR SUM(CAST(isAssistedOrder AS BIGINT))>0
            OR max(isBuyFlow)>0
            OR SUM(CAST(isConfigure AS BIGINT))>0
            OR SUM(CAST(isCheckoutStart AS BIGINT))>0;
        SELECT
            'SUCCESS' AS status,
            v_windowStart AS loadedSessionStartDate,
            v_windowEnd AS loadedSessionEndDate,
            'prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily' AS targetObject;
    END IF;
END;
-- ============================================================================
-- DEVELOPMENT / TEST EXAMPLES
-- Run these statements separately after deploying the procedure.
-- ============================================================================
-- --------------------------------------------------------------------------
-- A. PREFLIGHT ONLY
-- --------------------------------------------------------------------------
-- CALL prdrzranalytics.lab42.sdi_sp_mip_silver_actionsPerSessionPageCategory_daily(
--     p_asOfDate        => DATE '2026-09-28',
--     p_eventWindowDays => 1,
--     p_validateOnly    => TRUE
-- );
-- --------------------------------------------------------------------------
-- B. EXECUTE / REBUILD ONE SESSION-START DAY
-- --------------------------------------------------------------------------
-- CALL prdrzranalytics.lab42.sdi_sp_mip_silver_actionsPerSessionPageCategory_daily(
--     p_asOfDate        => DATE '2026-09-28',
--     p_eventWindowDays => 1,
--     p_validateOnly    => FALSE
-- );
-- --------------------------------------------------------------------------
-- C. VALIDATION 1: GRAIN UNIQUENESS
-- Expected: no rows.
-- --------------------------------------------------------------------------
-- SELECT
--     sessionId,
--     pageCategory,
--     COUNT(*) AS rowCount
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily
-- WHERE sessionStartDatePst = DATE '2026-09-28'
-- GROUP BY sessionId,pageCategory
-- HAVING COUNT(*) > 1
-- ORDER BY rowCount DESC
-- LIMIT 100;
-- --------------------------------------------------------------------------
-- D. VALIDATION 2: ZERO-SIGNAL FILTER
-- Expected: zero rows returned.
-- --------------------------------------------------------------------------
-- SELECT *
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily
-- WHERE sessionStartDatePst = DATE '2026-09-28'
--   AND pageViews = 0
--   AND orderCount = 0
--   AND vrCallEvents = 0
--   AND vrChatEvents = 0
--   AND storeLocatorEvents = 0
--   AND assistedOrderEvents = 0
--   AND hasBuyFlow = 0
--   AND configureEvents = 0
--   AND checkoutStartEvents = 0
-- LIMIT 100;
-- --------------------------------------------------------------------------
-- E. VALIDATION 3: ADDITIVE RECONCILIATION TO SESSION SILVER
-- Expected:
--   pageViewDiff = 0
--   orderCountDiff = 0
-- --------------------------------------------------------------------------
-- SELECT
--     a.sessionStartDatePst,
--     SUM(a.pageViews) AS actionPageViews,
--     s.sessionPageViews,
--     SUM(a.pageViews) - s.sessionPageViews AS pageViewDiff,
--     SUM(a.orderCount) AS actionOrderCount,
--     s.sessionOrderCount,
--     SUM(a.orderCount) - s.sessionOrderCount AS orderCountDiff
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily a
-- CROSS JOIN (
--     SELECT
--         SUM(pageViews) AS sessionPageViews,
--         SUM(orderCount) AS sessionOrderCount
--     FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily
--     WHERE sessionStartDatePst = DATE '2026-09-28'
-- ) s
-- WHERE a.sessionStartDatePst = DATE '2026-09-28'
-- GROUP BY a.sessionStartDatePst,s.sessionPageViews,s.sessionOrderCount;
