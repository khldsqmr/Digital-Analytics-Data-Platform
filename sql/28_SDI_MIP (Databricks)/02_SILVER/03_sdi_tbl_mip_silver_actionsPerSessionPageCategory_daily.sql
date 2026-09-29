-- ============================================================================
-- FILE  : 03_sdi_sp_mip_silver_actionsPerSessionPageCategory_daily.sql
-- LAYER : SILVER
-- PURPOSE:
--   One row per non-bounced session × page category with action metrics.
-- ============================================================================

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_silver_actionsPerSessionPageCategory_daily(
    IN p_asOfDate        DATE    DEFAULT NULL,
    IN p_eventWindowDays INT     DEFAULT 1,
    IN p_validateOnly    BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Silver session x page-category action layer for non-bounced sessions.'
AS
BEGIN
    DECLARE v_asOfDate DATE DEFAULT coalesce(
        p_asOfDate,
        date_add(
            to_date(from_utc_timestamp(current_timestamp(), 'America/Los_Angeles')),
            -1
        )
    );
    DECLARE v_windowStart DATE;
    DECLARE v_windowEnd DATE;
    DECLARE v_processedAt TIMESTAMP DEFAULT current_timestamp();

    IF p_eventWindowDays IS NULL OR p_eventWindowDays < 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'p_eventWindowDays must be >= 1.';
    END IF;

    SET v_windowEnd = v_asOfDate;
    SET v_windowStart = date_add(v_asOfDate, -(p_eventWindowDays - 1));

    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily
        WHERE sessionStartDatePst BETWEEN v_windowStart AND v_windowEnd
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Silver attributesPerSession returned no rows for the requested window.';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily
        WHERE eventDate BETWEEN date_add(v_windowStart, -1) AND date_add(v_windowEnd, 2)
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Silver detailsPerHit returned no rows for the page-category action lookup window.';
    END IF;

    IF p_validateOnly THEN
        SELECT
            'VALIDATION_ONLY' AS status,
            v_windowStart AS requestedSessionStartDate,
            v_windowEnd AS requestedSessionEndDate,
            date_add(v_windowStart, -1) AS hitLookupStart,
            date_add(v_windowEnd, 2) AS hitLookupEnd,
            'No Silver table was created or modified.' AS message;
    ELSE
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily (
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
            silverProcessedAt     TIMESTAMP
        )
        USING DELTA
        CLUSTER BY (sessionStartDatePst)
        COMMENT 'Silver: one row per non-bounced session × page category with manager action metrics plus MIP funnel extensions.';

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
                h.*
            FROM windowSessions s
            JOIN prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily h
              ON h.sessionId = s.sessionId
            WHERE h.eventDate BETWEEN date_add(v_windowStart, -1) AND date_add(v_windowEnd, 2)
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
            coalesce(pageCategory, '(not set)') AS pageCategory,
            max_by(
                buyFlowStep,
                struct(coalesce(buyFlowStepOrder, -1), eventTimestampUtc, coalesce(buyFlowStep, ''))
            ) AS buyFlowStep,
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

        SELECT
            'SUCCESS' AS status,
            v_windowStart AS loadedSessionStartDate,
            v_windowEnd AS loadedSessionEndDate,
            'prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily' AS targetObject;
    END IF;
END;

-- Test:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_silver_actionsPerSessionPageCategory_daily(
--   p_asOfDate => DATE '2026-09-28', p_eventWindowDays => 1, p_validateOnly => TRUE);
-- CALL prdrzranalytics.lab42.sdi_sp_mip_silver_actionsPerSessionPageCategory_daily(
--   p_asOfDate => DATE '2026-09-28', p_eventWindowDays => 1, p_validateOnly => FALSE);
