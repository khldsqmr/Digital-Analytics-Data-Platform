-- ============================================================================
-- FILE  : 03_sdi_sp_mip_silver_actionsPerSessionPageCategory_daily.sql
-- LAYER : SILVER
-- PURPOSE:
--   One row per non-bounced session x page category with action metrics.
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
            to_date(
                from_utc_timestamp(
                    current_timestamp(),
                    'America/Los_Angeles'
                )
            ),
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

    SET v_windowStart = date_add(
        v_asOfDate,
        -(p_eventWindowDays - 1)
    );

    -- Validate that eligible session-level Silver records exist.
    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily
        WHERE sessionStartDatePst BETWEEN v_windowStart AND v_windowEnd
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Silver attributesPerSession returned no rows for the requested window.';
    END IF;

    -- Validate that hit-level Silver records exist.
    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily
        WHERE eventDate
              BETWEEN date_add(v_windowStart, -1)
                  AND date_add(v_windowEnd, 2)
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
        COMMENT 'Silver: one row per non-bounced session x page category with manager action metrics plus MIP funnel extensions.';

        WITH windowSessions AS (
            SELECT
                a.sessionId,
                a.canonicalUserId,
                a.visitorId,
                a.sessionStartDatePst,
                a.weekStartDate,
                a.weekEndDate
            FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily AS a
            WHERE a.sessionStartDatePst
                  BETWEEN v_windowStart AND v_windowEnd
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
                h.pageCategory,
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

            FROM windowSessions AS s

            INNER JOIN prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily AS h
                ON h.sessionId = s.sessionId

            WHERE h.eventDate
                  BETWEEN date_add(v_windowStart, -1)
                      AND date_add(v_windowEnd, 2)
        ),

        aggregatedActions AS (
            SELECT
                sh.sessionId,

                max(
                    sh.canonicalUserId
                ) AS canonicalUserId,

                max(
                    sh.visitorId
                ) AS visitorId,

                max(
                    sh.sessionStartDatePst
                ) AS sessionStartDatePst,

                max(
                    sh.weekStartDate
                ) AS weekStartDate,

                max(
                    sh.weekEndDate
                ) AS weekEndDate,

                coalesce(
                    nullif(trim(sh.pageCategory), ''),
                    '(not set)'
                ) AS pageCategory,

                max_by(
                    sh.buyFlowStep,
                    struct(
                        coalesce(sh.buyFlowStepOrder, -1),
                        sh.eventTimestampUtc,
                        coalesce(sh.buyFlowStep, '')
                    )
                ) FILTER (
                    WHERE sh.buyFlowStep IS NOT NULL
                ) AS buyFlowStep,

                sum(
                    coalesce(sh.isPageView, 0)
                ) AS pageViews,

                sum(
                    coalesce(sh.isOrder, 0)
                ) AS orderCount,

                CASE
                    WHEN max(
                        CASE
                            WHEN coalesce(sh.isOrder, 0) = 1
                             AND sh.customerType = 'Prospect'
                                THEN 1
                            ELSE 0
                        END
                    ) = 1
                        THEN 'Prospect'

                    WHEN max(
                        CASE
                            WHEN coalesce(sh.isOrder, 0) = 1
                             AND sh.customerType = 'Care'
                                THEN 1
                            ELSE 0
                        END
                    ) = 1
                        THEN 'Care'

                    WHEN max(
                        CASE
                            WHEN coalesce(sh.isOrder, 0) = 1
                             AND sh.customerType = 'Customer'
                                THEN 1
                            ELSE 0
                        END
                    ) = 1
                        THEN 'Customer'

                    ELSE NULL
                END AS orderCustomerType,

                sum(
                    coalesce(sh.isVrCall, 0)
                ) AS vrCallEvents,

                sum(
                    coalesce(sh.isVrChat, 0)
                ) AS vrChatEvents,

                sum(
                    coalesce(sh.isStoreLocator, 0)
                ) AS storeLocatorEvents,

                sum(
                    coalesce(sh.isAssistedOrder, 0)
                ) AS assistedOrderEvents,

                max(
                    coalesce(sh.isBuyFlow, 0)
                ) AS hasBuyFlow,

                sum(
                    coalesce(sh.isConfigure, 0)
                ) AS configureEvents,

                sum(
                    coalesce(sh.isCheckoutStart, 0)
                ) AS checkoutStartEvents

            FROM sessionHits AS sh

            GROUP BY
                sh.sessionId,
                coalesce(
                    nullif(trim(sh.pageCategory), ''),
                    '(not set)'
                )

            HAVING
                   sum(coalesce(sh.isPageView, 0)) > 0
                OR sum(coalesce(sh.isOrder, 0)) > 0
                OR sum(coalesce(sh.isVrCall, 0)) > 0
                OR sum(coalesce(sh.isVrChat, 0)) > 0
                OR sum(coalesce(sh.isStoreLocator, 0)) > 0
                OR sum(coalesce(sh.isAssistedOrder, 0)) > 0
                OR max(coalesce(sh.isBuyFlow, 0)) > 0
                OR sum(coalesce(sh.isConfigure, 0)) > 0
                OR sum(coalesce(sh.isCheckoutStart, 0)) > 0
        )

        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily
        REPLACE WHERE sessionStartDatePst
                      BETWEEN v_windowStart AND v_windowEnd
        SELECT
            aa.sessionId,
            aa.canonicalUserId,
            aa.visitorId,
            aa.sessionStartDatePst,
            aa.weekStartDate,
            aa.weekEndDate,
            aa.pageCategory,
            aa.buyFlowStep,
            cast(aa.pageViews AS BIGINT) AS pageViews,
            cast(aa.orderCount AS BIGINT) AS orderCount,
            aa.orderCustomerType,
            cast(aa.vrCallEvents AS BIGINT) AS vrCallEvents,
            cast(aa.vrChatEvents AS BIGINT) AS vrChatEvents,
            cast(aa.storeLocatorEvents AS BIGINT) AS storeLocatorEvents,
            cast(aa.assistedOrderEvents AS BIGINT) AS assistedOrderEvents,
            aa.hasBuyFlow,
            cast(aa.configureEvents AS BIGINT) AS configureEvents,
            cast(aa.checkoutStartEvents AS BIGINT) AS checkoutStartEvents,
            v_processedAt AS silverProcessedAt
        FROM aggregatedActions AS aa;

        SELECT
            'SUCCESS' AS status,
            v_windowStart AS loadedSessionStartDate,
            v_windowEnd AS loadedSessionEndDate,
            'prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily' AS targetObject;
    END IF;
END;

-- ============================================================================
-- TEST: Validation only
-- ============================================================================

-- CALL prdrzranalytics.lab42.sdi_sp_mip_silver_actionsPerSessionPageCategory_daily(
--     p_asOfDate        => DATE '2026-09-28',
--     p_eventWindowDays => 1,
--     p_validateOnly    => TRUE
-- );

-- ============================================================================
-- TEST: Execute load
-- ============================================================================

-- CALL prdrzranalytics.lab42.sdi_sp_mip_silver_actionsPerSessionPageCategory_daily(
--     p_asOfDate        => DATE '2026-09-28',
--     p_eventWindowDays => 1,
--     p_validateOnly    => FALSE
-- );