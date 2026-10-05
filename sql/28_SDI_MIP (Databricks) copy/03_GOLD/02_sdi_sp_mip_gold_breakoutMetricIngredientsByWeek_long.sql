-- ============================================================================
-- FILE  : 02_sdi_sp_mip_gold_breakoutMetricIngredientsByWeek_long.sql
-- LAYER : GOLD
-- PURPOSE:
--   Gold breakout comparison ingredients by target week, breakout value, and metric.
--
-- DESIGN:
--   - One top-level CREATE OR REPLACE PROCEDURE per file.
--   - No run/job ID dependency during development.
--   - Uses control VIEWS, not persisted control tables.
--   - Validates required sources/control metadata before target creation/write.
--   - p_validateOnly = TRUE performs preflight only.
--   - Default as-of date is the previous Pacific calendar day.
--   - Comparison percentages are NOT persisted; Gold stores ingredients.
--   - PERFORMANCE: visitor-week Silver is restricted to target/prior/4-week/LY weeks before breakout expansion.
--   - PEER SET: actual breakout slices keep the existing attributed visitor/week contract.
--     Peer populations use overlapping session/page-category membership, so a visitor
--     may belong to both the selected slice and its peer set through another value.
--   - PEER BASIS: always the four-week trend, independent of the screen comparator.
--   - peerSetNumerator/peerSetDenominator store the selected slice's COUNTERFACTUAL
--     current value if it had moved at the peer-set rate. App Gold derives the peer
--     gap and excess/deficit volume from those existing columns.
--   - Impact-on-topline remains comparator-aware downstream:
--       (slice current - slice comparator) / topline comparator   for count metrics.
-- ============================================================================
CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_gold_breakoutMetricIngredientsByWeek_long(
    IN p_asOfDate       DATE    DEFAULT NULL,
    IN p_weeksToRebuild INT     DEFAULT 1,
    IN p_validateOnly   BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Gold breakout comparison ingredients by target week, breakout value, and metric.'
AS
BEGIN
    DECLARE v_asOfDate DATE DEFAULT coalesce(
        p_asOfDate,
        date_add(
            to_date(from_utc_timestamp(current_timestamp(), 'America/Los_Angeles')),
            -1
        )
    );
    DECLARE v_weekTo DATE;
    DECLARE v_weekFrom DATE;
    DECLARE v_weekEndTo DATE;
    DECLARE v_processedAt TIMESTAMP DEFAULT current_timestamp();
    -- ------------------------------------------------------------------------
    -- 1. Parameter validation
    -- ------------------------------------------------------------------------
    IF p_weeksToRebuild IS NULL OR p_weeksToRebuild < 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'p_weeksToRebuild must be >= 1.';
    END IF;
    SET v_weekTo = date_add(v_asOfDate, 1 - dayofweek(v_asOfDate));
    SET v_weekFrom = date_add(v_weekTo, -7 * (p_weeksToRebuild - 1));
    SET v_weekEndTo = date_add(v_weekTo, 6);
    -- ------------------------------------------------------------------------
    -- 2. Source/control preflight
    -- ------------------------------------------------------------------------
    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly a
        JOIN prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly x
          ON x.weekStartDate = a.weekStartDate
         AND x.visitorId = a.visitorId
        WHERE a.weekStartDate BETWEEN v_weekFrom AND v_weekTo
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Weekly Silver attribute/action visitor keys have no joined rows for the requested Gold target-week range.';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static
        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Fiscal calendar control view has no rows for the requested Gold target-week range.';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static
        WHERE isActive
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Metric catalog control view has no active metrics.';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static
        WHERE isActive AND isPrebuiltBreakout
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Breakout catalog control view has no active prebuilt breakouts.';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily
        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
          AND visitorId IS NOT NULL
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Silver attributesPerSession has no peer-membership source rows for the requested Gold target-week range.';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily
        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
          AND visitorId IS NOT NULL
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Silver actionsPerSessionPageCategory has no page-category peer-membership rows for the requested Gold target-week range.';
    END IF;
    -- ------------------------------------------------------------------------
    -- 3. Validation-only mode
    -- ------------------------------------------------------------------------
    IF p_validateOnly THEN
        SELECT
            'VALIDATION_ONLY' AS status,
            v_weekFrom AS rebuildWeekStartFrom,
            v_weekTo AS rebuildWeekStartTo,
            v_weekEndTo AS latestWeekEndDate,
            CASE WHEN v_asOfDate < v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,
            'No Gold table was created or modified.' AS message;
    ELSE
        -- --------------------------------------------------------------------
        -- 4. Create target only after preflight succeeds
        -- --------------------------------------------------------------------
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long (
targetWeekStartDate         DATE,
  targetWeekEndDate           DATE,
  fiscalQuarterLabel          STRING,
  fiscalWeekCode              STRING,
  weekLabel                   STRING,
  filterLob                   STRING COMMENT 'All for now; reserved for precomputed whole-report filter contexts',
  filterPlatform              STRING COMMENT 'All for now; reserved for precomputed whole-report filter contexts',
  breakoutType                STRING,
  breakoutLabel               STRING,
  breakoutValue               STRING,
  valueRankByNbv              INT COMMENT 'Rank in the target week by NBV',
  isTopN                      BOOLEAN COMMENT 'Target-week Top-N flag; later view can bucket false rows into (Other)',
  metricName                  STRING,
  metricLabel                 STRING,
  metricKind                  STRING,
  displayFormat               STRING,
  changeUnit                  STRING,
  thisWeekNumerator           DOUBLE,
  thisWeekDenominator         DOUBLE,
  priorWeekNumerator          DOUBLE,
  priorWeekDenominator        DOUBLE,
  fourWeekTrendNumerator      DOUBLE,
  fourWeekTrendDenominator    DOUBLE,
  sameWeekLyNumerator         DOUBLE,
  sameWeekLyDenominator       DOUBLE,
  thisWeekDataAvailable       BOOLEAN,
  priorWeekDataAvailable      BOOLEAN,
  fourWeekTrendWeekCount      INT,
  sameWeekLyDataAvailable     BOOLEAN,
  peerSetNumerator            DOUBLE COMMENT 'Counterfactual expected slice numerator if the selected value had moved at its four-week peer-set rate; NULL unless the full four-week peer window is available',
  peerSetDenominator          DOUBLE COMMENT 'Counterfactual expected slice denominator for ratio metrics; NULL for count metrics',
  goldProcessedAt             TIMESTAMP
        )
        USING DELTA
        CLUSTER BY (targetWeekStartDate, breakoutType, metricName)
        COMMENT 'Gold Breakouts: raw attributed values plus safe comparison ingredients. Non-Top-N rows are bucketed into (Other) later.';
        -- --------------------------------------------------------------------
        -- 5. Rebuild requested target-week range
        -- --------------------------------------------------------------------
        WITH targetCalendar AS (
            SELECT *
            FROM prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static
            WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
        ),
        requiredWeeks AS (
            SELECT weekStartDate FROM targetCalendar
            UNION
            SELECT priorWeekStartDate FROM targetCalendar
            UNION
            SELECT sameWeekLastYearStartDate FROM targetCalendar
            UNION
            SELECT explode(sequence(fourWeekAvgStartDate,fourWeekAvgEndDate,INTERVAL 7 DAYS)) AS weekStartDate
            FROM targetCalendar
        ),
        -- --------------------------------------------------------------------
        -- A. EXISTING ATTRIBUTED BREAKOUT ACTUALS
        --
        -- Keep the current dashboard slice contract unchanged: one attributed
        -- visitor/week value per breakout. This protects current API numbers,
        -- ranking, Top-N and synthetic (Other) behavior.
        -- --------------------------------------------------------------------
        visitorWeek AS (
            SELECT
                a.weekStartDate,
                a.visitorId,
                a.lob,
                a.platform,
                a.prospectVsBase,
                a.authState,
                a.channel,
                a.campaign,
                a.entryPage,
                a.utmSource,
                a.utmMedium,
                a.utmCampaign,
                a.pageCategory,
                a.device,
                a.region,
                a.buyFlowStep,
                x.nbv,
                x.sessionCount,
                x.pageViews,
                x.nbvBuyFlow,
                x.nbvConfigure,
                x.nbvCheckoutStart,
                x.orders,
                x.ordersAcquisition,
                x.ordersBase,
                x.ordersUnassisted,
                x.ordersAssisted,
                x.vrCalls,
                x.vrChats,
                x.storeLocator,
                x.orderCount
            FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly a
            INNER JOIN prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly x
              ON x.weekStartDate=a.weekStartDate
             AND x.visitorId=a.visitorId
            WHERE a.weekStartDate IN (SELECT weekStartDate FROM requiredWeeks)
        ),
        exploded AS (
            SELECT
                weekStartDate,
                d.breakoutType,
                d.breakoutValue,
                nbv,
                sessionCount,
                pageViews,
                nbvBuyFlow,
                nbvConfigure,
                nbvCheckoutStart,
                orders,
                ordersAcquisition,
                ordersBase,
                ordersUnassisted,
                ordersAssisted,
                vrCalls,
                vrChats,
                storeLocator,
                orderCount
            FROM visitorWeek
            LATERAL VIEW explode(array(
                named_struct('breakoutType','lob',            'breakoutValue',coalesce(lob,'Other')),
                named_struct('breakoutType','platform',       'breakoutValue',coalesce(platform,'(not set)')),
                named_struct('breakoutType','prospectVsBase', 'breakoutValue',coalesce(prospectVsBase,'Unknown')),
                named_struct('breakoutType','authState',      'breakoutValue',coalesce(authState,'(not set)')),
                named_struct('breakoutType','channel',        'breakoutValue',coalesce(channel,'(not set)')),
                named_struct('breakoutType','campaign',       'breakoutValue',coalesce(campaign,'(not set)')),
                named_struct('breakoutType','entryPage',      'breakoutValue',coalesce(entryPage,'(not set)')),
                named_struct('breakoutType','pageCategory',   'breakoutValue',coalesce(pageCategory,'(not set)')),
                named_struct('breakoutType','device',         'breakoutValue',coalesce(device,'Unknown')),
                named_struct('breakoutType','region',         'breakoutValue',coalesce(region,'(not available)')),
                named_struct('breakoutType','utmSource',      'breakoutValue',coalesce(utmSource,'(not set)')),
                named_struct('breakoutType','utmMedium',      'breakoutValue',coalesce(utmMedium,'(not set)')),
                named_struct('breakoutType','utmCampaign',    'breakoutValue',coalesce(utmCampaign,'(not set)')),
                named_struct('breakoutType','buyFlowStep',    'breakoutValue',coalesce(buyFlowStep,'Did not enter buy flow'))
            )) dview AS d
        ),
        weeklyWide AS (
            SELECT
                weekStartDate,
                breakoutType,
                breakoutValue,
                SUM(nbv) AS nbv,
                SUM(sessionCount) AS sessionCount,
                SUM(pageViews) AS pageViews,
                SUM(nbvBuyFlow) AS nbvBuyFlow,
                SUM(nbvConfigure) AS nbvConfigure,
                SUM(nbvCheckoutStart) AS nbvCheckoutStart,
                SUM(orders) AS orders,
                SUM(ordersAcquisition) AS ordersAcquisition,
                SUM(ordersBase) AS ordersBase,
                SUM(ordersUnassisted) AS ordersUnassisted,
                SUM(ordersAssisted) AS ordersAssisted,
                SUM(vrCalls) AS vrCalls,
                SUM(vrChats) AS vrChats,
                SUM(storeLocator) AS storeLocator,
                SUM(orderCount) AS orderCount
            FROM exploded
            GROUP BY weekStartDate,breakoutType,breakoutValue
        ),
        weeklyCounts AS (
            SELECT
                weekStartDate,
                breakoutType,
                breakoutValue,
                metricName,
                metricValue
            FROM weeklyWide
            LATERAL VIEW stack(
                15,
                'nbv',               cast(nbv AS DOUBLE),
                'sessionCount',      cast(sessionCount AS DOUBLE),
                'pageViews',         cast(pageViews AS DOUBLE),
                'nbvBuyFlow',        cast(nbvBuyFlow AS DOUBLE),
                'nbvConfigure',      cast(nbvConfigure AS DOUBLE),
                'nbvCheckoutStart',  cast(nbvCheckoutStart AS DOUBLE),
                'orders',            cast(orders AS DOUBLE),
                'ordersAcquisition', cast(ordersAcquisition AS DOUBLE),
                'ordersBase',        cast(ordersBase AS DOUBLE),
                'ordersUnassisted',  cast(ordersUnassisted AS DOUBLE),
                'ordersAssisted',    cast(ordersAssisted AS DOUBLE),
                'vrCalls',           cast(vrCalls AS DOUBLE),
                'vrChats',           cast(vrChats AS DOUBLE),
                'storeLocator',      cast(storeLocator AS DOUBLE),
                'orderCount',        cast(orderCount AS DOUBLE)
            ) s AS metricName,metricValue
        ),
        weeklyIngredients AS (
            SELECT
                c.weekStartDate,
                c.breakoutType,
                c.breakoutValue,
                c.metricName,
                c.metricValue AS numeratorValue,
                cast(NULL AS DOUBLE) AS denominatorValue
            FROM weeklyCounts c
            INNER JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static m
              ON m.metricName=c.metricName
             AND m.metricKind='count'
             AND m.isActive
            UNION ALL
            SELECT
                n.weekStartDate,
                n.breakoutType,
                n.breakoutValue,
                r.metricName,
                MAX(CASE WHEN n.metricName=r.numeratorMetric THEN n.metricValue END) AS numeratorValue,
                MAX(CASE WHEN n.metricName=r.denominatorMetric THEN n.metricValue END) AS denominatorValue
            FROM weeklyCounts n
            INNER JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static r
              ON r.metricKind='ratio'
             AND r.isActive
             AND n.metricName IN (r.numeratorMetric,r.denominatorMetric)
            GROUP BY n.weekStartDate,n.breakoutType,n.breakoutValue,r.metricName
        ),
        availableWeeks AS (
            SELECT DISTINCT weekStartDate
            FROM visitorWeek
        ),
        -- --------------------------------------------------------------------
        -- B. OVERLAPPING PEER MEMBERSHIP SOURCE
        --
        -- Peer populations must NOT be topline-minus-slice for unique visitors.
        -- A visitor can qualify for the selected value and its peer set if they
        -- also qualify through another value. Build the reusable membership at
        -- session/page-category grain, then collapse immediately.
        -- --------------------------------------------------------------------
        peerSessionBase AS (
            SELECT
                sessionId,
                visitorId,
                weekStartDate,
                pageViews,
                lobList,
                lobPageViews,
                platform,
                prospectVsBase,
                authState,
                channel,
                CASE
                    WHEN campaignCode IS NULL THEN '(not set)'
                    WHEN campaignName IS NOT NULL THEN concat(campaignCode,' · ',campaignName)
                    ELSE campaignCode
                END AS campaign,
                coalesce(entryPage,'(not set)') AS entryPage,
                coalesce(device,'Unknown') AS device,
                coalesce(region,'(not available)') AS region,
                coalesce(utmSource,'(not set)') AS utmSource,
                coalesce(utmMedium,'(not set)') AS utmMedium,
                coalesce(utmCampaign,'(not set)') AS utmCampaign,
                coalesce(deepestBuyFlowStep,'Did not enter buy flow') AS buyFlowStep,
                hasBuyFlow,
                hasConfigure,
                hasCheckoutStart,
                hasOrder,
                hasAcquisitionOrder,
                hasAssistedOrder,
                hasVrCall,
                hasVrChat,
                hasStoreLocator,
                orderCount
            FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily
            WHERE weekStartDate IN (SELECT weekStartDate FROM requiredWeeks)
              AND visitorId IS NOT NULL
        ),
        peerScalarMemberships AS (
            SELECT
                s.sessionId,
                s.visitorId,
                s.weekStartDate,
                d.breakoutType,
                d.breakoutValue,
                cast(1 AS BIGINT) AS sessionCount,
                s.pageViews,
                s.hasBuyFlow,
                s.hasConfigure,
                s.hasCheckoutStart,
                s.hasOrder,
                s.hasAcquisitionOrder,
                s.hasAssistedOrder,
                s.hasVrCall,
                s.hasVrChat,
                s.hasStoreLocator,
                s.orderCount
            FROM peerSessionBase s
            LATERAL VIEW explode(array(
                named_struct('breakoutType','platform',       'breakoutValue',coalesce(s.platform,'(not set)')),
                named_struct('breakoutType','prospectVsBase', 'breakoutValue',coalesce(s.prospectVsBase,'Unknown')),
                named_struct('breakoutType','authState',      'breakoutValue',coalesce(s.authState,'(not set)')),
                named_struct('breakoutType','channel',        'breakoutValue',coalesce(s.channel,'(not set)')),
                named_struct('breakoutType','campaign',       'breakoutValue',coalesce(s.campaign,'(not set)')),
                named_struct('breakoutType','entryPage',      'breakoutValue',coalesce(s.entryPage,'(not set)')),
                named_struct('breakoutType','device',         'breakoutValue',coalesce(s.device,'Unknown')),
                named_struct('breakoutType','region',         'breakoutValue',coalesce(s.region,'(not available)')),
                named_struct('breakoutType','utmSource',      'breakoutValue',coalesce(s.utmSource,'(not set)')),
                named_struct('breakoutType','utmMedium',      'breakoutValue',coalesce(s.utmMedium,'(not set)')),
                named_struct('breakoutType','utmCampaign',    'breakoutValue',coalesce(s.utmCampaign,'(not set)')),
                named_struct('breakoutType','buyFlowStep',    'breakoutValue',coalesce(s.buyFlowStep,'Did not enter buy flow'))
            )) dview AS d
        ),
        peerLobMemberships AS (
            SELECT
                s.sessionId,
                s.visitorId,
                s.weekStartDate,
                'lob' AS breakoutType,
                coalesce(lobValue,'Other') AS breakoutValue,
                cast(1 AS BIGINT) AS sessionCount,
                cast(coalesce(element_at(s.lobPageViews,lobValue),0) AS BIGINT) AS pageViews,
                s.hasBuyFlow,
                s.hasConfigure,
                s.hasCheckoutStart,
                s.hasOrder,
                s.hasAcquisitionOrder,
                s.hasAssistedOrder,
                s.hasVrCall,
                s.hasVrChat,
                s.hasStoreLocator,
                s.orderCount
            FROM peerSessionBase s
            LATERAL VIEW explode(
                CASE
                    WHEN s.lobList IS NULL OR size(s.lobList)=0 THEN array('Other')
                    ELSE s.lobList
                END
            ) lobView AS lobValue
        ),
        peerPageCategoryMemberships AS (
            SELECT
                a.sessionId,
                a.visitorId,
                a.weekStartDate,
                'pageCategory' AS breakoutType,
                coalesce(a.pageCategory,'(not set)') AS breakoutValue,
                cast(1 AS BIGINT) AS sessionCount,
                a.pageViews,
                a.hasBuyFlow,
                CASE WHEN a.configureEvents>0 THEN 1 ELSE 0 END AS hasConfigure,
                CASE WHEN a.checkoutStartEvents>0 THEN 1 ELSE 0 END AS hasCheckoutStart,
                CASE WHEN a.orderCount>0 THEN 1 ELSE 0 END AS hasOrder,
                CASE
                    WHEN a.orderCount>0 AND lower(trim(coalesce(a.orderCustomerType,'')))='prospect' THEN 1
                    ELSE 0
                END AS hasAcquisitionOrder,
                CASE WHEN a.assistedOrderEvents>0 THEN 1 ELSE 0 END AS hasAssistedOrder,
                CASE WHEN a.vrCallEvents>0 THEN 1 ELSE 0 END AS hasVrCall,
                CASE WHEN a.vrChatEvents>0 THEN 1 ELSE 0 END AS hasVrChat,
                CASE WHEN a.storeLocatorEvents>0 THEN 1 ELSE 0 END AS hasStoreLocator,
                a.orderCount
            FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily a
            WHERE a.weekStartDate IN (SELECT weekStartDate FROM requiredWeeks)
              AND a.visitorId IS NOT NULL
        ),
        peerMembershipRows AS (
            SELECT * FROM peerScalarMemberships
            UNION ALL
            SELECT * FROM peerLobMemberships
            UNION ALL
            SELECT * FROM peerPageCategoryMemberships
        ),
        peerVisitorBreakoutPrimitive AS (
            SELECT
                visitorId,
                weekStartDate,
                breakoutType,
                breakoutValue,
                COUNT(DISTINCT sessionId) AS sessionCount,
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
            FROM peerMembershipRows
            GROUP BY visitorId,weekStartDate,breakoutType,breakoutValue
        ),
        peerVisitorBreakout AS (
            SELECT
                *,
                cast(1 AS INT) AS nbv,
                greatest(orders-ordersAcquisition,0) AS ordersBase,
                greatest(orders-ordersAssisted,0) AS ordersUnassisted
            FROM peerVisitorBreakoutPrimitive
        ),
        peerUniqueLong AS (
            SELECT
                visitorId,
                weekStartDate,
                breakoutType,
                breakoutValue,
                metricName,
                metricFlag
            FROM peerVisitorBreakout
            LATERAL VIEW stack(
                12,
                'nbv',               cast(nbv AS INT),
                'nbvBuyFlow',        cast(nbvBuyFlow AS INT),
                'nbvConfigure',      cast(nbvConfigure AS INT),
                'nbvCheckoutStart',  cast(nbvCheckoutStart AS INT),
                'orders',            cast(orders AS INT),
                'ordersAcquisition', cast(ordersAcquisition AS INT),
                'ordersBase',        cast(ordersBase AS INT),
                'ordersUnassisted',  cast(ordersUnassisted AS INT),
                'ordersAssisted',    cast(ordersAssisted AS INT),
                'vrCalls',           cast(vrCalls AS INT),
                'vrChats',           cast(vrChats AS INT),
                'storeLocator',      cast(storeLocator AS INT)
            ) m AS metricName,metricFlag
        ),
        peerBreakoutUniverse AS (
            SELECT DISTINCT breakoutType,breakoutValue FROM exploded
            UNION
            SELECT DISTINCT breakoutType,breakoutValue FROM peerVisitorBreakout
        ),
        peerWeekGrid AS (
            SELECT
                w.weekStartDate,
                u.breakoutType,
                u.breakoutValue
            FROM availableWeeks w
            CROSS JOIN peerBreakoutUniverse u
            INNER JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static b
              ON b.breakoutType=u.breakoutType
             AND b.isActive
             AND b.isPrebuiltBreakout
        ),
        peerUniqueContext AS (
            SELECT
                weekStartDate,
                breakoutType,
                metricName,
                COUNT(DISTINCT CASE WHEN metricFlag=1 THEN visitorId END) AS contextValue
            FROM peerUniqueLong
            GROUP BY weekStartDate,breakoutType,metricName
        ),
        peerUniqueQualifying AS (
            SELECT
                visitorId,
                weekStartDate,
                breakoutType,
                metricName,
                COUNT(*) AS qualifyingValueCount,
                first(breakoutValue) AS soleBreakoutValue
            FROM peerUniqueLong
            WHERE metricFlag=1
            GROUP BY visitorId,weekStartDate,breakoutType,metricName
        ),
        peerUniqueExclusive AS (
            SELECT
                weekStartDate,
                breakoutType,
                metricName,
                soleBreakoutValue AS breakoutValue,
                COUNT(*) AS exclusiveValue
            FROM peerUniqueQualifying
            WHERE qualifyingValueCount=1
            GROUP BY weekStartDate,breakoutType,metricName,soleBreakoutValue
        ),
        peerUniqueByValue AS (
            SELECT
                g.weekStartDate,
                g.breakoutType,
                g.breakoutValue,
                c.metricName,
                cast(greatest(c.contextValue-coalesce(e.exclusiveValue,0),0) AS DOUBLE) AS metricValue
            FROM peerWeekGrid g
            INNER JOIN peerUniqueContext c
              ON c.weekStartDate=g.weekStartDate
             AND c.breakoutType=g.breakoutType
            LEFT JOIN peerUniqueExclusive e
              ON e.weekStartDate=g.weekStartDate
             AND e.breakoutType=g.breakoutType
             AND e.metricName=c.metricName
             AND e.breakoutValue=g.breakoutValue
        ),
        peerSessionCells AS (
            SELECT DISTINCT
                weekStartDate,
                breakoutType,
                breakoutValue,
                sessionId
            FROM peerMembershipRows
        ),
        peerSessionContext AS (
            SELECT
                weekStartDate,
                breakoutType,
                COUNT(DISTINCT sessionId) AS contextValue
            FROM peerSessionCells
            GROUP BY weekStartDate,breakoutType
        ),
        peerSessionQualifying AS (
            SELECT
                weekStartDate,
                breakoutType,
                sessionId,
                COUNT(*) AS qualifyingValueCount,
                first(breakoutValue) AS soleBreakoutValue
            FROM peerSessionCells
            GROUP BY weekStartDate,breakoutType,sessionId
        ),
        peerSessionExclusive AS (
            SELECT
                weekStartDate,
                breakoutType,
                soleBreakoutValue AS breakoutValue,
                COUNT(*) AS exclusiveValue
            FROM peerSessionQualifying
            WHERE qualifyingValueCount=1
            GROUP BY weekStartDate,breakoutType,soleBreakoutValue
        ),
        peerSessionByValue AS (
            SELECT
                g.weekStartDate,
                g.breakoutType,
                g.breakoutValue,
                cast(greatest(c.contextValue-coalesce(e.exclusiveValue,0),0) AS DOUBLE) AS metricValue
            FROM peerWeekGrid g
            INNER JOIN peerSessionContext c
              ON c.weekStartDate=g.weekStartDate
             AND c.breakoutType=g.breakoutType
            LEFT JOIN peerSessionExclusive e
              ON e.weekStartDate=g.weekStartDate
             AND e.breakoutType=g.breakoutType
             AND e.breakoutValue=g.breakoutValue
        ),
        peerPageViewSlice AS (
            SELECT
                weekStartDate,
                breakoutType,
                breakoutValue,
                SUM(pageViews) AS sliceValue
            FROM peerVisitorBreakout
            GROUP BY weekStartDate,breakoutType,breakoutValue
        ),
        peerPageViewContext AS (
            SELECT
                weekStartDate,
                breakoutType,
                SUM(sliceValue) AS contextValue
            FROM peerPageViewSlice
            GROUP BY weekStartDate,breakoutType
        ),
        peerPageViewByValue AS (
            SELECT
                g.weekStartDate,
                g.breakoutType,
                g.breakoutValue,
                cast(greatest(c.contextValue-coalesce(s.sliceValue,0),0) AS DOUBLE) AS metricValue
            FROM peerWeekGrid g
            INNER JOIN peerPageViewContext c
              ON c.weekStartDate=g.weekStartDate
             AND c.breakoutType=g.breakoutType
            LEFT JOIN peerPageViewSlice s
              ON s.weekStartDate=g.weekStartDate
             AND s.breakoutType=g.breakoutType
             AND s.breakoutValue=g.breakoutValue
        ),
        peerOrderSessionNonCategory AS (
            SELECT
                weekStartDate,
                breakoutType,
                sessionId,
                COUNT(DISTINCT breakoutValue) AS qualifyingValueCount,
                first(breakoutValue) AS soleBreakoutValue,
                MAX(orderCount) AS sessionOrderCount
            FROM peerMembershipRows
            WHERE breakoutType<>'pageCategory'
            GROUP BY weekStartDate,breakoutType,sessionId
        ),
        peerOrderContextNonCategory AS (
            SELECT
                weekStartDate,
                breakoutType,
                SUM(sessionOrderCount) AS contextValue
            FROM peerOrderSessionNonCategory
            GROUP BY weekStartDate,breakoutType
        ),
        peerOrderExclusiveNonCategory AS (
            SELECT
                weekStartDate,
                breakoutType,
                soleBreakoutValue AS breakoutValue,
                SUM(sessionOrderCount) AS exclusiveValue
            FROM peerOrderSessionNonCategory
            WHERE qualifyingValueCount=1
            GROUP BY weekStartDate,breakoutType,soleBreakoutValue
        ),
        peerOrderByValueNonCategory AS (
            SELECT
                g.weekStartDate,
                g.breakoutType,
                g.breakoutValue,
                cast(greatest(c.contextValue-coalesce(e.exclusiveValue,0),0) AS DOUBLE) AS metricValue
            FROM peerWeekGrid g
            INNER JOIN peerOrderContextNonCategory c
              ON c.weekStartDate=g.weekStartDate
             AND c.breakoutType=g.breakoutType
            LEFT JOIN peerOrderExclusiveNonCategory e
              ON e.weekStartDate=g.weekStartDate
             AND e.breakoutType=g.breakoutType
             AND e.breakoutValue=g.breakoutValue
            WHERE g.breakoutType<>'pageCategory'
        ),
        peerOrderSliceCategory AS (
            SELECT
                weekStartDate,
                breakoutValue,
                SUM(orderCount) AS sliceValue
            FROM peerVisitorBreakout
            WHERE breakoutType='pageCategory'
            GROUP BY weekStartDate,breakoutValue
        ),
        peerOrderContextCategory AS (
            SELECT
                weekStartDate,
                SUM(sliceValue) AS contextValue
            FROM peerOrderSliceCategory
            GROUP BY weekStartDate
        ),
        peerOrderByValueCategory AS (
            SELECT
                g.weekStartDate,
                g.breakoutType,
                g.breakoutValue,
                cast(greatest(c.contextValue-coalesce(s.sliceValue,0),0) AS DOUBLE) AS metricValue
            FROM peerWeekGrid g
            INNER JOIN peerOrderContextCategory c
              ON c.weekStartDate=g.weekStartDate
            LEFT JOIN peerOrderSliceCategory s
              ON s.weekStartDate=g.weekStartDate
             AND s.breakoutValue=g.breakoutValue
            WHERE g.breakoutType='pageCategory'
        ),
        peerWeeklyCounts AS (
            SELECT weekStartDate,breakoutType,breakoutValue,metricName,metricValue
            FROM peerUniqueByValue
            UNION ALL
            SELECT weekStartDate,breakoutType,breakoutValue,'sessionCount',metricValue
            FROM peerSessionByValue
            UNION ALL
            SELECT weekStartDate,breakoutType,breakoutValue,'pageViews',metricValue
            FROM peerPageViewByValue
            UNION ALL
            SELECT weekStartDate,breakoutType,breakoutValue,'orderCount',metricValue
            FROM peerOrderByValueNonCategory
            UNION ALL
            SELECT weekStartDate,breakoutType,breakoutValue,'orderCount',metricValue
            FROM peerOrderByValueCategory
        ),
        peerWeeklyIngredients AS (
            SELECT
                c.weekStartDate,
                c.breakoutType,
                c.breakoutValue,
                c.metricName,
                c.metricValue AS numeratorValue,
                cast(NULL AS DOUBLE) AS denominatorValue
            FROM peerWeeklyCounts c
            INNER JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static m
              ON m.metricName=c.metricName
             AND m.metricKind='count'
             AND m.isActive
            UNION ALL
            SELECT
                n.weekStartDate,
                n.breakoutType,
                n.breakoutValue,
                r.metricName,
                MAX(CASE WHEN n.metricName=r.numeratorMetric THEN n.metricValue END) AS numeratorValue,
                MAX(CASE WHEN n.metricName=r.denominatorMetric THEN n.metricValue END) AS denominatorValue
            FROM peerWeeklyCounts n
            INNER JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static r
              ON r.metricKind='ratio'
             AND r.isActive
             AND n.metricName IN (r.numeratorMetric,r.denominatorMetric)
            GROUP BY n.weekStartDate,n.breakoutType,n.breakoutValue,r.metricName
        ),
        -- --------------------------------------------------------------------
        -- C. COMPARISON WINDOWS AND TARGET ROWS
        -- --------------------------------------------------------------------
        fourWeekAvailability AS (
            SELECT
                t.weekStartDate AS targetWeekStartDate,
                COUNT(a.weekStartDate) AS fourWeekTrendWeekCount
            FROM targetCalendar t
            LEFT JOIN availableWeeks a
              ON a.weekStartDate BETWEEN t.fourWeekAvgStartDate AND t.fourWeekAvgEndDate
            GROUP BY t.weekStartDate
        ),
        targetWeeks AS (
            SELECT
                t.*,
                (cur.weekStartDate IS NOT NULL) AS thisWeekDataAvailable,
                (pw.weekStartDate IS NOT NULL) AS priorWeekDataAvailable,
                cast(coalesce(fwa.fourWeekTrendWeekCount,0) AS INT) AS fourWeekTrendWeekCount,
                (ly.weekStartDate IS NOT NULL) AS sameWeekLyDataAvailable
            FROM targetCalendar t
            LEFT JOIN availableWeeks cur
              ON cur.weekStartDate=t.weekStartDate
            LEFT JOIN availableWeeks pw
              ON pw.weekStartDate=t.priorWeekStartDate
            LEFT JOIN fourWeekAvailability fwa
              ON fwa.targetWeekStartDate=t.weekStartDate
            LEFT JOIN availableWeeks ly
              ON ly.weekStartDate=t.sameWeekLastYearStartDate
        ),
        candidateValues AS (
            SELECT DISTINCT
                t.weekStartDate AS targetWeekStartDate,
                h.breakoutType,
                h.breakoutValue
            FROM targetWeeks t
            INNER JOIN weeklyWide h
              ON h.weekStartDate=t.weekStartDate
              OR h.weekStartDate=t.priorWeekStartDate
              OR h.weekStartDate BETWEEN t.fourWeekAvgStartDate AND t.fourWeekAvgEndDate
              OR h.weekStartDate=t.sameWeekLastYearStartDate
        ),
        targetRanks AS (
            SELECT
                c.targetWeekStartDate,
                c.breakoutType,
                c.breakoutValue,
                cast(
                    row_number() OVER (
                        PARTITION BY c.targetWeekStartDate,c.breakoutType
                        ORDER BY coalesce(w.nbv,0) DESC,c.breakoutValue
                    ) AS INT
                ) AS valueRankByNbv
            FROM candidateValues c
            LEFT JOIN weeklyWide w
              ON w.weekStartDate=c.targetWeekStartDate
             AND w.breakoutType=c.breakoutType
             AND w.breakoutValue=c.breakoutValue
        ),
        assembled AS (
            SELECT
                t.weekStartDate AS targetWeekStartDate,
                t.weekEndDate AS targetWeekEndDate,
                t.fiscalQuarterLabel,
                t.fiscalWeekCode,
                t.weekLabel,
                'All' AS filterLob,
                'All' AS filterPlatform,
                v.breakoutType,
                b.breakoutLabel,
                v.breakoutValue,
                r.valueRankByNbv,
                CASE WHEN b.topN IS NULL THEN true ELSE r.valueRankByNbv<=b.topN END AS isTopN,
                m.metricName,
                m.metricLabel,
                m.metricKind,
                m.displayFormat,
                m.changeUnit,
                CASE WHEN t.thisWeekDataAvailable THEN coalesce(cur.numeratorValue,0D) END AS thisWeekNumerator,
                CASE WHEN m.metricKind='ratio' AND t.thisWeekDataAvailable THEN coalesce(cur.denominatorValue,0D) END AS thisWeekDenominator,
                CASE WHEN t.priorWeekDataAvailable THEN coalesce(pw.numeratorValue,0D) END AS priorWeekNumerator,
                CASE WHEN m.metricKind='ratio' AND t.priorWeekDataAvailable THEN coalesce(pw.denominatorValue,0D) END AS priorWeekDenominator,
                CASE WHEN t.fourWeekTrendWeekCount>0 THEN coalesce(SUM(fw.numeratorValue),0D) END AS fourWeekTrendNumerator,
                CASE WHEN m.metricKind='ratio' AND t.fourWeekTrendWeekCount>0 THEN coalesce(SUM(fw.denominatorValue),0D) END AS fourWeekTrendDenominator,
                CASE WHEN t.sameWeekLyDataAvailable THEN coalesce(ly.numeratorValue,0D) END AS sameWeekLyNumerator,
                CASE WHEN m.metricKind='ratio' AND t.sameWeekLyDataAvailable THEN coalesce(ly.denominatorValue,0D) END AS sameWeekLyDenominator,
                t.thisWeekDataAvailable,
                t.priorWeekDataAvailable,
                t.fourWeekTrendWeekCount,
                t.sameWeekLyDataAvailable,
                CASE WHEN t.thisWeekDataAvailable THEN coalesce(peerCur.numeratorValue,0D) END AS peerCurrentNumerator,
                CASE WHEN m.metricKind='ratio' AND t.thisWeekDataAvailable THEN coalesce(peerCur.denominatorValue,0D) END AS peerCurrentDenominator,
                CASE WHEN t.fourWeekTrendWeekCount>0 THEN coalesce(SUM(peerFw.numeratorValue),0D) END AS peerFourWeekNumerator,
                CASE WHEN m.metricKind='ratio' AND t.fourWeekTrendWeekCount>0 THEN coalesce(SUM(peerFw.denominatorValue),0D) END AS peerFourWeekDenominator
            FROM targetWeeks t
            INNER JOIN candidateValues v
              ON v.targetWeekStartDate=t.weekStartDate
            INNER JOIN targetRanks r
              ON r.targetWeekStartDate=v.targetWeekStartDate
             AND r.breakoutType=v.breakoutType
             AND r.breakoutValue=v.breakoutValue
            INNER JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static b
              ON b.breakoutType=v.breakoutType
             AND b.isActive
             AND b.isPrebuiltBreakout
            INNER JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static m
              ON m.isActive
            LEFT JOIN weeklyIngredients cur
              ON cur.weekStartDate=t.weekStartDate
             AND cur.breakoutType=v.breakoutType
             AND cur.breakoutValue=v.breakoutValue
             AND cur.metricName=m.metricName
            LEFT JOIN weeklyIngredients pw
              ON pw.weekStartDate=t.priorWeekStartDate
             AND pw.breakoutType=v.breakoutType
             AND pw.breakoutValue=v.breakoutValue
             AND pw.metricName=m.metricName
            LEFT JOIN weeklyIngredients fw
              ON fw.weekStartDate BETWEEN t.fourWeekAvgStartDate AND t.fourWeekAvgEndDate
             AND fw.breakoutType=v.breakoutType
             AND fw.breakoutValue=v.breakoutValue
             AND fw.metricName=m.metricName
            LEFT JOIN weeklyIngredients ly
              ON ly.weekStartDate=t.sameWeekLastYearStartDate
             AND ly.breakoutType=v.breakoutType
             AND ly.breakoutValue=v.breakoutValue
             AND ly.metricName=m.metricName
            LEFT JOIN peerWeeklyIngredients peerCur
              ON peerCur.weekStartDate=t.weekStartDate
             AND peerCur.breakoutType=v.breakoutType
             AND peerCur.breakoutValue=v.breakoutValue
             AND peerCur.metricName=m.metricName
            LEFT JOIN peerWeeklyIngredients peerFw
              ON peerFw.weekStartDate BETWEEN t.fourWeekAvgStartDate AND t.fourWeekAvgEndDate
             AND peerFw.breakoutType=v.breakoutType
             AND peerFw.breakoutValue=v.breakoutValue
             AND peerFw.metricName=m.metricName
            GROUP BY
                t.weekStartDate,t.weekEndDate,t.fiscalQuarterLabel,t.fiscalWeekCode,t.weekLabel,
                v.breakoutType,b.breakoutLabel,v.breakoutValue,r.valueRankByNbv,b.topN,
                m.metricName,m.metricLabel,m.metricKind,m.displayFormat,m.changeUnit,
                t.thisWeekDataAvailable,t.priorWeekDataAvailable,t.fourWeekTrendWeekCount,t.sameWeekLyDataAvailable,
                cur.numeratorValue,cur.denominatorValue,pw.numeratorValue,pw.denominatorValue,
                ly.numeratorValue,ly.denominatorValue,peerCur.numeratorValue,peerCur.denominatorValue
        ),
        finalRows AS (
            SELECT
                targetWeekStartDate,
                targetWeekEndDate,
                fiscalQuarterLabel,
                fiscalWeekCode,
                weekLabel,
                filterLob,
                filterPlatform,
                breakoutType,
                breakoutLabel,
                breakoutValue,
                valueRankByNbv,
                isTopN,
                metricName,
                metricLabel,
                metricKind,
                displayFormat,
                changeUnit,
                thisWeekNumerator,
                thisWeekDenominator,
                priorWeekNumerator,
                priorWeekDenominator,
                fourWeekTrendNumerator,
                fourWeekTrendDenominator,
                sameWeekLyNumerator,
                sameWeekLyDenominator,
                thisWeekDataAvailable,
                priorWeekDataAvailable,
                fourWeekTrendWeekCount,
                sameWeekLyDataAvailable,
                CASE
                    WHEN fourWeekTrendWeekCount<>4 THEN cast(NULL AS DOUBLE)
                    WHEN metricKind='count'
                     AND peerFourWeekNumerator>0
                        THEN try_divide(fourWeekTrendNumerator,cast(fourWeekTrendWeekCount AS DOUBLE))
                             * try_divide(
                                   peerCurrentNumerator,
                                   try_divide(peerFourWeekNumerator,cast(fourWeekTrendWeekCount AS DOUBLE))
                               )
                    WHEN metricKind='ratio'
                     AND nullif(fourWeekTrendDenominator,0D) IS NOT NULL
                     AND nullif(peerCurrentDenominator,0D) IS NOT NULL
                     AND nullif(peerFourWeekDenominator,0D) IS NOT NULL
                        THEN (
                            try_divide(fourWeekTrendNumerator,fourWeekTrendDenominator)
                            + (
                                try_divide(peerCurrentNumerator,peerCurrentDenominator)
                                - try_divide(peerFourWeekNumerator,peerFourWeekDenominator)
                              )
                        ) * fourWeekTrendDenominator
                    ELSE cast(NULL AS DOUBLE)
                END AS peerSetNumerator,
                CASE
                    WHEN fourWeekTrendWeekCount=4
                     AND metricKind='ratio'
                     AND nullif(fourWeekTrendDenominator,0D) IS NOT NULL
                     AND nullif(peerCurrentDenominator,0D) IS NOT NULL
                     AND nullif(peerFourWeekDenominator,0D) IS NOT NULL
                        THEN fourWeekTrendDenominator
                    ELSE cast(NULL AS DOUBLE)
                END AS peerSetDenominator,
                v_processedAt AS goldProcessedAt
            FROM assembled
        )
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
        REPLACE WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        SELECT * FROM finalRows;
        SELECT
            'SUCCESS' AS status,
            v_weekFrom AS rebuiltWeekStartFrom,
            v_weekTo AS rebuiltWeekStartTo,
            v_weekEndTo AS latestWeekEndDate,
            CASE WHEN v_asOfDate < v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long' AS targetObject;
    END IF;
END;
-- ============================================================================
-- DEVELOPMENT / TEST EXAMPLES
-- ============================================================================
-- A. PREFLIGHT ONLY
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_breakoutMetricIngredientsByWeek_long(
--     p_asOfDate       => DATE '2026-09-28',
--     p_weeksToRebuild => 1,
--     p_validateOnly   => TRUE
-- );
-- B. EXECUTE / REBUILD
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_breakoutMetricIngredientsByWeek_long(
--     p_asOfDate       => DATE '2026-09-28',
--     p_weeksToRebuild => 1,
--     p_validateOnly   => FALSE
-- );
-- C. VALIDATION 1: GRAIN UNIQUENESS
-- Expected: no rows.
-- SELECT
--     targetWeekStartDate,breakoutType,breakoutValue,metricName,COUNT(*) AS rowCount
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
-- WHERE targetWeekStartDate = DATE '2026-09-27'
-- GROUP BY targetWeekStartDate,breakoutType,breakoutValue,metricName
-- HAVING COUNT(*) > 1
-- ORDER BY rowCount DESC
-- LIMIT 100;
-- D. VALIDATION 2: NBV ADDITIVITY BY BREAKOUT
-- Every visitor receives exactly one weekly attributed value per breakout type.
-- Expected: nbvDiff = 0 for every breakoutType.
-- WITH total AS (
--     SELECT SUM(nbv) AS nbv
--     FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly
--     WHERE weekStartDate = DATE '2026-09-27'
-- ),
-- byBreakout AS (
--     SELECT breakoutType,SUM(thisWeekNumerator) AS nbv
--     FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
--     WHERE targetWeekStartDate = DATE '2026-09-27'
--       AND metricName = 'nbv'
--     GROUP BY breakoutType
-- )
-- SELECT breakoutType,b.nbv-t.nbv AS nbvDiff
-- FROM byBreakout b CROSS JOIN total t
-- ORDER BY breakoutType;
-- E. VALIDATION 3: CONTROL-CATALOG ELIGIBILITY
-- Expected: invalidRows = 0.
-- SELECT COUNT(*) AS invalidRows
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long g
-- LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static b
--   ON b.breakoutType=g.breakoutType
-- WHERE g.targetWeekStartDate = DATE '2026-09-27'
--   AND (b.breakoutType IS NULL OR NOT b.isActive OR NOT b.isPrebuiltBreakout);

-- --------------------------------------------------------------------------
-- F. VALIDATION 4: PEER COUNTERFACTUAL CONTRACT
-- Expected:
--   peerWithoutFullFourWeeks = 0
--   countPeerWithDenominator = 0
--   ratioPeerMissingDenominator = 0
-- --------------------------------------------------------------------------
-- SELECT
--     COUNT_IF(fourWeekTrendWeekCount<>4 AND peerSetNumerator IS NOT NULL) AS peerWithoutFullFourWeeks,
--     COUNT_IF(metricKind='count' AND peerSetDenominator IS NOT NULL) AS countPeerWithDenominator,
--     COUNT_IF(metricKind='ratio' AND peerSetNumerator IS NOT NULL AND peerSetDenominator IS NULL) AS ratioPeerMissingDenominator
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
-- WHERE targetWeekStartDate = DATE '2026-09-27';

-- --------------------------------------------------------------------------
-- G. VALIDATION 5: CHANNEL TAXONOMY REMAINS UNROLLED
-- Informational. Paid Search subtypes must remain distinct when present.
-- --------------------------------------------------------------------------
-- SELECT breakoutValue,COUNT(*) AS metricRows
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
-- WHERE targetWeekStartDate = DATE '2026-09-27'
--   AND breakoutType='channel'
--   AND breakoutValue LIKE 'Paid Search:%'
-- GROUP BY breakoutValue
-- ORDER BY breakoutValue;
