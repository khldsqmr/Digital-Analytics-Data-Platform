-- ============================================================================

-- FILE  : 03_sdi_sp_mip_gold_crosstabMetricIngredientsByWeek_long.sql

-- LAYER : GOLD
--
-- RUNTIME WRITE NOTE:
--   Uses static SQL with a scoped MERGE, matching the proven Silver execution pattern.
--   No EXECUTE IMMEDIATE, REPLACE WHERE, REPLACE USING, or dynamic DATE rendering.
--   Procedure-local week DATE variables are used directly in normal source filters
--   and in the bounded MERGE delete condition.
--
-- CALL COMPATIBILITY:
--   - Manual SQL: CALL with DATE / INT / BOOLEAN literals.
--   - Notebook: render only already-validated Python date/int values as SQL literals
--     in CALL because this runtime requires foldable stored-procedure arguments.
--

-- PURPOSE:

--   Gold crosstab comparison ingredients by target week, supported pair cell, and metric.

--

-- DESIGN:

--   - One top-level CREATE OR REPLACE PROCEDURE per file.

--   - No run/job ID dependency during development.

--   - Uses control VIEWS, not persisted control tables.

--   - Validates required sources/control metadata before target creation/write.

--   - p_validateOnly = TRUE performs preflight only.

--   - Default as-of date is the previous Pacific calendar day.

--   - Comparison percentages are NOT persisted; Gold stores ingredients.

--   - PERFORMANCE: visitor-week Silver is restricted to target/prior/4-week/LY weeks before pair expansion.

--   - PERFORMANCE: only dimensions required by the crosstab dimension map are projected.

--   - Every isActive row in sdi_vw_mip_control_crosstabCatalog_static is generated here.

--   - isPrebuiltPair is App/UI metadata only and is intentionally not persisted in analytical Gold.

--   - PEER SET: for a selected pair cell, peers are every OTHER real cell in the

--     same pairKey. This definition is symmetric and remains valid if the UI flips

--     row/column orientation.

--   - Peer membership is rebuilt from session/page-category Silver for the required

--     weeks only; no hit-level scan is required.

--   - PEER BASIS: always the four-week trend, independent of the screen comparator.

--   - peerSetNumerator/peerSetDenominator store the selected cell's COUNTERFACTUAL

--     current value if it had moved at the peer-set rate.

-- ============================================================================

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_gold_crosstabMetricIngredientsByWeek_long(

    IN p_asOfDate       DATE    DEFAULT NULL,

    IN p_weeksToRebuild INT     DEFAULT 1,

    IN p_validateOnly   BOOLEAN DEFAULT FALSE

)

LANGUAGE SQL

SQL SECURITY INVOKER

MODIFIES SQL DATA

COMMENT 'Gold crosstab comparison ingredients by target week, supported pair cell, and metric.'

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

        FROM prdrzranalytics.lab42.sdi_vw_mip_control_crosstabCatalog_static

        WHERE isActive

        LIMIT 1

    ) THEN

        SIGNAL SQLSTATE '45000'

            SET MESSAGE_TEXT = 'Crosstab catalog control view has no active pairs.';

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

        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long (

targetWeekStartDate         DATE,

  targetWeekEndDate           DATE,

  fiscalQuarterLabel          STRING,

  fiscalWeekCode              STRING,

  weekLabel                   STRING,

  filterLob                   STRING COMMENT 'All for now; reserved for precomputed whole-report filter contexts',

  filterPlatform              STRING COMMENT 'All for now; reserved for precomputed whole-report filter contexts',

  pairKey                     STRING,

  pairLabel                   STRING,

  rowBreakoutType             STRING,

  rowBreakoutValue            STRING,

  columnBreakoutType          STRING,

  columnBreakoutValue         STRING,

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

  peerSetNumerator            DOUBLE COMMENT 'Counterfactual expected cell numerator if the selected pair cell had moved at its four-week peer-set rate; NULL unless the full four-week peer window is available',

  peerSetDenominator          DOUBLE COMMENT 'Counterfactual expected cell denominator for ratio metrics; NULL for count metrics',

  goldProcessedAt             TIMESTAMP

        )

        USING DELTA

        CLUSTER BY (targetWeekStartDate, pairKey, metricName)

        COMMENT 'Gold Crosstabs: supported attributed breakout-pair cells with safe comparison ingredients.';

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

        -- A. EXISTING ATTRIBUTED CROSSTAB ACTUALS

        --

        -- Preserve the existing API-facing cell contract. Each visitor/week has

        -- one attributed value for each dimension, so Top-N and synthetic (Other)

        -- behavior downstream remain unchanged.

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

                a.pageCategory,

                a.device,

                a.region,

                a.utmSource,

                a.utmMedium,

                a.utmCampaign,

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

                x.orderCount,

                map(

                    'lob',            coalesce(a.lob,'Other'),

                    'platform',       coalesce(a.platform,'(not set)'),

                    'prospectVsBase', coalesce(a.prospectVsBase,'Unknown'),

                    'authState',      coalesce(a.authState,'(not set)'),

                    'channel',        coalesce(a.channel,'(not set)'),

                    'campaign',       coalesce(a.campaign,'(not set)'),

                    'entryPage',      coalesce(a.entryPage,'(not set)'),

                    'pageCategory',   coalesce(a.pageCategory,'(not set)'),

                    'device',         coalesce(a.device,'Unknown'),

                    'region',         coalesce(a.region,'(not available)'),

                    'utmSource',      coalesce(a.utmSource,'(not set)'),

                    'utmMedium',      coalesce(a.utmMedium,'(not set)'),

                    'utmCampaign',    coalesce(a.utmCampaign,'(not set)'),

                    'buyFlowStep',    coalesce(a.buyFlowStep,'Did not enter buy flow')

                ) AS dimensionMap

            FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly a

            INNER JOIN prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly x

              ON x.weekStartDate=a.weekStartDate

             AND x.visitorId=a.visitorId

            WHERE a.weekStartDate IN (SELECT weekStartDate FROM requiredWeeks)

        ),

        pairRows AS (

            SELECT

                v.weekStartDate,

                p.pairKey,

                p.pairLabel,

                p.rowBreakoutType,

                element_at(v.dimensionMap,p.rowBreakoutType) AS rowBreakoutValue,

                p.columnBreakoutType,

                element_at(v.dimensionMap,p.columnBreakoutType) AS columnBreakoutValue,

                v.nbv,

                v.sessionCount,

                v.pageViews,

                v.nbvBuyFlow,

                v.nbvConfigure,

                v.nbvCheckoutStart,

                v.orders,

                v.ordersAcquisition,

                v.ordersBase,

                v.ordersUnassisted,

                v.ordersAssisted,

                v.vrCalls,

                v.vrChats,

                v.storeLocator,

                v.orderCount

            FROM visitorWeek v

            INNER JOIN prdrzranalytics.lab42.sdi_vw_mip_control_crosstabCatalog_static p

              ON p.isActive

        ),

        weeklyWide AS (

            SELECT

                weekStartDate,

                pairKey,

                MAX(pairLabel) AS pairLabel,

                MAX(rowBreakoutType) AS rowBreakoutType,

                rowBreakoutValue,

                MAX(columnBreakoutType) AS columnBreakoutType,

                columnBreakoutValue,

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

            FROM pairRows

            GROUP BY weekStartDate,pairKey,rowBreakoutValue,columnBreakoutValue

        ),

        weeklyCounts AS (

            SELECT

                weekStartDate,

                pairKey,

                pairLabel,

                rowBreakoutType,

                rowBreakoutValue,

                columnBreakoutType,

                columnBreakoutValue,

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

                c.pairKey,

                c.pairLabel,

                c.rowBreakoutType,

                c.rowBreakoutValue,

                c.columnBreakoutType,

                c.columnBreakoutValue,

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

                n.pairKey,

                MAX(n.pairLabel) AS pairLabel,

                MAX(n.rowBreakoutType) AS rowBreakoutType,

                n.rowBreakoutValue,

                MAX(n.columnBreakoutType) AS columnBreakoutType,

                n.columnBreakoutValue,

                r.metricName,

                MAX(CASE WHEN n.metricName=r.numeratorMetric THEN n.metricValue END) AS numeratorValue,

                MAX(CASE WHEN n.metricName=r.denominatorMetric THEN n.metricValue END) AS denominatorValue

            FROM weeklyCounts n

            INNER JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static r

              ON r.metricKind='ratio'

             AND r.isActive

             AND n.metricName IN (r.numeratorMetric,r.denominatorMetric)

            GROUP BY n.weekStartDate,n.pairKey,n.rowBreakoutValue,n.columnBreakoutValue,r.metricName

        ),

        availableWeeks AS (

            SELECT DISTINCT weekStartDate

            FROM visitorWeek

        ),

        -- --------------------------------------------------------------------

        -- B. OVERLAPPING CROSSTAB PEER MEMBERSHIP

        --

        -- Peer definition is intentionally symmetric and orientation-independent:

        -- for a selected pair cell, the peer set is every OTHER cell in the same

        -- pairKey. This remains correct if the UI later flips row/column display.

        --

        -- A visitor may belong to both the selected cell and its peer set if the

        -- visitor qualifies through another real pair cell during the week.

        -- --------------------------------------------------------------------

        peerSessionBase AS (

            SELECT

                sessionId,

                visitorId,

                weekStartDate,

                pageViews,

                coalesce(authState,'(not set)') AS authState,

                coalesce(channel,'(not set)') AS channel,

                coalesce(entryPage,'(not set)') AS entryPage,

                coalesce(utmSource,'(not set)') AS utmSource,

                coalesce(utmMedium,'(not set)') AS utmMedium,

                coalesce(utmCampaign,'(not set)') AS utmCampaign,

                coalesce(device,'Unknown') AS device,

                deepestBuyFlowStep AS buyFlowStep,

                hasBuyFlow,

                hasConfigure,

                hasCheckoutStart,

                hasOrder,

                hasAcquisitionOrder,

                hasAssistedOrder,

                hasVrCall,

                hasVrChat,

                hasStoreLocator,

                orderCount,

                map(

                    'authState',   coalesce(authState,'(not set)'),

                    'channel',     coalesce(channel,'(not set)'),

                    'entryPage',   coalesce(entryPage,'(not set)'),

                    'utmSource',   coalesce(utmSource,'(not set)'),

                    'utmMedium',   coalesce(utmMedium,'(not set)'),

                    'utmCampaign', coalesce(utmCampaign,'(not set)'),

                    'device',      coalesce(device,'Unknown'),

                    'buyFlowStep', coalesce(deepestBuyFlowStep,'Did not enter buy flow')

                ) AS dimensionMap

            FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily

            WHERE weekStartDate IN (SELECT weekStartDate FROM requiredWeeks)

              AND visitorId IS NOT NULL

        ),

        peerScalarPairRows AS (

            SELECT

                s.sessionId,

                s.visitorId,

                s.weekStartDate,

                p.pairKey,

                p.pairLabel,

                p.rowBreakoutType,

                element_at(s.dimensionMap,p.rowBreakoutType) AS rowBreakoutValue,

                p.columnBreakoutType,

                element_at(s.dimensionMap,p.columnBreakoutType) AS columnBreakoutValue,

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

            INNER JOIN prdrzranalytics.lab42.sdi_vw_mip_control_crosstabCatalog_static p

              ON p.isActive

             AND p.pairKey<>'buyFlowStep__authState'

        ),

        peerBuyFlowMapped AS (

            SELECT

                a.sessionId,

                a.visitorId,

                a.weekStartDate,

                'buyFlowStep__authState' AS pairKey,

                'Buy flow step × Visitor type' AS pairLabel,

                'buyFlowStep' AS rowBreakoutType,

                coalesce(a.buyFlowStep,'(buy flow - step not mapped)') AS rowBreakoutValue,

                'authState' AS columnBreakoutType,

                coalesce(s.authState,'(not set)') AS columnBreakoutValue,

                cast(1 AS BIGINT) AS sessionCount,

                SUM(a.pageViews) AS pageViews,

                MAX(a.hasBuyFlow) AS hasBuyFlow,

                MAX(CASE WHEN a.configureEvents>0 THEN 1 ELSE 0 END) AS hasConfigure,

                MAX(CASE WHEN a.checkoutStartEvents>0 THEN 1 ELSE 0 END) AS hasCheckoutStart,

                MAX(CASE WHEN a.orderCount>0 THEN 1 ELSE 0 END) AS hasOrder,

                MAX(CASE

                    WHEN a.orderCount>0 AND lower(trim(coalesce(a.orderCustomerType,'')))='prospect' THEN 1

                    ELSE 0

                END) AS hasAcquisitionOrder,

                MAX(CASE WHEN a.assistedOrderEvents>0 THEN 1 ELSE 0 END) AS hasAssistedOrder,

                MAX(CASE WHEN a.vrCallEvents>0 THEN 1 ELSE 0 END) AS hasVrCall,

                MAX(CASE WHEN a.vrChatEvents>0 THEN 1 ELSE 0 END) AS hasVrChat,

                MAX(CASE WHEN a.storeLocatorEvents>0 THEN 1 ELSE 0 END) AS hasStoreLocator,

                SUM(a.orderCount) AS orderCount

            FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily a

            INNER JOIN peerSessionBase s

              ON s.sessionId=a.sessionId

             AND s.weekStartDate=a.weekStartDate

            WHERE a.weekStartDate IN (SELECT weekStartDate FROM requiredWeeks)

              AND a.visitorId IS NOT NULL

              AND a.buyFlowStep IS NOT NULL

            GROUP BY

                a.sessionId,a.visitorId,a.weekStartDate,

                coalesce(a.buyFlowStep,'(buy flow - step not mapped)'),

                coalesce(s.authState,'(not set)')

        ),

        peerBuyFlowMappedSessions AS (

            SELECT DISTINCT sessionId,weekStartDate

            FROM peerBuyFlowMapped

        ),

        peerBuyFlowFallback AS (

            SELECT

                s.sessionId,

                s.visitorId,

                s.weekStartDate,

                'buyFlowStep__authState' AS pairKey,

                'Buy flow step × Visitor type' AS pairLabel,

                'buyFlowStep' AS rowBreakoutType,

                CASE

                    WHEN s.hasBuyFlow=0 THEN 'Did not enter buy flow'

                    WHEN s.buyFlowStep IS NULL THEN '(buy flow - step not mapped)'

                    ELSE s.buyFlowStep

                END AS rowBreakoutValue,

                'authState' AS columnBreakoutType,

                coalesce(s.authState,'(not set)') AS columnBreakoutValue,

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

            LEFT JOIN peerBuyFlowMappedSessions m

              ON m.sessionId=s.sessionId

             AND m.weekStartDate=s.weekStartDate

            WHERE m.sessionId IS NULL

        ),

        peerPairRows AS (

            SELECT * FROM peerScalarPairRows

            UNION ALL

            SELECT * FROM peerBuyFlowMapped

            UNION ALL

            SELECT * FROM peerBuyFlowFallback

        ),

        peerVisitorPairPrimitive AS (

            SELECT

                visitorId,

                weekStartDate,

                pairKey,

                MAX(pairLabel) AS pairLabel,

                MAX(rowBreakoutType) AS rowBreakoutType,

                rowBreakoutValue,

                MAX(columnBreakoutType) AS columnBreakoutType,

                columnBreakoutValue,

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

            FROM peerPairRows

            GROUP BY visitorId,weekStartDate,pairKey,rowBreakoutValue,columnBreakoutValue

        ),

        peerVisitorPair AS (

            SELECT

                *,

                cast(1 AS INT) AS nbv,

                greatest(orders-ordersAcquisition,0) AS ordersBase,

                greatest(orders-ordersAssisted,0) AS ordersUnassisted

            FROM peerVisitorPairPrimitive

        ),

        peerUniqueLong AS (

            SELECT

                visitorId,

                weekStartDate,

                pairKey,

                rowBreakoutValue,

                columnBreakoutValue,

                metricName,

                metricFlag

            FROM peerVisitorPair

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

        peerPairUniverse AS (

            SELECT DISTINCT pairKey,rowBreakoutValue,columnBreakoutValue FROM weeklyWide

            UNION

            SELECT DISTINCT pairKey,rowBreakoutValue,columnBreakoutValue FROM peerVisitorPair

        ),

        peerWeekGrid AS (

            SELECT

                w.weekStartDate,

                u.pairKey,

                u.rowBreakoutValue,

                u.columnBreakoutValue

            FROM availableWeeks w

            CROSS JOIN peerPairUniverse u

            INNER JOIN prdrzranalytics.lab42.sdi_vw_mip_control_crosstabCatalog_static p

              ON p.pairKey=u.pairKey

             AND p.isActive

        ),

        peerUniqueContext AS (

            SELECT

                weekStartDate,

                pairKey,

                metricName,

                COUNT(DISTINCT CASE WHEN metricFlag=1 THEN visitorId END) AS contextValue

            FROM peerUniqueLong

            GROUP BY weekStartDate,pairKey,metricName

        ),

        peerUniqueQualifying AS (

            SELECT

                visitorId,

                weekStartDate,

                pairKey,

                metricName,

                COUNT(*) AS qualifyingCellCount,

                first(rowBreakoutValue) AS soleRowBreakoutValue,

                first(columnBreakoutValue) AS soleColumnBreakoutValue

            FROM peerUniqueLong

            WHERE metricFlag=1

            GROUP BY visitorId,weekStartDate,pairKey,metricName

        ),

        peerUniqueExclusive AS (

            SELECT

                weekStartDate,

                pairKey,

                metricName,

                soleRowBreakoutValue AS rowBreakoutValue,

                soleColumnBreakoutValue AS columnBreakoutValue,

                COUNT(*) AS exclusiveValue

            FROM peerUniqueQualifying

            WHERE qualifyingCellCount=1

            GROUP BY

                weekStartDate,pairKey,metricName,

                soleRowBreakoutValue,soleColumnBreakoutValue

        ),

        peerUniqueByCell AS (

            SELECT

                g.weekStartDate,

                g.pairKey,

                g.rowBreakoutValue,

                g.columnBreakoutValue,

                c.metricName,

                cast(greatest(c.contextValue-coalesce(e.exclusiveValue,0),0) AS DOUBLE) AS metricValue

            FROM peerWeekGrid g

            INNER JOIN peerUniqueContext c

              ON c.weekStartDate=g.weekStartDate

             AND c.pairKey=g.pairKey

            LEFT JOIN peerUniqueExclusive e

              ON e.weekStartDate=g.weekStartDate

             AND e.pairKey=g.pairKey

             AND e.metricName=c.metricName

             AND e.rowBreakoutValue=g.rowBreakoutValue

             AND e.columnBreakoutValue=g.columnBreakoutValue

        ),

        peerSessionCells AS (

            SELECT DISTINCT

                weekStartDate,

                pairKey,

                rowBreakoutValue,

                columnBreakoutValue,

                sessionId

            FROM peerPairRows

        ),

        peerSessionContext AS (

            SELECT

                weekStartDate,

                pairKey,

                COUNT(DISTINCT sessionId) AS contextValue

            FROM peerSessionCells

            GROUP BY weekStartDate,pairKey

        ),

        peerSessionQualifying AS (

            SELECT

                weekStartDate,

                pairKey,

                sessionId,

                COUNT(*) AS qualifyingCellCount,

                first(rowBreakoutValue) AS soleRowBreakoutValue,

                first(columnBreakoutValue) AS soleColumnBreakoutValue

            FROM peerSessionCells

            GROUP BY weekStartDate,pairKey,sessionId

        ),

        peerSessionExclusive AS (

            SELECT

                weekStartDate,

                pairKey,

                soleRowBreakoutValue AS rowBreakoutValue,

                soleColumnBreakoutValue AS columnBreakoutValue,

                COUNT(*) AS exclusiveValue

            FROM peerSessionQualifying

            WHERE qualifyingCellCount=1

            GROUP BY

                weekStartDate,pairKey,soleRowBreakoutValue,soleColumnBreakoutValue

        ),

        peerSessionByCell AS (

            SELECT

                g.weekStartDate,

                g.pairKey,

                g.rowBreakoutValue,

                g.columnBreakoutValue,

                cast(greatest(c.contextValue-coalesce(e.exclusiveValue,0),0) AS DOUBLE) AS metricValue

            FROM peerWeekGrid g

            INNER JOIN peerSessionContext c

              ON c.weekStartDate=g.weekStartDate

             AND c.pairKey=g.pairKey

            LEFT JOIN peerSessionExclusive e

              ON e.weekStartDate=g.weekStartDate

             AND e.pairKey=g.pairKey

             AND e.rowBreakoutValue=g.rowBreakoutValue

             AND e.columnBreakoutValue=g.columnBreakoutValue

        ),

        peerPageViewSlice AS (

            SELECT

                weekStartDate,

                pairKey,

                rowBreakoutValue,

                columnBreakoutValue,

                SUM(pageViews) AS sliceValue

            FROM peerVisitorPair

            GROUP BY weekStartDate,pairKey,rowBreakoutValue,columnBreakoutValue

        ),

        peerPageViewContext AS (

            SELECT

                weekStartDate,

                pairKey,

                SUM(sliceValue) AS contextValue

            FROM peerPageViewSlice

            GROUP BY weekStartDate,pairKey

        ),

        peerPageViewByCell AS (

            SELECT

                g.weekStartDate,

                g.pairKey,

                g.rowBreakoutValue,

                g.columnBreakoutValue,

                cast(greatest(c.contextValue-coalesce(s.sliceValue,0),0) AS DOUBLE) AS metricValue

            FROM peerWeekGrid g

            INNER JOIN peerPageViewContext c

              ON c.weekStartDate=g.weekStartDate

             AND c.pairKey=g.pairKey

            LEFT JOIN peerPageViewSlice s

              ON s.weekStartDate=g.weekStartDate

             AND s.pairKey=g.pairKey

             AND s.rowBreakoutValue=g.rowBreakoutValue

             AND s.columnBreakoutValue=g.columnBreakoutValue

        ),

        peerOrderSlice AS (

            SELECT

                weekStartDate,

                pairKey,

                rowBreakoutValue,

                columnBreakoutValue,

                SUM(orderCount) AS sliceValue

            FROM peerVisitorPair

            GROUP BY weekStartDate,pairKey,rowBreakoutValue,columnBreakoutValue

        ),

        peerOrderContext AS (

            SELECT

                weekStartDate,

                pairKey,

                SUM(sliceValue) AS contextValue

            FROM peerOrderSlice

            GROUP BY weekStartDate,pairKey

        ),

        peerOrderByCell AS (

            SELECT

                g.weekStartDate,

                g.pairKey,

                g.rowBreakoutValue,

                g.columnBreakoutValue,

                cast(greatest(c.contextValue-coalesce(s.sliceValue,0),0) AS DOUBLE) AS metricValue

            FROM peerWeekGrid g

            INNER JOIN peerOrderContext c

              ON c.weekStartDate=g.weekStartDate

             AND c.pairKey=g.pairKey

            LEFT JOIN peerOrderSlice s

              ON s.weekStartDate=g.weekStartDate

             AND s.pairKey=g.pairKey

             AND s.rowBreakoutValue=g.rowBreakoutValue

             AND s.columnBreakoutValue=g.columnBreakoutValue

        ),

        peerWeeklyCounts AS (

            SELECT weekStartDate,pairKey,rowBreakoutValue,columnBreakoutValue,metricName,metricValue

            FROM peerUniqueByCell

            UNION ALL

            SELECT weekStartDate,pairKey,rowBreakoutValue,columnBreakoutValue,'sessionCount',metricValue

            FROM peerSessionByCell

            UNION ALL

            SELECT weekStartDate,pairKey,rowBreakoutValue,columnBreakoutValue,'pageViews',metricValue

            FROM peerPageViewByCell

            UNION ALL

            SELECT weekStartDate,pairKey,rowBreakoutValue,columnBreakoutValue,'orderCount',metricValue

            FROM peerOrderByCell

        ),

        peerWeeklyIngredients AS (

            SELECT

                c.weekStartDate,

                c.pairKey,

                c.rowBreakoutValue,

                c.columnBreakoutValue,

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

                n.pairKey,

                n.rowBreakoutValue,

                n.columnBreakoutValue,

                r.metricName,

                MAX(CASE WHEN n.metricName=r.numeratorMetric THEN n.metricValue END) AS numeratorValue,

                MAX(CASE WHEN n.metricName=r.denominatorMetric THEN n.metricValue END) AS denominatorValue

            FROM peerWeeklyCounts n

            INNER JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static r

              ON r.metricKind='ratio'

             AND r.isActive

             AND n.metricName IN (r.numeratorMetric,r.denominatorMetric)

            GROUP BY

                n.weekStartDate,n.pairKey,n.rowBreakoutValue,n.columnBreakoutValue,r.metricName

        ),

        -- --------------------------------------------------------------------

        -- C. COMPARISON WINDOWS AND TARGET CELLS

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

        candidatePairs AS (

            SELECT DISTINCT

                t.weekStartDate AS targetWeekStartDate,

                h.pairKey,

                h.pairLabel,

                h.rowBreakoutType,

                h.rowBreakoutValue,

                h.columnBreakoutType,

                h.columnBreakoutValue

            FROM targetWeeks t

            INNER JOIN weeklyWide h

              ON h.weekStartDate=t.weekStartDate

              OR h.weekStartDate=t.priorWeekStartDate

              OR h.weekStartDate BETWEEN t.fourWeekAvgStartDate AND t.fourWeekAvgEndDate

              OR h.weekStartDate=t.sameWeekLastYearStartDate

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

                v.pairKey,

                v.pairLabel,

                v.rowBreakoutType,

                v.rowBreakoutValue,

                v.columnBreakoutType,

                v.columnBreakoutValue,

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

            INNER JOIN candidatePairs v

              ON v.targetWeekStartDate=t.weekStartDate

            INNER JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static m

              ON m.isActive

            LEFT JOIN weeklyIngredients cur

              ON cur.weekStartDate=t.weekStartDate

             AND cur.pairKey=v.pairKey

             AND cur.rowBreakoutValue=v.rowBreakoutValue

             AND cur.columnBreakoutValue=v.columnBreakoutValue

             AND cur.metricName=m.metricName

            LEFT JOIN weeklyIngredients pw

              ON pw.weekStartDate=t.priorWeekStartDate

             AND pw.pairKey=v.pairKey

             AND pw.rowBreakoutValue=v.rowBreakoutValue

             AND pw.columnBreakoutValue=v.columnBreakoutValue

             AND pw.metricName=m.metricName

            LEFT JOIN weeklyIngredients fw

              ON fw.weekStartDate BETWEEN t.fourWeekAvgStartDate AND t.fourWeekAvgEndDate

             AND fw.pairKey=v.pairKey

             AND fw.rowBreakoutValue=v.rowBreakoutValue

             AND fw.columnBreakoutValue=v.columnBreakoutValue

             AND fw.metricName=m.metricName

            LEFT JOIN weeklyIngredients ly

              ON ly.weekStartDate=t.sameWeekLastYearStartDate

             AND ly.pairKey=v.pairKey

             AND ly.rowBreakoutValue=v.rowBreakoutValue

             AND ly.columnBreakoutValue=v.columnBreakoutValue

             AND ly.metricName=m.metricName

            LEFT JOIN peerWeeklyIngredients peerCur

              ON peerCur.weekStartDate=t.weekStartDate

             AND peerCur.pairKey=v.pairKey

             AND peerCur.rowBreakoutValue=v.rowBreakoutValue

             AND peerCur.columnBreakoutValue=v.columnBreakoutValue

             AND peerCur.metricName=m.metricName

            LEFT JOIN peerWeeklyIngredients peerFw

              ON peerFw.weekStartDate BETWEEN t.fourWeekAvgStartDate AND t.fourWeekAvgEndDate

             AND peerFw.pairKey=v.pairKey

             AND peerFw.rowBreakoutValue=v.rowBreakoutValue

             AND peerFw.columnBreakoutValue=v.columnBreakoutValue

             AND peerFw.metricName=m.metricName

            GROUP BY

                t.weekStartDate,t.weekEndDate,t.fiscalQuarterLabel,t.fiscalWeekCode,t.weekLabel,

                v.pairKey,v.pairLabel,v.rowBreakoutType,v.rowBreakoutValue,

                v.columnBreakoutType,v.columnBreakoutValue,

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

                pairKey,

                pairLabel,

                rowBreakoutType,

                rowBreakoutValue,

                columnBreakoutType,

                columnBreakoutValue,

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

                            \+ (

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

        ,
        sourceRows AS (
            SELECT * FROM finalRows
        )
        MERGE INTO prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long AS t
        USING sourceRows AS s
          ON t.targetWeekStartDate = s.targetWeekStartDate
         AND t.filterLob = s.filterLob
         AND t.filterPlatform = s.filterPlatform
         AND t.pairKey = s.pairKey
         AND t.rowBreakoutValue = s.rowBreakoutValue
         AND t.columnBreakoutValue = s.columnBreakoutValue
         AND t.metricName = s.metricName

        WHEN MATCHED THEN
            UPDATE SET *

        WHEN NOT MATCHED THEN
            INSERT *

        WHEN NOT MATCHED BY SOURCE
         AND t.targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        THEN DELETE;

        SELECT

            'SUCCESS' AS status,

            v_weekFrom AS rebuiltWeekStartFrom,

            v_weekTo AS rebuiltWeekStartTo,

            v_weekEndTo AS latestWeekEndDate,

            CASE WHEN v_asOfDate < v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,

            'prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long' AS targetObject;

    END IF;

END;

-- ============================================================================

-- DEVELOPMENT / TEST EXAMPLES

-- ============================================================================

-- A. PREFLIGHT ONLY

-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_crosstabMetricIngredientsByWeek_long(

--     p_asOfDate       => DATE '2026-09-28',

--     p_weeksToRebuild => 1,

--     p_validateOnly   => TRUE

-- );

-- B. EXECUTE / REBUILD

-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_crosstabMetricIngredientsByWeek_long(

--     p_asOfDate       => DATE '2026-09-28',

--     p_weeksToRebuild => 1,

--     p_validateOnly   => FALSE

-- );

-- C. VALIDATION 1: CELL GRAIN UNIQUENESS

-- Expected: no rows.

-- SELECT

--     targetWeekStartDate,pairKey,rowBreakoutValue,columnBreakoutValue,metricName,

--     COUNT(*) AS rowCount

-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long

-- WHERE targetWeekStartDate = DATE '2026-09-27'

-- GROUP BY targetWeekStartDate,pairKey,rowBreakoutValue,columnBreakoutValue,metricName

-- HAVING COUNT(*) > 1

-- ORDER BY rowCount DESC

-- LIMIT 100;

-- D. VALIDATION 2: NBV ADDITIVITY BY PAIR

-- Each visitor maps to one cell for each active pair.

-- Expected: nbvDiff = 0 for every pairKey.

-- WITH total AS (

--     SELECT SUM(nbv) AS nbv

--     FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly

--     WHERE weekStartDate = DATE '2026-09-27'

-- ),

-- byPair AS (

--     SELECT pairKey,SUM(thisWeekNumerator) AS nbv

--     FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long

--     WHERE targetWeekStartDate = DATE '2026-09-27'

--       AND metricName = 'nbv'

--     GROUP BY pairKey

-- )

-- SELECT pairKey,p.nbv-t.nbv AS nbvDiff

-- FROM byPair p CROSS JOIN total t

-- ORDER BY pairKey;

-- E. VALIDATION 3: ACTIVE PAIR CATALOG

-- Expected: invalidRows = 0.

-- SELECT COUNT(*) AS invalidRows

-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long g

-- LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_crosstabCatalog_static p

--   ON p.pairKey=g.pairKey

-- WHERE g.targetWeekStartDate = DATE '2026-09-27'

--   AND (p.pairKey IS NULL OR NOT p.isActive);

-- --------------------------------------------------------------------------

-- F. VALIDATION 4: CROSSTAB PEER COUNTERFACTUAL CONTRACT

-- Expected:

--   peerWithoutFullFourWeeks = 0

--   countPeerWithDenominator = 0

--   ratioPeerMissingDenominator = 0

-- --------------------------------------------------------------------------

-- SELECT

--     COUNT_IF(fourWeekTrendWeekCount<>4 AND peerSetNumerator IS NOT NULL) AS peerWithoutFullFourWeeks,

--     COUNT_IF(metricKind='count' AND peerSetDenominator IS NOT NULL) AS countPeerWithDenominator,

--     COUNT_IF(metricKind='ratio' AND peerSetNumerator IS NOT NULL AND peerSetDenominator IS NULL) AS ratioPeerMissingDenominator

-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long

-- WHERE targetWeekStartDate = DATE '2026-09-27';

-- --------------------------------------------------------------------------

-- G. VALIDATION 5: CROSSTAB CELL GRAIN

-- Expected: no rows.

-- --------------------------------------------------------------------------

-- SELECT

--     targetWeekStartDate,pairKey,rowBreakoutValue,columnBreakoutValue,metricName,

--     COUNT(*) AS rowCount

-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long

-- WHERE targetWeekStartDate = DATE '2026-09-27'

-- GROUP BY targetWeekStartDate,pairKey,rowBreakoutValue,columnBreakoutValue,metricName

-- HAVING COUNT(*)>1

-- ORDER BY rowCount DESC;

-- ============================================================================
-- NOTEBOOK CALL EXAMPLE
-- ============================================================================
-- as_of_date: Python datetime.date
-- weeks_to_rebuild: validated Python int >= 1
--
-- call_sql = f"""
--     CALL prdrzranalytics.lab42.sdi_sp_mip_gold_crosstabMetricIngredientsByWeek_long(
--         p_asOfDate       => DATE '{as_of_date.isoformat()}',
--         p_weeksToRebuild => {int(weeks_to_rebuild)},
--         p_validateOnly   => FALSE
--     )
-- """
-- result = spark.sql(call_sql).collect()
-- ============================================================================

