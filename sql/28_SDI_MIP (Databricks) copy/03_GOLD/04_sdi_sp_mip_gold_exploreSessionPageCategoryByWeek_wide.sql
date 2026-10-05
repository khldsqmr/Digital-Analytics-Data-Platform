-- ============================================================================

-- FILE  : 04_sdi_sp_mip_gold_exploreSessionPageCategoryByWeek_wide.sql

-- LAYER : GOLD

-- PURPOSE:

--   Flexible session × page-category serving base for Explore.

--

-- NAMING:

--   ByWeek = temporal reporting context.

--   wide   = metric ingredients are stored as separate columns.

--

-- DESIGN:

--   - One top-level CREATE OR REPLACE PROCEDURE per file.

--   - No run/job ID dependency during development.

--   - Validates required Silver sources before target creation/write.

--   - p_validateOnly = TRUE performs preflight only.

--   - Default as-of date is the previous Pacific calendar day.

--   - The table is week-scoped but remains at session × page-category grain.

--   - PERFORMANCE: reads only NBV session-grain/page-category Silver; no hit-level Gold scan is required.

-- ============================================================================

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_gold_exploreSessionPageCategoryByWeek_wide(

    IN p_asOfDate       DATE    DEFAULT NULL,

    IN p_weeksToRebuild INT     DEFAULT 1,

    IN p_validateOnly   BOOLEAN DEFAULT FALSE

)

LANGUAGE SQL

SQL SECURITY INVOKER

MODIFIES SQL DATA

COMMENT 'Gold Explore session x page-category wide serving base, scoped by Sunday-Saturday reporting week.'

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

    -- 2. Source preflight

    -- ------------------------------------------------------------------------

    IF NOT EXISTS (

        SELECT 1

        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily

        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo

          AND visitorId IS NOT NULL

        LIMIT 1

    ) THEN

        SIGNAL SQLSTATE '45000'

            SET MESSAGE_TEXT = 'Silver attributesPerSession has no eligible visitor sessions for the requested Gold week range.';

    END IF;

    IF NOT EXISTS (

        SELECT 1

        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily

        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo

          AND visitorId IS NOT NULL

        LIMIT 1

    ) THEN

        SIGNAL SQLSTATE '45000'

            SET MESSAGE_TEXT = 'Silver actionsPerSessionPageCategory has no rows for the requested Gold week range.';

    END IF;

    IF NOT EXISTS (

        SELECT 1

        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily s

        JOIN prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily a

          ON a.sessionId = s.sessionId

         AND a.weekStartDate = s.weekStartDate

        WHERE s.weekStartDate BETWEEN v_weekFrom AND v_weekTo

          AND s.visitorId IS NOT NULL

        LIMIT 1

    ) THEN

        SIGNAL SQLSTATE '45000'

            SET MESSAGE_TEXT = 'Silver session attributes and session-page-category actions have no joined rows for the requested Gold week range.';

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

        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_exploreSessionPageCategoryByWeek_wide (

            weekStartDate         DATE,

            weekEndDate           DATE,

            sessionId             STRING,

            visitorId             STRING,

            lobList               ARRAY<STRING>,

            platform              STRING,

            prospectVsBase        STRING,

            authState             STRING,

            channel               STRING,

            campaign              STRING,

            entryPage             STRING,

            pageCategory          STRING,

            device                STRING,

            region                STRING,

            utmSource             STRING,

            utmMedium             STRING,

            utmCampaign           STRING,

            buyFlowStep           STRING,

            pageViews             BIGINT,

            orderCount            BIGINT,

            vrCallEvents          BIGINT,

            vrChatEvents          BIGINT,

            storeLocatorEvents    BIGINT,

            assistedOrderEvents   BIGINT,

            hasBuyFlow            INT,

            configureEvents       BIGINT,

            checkoutStartEvents   BIGINT,

            goldProcessedAt       TIMESTAMP

        )

        USING DELTA

        CLUSTER BY (weekStartDate)

        COMMENT 'Gold Explore: non-bounced session × page-category wide serving base for arbitrary filter/cross operations.';

        -- --------------------------------------------------------------------

        -- 5. Rebuild requested week-start range

        -- --------------------------------------------------------------------

        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_exploreSessionPageCategoryByWeek_wide

        REPLACE WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo

        SELECT

            s.weekStartDate,

            s.weekEndDate,

            s.sessionId,

            s.visitorId,

            s.lobList,

            s.platform,

            s.prospectVsBase,

            s.authState,

            s.channel,

            CASE

                WHEN s.campaignCode IS NULL THEN '(not set)'

                WHEN s.campaignName IS NOT NULL

                    THEN concat(s.campaignCode, ' · ', s.campaignName)

                ELSE s.campaignCode

            END AS campaign,

            s.entryPage,

            a.pageCategory,

            s.device,

            coalesce(s.region, '(not available)') AS region,

            s.utmSource,

            s.utmMedium,

            s.utmCampaign,

            CASE

                WHEN a.buyFlowStep IS NOT NULL THEN a.buyFlowStep

                WHEN s.hasBuyFlow = 0 THEN 'Did not enter buy flow'

                ELSE '(buy flow - step not mapped)'

            END AS buyFlowStep,

            a.pageViews,

            a.orderCount,

            a.vrCallEvents,

            a.vrChatEvents,

            a.storeLocatorEvents,

            a.assistedOrderEvents,

            a.hasBuyFlow,

            a.configureEvents,

            a.checkoutStartEvents,

            v_processedAt AS goldProcessedAt

        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily s

        JOIN prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily a

          ON  a.sessionId = s.sessionId

          AND a.weekStartDate = s.weekStartDate

        WHERE s.weekStartDate BETWEEN v_weekFrom AND v_weekTo

          AND a.weekStartDate BETWEEN v_weekFrom AND v_weekTo

          AND s.visitorId IS NOT NULL;

        SELECT

            'SUCCESS' AS status,

            v_weekFrom AS rebuiltWeekStartFrom,

            v_weekTo AS rebuiltWeekStartTo,

            v_weekEndTo AS latestWeekEndDate,

            CASE WHEN v_asOfDate < v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,

            'prdrzranalytics.lab42.sdi_tbl_mip_gold_exploreSessionPageCategoryByWeek_wide' AS targetObject;

    END IF;

END;

-- ============================================================================

-- DEVELOPMENT / TEST EXAMPLES

-- ============================================================================

-- A. PREFLIGHT ONLY

-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_exploreSessionPageCategoryByWeek_wide(

--     p_asOfDate       => DATE '2026-09-28',

--     p_weeksToRebuild => 1,

--     p_validateOnly   => TRUE

-- );

-- B. EXECUTE / REBUILD

-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_exploreSessionPageCategoryByWeek_wide(

--     p_asOfDate       => DATE '2026-09-28',

--     p_weeksToRebuild => 1,

--     p_validateOnly   => FALSE

-- );

-- C. VALIDATION 1: SESSION x PAGE-CATEGORY GRAIN

-- Expected: no rows.

-- SELECT weekStartDate,sessionId,pageCategory,COUNT(*) AS rowCount

-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_exploreSessionPageCategoryByWeek_wide

-- WHERE weekStartDate = DATE '2026-09-27'

-- GROUP BY weekStartDate,sessionId,pageCategory

-- HAVING COUNT(*) > 1

-- ORDER BY rowCount DESC

-- LIMIT 100;

-- D. VALIDATION 2: ADDITIVE RECONCILIATION TO PAGE-CATEGORY SILVER

-- Expected: pageViewDiff/orderCountDiff = 0.

-- WITH silver AS (

--     SELECT SUM(pageViews) AS pageViews,SUM(orderCount) AS orderCount

--     FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily

--     WHERE weekStartDate = DATE '2026-09-27' AND visitorId IS NOT NULL

-- ),

-- gold AS (

--     SELECT SUM(pageViews) AS pageViews,SUM(orderCount) AS orderCount

--     FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_exploreSessionPageCategoryByWeek_wide

--     WHERE weekStartDate = DATE '2026-09-27'

-- )

-- SELECT

--     g.pageViews-s.pageViews AS pageViewDiff,

--     g.orderCount-s.orderCount AS orderCountDiff

-- FROM gold g CROSS JOIN silver s;

-- E. VALIDATION 3: NBV SESSION MEMBERSHIP

-- Expected: invalidSessions = 0.

-- SELECT COUNT(*) AS invalidSessions

-- FROM (

--     SELECT DISTINCT g.sessionId

--     FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_exploreSessionPageCategoryByWeek_wide g

--     LEFT JOIN prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily s

--       ON s.sessionId=g.sessionId AND s.weekStartDate=g.weekStartDate

--     WHERE g.weekStartDate = DATE '2026-09-27'

--       AND s.sessionId IS NULL

-- );
