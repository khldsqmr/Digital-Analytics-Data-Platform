-- ============================================================================
-- FILE  : 10_sdi_sp_mip_gold_appExploreBase_wide.sql
-- LAYER : GOLD / APP
-- TAB   : Explore
-- PURPOSE:
--   Application-ready flexible Explore base at week × session × page-category grain.
--
-- DESIGN:
--   - One top-level CREATE OR REPLACE PROCEDURE per file.
--   - No app-table-to-app-table runtime dependency.
--   - Reads only reusable Gold analytical tables + control views.
--   - Incremental/idempotent at the whole reporting-week grain.
--   - p_weeksToRebuild controls the target-week slice rebuilt.
--   - p_validateOnly = TRUE performs preflight only.
--   - Default as-of date is the previous Pacific calendar day.
--   - Browser/API reads never recompute this transformation.
-- ============================================================================

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_gold_appExploreBase_wide(
    IN p_asOfDate       DATE    DEFAULT NULL,
    IN p_weeksToRebuild INT     DEFAULT 1,
    IN p_validateOnly   BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'MIP Gold app: Explore wide base. Dynamic API aggregation source; browser should not aggregate raw rows.'
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
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_exploreSessionPageCategoryByWeek_wide
        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Explore Gold analytical base has no rows for the requested app target-week range.';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static
        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Fiscal calendar control view has no rows for the requested app target-week range.';
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
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreBase_wide' AS targetObject,
            'No Gold app table was created or modified.' AS message;

    ELSE

        -- --------------------------------------------------------------------
        -- 4. Bootstrap target schema only if the table does not exist.
        --    The zero-row CTAS keeps the target schema exactly aligned to the
        --    application contract without materialized-view/serverless compute.
        -- --------------------------------------------------------------------
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreBase_wide
        USING DELTA
        CLUSTER BY (weekStartDate, platform, pageCategory)
        COMMENT 'MIP Gold app: Explore wide base. Dynamic API aggregation source; browser should not aggregate raw rows.'
        AS
        SELECT *
        FROM (
            SELECT
                appResult.*,
                v_processedAt AS appProcessedAt
            FROM (
                WITH
                scopeExplore AS (
                    SELECT *
                    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_exploreSessionPageCategoryByWeek_wide
                    WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
                )
                SELECT
                    g.weekStartDate,g.weekEndDate,
                    c.fiscalYear,c.fiscalQuarter,c.fiscalQuarterLabel,c.fiscalWeekOfQuarter,c.fiscalWeekCode,c.weekLabel,c.weekEndingLabel,
                    c.priorWeekStartDate,c.fourWeekAvgStartDate,c.fourWeekAvgEndDate,c.sameWeekLastYearStartDate,
                    g.sessionId,g.visitorId,
                    g.lobList,g.platform,g.prospectVsBase,g.authState,g.channel,g.campaign,g.entryPage,g.pageCategory,g.device,g.region,
                    g.utmSource,g.utmMedium,g.utmCampaign,g.buyFlowStep,
                    g.pageViews,g.orderCount,g.vrCallEvents,g.vrChatEvents,g.storeLocatorEvents,g.assistedOrderEvents,
                    g.hasBuyFlow,g.configureEvents,g.checkoutStartEvents,
                    CASE WHEN coalesce(g.orderCount,0)>0 THEN 1 ELSE 0 END AS hasOrder,
                    1 AS sessionPageCategoryRow,
                    g.goldProcessedAt
                FROM scopeExplore g
                LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c
                  ON c.weekStartDate=g.weekStartDate
            ) appResult
        ) schemaBootstrap
        WHERE 1 = 0;

        -- --------------------------------------------------------------------
        -- 5. Rebuild requested whole target-week range.
        --    Whole-week replacement is intentional because comparator ranks,
        --    Top-N membership and (Other) buckets can all change together.
        -- --------------------------------------------------------------------
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreBase_wide
        REPLACE WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
SELECT
    appResult.*,
    v_processedAt AS appProcessedAt
FROM (
    WITH
    scopeExplore AS (
        SELECT *
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_exploreSessionPageCategoryByWeek_wide
        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
    )
    SELECT
        g.weekStartDate,g.weekEndDate,
        c.fiscalYear,c.fiscalQuarter,c.fiscalQuarterLabel,c.fiscalWeekOfQuarter,c.fiscalWeekCode,c.weekLabel,c.weekEndingLabel,
        c.priorWeekStartDate,c.fourWeekAvgStartDate,c.fourWeekAvgEndDate,c.sameWeekLastYearStartDate,
        g.sessionId,g.visitorId,
        g.lobList,g.platform,g.prospectVsBase,g.authState,g.channel,g.campaign,g.entryPage,g.pageCategory,g.device,g.region,
        g.utmSource,g.utmMedium,g.utmCampaign,g.buyFlowStep,
        g.pageViews,g.orderCount,g.vrCallEvents,g.vrChatEvents,g.storeLocatorEvents,g.assistedOrderEvents,
        g.hasBuyFlow,g.configureEvents,g.checkoutStartEvents,
        CASE WHEN coalesce(g.orderCount,0)>0 THEN 1 ELSE 0 END AS hasOrder,
        1 AS sessionPageCategoryRow,
        g.goldProcessedAt
    FROM scopeExplore g
    LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c
      ON c.weekStartDate=g.weekStartDate
) appResult;

        -- --------------------------------------------------------------------
        -- 6. Success metadata
        -- --------------------------------------------------------------------
        SELECT
            'SUCCESS' AS status,
            v_weekFrom AS rebuiltWeekStartFrom,
            v_weekTo AS rebuiltWeekStartTo,
            v_weekEndTo AS latestWeekEndDate,
            CASE WHEN v_asOfDate < v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreBase_wide' AS targetObject,
            v_processedAt AS appProcessedAt;

    END IF;
END;

-- Development examples:
--
-- Preflight only:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appExploreBase_wide(
--     p_asOfDate       => DATE '2026-09-28',
--     p_weeksToRebuild => 1,
--     p_validateOnly   => TRUE
-- );
--
-- Load / rebuild:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appExploreBase_wide(
--     p_asOfDate       => DATE '2026-09-28',
--     p_weeksToRebuild => 1,
--     p_validateOnly   => FALSE
-- );
