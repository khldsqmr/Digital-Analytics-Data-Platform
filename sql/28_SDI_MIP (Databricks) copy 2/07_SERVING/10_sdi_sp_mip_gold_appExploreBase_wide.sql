-- ============================================================================
-- RUNTIME-SAFE WRITE REVISION:
--   - Persisted table schema/API contract unchanged.
--   - Original comparison formulas retained, including existing 4-week behavior.
--   - Scoped INSERT ... REPLACE WHERE replaced with static MERGE.
--   - Metric Catalog is authoritative for metricLabel; NBV displays as Total NBV.

-- FILE  : 10_sdi_sp_mip_gold_appExploreBase_wide.sql

-- LAYER : GOLD / APP

-- TAB   : Explore

-- PURPOSE:

--   Canonical dynamic Explore source at week × session × page-category grain.

--   The API applies arbitrary filters first, then performs selected Rows × Columns

--   aggregation, comparator math and Top-N in Databricks SQL.

--

-- IMPORTANT:

--   - Do NOT precompute every filter / row / column combination here.

--   - Do NOT precompute Top-N here; Top-N must be calculated AFTER Explore filters.

--   - Quick Filters are presets over the same underlying dimensions, not extra facts.

--   - Comparator anchors are persisted so priorWeek / fourWeek / lastYear can be

--     resolved without rebuilding business calendar logic in the API.

--   - Historical weeks required by the selected comparator must exist in this base.

-- ============================================================================



CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_gold_appExploreBase_wide(

    IN p_asOfDate DATE DEFAULT NULL,

    IN p_weeksToRebuild INT DEFAULT 1,

    IN p_validateOnly BOOLEAN DEFAULT FALSE

)

LANGUAGE SQL

SQL SECURITY INVOKER

MODIFIES SQL DATA

COMMENT 'MIP Gold App: canonical dynamic Explore base; filter first, then aggregate Rows × Columns in Databricks SQL.'

AS

BEGIN

    DECLARE v_asOfDate DATE DEFAULT coalesce(

        p_asOfDate,

        date_add(to_date(from_utc_timestamp(current_timestamp(),'America/Los_Angeles')),-1)

    );

    DECLARE v_weekTo DATE;

    DECLARE v_weekFrom DATE;

    DECLARE v_weekEndTo DATE;

    DECLARE v_processedAt TIMESTAMP DEFAULT current_timestamp();



    IF p_weeksToRebuild IS NULL OR p_weeksToRebuild<1 THEN

        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='p_weeksToRebuild must be >= 1.';

    END IF;



    SET v_weekTo=date_add(v_asOfDate,1-dayofweek(v_asOfDate));

    SET v_weekFrom=date_add(v_weekTo,-7*(p_weeksToRebuild-1));

    SET v_weekEndTo=date_add(v_weekTo,6);



    IF NOT EXISTS(

        SELECT 1

        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_exploreSessionPageCategoryByWeek_wide

        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo

        LIMIT 1

    ) THEN

        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Explore Gold analytical base has no rows for the requested App week range.';

    END IF;



    IF NOT EXISTS(

        SELECT 1

        FROM prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static

        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo

        LIMIT 1

    ) THEN

        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Fiscal Calendar has no rows for the requested App week range.';

    END IF;



    IF EXISTS(

        SELECT 1

        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_exploreSessionPageCategoryByWeek_wide g

        LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c

          ON c.weekStartDate=g.weekStartDate

        WHERE g.weekStartDate BETWEEN v_weekFrom AND v_weekTo

          AND c.weekStartDate IS NULL

        LIMIT 1

    ) THEN

        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Explore Gold contains weekStartDate values missing from Fiscal Calendar.';

    END IF;



    IF EXISTS(

        SELECT 1

        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_exploreSessionPageCategoryByWeek_wide

        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo

        GROUP BY weekStartDate,sessionId,pageCategory

        HAVING count(*)>1

        LIMIT 1

    ) THEN

        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Duplicate Explore Gold analytical grain detected at weekStartDate × sessionId × pageCategory.';

    END IF;



    IF p_validateOnly THEN

        SELECT

            'VALIDATION_ONLY' AS status,

            v_weekFrom AS rebuildWeekStartFrom,

            v_weekTo AS rebuildWeekStartTo,

            v_weekEndTo AS latestWeekEndDate,

            CASE WHEN v_asOfDate<v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,

            'priorWeek | fourWeek | lastYear' AS supportedComparisons,

            'Filter first → aggregate → rank → Top-N' AS exploreProcessingRule,

            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreBase_wide' AS targetObject,

            'Validation passed. No Gold App table was created or modified.' AS message;

    ELSE

        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreBase_wide

        USING DELTA

        CLUSTER BY (weekStartDate,platform,pageCategory)

        COMMENT 'MIP Gold App: canonical dynamic Explore base at week × session × page-category grain.'

        AS

        SELECT *

        FROM(

            SELECT

                g.weekStartDate,g.weekEndDate,

                c.fiscalYear,c.fiscalQuarter,c.fiscalQuarterLabel,c.fiscalWeekOfQuarter,c.fiscalWeekCode,c.weekLabel,c.weekEndingLabel,

                c.priorWeekStartDate,c.fourWeekAvgStartDate,c.fourWeekAvgEndDate,c.sameWeekLastYearStartDate,

                g.sessionId,g.visitorId,

                g.lobList,

                coalesce(g.platform,'(not set)') AS platform,

                coalesce(g.prospectVsBase,'Unknown') AS prospectVsBase,

                coalesce(g.authState,'(not set)') AS authState,

                coalesce(g.channel,'(not set)') AS channel,

                coalesce(g.campaign,'(not set)') AS campaign,

                coalesce(g.entryPage,'(not set)') AS entryPage,

                coalesce(g.pageCategory,'(not set)') AS pageCategory,

                coalesce(g.device,'Unknown') AS device,

                coalesce(g.region,'(not available)') AS region,

                coalesce(g.utmSource,'(not set)') AS utmSource,

                coalesce(g.utmMedium,'(not set)') AS utmMedium,

                coalesce(g.utmCampaign,'(not set)') AS utmCampaign,

                coalesce(g.buyFlowStep,'Did not enter buy flow') AS buyFlowStep,

                g.pageViews,g.orderCount,g.vrCallEvents,g.vrChatEvents,g.storeLocatorEvents,g.assistedOrderEvents,

                g.hasBuyFlow,g.configureEvents,g.checkoutStartEvents,

                CASE WHEN coalesce(g.orderCount,0)>0 THEN 1 ELSE 0 END AS hasOrder,

                1 AS sessionPageCategoryRow,

                g.goldProcessedAt,

                v_processedAt AS appProcessedAt

            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_exploreSessionPageCategoryByWeek_wide g

            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c

              ON c.weekStartDate=g.weekStartDate

            WHERE g.weekStartDate BETWEEN v_weekFrom AND v_weekTo

        ) bootstrap

        WHERE 1=0;



        WITH sourceRows AS (
SELECT

            g.weekStartDate,g.weekEndDate,

            c.fiscalYear,c.fiscalQuarter,c.fiscalQuarterLabel,c.fiscalWeekOfQuarter,c.fiscalWeekCode,c.weekLabel,c.weekEndingLabel,

            c.priorWeekStartDate,c.fourWeekAvgStartDate,c.fourWeekAvgEndDate,c.sameWeekLastYearStartDate,

            g.sessionId,g.visitorId,

            g.lobList,

            coalesce(g.platform,'(not set)') AS platform,

            coalesce(g.prospectVsBase,'Unknown') AS prospectVsBase,

            coalesce(g.authState,'(not set)') AS authState,

            coalesce(g.channel,'(not set)') AS channel,

            coalesce(g.campaign,'(not set)') AS campaign,

            coalesce(g.entryPage,'(not set)') AS entryPage,

            coalesce(g.pageCategory,'(not set)') AS pageCategory,

            coalesce(g.device,'Unknown') AS device,

            coalesce(g.region,'(not available)') AS region,

            coalesce(g.utmSource,'(not set)') AS utmSource,

            coalesce(g.utmMedium,'(not set)') AS utmMedium,

            coalesce(g.utmCampaign,'(not set)') AS utmCampaign,

            coalesce(g.buyFlowStep,'Did not enter buy flow') AS buyFlowStep,

            g.pageViews,g.orderCount,g.vrCallEvents,g.vrChatEvents,g.storeLocatorEvents,g.assistedOrderEvents,

            g.hasBuyFlow,g.configureEvents,g.checkoutStartEvents,

            CASE WHEN coalesce(g.orderCount,0)>0 THEN 1 ELSE 0 END AS hasOrder,

            1 AS sessionPageCategoryRow,

            g.goldProcessedAt,

            v_processedAt AS appProcessedAt

        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_exploreSessionPageCategoryByWeek_wide g

        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c

          ON c.weekStartDate=g.weekStartDate

        WHERE g.weekStartDate BETWEEN v_weekFrom AND v_weekTo
        )
        MERGE INTO prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreBase_wide AS t
        USING sourceRows AS s
          ON t.weekStartDate = s.weekStartDate
         AND t.sessionId <=> s.sessionId
         AND t.pageCategory <=> s.pageCategory
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        WHEN NOT MATCHED BY SOURCE
         AND t.weekStartDate BETWEEN v_weekFrom AND v_weekTo
        THEN DELETE;



        SELECT

            'SUCCESS' AS status,

            v_weekFrom AS rebuiltWeekStartFrom,

            v_weekTo AS rebuiltWeekStartTo,

            v_weekEndTo AS latestWeekEndDate,

            CASE WHEN v_asOfDate<v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,

            'priorWeek | fourWeek | lastYear' AS supportedComparisons,

            'Filter first → aggregate → rank → Top-N' AS exploreProcessingRule,

            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreBase_wide' AS targetObject,

            v_processedAt AS appProcessedAt;

    END IF;

END;



-- EXPLORE AXIS OPTIONS:

-- SELECT breakoutType,breakoutLabel,sortOrder

-- FROM prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static

-- WHERE isActive AND isExploreDimension

--   AND breakoutType IN('channel','authState','prospectVsBase','entryPage','pageCategory','device',

--                       'utmSource','utmMedium','utmCampaign','buyFlowStep','campaign','platform')

-- ORDER BY sortOrder;

--

-- IMPORTANT:

-- Do not expose region while inactive/placeholder.

-- Do not expose LOB as a simple scalar axis while Explore stores lobList ARRAY<STRING>.

-- LOB may still be supported as a filter via array_contains(lobList,?).

--

-- INITIAL HISTORY:

-- priorWeek / fourWeek / lastYear comparisons require the corresponding historical weeks

-- to exist in this App base. Seed enough history before production; routine runs can then

-- continue with a small p_weeksToRebuild window.

-- ============================================================================

-- DEVELOPMENT / DEPLOYMENT EXAMPLES

-- ============================================================================



-- Preflight only:

-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appExploreBase_wide(

--     p_asOfDate=>DATE '2026-09-28',

--     p_weeksToRebuild=>1,

--     p_validateOnly=>TRUE

-- );



-- Rebuild latest week:

-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appExploreBase_wide(

--     p_asOfDate=>DATE '2026-09-28',

--     p_weeksToRebuild=>1,

--     p_validateOnly=>FALSE

-- );



-- Backfill / rebuild multiple weeks:

-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appExploreBase_wide(

--     p_asOfDate=>DATE '2026-09-28',

--     p_weeksToRebuild=>12,

--     p_validateOnly=>FALSE

-- );



-- Validate loaded week counts:

-- SELECT weekStartDate,count(*) AS rows

-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreBase_wide

-- GROUP BY weekStartDate

-- ORDER BY weekStartDate DESC;



-- Validate canonical grain; expected result = 0 rows:

-- SELECT weekStartDate,sessionId,pageCategory,count(*) AS rowCount

-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreBase_wide

-- GROUP BY weekStartDate,sessionId,pageCategory

-- HAVING count(*)>1

-- ORDER BY rowCount DESC;



-- Explore axis options:

-- SELECT breakoutType,breakoutLabel,sortOrder

-- FROM prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static

-- WHERE isActive

--   AND isExploreDimension

--   AND breakoutType IN(

--       'channel','authState','prospectVsBase','entryPage','pageCategory',

--       'device','utmSource','utmMedium','utmCampaign','buyFlowStep',

--       'campaign','platform'

--   )

-- ORDER BY sortOrder;



-- LOB filter example:

-- WHERE array_contains(lobList,?)



-- Comparator history requirement:

-- priorWeek requires the prior reporting week to be present.

-- fourWeek requires the previous four reporting weeks to be present for a complete window.

-- lastYear requires the same fiscal/reporting week last year to be present.

-- Seed sufficient history before production; normal incremental runs can remain small afterward.



-- IMPORTANT EXPLORE RULE:

-- Apply user filters FIRST, then aggregate selected Rows × Columns, then rank and apply Top-N.

-- Do not precompute every possible Explore filter / Rows / Columns combination in this table.

-- ============================================================================
-- NOTEBOOK CALL PATTERN (foldable SQL literals; do not use args= for CALL)
-- ============================================================================
-- Python:
-- from datetime import date
-- as_of_date = date(2026,10,3)
-- weeks_to_rebuild = 1
-- validate_only = False
-- validate_literal = 'TRUE' if validate_only else 'FALSE'
-- call_sql = f"""
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appExploreBase_wide(
--   p_asOfDate       => DATE '{as_of_date.isoformat()}',
--   p_weeksToRebuild => {int(weeks_to_rebuild)},
--   p_validateOnly   => {validate_literal}
-- )
-- """
-- result = spark.sql(call_sql).collect()
-- display(result)
