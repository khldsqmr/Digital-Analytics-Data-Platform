-- ============================================================================
-- FILE  : 07_sdi_sp_mip_gold_appBreakoutsAbsoluteTrend_long.sql
-- LAYER : GOLD / APP
-- TAB   : Breakouts
-- PURPOSE:
--   Application-ready absolute breakout trend, computed directly from analytical Gold rather than sibling app tables.
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

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_gold_appBreakoutsAbsoluteTrend_long(
    IN p_asOfDate       DATE    DEFAULT NULL,
    IN p_weeksToRebuild INT     DEFAULT 1,
    IN p_validateOnly   BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'MIP Gold app: Breakouts absolute trend. Actual and benchmark series for Top5/Top10/All; All = Top100 + Other.'
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
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
        WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Breakout Gold analytical ingredients has no rows for the requested app target-week range.';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
        WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Overview Gold analytical ingredients has no rows for the requested app target-week range.';
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
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsAbsoluteTrend_long' AS targetObject,
            'No Gold app table was created or modified.' AS message;

    ELSE

        -- --------------------------------------------------------------------
        -- 4. Bootstrap target schema only if the table does not exist.
        --    The zero-row CTAS keeps the target schema exactly aligned to the
        --    application contract without materialized-view/serverless compute.
        -- --------------------------------------------------------------------
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsAbsoluteTrend_long
        USING DELTA
        CLUSTER BY (
            targetWeekStartDate,
            metricName,
            breakoutType,
            comparisonType
        )
        COMMENT 'MIP Gold app: Breakouts absolute trend. Actual and benchmark series for Top5/Top10/All; All = Top100 + Other.'
        AS
        SELECT *
        FROM (
            SELECT
                appResult.*,
                v_processedAt AS appProcessedAt
            FROM (
                WITH waterfallDirect AS (
                    WITH breakoutsComparison AS (
                        WITH
                        scopeBreakouts AS (
                            SELECT *
                            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
                            WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
                        ),
                        scopeOverview AS (
                            SELECT *
                            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
                            WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
                        ),
                        base AS (
                            SELECT
                                g.targetWeekStartDate,
                                g.targetWeekEndDate,
                                g.fiscalQuarterLabel,
                                g.fiscalWeekCode,
                                g.weekLabel,
                                c.weekEndingLabel,
                                c.priorWeekStartDate,
                                c.fourWeekAvgStartDate,
                                c.fourWeekAvgEndDate,
                                c.sameWeekLastYearStartDate,
                                g.filterLob,
                                g.filterPlatform,
                                g.breakoutType,
                                g.breakoutLabel,
                                g.breakoutValue,
                                g.valueRankByNbv AS goldValueRankByNbv,
                                g.isTopN AS goldIsConfiguredTopN,
                                bc.topN AS configuredTopN,
                                bc.pairTopN AS configuredPairTopN,
                                bc.definitionStatus AS breakoutDefinitionStatus,
                                bc.sortOrder AS breakoutSortOrder,
                                g.metricName,
                                g.metricLabel,
                                mc.metricDescription,
                                g.metricKind,
                                g.displayFormat,
                                g.changeUnit,
                                mc.definitionStatus AS metricDefinitionStatus,
                                mc.sortOrder AS metricSortOrder,
                                g.thisWeekNumerator,
                                g.thisWeekDenominator,
                                g.priorWeekNumerator,
                                g.priorWeekDenominator,
                                g.fourWeekTrendNumerator,
                                g.fourWeekTrendDenominator,
                                g.sameWeekLyNumerator,
                                g.sameWeekLyDenominator,
                                g.peerSetNumerator,
                                g.peerSetDenominator,
                                g.thisWeekDataAvailable,
                                g.priorWeekDataAvailable,
                                g.fourWeekTrendWeekCount,
                                g.sameWeekLyDataAvailable,
                                g.goldProcessedAt
                            FROM scopeBreakouts g
                            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static bc
                              ON bc.breakoutType=g.breakoutType AND bc.isActive AND bc.isPrebuiltBreakout
                            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
                              ON mc.metricName=g.metricName AND mc.isActive
                            LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c
                              ON c.weekStartDate=g.targetWeekStartDate
                        ),

                        comparisonLong AS (
                            SELECT
                                base.*,
                                'priorWeek' AS comparisonType,
                                'Prior week' AS comparisonLabel,
                                10 AS comparisonSortOrder,
                                priorWeekStartDate AS comparisonStartDate,
                                date_add(priorWeekStartDate, 6) AS comparisonEndDate,
                                1 AS comparisonWeekCount,
                                priorWeekDataAvailable AS comparisonDataAvailable,
                                priorWeekDataAvailable AS comparisonWindowComplete,
                                priorWeekNumerator AS comparisonNumerator,
                                priorWeekDenominator AS comparisonDenominator
                            FROM base

                            UNION ALL

                            SELECT
                                base.*,
                                'fourWeek' AS comparisonType,
                                '4-wk trend' AS comparisonLabel,
                                20 AS comparisonSortOrder,
                                fourWeekAvgStartDate AS comparisonStartDate,
                                fourWeekAvgEndDate AS comparisonEndDate,
                                fourWeekTrendWeekCount AS comparisonWeekCount,
                                fourWeekTrendWeekCount > 0 AS comparisonDataAvailable,
                                fourWeekTrendWeekCount = 4 AS comparisonWindowComplete,
                                CASE
                                    WHEN metricKind = 'count' AND fourWeekTrendWeekCount > 0
                                        THEN try_divide(fourWeekTrendNumerator, cast(fourWeekTrendWeekCount AS DOUBLE))
                                    ELSE fourWeekTrendNumerator
                                END AS comparisonNumerator,
                                CASE
                                    WHEN metricKind = 'count' THEN NULL
                                    ELSE fourWeekTrendDenominator
                                END AS comparisonDenominator
                            FROM base

                            UNION ALL

                            SELECT
                                base.*,
                                'lastYear' AS comparisonType,
                                'Same wk LY' AS comparisonLabel,
                                30 AS comparisonSortOrder,
                                sameWeekLastYearStartDate AS comparisonStartDate,
                                date_add(sameWeekLastYearStartDate, 6) AS comparisonEndDate,
                                1 AS comparisonWeekCount,
                                sameWeekLyDataAvailable AS comparisonDataAvailable,
                                sameWeekLyDataAvailable AS comparisonWindowComplete,
                                sameWeekLyNumerator AS comparisonNumerator,
                                sameWeekLyDenominator AS comparisonDenominator
                            FROM base
                        ),
                        valuesCalculated AS (
                            SELECT
                                *,
                                CASE
                                    WHEN metricKind = 'ratio' THEN try_divide(thisWeekNumerator, thisWeekDenominator)
                                    ELSE thisWeekNumerator
                                END AS currentValue,
                                CASE
                                    WHEN metricKind = 'ratio' THEN try_divide(comparisonNumerator, comparisonDenominator)
                                    ELSE comparisonNumerator
                                END AS comparisonValue
                            FROM comparisonLong
                        ),
                        deltasCalculated AS (
                            SELECT
                                *,
                                currentValue - comparisonValue AS absoluteDeltaValue,
                                CASE
                                    WHEN NOT thisWeekDataAvailable OR NOT comparisonDataAvailable
                                      OR currentValue IS NULL OR comparisonValue IS NULL THEN NULL
                                    WHEN changeUnit = 'pp' THEN 100D * (currentValue - comparisonValue)
                                    WHEN changeUnit = 'pct' THEN 100D * (try_divide(currentValue, comparisonValue) - 1D)
                                    ELSE NULL
                                END AS changeValue,
                                CASE
                                    WHEN currentValue IS NULL OR comparisonValue IS NULL THEN 'unavailable'
                                    WHEN currentValue > comparisonValue THEN 'up'
                                    WHEN currentValue < comparisonValue THEN 'down'
                                    ELSE 'flat'
                                END AS changeDirection
                            FROM valuesCalculated
                        )
                        ,
                        peerCalculated AS (
                            SELECT
                                d.*,
                                CASE WHEN metricKind='ratio' THEN try_divide(peerSetNumerator,peerSetDenominator)
                                     ELSE peerSetNumerator END AS peerSetValue,
                                CASE WHEN metricKind='ratio' THEN peerSetNumerator IS NOT NULL AND nullif(peerSetDenominator,0D) IS NOT NULL
                                     ELSE peerSetNumerator IS NOT NULL END AS peerSetDataAvailable
                            FROM deltasCalculated d
                        ),
                        toplineBase AS (
                            SELECT
                                g.targetWeekStartDate,
                                g.filterLob,
                                g.filterPlatform,
                                g.metricName,
                                g.metricKind,
                                g.thisWeekNumerator,
                                g.thisWeekDenominator,
                                g.priorWeekNumerator,
                                g.priorWeekDenominator,
                                g.fourWeekTrendNumerator,
                                g.fourWeekTrendDenominator,
                                g.sameWeekLyNumerator,
                                g.sameWeekLyDenominator,
                                g.fourWeekTrendWeekCount
                            FROM scopeOverview g
                        ),
                        toplineLong AS (
                            SELECT *, 'priorWeek' AS comparisonType,
                                   priorWeekNumerator AS comparisonNumerator,
                                   priorWeekDenominator AS comparisonDenominator
                            FROM toplineBase
                            UNION ALL
                            SELECT *, 'fourWeek' AS comparisonType,
                                   CASE WHEN metricKind='count' AND fourWeekTrendWeekCount>0
                                        THEN try_divide(fourWeekTrendNumerator,cast(fourWeekTrendWeekCount AS DOUBLE))
                                        ELSE fourWeekTrendNumerator END AS comparisonNumerator,
                                   CASE WHEN metricKind='count' THEN NULL ELSE fourWeekTrendDenominator END AS comparisonDenominator
                            FROM toplineBase
                            UNION ALL
                            SELECT *, 'lastYear' AS comparisonType,
                                   sameWeekLyNumerator AS comparisonNumerator,
                                   sameWeekLyDenominator AS comparisonDenominator
                            FROM toplineBase
                        ),
                        toplineValues AS (
                            SELECT
                                *,
                                CASE WHEN metricKind='ratio' THEN try_divide(thisWeekNumerator,thisWeekDenominator)
                                     ELSE thisWeekNumerator END AS toplineCurrentValue,
                                CASE WHEN metricKind='ratio' THEN try_divide(comparisonNumerator,comparisonDenominator)
                                     ELSE comparisonNumerator END AS toplineComparisonValue
                            FROM toplineLong
                        ),
                        withTopline AS (
                            SELECT
                                p.*,
                                t.toplineCurrentValue,
                                t.toplineComparisonValue,
                                t.thisWeekNumerator AS toplineCurrentNumerator,
                                t.thisWeekDenominator AS toplineCurrentDenominator,
                                t.comparisonNumerator AS toplineComparisonNumerator,
                                t.comparisonDenominator AS toplineComparisonDenominator,
                                CASE
                                    WHEN p.metricKind='count' THEN
                                        100D * try_divide(p.absoluteDeltaValue,t.toplineComparisonValue)
                                    WHEN p.metricKind='ratio' THEN
                                        100D * (
                                            try_divide(p.thisWeekNumerator,t.thisWeekDenominator)
                                            - try_divide(p.comparisonNumerator,t.comparisonDenominator)
                                        )
                                    ELSE NULL
                                END AS impactOnToplineValue,
                                CASE WHEN p.metricKind='count' THEN 'pct'
                                     WHEN p.metricKind='ratio' THEN 'pp'
                                     ELSE NULL END AS impactOnToplineUnit,
                                p.currentValue - p.peerSetValue AS peerSetAbsoluteDeltaValue,
                                CASE
                                    WHEN NOT p.peerSetDataAvailable OR p.currentValue IS NULL THEN NULL
                                    WHEN p.changeUnit='pp' THEN 100D*(p.currentValue-p.peerSetValue)
                                    WHEN p.changeUnit='pct' THEN 100D*(try_divide(p.currentValue,p.peerSetValue)-1D)
                                    ELSE NULL
                                END AS peerSetChangeValue
                            FROM peerCalculated p
                            LEFT JOIN toplineValues t
                              ON t.targetWeekStartDate=p.targetWeekStartDate
                             AND t.filterLob=p.filterLob
                             AND t.filterPlatform=p.filterPlatform
                             AND t.metricName=p.metricName
                             AND t.comparisonType=p.comparisonType
                        ),
                        ranked AS (
                            SELECT
                                *,
                                CASE WHEN comparisonDataAvailable AND impactOnToplineValue IS NOT NULL THEN
                                    row_number() OVER (
                                        PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName,breakoutType,comparisonType
                                        ORDER BY abs(impactOnToplineValue) DESC NULLS LAST,
                                                 abs(absoluteDeltaValue) DESC NULLS LAST,
                                                 breakoutValue
                                    )
                                END AS impactRankWithinBreakout,
                                CASE WHEN comparisonDataAvailable AND impactOnToplineValue IS NOT NULL THEN
                                    row_number() OVER (
                                        PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName,comparisonType
                                        ORDER BY abs(impactOnToplineValue) DESC NULLS LAST,
                                                 abs(absoluteDeltaValue) DESC NULLS LAST,
                                                 breakoutType,breakoutValue
                                    )
                                END AS impactRankAcrossBreakouts
                            FROM withTopline
                        )
                        ,
                        allBucketMembers AS (
                            SELECT
                                r.*,
                                CASE
                                    WHEN impactRankWithinBreakout <= 100
                                        THEN concat('VALUE::', coalesce(breakoutValue, '(null)'))
                                    ELSE 'OTHER::REMAINDER'
                                END AS displayBucketKey,
                                CASE
                                    WHEN impactRankWithinBreakout <= 100 THEN breakoutValue
                                    ELSE '(Other)'
                                END AS displayBreakoutValue,
                                CASE
                                    WHEN impactRankWithinBreakout <= 100 THEN FALSE
                                    ELSE TRUE
                                END AS isSyntheticOtherMember
                            FROM ranked r
                        ),
                        allBucketAgg AS (
                            SELECT
                                targetWeekStartDate,
                                targetWeekEndDate,
                                fiscalQuarterLabel,
                                fiscalWeekCode,
                                weekLabel,
                                weekEndingLabel,
                                filterLob,
                                filterPlatform,

                                breakoutType,
                                breakoutLabel,
                                breakoutSortOrder,
                                breakoutDefinitionStatus,
                                configuredTopN,
                                configuredPairTopN,

                                metricName,
                                metricLabel,
                                metricDescription,
                                metricKind,
                                displayFormat,
                                changeUnit,
                                metricSortOrder,
                                metricDefinitionStatus,

                                comparisonType,
                                comparisonLabel,
                                comparisonSortOrder,
                                comparisonStartDate,
                                comparisonEndDate,
                                comparisonWeekCount,
                                comparisonDataAvailable,
                                comparisonWindowComplete,

                                displayBucketKey,
                                displayBreakoutValue AS breakoutValue,
                                max(CASE WHEN isSyntheticOtherMember THEN 1 ELSE 0 END) = 1 AS isOtherBucket,

                                CASE
                                    WHEN max(CASE WHEN isSyntheticOtherMember THEN 1 ELSE 0 END) = 1 THEN 101
                                    ELSE min(impactRankWithinBreakout)
                                END AS displayRankWithinBreakout,

                                count(*) AS rawMemberCount,
                                min(impactRankWithinBreakout) AS rawMinImpactRankWithinBreakout,
                                max(impactRankWithinBreakout) AS rawMaxImpactRankWithinBreakout,

                                sum(thisWeekNumerator) AS currentNumerator,
                                sum(thisWeekDenominator) AS currentDenominator,
                                sum(comparisonNumerator) AS comparisonNumerator,
                                sum(comparisonDenominator) AS comparisonDenominator,

                                sum(peerSetNumerator) AS peerSetNumerator,
                                sum(peerSetDenominator) AS peerSetDenominator,

                                max(toplineCurrentNumerator) AS toplineCurrentNumerator,
                                max(toplineCurrentDenominator) AS toplineCurrentDenominator,
                                max(toplineComparisonNumerator) AS toplineComparisonNumerator,
                                max(toplineComparisonDenominator) AS toplineComparisonDenominator,
                                max(toplineCurrentValue) AS toplineCurrentValue,
                                max(toplineComparisonValue) AS toplineComparisonValue,

                                thisWeekDataAvailable,
                                max(goldProcessedAt) AS goldProcessedAt
                            FROM allBucketMembers
                            GROUP BY
                                targetWeekStartDate,
                                targetWeekEndDate,
                                fiscalQuarterLabel,
                                fiscalWeekCode,
                                weekLabel,
                                weekEndingLabel,
                                filterLob,
                                filterPlatform,
                                breakoutType,
                                breakoutLabel,
                                breakoutSortOrder,
                                breakoutDefinitionStatus,
                                configuredTopN,
                                configuredPairTopN,
                                metricName,
                                metricLabel,
                                metricDescription,
                                metricKind,
                                displayFormat,
                                changeUnit,
                                metricSortOrder,
                                metricDefinitionStatus,
                                comparisonType,
                                comparisonLabel,
                                comparisonSortOrder,
                                comparisonStartDate,
                                comparisonEndDate,
                                comparisonWeekCount,
                                comparisonDataAvailable,
                                comparisonWindowComplete,
                                displayBucketKey,
                                displayBreakoutValue,
                                thisWeekDataAvailable
                        ),
                        allBucketValues AS (
                            SELECT
                                a.*,
                                CASE
                                    WHEN metricKind = 'ratio' THEN try_divide(currentNumerator, currentDenominator)
                                    ELSE currentNumerator
                                END AS currentValue,
                                CASE
                                    WHEN metricKind = 'ratio' THEN try_divide(comparisonNumerator, comparisonDenominator)
                                    ELSE comparisonNumerator
                                END AS comparisonValue,
                                CASE
                                    WHEN metricKind = 'ratio' THEN try_divide(peerSetNumerator, peerSetDenominator)
                                    ELSE peerSetNumerator
                                END AS peerSetValue,
                                CASE
                                    WHEN metricKind = 'ratio'
                                        THEN peerSetNumerator IS NOT NULL AND nullif(peerSetDenominator, 0D) IS NOT NULL
                                    ELSE peerSetNumerator IS NOT NULL
                                END AS peerSetDataAvailable
                            FROM allBucketAgg a
                        ),
                        allBucketCalculated AS (
                            SELECT
                                v.*,
                                currentValue - comparisonValue AS absoluteDeltaValue,
                                CASE
                                    WHEN NOT thisWeekDataAvailable OR NOT comparisonDataAvailable
                                      OR currentValue IS NULL OR comparisonValue IS NULL THEN NULL
                                    WHEN changeUnit = 'pp' THEN 100D * (currentValue - comparisonValue)
                                    WHEN changeUnit = 'pct' THEN 100D * (try_divide(currentValue, comparisonValue) - 1D)
                                    ELSE NULL
                                END AS changeValue,
                                CASE
                                    WHEN currentValue IS NULL OR comparisonValue IS NULL THEN 'unavailable'
                                    WHEN currentValue > comparisonValue THEN 'up'
                                    WHEN currentValue < comparisonValue THEN 'down'
                                    ELSE 'flat'
                                END AS changeDirection,
                                CASE
                                    WHEN metricKind = 'count' THEN
                                        100D * try_divide(currentValue - comparisonValue, toplineComparisonValue)
                                    WHEN metricKind = 'ratio' THEN
                                        100D * (
                                            try_divide(currentNumerator, toplineCurrentDenominator)
                                            - try_divide(comparisonNumerator, toplineComparisonDenominator)
                                        )
                                    ELSE NULL
                                END AS impactOnToplineValue,
                                CASE
                                    WHEN metricKind = 'count' THEN 'pct'
                                    WHEN metricKind = 'ratio' THEN 'pp'
                                    ELSE NULL
                                END AS impactOnToplineUnit,
                                currentValue - peerSetValue AS peerSetAbsoluteDeltaValue,
                                CASE
                                    WHEN NOT peerSetDataAvailable OR currentValue IS NULL THEN NULL
                                    WHEN changeUnit = 'pp' THEN 100D * (currentValue - peerSetValue)
                                    WHEN changeUnit = 'pct' THEN 100D * (try_divide(currentValue, peerSetValue) - 1D)
                                    ELSE NULL
                                END AS peerSetChangeValue
                            FROM allBucketValues v
                        )

                        SELECT
                            targetWeekStartDate,
                            targetWeekEndDate,
                            fiscalQuarterLabel,
                            fiscalWeekCode,
                            weekLabel,
                            weekEndingLabel,
                            filterLob,
                            filterPlatform,

                            breakoutType,
                            breakoutLabel,
                            breakoutValue,
                            breakoutSortOrder,
                            breakoutDefinitionStatus,
                            configuredTopN,
                            configuredPairTopN,

                            displayRankWithinBreakout,
                            isOtherBucket,
                            rawMemberCount,
                            rawMinImpactRankWithinBreakout,
                            rawMaxImpactRankWithinBreakout,

                            metricName,
                            metricLabel,
                            metricDescription,
                            metricKind,
                            displayFormat,
                            changeUnit,
                            metricSortOrder,
                            metricDefinitionStatus,

                            comparisonType,
                            comparisonLabel,
                            comparisonSortOrder,
                            comparisonStartDate,
                            comparisonEndDate,
                            comparisonWeekCount,
                            comparisonDataAvailable,
                            comparisonWindowComplete,

                            currentNumerator,
                            currentDenominator,
                            comparisonNumerator,
                            comparisonDenominator,
                            currentValue,
                            comparisonValue,
                            absoluteDeltaValue,
                            changeValue,
                            changeDirection,

                            toplineCurrentNumerator,
                            toplineCurrentDenominator,
                            toplineComparisonNumerator,
                            toplineComparisonDenominator,
                            toplineCurrentValue,
                            toplineComparisonValue,
                            impactOnToplineValue,
                            impactOnToplineUnit,

                            peerSetNumerator,
                            peerSetDenominator,
                            peerSetValue,
                            peerSetAbsoluteDeltaValue,
                            peerSetChangeValue,
                            peerSetDataAvailable,

                            thisWeekDataAvailable,
                            goldProcessedAt
                        FROM allBucketCalculated
                    ),
                    sizeConfig AS (
                        SELECT * FROM VALUES
                            ('top5',  'Top 5',  5,   10),
                            ('top10', 'Top 10', 10,  20),
                            ('all',   'All',    100, 30)
                        AS s(displaySize, displaySizeLabel, displayLimit, displaySizeSortOrder)
                    ),
                    expanded AS (
                        SELECT
                            b.*,
                            s.displaySize,
                            s.displaySizeLabel,
                            s.displayLimit,
                            s.displaySizeSortOrder,

                            CASE
                                WHEN NOT b.isOtherBucket AND b.displayRankWithinBreakout <= s.displayLimit
                                    THEN concat('VALUE::', coalesce(b.breakoutValue, '(null)'))
                                ELSE 'OTHER::REMAINDER'
                            END AS sizeBucketKey,

                            CASE
                                WHEN NOT b.isOtherBucket AND b.displayRankWithinBreakout <= s.displayLimit
                                    THEN b.breakoutValue
                                ELSE '(Other)'
                            END AS displayBreakoutValue,

                            CASE
                                WHEN NOT b.isOtherBucket AND b.displayRankWithinBreakout <= s.displayLimit
                                    THEN FALSE
                                ELSE TRUE
                            END AS sizeOtherMember
                        FROM breakoutsComparison b
                        CROSS JOIN sizeConfig s
                        WHERE b.comparisonDataAvailable
                    ),
                    bucketAgg AS (
                        SELECT
                            targetWeekStartDate,
                            targetWeekEndDate,
                            fiscalQuarterLabel,
                            fiscalWeekCode,
                            weekLabel,
                            weekEndingLabel,
                            filterLob,
                            filterPlatform,

                            metricName,
                            metricLabel,
                            metricDescription,
                            metricKind,
                            displayFormat,
                            changeUnit,
                            metricSortOrder,
                            metricDefinitionStatus,

                            breakoutType,
                            breakoutLabel,
                            breakoutSortOrder,
                            breakoutDefinitionStatus,

                            comparisonType,
                            comparisonLabel,
                            comparisonSortOrder,
                            comparisonStartDate,
                            comparisonEndDate,
                            comparisonWeekCount,
                            comparisonWindowComplete,

                            displaySize,
                            displaySizeLabel,
                            displayLimit,
                            displaySizeSortOrder,
                            sizeBucketKey,
                            displayBreakoutValue AS breakoutValue,
                            max(CASE WHEN sizeOtherMember THEN 1 ELSE 0 END) = 1 AS isOtherBucket,

                            CASE
                                WHEN max(CASE WHEN sizeOtherMember THEN 1 ELSE 0 END) = 1 THEN displayLimit + 1
                                ELSE min(displayRankWithinBreakout)
                            END AS displayRank,

                            sum(rawMemberCount) AS rawMemberCount,
                            min(rawMinImpactRankWithinBreakout) AS rawMinImpactRankWithinBreakout,
                            max(rawMaxImpactRankWithinBreakout) AS rawMaxImpactRankWithinBreakout,

                            sum(currentNumerator) AS currentNumerator,
                            sum(currentDenominator) AS currentDenominator,
                            sum(comparisonNumerator) AS comparisonNumerator,
                            sum(comparisonDenominator) AS comparisonDenominator,

                            max(toplineCurrentNumerator) AS toplineCurrentNumerator,
                            max(toplineCurrentDenominator) AS toplineCurrentDenominator,
                            max(toplineComparisonNumerator) AS toplineComparisonNumerator,
                            max(toplineComparisonDenominator) AS toplineComparisonDenominator,
                            max(toplineCurrentValue) AS toplineCurrentValue,
                            max(toplineComparisonValue) AS toplineComparisonValue,

                            max(goldProcessedAt) AS goldProcessedAt
                        FROM expanded
                        GROUP BY
                            targetWeekStartDate,
                            targetWeekEndDate,
                            fiscalQuarterLabel,
                            fiscalWeekCode,
                            weekLabel,
                            weekEndingLabel,
                            filterLob,
                            filterPlatform,
                            metricName,
                            metricLabel,
                            metricDescription,
                            metricKind,
                            displayFormat,
                            changeUnit,
                            metricSortOrder,
                            metricDefinitionStatus,
                            breakoutType,
                            breakoutLabel,
                            breakoutSortOrder,
                            breakoutDefinitionStatus,
                            comparisonType,
                            comparisonLabel,
                            comparisonSortOrder,
                            comparisonStartDate,
                            comparisonEndDate,
                            comparisonWeekCount,
                            comparisonWindowComplete,
                            displaySize,
                            displaySizeLabel,
                            displayLimit,
                            displaySizeSortOrder,
                            sizeBucketKey,
                            displayBreakoutValue
                    ),
                    bucketValues AS (
                        SELECT
                            a.*,
                            CASE
                                WHEN metricKind = 'ratio' THEN try_divide(currentNumerator, currentDenominator)
                                ELSE currentNumerator
                            END AS currentValue,
                            CASE
                                WHEN metricKind = 'ratio' THEN try_divide(comparisonNumerator, comparisonDenominator)
                                ELSE comparisonNumerator
                            END AS comparisonValue
                        FROM bucketAgg a
                    ),
                    calculated AS (
                        SELECT
                            v.*,
                            currentValue - comparisonValue AS absoluteDeltaValue,
                            CASE
                                WHEN changeUnit = 'pp' THEN 100D * (currentValue - comparisonValue)
                                WHEN changeUnit = 'pct' THEN 100D * (try_divide(currentValue, comparisonValue) - 1D)
                                ELSE NULL
                            END AS changeValue,
                            CASE
                                WHEN currentValue IS NULL OR comparisonValue IS NULL THEN 'unavailable'
                                WHEN currentValue > comparisonValue THEN 'up'
                                WHEN currentValue < comparisonValue THEN 'down'
                                ELSE 'flat'
                            END AS changeDirection,
                            CASE
                                WHEN metricKind = 'count' THEN
                                    100D * try_divide(currentValue - comparisonValue, toplineComparisonValue)
                                WHEN metricKind = 'ratio' THEN
                                    100D * (
                                        try_divide(currentNumerator, toplineCurrentDenominator)
                                        - try_divide(comparisonNumerator, toplineComparisonDenominator)
                                    )
                                ELSE NULL
                            END AS impactOnToplineValue,
                            CASE WHEN metricKind = 'count' THEN 'pct'
                                 WHEN metricKind = 'ratio' THEN 'pp'
                                 ELSE NULL END AS impactOnToplineUnit,
                            CASE WHEN metricKind = 'count' THEN currentValue - comparisonValue
                                 WHEN metricKind = 'ratio' THEN
                                    100D * (
                                        try_divide(currentNumerator, toplineCurrentDenominator)
                                        - try_divide(comparisonNumerator, toplineComparisonDenominator)
                                    )
                                 ELSE NULL END AS waterfallDeltaValue,
                            CASE WHEN metricKind = 'count' THEN 'number'
                                 WHEN metricKind = 'ratio' THEN 'pp'
                                 ELSE NULL END AS waterfallDeltaUnit,
                            CASE WHEN metricKind = 'count' THEN toplineCurrentValue - toplineComparisonValue
                                 WHEN metricKind = 'ratio' THEN 100D * (toplineCurrentValue - toplineComparisonValue)
                                 ELSE NULL END AS toplineWaterfallDeltaValue
                        FROM bucketValues v
                    ),
                    recon AS (
                        SELECT
                            c.*,
                            sum(waterfallDeltaValue) OVER (
                                PARTITION BY targetWeekStartDate, filterLob, filterPlatform, metricName,
                                             breakoutType, comparisonType, displaySize
                            ) AS displayedWaterfallDeltaSum
                        FROM calculated c
                    )
                    SELECT
                        targetWeekStartDate,
                        targetWeekEndDate,
                        fiscalQuarterLabel,
                        fiscalWeekCode,
                        weekLabel,
                        weekEndingLabel,
                        filterLob,
                        filterPlatform,

                        metricName,
                        metricLabel,
                        metricDescription,
                        metricKind,
                        displayFormat,
                        changeUnit,
                        metricSortOrder,
                        metricDefinitionStatus,

                        breakoutType,
                        breakoutLabel,
                        breakoutValue,
                        breakoutSortOrder,
                        breakoutDefinitionStatus,

                        comparisonType,
                        comparisonLabel,
                        comparisonSortOrder,
                        comparisonStartDate,
                        comparisonEndDate,
                        comparisonWeekCount,
                        comparisonWindowComplete,

                        displaySize,
                        displaySizeLabel,
                        displayLimit,
                        displaySizeSortOrder,
                        displayRank,
                        isOtherBucket,
                        rawMemberCount,
                        rawMinImpactRankWithinBreakout,
                        rawMaxImpactRankWithinBreakout,

                        comparisonValue AS sliceStartValue,
                        currentValue AS sliceEndValue,
                        currentNumerator,
                        currentDenominator,
                        comparisonNumerator,
                        comparisonDenominator,
                        absoluteDeltaValue,
                        changeValue,
                        changeDirection,

                        toplineComparisonValue AS waterfallStartValue,
                        toplineCurrentValue AS waterfallEndValue,
                        toplineWaterfallDeltaValue,
                        waterfallDeltaValue,
                        waterfallDeltaUnit,
                        impactOnToplineValue,
                        impactOnToplineUnit,

                        displayedWaterfallDeltaSum,
                        toplineWaterfallDeltaValue - displayedWaterfallDeltaSum AS waterfallReconciliationResidual,

                        goldProcessedAt
                    FROM recon
                )
                SELECT
                    targetWeekStartDate,
                    targetWeekEndDate,
                    fiscalQuarterLabel,
                    fiscalWeekCode,
                    weekLabel,
                    weekEndingLabel,
                    filterLob,
                    filterPlatform,

                    metricName,
                    metricLabel,
                    metricDescription,
                    metricKind,
                    displayFormat,
                    changeUnit,
                    metricSortOrder,
                    metricDefinitionStatus,

                    breakoutType,
                    breakoutLabel,
                    breakoutValue,
                    breakoutSortOrder,
                    breakoutDefinitionStatus,

                    comparisonType,
                    comparisonLabel,
                    comparisonSortOrder,
                    comparisonStartDate,
                    comparisonEndDate,
                    comparisonWeekCount,
                    comparisonWindowComplete,

                    displaySize,
                    displaySizeLabel,
                    displayLimit,
                    displaySizeSortOrder,
                    displayRank,
                    isOtherBucket,
                    rawMemberCount,

                    sliceEndValue AS actualValue,
                    sliceStartValue AS benchmarkValue,
                    absoluteDeltaValue,
                    changeValue,
                    changeDirection,
                    impactOnToplineValue,
                    impactOnToplineUnit,

                    goldProcessedAt
                FROM waterfallDirect
            ) appResult
        ) schemaBootstrap
        WHERE 1 = 0;

        -- --------------------------------------------------------------------
        -- 5. Rebuild requested whole target-week range.
        --    Whole-week replacement is intentional because comparator ranks,
        --    Top-N membership and (Other) buckets can all change together.
        -- --------------------------------------------------------------------
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsAbsoluteTrend_long
        REPLACE WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
SELECT
    appResult.*,
    v_processedAt AS appProcessedAt
FROM (
    WITH waterfallDirect AS (
        WITH breakoutsComparison AS (
            WITH
            scopeBreakouts AS (
                SELECT *
                FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
                WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
            ),
            scopeOverview AS (
                SELECT *
                FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
                WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
            ),
            base AS (
                SELECT
                    g.targetWeekStartDate,
                    g.targetWeekEndDate,
                    g.fiscalQuarterLabel,
                    g.fiscalWeekCode,
                    g.weekLabel,
                    c.weekEndingLabel,
                    c.priorWeekStartDate,
                    c.fourWeekAvgStartDate,
                    c.fourWeekAvgEndDate,
                    c.sameWeekLastYearStartDate,
                    g.filterLob,
                    g.filterPlatform,
                    g.breakoutType,
                    g.breakoutLabel,
                    g.breakoutValue,
                    g.valueRankByNbv AS goldValueRankByNbv,
                    g.isTopN AS goldIsConfiguredTopN,
                    bc.topN AS configuredTopN,
                    bc.pairTopN AS configuredPairTopN,
                    bc.definitionStatus AS breakoutDefinitionStatus,
                    bc.sortOrder AS breakoutSortOrder,
                    g.metricName,
                    g.metricLabel,
                    mc.metricDescription,
                    g.metricKind,
                    g.displayFormat,
                    g.changeUnit,
                    mc.definitionStatus AS metricDefinitionStatus,
                    mc.sortOrder AS metricSortOrder,
                    g.thisWeekNumerator,
                    g.thisWeekDenominator,
                    g.priorWeekNumerator,
                    g.priorWeekDenominator,
                    g.fourWeekTrendNumerator,
                    g.fourWeekTrendDenominator,
                    g.sameWeekLyNumerator,
                    g.sameWeekLyDenominator,
                    g.peerSetNumerator,
                    g.peerSetDenominator,
                    g.thisWeekDataAvailable,
                    g.priorWeekDataAvailable,
                    g.fourWeekTrendWeekCount,
                    g.sameWeekLyDataAvailable,
                    g.goldProcessedAt
                FROM scopeBreakouts g
                JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static bc
                  ON bc.breakoutType=g.breakoutType AND bc.isActive AND bc.isPrebuiltBreakout
                JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
                  ON mc.metricName=g.metricName AND mc.isActive
                LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c
                  ON c.weekStartDate=g.targetWeekStartDate
            ),

            comparisonLong AS (
                SELECT
                    base.*,
                    'priorWeek' AS comparisonType,
                    'Prior week' AS comparisonLabel,
                    10 AS comparisonSortOrder,
                    priorWeekStartDate AS comparisonStartDate,
                    date_add(priorWeekStartDate, 6) AS comparisonEndDate,
                    1 AS comparisonWeekCount,
                    priorWeekDataAvailable AS comparisonDataAvailable,
                    priorWeekDataAvailable AS comparisonWindowComplete,
                    priorWeekNumerator AS comparisonNumerator,
                    priorWeekDenominator AS comparisonDenominator
                FROM base

                UNION ALL

                SELECT
                    base.*,
                    'fourWeek' AS comparisonType,
                    '4-wk trend' AS comparisonLabel,
                    20 AS comparisonSortOrder,
                    fourWeekAvgStartDate AS comparisonStartDate,
                    fourWeekAvgEndDate AS comparisonEndDate,
                    fourWeekTrendWeekCount AS comparisonWeekCount,
                    fourWeekTrendWeekCount > 0 AS comparisonDataAvailable,
                    fourWeekTrendWeekCount = 4 AS comparisonWindowComplete,
                    CASE
                        WHEN metricKind = 'count' AND fourWeekTrendWeekCount > 0
                            THEN try_divide(fourWeekTrendNumerator, cast(fourWeekTrendWeekCount AS DOUBLE))
                        ELSE fourWeekTrendNumerator
                    END AS comparisonNumerator,
                    CASE
                        WHEN metricKind = 'count' THEN NULL
                        ELSE fourWeekTrendDenominator
                    END AS comparisonDenominator
                FROM base

                UNION ALL

                SELECT
                    base.*,
                    'lastYear' AS comparisonType,
                    'Same wk LY' AS comparisonLabel,
                    30 AS comparisonSortOrder,
                    sameWeekLastYearStartDate AS comparisonStartDate,
                    date_add(sameWeekLastYearStartDate, 6) AS comparisonEndDate,
                    1 AS comparisonWeekCount,
                    sameWeekLyDataAvailable AS comparisonDataAvailable,
                    sameWeekLyDataAvailable AS comparisonWindowComplete,
                    sameWeekLyNumerator AS comparisonNumerator,
                    sameWeekLyDenominator AS comparisonDenominator
                FROM base
            ),
            valuesCalculated AS (
                SELECT
                    *,
                    CASE
                        WHEN metricKind = 'ratio' THEN try_divide(thisWeekNumerator, thisWeekDenominator)
                        ELSE thisWeekNumerator
                    END AS currentValue,
                    CASE
                        WHEN metricKind = 'ratio' THEN try_divide(comparisonNumerator, comparisonDenominator)
                        ELSE comparisonNumerator
                    END AS comparisonValue
                FROM comparisonLong
            ),
            deltasCalculated AS (
                SELECT
                    *,
                    currentValue - comparisonValue AS absoluteDeltaValue,
                    CASE
                        WHEN NOT thisWeekDataAvailable OR NOT comparisonDataAvailable
                          OR currentValue IS NULL OR comparisonValue IS NULL THEN NULL
                        WHEN changeUnit = 'pp' THEN 100D * (currentValue - comparisonValue)
                        WHEN changeUnit = 'pct' THEN 100D * (try_divide(currentValue, comparisonValue) - 1D)
                        ELSE NULL
                    END AS changeValue,
                    CASE
                        WHEN currentValue IS NULL OR comparisonValue IS NULL THEN 'unavailable'
                        WHEN currentValue > comparisonValue THEN 'up'
                        WHEN currentValue < comparisonValue THEN 'down'
                        ELSE 'flat'
                    END AS changeDirection
                FROM valuesCalculated
            )
            ,
            peerCalculated AS (
                SELECT
                    d.*,
                    CASE WHEN metricKind='ratio' THEN try_divide(peerSetNumerator,peerSetDenominator)
                         ELSE peerSetNumerator END AS peerSetValue,
                    CASE WHEN metricKind='ratio' THEN peerSetNumerator IS NOT NULL AND nullif(peerSetDenominator,0D) IS NOT NULL
                         ELSE peerSetNumerator IS NOT NULL END AS peerSetDataAvailable
                FROM deltasCalculated d
            ),
            toplineBase AS (
                SELECT
                    g.targetWeekStartDate,
                    g.filterLob,
                    g.filterPlatform,
                    g.metricName,
                    g.metricKind,
                    g.thisWeekNumerator,
                    g.thisWeekDenominator,
                    g.priorWeekNumerator,
                    g.priorWeekDenominator,
                    g.fourWeekTrendNumerator,
                    g.fourWeekTrendDenominator,
                    g.sameWeekLyNumerator,
                    g.sameWeekLyDenominator,
                    g.fourWeekTrendWeekCount
                FROM scopeOverview g
            ),
            toplineLong AS (
                SELECT *, 'priorWeek' AS comparisonType,
                       priorWeekNumerator AS comparisonNumerator,
                       priorWeekDenominator AS comparisonDenominator
                FROM toplineBase
                UNION ALL
                SELECT *, 'fourWeek' AS comparisonType,
                       CASE WHEN metricKind='count' AND fourWeekTrendWeekCount>0
                            THEN try_divide(fourWeekTrendNumerator,cast(fourWeekTrendWeekCount AS DOUBLE))
                            ELSE fourWeekTrendNumerator END AS comparisonNumerator,
                       CASE WHEN metricKind='count' THEN NULL ELSE fourWeekTrendDenominator END AS comparisonDenominator
                FROM toplineBase
                UNION ALL
                SELECT *, 'lastYear' AS comparisonType,
                       sameWeekLyNumerator AS comparisonNumerator,
                       sameWeekLyDenominator AS comparisonDenominator
                FROM toplineBase
            ),
            toplineValues AS (
                SELECT
                    *,
                    CASE WHEN metricKind='ratio' THEN try_divide(thisWeekNumerator,thisWeekDenominator)
                         ELSE thisWeekNumerator END AS toplineCurrentValue,
                    CASE WHEN metricKind='ratio' THEN try_divide(comparisonNumerator,comparisonDenominator)
                         ELSE comparisonNumerator END AS toplineComparisonValue
                FROM toplineLong
            ),
            withTopline AS (
                SELECT
                    p.*,
                    t.toplineCurrentValue,
                    t.toplineComparisonValue,
                    t.thisWeekNumerator AS toplineCurrentNumerator,
                    t.thisWeekDenominator AS toplineCurrentDenominator,
                    t.comparisonNumerator AS toplineComparisonNumerator,
                    t.comparisonDenominator AS toplineComparisonDenominator,
                    CASE
                        WHEN p.metricKind='count' THEN
                            100D * try_divide(p.absoluteDeltaValue,t.toplineComparisonValue)
                        WHEN p.metricKind='ratio' THEN
                            100D * (
                                try_divide(p.thisWeekNumerator,t.thisWeekDenominator)
                                - try_divide(p.comparisonNumerator,t.comparisonDenominator)
                            )
                        ELSE NULL
                    END AS impactOnToplineValue,
                    CASE WHEN p.metricKind='count' THEN 'pct'
                         WHEN p.metricKind='ratio' THEN 'pp'
                         ELSE NULL END AS impactOnToplineUnit,
                    p.currentValue - p.peerSetValue AS peerSetAbsoluteDeltaValue,
                    CASE
                        WHEN NOT p.peerSetDataAvailable OR p.currentValue IS NULL THEN NULL
                        WHEN p.changeUnit='pp' THEN 100D*(p.currentValue-p.peerSetValue)
                        WHEN p.changeUnit='pct' THEN 100D*(try_divide(p.currentValue,p.peerSetValue)-1D)
                        ELSE NULL
                    END AS peerSetChangeValue
                FROM peerCalculated p
                LEFT JOIN toplineValues t
                  ON t.targetWeekStartDate=p.targetWeekStartDate
                 AND t.filterLob=p.filterLob
                 AND t.filterPlatform=p.filterPlatform
                 AND t.metricName=p.metricName
                 AND t.comparisonType=p.comparisonType
            ),
            ranked AS (
                SELECT
                    *,
                    CASE WHEN comparisonDataAvailable AND impactOnToplineValue IS NOT NULL THEN
                        row_number() OVER (
                            PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName,breakoutType,comparisonType
                            ORDER BY abs(impactOnToplineValue) DESC NULLS LAST,
                                     abs(absoluteDeltaValue) DESC NULLS LAST,
                                     breakoutValue
                        )
                    END AS impactRankWithinBreakout,
                    CASE WHEN comparisonDataAvailable AND impactOnToplineValue IS NOT NULL THEN
                        row_number() OVER (
                            PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName,comparisonType
                            ORDER BY abs(impactOnToplineValue) DESC NULLS LAST,
                                     abs(absoluteDeltaValue) DESC NULLS LAST,
                                     breakoutType,breakoutValue
                        )
                    END AS impactRankAcrossBreakouts
                FROM withTopline
            )
            ,
            allBucketMembers AS (
                SELECT
                    r.*,
                    CASE
                        WHEN impactRankWithinBreakout <= 100
                            THEN concat('VALUE::', coalesce(breakoutValue, '(null)'))
                        ELSE 'OTHER::REMAINDER'
                    END AS displayBucketKey,
                    CASE
                        WHEN impactRankWithinBreakout <= 100 THEN breakoutValue
                        ELSE '(Other)'
                    END AS displayBreakoutValue,
                    CASE
                        WHEN impactRankWithinBreakout <= 100 THEN FALSE
                        ELSE TRUE
                    END AS isSyntheticOtherMember
                FROM ranked r
            ),
            allBucketAgg AS (
                SELECT
                    targetWeekStartDate,
                    targetWeekEndDate,
                    fiscalQuarterLabel,
                    fiscalWeekCode,
                    weekLabel,
                    weekEndingLabel,
                    filterLob,
                    filterPlatform,

                    breakoutType,
                    breakoutLabel,
                    breakoutSortOrder,
                    breakoutDefinitionStatus,
                    configuredTopN,
                    configuredPairTopN,

                    metricName,
                    metricLabel,
                    metricDescription,
                    metricKind,
                    displayFormat,
                    changeUnit,
                    metricSortOrder,
                    metricDefinitionStatus,

                    comparisonType,
                    comparisonLabel,
                    comparisonSortOrder,
                    comparisonStartDate,
                    comparisonEndDate,
                    comparisonWeekCount,
                    comparisonDataAvailable,
                    comparisonWindowComplete,

                    displayBucketKey,
                    displayBreakoutValue AS breakoutValue,
                    max(CASE WHEN isSyntheticOtherMember THEN 1 ELSE 0 END) = 1 AS isOtherBucket,

                    CASE
                        WHEN max(CASE WHEN isSyntheticOtherMember THEN 1 ELSE 0 END) = 1 THEN 101
                        ELSE min(impactRankWithinBreakout)
                    END AS displayRankWithinBreakout,

                    count(*) AS rawMemberCount,
                    min(impactRankWithinBreakout) AS rawMinImpactRankWithinBreakout,
                    max(impactRankWithinBreakout) AS rawMaxImpactRankWithinBreakout,

                    sum(thisWeekNumerator) AS currentNumerator,
                    sum(thisWeekDenominator) AS currentDenominator,
                    sum(comparisonNumerator) AS comparisonNumerator,
                    sum(comparisonDenominator) AS comparisonDenominator,

                    sum(peerSetNumerator) AS peerSetNumerator,
                    sum(peerSetDenominator) AS peerSetDenominator,

                    max(toplineCurrentNumerator) AS toplineCurrentNumerator,
                    max(toplineCurrentDenominator) AS toplineCurrentDenominator,
                    max(toplineComparisonNumerator) AS toplineComparisonNumerator,
                    max(toplineComparisonDenominator) AS toplineComparisonDenominator,
                    max(toplineCurrentValue) AS toplineCurrentValue,
                    max(toplineComparisonValue) AS toplineComparisonValue,

                    thisWeekDataAvailable,
                    max(goldProcessedAt) AS goldProcessedAt
                FROM allBucketMembers
                GROUP BY
                    targetWeekStartDate,
                    targetWeekEndDate,
                    fiscalQuarterLabel,
                    fiscalWeekCode,
                    weekLabel,
                    weekEndingLabel,
                    filterLob,
                    filterPlatform,
                    breakoutType,
                    breakoutLabel,
                    breakoutSortOrder,
                    breakoutDefinitionStatus,
                    configuredTopN,
                    configuredPairTopN,
                    metricName,
                    metricLabel,
                    metricDescription,
                    metricKind,
                    displayFormat,
                    changeUnit,
                    metricSortOrder,
                    metricDefinitionStatus,
                    comparisonType,
                    comparisonLabel,
                    comparisonSortOrder,
                    comparisonStartDate,
                    comparisonEndDate,
                    comparisonWeekCount,
                    comparisonDataAvailable,
                    comparisonWindowComplete,
                    displayBucketKey,
                    displayBreakoutValue,
                    thisWeekDataAvailable
            ),
            allBucketValues AS (
                SELECT
                    a.*,
                    CASE
                        WHEN metricKind = 'ratio' THEN try_divide(currentNumerator, currentDenominator)
                        ELSE currentNumerator
                    END AS currentValue,
                    CASE
                        WHEN metricKind = 'ratio' THEN try_divide(comparisonNumerator, comparisonDenominator)
                        ELSE comparisonNumerator
                    END AS comparisonValue,
                    CASE
                        WHEN metricKind = 'ratio' THEN try_divide(peerSetNumerator, peerSetDenominator)
                        ELSE peerSetNumerator
                    END AS peerSetValue,
                    CASE
                        WHEN metricKind = 'ratio'
                            THEN peerSetNumerator IS NOT NULL AND nullif(peerSetDenominator, 0D) IS NOT NULL
                        ELSE peerSetNumerator IS NOT NULL
                    END AS peerSetDataAvailable
                FROM allBucketAgg a
            ),
            allBucketCalculated AS (
                SELECT
                    v.*,
                    currentValue - comparisonValue AS absoluteDeltaValue,
                    CASE
                        WHEN NOT thisWeekDataAvailable OR NOT comparisonDataAvailable
                          OR currentValue IS NULL OR comparisonValue IS NULL THEN NULL
                        WHEN changeUnit = 'pp' THEN 100D * (currentValue - comparisonValue)
                        WHEN changeUnit = 'pct' THEN 100D * (try_divide(currentValue, comparisonValue) - 1D)
                        ELSE NULL
                    END AS changeValue,
                    CASE
                        WHEN currentValue IS NULL OR comparisonValue IS NULL THEN 'unavailable'
                        WHEN currentValue > comparisonValue THEN 'up'
                        WHEN currentValue < comparisonValue THEN 'down'
                        ELSE 'flat'
                    END AS changeDirection,
                    CASE
                        WHEN metricKind = 'count' THEN
                            100D * try_divide(currentValue - comparisonValue, toplineComparisonValue)
                        WHEN metricKind = 'ratio' THEN
                            100D * (
                                try_divide(currentNumerator, toplineCurrentDenominator)
                                - try_divide(comparisonNumerator, toplineComparisonDenominator)
                            )
                        ELSE NULL
                    END AS impactOnToplineValue,
                    CASE
                        WHEN metricKind = 'count' THEN 'pct'
                        WHEN metricKind = 'ratio' THEN 'pp'
                        ELSE NULL
                    END AS impactOnToplineUnit,
                    currentValue - peerSetValue AS peerSetAbsoluteDeltaValue,
                    CASE
                        WHEN NOT peerSetDataAvailable OR currentValue IS NULL THEN NULL
                        WHEN changeUnit = 'pp' THEN 100D * (currentValue - peerSetValue)
                        WHEN changeUnit = 'pct' THEN 100D * (try_divide(currentValue, peerSetValue) - 1D)
                        ELSE NULL
                    END AS peerSetChangeValue
                FROM allBucketValues v
            )

            SELECT
                targetWeekStartDate,
                targetWeekEndDate,
                fiscalQuarterLabel,
                fiscalWeekCode,
                weekLabel,
                weekEndingLabel,
                filterLob,
                filterPlatform,

                breakoutType,
                breakoutLabel,
                breakoutValue,
                breakoutSortOrder,
                breakoutDefinitionStatus,
                configuredTopN,
                configuredPairTopN,

                displayRankWithinBreakout,
                isOtherBucket,
                rawMemberCount,
                rawMinImpactRankWithinBreakout,
                rawMaxImpactRankWithinBreakout,

                metricName,
                metricLabel,
                metricDescription,
                metricKind,
                displayFormat,
                changeUnit,
                metricSortOrder,
                metricDefinitionStatus,

                comparisonType,
                comparisonLabel,
                comparisonSortOrder,
                comparisonStartDate,
                comparisonEndDate,
                comparisonWeekCount,
                comparisonDataAvailable,
                comparisonWindowComplete,

                currentNumerator,
                currentDenominator,
                comparisonNumerator,
                comparisonDenominator,
                currentValue,
                comparisonValue,
                absoluteDeltaValue,
                changeValue,
                changeDirection,

                toplineCurrentNumerator,
                toplineCurrentDenominator,
                toplineComparisonNumerator,
                toplineComparisonDenominator,
                toplineCurrentValue,
                toplineComparisonValue,
                impactOnToplineValue,
                impactOnToplineUnit,

                peerSetNumerator,
                peerSetDenominator,
                peerSetValue,
                peerSetAbsoluteDeltaValue,
                peerSetChangeValue,
                peerSetDataAvailable,

                thisWeekDataAvailable,
                goldProcessedAt
            FROM allBucketCalculated
        ),
        sizeConfig AS (
            SELECT * FROM VALUES
                ('top5',  'Top 5',  5,   10),
                ('top10', 'Top 10', 10,  20),
                ('all',   'All',    100, 30)
            AS s(displaySize, displaySizeLabel, displayLimit, displaySizeSortOrder)
        ),
        expanded AS (
            SELECT
                b.*,
                s.displaySize,
                s.displaySizeLabel,
                s.displayLimit,
                s.displaySizeSortOrder,

                CASE
                    WHEN NOT b.isOtherBucket AND b.displayRankWithinBreakout <= s.displayLimit
                        THEN concat('VALUE::', coalesce(b.breakoutValue, '(null)'))
                    ELSE 'OTHER::REMAINDER'
                END AS sizeBucketKey,

                CASE
                    WHEN NOT b.isOtherBucket AND b.displayRankWithinBreakout <= s.displayLimit
                        THEN b.breakoutValue
                    ELSE '(Other)'
                END AS displayBreakoutValue,

                CASE
                    WHEN NOT b.isOtherBucket AND b.displayRankWithinBreakout <= s.displayLimit
                        THEN FALSE
                    ELSE TRUE
                END AS sizeOtherMember
            FROM breakoutsComparison b
            CROSS JOIN sizeConfig s
            WHERE b.comparisonDataAvailable
        ),
        bucketAgg AS (
            SELECT
                targetWeekStartDate,
                targetWeekEndDate,
                fiscalQuarterLabel,
                fiscalWeekCode,
                weekLabel,
                weekEndingLabel,
                filterLob,
                filterPlatform,

                metricName,
                metricLabel,
                metricDescription,
                metricKind,
                displayFormat,
                changeUnit,
                metricSortOrder,
                metricDefinitionStatus,

                breakoutType,
                breakoutLabel,
                breakoutSortOrder,
                breakoutDefinitionStatus,

                comparisonType,
                comparisonLabel,
                comparisonSortOrder,
                comparisonStartDate,
                comparisonEndDate,
                comparisonWeekCount,
                comparisonWindowComplete,

                displaySize,
                displaySizeLabel,
                displayLimit,
                displaySizeSortOrder,
                sizeBucketKey,
                displayBreakoutValue AS breakoutValue,
                max(CASE WHEN sizeOtherMember THEN 1 ELSE 0 END) = 1 AS isOtherBucket,

                CASE
                    WHEN max(CASE WHEN sizeOtherMember THEN 1 ELSE 0 END) = 1 THEN displayLimit + 1
                    ELSE min(displayRankWithinBreakout)
                END AS displayRank,

                sum(rawMemberCount) AS rawMemberCount,
                min(rawMinImpactRankWithinBreakout) AS rawMinImpactRankWithinBreakout,
                max(rawMaxImpactRankWithinBreakout) AS rawMaxImpactRankWithinBreakout,

                sum(currentNumerator) AS currentNumerator,
                sum(currentDenominator) AS currentDenominator,
                sum(comparisonNumerator) AS comparisonNumerator,
                sum(comparisonDenominator) AS comparisonDenominator,

                max(toplineCurrentNumerator) AS toplineCurrentNumerator,
                max(toplineCurrentDenominator) AS toplineCurrentDenominator,
                max(toplineComparisonNumerator) AS toplineComparisonNumerator,
                max(toplineComparisonDenominator) AS toplineComparisonDenominator,
                max(toplineCurrentValue) AS toplineCurrentValue,
                max(toplineComparisonValue) AS toplineComparisonValue,

                max(goldProcessedAt) AS goldProcessedAt
            FROM expanded
            GROUP BY
                targetWeekStartDate,
                targetWeekEndDate,
                fiscalQuarterLabel,
                fiscalWeekCode,
                weekLabel,
                weekEndingLabel,
                filterLob,
                filterPlatform,
                metricName,
                metricLabel,
                metricDescription,
                metricKind,
                displayFormat,
                changeUnit,
                metricSortOrder,
                metricDefinitionStatus,
                breakoutType,
                breakoutLabel,
                breakoutSortOrder,
                breakoutDefinitionStatus,
                comparisonType,
                comparisonLabel,
                comparisonSortOrder,
                comparisonStartDate,
                comparisonEndDate,
                comparisonWeekCount,
                comparisonWindowComplete,
                displaySize,
                displaySizeLabel,
                displayLimit,
                displaySizeSortOrder,
                sizeBucketKey,
                displayBreakoutValue
        ),
        bucketValues AS (
            SELECT
                a.*,
                CASE
                    WHEN metricKind = 'ratio' THEN try_divide(currentNumerator, currentDenominator)
                    ELSE currentNumerator
                END AS currentValue,
                CASE
                    WHEN metricKind = 'ratio' THEN try_divide(comparisonNumerator, comparisonDenominator)
                    ELSE comparisonNumerator
                END AS comparisonValue
            FROM bucketAgg a
        ),
        calculated AS (
            SELECT
                v.*,
                currentValue - comparisonValue AS absoluteDeltaValue,
                CASE
                    WHEN changeUnit = 'pp' THEN 100D * (currentValue - comparisonValue)
                    WHEN changeUnit = 'pct' THEN 100D * (try_divide(currentValue, comparisonValue) - 1D)
                    ELSE NULL
                END AS changeValue,
                CASE
                    WHEN currentValue IS NULL OR comparisonValue IS NULL THEN 'unavailable'
                    WHEN currentValue > comparisonValue THEN 'up'
                    WHEN currentValue < comparisonValue THEN 'down'
                    ELSE 'flat'
                END AS changeDirection,
                CASE
                    WHEN metricKind = 'count' THEN
                        100D * try_divide(currentValue - comparisonValue, toplineComparisonValue)
                    WHEN metricKind = 'ratio' THEN
                        100D * (
                            try_divide(currentNumerator, toplineCurrentDenominator)
                            - try_divide(comparisonNumerator, toplineComparisonDenominator)
                        )
                    ELSE NULL
                END AS impactOnToplineValue,
                CASE WHEN metricKind = 'count' THEN 'pct'
                     WHEN metricKind = 'ratio' THEN 'pp'
                     ELSE NULL END AS impactOnToplineUnit,
                CASE WHEN metricKind = 'count' THEN currentValue - comparisonValue
                     WHEN metricKind = 'ratio' THEN
                        100D * (
                            try_divide(currentNumerator, toplineCurrentDenominator)
                            - try_divide(comparisonNumerator, toplineComparisonDenominator)
                        )
                     ELSE NULL END AS waterfallDeltaValue,
                CASE WHEN metricKind = 'count' THEN 'number'
                     WHEN metricKind = 'ratio' THEN 'pp'
                     ELSE NULL END AS waterfallDeltaUnit,
                CASE WHEN metricKind = 'count' THEN toplineCurrentValue - toplineComparisonValue
                     WHEN metricKind = 'ratio' THEN 100D * (toplineCurrentValue - toplineComparisonValue)
                     ELSE NULL END AS toplineWaterfallDeltaValue
            FROM bucketValues v
        ),
        recon AS (
            SELECT
                c.*,
                sum(waterfallDeltaValue) OVER (
                    PARTITION BY targetWeekStartDate, filterLob, filterPlatform, metricName,
                                 breakoutType, comparisonType, displaySize
                ) AS displayedWaterfallDeltaSum
            FROM calculated c
        )
        SELECT
            targetWeekStartDate,
            targetWeekEndDate,
            fiscalQuarterLabel,
            fiscalWeekCode,
            weekLabel,
            weekEndingLabel,
            filterLob,
            filterPlatform,

            metricName,
            metricLabel,
            metricDescription,
            metricKind,
            displayFormat,
            changeUnit,
            metricSortOrder,
            metricDefinitionStatus,

            breakoutType,
            breakoutLabel,
            breakoutValue,
            breakoutSortOrder,
            breakoutDefinitionStatus,

            comparisonType,
            comparisonLabel,
            comparisonSortOrder,
            comparisonStartDate,
            comparisonEndDate,
            comparisonWeekCount,
            comparisonWindowComplete,

            displaySize,
            displaySizeLabel,
            displayLimit,
            displaySizeSortOrder,
            displayRank,
            isOtherBucket,
            rawMemberCount,
            rawMinImpactRankWithinBreakout,
            rawMaxImpactRankWithinBreakout,

            comparisonValue AS sliceStartValue,
            currentValue AS sliceEndValue,
            currentNumerator,
            currentDenominator,
            comparisonNumerator,
            comparisonDenominator,
            absoluteDeltaValue,
            changeValue,
            changeDirection,

            toplineComparisonValue AS waterfallStartValue,
            toplineCurrentValue AS waterfallEndValue,
            toplineWaterfallDeltaValue,
            waterfallDeltaValue,
            waterfallDeltaUnit,
            impactOnToplineValue,
            impactOnToplineUnit,

            displayedWaterfallDeltaSum,
            toplineWaterfallDeltaValue - displayedWaterfallDeltaSum AS waterfallReconciliationResidual,

            goldProcessedAt
        FROM recon
    )
    SELECT
        targetWeekStartDate,
        targetWeekEndDate,
        fiscalQuarterLabel,
        fiscalWeekCode,
        weekLabel,
        weekEndingLabel,
        filterLob,
        filterPlatform,

        metricName,
        metricLabel,
        metricDescription,
        metricKind,
        displayFormat,
        changeUnit,
        metricSortOrder,
        metricDefinitionStatus,

        breakoutType,
        breakoutLabel,
        breakoutValue,
        breakoutSortOrder,
        breakoutDefinitionStatus,

        comparisonType,
        comparisonLabel,
        comparisonSortOrder,
        comparisonStartDate,
        comparisonEndDate,
        comparisonWeekCount,
        comparisonWindowComplete,

        displaySize,
        displaySizeLabel,
        displayLimit,
        displaySizeSortOrder,
        displayRank,
        isOtherBucket,
        rawMemberCount,

        sliceEndValue AS actualValue,
        sliceStartValue AS benchmarkValue,
        absoluteDeltaValue,
        changeValue,
        changeDirection,
        impactOnToplineValue,
        impactOnToplineUnit,

        goldProcessedAt
    FROM waterfallDirect
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
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsAbsoluteTrend_long' AS targetObject,
            v_processedAt AS appProcessedAt;

    END IF;
END;

-- Development examples:
--
-- Preflight only:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appBreakoutsAbsoluteTrend_long(
--     p_asOfDate       => DATE '2026-09-28',
--     p_weeksToRebuild => 1,
--     p_validateOnly   => TRUE
-- );
--
-- Load / rebuild:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appBreakoutsAbsoluteTrend_long(
--     p_asOfDate       => DATE '2026-09-28',
--     p_weeksToRebuild => 1,
--     p_validateOnly   => FALSE
-- );
