-- ============================================================================
-- FILE  : 07_sdi_sp_mip_gold_appBreakoutsAbsoluteTrend_long.sql
-- LAYER : GOLD / APP
-- TAB   : Breakouts
-- SECTION: Absolute trend by breakout
--
-- PURPOSE:
--   Application-ready Absolute Trend cards.
--
-- UI FILTERS:
--   Quarter
--   Week
--   Metric
--   Breakout
--   Comparator = priorWeek | fourWeek | lastYear
--   LOB / Platform where applicable
--
-- SCREENSHOT BEHAVIOR:
--   Selected target week determines the cards/slices shown.
--   Each card contains:
--     - selected-week current value
--     - selected-week absolute delta vs chosen comparator
--     - selected-week impact on topline
--     - direction for green/red styling
--     - historical actual series
--     - historical selected-comparator benchmark series (dashed)
--
-- IMPORTANT:
--   Card membership is frozen from the selected TARGET WEEK.
--   Historical weeks therefore show the SAME slice/member set instead of
--   independently re-ranking every historical week.
--
-- APP-GOLD ELIGIBILITY:
--   Metrics   -> isActive AND showOnBreakouts
--   Breakouts -> isActive AND isPrebuiltBreakout
--
-- DISPLAY SIZE:
--   top5  = target-week Top 5 by selected-comparator impact + (Other)
--   top10 = target-week Top 10 by selected-comparator impact + (Other)
--   all   = target-week Top 100 by selected-comparator impact + (Other)
--
-- Current UI screenshot can simply request displaySize='all'.
--
-- HISTORICAL WINDOW:
--   8 reporting weeks including the selected target week.
--
-- Numerators/denominators are calculation-only and are NOT persisted.
-- ============================================================================

-- ONE-TIME MIGRATION ONLY:
-- DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsAbsoluteTrend_long;

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_gold_appBreakoutsAbsoluteTrend_long(
    IN p_asOfDate DATE DEFAULT NULL,
    IN p_weeksToRebuild INT DEFAULT 1,
    IN p_validateOnly BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'MIP Gold App: Absolute breakout trend with target-week card selection and stable 8-week actual/comparator series.'
AS
BEGIN
    DECLARE v_asOfDate DATE DEFAULT coalesce(
        p_asOfDate,
        date_add(to_date(from_utc_timestamp(current_timestamp(),'America/Los_Angeles')),-1)
    );
    DECLARE v_weekTo DATE;
    DECLARE v_weekFrom DATE;
    DECLARE v_weekEndTo DATE;
    DECLARE v_sourceWeekFrom DATE;
    DECLARE v_historyWeeks INT DEFAULT 8;
    DECLARE v_processedAt TIMESTAMP DEFAULT current_timestamp();

    -- =========================================================================
    -- 1. Parameters
    -- =========================================================================
    IF p_weeksToRebuild IS NULL OR p_weeksToRebuild<1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='p_weeksToRebuild must be >= 1.';
    END IF;

    SET v_weekTo=date_add(v_asOfDate,1-dayofweek(v_asOfDate));
    SET v_weekFrom=date_add(v_weekTo,-7*(p_weeksToRebuild-1));
    SET v_weekEndTo=date_add(v_weekTo,6);
    SET v_sourceWeekFrom=date_add(v_weekFrom,-7*(v_historyWeeks-1));

    -- =========================================================================
    -- 2. Eligible-source preflight
    -- =========================================================================
    IF NOT EXISTS(
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long g
        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
          ON mc.metricName=g.metricName
         AND mc.isActive
         AND mc.showOnBreakouts
        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static bc
          ON bc.breakoutType=g.breakoutType
         AND bc.isActive
         AND bc.isPrebuiltBreakout
        WHERE g.targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='Breakout Gold has no eligible Absolute Trend rows for the requested target-week range.';
    END IF;

    IF NOT EXISTS(
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long g
        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
          ON mc.metricName=g.metricName
         AND mc.isActive
         AND mc.showOnBreakouts
        WHERE g.targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='Overview Gold has no eligible Breakouts metrics for the requested target-week range.';
    END IF;

    IF NOT EXISTS(
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static
        WHERE isActive AND showOnBreakouts
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='Metric Catalog has no active Breakouts metrics.';
    END IF;

    IF NOT EXISTS(
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static
        WHERE isActive AND isPrebuiltBreakout
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='Breakout Catalog has no active prebuilt breakouts.';
    END IF;

    IF NOT EXISTS(
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static
        WHERE weekStartDate BETWEEN v_sourceWeekFrom AND v_weekTo
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='Fiscal Calendar has no rows for the required Absolute Trend history.';
    END IF;

    -- =========================================================================
    -- 3. Grain / metadata validation
    -- =========================================================================
    IF EXISTS(
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long g
        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
          ON mc.metricName=g.metricName
         AND mc.isActive
         AND mc.showOnBreakouts
        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static bc
          ON bc.breakoutType=g.breakoutType
         AND bc.isActive
         AND bc.isPrebuiltBreakout
        WHERE g.targetWeekStartDate BETWEEN v_sourceWeekFrom AND v_weekTo
        GROUP BY
            g.targetWeekStartDate,
            g.filterLob,
            g.filterPlatform,
            g.metricName,
            g.breakoutType,
            g.breakoutValue
        HAVING count(*)>1
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='Duplicate eligible Breakout Gold analytical keys detected.';
    END IF;

    IF EXISTS(
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long g
        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
          ON mc.metricName=g.metricName
         AND mc.isActive
         AND mc.showOnBreakouts
        WHERE g.targetWeekStartDate BETWEEN v_sourceWeekFrom AND v_weekTo
        GROUP BY
            g.targetWeekStartDate,
            g.filterLob,
            g.filterPlatform,
            g.metricName
        HAVING count(*)>1
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='Duplicate eligible Overview Gold analytical keys detected.';
    END IF;

    IF EXISTS(
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static
        WHERE isActive
          AND showOnBreakouts
          AND (
              metricKind NOT IN('count','ratio')
              OR displayFormat NOT IN('number','percent')
              OR changeUnit NOT IN('pct','pp')
          )
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='Breakouts Metric Catalog contains unsupported metric metadata.';
    END IF;

    -- =========================================================================
    -- 4. Validation only
    -- =========================================================================
    IF p_validateOnly THEN
        SELECT
            'VALIDATION_ONLY' AS status,
            v_weekFrom AS rebuildTargetWeekFrom,
            v_weekTo AS rebuildTargetWeekTo,
            v_sourceWeekFrom AS requiredHistoryWeekFrom,
            v_historyWeeks AS sparklineWeeks,
            'priorWeek | fourWeek | lastYear' AS supportedComparisons,
            'top5 | top10 | all' AS supportedDisplaySizes,
            'isActive=true AND showOnBreakouts=true' AS metricEligibility,
            'isActive=true AND isPrebuiltBreakout=true' AS breakoutEligibility,
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsAbsoluteTrend_long' AS targetObject,
            'Validation passed. No Gold App table was created or modified.' AS message;
    ELSE

        -- =====================================================================
        -- 5. App contract
        -- =====================================================================
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsAbsoluteTrend_long(
            targetWeekStartDate DATE,
            targetWeekEndDate DATE,
            fiscalYear INT,
            fiscalQuarterLabel STRING,
            fiscalWeekCode STRING,
            weekLabel STRING,
            weekEndingLabel STRING,

            filterLob STRING,
            filterPlatform STRING,

            metricName STRING,
            metricLabel STRING,
            metricDescription STRING,
            metricKind STRING,
            displayFormat STRING,
            changeUnit STRING,
            metricSortOrder INT,

            breakoutType STRING,
            breakoutLabel STRING,
            breakoutSortOrder INT,

            comparisonType STRING,
            comparisonLabel STRING,
            comparisonSortOrder INT,
            comparisonWindowComplete BOOLEAN,

            displaySize STRING,
            displaySizeLabel STRING,
            displayLimit INT,
            displaySizeSortOrder INT,

            cardKey STRING,
            breakoutValue STRING,
            isOtherBucket BOOLEAN,
            displayRank BIGINT,
            cardSortOrder BIGINT,
            cardDirection STRING,

            targetCurrentValue DOUBLE,
            targetCurrentValueDisplay STRING,

            targetComparisonValue DOUBLE,
            targetComparisonValueDisplay STRING,

            targetAbsoluteDiffValue DOUBLE,
            targetAbsoluteDiffDisplay STRING,

            targetChangeValue DOUBLE,
            targetChangeDisplay STRING,

            impactOnToplineValue DOUBLE,
            impactOnToplineDisplay STRING,
            impactOnToplineUnit STRING,

            sparklineWeeks INT,

            seriesWeekStartDate DATE,
            seriesWeekEndDate DATE,
            seriesFiscalYear INT,
            seriesFiscalQuarterLabel STRING,
            seriesFiscalWeekCode STRING,
            seriesWeekLabel STRING,
            seriesWeekEndingLabel STRING,
            seriesSortOrder BIGINT,
            isSelectedWeek BOOLEAN,

            seriesActualValue DOUBLE,
            seriesActualValueDisplay STRING,
            seriesBenchmarkValue DOUBLE,
            seriesBenchmarkValueDisplay STRING,
            seriesActualDataAvailable BOOLEAN,
            seriesBenchmarkDataAvailable BOOLEAN,

            appProcessedAt TIMESTAMP
        )
        USING DELTA
        CLUSTER BY(targetWeekStartDate,metricName,breakoutType)
        COMMENT 'MIP Gold App: Absolute Trend cards. Target-week card membership plus stable 8-week actual and selected-comparator benchmark series.';

        -- =====================================================================
        -- 6. Rebuild target weeks
        -- =====================================================================
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsAbsoluteTrend_long
        REPLACE WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo

        WITH breakoutBase AS(
            SELECT
                g.targetWeekStartDate,
                g.targetWeekEndDate,
                c.fiscalYear,
                g.fiscalQuarterLabel,
                g.fiscalWeekCode,
                g.weekLabel,
                c.weekEndingLabel,

                g.filterLob,
                g.filterPlatform,

                g.metricName,
                CASE WHEN g.metricName='nbv' THEN 'Total UPV' ELSE mc.metricLabel END AS metricLabel,
                mc.metricDescription,
                mc.metricKind,
                mc.displayFormat,
                mc.changeUnit,
                mc.sortOrder AS metricSortOrder,

                g.breakoutType,
                bc.breakoutLabel,
                bc.sortOrder AS breakoutSortOrder,
                g.breakoutValue,

                g.thisWeekNumerator,
                g.thisWeekDenominator,
                g.priorWeekNumerator,
                g.priorWeekDenominator,
                g.fourWeekTrendNumerator,
                g.fourWeekTrendDenominator,
                g.sameWeekLyNumerator,
                g.sameWeekLyDenominator,

                g.thisWeekDataAvailable,
                g.priorWeekDataAvailable,
                g.fourWeekTrendWeekCount,
                g.sameWeekLyDataAvailable
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long g
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
              ON mc.metricName=g.metricName
             AND mc.isActive
             AND mc.showOnBreakouts
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static bc
              ON bc.breakoutType=g.breakoutType
             AND bc.isActive
             AND bc.isPrebuiltBreakout
            LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c
              ON c.weekStartDate=g.targetWeekStartDate
            WHERE g.targetWeekStartDate BETWEEN v_sourceWeekFrom AND v_weekTo
        ),

        breakoutComparison AS(
            SELECT
                b.*,
                'priorWeek' AS comparisonType,
                'Prior week' AS comparisonLabel,
                10 AS comparisonSortOrder,
                b.priorWeekDataAvailable AS comparisonDataAvailable,
                b.priorWeekDataAvailable AS comparisonWindowComplete,
                b.priorWeekNumerator AS comparisonNumerator,
                b.priorWeekDenominator AS comparisonDenominator
            FROM breakoutBase b

            UNION ALL

            SELECT
                b.*,
                'fourWeek',
                '4-wk trend',
                20,
                b.fourWeekTrendWeekCount>0,
                b.fourWeekTrendWeekCount=4,
                CASE
                    WHEN b.metricKind='count' AND b.fourWeekTrendWeekCount>0
                        THEN try_divide(b.fourWeekTrendNumerator,cast(b.fourWeekTrendWeekCount AS DOUBLE))
                    ELSE b.fourWeekTrendNumerator
                END,
                CASE
                    WHEN b.metricKind='count' THEN NULL
                    ELSE b.fourWeekTrendDenominator
                END
            FROM breakoutBase b

            UNION ALL

            SELECT
                b.*,
                'lastYear',
                'Same wk LY',
                30,
                b.sameWeekLyDataAvailable,
                b.sameWeekLyDataAvailable,
                b.sameWeekLyNumerator,
                b.sameWeekLyDenominator
            FROM breakoutBase b
        ),

        breakoutValues AS(
            SELECT
                *,
                CASE
                    WHEN NOT thisWeekDataAvailable THEN NULL
                    WHEN metricKind='ratio'
                        THEN try_divide(thisWeekNumerator,thisWeekDenominator)
                    ELSE thisWeekNumerator
                END AS currentValue,

                CASE
                    WHEN NOT comparisonDataAvailable THEN NULL
                    WHEN metricKind='ratio'
                        THEN try_divide(comparisonNumerator,comparisonDenominator)
                    ELSE comparisonNumerator
                END AS comparisonValue
            FROM breakoutComparison
        ),

        overviewBase AS(
            SELECT
                g.targetWeekStartDate,
                g.filterLob,
                g.filterPlatform,
                g.metricName,
                mc.metricKind,
                mc.changeUnit,

                g.thisWeekNumerator,
                g.thisWeekDenominator,
                g.priorWeekNumerator,
                g.priorWeekDenominator,
                g.fourWeekTrendNumerator,
                g.fourWeekTrendDenominator,
                g.sameWeekLyNumerator,
                g.sameWeekLyDenominator,

                g.thisWeekDataAvailable,
                g.priorWeekDataAvailable,
                g.fourWeekTrendWeekCount,
                g.sameWeekLyDataAvailable
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long g
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
              ON mc.metricName=g.metricName
             AND mc.isActive
             AND mc.showOnBreakouts
            WHERE g.targetWeekStartDate BETWEEN v_sourceWeekFrom AND v_weekTo
        ),

        overviewComparison AS(
            SELECT
                o.*,
                'priorWeek' AS comparisonType,
                o.priorWeekDataAvailable AS comparisonDataAvailable,
                o.priorWeekNumerator AS comparisonNumerator,
                o.priorWeekDenominator AS comparisonDenominator
            FROM overviewBase o

            UNION ALL

            SELECT
                o.*,
                'fourWeek',
                o.fourWeekTrendWeekCount>0,
                CASE
                    WHEN o.metricKind='count' AND o.fourWeekTrendWeekCount>0
                        THEN try_divide(o.fourWeekTrendNumerator,cast(o.fourWeekTrendWeekCount AS DOUBLE))
                    ELSE o.fourWeekTrendNumerator
                END,
                CASE
                    WHEN o.metricKind='count' THEN NULL
                    ELSE o.fourWeekTrendDenominator
                END
            FROM overviewBase o

            UNION ALL

            SELECT
                o.*,
                'lastYear',
                o.sameWeekLyDataAvailable,
                o.sameWeekLyNumerator,
                o.sameWeekLyDenominator
            FROM overviewBase o
        ),

        overviewValues AS(
            SELECT
                *,
                CASE
                    WHEN NOT thisWeekDataAvailable THEN NULL
                    WHEN metricKind='ratio'
                        THEN try_divide(thisWeekNumerator,thisWeekDenominator)
                    ELSE thisWeekNumerator
                END AS toplineCurrentValue,

                CASE
                    WHEN NOT comparisonDataAvailable THEN NULL
                    WHEN metricKind='ratio'
                        THEN try_divide(comparisonNumerator,comparisonDenominator)
                    ELSE comparisonNumerator
                END AS toplineComparisonValue
            FROM overviewComparison
        ),

        targetScored AS(
            SELECT
                b.*,

                o.toplineCurrentValue,
                o.toplineComparisonValue,
                o.thisWeekDenominator AS toplineCurrentDenominator,
                o.comparisonDenominator AS toplineComparisonDenominator,

                b.currentValue-b.comparisonValue AS absoluteDiffValue,

                CASE
                    WHEN b.currentValue IS NULL OR b.comparisonValue IS NULL THEN NULL
                    WHEN b.changeUnit='pp'
                        THEN 100D*(b.currentValue-b.comparisonValue)
                    WHEN b.changeUnit='pct'
                        THEN 100D*(try_divide(b.currentValue,b.comparisonValue)-1D)
                END AS changeRaw,

                CASE
                    WHEN NOT b.comparisonDataAvailable
                      OR b.currentValue IS NULL
                      OR b.comparisonValue IS NULL THEN NULL

                    WHEN b.metricKind='count'
                        THEN 100D*try_divide(
                            b.currentValue-b.comparisonValue,
                            o.toplineComparisonValue
                        )

                    WHEN b.metricKind='ratio'
                        THEN 100D*(
                            try_divide(b.thisWeekNumerator,o.thisWeekDenominator)
                            -try_divide(b.comparisonNumerator,o.comparisonDenominator)
                        )
                END AS impactOnToplineRaw,

                CASE
                    WHEN b.metricKind='count' THEN 'pct'
                    WHEN b.metricKind='ratio' THEN 'pp'
                END AS impactOnToplineUnit
            FROM breakoutValues b
            JOIN overviewValues o
              ON o.targetWeekStartDate=b.targetWeekStartDate
             AND o.filterLob=b.filterLob
             AND o.filterPlatform=b.filterPlatform
             AND o.metricName=b.metricName
             AND o.comparisonType=b.comparisonType
            WHERE b.targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
              AND b.comparisonDataAvailable
        ),

        targetRanked AS(
            SELECT
                *,
                row_number() OVER(
                    PARTITION BY
                        targetWeekStartDate,
                        filterLob,
                        filterPlatform,
                        metricName,
                        breakoutType,
                        comparisonType
                    ORDER BY
                        abs(impactOnToplineRaw) DESC NULLS LAST,
                        abs(absoluteDiffValue) DESC NULLS LAST,
                        breakoutValue
                ) AS impactRankWithinBreakout
            FROM targetScored
            WHERE impactOnToplineRaw IS NOT NULL
        ),

        sizeConfig AS(
            SELECT * FROM VALUES
                ('top5','Top 5',5,10),
                ('top10','Top 10',10,20),
                ('all','All',100,30)
            AS s(displaySize,displaySizeLabel,displayLimit,displaySizeSortOrder)
        ),

        targetMapping AS(
            SELECT
                r.targetWeekStartDate,
                r.targetWeekEndDate,
                r.fiscalYear,
                r.fiscalQuarterLabel,
                r.fiscalWeekCode,
                r.weekLabel,
                r.weekEndingLabel,

                r.filterLob,
                r.filterPlatform,

                r.metricName,
                r.metricLabel,
                r.metricDescription,
                r.metricKind,
                r.displayFormat,
                r.changeUnit,
                r.metricSortOrder,

                r.breakoutType,
                r.breakoutLabel,
                r.breakoutSortOrder,

                r.comparisonType,
                r.comparisonLabel,
                r.comparisonSortOrder,
                r.comparisonWindowComplete,

                s.displaySize,
                s.displaySizeLabel,
                s.displayLimit,
                s.displaySizeSortOrder,

                r.breakoutValue AS sourceBreakoutValue,

                CASE
                    WHEN r.impactRankWithinBreakout<=s.displayLimit
                        THEN concat('VALUE::',coalesce(r.breakoutValue,'(null)'))
                    ELSE 'OTHER::REMAINDER'
                END AS cardKey,

                CASE
                    WHEN r.impactRankWithinBreakout<=s.displayLimit
                        THEN r.breakoutValue
                    ELSE '(Other)'
                END AS cardBreakoutValue,

                r.impactRankWithinBreakout>s.displayLimit AS isSyntheticOtherMember,
                r.impactRankWithinBreakout,

                r.thisWeekNumerator,
                r.thisWeekDenominator,
                r.comparisonNumerator,
                r.comparisonDenominator,

                r.toplineCurrentValue,
                r.toplineComparisonValue,
                r.toplineCurrentDenominator,
                r.toplineComparisonDenominator
            FROM targetRanked r
            CROSS JOIN sizeConfig s
        ),

        targetBucketAgg AS(
            SELECT
                targetWeekStartDate,
                targetWeekEndDate,
                fiscalYear,
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

                breakoutType,
                breakoutLabel,
                breakoutSortOrder,

                comparisonType,
                comparisonLabel,
                comparisonSortOrder,
                min(CASE WHEN comparisonWindowComplete THEN 1 ELSE 0 END)=1
                    AS comparisonWindowComplete,

                displaySize,
                displaySizeLabel,
                displayLimit,
                displaySizeSortOrder,

                cardKey,
                cardBreakoutValue AS breakoutValue,

                max(CASE WHEN isSyntheticOtherMember THEN 1 ELSE 0 END)=1
                    AS isOtherBucket,

                CASE
                    WHEN max(CASE WHEN isSyntheticOtherMember THEN 1 ELSE 0 END)=1
                        THEN cast(displayLimit+1 AS BIGINT)
                    ELSE min(impactRankWithinBreakout)
                END AS displayRank,

                sum(thisWeekNumerator) AS targetCurrentNumerator,
                sum(thisWeekDenominator) AS targetCurrentDenominator,
                sum(comparisonNumerator) AS targetComparisonNumerator,
                sum(comparisonDenominator) AS targetComparisonDenominator,

                max(toplineCurrentValue) AS toplineCurrentValue,
                max(toplineComparisonValue) AS toplineComparisonValue,
                max(toplineCurrentDenominator) AS toplineCurrentDenominator,
                max(toplineComparisonDenominator) AS toplineComparisonDenominator
            FROM targetMapping
            GROUP BY
                targetWeekStartDate,
                targetWeekEndDate,
                fiscalYear,
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
                breakoutType,
                breakoutLabel,
                breakoutSortOrder,
                comparisonType,
                comparisonLabel,
                comparisonSortOrder,
                displaySize,
                displaySizeLabel,
                displayLimit,
                displaySizeSortOrder,
                cardKey,
                cardBreakoutValue
        ),

        targetBucketValues AS(
            SELECT
                *,
                CASE
                    WHEN metricKind='ratio'
                        THEN try_divide(targetCurrentNumerator,targetCurrentDenominator)
                    ELSE targetCurrentNumerator
                END AS targetCurrentValue,

                CASE
                    WHEN metricKind='ratio'
                        THEN try_divide(targetComparisonNumerator,targetComparisonDenominator)
                    ELSE targetComparisonNumerator
                END AS targetComparisonValue
            FROM targetBucketAgg
        ),

        targetCardsCalculated AS(
            SELECT
                *,

                targetCurrentValue-targetComparisonValue AS targetAbsoluteDiffValue,

                CASE
                    WHEN targetCurrentValue IS NULL OR targetComparisonValue IS NULL THEN NULL
                    WHEN changeUnit='pp'
                        THEN 100D*(targetCurrentValue-targetComparisonValue)
                    WHEN changeUnit='pct'
                        THEN 100D*(try_divide(targetCurrentValue,targetComparisonValue)-1D)
                END AS targetChangeRaw,

                CASE
                    WHEN metricKind='count'
                        THEN 100D*try_divide(
                            targetCurrentValue-targetComparisonValue,
                            toplineComparisonValue
                        )

                    WHEN metricKind='ratio'
                        THEN 100D*(
                            try_divide(targetCurrentNumerator,toplineCurrentDenominator)
                            -try_divide(targetComparisonNumerator,toplineComparisonDenominator)
                        )
                END AS impactOnToplineRaw,

                CASE
                    WHEN metricKind='count' THEN 'pct'
                    WHEN metricKind='ratio' THEN 'pp'
                END AS impactOnToplineUnit,

                CASE
                    WHEN targetCurrentValue>targetComparisonValue THEN 'up'
                    WHEN targetCurrentValue<targetComparisonValue THEN 'down'
                    WHEN targetCurrentValue=targetComparisonValue THEN 'flat'
                    ELSE 'unavailable'
                END AS cardDirection
            FROM targetBucketValues
        ),

        targetCardsRounded AS(
            SELECT
                *,
                CASE
                    WHEN targetChangeRaw IS NULL THEN NULL
                    WHEN abs(targetChangeRaw)<0.05D THEN 0D
                    ELSE round(targetChangeRaw,1)
                END AS targetChangeValue,

                CASE
                    WHEN impactOnToplineRaw IS NULL THEN NULL
                    WHEN abs(impactOnToplineRaw)<0.05D THEN 0D
                    ELSE round(impactOnToplineRaw,1)
                END AS impactOnToplineValue
            FROM targetCardsCalculated
        ),

        targetCardsOrdered AS(
            SELECT
                *,
                row_number() OVER(
                    PARTITION BY
                        targetWeekStartDate,
                        filterLob,
                        filterPlatform,
                        metricName,
                        breakoutType,
                        comparisonType,
                        displaySize
                    ORDER BY
                        targetCurrentValue DESC NULLS LAST,
                        isOtherBucket,
                        breakoutValue
                ) AS cardSortOrder
            FROM targetCardsRounded
        ),

        -- =====================================================================
        -- Target-week membership is now frozen.
        -- Join those exact members to the preceding historical weeks.
        -- =====================================================================
        seriesMembers AS(
            SELECT
                m.targetWeekStartDate,
                m.filterLob,
                m.filterPlatform,
                m.metricName,
                m.breakoutType,
                m.comparisonType,
                m.displaySize,
                m.cardKey,
                m.cardBreakoutValue,
                m.sourceBreakoutValue,

                c.weekStartDate AS seriesWeekStartDate,
                c.weekEndDate AS seriesWeekEndDate,
                c.fiscalYear AS seriesFiscalYear,
                c.fiscalQuarterLabel AS seriesFiscalQuarterLabel,
                c.fiscalWeekCode AS seriesFiscalWeekCode,
                c.weekLabel AS seriesWeekLabel,
                c.weekEndingLabel AS seriesWeekEndingLabel,

                b.thisWeekNumerator AS seriesCurrentNumerator,
                b.thisWeekDenominator AS seriesCurrentDenominator,
                b.comparisonNumerator AS seriesComparisonNumerator,
                b.comparisonDenominator AS seriesComparisonDenominator,

                b.thisWeekDataAvailable AS seriesActualDataAvailable,
                b.comparisonDataAvailable AS seriesBenchmarkDataAvailable
            FROM targetMapping m
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c
              ON c.weekStartDate BETWEEN
                    date_add(m.targetWeekStartDate,-7*(v_historyWeeks-1))
                    AND m.targetWeekStartDate
            LEFT JOIN breakoutComparison b
              ON b.targetWeekStartDate=c.weekStartDate
             AND b.filterLob=m.filterLob
             AND b.filterPlatform=m.filterPlatform
             AND b.metricName=m.metricName
             AND b.breakoutType=m.breakoutType
             AND b.breakoutValue=m.sourceBreakoutValue
             AND b.comparisonType=m.comparisonType
        ),

        seriesAgg AS(
            SELECT
                targetWeekStartDate,
                filterLob,
                filterPlatform,
                metricName,
                breakoutType,
                comparisonType,
                displaySize,
                cardKey,
                cardBreakoutValue,

                seriesWeekStartDate,
                seriesWeekEndDate,
                seriesFiscalYear,
                seriesFiscalQuarterLabel,
                seriesFiscalWeekCode,
                seriesWeekLabel,
                seriesWeekEndingLabel,

                sum(coalesce(seriesCurrentNumerator,0D)) AS seriesCurrentNumerator,
                sum(coalesce(seriesCurrentDenominator,0D)) AS seriesCurrentDenominator,
                sum(coalesce(seriesComparisonNumerator,0D)) AS seriesComparisonNumerator,
                sum(coalesce(seriesComparisonDenominator,0D)) AS seriesComparisonDenominator,

                max(CASE WHEN seriesActualDataAvailable THEN 1 ELSE 0 END)=1
                    AS seriesActualDataAvailable,

                max(CASE WHEN seriesBenchmarkDataAvailable THEN 1 ELSE 0 END)=1
                    AS seriesBenchmarkDataAvailable
            FROM seriesMembers
            GROUP BY
                targetWeekStartDate,
                filterLob,
                filterPlatform,
                metricName,
                breakoutType,
                comparisonType,
                displaySize,
                cardKey,
                cardBreakoutValue,
                seriesWeekStartDate,
                seriesWeekEndDate,
                seriesFiscalYear,
                seriesFiscalQuarterLabel,
                seriesFiscalWeekCode,
                seriesWeekLabel,
                seriesWeekEndingLabel
        ),

        seriesValues AS(
            SELECT
                s.*,
                c.metricKind,
                c.displayFormat,

                CASE
                    WHEN c.metricKind='ratio' THEN
                        CASE
                            WHEN s.seriesActualDataAvailable
                                THEN try_divide(s.seriesCurrentNumerator,s.seriesCurrentDenominator)
                            ELSE NULL
                        END
                    ELSE
                        CASE
                            WHEN s.seriesActualDataAvailable
                                THEN s.seriesCurrentNumerator
                            ELSE 0D
                        END
                END AS seriesActualValue,

                CASE
                    WHEN NOT s.seriesBenchmarkDataAvailable THEN NULL
                    WHEN c.metricKind='ratio'
                        THEN try_divide(s.seriesComparisonNumerator,s.seriesComparisonDenominator)
                    ELSE s.seriesComparisonNumerator
                END AS seriesBenchmarkValue
            FROM seriesAgg s
            JOIN targetCardsOrdered c
              ON c.targetWeekStartDate=s.targetWeekStartDate
             AND c.filterLob=s.filterLob
             AND c.filterPlatform=s.filterPlatform
             AND c.metricName=s.metricName
             AND c.breakoutType=s.breakoutType
             AND c.comparisonType=s.comparisonType
             AND c.displaySize=s.displaySize
             AND c.cardKey=s.cardKey
        ),

        seriesOrdered AS(
            SELECT
                *,
                row_number() OVER(
                    PARTITION BY
                        targetWeekStartDate,
                        filterLob,
                        filterPlatform,
                        metricName,
                        breakoutType,
                        comparisonType,
                        displaySize,
                        cardKey
                    ORDER BY seriesWeekStartDate
                ) AS seriesSortOrder
            FROM seriesValues
        ),

        finalJoined AS(
            SELECT
                c.targetWeekStartDate,
                c.targetWeekEndDate,
                c.fiscalYear,
                c.fiscalQuarterLabel,
                c.fiscalWeekCode,
                c.weekLabel,
                c.weekEndingLabel,

                c.filterLob,
                c.filterPlatform,

                c.metricName,
                c.metricLabel,
                c.metricDescription,
                c.metricKind,
                c.displayFormat,
                c.changeUnit,
                c.metricSortOrder,

                c.breakoutType,
                c.breakoutLabel,
                c.breakoutSortOrder,

                c.comparisonType,
                c.comparisonLabel,
                c.comparisonSortOrder,
                c.comparisonWindowComplete,

                c.displaySize,
                c.displaySizeLabel,
                c.displayLimit,
                c.displaySizeSortOrder,

                c.cardKey,
                c.breakoutValue,
                c.isOtherBucket,
                c.displayRank,
                c.cardSortOrder,
                c.cardDirection,

                c.targetCurrentValue,
                c.targetComparisonValue,
                c.targetAbsoluteDiffValue,
                c.targetChangeValue,
                c.impactOnToplineValue,
                c.impactOnToplineUnit,

                v_historyWeeks AS sparklineWeeks,

                s.seriesWeekStartDate,
                s.seriesWeekEndDate,
                s.seriesFiscalYear,
                s.seriesFiscalQuarterLabel,
                s.seriesFiscalWeekCode,
                s.seriesWeekLabel,
                s.seriesWeekEndingLabel,
                s.seriesSortOrder,

                s.seriesWeekStartDate=c.targetWeekStartDate AS isSelectedWeek,

                s.seriesActualValue,
                s.seriesBenchmarkValue,
                s.seriesActualDataAvailable,
                s.seriesBenchmarkDataAvailable
            FROM targetCardsOrdered c
            JOIN seriesOrdered s
              ON s.targetWeekStartDate=c.targetWeekStartDate
             AND s.filterLob=c.filterLob
             AND s.filterPlatform=c.filterPlatform
             AND s.metricName=c.metricName
             AND s.breakoutType=c.breakoutType
             AND s.comparisonType=c.comparisonType
             AND s.displaySize=c.displaySize
             AND s.cardKey=c.cardKey
        ),

        formatted AS(
            SELECT
                *,

                CASE
                    WHEN targetCurrentValue IS NULL THEN NULL
                    WHEN displayFormat='percent'
                        THEN concat(format_number(100D*targetCurrentValue,1),'%')
                    WHEN abs(targetCurrentValue)>=1000000000D
                        THEN concat(regexp_replace(format_number(targetCurrentValue/1000000000D,1),'\\.0$',''),'B')
                    WHEN abs(targetCurrentValue)>=1000000D
                        THEN concat(regexp_replace(format_number(targetCurrentValue/1000000D,1),'\\.0$',''),'M')
                    WHEN abs(targetCurrentValue)>=1000D
                        THEN concat(regexp_replace(format_number(targetCurrentValue/1000D,1),'\\.0$',''),'K')
                    ELSE format_number(targetCurrentValue,0)
                END AS targetCurrentValueDisplay,

                CASE
                    WHEN targetComparisonValue IS NULL THEN NULL
                    WHEN displayFormat='percent'
                        THEN concat(format_number(100D*targetComparisonValue,1),'%')
                    WHEN abs(targetComparisonValue)>=1000000000D
                        THEN concat(regexp_replace(format_number(targetComparisonValue/1000000000D,1),'\\.0$',''),'B')
                    WHEN abs(targetComparisonValue)>=1000000D
                        THEN concat(regexp_replace(format_number(targetComparisonValue/1000000D,1),'\\.0$',''),'M')
                    WHEN abs(targetComparisonValue)>=1000D
                        THEN concat(regexp_replace(format_number(targetComparisonValue/1000D,1),'\\.0$',''),'K')
                    ELSE format_number(targetComparisonValue,0)
                END AS targetComparisonValueDisplay,

                CASE
                    WHEN targetAbsoluteDiffValue IS NULL THEN NULL
                    WHEN metricKind='ratio'
                        THEN concat(
                            CASE WHEN targetAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            format_number(
                                CASE
                                    WHEN abs(100D*targetAbsoluteDiffValue)<0.05D THEN 0D
                                    ELSE round(100D*targetAbsoluteDiffValue,1)
                                END,
                                1
                            ),
                            'pp'
                        )
                    WHEN abs(targetAbsoluteDiffValue)>=1000000000D
                        THEN concat(
                            CASE WHEN targetAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(targetAbsoluteDiffValue/1000000000D,1),'\\.0$',''),
                            'B'
                        )
                    WHEN abs(targetAbsoluteDiffValue)>=1000000D
                        THEN concat(
                            CASE WHEN targetAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(targetAbsoluteDiffValue/1000000D,1),'\\.0$',''),
                            'M'
                        )
                    WHEN abs(targetAbsoluteDiffValue)>=1000D
                        THEN concat(
                            CASE WHEN targetAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(targetAbsoluteDiffValue/1000D,1),'\\.0$',''),
                            'K'
                        )
                    ELSE concat(
                        CASE WHEN targetAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                        format_number(targetAbsoluteDiffValue,0)
                    )
                END AS targetAbsoluteDiffDisplay,

                CASE
                    WHEN targetChangeValue IS NULL THEN NULL
                    WHEN changeUnit='pp'
                        THEN concat(
                            CASE WHEN targetChangeValue>0D THEN '+' ELSE '' END,
                            format_number(targetChangeValue,1),
                            'pp'
                        )
                    ELSE concat(
                        CASE WHEN targetChangeValue>0D THEN '+' ELSE '' END,
                        format_number(targetChangeValue,1),
                        '%'
                    )
                END AS targetChangeDisplay,

                CASE
                    WHEN impactOnToplineValue IS NULL THEN NULL
                    WHEN impactOnToplineUnit='pp'
                        THEN concat(
                            CASE WHEN impactOnToplineValue>0D THEN '+' ELSE '' END,
                            format_number(impactOnToplineValue,1),
                            'pp'
                        )
                    ELSE concat(
                        CASE WHEN impactOnToplineValue>0D THEN '+' ELSE '' END,
                        format_number(impactOnToplineValue,1),
                        '%'
                    )
                END AS impactOnToplineDisplay,

                CASE
                    WHEN seriesActualValue IS NULL THEN NULL
                    WHEN displayFormat='percent'
                        THEN concat(format_number(100D*seriesActualValue,1),'%')
                    WHEN abs(seriesActualValue)>=1000000000D
                        THEN concat(regexp_replace(format_number(seriesActualValue/1000000000D,1),'\\.0$',''),'B')
                    WHEN abs(seriesActualValue)>=1000000D
                        THEN concat(regexp_replace(format_number(seriesActualValue/1000000D,1),'\\.0$',''),'M')
                    WHEN abs(seriesActualValue)>=1000D
                        THEN concat(regexp_replace(format_number(seriesActualValue/1000D,1),'\\.0$',''),'K')
                    ELSE format_number(seriesActualValue,0)
                END AS seriesActualValueDisplay,

                CASE
                    WHEN seriesBenchmarkValue IS NULL THEN NULL
                    WHEN displayFormat='percent'
                        THEN concat(format_number(100D*seriesBenchmarkValue,1),'%')
                    WHEN abs(seriesBenchmarkValue)>=1000000000D
                        THEN concat(regexp_replace(format_number(seriesBenchmarkValue/1000000000D,1),'\\.0$',''),'B')
                    WHEN abs(seriesBenchmarkValue)>=1000000D
                        THEN concat(regexp_replace(format_number(seriesBenchmarkValue/1000000D,1),'\\.0$',''),'M')
                    WHEN abs(seriesBenchmarkValue)>=1000D
                        THEN concat(regexp_replace(format_number(seriesBenchmarkValue/1000D,1),'\\.0$',''),'K')
                    ELSE format_number(seriesBenchmarkValue,0)
                END AS seriesBenchmarkValueDisplay
            FROM finalJoined
        )

        SELECT
            targetWeekStartDate,
            targetWeekEndDate,
            fiscalYear,
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

            breakoutType,
            breakoutLabel,
            breakoutSortOrder,

            comparisonType,
            comparisonLabel,
            comparisonSortOrder,
            comparisonWindowComplete,

            displaySize,
            displaySizeLabel,
            displayLimit,
            displaySizeSortOrder,

            cardKey,
            breakoutValue,
            isOtherBucket,
            displayRank,
            cardSortOrder,
            cardDirection,

            targetCurrentValue,
            targetCurrentValueDisplay,

            targetComparisonValue,
            targetComparisonValueDisplay,

            targetAbsoluteDiffValue,
            targetAbsoluteDiffDisplay,

            targetChangeValue,
            targetChangeDisplay,

            impactOnToplineValue,
            impactOnToplineDisplay,
            impactOnToplineUnit,

            sparklineWeeks,

            seriesWeekStartDate,
            seriesWeekEndDate,
            seriesFiscalYear,
            seriesFiscalQuarterLabel,
            seriesFiscalWeekCode,
            seriesWeekLabel,
            seriesWeekEndingLabel,
            seriesSortOrder,
            isSelectedWeek,

            seriesActualValue,
            seriesActualValueDisplay,
            seriesBenchmarkValue,
            seriesBenchmarkValueDisplay,
            seriesActualDataAvailable,
            seriesBenchmarkDataAvailable,

            v_processedAt AS appProcessedAt
        FROM formatted;

        -- =====================================================================
        -- 7. Success
        -- =====================================================================
        SELECT
            'SUCCESS' AS status,
            v_weekFrom AS rebuiltTargetWeekFrom,
            v_weekTo AS rebuiltTargetWeekTo,
            v_sourceWeekFrom AS sourceHistoryWeekFrom,
            v_historyWeeks AS sparklineWeeks,
            'priorWeek | fourWeek | lastYear' AS supportedComparisons,
            'top5 | top10 | all' AS supportedDisplaySizes,
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsAbsoluteTrend_long' AS targetObject,
            v_processedAt AS appProcessedAt;
    END IF;
END;

-- ============================================================================
-- DEVELOPMENT
-- ============================================================================

-- ONE TIME ONLY:
-- DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsAbsoluteTrend_long;

-- Validation:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appBreakoutsAbsoluteTrend_long(
--     p_asOfDate=>DATE '2026-09-28',
--     p_weeksToRebuild=>1,
--     p_validateOnly=>TRUE
-- );

-- Rebuild:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appBreakoutsAbsoluteTrend_long(
--     p_asOfDate=>DATE '2026-09-28',
--     p_weeksToRebuild=>12,
--     p_validateOnly=>FALSE
-- );

-- ============================================================================
-- SCREENSHOT / API EXAMPLE
--
-- Q3 2026
-- W7
-- Total UPV
-- Channel
-- 4-wk
--
-- Current visual can use displaySize='all'.
-- ============================================================================

-- SELECT *
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsAbsoluteTrend_long
-- WHERE fiscalYear=2026
--   AND targetWeekStartDate=DATE '2026-08-09'
--   AND filterLob='All'
--   AND filterPlatform='All'
--   AND metricName='nbv'
--   AND breakoutType='channel'
--   AND comparisonType='fourWeek'
--   AND displaySize='all'
-- ORDER BY cardSortOrder,seriesSortOrder;

-- ============================================================================
-- CARD HEADER/SUMMARY
-- One row per card after DISTINCT.
-- ============================================================================

-- SELECT DISTINCT
--     cardKey,
--     breakoutValue,
--     isOtherBucket,
--     cardSortOrder,
--     cardDirection,
--     targetCurrentValue,
--     targetCurrentValueDisplay,
--     targetComparisonValue,
--     targetComparisonValueDisplay,
--     targetAbsoluteDiffValue,
--     targetAbsoluteDiffDisplay,
--     targetChangeValue,
--     targetChangeDisplay,
--     impactOnToplineValue,
--     impactOnToplineDisplay,
--     impactOnToplineUnit
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsAbsoluteTrend_long
-- WHERE targetWeekStartDate=DATE '2026-08-09'
--   AND filterLob='All'
--   AND filterPlatform='All'
--   AND metricName='nbv'
--   AND breakoutType='channel'
--   AND comparisonType='fourWeek'
--   AND displaySize='all'
-- ORDER BY cardSortOrder;

-- Screenshot-style result:
--
-- Paid Search
--   494K
--   +42.8K
--   +2.6% of topline
--
-- Organic Search
--   339K
--   -19.5K
--   -1.2% of topline
--
-- Direct
--   299K
--   -27.5K
--   -1.6% of topline
--
-- Social
--   144K
--   +2.2K
--   +0.1% of topline
--
-- ...

-- ============================================================================
-- SPARKLINE SERIES FOR ONE CARD
-- No calculations required in API.
-- ============================================================================

-- SELECT
--     breakoutValue,
--     cardDirection,
--     seriesWeekStartDate,
--     seriesFiscalWeekCode,
--     seriesWeekLabel,
--     seriesSortOrder,
--     seriesActualValue,
--     seriesActualValueDisplay,
--     seriesBenchmarkValue,
--     seriesBenchmarkValueDisplay,
--     isSelectedWeek
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsAbsoluteTrend_long
-- WHERE targetWeekStartDate=DATE '2026-08-09'
--   AND filterLob='All'
--   AND filterPlatform='All'
--   AND metricName='nbv'
--   AND breakoutType='channel'
--   AND comparisonType='fourWeek'
--   AND displaySize='all'
--   AND cardKey='VALUE::Paid Search'
-- ORDER BY seriesSortOrder;

-- ============================================================================
-- ELIGIBILITY CHECKS
-- Expected zero rows.
-- ============================================================================

-- SELECT DISTINCT metricName,metricLabel
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsAbsoluteTrend_long a
-- WHERE NOT EXISTS(
--     SELECT 1
--     FROM prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
--     WHERE mc.metricName=a.metricName
--       AND mc.isActive
--       AND mc.showOnBreakouts
-- );

-- SELECT DISTINCT breakoutType,breakoutLabel
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsAbsoluteTrend_long a
-- WHERE NOT EXISTS(
--     SELECT 1
--     FROM prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static bc
--     WHERE bc.breakoutType=a.breakoutType
--       AND bc.isActive
--       AND bc.isPrebuiltBreakout
-- );

-- ============================================================================
-- SERIES CHECK
-- Every card should normally have 8 reporting-week rows.
-- ============================================================================

-- SELECT
--     targetWeekStartDate,
--     metricName,
--     breakoutType,
--     comparisonType,
--     displaySize,
--     cardKey,
--     count(*) AS seriesRows
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsAbsoluteTrend_long
-- GROUP BY
--     targetWeekStartDate,
--     metricName,
--     breakoutType,
--     comparisonType,
--     displaySize,
--     cardKey
-- HAVING count(*)<>8;

-- ============================================================================
-- DUPLICATE CHECK
-- Expected zero rows.
-- ============================================================================

-- SELECT
--     targetWeekStartDate,
--     filterLob,
--     filterPlatform,
--     metricName,
--     breakoutType,
--     comparisonType,
--     displaySize,
--     cardKey,
--     seriesWeekStartDate,
--     count(*) AS rowCount
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsAbsoluteTrend_long
-- GROUP BY
--     targetWeekStartDate,
--     filterLob,
--     filterPlatform,
--     metricName,
--     breakoutType,
--     comparisonType,
--     displaySize,
--     cardKey,
--     seriesWeekStartDate
-- HAVING count(*)>1;