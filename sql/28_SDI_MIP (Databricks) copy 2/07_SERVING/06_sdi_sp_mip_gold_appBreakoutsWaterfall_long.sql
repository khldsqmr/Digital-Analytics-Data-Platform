-- ============================================================================
-- FILE  : 06_sdi_sp_mip_gold_appBreakoutsWaterfall_long.sql
-- LAYER : GOLD / APP
-- TAB   : Breakouts
-- SECTION: What changed vs selected comparison
--
-- PURPOSE:
--   Fully application-ready waterfall contract.
--
-- UI FILTERS:
--   Quarter
--   Week
--   Metric
--   Breakout
--   Comparator = priorWeek | fourWeek | lastYear
--   Show       = top5 | top10 | all
--   LOB / Platform where applicable
--
-- APP-GOLD ELIGIBILITY:
--   Metrics   -> isActive AND showOnBreakouts
--   Breakouts -> isActive AND isPrebuiltBreakout
--
-- DISPLAY SIZE:
--   top5  = Top 5 comparator-ranked slices + (Other)
--   top10 = Top 10 comparator-ranked slices + (Other)
--   all   = Top 100 comparator-ranked slices + (Other)
--
-- API/FRONTEND DO NOT CALCULATE:
--   Top-N / Other
--   increases / decreases
--   increase/decrease counts
--   net change
--   topline %/pp change
--   bar order
--   cumulative waterfall bar start/end coordinates
--   display formatting
--
-- Numerators/denominators remain internal to this procedure only.
-- ============================================================================

-- ONE-TIME MIGRATION ONLY:
-- DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsWaterfall_long;

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_gold_appBreakoutsWaterfall_long(
    IN p_asOfDate DATE DEFAULT NULL,
    IN p_weeksToRebuild INT DEFAULT 1,
    IN p_validateOnly BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'MIP Gold App: Breakouts waterfall with approved Breakouts metrics, Top5/Top10/All+Other, precomputed summaries and chart geometry.'
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

    -- =========================================================================
    -- 2. Eligible source preflight
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
            SET MESSAGE_TEXT='Breakout Gold has no eligible Breakouts App rows for the requested week range.';
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
            SET MESSAGE_TEXT='Overview Gold has no eligible Breakouts metrics for the requested week range.';
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
        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='Fiscal Calendar has no rows for the requested week range.';
    END IF;

    -- =========================================================================
    -- 3. Eligible-grain validation
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
        WHERE g.targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
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
            SET MESSAGE_TEXT='Duplicate eligible Breakout Gold keys detected.';
    END IF;

    IF EXISTS(
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long g
        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
          ON mc.metricName=g.metricName
         AND mc.isActive
         AND mc.showOnBreakouts
        WHERE g.targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        GROUP BY
            g.targetWeekStartDate,
            g.filterLob,
            g.filterPlatform,
            g.metricName
        HAVING count(*)>1
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='Duplicate eligible Overview Gold keys detected.';
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
    -- 4. Validation-only
    -- =========================================================================
    IF p_validateOnly THEN
        SELECT
            'VALIDATION_ONLY' AS status,
            v_weekFrom AS rebuildWeekStartFrom,
            v_weekTo AS rebuildWeekStartTo,
            v_weekEndTo AS latestWeekEndDate,
            CASE WHEN v_asOfDate<v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,
            'priorWeek | fourWeek | lastYear' AS supportedComparisons,
            'top5 | top10 | all' AS supportedDisplaySizes,
            'isActive=true AND showOnBreakouts=true' AS metricEligibility,
            'isActive=true AND isPrebuiltBreakout=true' AS breakoutEligibility,
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsWaterfall_long' AS targetObject,
            'Validation passed. No Gold App table was created or modified.' AS message;
    ELSE

        -- =====================================================================
        -- 5. Application-ready target contract
        -- =====================================================================
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsWaterfall_long(
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
            comparisonDataAvailable BOOLEAN,
            comparisonWindowComplete BOOLEAN,

            displaySize STRING,
            displaySizeLabel STRING,
            displayLimit INT,
            displaySizeSortOrder INT,

            breakoutValue STRING,
            isOtherBucket BOOLEAN,
            displayRank BIGINT,

            barDirection STRING,
            barDirectionSortOrder INT,
            barRankWithinDirection BIGINT,
            barSortOrder BIGINT,

            sliceComparisonValue DOUBLE,
            sliceComparisonValueDisplay STRING,
            sliceCurrentValue DOUBLE,
            sliceCurrentValueDisplay STRING,
            sliceChangeValue DOUBLE,
            sliceChangeDisplay STRING,

            waterfallDeltaValue DOUBLE,
            waterfallDeltaDisplay STRING,
            waterfallDeltaUnit STRING,

            impactOnToplineValue DOUBLE,
            impactOnToplineDisplay STRING,
            impactOnToplineUnit STRING,

            barStartValue DOUBLE,
            barEndValue DOUBLE,

            waterfallStartValue DOUBLE,
            waterfallStartDisplay STRING,

            increaseTotalValue DOUBLE,
            increaseTotalDisplay STRING,
            increaseSliceCount BIGINT,

            decreaseTotalValue DOUBLE,
            decreaseTotalDisplay STRING,
            decreaseSliceCount BIGINT,

            netChangeValue DOUBLE,
            netChangeDisplay STRING,

            toplineChangeValue DOUBLE,
            toplineChangeDisplay STRING,

            waterfallEndValue DOUBLE,
            waterfallEndDisplay STRING,

            displayedWaterfallDeltaSum DOUBLE,
            waterfallReconciliationResidual DOUBLE,

            appProcessedAt TIMESTAMP
        )
        USING DELTA
        CLUSTER BY(targetWeekStartDate,metricName,breakoutType)
        COMMENT 'MIP Gold App: Breakouts waterfall. Top5/Top10/All+Other with precomputed summary values, bar ordering and cumulative chart geometry.';

        -- =====================================================================
        -- 6. Rebuild requested weeks
        -- =====================================================================
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsWaterfall_long
        REPLACE WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo

        WITH base AS(
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
            WHERE g.targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        ),

        toplineBase AS(
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
            WHERE g.targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        ),

        comparisonLong AS(
            SELECT
                b.*,
                'priorWeek' AS comparisonType,
                'Prior week' AS comparisonLabel,
                10 AS comparisonSortOrder,
                b.priorWeekDataAvailable AS comparisonDataAvailable,
                b.priorWeekDataAvailable AS comparisonWindowComplete,
                b.priorWeekNumerator AS comparisonNumerator,
                b.priorWeekDenominator AS comparisonDenominator
            FROM base b

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
            FROM base b

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
            FROM base b
        ),

        sliceValues AS(
            SELECT
                *,
                CASE
                    WHEN NOT thisWeekDataAvailable THEN NULL
                    WHEN metricKind='ratio' THEN try_divide(thisWeekNumerator,thisWeekDenominator)
                    ELSE thisWeekNumerator
                END AS currentValue,

                CASE
                    WHEN NOT comparisonDataAvailable THEN NULL
                    WHEN metricKind='ratio' THEN try_divide(comparisonNumerator,comparisonDenominator)
                    ELSE comparisonNumerator
                END AS comparisonValue
            FROM comparisonLong
        ),

        toplineLong AS(
            SELECT
                t.*,
                'priorWeek' AS comparisonType,
                t.priorWeekDataAvailable AS comparisonDataAvailable,
                t.priorWeekDataAvailable AS comparisonWindowComplete,
                t.priorWeekNumerator AS comparisonNumerator,
                t.priorWeekDenominator AS comparisonDenominator
            FROM toplineBase t

            UNION ALL

            SELECT
                t.*,
                'fourWeek',
                t.fourWeekTrendWeekCount>0,
                t.fourWeekTrendWeekCount=4,
                CASE
                    WHEN t.metricKind='count' AND t.fourWeekTrendWeekCount>0
                        THEN try_divide(t.fourWeekTrendNumerator,cast(t.fourWeekTrendWeekCount AS DOUBLE))
                    ELSE t.fourWeekTrendNumerator
                END,
                CASE
                    WHEN t.metricKind='count' THEN NULL
                    ELSE t.fourWeekTrendDenominator
                END
            FROM toplineBase t

            UNION ALL

            SELECT
                t.*,
                'lastYear',
                t.sameWeekLyDataAvailable,
                t.sameWeekLyDataAvailable,
                t.sameWeekLyNumerator,
                t.sameWeekLyDenominator
            FROM toplineBase t
        ),

        toplineValues AS(
            SELECT
                *,
                CASE
                    WHEN NOT thisWeekDataAvailable THEN NULL
                    WHEN metricKind='ratio' THEN try_divide(thisWeekNumerator,thisWeekDenominator)
                    ELSE thisWeekNumerator
                END AS toplineCurrentValue,

                CASE
                    WHEN NOT comparisonDataAvailable THEN NULL
                    WHEN metricKind='ratio' THEN try_divide(comparisonNumerator,comparisonDenominator)
                    ELSE comparisonNumerator
                END AS toplineComparisonValue
            FROM toplineLong
        ),

        calculatedRaw AS(
            SELECT
                s.*,

                t.toplineCurrentValue,
                t.toplineComparisonValue,
                t.thisWeekDenominator AS toplineCurrentDenominator,
                t.comparisonDenominator AS toplineComparisonDenominator,

                s.currentValue-s.comparisonValue AS sliceAbsoluteDiffValue,

                CASE
                    WHEN s.currentValue IS NULL OR s.comparisonValue IS NULL THEN NULL
                    WHEN s.changeUnit='pp'
                        THEN 100D*(s.currentValue-s.comparisonValue)
                    WHEN s.changeUnit='pct'
                        THEN 100D*(try_divide(s.currentValue,s.comparisonValue)-1D)
                END AS sliceChangeRaw,

                CASE
                    WHEN s.currentValue IS NULL OR s.comparisonValue IS NULL THEN NULL
                    WHEN s.metricKind='count'
                        THEN s.currentValue-s.comparisonValue
                    WHEN s.metricKind='ratio'
                        THEN 100D*(
                            try_divide(s.thisWeekNumerator,t.thisWeekDenominator)
                            -try_divide(s.comparisonNumerator,t.comparisonDenominator)
                        )
                END AS waterfallDeltaRaw,

                CASE
                    WHEN s.metricKind='count' THEN 'number'
                    WHEN s.metricKind='ratio' THEN 'pp'
                END AS waterfallDeltaUnit,

                CASE
                    WHEN s.currentValue IS NULL OR s.comparisonValue IS NULL THEN NULL
                    WHEN s.metricKind='count'
                        THEN 100D*try_divide(
                            s.currentValue-s.comparisonValue,
                            t.toplineComparisonValue
                        )
                    WHEN s.metricKind='ratio'
                        THEN 100D*(
                            try_divide(s.thisWeekNumerator,t.thisWeekDenominator)
                            -try_divide(s.comparisonNumerator,t.comparisonDenominator)
                        )
                END AS impactOnToplineRaw,

                CASE
                    WHEN s.metricKind='count' THEN 'pct'
                    WHEN s.metricKind='ratio' THEN 'pp'
                END AS impactOnToplineUnit
            FROM sliceValues s
            JOIN toplineValues t
              ON t.targetWeekStartDate=s.targetWeekStartDate
             AND t.filterLob=s.filterLob
             AND t.filterPlatform=s.filterPlatform
             AND t.metricName=s.metricName
             AND t.comparisonType=s.comparisonType
        ),

        ranked AS(
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
                        abs(sliceAbsoluteDiffValue) DESC NULLS LAST,
                        breakoutValue
                ) AS impactRankWithinBreakout
            FROM calculatedRaw
            WHERE comparisonDataAvailable
              AND impactOnToplineRaw IS NOT NULL
        ),

        sizeConfig AS(
            SELECT * FROM VALUES
                ('top5','Top 5',5,10),
                ('top10','Top 10',10,20),
                ('all','All',100,30)
            AS s(displaySize,displaySizeLabel,displayLimit,displaySizeSortOrder)
        ),

        expanded AS(
            SELECT
                r.*,
                s.displaySize,
                s.displaySizeLabel,
                s.displayLimit,
                s.displaySizeSortOrder,

                CASE
                    WHEN r.impactRankWithinBreakout<=s.displayLimit
                        THEN concat('VALUE::',coalesce(r.breakoutValue,'(null)'))
                    ELSE 'OTHER::REMAINDER'
                END AS displayBucketKey,

                CASE
                    WHEN r.impactRankWithinBreakout<=s.displayLimit
                        THEN r.breakoutValue
                    ELSE '(Other)'
                END AS displayBreakoutValue,

                r.impactRankWithinBreakout>s.displayLimit AS isSyntheticOtherMember
            FROM ranked r
            CROSS JOIN sizeConfig s
        ),

        bucketAgg AS(
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

                min(CASE WHEN comparisonDataAvailable THEN 1 ELSE 0 END)=1
                    AS comparisonDataAvailable,

                min(CASE WHEN comparisonWindowComplete THEN 1 ELSE 0 END)=1
                    AS comparisonWindowComplete,

                displaySize,
                displaySizeLabel,
                displayLimit,
                displaySizeSortOrder,

                displayBucketKey,
                displayBreakoutValue AS breakoutValue,

                max(CASE WHEN isSyntheticOtherMember THEN 1 ELSE 0 END)=1
                    AS isOtherBucket,

                CASE
                    WHEN max(CASE WHEN isSyntheticOtherMember THEN 1 ELSE 0 END)=1
                        THEN cast(displayLimit+1 AS BIGINT)
                    ELSE min(impactRankWithinBreakout)
                END AS displayRank,

                sum(thisWeekNumerator) AS currentNumerator,
                sum(thisWeekDenominator) AS currentDenominator,

                sum(comparisonNumerator) AS comparisonNumerator,
                sum(comparisonDenominator) AS comparisonDenominator,

                max(toplineCurrentValue) AS toplineCurrentValue,
                max(toplineComparisonValue) AS toplineComparisonValue,
                max(toplineCurrentDenominator) AS toplineCurrentDenominator,
                max(toplineComparisonDenominator) AS toplineComparisonDenominator
            FROM expanded
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
                displayBucketKey,
                displayBreakoutValue
        ),

        bucketValues AS(
            SELECT
                *,
                CASE
                    WHEN metricKind='ratio'
                        THEN try_divide(currentNumerator,currentDenominator)
                    ELSE currentNumerator
                END AS sliceCurrentValue,

                CASE
                    WHEN metricKind='ratio'
                        THEN try_divide(comparisonNumerator,comparisonDenominator)
                    ELSE comparisonNumerator
                END AS sliceComparisonValue,

                CASE
                    WHEN metricKind='count'
                        THEN toplineComparisonValue
                    WHEN metricKind='ratio'
                        THEN 100D*toplineComparisonValue
                END AS waterfallStartValue,

                CASE
                    WHEN metricKind='count'
                        THEN toplineCurrentValue
                    WHEN metricKind='ratio'
                        THEN 100D*toplineCurrentValue
                END AS waterfallEndValue
            FROM bucketAgg
        ),

        bucketCalculated AS(
            SELECT
                *,

                CASE
                    WHEN sliceCurrentValue IS NULL OR sliceComparisonValue IS NULL THEN NULL
                    WHEN changeUnit='pp'
                        THEN 100D*(sliceCurrentValue-sliceComparisonValue)
                    WHEN changeUnit='pct'
                        THEN 100D*(try_divide(sliceCurrentValue,sliceComparisonValue)-1D)
                END AS sliceChangeRaw,

                CASE
                    WHEN metricKind='count'
                        THEN sliceCurrentValue-sliceComparisonValue
                    WHEN metricKind='ratio'
                        THEN 100D*(
                            try_divide(currentNumerator,toplineCurrentDenominator)
                            -try_divide(comparisonNumerator,toplineComparisonDenominator)
                        )
                END AS waterfallDeltaValue,

                CASE
                    WHEN metricKind='count' THEN 'number'
                    WHEN metricKind='ratio' THEN 'pp'
                END AS waterfallDeltaUnit,

                CASE
                    WHEN metricKind='count'
                        THEN 100D*try_divide(
                            sliceCurrentValue-sliceComparisonValue,
                            toplineComparisonValue
                        )
                    WHEN metricKind='ratio'
                        THEN 100D*(
                            try_divide(currentNumerator,toplineCurrentDenominator)
                            -try_divide(comparisonNumerator,toplineComparisonDenominator)
                        )
                END AS impactOnToplineRaw,

                CASE
                    WHEN metricKind='count' THEN 'pct'
                    WHEN metricKind='ratio' THEN 'pp'
                END AS impactOnToplineUnit,

                waterfallEndValue-waterfallStartValue AS netChangeValue,

                CASE
                    WHEN changeUnit='pp'
                        THEN 100D*(toplineCurrentValue-toplineComparisonValue)
                    WHEN changeUnit='pct'
                        THEN 100D*(try_divide(toplineCurrentValue,toplineComparisonValue)-1D)
                END AS toplineChangeRaw
            FROM bucketValues
        ),

        directional AS(
            SELECT
                *,
                CASE
                    WHEN waterfallDeltaValue>0D THEN 'increase'
                    WHEN waterfallDeltaValue<0D THEN 'decrease'
                    ELSE 'flat'
                END AS barDirection,

                CASE
                    WHEN waterfallDeltaValue>0D THEN 10
                    WHEN waterfallDeltaValue<0D THEN 20
                    ELSE 30
                END AS barDirectionSortOrder
            FROM bucketCalculated
        ),

        summaryValues AS(
            SELECT
                *,
                sum(CASE WHEN waterfallDeltaValue>0D THEN waterfallDeltaValue ELSE 0D END)
                    OVER(
                        PARTITION BY
                            targetWeekStartDate,
                            filterLob,
                            filterPlatform,
                            metricName,
                            breakoutType,
                            comparisonType,
                            displaySize
                    ) AS increaseTotalValue,

                sum(CASE WHEN waterfallDeltaValue>0D THEN 1 ELSE 0 END)
                    OVER(
                        PARTITION BY
                            targetWeekStartDate,
                            filterLob,
                            filterPlatform,
                            metricName,
                            breakoutType,
                            comparisonType,
                            displaySize
                    ) AS increaseSliceCount,

                sum(CASE WHEN waterfallDeltaValue<0D THEN waterfallDeltaValue ELSE 0D END)
                    OVER(
                        PARTITION BY
                            targetWeekStartDate,
                            filterLob,
                            filterPlatform,
                            metricName,
                            breakoutType,
                            comparisonType,
                            displaySize
                    ) AS decreaseTotalValue,

                sum(CASE WHEN waterfallDeltaValue<0D THEN 1 ELSE 0 END)
                    OVER(
                        PARTITION BY
                            targetWeekStartDate,
                            filterLob,
                            filterPlatform,
                            metricName,
                            breakoutType,
                            comparisonType,
                            displaySize
                    ) AS decreaseSliceCount,

                sum(waterfallDeltaValue)
                    OVER(
                        PARTITION BY
                            targetWeekStartDate,
                            filterLob,
                            filterPlatform,
                            metricName,
                            breakoutType,
                            comparisonType,
                            displaySize
                    ) AS displayedWaterfallDeltaSum
            FROM directional
        ),

        directionRanked AS(
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
                        barDirection
                    ORDER BY
                        abs(waterfallDeltaValue) DESC NULLS LAST,
                        displayRank,
                        breakoutValue
                ) AS barRankWithinDirection
            FROM summaryValues
        ),

        ordered AS(
            SELECT
                *,
                cast(
                    barDirectionSortOrder*1000
                    +barRankWithinDirection
                    AS BIGINT
                ) AS barSortOrder
            FROM directionRanked
        ),

        geometry AS(
            SELECT
                *,

                waterfallStartValue
                +coalesce(
                    sum(waterfallDeltaValue) OVER(
                        PARTITION BY
                            targetWeekStartDate,
                            filterLob,
                            filterPlatform,
                            metricName,
                            breakoutType,
                            comparisonType,
                            displaySize
                        ORDER BY barSortOrder
                        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                    ),
                    0D
                ) AS barStartValue,

                waterfallStartValue
                +sum(waterfallDeltaValue) OVER(
                    PARTITION BY
                        targetWeekStartDate,
                        filterLob,
                        filterPlatform,
                        metricName,
                        breakoutType,
                        comparisonType,
                        displaySize
                    ORDER BY barSortOrder
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS barEndValue
            FROM ordered
        ),

        rounded AS(
            SELECT
                *,

                CASE
                    WHEN sliceChangeRaw IS NULL THEN NULL
                    WHEN abs(sliceChangeRaw)<0.05D THEN 0D
                    ELSE round(sliceChangeRaw,1)
                END AS sliceChangeValue,

                CASE
                    WHEN impactOnToplineRaw IS NULL THEN NULL
                    WHEN abs(impactOnToplineRaw)<0.05D THEN 0D
                    ELSE round(impactOnToplineRaw,1)
                END AS impactOnToplineValue,

                CASE
                    WHEN toplineChangeRaw IS NULL THEN NULL
                    WHEN abs(toplineChangeRaw)<0.05D THEN 0D
                    ELSE round(toplineChangeRaw,1)
                END AS toplineChangeValue,

                netChangeValue-displayedWaterfallDeltaSum
                    AS waterfallReconciliationResidual
            FROM geometry
        ),

        formatted AS(
            SELECT
                *,

                -- Slice comparison display
                CASE
                    WHEN sliceComparisonValue IS NULL THEN NULL
                    WHEN displayFormat='percent'
                        THEN concat(format_number(100D*sliceComparisonValue,1),'%')
                    WHEN abs(sliceComparisonValue)>=1000000000D
                        THEN concat(regexp_replace(format_number(sliceComparisonValue/1000000000D,1),'\\.0$',''),'B')
                    WHEN abs(sliceComparisonValue)>=1000000D
                        THEN concat(regexp_replace(format_number(sliceComparisonValue/1000000D,1),'\\.0$',''),'M')
                    WHEN abs(sliceComparisonValue)>=1000D
                        THEN concat(regexp_replace(format_number(sliceComparisonValue/1000D,1),'\\.0$',''),'K')
                    ELSE format_number(sliceComparisonValue,0)
                END AS sliceComparisonValueDisplay,

                -- This week display
                CASE
                    WHEN sliceCurrentValue IS NULL THEN NULL
                    WHEN displayFormat='percent'
                        THEN concat(format_number(100D*sliceCurrentValue,1),'%')
                    WHEN abs(sliceCurrentValue)>=1000000000D
                        THEN concat(regexp_replace(format_number(sliceCurrentValue/1000000000D,1),'\\.0$',''),'B')
                    WHEN abs(sliceCurrentValue)>=1000000D
                        THEN concat(regexp_replace(format_number(sliceCurrentValue/1000000D,1),'\\.0$',''),'M')
                    WHEN abs(sliceCurrentValue)>=1000D
                        THEN concat(regexp_replace(format_number(sliceCurrentValue/1000D,1),'\\.0$',''),'K')
                    ELSE format_number(sliceCurrentValue,0)
                END AS sliceCurrentValueDisplay,

                -- Per-slice comparison change
                CASE
                    WHEN sliceChangeValue IS NULL THEN NULL
                    WHEN changeUnit='pp'
                        THEN concat(
                            CASE WHEN sliceChangeValue>0D THEN '+' ELSE '' END,
                            format_number(sliceChangeValue,1),
                            'pp'
                        )
                    ELSE concat(
                        CASE WHEN sliceChangeValue>0D THEN '+' ELSE '' END,
                        format_number(sliceChangeValue,1),
                        '%'
                    )
                END AS sliceChangeDisplay,

                -- Waterfall bar label
                CASE
                    WHEN waterfallDeltaValue IS NULL THEN NULL
                    WHEN waterfallDeltaUnit='pp'
                        THEN concat(
                            CASE WHEN waterfallDeltaValue>0D THEN '+' ELSE '' END,
                            format_number(
                                CASE
                                    WHEN abs(waterfallDeltaValue)<0.05D THEN 0D
                                    ELSE waterfallDeltaValue
                                END,
                                1
                            ),
                            'pp'
                        )
                    WHEN abs(waterfallDeltaValue)>=1000000000D
                        THEN concat(
                            CASE WHEN waterfallDeltaValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(waterfallDeltaValue/1000000000D,1),'\\.0$',''),
                            'B'
                        )
                    WHEN abs(waterfallDeltaValue)>=1000000D
                        THEN concat(
                            CASE WHEN waterfallDeltaValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(waterfallDeltaValue/1000000D,1),'\\.0$',''),
                            'M'
                        )
                    WHEN abs(waterfallDeltaValue)>=1000D
                        THEN concat(
                            CASE WHEN waterfallDeltaValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(waterfallDeltaValue/1000D,1),'\\.0$',''),
                            'K'
                        )
                    ELSE concat(
                        CASE WHEN waterfallDeltaValue>0D THEN '+' ELSE '' END,
                        format_number(waterfallDeltaValue,0)
                    )
                END AS waterfallDeltaDisplay,

                -- Impact display
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

                -- Baseline / selected comparator
                CASE
                    WHEN waterfallStartValue IS NULL THEN NULL
                    WHEN metricKind='ratio'
                        THEN concat(format_number(waterfallStartValue,1),'%')
                    WHEN abs(waterfallStartValue)>=1000000000D
                        THEN concat(regexp_replace(format_number(waterfallStartValue/1000000000D,1),'\\.0$',''),'B')
                    WHEN abs(waterfallStartValue)>=1000000D
                        THEN concat(regexp_replace(format_number(waterfallStartValue/1000000D,1),'\\.0$',''),'M')
                    WHEN abs(waterfallStartValue)>=1000D
                        THEN concat(regexp_replace(format_number(waterfallStartValue/1000D,1),'\\.0$',''),'K')
                    ELSE format_number(waterfallStartValue,0)
                END AS waterfallStartDisplay,

                -- Total positive contribution
                CASE
                    WHEN waterfallDeltaUnit='pp'
                        THEN concat('+',format_number(increaseTotalValue,1),'pp')
                    WHEN abs(increaseTotalValue)>=1000000000D
                        THEN concat('+',regexp_replace(format_number(increaseTotalValue/1000000000D,1),'\\.0$',''),'B')
                    WHEN abs(increaseTotalValue)>=1000000D
                        THEN concat('+',regexp_replace(format_number(increaseTotalValue/1000000D,1),'\\.0$',''),'M')
                    WHEN abs(increaseTotalValue)>=1000D
                        THEN concat('+',regexp_replace(format_number(increaseTotalValue/1000D,1),'\\.0$',''),'K')
                    ELSE concat('+',format_number(increaseTotalValue,0))
                END AS increaseTotalDisplay,

                -- Total negative contribution
                CASE
                    WHEN waterfallDeltaUnit='pp'
                        THEN concat(format_number(decreaseTotalValue,1),'pp')
                    WHEN abs(decreaseTotalValue)>=1000000000D
                        THEN concat(regexp_replace(format_number(decreaseTotalValue/1000000000D,1),'\\.0$',''),'B')
                    WHEN abs(decreaseTotalValue)>=1000000D
                        THEN concat(regexp_replace(format_number(decreaseTotalValue/1000000D,1),'\\.0$',''),'M')
                    WHEN abs(decreaseTotalValue)>=1000D
                        THEN concat(regexp_replace(format_number(decreaseTotalValue/1000D,1),'\\.0$',''),'K')
                    ELSE format_number(decreaseTotalValue,0)
                END AS decreaseTotalDisplay,

                -- Net change
                CASE
                    WHEN waterfallDeltaUnit='pp'
                        THEN concat(
                            CASE WHEN netChangeValue>0D THEN '+' ELSE '' END,
                            format_number(netChangeValue,1),
                            'pp'
                        )
                    WHEN abs(netChangeValue)>=1000000000D
                        THEN concat(
                            CASE WHEN netChangeValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(netChangeValue/1000000000D,1),'\\.0$',''),
                            'B'
                        )
                    WHEN abs(netChangeValue)>=1000000D
                        THEN concat(
                            CASE WHEN netChangeValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(netChangeValue/1000000D,1),'\\.0$',''),
                            'M'
                        )
                    WHEN abs(netChangeValue)>=1000D
                        THEN concat(
                            CASE WHEN netChangeValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(netChangeValue/1000D,1),'\\.0$',''),
                            'K'
                        )
                    ELSE concat(
                        CASE WHEN netChangeValue>0D THEN '+' ELSE '' END,
                        format_number(netChangeValue,0)
                    )
                END AS netChangeDisplay,

                -- Topline % / pp change shown below Net change
                CASE
                    WHEN toplineChangeValue IS NULL THEN NULL
                    WHEN changeUnit='pp'
                        THEN concat(
                            CASE WHEN toplineChangeValue>0D THEN '+' ELSE '' END,
                            format_number(toplineChangeValue,1),
                            'pp'
                        )
                    ELSE concat(
                        CASE WHEN toplineChangeValue>0D THEN '+' ELSE '' END,
                        format_number(toplineChangeValue,1),
                        '%'
                    )
                END AS toplineChangeDisplay,

                -- This week topline
                CASE
                    WHEN waterfallEndValue IS NULL THEN NULL
                    WHEN metricKind='ratio'
                        THEN concat(format_number(waterfallEndValue,1),'%')
                    WHEN abs(waterfallEndValue)>=1000000000D
                        THEN concat(regexp_replace(format_number(waterfallEndValue/1000000000D,1),'\\.0$',''),'B')
                    WHEN abs(waterfallEndValue)>=1000000D
                        THEN concat(regexp_replace(format_number(waterfallEndValue/1000000D,1),'\\.0$',''),'M')
                    WHEN abs(waterfallEndValue)>=1000D
                        THEN concat(regexp_replace(format_number(waterfallEndValue/1000D,1),'\\.0$',''),'K')
                    ELSE format_number(waterfallEndValue,0)
                END AS waterfallEndDisplay
            FROM rounded
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
            comparisonDataAvailable,
            comparisonWindowComplete,

            displaySize,
            displaySizeLabel,
            displayLimit,
            displaySizeSortOrder,

            breakoutValue,
            isOtherBucket,
            displayRank,

            barDirection,
            barDirectionSortOrder,
            barRankWithinDirection,
            barSortOrder,

            sliceComparisonValue,
            sliceComparisonValueDisplay,
            sliceCurrentValue,
            sliceCurrentValueDisplay,
            sliceChangeValue,
            sliceChangeDisplay,

            waterfallDeltaValue,
            waterfallDeltaDisplay,
            waterfallDeltaUnit,

            impactOnToplineValue,
            impactOnToplineDisplay,
            impactOnToplineUnit,

            barStartValue,
            barEndValue,

            waterfallStartValue,
            waterfallStartDisplay,

            increaseTotalValue,
            increaseTotalDisplay,
            increaseSliceCount,

            decreaseTotalValue,
            decreaseTotalDisplay,
            decreaseSliceCount,

            netChangeValue,
            netChangeDisplay,

            toplineChangeValue,
            toplineChangeDisplay,

            waterfallEndValue,
            waterfallEndDisplay,

            displayedWaterfallDeltaSum,
            waterfallReconciliationResidual,

            v_processedAt AS appProcessedAt
        FROM formatted;

        -- =====================================================================
        -- 7. Success
        -- =====================================================================
        SELECT
            'SUCCESS' AS status,
            v_weekFrom AS rebuiltWeekStartFrom,
            v_weekTo AS rebuiltWeekStartTo,
            v_weekEndTo AS latestWeekEndDate,
            CASE WHEN v_asOfDate<v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,
            'priorWeek | fourWeek | lastYear' AS supportedComparisons,
            'top5 | top10 | all' AS supportedDisplaySizes,
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsWaterfall_long' AS targetObject,
            v_processedAt AS appProcessedAt;
    END IF;
END;

-- ============================================================================
-- DEVELOPMENT
-- ============================================================================

-- ONE TIME ONLY before first run of this redesigned schema:
-- DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsWaterfall_long;

-- Validation:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appBreakoutsWaterfall_long(
--     p_asOfDate=>DATE '2026-09-28',
--     p_weeksToRebuild=>1,
--     p_validateOnly=>TRUE
-- );

-- Build/rebuild:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appBreakoutsWaterfall_long(
--     p_asOfDate=>DATE '2026-09-28',
--     p_weeksToRebuild=>12,
--     p_validateOnly=>FALSE
-- );

-- ============================================================================
-- API EXAMPLE
--
-- Screenshot equivalent:
--   Q3 2026
--   W7
--   Total UPV
--   Channel
--   4-wk
--   All
-- ============================================================================

-- SELECT *
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsWaterfall_long
-- WHERE fiscalYear=2026
--   AND targetWeekStartDate=DATE '2026-08-09'
--   AND filterLob='All'
--   AND filterPlatform='All'
--   AND metricName='nbv'
--   AND breakoutType='channel'
--   AND comparisonType='fourWeek'
--   AND displaySize='all'
-- ORDER BY barSortOrder;

-- ============================================================================
-- API HEADER SUMMARY
--
-- Every bar row repeats these fields, so API can take one row rather than
-- calculate anything.
-- ============================================================================

-- SELECT DISTINCT
--     comparisonLabel,
--     waterfallStartValue,
--     waterfallStartDisplay,
--     increaseTotalValue,
--     increaseTotalDisplay,
--     increaseSliceCount,
--     decreaseTotalValue,
--     decreaseTotalDisplay,
--     decreaseSliceCount,
--     netChangeValue,
--     netChangeDisplay,
--     toplineChangeValue,
--     toplineChangeDisplay,
--     waterfallEndValue,
--     waterfallEndDisplay
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsWaterfall_long
-- WHERE targetWeekStartDate=DATE '2026-08-09'
--   AND filterLob='All'
--   AND filterPlatform='All'
--   AND metricName='nbv'
--   AND breakoutType='channel'
--   AND comparisonType='fourWeek'
--   AND displaySize='all';

-- Screenshot-style result:
--
-- 4-WK TREND      1.7M
-- INCREASES       +45K     2 slices
-- DECREASES       -170K    6 slices
-- NET CHANGE      -125K    -7.5%
-- THIS WEEK       1.5M

-- ============================================================================
-- BAR DATA
-- ============================================================================

-- SELECT
--     breakoutValue,
--     isOtherBucket,
--     barDirection,
--     barSortOrder,
--     waterfallDeltaValue,
--     waterfallDeltaDisplay,
--     barStartValue,
--     barEndValue
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsWaterfall_long
-- WHERE targetWeekStartDate=DATE '2026-08-09'
--   AND filterLob='All'
--   AND filterPlatform='All'
--   AND metricName='nbv'
--   AND breakoutType='channel'
--   AND comparisonType='fourWeek'
--   AND displaySize='all'
-- ORDER BY barSortOrder;

-- Expected ordering pattern:
-- Paid Search   increase
-- Social        increase
-- SMS           decrease
-- Direct        decrease
-- Organic Search decrease
-- Email         decrease
-- Programmatic  decrease
-- (Other)       decrease

-- ============================================================================
-- RECONCILIATION
-- Expected residual approximately 0.
-- ============================================================================

-- SELECT DISTINCT
--     targetWeekStartDate,
--     metricName,
--     breakoutType,
--     comparisonType,
--     displaySize,
--     waterfallStartValue,
--     waterfallEndValue,
--     netChangeValue,
--     displayedWaterfallDeltaSum,
--     waterfallReconciliationResidual
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsWaterfall_long
-- WHERE targetWeekStartDate BETWEEN DATE '2026-08-01' AND DATE '2026-09-30'
-- ORDER BY targetWeekStartDate,metricName,breakoutType,comparisonType,displaySize;

-- ============================================================================
-- ELIGIBILITY CHECKS
-- Both expected zero rows.
-- ============================================================================

-- SELECT DISTINCT metricName,metricLabel
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsWaterfall_long a
-- WHERE NOT EXISTS(
--     SELECT 1
--     FROM prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
--     WHERE mc.metricName=a.metricName
--       AND mc.isActive
--       AND mc.showOnBreakouts
-- );

-- SELECT DISTINCT breakoutType,breakoutLabel
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsWaterfall_long a
-- WHERE NOT EXISTS(
--     SELECT 1
--     FROM prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static bc
--     WHERE bc.breakoutType=a.breakoutType
--       AND bc.isActive
--       AND bc.isPrebuiltBreakout
-- );

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
--     breakoutValue,
--     count(*) AS rowCount
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsWaterfall_long
-- GROUP BY
--     targetWeekStartDate,
--     filterLob,
--     filterPlatform,
--     metricName,
--     breakoutType,
--     comparisonType,
--     displaySize,
--     breakoutValue
-- HAVING count(*)>1;