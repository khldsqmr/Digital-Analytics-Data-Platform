-- ============================================================================
-- FILE  : 03_sdi_sp_mip_gold_appOverviewToplineMovers_long.sql
-- LAYER : GOLD / APP
-- TAB   : Overview
-- SECTION: What moved the topline
--
-- UI:
--   Quarter  -> fiscalQuarterLabel
--   Week     -> targetWeekStartDate / fiscalWeekCode / weekEndingLabel
--   Metric   -> metricName
--   Compare  -> priorWeek | fourWeek | lastYear
--   Show     -> Top 5 | Top 10 | All (Top 100)
--
-- Each returned row simultaneously contains:
--   This week
--   vs Prior week
--   vs 4-wk trend
--   vs Same wk LY
--   vs Peer set
--   Impact on topline for the selected comparisonType
--
-- Slices are NOT hardcoded. Active prebuilt breakout types are read from:
--   sdi_vw_mip_control_breakoutCatalog_static
--
-- IMPORTANT:
--   Forecast is not fabricated here. Current forecast Gold is topline/metric
--   grain and does not provide breakoutType + breakoutValue forecast values.
-- ============================================================================

-- ONE-TIME MIGRATION ONLY:
-- DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewToplineMovers_long;

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewToplineMovers_long(
    IN p_asOfDate DATE DEFAULT NULL,
    IN p_weeksToRebuild INT DEFAULT 1,
    IN p_validateOnly BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'MIP Gold App: What moved the topline. Global comparator-aware ranking across every active prebuilt breakout.'
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
    -- 2. Preflight
    -- =========================================================================
    IF NOT EXISTS(
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
        WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='Breakout Gold analytical ingredients has no rows for the requested target-week range.';
    END IF;

    IF NOT EXISTS(
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
        WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='Overview Gold analytical ingredients has no rows for the requested target-week range.';
    END IF;

    IF NOT EXISTS(
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static
        WHERE isActive
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='Metric Catalog has no active metrics.';
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
            SET MESSAGE_TEXT='Fiscal Calendar has no rows for the requested target-week range.';
    END IF;

    IF EXISTS(
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
        WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        GROUP BY targetWeekStartDate,filterLob,filterPlatform,breakoutType,breakoutValue,metricName
        HAVING count(*)>1
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='Duplicate Breakout Gold analytical keys detected for the requested target-week range.';
    END IF;

    IF EXISTS(
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static
        WHERE isActive
          AND (
              metricKind NOT IN('count','ratio')
              OR displayFormat NOT IN('number','percent')
              OR changeUnit NOT IN('pct','pp')
          )
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='Metric Catalog contains unsupported metricKind, displayFormat or changeUnit values.';
    END IF;

    -- =========================================================================
    -- 3. Validation only
    -- =========================================================================
    IF p_validateOnly THEN
        SELECT
            'VALIDATION_ONLY' AS status,
            v_weekFrom AS rebuildWeekStartFrom,
            v_weekTo AS rebuildWeekStartTo,
            v_weekEndTo AS latestWeekEndDate,
            CASE WHEN v_asOfDate<v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,
            'priorWeek | fourWeek | lastYear' AS supportedMoverComparisons,
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewToplineMovers_long' AS targetObject,
            'Validation passed. No Gold App table was created or modified.' AS message;
    ELSE

        -- =====================================================================
        -- 4. App contract
        -- =====================================================================
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewToplineMovers_long(
            targetWeekStartDate DATE,
            targetWeekEndDate DATE,
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

            comparisonType STRING,
            comparisonLabel STRING,
            comparisonSortOrder INT,

            breakoutType STRING,
            breakoutLabel STRING,
            breakoutValue STRING,
            breakoutSortOrder INT,
            isOtherBucket BOOLEAN,

            currentValue DOUBLE,
            currentValueDisplay STRING,

            priorWeekAbsoluteDiffValue DOUBLE,
            priorWeekAbsoluteDiffDisplay STRING,
            priorWeekChangeValue DOUBLE,
            priorWeekChangeDisplay STRING,

            fourWeekAbsoluteDiffValue DOUBLE,
            fourWeekAbsoluteDiffDisplay STRING,
            fourWeekChangeValue DOUBLE,
            fourWeekChangeDisplay STRING,

            lastYearAbsoluteDiffValue DOUBLE,
            lastYearAbsoluteDiffDisplay STRING,
            lastYearChangeValue DOUBLE,
            lastYearChangeDisplay STRING,

            peerSetValue DOUBLE,
            peerSetValueDisplay STRING,
            peerSetAbsoluteDiffValue DOUBLE,
            peerSetAbsoluteDiffDisplay STRING,
            peerSetChangeValue DOUBLE,
            peerSetChangeDisplay STRING,

            impactOnToplineValue DOUBLE,
            impactOnToplineDisplay STRING,
            impactOnToplineUnit STRING,

            impactRankAcrossBreakouts BIGINT,
            isTop5 BOOLEAN,
            isTop10 BOOLEAN,
            isTop100 BOOLEAN,

            candidateRowCount BIGINT,
            clearsOnePercent BOOLEAN,
            clearsOnePercentCount BIGINT,

            appProcessedAt TIMESTAMP
        )
        USING DELTA
        CLUSTER BY(targetWeekStartDate,metricName,comparisonType)
        COMMENT 'MIP Gold App: What moved the topline. Every slice of every active prebuilt breakout ranked globally by selected comparator impact.';

        -- =====================================================================
        -- 5. Rebuild requested reporting weeks
        -- =====================================================================
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewToplineMovers_long
        REPLACE WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        WITH scopeBreakouts AS(
            SELECT *
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
            WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        ),
        scopeOverview AS(
            SELECT *
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
            WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        ),
        base AS(
            SELECT
                g.targetWeekStartDate,
                g.targetWeekEndDate,
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
                g.breakoutValue,
                bc.sortOrder AS breakoutSortOrder,

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
                g.sameWeekLyDataAvailable
            FROM scopeBreakouts g
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static bc
              ON bc.breakoutType=g.breakoutType
             AND bc.isActive
             AND bc.isPrebuiltBreakout
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
              ON mc.metricName=g.metricName
             AND mc.isActive
            LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c
              ON c.weekStartDate=g.targetWeekStartDate
        ),
        toplineBase AS(
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
        toplineValues AS(
            SELECT
                *,
                CASE
                    WHEN metricKind='ratio' THEN try_divide(thisWeekNumerator,thisWeekDenominator)
                    ELSE thisWeekNumerator
                END AS toplineCurrentValue,
                CASE
                    WHEN metricKind='ratio' THEN try_divide(priorWeekNumerator,priorWeekDenominator)
                    ELSE priorWeekNumerator
                END AS toplinePriorWeekValue,
                CASE
                    WHEN metricKind='ratio' THEN try_divide(fourWeekTrendNumerator,fourWeekTrendDenominator)
                    WHEN fourWeekTrendWeekCount>0 THEN try_divide(fourWeekTrendNumerator,cast(fourWeekTrendWeekCount AS DOUBLE))
                    ELSE NULL
                END AS toplineFourWeekValue,
                CASE
                    WHEN metricKind='ratio' THEN try_divide(sameWeekLyNumerator,sameWeekLyDenominator)
                    ELSE sameWeekLyNumerator
                END AS toplineLastYearValue
            FROM toplineBase
        ),
        rawValues AS(
            SELECT
                b.*,
                CASE
                    WHEN NOT thisWeekDataAvailable THEN NULL
                    WHEN metricKind='ratio' THEN try_divide(thisWeekNumerator,thisWeekDenominator)
                    ELSE thisWeekNumerator
                END AS currentValue,
                CASE
                    WHEN NOT priorWeekDataAvailable THEN NULL
                    WHEN metricKind='ratio' THEN try_divide(priorWeekNumerator,priorWeekDenominator)
                    ELSE priorWeekNumerator
                END AS priorWeekValue,
                CASE
                    WHEN fourWeekTrendWeekCount<=0 THEN NULL
                    WHEN metricKind='ratio' THEN try_divide(fourWeekTrendNumerator,fourWeekTrendDenominator)
                    ELSE try_divide(fourWeekTrendNumerator,cast(fourWeekTrendWeekCount AS DOUBLE))
                END AS fourWeekValue,
                CASE
                    WHEN NOT sameWeekLyDataAvailable THEN NULL
                    WHEN metricKind='ratio' THEN try_divide(sameWeekLyNumerator,sameWeekLyDenominator)
                    ELSE sameWeekLyNumerator
                END AS lastYearValue,
                CASE
                    WHEN metricKind='ratio' THEN try_divide(peerSetNumerator,peerSetDenominator)
                    ELSE peerSetNumerator
                END AS peerSetValue
            FROM base b
        ),
        comparisonLong AS(
            SELECT
                r.*,
                'priorWeek' AS comparisonType,
                'Prior week' AS comparisonLabel,
                10 AS comparisonSortOrder,
                r.priorWeekDataAvailable AS comparisonDataAvailable,
                r.priorWeekValue AS selectedComparisonValue,
                r.priorWeekNumerator AS selectedComparisonNumerator,
                r.priorWeekDenominator AS selectedComparisonDenominator
            FROM rawValues r

            UNION ALL

            SELECT
                r.*,
                'fourWeek' AS comparisonType,
                '4-wk trend' AS comparisonLabel,
                20 AS comparisonSortOrder,
                r.fourWeekTrendWeekCount>0 AS comparisonDataAvailable,
                r.fourWeekValue AS selectedComparisonValue,
                r.fourWeekTrendNumerator AS selectedComparisonNumerator,
                r.fourWeekTrendDenominator AS selectedComparisonDenominator
            FROM rawValues r

            UNION ALL

            SELECT
                r.*,
                'lastYear' AS comparisonType,
                'Same wk LY' AS comparisonLabel,
                30 AS comparisonSortOrder,
                r.sameWeekLyDataAvailable AS comparisonDataAvailable,
                r.lastYearValue AS selectedComparisonValue,
                r.sameWeekLyNumerator AS selectedComparisonNumerator,
                r.sameWeekLyDenominator AS selectedComparisonDenominator
            FROM rawValues r
        ),
        rawWithTopline AS(
            SELECT
                c.*,
                t.toplineCurrentValue,
                t.thisWeekDenominator AS toplineCurrentDenominator,

                t.toplinePriorWeekValue,
                t.priorWeekDenominator AS toplinePriorWeekDenominator,

                t.toplineFourWeekValue,
                t.fourWeekTrendDenominator AS toplineFourWeekDenominator,

                t.toplineLastYearValue,
                t.sameWeekLyDenominator AS toplineLastYearDenominator,

                CASE c.comparisonType
                    WHEN 'priorWeek' THEN t.toplinePriorWeekValue
                    WHEN 'fourWeek' THEN t.toplineFourWeekValue
                    WHEN 'lastYear' THEN t.toplineLastYearValue
                END AS selectedToplineComparisonValue,

                CASE c.comparisonType
                    WHEN 'priorWeek' THEN t.priorWeekDenominator
                    WHEN 'fourWeek' THEN t.fourWeekTrendDenominator
                    WHEN 'lastYear' THEN t.sameWeekLyDenominator
                END AS selectedToplineComparisonDenominator
            FROM comparisonLong c
            JOIN toplineValues t
              ON t.targetWeekStartDate=c.targetWeekStartDate
             AND t.filterLob=c.filterLob
             AND t.filterPlatform=c.filterPlatform
             AND t.metricName=c.metricName
        ),
        rawImpact AS(
            SELECT
                *,
                currentValue-selectedComparisonValue AS selectedAbsoluteDiffValue,
                CASE
                    WHEN NOT comparisonDataAvailable
                      OR currentValue IS NULL
                      OR selectedComparisonValue IS NULL THEN NULL
                    WHEN changeUnit='pp' THEN 100D*(currentValue-selectedComparisonValue)
                    WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,selectedComparisonValue)-1D)
                END AS selectedChangeValue,
                CASE
                    WHEN NOT comparisonDataAvailable
                      OR currentValue IS NULL
                      OR selectedComparisonValue IS NULL THEN NULL
                    WHEN metricKind='count' THEN
                        100D*try_divide(currentValue-selectedComparisonValue,selectedToplineComparisonValue)
                    WHEN metricKind='ratio' THEN
                        100D*(
                            try_divide(thisWeekNumerator,toplineCurrentDenominator)
                            -try_divide(selectedComparisonNumerator,selectedToplineComparisonDenominator)
                        )
                END AS impactOnToplineRaw,
                CASE
                    WHEN metricKind='count' THEN 'pct'
                    WHEN metricKind='ratio' THEN 'pp'
                END AS impactOnToplineUnit
            FROM rawWithTopline
        ),
        eligible AS(
            SELECT *
            FROM rawImpact
            WHERE comparisonDataAvailable
              AND impactOnToplineRaw IS NOT NULL
        ),
        rankedWithinBreakout AS(
            SELECT
                *,
                row_number() OVER(
                    PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName,breakoutType,comparisonType
                    ORDER BY abs(impactOnToplineRaw) DESC NULLS LAST,
                             abs(selectedAbsoluteDiffValue) DESC NULLS LAST,
                             breakoutValue
                ) AS impactRankWithinBreakout
            FROM eligible
        ),
        bucketMembers AS(
            SELECT
                *,
                CASE
                    WHEN impactRankWithinBreakout<=100
                        THEN concat('VALUE::',coalesce(breakoutValue,'(null)'))
                    ELSE 'OTHER::REMAINDER'
                END AS displayBucketKey,
                CASE
                    WHEN impactRankWithinBreakout<=100 THEN breakoutValue
                    ELSE '(Other)'
                END AS displayBreakoutValue,
                impactRankWithinBreakout>100 AS isSyntheticOtherMember
            FROM rankedWithinBreakout
        ),
        bucketAgg AS(
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

                comparisonType,
                comparisonLabel,
                comparisonSortOrder,

                breakoutType,
                breakoutLabel,
                displayBreakoutValue AS breakoutValue,
                breakoutSortOrder,
                max(CASE WHEN isSyntheticOtherMember THEN 1 ELSE 0 END)=1 AS isOtherBucket,

                sum(thisWeekNumerator) AS currentNumerator,
                sum(thisWeekDenominator) AS currentDenominator,

                sum(priorWeekNumerator) AS priorWeekNumerator,
                sum(priorWeekDenominator) AS priorWeekDenominator,

                sum(fourWeekTrendNumerator) AS fourWeekTrendNumerator,
                sum(fourWeekTrendDenominator) AS fourWeekTrendDenominator,
                max(fourWeekTrendWeekCount) AS fourWeekTrendWeekCount,

                sum(sameWeekLyNumerator) AS sameWeekLyNumerator,
                sum(sameWeekLyDenominator) AS sameWeekLyDenominator,

                sum(peerSetNumerator) AS peerSetNumerator,
                sum(peerSetDenominator) AS peerSetDenominator,

                max(toplineCurrentValue) AS toplineCurrentValue,
                max(toplineCurrentDenominator) AS toplineCurrentDenominator,

                max(toplinePriorWeekValue) AS toplinePriorWeekValue,
                max(toplinePriorWeekDenominator) AS toplinePriorWeekDenominator,

                max(toplineFourWeekValue) AS toplineFourWeekValue,
                max(toplineFourWeekDenominator) AS toplineFourWeekDenominator,

                max(toplineLastYearValue) AS toplineLastYearValue,
                max(toplineLastYearDenominator) AS toplineLastYearDenominator
            FROM bucketMembers
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
                comparisonType,
                comparisonLabel,
                comparisonSortOrder,
                breakoutType,
                breakoutLabel,
                breakoutSortOrder,
                displayBucketKey,
                displayBreakoutValue
        ),
        bucketValues AS(
            SELECT
                *,
                CASE
                    WHEN metricKind='ratio' THEN try_divide(currentNumerator,currentDenominator)
                    ELSE currentNumerator
                END AS currentValue,

                CASE
                    WHEN metricKind='ratio' THEN try_divide(priorWeekNumerator,priorWeekDenominator)
                    ELSE priorWeekNumerator
                END AS priorWeekValue,

                CASE
                    WHEN metricKind='ratio' THEN try_divide(fourWeekTrendNumerator,fourWeekTrendDenominator)
                    WHEN fourWeekTrendWeekCount>0 THEN try_divide(fourWeekTrendNumerator,cast(fourWeekTrendWeekCount AS DOUBLE))
                    ELSE NULL
                END AS fourWeekValue,

                CASE
                    WHEN metricKind='ratio' THEN try_divide(sameWeekLyNumerator,sameWeekLyDenominator)
                    ELSE sameWeekLyNumerator
                END AS lastYearValue,

                CASE
                    WHEN metricKind='ratio' THEN try_divide(peerSetNumerator,peerSetDenominator)
                    ELSE peerSetNumerator
                END AS peerSetValue
            FROM bucketAgg
        ),
        selectedValues AS(
            SELECT
                *,
                CASE comparisonType
                    WHEN 'priorWeek' THEN priorWeekValue
                    WHEN 'fourWeek' THEN fourWeekValue
                    WHEN 'lastYear' THEN lastYearValue
                END AS selectedComparisonValue,

                CASE comparisonType
                    WHEN 'priorWeek' THEN priorWeekNumerator
                    WHEN 'fourWeek' THEN fourWeekTrendNumerator
                    WHEN 'lastYear' THEN sameWeekLyNumerator
                END AS selectedComparisonNumerator,

                CASE comparisonType
                    WHEN 'priorWeek' THEN toplinePriorWeekValue
                    WHEN 'fourWeek' THEN toplineFourWeekValue
                    WHEN 'lastYear' THEN toplineLastYearValue
                END AS selectedToplineComparisonValue,

                CASE comparisonType
                    WHEN 'priorWeek' THEN toplinePriorWeekDenominator
                    WHEN 'fourWeek' THEN toplineFourWeekDenominator
                    WHEN 'lastYear' THEN toplineLastYearDenominator
                END AS selectedToplineComparisonDenominator
            FROM bucketValues
        ),
        deltas AS(
            SELECT
                *,

                currentValue-priorWeekValue AS priorWeekAbsoluteDiffValue,
                CASE
                    WHEN currentValue IS NULL OR priorWeekValue IS NULL THEN NULL
                    WHEN changeUnit='pp' THEN 100D*(currentValue-priorWeekValue)
                    WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,priorWeekValue)-1D)
                END AS priorWeekChangeRaw,

                currentValue-fourWeekValue AS fourWeekAbsoluteDiffValue,
                CASE
                    WHEN currentValue IS NULL OR fourWeekValue IS NULL THEN NULL
                    WHEN changeUnit='pp' THEN 100D*(currentValue-fourWeekValue)
                    WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,fourWeekValue)-1D)
                END AS fourWeekChangeRaw,

                currentValue-lastYearValue AS lastYearAbsoluteDiffValue,
                CASE
                    WHEN currentValue IS NULL OR lastYearValue IS NULL THEN NULL
                    WHEN changeUnit='pp' THEN 100D*(currentValue-lastYearValue)
                    WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,lastYearValue)-1D)
                END AS lastYearChangeRaw,

                currentValue-peerSetValue AS peerSetAbsoluteDiffValue,
                CASE
                    WHEN currentValue IS NULL OR peerSetValue IS NULL THEN NULL
                    WHEN changeUnit='pp' THEN 100D*(currentValue-peerSetValue)
                    WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,peerSetValue)-1D)
                END AS peerSetChangeRaw,

                currentValue-selectedComparisonValue AS selectedAbsoluteDiffValue,

                CASE
                    WHEN metricKind='count' THEN
                        100D*try_divide(currentValue-selectedComparisonValue,selectedToplineComparisonValue)
                    WHEN metricKind='ratio' THEN
                        100D*(
                            try_divide(currentNumerator,toplineCurrentDenominator)
                            -try_divide(selectedComparisonNumerator,selectedToplineComparisonDenominator)
                        )
                END AS impactOnToplineRaw,

                CASE
                    WHEN metricKind='count' THEN 'pct'
                    WHEN metricKind='ratio' THEN 'pp'
                END AS impactOnToplineUnit
            FROM selectedValues
        ),
        rankedGlobal AS(
            SELECT
                *,
                row_number() OVER(
                    PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName,comparisonType
                    ORDER BY abs(impactOnToplineRaw) DESC NULLS LAST,
                             abs(selectedAbsoluteDiffValue) DESC NULLS LAST,
                             breakoutType,
                             breakoutValue
                ) AS impactRankAcrossBreakouts,

                count(*) OVER(
                    PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName,comparisonType
                ) AS candidateRowCount,

                sum(CASE WHEN abs(impactOnToplineRaw)>=1D THEN 1 ELSE 0 END) OVER(
                    PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName,comparisonType
                ) AS clearsOnePercentCount
            FROM deltas
            WHERE impactOnToplineRaw IS NOT NULL
        ),
        rounded AS(
            SELECT
                *,
                CASE
                    WHEN priorWeekChangeRaw IS NULL THEN NULL
                    WHEN abs(priorWeekChangeRaw)<0.05D THEN 0D
                    ELSE round(priorWeekChangeRaw,1)
                END AS priorWeekChangeValue,

                CASE
                    WHEN fourWeekChangeRaw IS NULL THEN NULL
                    WHEN abs(fourWeekChangeRaw)<0.05D THEN 0D
                    ELSE round(fourWeekChangeRaw,1)
                END AS fourWeekChangeValue,

                CASE
                    WHEN lastYearChangeRaw IS NULL THEN NULL
                    WHEN abs(lastYearChangeRaw)<0.05D THEN 0D
                    ELSE round(lastYearChangeRaw,1)
                END AS lastYearChangeValue,

                CASE
                    WHEN peerSetChangeRaw IS NULL THEN NULL
                    WHEN abs(peerSetChangeRaw)<0.05D THEN 0D
                    ELSE round(peerSetChangeRaw,1)
                END AS peerSetChangeValue,

                CASE
                    WHEN impactOnToplineRaw IS NULL THEN NULL
                    WHEN abs(impactOnToplineRaw)<0.05D THEN 0D
                    ELSE round(impactOnToplineRaw,1)
                END AS impactOnToplineValue
            FROM rankedGlobal
        ),
        formatted AS(
            SELECT
                *,

                CASE
                    WHEN currentValue IS NULL THEN NULL
                    WHEN displayFormat='percent'
                        THEN concat(format_number(100D*currentValue,1),'%')
                    WHEN abs(currentValue)>=1000000000D
                        THEN concat(regexp_replace(format_number(currentValue/1000000000D,1),'\\.0$',''),'B')
                    WHEN abs(currentValue)>=1000000D
                        THEN concat(regexp_replace(format_number(currentValue/1000000D,1),'\\.0$',''),'M')
                    WHEN abs(currentValue)>=1000D
                        THEN concat(regexp_replace(format_number(currentValue/1000D,1),'\\.0$',''),'K')
                    ELSE format_number(currentValue,0)
                END AS currentValueDisplay,

                CASE
                    WHEN priorWeekAbsoluteDiffValue IS NULL THEN NULL
                    WHEN metricKind='ratio'
                        THEN concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                    format_number(100D*priorWeekAbsoluteDiffValue,1),'pp')
                    WHEN abs(priorWeekAbsoluteDiffValue)>=1000000000D
                        THEN concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                    regexp_replace(format_number(priorWeekAbsoluteDiffValue/1000000000D,1),'\\.0$',''),'B')
                    WHEN abs(priorWeekAbsoluteDiffValue)>=1000000D
                        THEN concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                    regexp_replace(format_number(priorWeekAbsoluteDiffValue/1000000D,1),'\\.0$',''),'M')
                    WHEN abs(priorWeekAbsoluteDiffValue)>=1000D
                        THEN concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                    regexp_replace(format_number(priorWeekAbsoluteDiffValue/1000D,1),'\\.0$',''),'K')
                    ELSE concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                format_number(priorWeekAbsoluteDiffValue,0))
                END AS priorWeekAbsoluteDiffDisplay,

                CASE
                    WHEN priorWeekChangeValue IS NULL THEN NULL
                    WHEN changeUnit='pp'
                        THEN concat(CASE WHEN priorWeekChangeValue>0D THEN '+' ELSE '' END,format_number(priorWeekChangeValue,1),'pp')
                    ELSE concat(CASE WHEN priorWeekChangeValue>0D THEN '+' ELSE '' END,format_number(priorWeekChangeValue,1),'%')
                END AS priorWeekChangeDisplay,

                CASE
                    WHEN fourWeekAbsoluteDiffValue IS NULL THEN NULL
                    WHEN metricKind='ratio'
                        THEN concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                    format_number(100D*fourWeekAbsoluteDiffValue,1),'pp')
                    WHEN abs(fourWeekAbsoluteDiffValue)>=1000000000D
                        THEN concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                    regexp_replace(format_number(fourWeekAbsoluteDiffValue/1000000000D,1),'\\.0$',''),'B')
                    WHEN abs(fourWeekAbsoluteDiffValue)>=1000000D
                        THEN concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                    regexp_replace(format_number(fourWeekAbsoluteDiffValue/1000000D,1),'\\.0$',''),'M')
                    WHEN abs(fourWeekAbsoluteDiffValue)>=1000D
                        THEN concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                    regexp_replace(format_number(fourWeekAbsoluteDiffValue/1000D,1),'\\.0$',''),'K')
                    ELSE concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                format_number(fourWeekAbsoluteDiffValue,0))
                END AS fourWeekAbsoluteDiffDisplay,

                CASE
                    WHEN fourWeekChangeValue IS NULL THEN NULL
                    WHEN changeUnit='pp'
                        THEN concat(CASE WHEN fourWeekChangeValue>0D THEN '+' ELSE '' END,format_number(fourWeekChangeValue,1),'pp')
                    ELSE concat(CASE WHEN fourWeekChangeValue>0D THEN '+' ELSE '' END,format_number(fourWeekChangeValue,1),'%')
                END AS fourWeekChangeDisplay,

                CASE
                    WHEN lastYearAbsoluteDiffValue IS NULL THEN NULL
                    WHEN metricKind='ratio'
                        THEN concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                    format_number(100D*lastYearAbsoluteDiffValue,1),'pp')
                    WHEN abs(lastYearAbsoluteDiffValue)>=1000000000D
                        THEN concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                    regexp_replace(format_number(lastYearAbsoluteDiffValue/1000000000D,1),'\\.0$',''),'B')
                    WHEN abs(lastYearAbsoluteDiffValue)>=1000000D
                        THEN concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                    regexp_replace(format_number(lastYearAbsoluteDiffValue/1000000D,1),'\\.0$',''),'M')
                    WHEN abs(lastYearAbsoluteDiffValue)>=1000D
                        THEN concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                    regexp_replace(format_number(lastYearAbsoluteDiffValue/1000D,1),'\\.0$',''),'K')
                    ELSE concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                format_number(lastYearAbsoluteDiffValue,0))
                END AS lastYearAbsoluteDiffDisplay,

                CASE
                    WHEN lastYearChangeValue IS NULL THEN NULL
                    WHEN changeUnit='pp'
                        THEN concat(CASE WHEN lastYearChangeValue>0D THEN '+' ELSE '' END,format_number(lastYearChangeValue,1),'pp')
                    ELSE concat(CASE WHEN lastYearChangeValue>0D THEN '+' ELSE '' END,format_number(lastYearChangeValue,1),'%')
                END AS lastYearChangeDisplay,

                CASE
                    WHEN peerSetValue IS NULL THEN NULL
                    WHEN displayFormat='percent'
                        THEN concat(format_number(100D*peerSetValue,1),'%')
                    WHEN abs(peerSetValue)>=1000000000D
                        THEN concat(regexp_replace(format_number(peerSetValue/1000000000D,1),'\\.0$',''),'B')
                    WHEN abs(peerSetValue)>=1000000D
                        THEN concat(regexp_replace(format_number(peerSetValue/1000000D,1),'\\.0$',''),'M')
                    WHEN abs(peerSetValue)>=1000D
                        THEN concat(regexp_replace(format_number(peerSetValue/1000D,1),'\\.0$',''),'K')
                    ELSE format_number(peerSetValue,0)
                END AS peerSetValueDisplay,

                CASE
                    WHEN peerSetAbsoluteDiffValue IS NULL THEN NULL
                    WHEN metricKind='ratio'
                        THEN concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                    format_number(100D*peerSetAbsoluteDiffValue,1),'pp')
                    WHEN abs(peerSetAbsoluteDiffValue)>=1000000000D
                        THEN concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                    regexp_replace(format_number(peerSetAbsoluteDiffValue/1000000000D,1),'\\.0$',''),'B')
                    WHEN abs(peerSetAbsoluteDiffValue)>=1000000D
                        THEN concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                    regexp_replace(format_number(peerSetAbsoluteDiffValue/1000000D,1),'\\.0$',''),'M')
                    WHEN abs(peerSetAbsoluteDiffValue)>=1000D
                        THEN concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                    regexp_replace(format_number(peerSetAbsoluteDiffValue/1000D,1),'\\.0$',''),'K')
                    ELSE concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                format_number(peerSetAbsoluteDiffValue,0))
                END AS peerSetAbsoluteDiffDisplay,

                CASE
                    WHEN peerSetChangeValue IS NULL THEN NULL
                    WHEN changeUnit='pp'
                        THEN concat(CASE WHEN peerSetChangeValue>0D THEN '+' ELSE '' END,format_number(peerSetChangeValue,1),'pp')
                    ELSE concat(CASE WHEN peerSetChangeValue>0D THEN '+' ELSE '' END,format_number(peerSetChangeValue,1),'%')
                END AS peerSetChangeDisplay,

                CASE
                    WHEN impactOnToplineValue IS NULL THEN NULL
                    WHEN impactOnToplineUnit='pp'
                        THEN concat(CASE WHEN impactOnToplineValue>0D THEN '+' ELSE '' END,format_number(impactOnToplineValue,1),'pp')
                    ELSE concat(CASE WHEN impactOnToplineValue>0D THEN '+' ELSE '' END,format_number(impactOnToplineValue,1),'%')
                END AS impactOnToplineDisplay
            FROM rounded
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

            comparisonType,
            comparisonLabel,
            comparisonSortOrder,

            breakoutType,
            breakoutLabel,
            breakoutValue,
            breakoutSortOrder,
            isOtherBucket,

            currentValue,
            currentValueDisplay,

            priorWeekAbsoluteDiffValue,
            priorWeekAbsoluteDiffDisplay,
            priorWeekChangeValue,
            priorWeekChangeDisplay,

            fourWeekAbsoluteDiffValue,
            fourWeekAbsoluteDiffDisplay,
            fourWeekChangeValue,
            fourWeekChangeDisplay,

            lastYearAbsoluteDiffValue,
            lastYearAbsoluteDiffDisplay,
            lastYearChangeValue,
            lastYearChangeDisplay,

            peerSetValue,
            peerSetValueDisplay,
            peerSetAbsoluteDiffValue,
            peerSetAbsoluteDiffDisplay,
            peerSetChangeValue,
            peerSetChangeDisplay,

            impactOnToplineValue,
            impactOnToplineDisplay,
            impactOnToplineUnit,

            impactRankAcrossBreakouts,
            impactRankAcrossBreakouts<=5 AS isTop5,
            impactRankAcrossBreakouts<=10 AS isTop10,
            impactRankAcrossBreakouts<=100 AS isTop100,

            candidateRowCount,
            abs(impactOnToplineRaw)>=1D AS clearsOnePercent,
            clearsOnePercentCount,

            v_processedAt AS appProcessedAt
        FROM formatted;

        -- =====================================================================
        -- 6. Success
        -- =====================================================================
        SELECT
            'SUCCESS' AS status,
            v_weekFrom AS rebuiltWeekStartFrom,
            v_weekTo AS rebuiltWeekStartTo,
            v_weekEndTo AS latestWeekEndDate,
            CASE WHEN v_asOfDate<v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,
            'priorWeek | fourWeek | lastYear' AS supportedMoverComparisons,
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewToplineMovers_long' AS targetObject,
            v_processedAt AS appProcessedAt;
    END IF;
END;

-- ============================================================================
-- DEVELOPMENT
-- ============================================================================

-- One-time migration before first run of this redesigned contract:
-- DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewToplineMovers_long;

-- Preflight:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewToplineMovers_long(
--     p_asOfDate=>DATE '2026-09-28',
--     p_weeksToRebuild=>1,
--     p_validateOnly=>TRUE
-- );

-- Load/rebuild:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewToplineMovers_long(
--     p_asOfDate=>DATE '2026-09-28',
--     p_weeksToRebuild=>12,
--     p_validateOnly=>FALSE
-- );

-- ============================================================================
-- UI EXAMPLE
-- Q3 2026 | W7 | Total UPV | 4-wk | All
-- ============================================================================

-- SELECT
--     breakoutLabel,
--     breakoutValue,
--     currentValueDisplay,
--     priorWeekAbsoluteDiffDisplay,
--     priorWeekChangeDisplay,
--     fourWeekAbsoluteDiffDisplay,
--     fourWeekChangeDisplay,
--     lastYearAbsoluteDiffDisplay,
--     lastYearChangeDisplay,
--     peerSetChangeDisplay,
--     peerSetAbsoluteDiffDisplay,
--     impactOnToplineDisplay,
--     impactRankAcrossBreakouts,
--     clearsOnePercentCount,
--     candidateRowCount
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewToplineMovers_long
-- WHERE fiscalQuarterLabel='Q3'
--   AND targetWeekStartDate=DATE '2026-08-09'
--   AND metricName='nbv'
--   AND comparisonType='fourWeek'
--   AND isTop100
-- ORDER BY impactRankAcrossBreakouts;

-- Top 5:
-- ... AND isTop5

-- Top 10:
-- ... AND isTop10

-- All:
-- ... AND isTop100

-- Duplicate check; expected 0:
-- SELECT
--     targetWeekStartDate,
--     filterLob,
--     filterPlatform,
--     metricName,
--     comparisonType,
--     breakoutType,
--     breakoutValue,
--     count(*) AS rowCount
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewToplineMovers_long
-- GROUP BY
--     targetWeekStartDate,
--     filterLob,
--     filterPlatform,
--     metricName,
--     comparisonType,
--     breakoutType,
--     breakoutValue
-- HAVING count(*)>1;