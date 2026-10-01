-- ============================================================================
-- FILE  : 08_sdi_sp_mip_gold_appCrosstabsMatrix_long.sql
-- LAYER : GOLD / APP
-- TAB   : Crosstabs
-- SECTION: Crosstab matrix
--
-- PURPOSE:
--   Fully application-ready Crosstab matrix.
--
-- UI FILTERS:
--   Quarter
--   Week
--   Metric
--   Rows
--   Columns
--   Comparator = priorWeek | fourWeek | lastYear
--   Size       = top5 | top8 | top10 | all
--   LOB / Platform where applicable
--
-- APP ELIGIBILITY:
--   metric -> isActive AND showOnBreakouts
--   pair   -> active Crosstab pair
--   axes   -> active breakout dimensions
--
-- BEHAVIOR:
--   - Rows and Columns are independently ranked by THIS-WEEK level.
--   - Both normal and swapped orientations are persisted.
--   - Top5 / Top8 / Top10 / All are materialized.
--   - All = Top100 + scoped (Other).
--   - Ratio metrics reaggregate numerator/denominator internally before ratio.
--
-- FINAL APP TABLE DOES NOT EXPOSE NUMERATORS/DENOMINATORS.
--
-- SCREENSHOT:
--   Cell:
--     absolute change
--     percentage / pp change
--
--   Row total:
--     this-week level
--     selected-comparator change
--
--   Column total:
--     this-week level
--     selected-comparator change
--
--   Bottom-right:
--     topline this-week level
--     topline absolute change
-- ============================================================================

-- ONE-TIME MIGRATION ONLY:
-- DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsMatrix_long;

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_gold_appCrosstabsMatrix_long(
    IN p_asOfDate DATE DEFAULT NULL,
    IN p_weeksToRebuild INT DEFAULT 1,
    IN p_validateOnly BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'MIP Gold App: Crosstab matrix with dynamic supported row/column orientation, comparator-aware values and Top5/Top8/Top10/All.'
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
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long g
        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_crosstabCatalog_static pc
          ON pc.pairKey=g.pairKey
         AND pc.isActive
        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
          ON mc.metricName=g.metricName
         AND mc.isActive
         AND mc.showOnBreakouts
        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static rb
          ON rb.breakoutType=g.rowBreakoutType
         AND rb.isActive
        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static cb
          ON cb.breakoutType=g.columnBreakoutType
         AND cb.isActive
        WHERE g.targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='Crosstab Gold has no eligible App rows for the requested week range.';
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
            SET MESSAGE_TEXT='Overview Gold has no eligible Crosstab metrics for the requested week range.';
    END IF;

    IF NOT EXISTS(
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_crosstabCatalog_static
        WHERE isActive
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='Crosstab Catalog has no active pairs.';
    END IF;

    IF NOT EXISTS(
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static
        WHERE isActive AND showOnBreakouts
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='Metric Catalog has no active diagnostic metrics.';
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
    -- 3. Duplicate grain checks
    -- =========================================================================
    IF EXISTS(
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long g
        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_crosstabCatalog_static pc
          ON pc.pairKey=g.pairKey
         AND pc.isActive
        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
          ON mc.metricName=g.metricName
         AND mc.isActive
         AND mc.showOnBreakouts
        WHERE g.targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        GROUP BY
            g.targetWeekStartDate,
            g.filterLob,
            g.filterPlatform,
            g.pairKey,
            g.rowBreakoutValue,
            g.columnBreakoutValue,
            g.metricName
        HAVING count(*)>1
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='Duplicate eligible Crosstab Gold analytical keys detected.';
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
            SET MESSAGE_TEXT='Crosstab Metric Catalog contains unsupported metadata.';
    END IF;

    -- =========================================================================
    -- 4. Validation only
    -- =========================================================================
    IF p_validateOnly THEN
        SELECT
            'VALIDATION_ONLY' AS status,
            v_weekFrom AS rebuildWeekStartFrom,
            v_weekTo AS rebuildWeekStartTo,
            'priorWeek | fourWeek | lastYear' AS supportedComparisons,
            'top5 | top8 | top10 | all' AS supportedDisplaySizes,
            TRUE AS supportsSwappedAxes,
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsMatrix_long' AS targetObject,
            'Validation passed. No Gold App table was modified.' AS message;
    ELSE

        -- =====================================================================
        -- 5. App table
        -- =====================================================================
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsMatrix_long(
            targetWeekStartDate DATE,
            targetWeekEndDate DATE,
            fiscalYear INT,
            fiscalQuarterLabel STRING,
            fiscalWeekCode STRING,
            weekLabel STRING,
            weekEndingLabel STRING,

            filterLob STRING,
            filterPlatform STRING,

            sourcePairKey STRING,
            sourcePairLabel STRING,
            sourcePairSortOrder INT,

            pairKey STRING,
            pairLabel STRING,
            isSwappedOrientation BOOLEAN,

            rowBreakoutType STRING,
            rowBreakoutLabel STRING,
            rowBreakoutSortOrder INT,

            columnBreakoutType STRING,
            columnBreakoutLabel STRING,
            columnBreakoutSortOrder INT,

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
            comparisonDataAvailable BOOLEAN,
            comparisonWindowComplete BOOLEAN,

            displaySize STRING,
            displaySizeLabel STRING,
            displayLimit INT,
            displaySizeSortOrder INT,

            rowBreakoutValue STRING,
            rowDisplayRank BIGINT,
            isRowOtherBucket BOOLEAN,

            columnBreakoutValue STRING,
            columnDisplayRank BIGINT,
            isColumnOtherBucket BOOLEAN,

            cellSortOrder BIGINT,

            cellCurrentValue DOUBLE,
            cellCurrentValueDisplay STRING,

            cellComparisonValue DOUBLE,
            cellComparisonValueDisplay STRING,

            cellAbsoluteDiffValue DOUBLE,
            cellAbsoluteDiffDisplay STRING,

            cellChangeValue DOUBLE,
            cellChangeDisplay STRING,
            cellDirection STRING,

            rowTotalCurrentValue DOUBLE,
            rowTotalCurrentDisplay STRING,
            rowTotalAbsoluteDiffValue DOUBLE,
            rowTotalAbsoluteDiffDisplay STRING,
            rowTotalChangeValue DOUBLE,
            rowTotalChangeDisplay STRING,

            columnTotalCurrentValue DOUBLE,
            columnTotalCurrentDisplay STRING,
            columnTotalAbsoluteDiffValue DOUBLE,
            columnTotalAbsoluteDiffDisplay STRING,
            columnTotalChangeValue DOUBLE,
            columnTotalChangeDisplay STRING,

            toplineCurrentValue DOUBLE,
            toplineCurrentDisplay STRING,
            toplineAbsoluteDiffValue DOUBLE,
            toplineAbsoluteDiffDisplay STRING,
            toplineChangeValue DOUBLE,
            toplineChangeDisplay STRING,

            appProcessedAt TIMESTAMP
        )
        USING DELTA
        CLUSTER BY(targetWeekStartDate,metricName,pairKey,displaySize)
        COMMENT 'MIP Gold App: Crosstab matrix with both axis orientations and render-ready cells, totals and comparator values.';

        -- =====================================================================
        -- 6. Rebuild
        -- =====================================================================
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsMatrix_long
        REPLACE WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo

        WITH sourceBase AS(
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

                g.pairKey AS sourcePairKey,
                pc.pairLabel AS sourcePairLabel,
                pc.sortOrder AS sourcePairSortOrder,

                g.rowBreakoutType AS sourceRowBreakoutType,
                rb.breakoutLabel AS sourceRowBreakoutLabel,
                rb.sortOrder AS sourceRowBreakoutSortOrder,
                g.rowBreakoutValue AS sourceRowBreakoutValue,

                g.columnBreakoutType AS sourceColumnBreakoutType,
                cb.breakoutLabel AS sourceColumnBreakoutLabel,
                cb.sortOrder AS sourceColumnBreakoutSortOrder,
                g.columnBreakoutValue AS sourceColumnBreakoutValue,

                g.metricName,
                CASE WHEN g.metricName='nbv' THEN 'Total UPV' ELSE mc.metricLabel END AS metricLabel,
                mc.metricDescription,
                mc.metricKind,
                mc.displayFormat,
                mc.changeUnit,
                mc.sortOrder AS metricSortOrder,

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
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long g
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_crosstabCatalog_static pc
              ON pc.pairKey=g.pairKey
             AND pc.isActive
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static rb
              ON rb.breakoutType=g.rowBreakoutType
             AND rb.isActive
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static cb
              ON cb.breakoutType=g.columnBreakoutType
             AND cb.isActive
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
              ON mc.metricName=g.metricName
             AND mc.isActive
             AND mc.showOnBreakouts
            LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c
              ON c.weekStartDate=g.targetWeekStartDate
            WHERE g.targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        ),

        -- =====================================================================
        -- Store BOTH orientations.
        --
        -- This means:
        -- Rows=Channel, Columns=Entry page
        -- AND
        -- Rows=Entry page, Columns=Channel
        --
        -- are both simple App-table filters.
        -- =====================================================================
        oriented AS(
            SELECT
                s.*,

                sourcePairKey AS pairKey,
                sourcePairLabel AS pairLabel,
                FALSE AS isSwappedOrientation,

                sourceRowBreakoutType AS rowBreakoutType,
                sourceRowBreakoutLabel AS rowBreakoutLabel,
                sourceRowBreakoutSortOrder AS rowBreakoutSortOrder,
                sourceRowBreakoutValue AS rowBreakoutValue,

                sourceColumnBreakoutType AS columnBreakoutType,
                sourceColumnBreakoutLabel AS columnBreakoutLabel,
                sourceColumnBreakoutSortOrder AS columnBreakoutSortOrder,
                sourceColumnBreakoutValue AS columnBreakoutValue
            FROM sourceBase s

            UNION ALL

            SELECT
                s.*,

                concat(sourceColumnBreakoutType,'__',sourceRowBreakoutType) AS pairKey,
                concat(sourceColumnBreakoutLabel,' × ',sourceRowBreakoutLabel) AS pairLabel,
                TRUE AS isSwappedOrientation,

                sourceColumnBreakoutType,
                sourceColumnBreakoutLabel,
                sourceColumnBreakoutSortOrder,
                sourceColumnBreakoutValue,

                sourceRowBreakoutType,
                sourceRowBreakoutLabel,
                sourceRowBreakoutSortOrder,
                sourceRowBreakoutValue
            FROM sourceBase s
            WHERE sourceRowBreakoutType<>sourceColumnBreakoutType
        ),

        comparisonLong AS(
            SELECT
                o.*,
                'priorWeek' AS comparisonType,
                'Prior week' AS comparisonLabel,
                10 AS comparisonSortOrder,
                priorWeekDataAvailable AS comparisonDataAvailable,
                priorWeekDataAvailable AS comparisonWindowComplete,
                priorWeekNumerator AS comparisonNumerator,
                priorWeekDenominator AS comparisonDenominator
            FROM oriented o

            UNION ALL

            SELECT
                o.*,
                'fourWeek',
                '4-wk trend',
                20,
                fourWeekTrendWeekCount>0,
                fourWeekTrendWeekCount=4,
                CASE
                    WHEN metricKind='count' AND fourWeekTrendWeekCount>0
                        THEN try_divide(fourWeekTrendNumerator,cast(fourWeekTrendWeekCount AS DOUBLE))
                    ELSE fourWeekTrendNumerator
                END,
                CASE
                    WHEN metricKind='count' THEN NULL
                    ELSE fourWeekTrendDenominator
                END
            FROM oriented o

            UNION ALL

            SELECT
                o.*,
                'lastYear',
                'Same wk LY',
                30,
                sameWeekLyDataAvailable,
                sameWeekLyDataAvailable,
                sameWeekLyNumerator,
                sameWeekLyDenominator
            FROM oriented o
        ),

        -- =====================================================================
        -- Axis totals before Top-N bucketing.
        -- These determine what is Top5 / Top8 / Top10.
        -- =====================================================================
        rowAgg AS(
            SELECT
                targetWeekStartDate,
                filterLob,
                filterPlatform,
                pairKey,
                metricName,
                comparisonType,
                rowBreakoutValue,
                metricKind,

                sum(thisWeekNumerator) AS currentNumerator,
                sum(thisWeekDenominator) AS currentDenominator
            FROM comparisonLong
            GROUP BY
                targetWeekStartDate,
                filterLob,
                filterPlatform,
                pairKey,
                metricName,
                comparisonType,
                rowBreakoutValue,
                metricKind
        ),

        rowValues AS(
            SELECT
                *,
                CASE
                    WHEN metricKind='ratio'
                        THEN try_divide(currentNumerator,currentDenominator)
                    ELSE currentNumerator
                END AS rowCurrentValue
            FROM rowAgg
        ),

        rowRanks AS(
            SELECT
                *,
                row_number() OVER(
                    PARTITION BY
                        targetWeekStartDate,
                        filterLob,
                        filterPlatform,
                        pairKey,
                        metricName,
                        comparisonType
                    ORDER BY
                        rowCurrentValue DESC NULLS LAST,
                        rowBreakoutValue
                ) AS rowRankByMetric
            FROM rowValues
        ),

        columnAgg AS(
            SELECT
                targetWeekStartDate,
                filterLob,
                filterPlatform,
                pairKey,
                metricName,
                comparisonType,
                columnBreakoutValue,
                metricKind,

                sum(thisWeekNumerator) AS currentNumerator,
                sum(thisWeekDenominator) AS currentDenominator
            FROM comparisonLong
            GROUP BY
                targetWeekStartDate,
                filterLob,
                filterPlatform,
                pairKey,
                metricName,
                comparisonType,
                columnBreakoutValue,
                metricKind
        ),

        columnValues AS(
            SELECT
                *,
                CASE
                    WHEN metricKind='ratio'
                        THEN try_divide(currentNumerator,currentDenominator)
                    ELSE currentNumerator
                END AS columnCurrentValue
            FROM columnAgg
        ),

        columnRanks AS(
            SELECT
                *,
                row_number() OVER(
                    PARTITION BY
                        targetWeekStartDate,
                        filterLob,
                        filterPlatform,
                        pairKey,
                        metricName,
                        comparisonType
                    ORDER BY
                        columnCurrentValue DESC NULLS LAST,
                        columnBreakoutValue
                ) AS columnRankByMetric
            FROM columnValues
        ),

        rankedCells AS(
            SELECT
                c.*,
                r.rowRankByMetric,
                k.columnRankByMetric
            FROM comparisonLong c
            JOIN rowRanks r
              ON r.targetWeekStartDate=c.targetWeekStartDate
             AND r.filterLob=c.filterLob
             AND r.filterPlatform=c.filterPlatform
             AND r.pairKey=c.pairKey
             AND r.metricName=c.metricName
             AND r.comparisonType=c.comparisonType
             AND r.rowBreakoutValue=c.rowBreakoutValue
            JOIN columnRanks k
              ON k.targetWeekStartDate=c.targetWeekStartDate
             AND k.filterLob=c.filterLob
             AND k.filterPlatform=c.filterPlatform
             AND k.pairKey=c.pairKey
             AND k.metricName=c.metricName
             AND k.comparisonType=c.comparisonType
             AND k.columnBreakoutValue=c.columnBreakoutValue
        ),

        sizeConfig AS(
            SELECT * FROM VALUES
                ('top5','Top 5',5,10),
                ('top8','Top 8',8,20),
                ('top10','Top 10',10,30),
                ('all','All',100,40)
            AS s(
                displaySize,
                displaySizeLabel,
                displayLimit,
                displaySizeSortOrder
            )
        ),

        expanded AS(
            SELECT
                r.*,
                s.displaySize,
                s.displaySizeLabel,
                s.displayLimit,
                s.displaySizeSortOrder,

                CASE
                    WHEN rowRankByMetric<=s.displayLimit
                        THEN concat('ROW::VALUE::',coalesce(rowBreakoutValue,'(null)'))
                    ELSE 'ROW::OTHER::REMAINDER'
                END AS rowBucketKey,

                CASE
                    WHEN rowRankByMetric<=s.displayLimit
                        THEN rowBreakoutValue
                    ELSE '(Other)'
                END AS displayRowBreakoutValue,

                rowRankByMetric>s.displayLimit AS rowOtherMember,

                CASE
                    WHEN columnRankByMetric<=s.displayLimit
                        THEN concat('COL::VALUE::',coalesce(columnBreakoutValue,'(null)'))
                    ELSE 'COL::OTHER::REMAINDER'
                END AS columnBucketKey,

                CASE
                    WHEN columnRankByMetric<=s.displayLimit
                        THEN columnBreakoutValue
                    ELSE '(Other)'
                END AS displayColumnBreakoutValue,

                columnRankByMetric>s.displayLimit AS columnOtherMember
            FROM rankedCells r
            CROSS JOIN sizeConfig s
        ),

        -- =====================================================================
        -- Reaggregate after Top-N / Other mapping.
        -- Ratios remain ratio-safe because ingredients are summed first.
        -- =====================================================================
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

                sourcePairKey,
                sourcePairLabel,
                sourcePairSortOrder,

                pairKey,
                pairLabel,
                isSwappedOrientation,

                rowBreakoutType,
                rowBreakoutLabel,
                rowBreakoutSortOrder,

                columnBreakoutType,
                columnBreakoutLabel,
                columnBreakoutSortOrder,

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

                min(CASE WHEN comparisonDataAvailable THEN 1 ELSE 0 END)=1
                    AS comparisonDataAvailable,

                min(CASE WHEN comparisonWindowComplete THEN 1 ELSE 0 END)=1
                    AS comparisonWindowComplete,

                displaySize,
                displaySizeLabel,
                displayLimit,
                displaySizeSortOrder,

                rowBucketKey,
                displayRowBreakoutValue AS rowBreakoutValue,

                max(CASE WHEN rowOtherMember THEN 1 ELSE 0 END)=1
                    AS isRowOtherBucket,

                CASE
                    WHEN max(CASE WHEN rowOtherMember THEN 1 ELSE 0 END)=1
                        THEN cast(displayLimit+1 AS BIGINT)
                    ELSE min(rowRankByMetric)
                END AS rowDisplayRank,

                columnBucketKey,
                displayColumnBreakoutValue AS columnBreakoutValue,

                max(CASE WHEN columnOtherMember THEN 1 ELSE 0 END)=1
                    AS isColumnOtherBucket,

                CASE
                    WHEN max(CASE WHEN columnOtherMember THEN 1 ELSE 0 END)=1
                        THEN cast(displayLimit+1 AS BIGINT)
                    ELSE min(columnRankByMetric)
                END AS columnDisplayRank,

                sum(thisWeekNumerator) AS currentNumerator,
                sum(thisWeekDenominator) AS currentDenominator,

                sum(comparisonNumerator) AS comparisonNumerator,
                sum(comparisonDenominator) AS comparisonDenominator
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
                sourcePairKey,
                sourcePairLabel,
                sourcePairSortOrder,
                pairKey,
                pairLabel,
                isSwappedOrientation,
                rowBreakoutType,
                rowBreakoutLabel,
                rowBreakoutSortOrder,
                columnBreakoutType,
                columnBreakoutLabel,
                columnBreakoutSortOrder,
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
                displaySize,
                displaySizeLabel,
                displayLimit,
                displaySizeSortOrder,
                rowBucketKey,
                displayRowBreakoutValue,
                columnBucketKey,
                displayColumnBreakoutValue
        ),

        cellValues AS(
            SELECT
                *,

                CASE
                    WHEN metricKind='ratio'
                        THEN try_divide(currentNumerator,currentDenominator)
                    ELSE currentNumerator
                END AS cellCurrentValue,

                CASE
                    WHEN metricKind='ratio'
                        THEN try_divide(comparisonNumerator,comparisonDenominator)
                    ELSE comparisonNumerator
                END AS cellComparisonValue
            FROM bucketAgg
        ),

        cellCalculated AS(
            SELECT
                *,

                cellCurrentValue-cellComparisonValue AS cellAbsoluteDiffValue,

                CASE
                    WHEN cellCurrentValue IS NULL OR cellComparisonValue IS NULL THEN NULL
                    WHEN changeUnit='pp'
                        THEN 100D*(cellCurrentValue-cellComparisonValue)
                    WHEN changeUnit='pct'
                        THEN 100D*(try_divide(cellCurrentValue,cellComparisonValue)-1D)
                END AS cellChangeRaw,

                CASE
                    WHEN cellCurrentValue>cellComparisonValue THEN 'up'
                    WHEN cellCurrentValue<cellComparisonValue THEN 'down'
                    WHEN cellCurrentValue=cellComparisonValue THEN 'flat'
                    ELSE 'unavailable'
                END AS cellDirection,

                cast(rowDisplayRank*1000+columnDisplayRank AS BIGINT)
                    AS cellSortOrder
            FROM cellValues
        ),

        -- =====================================================================
        -- Displayed row totals.
        -- =====================================================================
        rowTotalsAgg AS(
            SELECT
                targetWeekStartDate,
                filterLob,
                filterPlatform,
                pairKey,
                metricName,
                comparisonType,
                displaySize,
                rowBreakoutValue,
                rowDisplayRank,
                metricKind,
                changeUnit,

                sum(currentNumerator) AS currentNumerator,
                sum(currentDenominator) AS currentDenominator,
                sum(comparisonNumerator) AS comparisonNumerator,
                sum(comparisonDenominator) AS comparisonDenominator
            FROM cellCalculated
            GROUP BY
                targetWeekStartDate,
                filterLob,
                filterPlatform,
                pairKey,
                metricName,
                comparisonType,
                displaySize,
                rowBreakoutValue,
                rowDisplayRank,
                metricKind,
                changeUnit
        ),

        rowTotals AS(
            SELECT
                *,

                CASE
                    WHEN metricKind='ratio'
                        THEN try_divide(currentNumerator,currentDenominator)
                    ELSE currentNumerator
                END AS rowTotalCurrentValue,

                CASE
                    WHEN metricKind='ratio'
                        THEN try_divide(comparisonNumerator,comparisonDenominator)
                    ELSE comparisonNumerator
                END AS rowTotalComparisonValue
            FROM rowTotalsAgg
        ),

        rowTotalsCalculated AS(
            SELECT
                *,

                rowTotalCurrentValue-rowTotalComparisonValue
                    AS rowTotalAbsoluteDiffValue,

                CASE
                    WHEN rowTotalCurrentValue IS NULL OR rowTotalComparisonValue IS NULL THEN NULL
                    WHEN changeUnit='pp'
                        THEN 100D*(rowTotalCurrentValue-rowTotalComparisonValue)
                    WHEN changeUnit='pct'
                        THEN 100D*(try_divide(rowTotalCurrentValue,rowTotalComparisonValue)-1D)
                END AS rowTotalChangeRaw
            FROM rowTotals
        ),

        -- =====================================================================
        -- Displayed column totals.
        -- =====================================================================
        columnTotalsAgg AS(
            SELECT
                targetWeekStartDate,
                filterLob,
                filterPlatform,
                pairKey,
                metricName,
                comparisonType,
                displaySize,
                columnBreakoutValue,
                columnDisplayRank,
                metricKind,
                changeUnit,

                sum(currentNumerator) AS currentNumerator,
                sum(currentDenominator) AS currentDenominator,
                sum(comparisonNumerator) AS comparisonNumerator,
                sum(comparisonDenominator) AS comparisonDenominator
            FROM cellCalculated
            GROUP BY
                targetWeekStartDate,
                filterLob,
                filterPlatform,
                pairKey,
                metricName,
                comparisonType,
                displaySize,
                columnBreakoutValue,
                columnDisplayRank,
                metricKind,
                changeUnit
        ),

        columnTotals AS(
            SELECT
                *,

                CASE
                    WHEN metricKind='ratio'
                        THEN try_divide(currentNumerator,currentDenominator)
                    ELSE currentNumerator
                END AS columnTotalCurrentValue,

                CASE
                    WHEN metricKind='ratio'
                        THEN try_divide(comparisonNumerator,comparisonDenominator)
                    ELSE comparisonNumerator
                END AS columnTotalComparisonValue
            FROM columnTotalsAgg
        ),

        columnTotalsCalculated AS(
            SELECT
                *,

                columnTotalCurrentValue-columnTotalComparisonValue
                    AS columnTotalAbsoluteDiffValue,

                CASE
                    WHEN columnTotalCurrentValue IS NULL OR columnTotalComparisonValue IS NULL THEN NULL
                    WHEN changeUnit='pp'
                        THEN 100D*(columnTotalCurrentValue-columnTotalComparisonValue)
                    WHEN changeUnit='pct'
                        THEN 100D*(try_divide(columnTotalCurrentValue,columnTotalComparisonValue)-1D)
                END AS columnTotalChangeRaw
            FROM columnTotals
        ),

        -- =====================================================================
        -- Topline from Overview Gold, not by summing displayed cells.
        -- =====================================================================
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

        toplineLong AS(
            SELECT
                t.*,
                'priorWeek' AS comparisonType,
                t.priorWeekNumerator AS comparisonNumerator,
                t.priorWeekDenominator AS comparisonDenominator
            FROM toplineBase t

            UNION ALL

            SELECT
                t.*,
                'fourWeek',
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
                t.sameWeekLyNumerator,
                t.sameWeekLyDenominator
            FROM toplineBase t
        ),

        toplineValues AS(
            SELECT
                *,

                CASE
                    WHEN metricKind='ratio'
                        THEN try_divide(thisWeekNumerator,thisWeekDenominator)
                    ELSE thisWeekNumerator
                END AS toplineCurrentValue,

                CASE
                    WHEN metricKind='ratio'
                        THEN try_divide(comparisonNumerator,comparisonDenominator)
                    ELSE comparisonNumerator
                END AS toplineComparisonValue
            FROM toplineLong
        ),

        toplineCalculated AS(
            SELECT
                *,

                toplineCurrentValue-toplineComparisonValue
                    AS toplineAbsoluteDiffValue,

                CASE
                    WHEN toplineCurrentValue IS NULL OR toplineComparisonValue IS NULL THEN NULL
                    WHEN changeUnit='pp'
                        THEN 100D*(toplineCurrentValue-toplineComparisonValue)
                    WHEN changeUnit='pct'
                        THEN 100D*(try_divide(toplineCurrentValue,toplineComparisonValue)-1D)
                END AS toplineChangeRaw
            FROM toplineValues
        ),

        joined AS(
            SELECT
                c.*,

                r.rowTotalCurrentValue,
                r.rowTotalAbsoluteDiffValue,
                r.rowTotalChangeRaw,

                k.columnTotalCurrentValue,
                k.columnTotalAbsoluteDiffValue,
                k.columnTotalChangeRaw,

                t.toplineCurrentValue,
                t.toplineAbsoluteDiffValue,
                t.toplineChangeRaw
            FROM cellCalculated c

            JOIN rowTotalsCalculated r
              ON r.targetWeekStartDate=c.targetWeekStartDate
             AND r.filterLob=c.filterLob
             AND r.filterPlatform=c.filterPlatform
             AND r.pairKey=c.pairKey
             AND r.metricName=c.metricName
             AND r.comparisonType=c.comparisonType
             AND r.displaySize=c.displaySize
             AND r.rowBreakoutValue=c.rowBreakoutValue
             AND r.rowDisplayRank=c.rowDisplayRank

            JOIN columnTotalsCalculated k
              ON k.targetWeekStartDate=c.targetWeekStartDate
             AND k.filterLob=c.filterLob
             AND k.filterPlatform=c.filterPlatform
             AND k.pairKey=c.pairKey
             AND k.metricName=c.metricName
             AND k.comparisonType=c.comparisonType
             AND k.displaySize=c.displaySize
             AND k.columnBreakoutValue=c.columnBreakoutValue
             AND k.columnDisplayRank=c.columnDisplayRank

            JOIN toplineCalculated t
              ON t.targetWeekStartDate=c.targetWeekStartDate
             AND t.filterLob=c.filterLob
             AND t.filterPlatform=c.filterPlatform
             AND t.metricName=c.metricName
             AND t.comparisonType=c.comparisonType
        ),

        rounded AS(
            SELECT
                *,

                CASE
                    WHEN cellChangeRaw IS NULL THEN NULL
                    WHEN abs(cellChangeRaw)<0.05D THEN 0D
                    ELSE round(cellChangeRaw,1)
                END AS cellChangeValue,

                CASE
                    WHEN rowTotalChangeRaw IS NULL THEN NULL
                    WHEN abs(rowTotalChangeRaw)<0.05D THEN 0D
                    ELSE round(rowTotalChangeRaw,1)
                END AS rowTotalChangeValue,

                CASE
                    WHEN columnTotalChangeRaw IS NULL THEN NULL
                    WHEN abs(columnTotalChangeRaw)<0.05D THEN 0D
                    ELSE round(columnTotalChangeRaw,1)
                END AS columnTotalChangeValue,

                CASE
                    WHEN toplineChangeRaw IS NULL THEN NULL
                    WHEN abs(toplineChangeRaw)<0.05D THEN 0D
                    ELSE round(toplineChangeRaw,1)
                END AS toplineChangeValue
            FROM joined
        ),

        formatted AS(
            SELECT
                *,

                -- -------------------------------------------------------------
                -- Cell current
                -- -------------------------------------------------------------
                CASE
                    WHEN cellCurrentValue IS NULL THEN NULL
                    WHEN displayFormat='percent'
                        THEN concat(format_number(100D*cellCurrentValue,1),'%')
                    WHEN abs(cellCurrentValue)>=1000000000D
                        THEN concat(regexp_replace(format_number(cellCurrentValue/1000000000D,1),'\\.0$',''),'B')
                    WHEN abs(cellCurrentValue)>=1000000D
                        THEN concat(regexp_replace(format_number(cellCurrentValue/1000000D,1),'\\.0$',''),'M')
                    WHEN abs(cellCurrentValue)>=1000D
                        THEN concat(regexp_replace(format_number(cellCurrentValue/1000D,1),'\\.0$',''),'K')
                    ELSE format_number(cellCurrentValue,0)
                END AS cellCurrentValueDisplay,

                -- Comparator cell level
                CASE
                    WHEN cellComparisonValue IS NULL THEN NULL
                    WHEN displayFormat='percent'
                        THEN concat(format_number(100D*cellComparisonValue,1),'%')
                    WHEN abs(cellComparisonValue)>=1000000000D
                        THEN concat(regexp_replace(format_number(cellComparisonValue/1000000000D,1),'\\.0$',''),'B')
                    WHEN abs(cellComparisonValue)>=1000000D
                        THEN concat(regexp_replace(format_number(cellComparisonValue/1000000D,1),'\\.0$',''),'M')
                    WHEN abs(cellComparisonValue)>=1000D
                        THEN concat(regexp_replace(format_number(cellComparisonValue/1000D,1),'\\.0$',''),'K')
                    ELSE format_number(cellComparisonValue,0)
                END AS cellComparisonValueDisplay,

                -- Screenshot first number in each matrix cell.
                CASE
                    WHEN cellAbsoluteDiffValue IS NULL THEN NULL

                    WHEN metricKind='ratio'
                        THEN concat(
                            CASE WHEN cellAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            format_number(100D*cellAbsoluteDiffValue,1),
                            'pp'
                        )

                    WHEN abs(cellAbsoluteDiffValue)>=1000000000D
                        THEN concat(
                            CASE WHEN cellAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(cellAbsoluteDiffValue/1000000000D,1),'\\.0$',''),
                            'B'
                        )

                    WHEN abs(cellAbsoluteDiffValue)>=1000000D
                        THEN concat(
                            CASE WHEN cellAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(cellAbsoluteDiffValue/1000000D,1),'\\.0$',''),
                            'M'
                        )

                    WHEN abs(cellAbsoluteDiffValue)>=1000D
                        THEN concat(
                            CASE WHEN cellAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(cellAbsoluteDiffValue/1000D,1),'\\.0$',''),
                            'K'
                        )

                    ELSE concat(
                        CASE WHEN cellAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                        format_number(cellAbsoluteDiffValue,0)
                    )
                END AS cellAbsoluteDiffDisplay,

                -- Screenshot second number in each matrix cell.
                CASE
                    WHEN cellChangeValue IS NULL THEN NULL
                    WHEN changeUnit='pp'
                        THEN concat(
                            CASE WHEN cellChangeValue>0D THEN '+' ELSE '' END,
                            format_number(cellChangeValue,1),
                            'pp'
                        )
                    ELSE concat(
                        CASE WHEN cellChangeValue>0D THEN '+' ELSE '' END,
                        format_number(cellChangeValue,1),
                        '%'
                    )
                END AS cellChangeDisplay,

                -- -------------------------------------------------------------
                -- Row-total right side
                -- -------------------------------------------------------------
                CASE
                    WHEN rowTotalCurrentValue IS NULL THEN NULL
                    WHEN displayFormat='percent'
                        THEN concat(format_number(100D*rowTotalCurrentValue,1),'%')
                    WHEN abs(rowTotalCurrentValue)>=1000000000D
                        THEN concat(regexp_replace(format_number(rowTotalCurrentValue/1000000000D,1),'\\.0$',''),'B')
                    WHEN abs(rowTotalCurrentValue)>=1000000D
                        THEN concat(regexp_replace(format_number(rowTotalCurrentValue/1000000D,1),'\\.0$',''),'M')
                    WHEN abs(rowTotalCurrentValue)>=1000D
                        THEN concat(regexp_replace(format_number(rowTotalCurrentValue/1000D,1),'\\.0$',''),'K')
                    ELSE format_number(rowTotalCurrentValue,0)
                END AS rowTotalCurrentDisplay,

                CASE
                    WHEN rowTotalAbsoluteDiffValue IS NULL THEN NULL
                    WHEN metricKind='ratio'
                        THEN concat(
                            CASE WHEN rowTotalAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            format_number(100D*rowTotalAbsoluteDiffValue,1),
                            'pp'
                        )
                    WHEN abs(rowTotalAbsoluteDiffValue)>=1000000D
                        THEN concat(
                            CASE WHEN rowTotalAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(rowTotalAbsoluteDiffValue/1000000D,1),'\\.0$',''),
                            'M'
                        )
                    WHEN abs(rowTotalAbsoluteDiffValue)>=1000D
                        THEN concat(
                            CASE WHEN rowTotalAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(rowTotalAbsoluteDiffValue/1000D,1),'\\.0$',''),
                            'K'
                        )
                    ELSE concat(
                        CASE WHEN rowTotalAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                        format_number(rowTotalAbsoluteDiffValue,0)
                    )
                END AS rowTotalAbsoluteDiffDisplay,

                CASE
                    WHEN rowTotalChangeValue IS NULL THEN NULL
                    WHEN changeUnit='pp'
                        THEN concat(
                            CASE WHEN rowTotalChangeValue>0D THEN '+' ELSE '' END,
                            format_number(rowTotalChangeValue,1),
                            'pp'
                        )
                    ELSE concat(
                        CASE WHEN rowTotalChangeValue>0D THEN '+' ELSE '' END,
                        format_number(rowTotalChangeValue,1),
                        '%'
                    )
                END AS rowTotalChangeDisplay,

                -- -------------------------------------------------------------
                -- Column-total bottom row
                -- -------------------------------------------------------------
                CASE
                    WHEN columnTotalCurrentValue IS NULL THEN NULL
                    WHEN displayFormat='percent'
                        THEN concat(format_number(100D*columnTotalCurrentValue,1),'%')
                    WHEN abs(columnTotalCurrentValue)>=1000000000D
                        THEN concat(regexp_replace(format_number(columnTotalCurrentValue/1000000000D,1),'\\.0$',''),'B')
                    WHEN abs(columnTotalCurrentValue)>=1000000D
                        THEN concat(regexp_replace(format_number(columnTotalCurrentValue/1000000D,1),'\\.0$',''),'M')
                    WHEN abs(columnTotalCurrentValue)>=1000D
                        THEN concat(regexp_replace(format_number(columnTotalCurrentValue/1000D,1),'\\.0$',''),'K')
                    ELSE format_number(columnTotalCurrentValue,0)
                END AS columnTotalCurrentDisplay,

                CASE
                    WHEN columnTotalAbsoluteDiffValue IS NULL THEN NULL
                    WHEN metricKind='ratio'
                        THEN concat(
                            CASE WHEN columnTotalAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            format_number(100D*columnTotalAbsoluteDiffValue,1),
                            'pp'
                        )
                    WHEN abs(columnTotalAbsoluteDiffValue)>=1000000D
                        THEN concat(
                            CASE WHEN columnTotalAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(columnTotalAbsoluteDiffValue/1000000D,1),'\\.0$',''),
                            'M'
                        )
                    WHEN abs(columnTotalAbsoluteDiffValue)>=1000D
                        THEN concat(
                            CASE WHEN columnTotalAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(columnTotalAbsoluteDiffValue/1000D,1),'\\.0$',''),
                            'K'
                        )
                    ELSE concat(
                        CASE WHEN columnTotalAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                        format_number(columnTotalAbsoluteDiffValue,0)
                    )
                END AS columnTotalAbsoluteDiffDisplay,

                CASE
                    WHEN columnTotalChangeValue IS NULL THEN NULL
                    WHEN changeUnit='pp'
                        THEN concat(
                            CASE WHEN columnTotalChangeValue>0D THEN '+' ELSE '' END,
                            format_number(columnTotalChangeValue,1),
                            'pp'
                        )
                    ELSE concat(
                        CASE WHEN columnTotalChangeValue>0D THEN '+' ELSE '' END,
                        format_number(columnTotalChangeValue,1),
                        '%'
                    )
                END AS columnTotalChangeDisplay,

                -- -------------------------------------------------------------
                -- Bottom-right topline
                -- -------------------------------------------------------------
                CASE
                    WHEN toplineCurrentValue IS NULL THEN NULL
                    WHEN displayFormat='percent'
                        THEN concat(format_number(100D*toplineCurrentValue,1),'%')
                    WHEN abs(toplineCurrentValue)>=1000000000D
                        THEN concat(regexp_replace(format_number(toplineCurrentValue/1000000000D,1),'\\.0$',''),'B')
                    WHEN abs(toplineCurrentValue)>=1000000D
                        THEN concat(regexp_replace(format_number(toplineCurrentValue/1000000D,1),'\\.0$',''),'M')
                    WHEN abs(toplineCurrentValue)>=1000D
                        THEN concat(regexp_replace(format_number(toplineCurrentValue/1000D,1),'\\.0$',''),'K')
                    ELSE format_number(toplineCurrentValue,0)
                END AS toplineCurrentDisplay,

                CASE
                    WHEN toplineAbsoluteDiffValue IS NULL THEN NULL
                    WHEN metricKind='ratio'
                        THEN concat(
                            CASE WHEN toplineAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            format_number(100D*toplineAbsoluteDiffValue,1),
                            'pp'
                        )
                    WHEN abs(toplineAbsoluteDiffValue)>=1000000000D
                        THEN concat(
                            CASE WHEN toplineAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(toplineAbsoluteDiffValue/1000000000D,1),'\\.0$',''),
                            'B'
                        )
                    WHEN abs(toplineAbsoluteDiffValue)>=1000000D
                        THEN concat(
                            CASE WHEN toplineAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(toplineAbsoluteDiffValue/1000000D,1),'\\.0$',''),
                            'M'
                        )
                    WHEN abs(toplineAbsoluteDiffValue)>=1000D
                        THEN concat(
                            CASE WHEN toplineAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(toplineAbsoluteDiffValue/1000D,1),'\\.0$',''),
                            'K'
                        )
                    ELSE concat(
                        CASE WHEN toplineAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                        format_number(toplineAbsoluteDiffValue,0)
                    )
                END AS toplineAbsoluteDiffDisplay,

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
                END AS toplineChangeDisplay
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

            sourcePairKey,
            sourcePairLabel,
            sourcePairSortOrder,

            pairKey,
            pairLabel,
            isSwappedOrientation,

            rowBreakoutType,
            rowBreakoutLabel,
            rowBreakoutSortOrder,

            columnBreakoutType,
            columnBreakoutLabel,
            columnBreakoutSortOrder,

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
            comparisonDataAvailable,
            comparisonWindowComplete,

            displaySize,
            displaySizeLabel,
            displayLimit,
            displaySizeSortOrder,

            rowBreakoutValue,
            rowDisplayRank,
            isRowOtherBucket,

            columnBreakoutValue,
            columnDisplayRank,
            isColumnOtherBucket,

            cellSortOrder,

            cellCurrentValue,
            cellCurrentValueDisplay,

            cellComparisonValue,
            cellComparisonValueDisplay,

            cellAbsoluteDiffValue,
            cellAbsoluteDiffDisplay,

            cellChangeValue,
            cellChangeDisplay,
            cellDirection,

            rowTotalCurrentValue,
            rowTotalCurrentDisplay,
            rowTotalAbsoluteDiffValue,
            rowTotalAbsoluteDiffDisplay,
            rowTotalChangeValue,
            rowTotalChangeDisplay,

            columnTotalCurrentValue,
            columnTotalCurrentDisplay,
            columnTotalAbsoluteDiffValue,
            columnTotalAbsoluteDiffDisplay,
            columnTotalChangeValue,
            columnTotalChangeDisplay,

            toplineCurrentValue,
            toplineCurrentDisplay,
            toplineAbsoluteDiffValue,
            toplineAbsoluteDiffDisplay,
            toplineChangeValue,
            toplineChangeDisplay,

            v_processedAt AS appProcessedAt
        FROM formatted;

        SELECT
            'SUCCESS' AS status,
            v_weekFrom AS rebuiltWeekStartFrom,
            v_weekTo AS rebuiltWeekStartTo,
            'priorWeek | fourWeek | lastYear' AS supportedComparisons,
            'top5 | top8 | top10 | all' AS supportedDisplaySizes,
            TRUE AS supportsSwappedAxes,
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsMatrix_long' AS targetObject,
            v_processedAt AS appProcessedAt;
    END IF;
END;

-- ============================================================================
-- DEVELOPMENT
-- ============================================================================

-- ONE TIME ONLY:
-- DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsMatrix_long;

-- Validate:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appCrosstabsMatrix_long(
--     p_asOfDate=>DATE '2026-09-28',
--     p_weeksToRebuild=>1,
--     p_validateOnly=>TRUE
-- );

-- Build:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appCrosstabsMatrix_long(
--     p_asOfDate=>DATE '2026-09-28',
--     p_weeksToRebuild=>12,
--     p_validateOnly=>FALSE
-- );

-- ============================================================================
-- SCREENSHOT API QUERY
--
-- Q3 2026
-- W7 / 9-15 Aug
-- Total UPV
-- Prior wk | 4-wk | Last yr
-- Rows    = Channel
-- Columns = Entry page
-- Size    = Top 8
-- ============================================================================

-- SELECT *
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsMatrix_long
-- WHERE fiscalYear=2026
--   AND fiscalQuarterLabel='Q3'
--   AND targetWeekStartDate=DATE '2026-08-09'
--   AND filterLob='All'
--   AND filterPlatform='All'
--   AND metricName='nbv'
--   AND rowBreakoutType='channel'
--   AND columnBreakoutType='entryPage'
--   AND comparisonType='fourWeek'
--   AND displaySize='top8'
-- ORDER BY rowDisplayRank,columnDisplayRank;

-- ============================================================================
-- ROWS / COLUMNS OPTIONS
--
-- API only reads what App Gold actually supports.
-- ============================================================================

-- SELECT DISTINCT
--     rowBreakoutType,
--     rowBreakoutLabel,
--     rowBreakoutSortOrder
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsMatrix_long
-- WHERE targetWeekStartDate=DATE '2026-08-09'
--   AND metricName='nbv'
-- ORDER BY rowBreakoutSortOrder;

-- Once Rows is selected:
--
-- SELECT DISTINCT
--     columnBreakoutType,
--     columnBreakoutLabel,
--     columnBreakoutSortOrder
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsMatrix_long
-- WHERE targetWeekStartDate=DATE '2026-08-09'
--   AND metricName='nbv'
--   AND rowBreakoutType='channel'
-- ORDER BY columnBreakoutSortOrder;

-- ============================================================================
-- PRE-BUILT CHIPS
--
-- Use original orientation only so each pair appears once.
-- ============================================================================

-- SELECT DISTINCT
--     sourcePairKey,
--     sourcePairLabel,
--     sourcePairSortOrder
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsMatrix_long
-- WHERE NOT isSwappedOrientation
-- ORDER BY sourcePairSortOrder;

-- ============================================================================
-- SIZE OPTIONS
-- ============================================================================

-- SELECT DISTINCT
--     displaySize,
--     displaySizeLabel,
--     displayLimit,
--     displaySizeSortOrder
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsMatrix_long
-- ORDER BY displaySizeSortOrder;

-- ============================================================================
-- MATRIX CELL EXAMPLE
--
-- Paid Search × /
--   -3.6K
--   -6.4%
-- ============================================================================

-- SELECT
--     rowBreakoutValue,
--     columnBreakoutValue,
--     cellAbsoluteDiffDisplay,
--     cellChangeDisplay,
--     cellDirection
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsMatrix_long
-- WHERE targetWeekStartDate=DATE '2026-08-09'
--   AND metricName='nbv'
--   AND rowBreakoutType='channel'
--   AND columnBreakoutType='entryPage'
--   AND comparisonType='fourWeek'
--   AND displaySize='top8'
-- ORDER BY rowDisplayRank,columnDisplayRank;

-- ============================================================================
-- ROW TOTALS
--
-- API can DISTINCT these fields per row; no calculations.
-- ============================================================================

-- SELECT DISTINCT
--     rowBreakoutValue,
--     rowDisplayRank,
--     rowTotalCurrentValue,
--     rowTotalCurrentDisplay,
--     rowTotalAbsoluteDiffValue,
--     rowTotalAbsoluteDiffDisplay,
--     rowTotalChangeValue,
--     rowTotalChangeDisplay
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsMatrix_long
-- WHERE targetWeekStartDate=DATE '2026-08-09'
--   AND metricName='nbv'
--   AND rowBreakoutType='channel'
--   AND columnBreakoutType='entryPage'
--   AND comparisonType='fourWeek'
--   AND displaySize='top8'
-- ORDER BY rowDisplayRank;

-- ============================================================================
-- COLUMN TOTALS
-- ============================================================================

-- SELECT DISTINCT
--     columnBreakoutValue,
--     columnDisplayRank,
--     columnTotalCurrentValue,
--     columnTotalCurrentDisplay,
--     columnTotalAbsoluteDiffValue,
--     columnTotalAbsoluteDiffDisplay,
--     columnTotalChangeValue,
--     columnTotalChangeDisplay
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsMatrix_long
-- WHERE targetWeekStartDate=DATE '2026-08-09'
--   AND metricName='nbv'
--   AND rowBreakoutType='channel'
--   AND columnBreakoutType='entryPage'
--   AND comparisonType='fourWeek'
--   AND displaySize='top8'
-- ORDER BY columnDisplayRank;

-- ============================================================================
-- BOTTOM-RIGHT TOPLINE
-- ============================================================================

-- SELECT DISTINCT
--     toplineCurrentValue,
--     toplineCurrentDisplay,
--     toplineAbsoluteDiffValue,
--     toplineAbsoluteDiffDisplay,
--     toplineChangeValue,
--     toplineChangeDisplay
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsMatrix_long
-- WHERE targetWeekStartDate=DATE '2026-08-09'
--   AND metricName='nbv'
--   AND rowBreakoutType='channel'
--   AND columnBreakoutType='entryPage'
--   AND comparisonType='fourWeek'
--   AND displaySize='top8';

-- ============================================================================
-- DUPLICATE CHECK
-- Expected zero.
-- ============================================================================

-- SELECT
--     targetWeekStartDate,
--     filterLob,
--     filterPlatform,
--     metricName,
--     pairKey,
--     comparisonType,
--     displaySize,
--     rowBreakoutValue,
--     columnBreakoutValue,
--     count(*) AS rowCount
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsMatrix_long
-- GROUP BY
--     targetWeekStartDate,
--     filterLob,
--     filterPlatform,
--     metricName,
--     pairKey,
--     comparisonType,
--     displaySize,
--     rowBreakoutValue,
--     columnBreakoutValue
-- HAVING count(*)>1;


[DELTA_CLUSTERING_COLUMN_MISSING_STATS] Liquid clustering requires clustering columns to have stats. Couldn't find clustering column(s) 'displaySize' in stats schema:
root
 |-- targetWeekStartDate: date (nullable = true)
 |-- targetWeekEndDate: date (nullable = true)
 |-- fiscalYear: integer (nullable = true)
 |-- fiscalQuarterLabel: string (nullable = true)
 |-- fiscalWeekCode: string (nullable = true)
 |-- weekLabel: string (nullable = true)
 |-- weekEndingLabel: string (nullable = true)
 |-- filterLob: string (nullable = true)
 |-- filterPlatform: string (nullable = true)
 |-- sourcePairKey: string (nullable = true)
 |-- sourcePairLabel: string (nullable = true)
 |-- sourcePairSortOrder: integer (nullable = true)
 |-- pairKey: string (nullable = true)
 |-- pairLabel: string (nullable = true)
 |-- isSwappedOrientation: boolean (nullable = true)
 |-- rowBreakoutType: string (nullable = true)
 |-- rowBreakoutLabel: string (nullable = true)
 |-- rowBreakoutSortOrder: integer (nullable = true)
 |-- columnBreakoutType: string (nullable = true)
 |-- columnBreakoutLabel: string (nullable = true)
 |-- columnBreakoutSortOrder: integer (nullable = true)
 |-- metricName: string (nullable = true)
 |-- metricLabel: string (nullable = true)
 |-- metricDescription: string (nullable = true)
 |-- metricKind: string (nullable = true)
 |-- displayFormat: string (nullable = true)
 |-- changeUnit: string (nullable = true)
 |-- metricSortOrder: integer (nullable = true)
 |-- comparisonType: string (nullable = true)
 |-- comparisonLabel: string (nullable = true)
 |-- comparisonSortOrder: integer (nullable = true)
 |-- comparisonDataAvailable: boolean (nullable = true)