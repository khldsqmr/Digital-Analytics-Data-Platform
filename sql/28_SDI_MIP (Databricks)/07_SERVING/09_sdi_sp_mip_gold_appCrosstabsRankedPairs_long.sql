-- ============================================================================
-- FILE  : 09_sdi_sp_mip_gold_appCrosstabsRankedPairs_long.sql
-- LAYER : GOLD / APP
-- TAB   : Crosstabs
-- PURPOSE:
--   Application-ready Every pair ranked contract, computed directly from analytical Gold.
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

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_gold_appCrosstabsRankedPairs_long(
    IN p_asOfDate       DATE    DEFAULT NULL,
    IN p_weeksToRebuild INT     DEFAULT 1,
    IN p_validateOnly   BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'MIP Gold app: Crosstabs Every pair ranked. Global comparator-aware ranking over All=Top100+Other pair cells.'
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
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long
        WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Crosstab Gold analytical ingredients has no rows for the requested app target-week range.';
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
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_crosstabCatalog_static
        WHERE isActive
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Crosstab catalog control view has no active pairs.';
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
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsRankedPairs_long' AS targetObject,
            'No Gold app table was created or modified.' AS message;

    ELSE

        -- --------------------------------------------------------------------
        -- 4. Bootstrap target schema only if the table does not exist.
        --    The zero-row CTAS keeps the target schema exactly aligned to the
        --    application contract without materialized-view/serverless compute.
        -- --------------------------------------------------------------------
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsRankedPairs_long
        USING DELTA
-- comparisonType intentionally excluded from liquid clustering because it may fall outside the default Delta stats schema.
        CLUSTER BY (targetWeekStartDate, pairKey, metricName)
        COMMENT 'MIP Gold app: Crosstabs Every pair ranked. Global comparator-aware ranking over All=Top100+Other pair cells.'
        AS
        SELECT *
        FROM (
            SELECT
                appResult.*,
                v_processedAt AS appProcessedAt
            FROM (
                WITH crosstabsMatrix AS (
                    WITH
                    scopeCrosstabs AS (
                        SELECT *
                        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long
                        WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
                    ),
                    scopeOverview AS (
                        SELECT *
                        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
                        WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
                    ),
                    base AS (
                        SELECT
                            g.targetWeekStartDate,g.targetWeekEndDate,g.fiscalQuarterLabel,g.fiscalWeekCode,g.weekLabel,
                            c.weekEndingLabel,c.priorWeekStartDate,c.fourWeekAvgStartDate,c.fourWeekAvgEndDate,c.sameWeekLastYearStartDate,
                            g.filterLob,g.filterPlatform,
                            g.pairKey,g.pairLabel,pc.sortOrder AS pairSortOrder,
                            g.rowBreakoutType,g.rowBreakoutValue,rb.pairTopN AS configuredRowPairTopN,
                            g.columnBreakoutType,g.columnBreakoutValue,cb.pairTopN AS configuredColumnPairTopN,
                            g.metricName,g.metricLabel,mc.metricDescription,g.metricKind,g.displayFormat,g.changeUnit,
                            mc.definitionStatus AS metricDefinitionStatus,mc.sortOrder AS metricSortOrder,
                            g.thisWeekNumerator,g.thisWeekDenominator,g.priorWeekNumerator,g.priorWeekDenominator,
                            g.fourWeekTrendNumerator,g.fourWeekTrendDenominator,g.sameWeekLyNumerator,g.sameWeekLyDenominator,
                            g.peerSetNumerator,g.peerSetDenominator,
                            g.thisWeekDataAvailable,g.priorWeekDataAvailable,g.fourWeekTrendWeekCount,g.sameWeekLyDataAvailable,
                            g.goldProcessedAt
                        FROM scopeCrosstabs g
                        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_crosstabCatalog_static pc ON pc.pairKey=g.pairKey AND pc.isActive
                        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static rb ON rb.breakoutType=g.rowBreakoutType AND rb.isActive
                        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static cb ON cb.breakoutType=g.columnBreakoutType AND cb.isActive
                        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc ON mc.metricName=g.metricName AND mc.isActive
                        LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c ON c.weekStartDate=g.targetWeekStartDate
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
                        SELECT targetWeekStartDate,filterLob,filterPlatform,metricName,metricKind,
                               thisWeekNumerator,thisWeekDenominator,priorWeekNumerator,priorWeekDenominator,
                               fourWeekTrendNumerator,fourWeekTrendDenominator,sameWeekLyNumerator,sameWeekLyDenominator,fourWeekTrendWeekCount
                        FROM scopeOverview
                    ),
                    toplineLong AS (
                        SELECT *, 'priorWeek' AS comparisonType, priorWeekNumerator AS comparisonNumerator, priorWeekDenominator AS comparisonDenominator FROM toplineBase
                        UNION ALL
                        SELECT *, 'fourWeek' AS comparisonType,
                               CASE WHEN metricKind='count' AND fourWeekTrendWeekCount>0 THEN try_divide(fourWeekTrendNumerator,cast(fourWeekTrendWeekCount AS DOUBLE)) ELSE fourWeekTrendNumerator END,
                               CASE WHEN metricKind='count' THEN NULL ELSE fourWeekTrendDenominator END
                        FROM toplineBase
                        UNION ALL
                        SELECT *, 'lastYear' AS comparisonType, sameWeekLyNumerator, sameWeekLyDenominator FROM toplineBase
                    ),
                    toplineValues AS (
                        SELECT *,
                               CASE WHEN metricKind='ratio' THEN try_divide(thisWeekNumerator,thisWeekDenominator) ELSE thisWeekNumerator END AS toplineCurrentValue,
                               CASE WHEN metricKind='ratio' THEN try_divide(comparisonNumerator,comparisonDenominator) ELSE comparisonNumerator END AS toplineComparisonValue
                        FROM toplineLong
                    ),
                    cellValues AS (
                        SELECT
                            p.*,
                            t.toplineCurrentValue,t.toplineComparisonValue,
                            t.thisWeekDenominator AS toplineCurrentDenominator,t.comparisonDenominator AS toplineComparisonDenominator,
                            CASE WHEN p.metricKind='count' THEN 100D*try_divide(p.absoluteDeltaValue,t.toplineComparisonValue)
                                 WHEN p.metricKind='ratio' THEN 100D*(try_divide(p.thisWeekNumerator,t.thisWeekDenominator)-try_divide(p.comparisonNumerator,t.comparisonDenominator)) END AS impactOnToplineValue,
                            CASE WHEN p.metricKind='count' THEN 'pct' WHEN p.metricKind='ratio' THEN 'pp' END AS impactOnToplineUnit,
                            p.currentValue-p.peerSetValue AS peerSetAbsoluteDeltaValue,
                            CASE WHEN NOT p.peerSetDataAvailable THEN NULL
                                 WHEN p.changeUnit='pp' THEN 100D*(p.currentValue-p.peerSetValue)
                                 WHEN p.changeUnit='pct' THEN 100D*(try_divide(p.currentValue,p.peerSetValue)-1D) END AS peerSetChangeValue
                        FROM peerCalculated p
                        LEFT JOIN toplineValues t
                          ON t.targetWeekStartDate=p.targetWeekStartDate AND t.filterLob=p.filterLob AND t.filterPlatform=p.filterPlatform
                         AND t.metricName=p.metricName AND t.comparisonType=p.comparisonType
                    ),
                    rowAgg AS (
                        SELECT
                            targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType,rowBreakoutValue,metricKind,
                            sum(thisWeekNumerator) AS currentNumerator,sum(thisWeekDenominator) AS currentDenominator,
                            sum(comparisonNumerator) AS comparisonNumerator,sum(comparisonDenominator) AS comparisonDenominator
                        FROM cellValues
                        GROUP BY targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType,rowBreakoutValue,metricKind
                    ),
                    rowValues AS (
                        SELECT *,
                            CASE WHEN metricKind='ratio' THEN try_divide(currentNumerator,currentDenominator) ELSE currentNumerator END AS rowCurrentValue,
                            CASE WHEN metricKind='ratio' THEN try_divide(comparisonNumerator,comparisonDenominator) ELSE comparisonNumerator END AS rowComparisonValue
                        FROM rowAgg
                    ),
                    rowRanks AS (
                        SELECT *, row_number() OVER (
                            PARTITION BY targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType
                            ORDER BY rowCurrentValue DESC NULLS LAST,rowBreakoutValue
                        ) AS rowRankByMetric
                        FROM rowValues
                    ),
                    columnAgg AS (
                        SELECT
                            targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType,columnBreakoutValue,metricKind,
                            sum(thisWeekNumerator) AS currentNumerator,sum(thisWeekDenominator) AS currentDenominator,
                            sum(comparisonNumerator) AS comparisonNumerator,sum(comparisonDenominator) AS comparisonDenominator
                        FROM cellValues
                        GROUP BY targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType,columnBreakoutValue,metricKind
                    ),
                    columnValues AS (
                        SELECT *,
                            CASE WHEN metricKind='ratio' THEN try_divide(currentNumerator,currentDenominator) ELSE currentNumerator END AS columnCurrentValue,
                            CASE WHEN metricKind='ratio' THEN try_divide(comparisonNumerator,comparisonDenominator) ELSE comparisonNumerator END AS columnComparisonValue
                        FROM columnAgg
                    ),
                    columnRanks AS (
                        SELECT *, row_number() OVER (
                            PARTITION BY targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType
                            ORDER BY columnCurrentValue DESC NULLS LAST,columnBreakoutValue
                        ) AS columnRankByMetric
                        FROM columnValues
                    ),
                    ranked AS (
                        SELECT
                            c.*,
                            r.rowCurrentValue,r.rowComparisonValue,r.rowRankByMetric,
                            k.columnCurrentValue,k.columnComparisonValue,k.columnRankByMetric,
                            CASE WHEN c.comparisonDataAvailable AND c.impactOnToplineValue IS NOT NULL THEN
                                row_number() OVER (
                                    PARTITION BY c.targetWeekStartDate,c.filterLob,c.filterPlatform,c.pairKey,c.metricName,c.comparisonType
                                    ORDER BY abs(c.impactOnToplineValue) DESC NULLS LAST,abs(c.absoluteDeltaValue) DESC NULLS LAST,
                                             c.rowBreakoutValue,c.columnBreakoutValue
                                )
                            END AS cellImpactRankWithinPair,
                            CASE WHEN c.comparisonDataAvailable AND c.impactOnToplineValue IS NOT NULL THEN
                                row_number() OVER (
                                    PARTITION BY c.targetWeekStartDate,c.filterLob,c.filterPlatform,c.metricName,c.comparisonType
                                    ORDER BY abs(c.impactOnToplineValue) DESC NULLS LAST,abs(c.absoluteDeltaValue) DESC NULLS LAST,
                                             c.pairKey,c.rowBreakoutValue,c.columnBreakoutValue
                                )
                            END AS cellImpactRankAcrossPairs
                        FROM cellValues c
                        LEFT JOIN rowRanks r
                          ON r.targetWeekStartDate=c.targetWeekStartDate AND r.filterLob=c.filterLob AND r.filterPlatform=c.filterPlatform
                         AND r.pairKey=c.pairKey AND r.metricName=c.metricName AND r.comparisonType=c.comparisonType
                         AND r.rowBreakoutValue=c.rowBreakoutValue
                        LEFT JOIN columnRanks k
                          ON k.targetWeekStartDate=c.targetWeekStartDate AND k.filterLob=c.filterLob AND k.filterPlatform=c.filterPlatform
                         AND k.pairKey=c.pairKey AND k.metricName=c.metricName AND k.comparisonType=c.comparisonType
                         AND k.columnBreakoutValue=c.columnBreakoutValue
                    )
                    ,
                    sizeConfig AS (
                        SELECT * FROM VALUES
                            ('top5',  'Top 5',  5,   10),
                            ('top8',  'Top 8',  8,   20),
                            ('top10', 'Top 10', 10,  30),
                            ('all',   'All',    100, 40)
                        AS s(displaySize, displaySizeLabel, displayLimit, displaySizeSortOrder)
                    ),
                    expanded AS (
                        SELECT
                            r.*,
                            s.displaySize,
                            s.displaySizeLabel,
                            s.displayLimit,
                            s.displaySizeSortOrder,

                            CASE
                                WHEN rowRankByMetric <= s.displayLimit
                                    THEN concat('ROW::VALUE::', coalesce(rowBreakoutValue, '(null)'))
                                ELSE 'ROW::OTHER::REMAINDER'
                            END AS rowBucketKey,
                            CASE
                                WHEN rowRankByMetric <= s.displayLimit THEN rowBreakoutValue
                                ELSE '(Other)'
                            END AS displayRowBreakoutValue,
                            rowRankByMetric > s.displayLimit AS rowOtherMember,

                            CASE
                                WHEN columnRankByMetric <= s.displayLimit
                                    THEN concat('COL::VALUE::', coalesce(columnBreakoutValue, '(null)'))
                                ELSE 'COL::OTHER::REMAINDER'
                            END AS columnBucketKey,
                            CASE
                                WHEN columnRankByMetric <= s.displayLimit THEN columnBreakoutValue
                                ELSE '(Other)'
                            END AS displayColumnBreakoutValue,
                            columnRankByMetric > s.displayLimit AS columnOtherMember
                        FROM ranked r
                        CROSS JOIN sizeConfig s
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

                            pairKey,
                            pairLabel,
                            pairSortOrder,
                            rowBreakoutType,
                            columnBreakoutType,
                            configuredRowPairTopN,
                            configuredColumnPairTopN,

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

                            displaySize,
                            displaySizeLabel,
                            displayLimit,
                            displaySizeSortOrder,

                            rowBucketKey,
                            displayRowBreakoutValue AS rowBreakoutValue,
                            max(CASE WHEN rowOtherMember THEN 1 ELSE 0 END) = 1 AS isRowOtherBucket,
                            CASE
                                WHEN max(CASE WHEN rowOtherMember THEN 1 ELSE 0 END) = 1 THEN displayLimit + 1
                                ELSE min(rowRankByMetric)
                            END AS rowDisplayRank,

                            columnBucketKey,
                            displayColumnBreakoutValue AS columnBreakoutValue,
                            max(CASE WHEN columnOtherMember THEN 1 ELSE 0 END) = 1 AS isColumnOtherBucket,
                            CASE
                                WHEN max(CASE WHEN columnOtherMember THEN 1 ELSE 0 END) = 1 THEN displayLimit + 1
                                ELSE min(columnRankByMetric)
                            END AS columnDisplayRank,

                            count(*) AS rawCellMemberCount,

                            sum(thisWeekNumerator) AS currentNumerator,
                            sum(thisWeekDenominator) AS currentDenominator,
                            sum(comparisonNumerator) AS comparisonNumerator,
                            sum(comparisonDenominator) AS comparisonDenominator,

                            sum(peerSetNumerator) AS peerSetNumerator,
                            sum(peerSetDenominator) AS peerSetDenominator,

                            max(toplineCurrentValue) AS toplineCurrentValue,
                            max(toplineComparisonValue) AS toplineComparisonValue,
                            max(toplineCurrentDenominator) AS toplineCurrentDenominator,
                            max(toplineComparisonDenominator) AS toplineComparisonDenominator,

                            thisWeekDataAvailable,
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
                            pairKey,
                            pairLabel,
                            pairSortOrder,
                            rowBreakoutType,
                            columnBreakoutType,
                            configuredRowPairTopN,
                            configuredColumnPairTopN,
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
                            displaySize,
                            displaySizeLabel,
                            displayLimit,
                            displaySizeSortOrder,
                            rowBucketKey,
                            displayRowBreakoutValue,
                            columnBucketKey,
                            displayColumnBreakoutValue,
                            thisWeekDataAvailable
                    ),
                    bucketValues AS (
                        SELECT
                            a.*,
                            CASE WHEN metricKind='ratio' THEN try_divide(currentNumerator,currentDenominator)
                                 ELSE currentNumerator END AS currentValue,
                            CASE WHEN metricKind='ratio' THEN try_divide(comparisonNumerator,comparisonDenominator)
                                 ELSE comparisonNumerator END AS comparisonValue,
                            CASE WHEN metricKind='ratio' THEN try_divide(peerSetNumerator,peerSetDenominator)
                                 ELSE peerSetNumerator END AS peerSetValue,
                            CASE WHEN metricKind='ratio'
                                      THEN peerSetNumerator IS NOT NULL AND nullif(peerSetDenominator,0D) IS NOT NULL
                                 ELSE peerSetNumerator IS NOT NULL END AS peerSetDataAvailable
                        FROM bucketAgg a
                    ),
                    bucketCalculated AS (
                        SELECT
                            v.*,
                            currentValue-comparisonValue AS absoluteDeltaValue,
                            CASE
                                WHEN NOT thisWeekDataAvailable OR NOT comparisonDataAvailable
                                  OR currentValue IS NULL OR comparisonValue IS NULL THEN NULL
                                WHEN changeUnit='pp' THEN 100D*(currentValue-comparisonValue)
                                WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,comparisonValue)-1D)
                                ELSE NULL
                            END AS changeValue,
                            CASE
                                WHEN currentValue IS NULL OR comparisonValue IS NULL THEN 'unavailable'
                                WHEN currentValue > comparisonValue THEN 'up'
                                WHEN currentValue < comparisonValue THEN 'down'
                                ELSE 'flat'
                            END AS changeDirection,
                            CASE
                                WHEN metricKind='count' THEN 100D*try_divide(currentValue-comparisonValue,toplineComparisonValue)
                                WHEN metricKind='ratio' THEN 100D*(
                                    try_divide(currentNumerator,toplineCurrentDenominator)
                                    - try_divide(comparisonNumerator,toplineComparisonDenominator)
                                )
                                ELSE NULL
                            END AS impactOnToplineValue,
                            CASE WHEN metricKind='count' THEN 'pct'
                                 WHEN metricKind='ratio' THEN 'pp'
                                 ELSE NULL END AS impactOnToplineUnit,
                            currentValue-peerSetValue AS peerSetAbsoluteDeltaValue,
                            CASE
                                WHEN NOT peerSetDataAvailable OR currentValue IS NULL THEN NULL
                                WHEN changeUnit='pp' THEN 100D*(currentValue-peerSetValue)
                                WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,peerSetValue)-1D)
                                ELSE NULL
                            END AS peerSetChangeValue
                        FROM bucketValues v
                    ),
                    rowAggDisplay AS (
                        SELECT
                            targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType,displaySize,
                            rowBreakoutValue,rowDisplayRank,isRowOtherBucket,metricKind,
                            sum(currentNumerator) AS rowCurrentNumerator,
                            sum(currentDenominator) AS rowCurrentDenominator,
                            sum(comparisonNumerator) AS rowComparisonNumerator,
                            sum(comparisonDenominator) AS rowComparisonDenominator
                        FROM bucketCalculated
                        GROUP BY
                            targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType,displaySize,
                            rowBreakoutValue,rowDisplayRank,isRowOtherBucket,metricKind
                    ),
                    rowValuesDisplay AS (
                        SELECT
                            *,
                            CASE WHEN metricKind='ratio' THEN try_divide(rowCurrentNumerator,rowCurrentDenominator)
                                 ELSE rowCurrentNumerator END AS rowCurrentValue,
                            CASE WHEN metricKind='ratio' THEN try_divide(rowComparisonNumerator,rowComparisonDenominator)
                                 ELSE rowComparisonNumerator END AS rowComparisonValue
                        FROM rowAggDisplay
                    ),
                    columnAggDisplay AS (
                        SELECT
                            targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType,displaySize,
                            columnBreakoutValue,columnDisplayRank,isColumnOtherBucket,metricKind,
                            sum(currentNumerator) AS columnCurrentNumerator,
                            sum(currentDenominator) AS columnCurrentDenominator,
                            sum(comparisonNumerator) AS columnComparisonNumerator,
                            sum(comparisonDenominator) AS columnComparisonDenominator
                        FROM bucketCalculated
                        GROUP BY
                            targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType,displaySize,
                            columnBreakoutValue,columnDisplayRank,isColumnOtherBucket,metricKind
                    ),
                    columnValuesDisplay AS (
                        SELECT
                            *,
                            CASE WHEN metricKind='ratio' THEN try_divide(columnCurrentNumerator,columnCurrentDenominator)
                                 ELSE columnCurrentNumerator END AS columnCurrentValue,
                            CASE WHEN metricKind='ratio' THEN try_divide(columnComparisonNumerator,columnComparisonDenominator)
                                 ELSE columnComparisonNumerator END AS columnComparisonValue
                        FROM columnAggDisplay
                    ),
                    withTotals AS (
                        SELECT
                            c.*,
                            r.rowCurrentValue,
                            r.rowComparisonValue,
                            k.columnCurrentValue,
                            k.columnComparisonValue
                        FROM bucketCalculated c
                        LEFT JOIN rowValuesDisplay r
                          ON r.targetWeekStartDate=c.targetWeekStartDate
                         AND r.filterLob=c.filterLob
                         AND r.filterPlatform=c.filterPlatform
                         AND r.pairKey=c.pairKey
                         AND r.metricName=c.metricName
                         AND r.comparisonType=c.comparisonType
                         AND r.displaySize=c.displaySize
                         AND r.rowBreakoutValue=c.rowBreakoutValue
                         AND r.rowDisplayRank=c.rowDisplayRank
                        LEFT JOIN columnValuesDisplay k
                          ON k.targetWeekStartDate=c.targetWeekStartDate
                         AND k.filterLob=c.filterLob
                         AND k.filterPlatform=c.filterPlatform
                         AND k.pairKey=c.pairKey
                         AND k.metricName=c.metricName
                         AND k.comparisonType=c.comparisonType
                         AND k.displaySize=c.displaySize
                         AND k.columnBreakoutValue=c.columnBreakoutValue
                         AND k.columnDisplayRank=c.columnDisplayRank
                    ),
                    finalRanked AS (
                        SELECT
                            w.*,
                            row_number() OVER (
                                PARTITION BY targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType,displaySize
                                ORDER BY abs(impactOnToplineValue) DESC NULLS LAST,
                                         abs(absoluteDeltaValue) DESC NULLS LAST,
                                         rowBreakoutValue,columnBreakoutValue
                            ) AS cellImpactRankWithinPair,
                            row_number() OVER (
                                PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName,comparisonType,displaySize
                                ORDER BY abs(impactOnToplineValue) DESC NULLS LAST,
                                         abs(absoluteDeltaValue) DESC NULLS LAST,
                                         pairKey,rowBreakoutValue,columnBreakoutValue
                            ) AS cellImpactRankAcrossPairs
                        FROM withTotals w
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

                        pairKey,
                        pairLabel,
                        pairSortOrder,

                        displaySize,
                        displaySizeLabel,
                        displayLimit,
                        displaySizeSortOrder,

                        rowBreakoutType,
                        rowBreakoutValue,
                        rowDisplayRank,
                        isRowOtherBucket,
                        rowCurrentValue,
                        rowComparisonValue,
                        configuredRowPairTopN,

                        columnBreakoutType,
                        columnBreakoutValue,
                        columnDisplayRank,
                        isColumnOtherBucket,
                        columnCurrentValue,
                        columnComparisonValue,
                        configuredColumnPairTopN,

                        rawCellMemberCount,

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

                        toplineCurrentValue,
                        toplineComparisonValue,
                        impactOnToplineValue,
                        impactOnToplineUnit,

                        cellImpactRankWithinPair,
                        cellImpactRankAcrossPairs,

                        peerSetValue,
                        peerSetAbsoluteDeltaValue,
                        peerSetChangeValue,
                        peerSetDataAvailable,

                        thisWeekDataAvailable,
                        goldProcessedAt
                    FROM finalRanked
                ),
                ranked AS (
                    SELECT
                        m.*,
                        row_number() OVER (
                            PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName,comparisonType
                            ORDER BY
                                abs(impactOnToplineValue) DESC NULLS LAST,
                                abs(absoluteDeltaValue) DESC NULLS LAST,
                                pairKey,rowBreakoutValue,columnBreakoutValue
                        ) AS globalIntersectionImpactRank,
                        count(*) OVER (
                            PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName,comparisonType
                        ) AS candidateIntersectionCount
                    FROM crosstabsMatrix m
                    WHERE displaySize='all'
                      AND comparisonDataAvailable
                      AND impactOnToplineValue IS NOT NULL
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

                    pairKey,
                    pairLabel,
                    pairSortOrder,

                    rowBreakoutType,
                    rowBreakoutValue,
                    rowDisplayRank,
                    isRowOtherBucket,
                    columnBreakoutType,
                    columnBreakoutValue,
                    columnDisplayRank,
                    isColumnOtherBucket,
                    concat(rowBreakoutValue,' × ',columnBreakoutValue) AS intersectionLabel,

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
                    comparisonStartDate,
                    comparisonEndDate,
                    comparisonWeekCount,
                    comparisonDataAvailable,
                    comparisonWindowComplete,

                    currentValue,
                    comparisonValue,
                    absoluteDeltaValue,
                    changeValue,
                    changeDirection,

                    toplineCurrentValue,
                    toplineComparisonValue,
                    impactOnToplineValue,
                    impactOnToplineUnit,

                    cellImpactRankWithinPair,
                    globalIntersectionImpactRank AS cellImpactRankAcrossPairs,

                    coalesce(globalIntersectionImpactRank<=5,FALSE) AS isTop5,
                    coalesce(globalIntersectionImpactRank<=10,FALSE) AS isTop10,
                    coalesce(globalIntersectionImpactRank<=18,FALSE) AS isTop18,
                    coalesce(globalIntersectionImpactRank<=100,FALSE) AS isTop100,

                    candidateIntersectionCount,
                    greatest(candidateIntersectionCount-100,0) AS allSuppressedIntersectionCount,
                    100 AS allSelectionLimit,
                    'All = Top 100 globally ranked intersections. Row/column (Other) buckets are already scoped within each pair; no cross-pair numeric Other is created.' AS allSelectionRule,

                    peerSetValue,
                    peerSetChangeValue,
                    peerSetDataAvailable,

                    goldProcessedAt
                FROM ranked
            ) appResult
        ) schemaBootstrap
        WHERE 1 = 0;

        -- --------------------------------------------------------------------
        -- 5. Rebuild requested whole target-week range.
        --    Whole-week replacement is intentional because comparator ranks,
        --    Top-N membership and (Other) buckets can all change together.
        -- --------------------------------------------------------------------
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsRankedPairs_long
        REPLACE WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
SELECT
    appResult.*,
    v_processedAt AS appProcessedAt
FROM (
    WITH crosstabsMatrix AS (
        WITH
        scopeCrosstabs AS (
            SELECT *
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long
            WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        ),
        scopeOverview AS (
            SELECT *
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
            WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        ),
        base AS (
            SELECT
                g.targetWeekStartDate,g.targetWeekEndDate,g.fiscalQuarterLabel,g.fiscalWeekCode,g.weekLabel,
                c.weekEndingLabel,c.priorWeekStartDate,c.fourWeekAvgStartDate,c.fourWeekAvgEndDate,c.sameWeekLastYearStartDate,
                g.filterLob,g.filterPlatform,
                g.pairKey,g.pairLabel,pc.sortOrder AS pairSortOrder,
                g.rowBreakoutType,g.rowBreakoutValue,rb.pairTopN AS configuredRowPairTopN,
                g.columnBreakoutType,g.columnBreakoutValue,cb.pairTopN AS configuredColumnPairTopN,
                g.metricName,g.metricLabel,mc.metricDescription,g.metricKind,g.displayFormat,g.changeUnit,
                mc.definitionStatus AS metricDefinitionStatus,mc.sortOrder AS metricSortOrder,
                g.thisWeekNumerator,g.thisWeekDenominator,g.priorWeekNumerator,g.priorWeekDenominator,
                g.fourWeekTrendNumerator,g.fourWeekTrendDenominator,g.sameWeekLyNumerator,g.sameWeekLyDenominator,
                g.peerSetNumerator,g.peerSetDenominator,
                g.thisWeekDataAvailable,g.priorWeekDataAvailable,g.fourWeekTrendWeekCount,g.sameWeekLyDataAvailable,
                g.goldProcessedAt
            FROM scopeCrosstabs g
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_crosstabCatalog_static pc ON pc.pairKey=g.pairKey AND pc.isActive
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static rb ON rb.breakoutType=g.rowBreakoutType AND rb.isActive
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static cb ON cb.breakoutType=g.columnBreakoutType AND cb.isActive
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc ON mc.metricName=g.metricName AND mc.isActive
            LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c ON c.weekStartDate=g.targetWeekStartDate
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
            SELECT targetWeekStartDate,filterLob,filterPlatform,metricName,metricKind,
                   thisWeekNumerator,thisWeekDenominator,priorWeekNumerator,priorWeekDenominator,
                   fourWeekTrendNumerator,fourWeekTrendDenominator,sameWeekLyNumerator,sameWeekLyDenominator,fourWeekTrendWeekCount
            FROM scopeOverview
        ),
        toplineLong AS (
            SELECT *, 'priorWeek' AS comparisonType, priorWeekNumerator AS comparisonNumerator, priorWeekDenominator AS comparisonDenominator FROM toplineBase
            UNION ALL
            SELECT *, 'fourWeek' AS comparisonType,
                   CASE WHEN metricKind='count' AND fourWeekTrendWeekCount>0 THEN try_divide(fourWeekTrendNumerator,cast(fourWeekTrendWeekCount AS DOUBLE)) ELSE fourWeekTrendNumerator END,
                   CASE WHEN metricKind='count' THEN NULL ELSE fourWeekTrendDenominator END
            FROM toplineBase
            UNION ALL
            SELECT *, 'lastYear' AS comparisonType, sameWeekLyNumerator, sameWeekLyDenominator FROM toplineBase
        ),
        toplineValues AS (
            SELECT *,
                   CASE WHEN metricKind='ratio' THEN try_divide(thisWeekNumerator,thisWeekDenominator) ELSE thisWeekNumerator END AS toplineCurrentValue,
                   CASE WHEN metricKind='ratio' THEN try_divide(comparisonNumerator,comparisonDenominator) ELSE comparisonNumerator END AS toplineComparisonValue
            FROM toplineLong
        ),
        cellValues AS (
            SELECT
                p.*,
                t.toplineCurrentValue,t.toplineComparisonValue,
                t.thisWeekDenominator AS toplineCurrentDenominator,t.comparisonDenominator AS toplineComparisonDenominator,
                CASE WHEN p.metricKind='count' THEN 100D*try_divide(p.absoluteDeltaValue,t.toplineComparisonValue)
                     WHEN p.metricKind='ratio' THEN 100D*(try_divide(p.thisWeekNumerator,t.thisWeekDenominator)-try_divide(p.comparisonNumerator,t.comparisonDenominator)) END AS impactOnToplineValue,
                CASE WHEN p.metricKind='count' THEN 'pct' WHEN p.metricKind='ratio' THEN 'pp' END AS impactOnToplineUnit,
                p.currentValue-p.peerSetValue AS peerSetAbsoluteDeltaValue,
                CASE WHEN NOT p.peerSetDataAvailable THEN NULL
                     WHEN p.changeUnit='pp' THEN 100D*(p.currentValue-p.peerSetValue)
                     WHEN p.changeUnit='pct' THEN 100D*(try_divide(p.currentValue,p.peerSetValue)-1D) END AS peerSetChangeValue
            FROM peerCalculated p
            LEFT JOIN toplineValues t
              ON t.targetWeekStartDate=p.targetWeekStartDate AND t.filterLob=p.filterLob AND t.filterPlatform=p.filterPlatform
             AND t.metricName=p.metricName AND t.comparisonType=p.comparisonType
        ),
        rowAgg AS (
            SELECT
                targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType,rowBreakoutValue,metricKind,
                sum(thisWeekNumerator) AS currentNumerator,sum(thisWeekDenominator) AS currentDenominator,
                sum(comparisonNumerator) AS comparisonNumerator,sum(comparisonDenominator) AS comparisonDenominator
            FROM cellValues
            GROUP BY targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType,rowBreakoutValue,metricKind
        ),
        rowValues AS (
            SELECT *,
                CASE WHEN metricKind='ratio' THEN try_divide(currentNumerator,currentDenominator) ELSE currentNumerator END AS rowCurrentValue,
                CASE WHEN metricKind='ratio' THEN try_divide(comparisonNumerator,comparisonDenominator) ELSE comparisonNumerator END AS rowComparisonValue
            FROM rowAgg
        ),
        rowRanks AS (
            SELECT *, row_number() OVER (
                PARTITION BY targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType
                ORDER BY rowCurrentValue DESC NULLS LAST,rowBreakoutValue
            ) AS rowRankByMetric
            FROM rowValues
        ),
        columnAgg AS (
            SELECT
                targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType,columnBreakoutValue,metricKind,
                sum(thisWeekNumerator) AS currentNumerator,sum(thisWeekDenominator) AS currentDenominator,
                sum(comparisonNumerator) AS comparisonNumerator,sum(comparisonDenominator) AS comparisonDenominator
            FROM cellValues
            GROUP BY targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType,columnBreakoutValue,metricKind
        ),
        columnValues AS (
            SELECT *,
                CASE WHEN metricKind='ratio' THEN try_divide(currentNumerator,currentDenominator) ELSE currentNumerator END AS columnCurrentValue,
                CASE WHEN metricKind='ratio' THEN try_divide(comparisonNumerator,comparisonDenominator) ELSE comparisonNumerator END AS columnComparisonValue
            FROM columnAgg
        ),
        columnRanks AS (
            SELECT *, row_number() OVER (
                PARTITION BY targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType
                ORDER BY columnCurrentValue DESC NULLS LAST,columnBreakoutValue
            ) AS columnRankByMetric
            FROM columnValues
        ),
        ranked AS (
            SELECT
                c.*,
                r.rowCurrentValue,r.rowComparisonValue,r.rowRankByMetric,
                k.columnCurrentValue,k.columnComparisonValue,k.columnRankByMetric,
                CASE WHEN c.comparisonDataAvailable AND c.impactOnToplineValue IS NOT NULL THEN
                    row_number() OVER (
                        PARTITION BY c.targetWeekStartDate,c.filterLob,c.filterPlatform,c.pairKey,c.metricName,c.comparisonType
                        ORDER BY abs(c.impactOnToplineValue) DESC NULLS LAST,abs(c.absoluteDeltaValue) DESC NULLS LAST,
                                 c.rowBreakoutValue,c.columnBreakoutValue
                    )
                END AS cellImpactRankWithinPair,
                CASE WHEN c.comparisonDataAvailable AND c.impactOnToplineValue IS NOT NULL THEN
                    row_number() OVER (
                        PARTITION BY c.targetWeekStartDate,c.filterLob,c.filterPlatform,c.metricName,c.comparisonType
                        ORDER BY abs(c.impactOnToplineValue) DESC NULLS LAST,abs(c.absoluteDeltaValue) DESC NULLS LAST,
                                 c.pairKey,c.rowBreakoutValue,c.columnBreakoutValue
                    )
                END AS cellImpactRankAcrossPairs
            FROM cellValues c
            LEFT JOIN rowRanks r
              ON r.targetWeekStartDate=c.targetWeekStartDate AND r.filterLob=c.filterLob AND r.filterPlatform=c.filterPlatform
             AND r.pairKey=c.pairKey AND r.metricName=c.metricName AND r.comparisonType=c.comparisonType
             AND r.rowBreakoutValue=c.rowBreakoutValue
            LEFT JOIN columnRanks k
              ON k.targetWeekStartDate=c.targetWeekStartDate AND k.filterLob=c.filterLob AND k.filterPlatform=c.filterPlatform
             AND k.pairKey=c.pairKey AND k.metricName=c.metricName AND k.comparisonType=c.comparisonType
             AND k.columnBreakoutValue=c.columnBreakoutValue
        )
        ,
        sizeConfig AS (
            SELECT * FROM VALUES
                ('top5',  'Top 5',  5,   10),
                ('top8',  'Top 8',  8,   20),
                ('top10', 'Top 10', 10,  30),
                ('all',   'All',    100, 40)
            AS s(displaySize, displaySizeLabel, displayLimit, displaySizeSortOrder)
        ),
        expanded AS (
            SELECT
                r.*,
                s.displaySize,
                s.displaySizeLabel,
                s.displayLimit,
                s.displaySizeSortOrder,

                CASE
                    WHEN rowRankByMetric <= s.displayLimit
                        THEN concat('ROW::VALUE::', coalesce(rowBreakoutValue, '(null)'))
                    ELSE 'ROW::OTHER::REMAINDER'
                END AS rowBucketKey,
                CASE
                    WHEN rowRankByMetric <= s.displayLimit THEN rowBreakoutValue
                    ELSE '(Other)'
                END AS displayRowBreakoutValue,
                rowRankByMetric > s.displayLimit AS rowOtherMember,

                CASE
                    WHEN columnRankByMetric <= s.displayLimit
                        THEN concat('COL::VALUE::', coalesce(columnBreakoutValue, '(null)'))
                    ELSE 'COL::OTHER::REMAINDER'
                END AS columnBucketKey,
                CASE
                    WHEN columnRankByMetric <= s.displayLimit THEN columnBreakoutValue
                    ELSE '(Other)'
                END AS displayColumnBreakoutValue,
                columnRankByMetric > s.displayLimit AS columnOtherMember
            FROM ranked r
            CROSS JOIN sizeConfig s
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

                pairKey,
                pairLabel,
                pairSortOrder,
                rowBreakoutType,
                columnBreakoutType,
                configuredRowPairTopN,
                configuredColumnPairTopN,

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

                displaySize,
                displaySizeLabel,
                displayLimit,
                displaySizeSortOrder,

                rowBucketKey,
                displayRowBreakoutValue AS rowBreakoutValue,
                max(CASE WHEN rowOtherMember THEN 1 ELSE 0 END) = 1 AS isRowOtherBucket,
                CASE
                    WHEN max(CASE WHEN rowOtherMember THEN 1 ELSE 0 END) = 1 THEN displayLimit + 1
                    ELSE min(rowRankByMetric)
                END AS rowDisplayRank,

                columnBucketKey,
                displayColumnBreakoutValue AS columnBreakoutValue,
                max(CASE WHEN columnOtherMember THEN 1 ELSE 0 END) = 1 AS isColumnOtherBucket,
                CASE
                    WHEN max(CASE WHEN columnOtherMember THEN 1 ELSE 0 END) = 1 THEN displayLimit + 1
                    ELSE min(columnRankByMetric)
                END AS columnDisplayRank,

                count(*) AS rawCellMemberCount,

                sum(thisWeekNumerator) AS currentNumerator,
                sum(thisWeekDenominator) AS currentDenominator,
                sum(comparisonNumerator) AS comparisonNumerator,
                sum(comparisonDenominator) AS comparisonDenominator,

                sum(peerSetNumerator) AS peerSetNumerator,
                sum(peerSetDenominator) AS peerSetDenominator,

                max(toplineCurrentValue) AS toplineCurrentValue,
                max(toplineComparisonValue) AS toplineComparisonValue,
                max(toplineCurrentDenominator) AS toplineCurrentDenominator,
                max(toplineComparisonDenominator) AS toplineComparisonDenominator,

                thisWeekDataAvailable,
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
                pairKey,
                pairLabel,
                pairSortOrder,
                rowBreakoutType,
                columnBreakoutType,
                configuredRowPairTopN,
                configuredColumnPairTopN,
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
                displaySize,
                displaySizeLabel,
                displayLimit,
                displaySizeSortOrder,
                rowBucketKey,
                displayRowBreakoutValue,
                columnBucketKey,
                displayColumnBreakoutValue,
                thisWeekDataAvailable
        ),
        bucketValues AS (
            SELECT
                a.*,
                CASE WHEN metricKind='ratio' THEN try_divide(currentNumerator,currentDenominator)
                     ELSE currentNumerator END AS currentValue,
                CASE WHEN metricKind='ratio' THEN try_divide(comparisonNumerator,comparisonDenominator)
                     ELSE comparisonNumerator END AS comparisonValue,
                CASE WHEN metricKind='ratio' THEN try_divide(peerSetNumerator,peerSetDenominator)
                     ELSE peerSetNumerator END AS peerSetValue,
                CASE WHEN metricKind='ratio'
                          THEN peerSetNumerator IS NOT NULL AND nullif(peerSetDenominator,0D) IS NOT NULL
                     ELSE peerSetNumerator IS NOT NULL END AS peerSetDataAvailable
            FROM bucketAgg a
        ),
        bucketCalculated AS (
            SELECT
                v.*,
                currentValue-comparisonValue AS absoluteDeltaValue,
                CASE
                    WHEN NOT thisWeekDataAvailable OR NOT comparisonDataAvailable
                      OR currentValue IS NULL OR comparisonValue IS NULL THEN NULL
                    WHEN changeUnit='pp' THEN 100D*(currentValue-comparisonValue)
                    WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,comparisonValue)-1D)
                    ELSE NULL
                END AS changeValue,
                CASE
                    WHEN currentValue IS NULL OR comparisonValue IS NULL THEN 'unavailable'
                    WHEN currentValue > comparisonValue THEN 'up'
                    WHEN currentValue < comparisonValue THEN 'down'
                    ELSE 'flat'
                END AS changeDirection,
                CASE
                    WHEN metricKind='count' THEN 100D*try_divide(currentValue-comparisonValue,toplineComparisonValue)
                    WHEN metricKind='ratio' THEN 100D*(
                        try_divide(currentNumerator,toplineCurrentDenominator)
                        - try_divide(comparisonNumerator,toplineComparisonDenominator)
                    )
                    ELSE NULL
                END AS impactOnToplineValue,
                CASE WHEN metricKind='count' THEN 'pct'
                     WHEN metricKind='ratio' THEN 'pp'
                     ELSE NULL END AS impactOnToplineUnit,
                currentValue-peerSetValue AS peerSetAbsoluteDeltaValue,
                CASE
                    WHEN NOT peerSetDataAvailable OR currentValue IS NULL THEN NULL
                    WHEN changeUnit='pp' THEN 100D*(currentValue-peerSetValue)
                    WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,peerSetValue)-1D)
                    ELSE NULL
                END AS peerSetChangeValue
            FROM bucketValues v
        ),
        rowAggDisplay AS (
            SELECT
                targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType,displaySize,
                rowBreakoutValue,rowDisplayRank,isRowOtherBucket,metricKind,
                sum(currentNumerator) AS rowCurrentNumerator,
                sum(currentDenominator) AS rowCurrentDenominator,
                sum(comparisonNumerator) AS rowComparisonNumerator,
                sum(comparisonDenominator) AS rowComparisonDenominator
            FROM bucketCalculated
            GROUP BY
                targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType,displaySize,
                rowBreakoutValue,rowDisplayRank,isRowOtherBucket,metricKind
        ),
        rowValuesDisplay AS (
            SELECT
                *,
                CASE WHEN metricKind='ratio' THEN try_divide(rowCurrentNumerator,rowCurrentDenominator)
                     ELSE rowCurrentNumerator END AS rowCurrentValue,
                CASE WHEN metricKind='ratio' THEN try_divide(rowComparisonNumerator,rowComparisonDenominator)
                     ELSE rowComparisonNumerator END AS rowComparisonValue
            FROM rowAggDisplay
        ),
        columnAggDisplay AS (
            SELECT
                targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType,displaySize,
                columnBreakoutValue,columnDisplayRank,isColumnOtherBucket,metricKind,
                sum(currentNumerator) AS columnCurrentNumerator,
                sum(currentDenominator) AS columnCurrentDenominator,
                sum(comparisonNumerator) AS columnComparisonNumerator,
                sum(comparisonDenominator) AS columnComparisonDenominator
            FROM bucketCalculated
            GROUP BY
                targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType,displaySize,
                columnBreakoutValue,columnDisplayRank,isColumnOtherBucket,metricKind
        ),
        columnValuesDisplay AS (
            SELECT
                *,
                CASE WHEN metricKind='ratio' THEN try_divide(columnCurrentNumerator,columnCurrentDenominator)
                     ELSE columnCurrentNumerator END AS columnCurrentValue,
                CASE WHEN metricKind='ratio' THEN try_divide(columnComparisonNumerator,columnComparisonDenominator)
                     ELSE columnComparisonNumerator END AS columnComparisonValue
            FROM columnAggDisplay
        ),
        withTotals AS (
            SELECT
                c.*,
                r.rowCurrentValue,
                r.rowComparisonValue,
                k.columnCurrentValue,
                k.columnComparisonValue
            FROM bucketCalculated c
            LEFT JOIN rowValuesDisplay r
              ON r.targetWeekStartDate=c.targetWeekStartDate
             AND r.filterLob=c.filterLob
             AND r.filterPlatform=c.filterPlatform
             AND r.pairKey=c.pairKey
             AND r.metricName=c.metricName
             AND r.comparisonType=c.comparisonType
             AND r.displaySize=c.displaySize
             AND r.rowBreakoutValue=c.rowBreakoutValue
             AND r.rowDisplayRank=c.rowDisplayRank
            LEFT JOIN columnValuesDisplay k
              ON k.targetWeekStartDate=c.targetWeekStartDate
             AND k.filterLob=c.filterLob
             AND k.filterPlatform=c.filterPlatform
             AND k.pairKey=c.pairKey
             AND k.metricName=c.metricName
             AND k.comparisonType=c.comparisonType
             AND k.displaySize=c.displaySize
             AND k.columnBreakoutValue=c.columnBreakoutValue
             AND k.columnDisplayRank=c.columnDisplayRank
        ),
        finalRanked AS (
            SELECT
                w.*,
                row_number() OVER (
                    PARTITION BY targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType,displaySize
                    ORDER BY abs(impactOnToplineValue) DESC NULLS LAST,
                             abs(absoluteDeltaValue) DESC NULLS LAST,
                             rowBreakoutValue,columnBreakoutValue
                ) AS cellImpactRankWithinPair,
                row_number() OVER (
                    PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName,comparisonType,displaySize
                    ORDER BY abs(impactOnToplineValue) DESC NULLS LAST,
                             abs(absoluteDeltaValue) DESC NULLS LAST,
                             pairKey,rowBreakoutValue,columnBreakoutValue
                ) AS cellImpactRankAcrossPairs
            FROM withTotals w
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

            pairKey,
            pairLabel,
            pairSortOrder,

            displaySize,
            displaySizeLabel,
            displayLimit,
            displaySizeSortOrder,

            rowBreakoutType,
            rowBreakoutValue,
            rowDisplayRank,
            isRowOtherBucket,
            rowCurrentValue,
            rowComparisonValue,
            configuredRowPairTopN,

            columnBreakoutType,
            columnBreakoutValue,
            columnDisplayRank,
            isColumnOtherBucket,
            columnCurrentValue,
            columnComparisonValue,
            configuredColumnPairTopN,

            rawCellMemberCount,

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

            toplineCurrentValue,
            toplineComparisonValue,
            impactOnToplineValue,
            impactOnToplineUnit,

            cellImpactRankWithinPair,
            cellImpactRankAcrossPairs,

            peerSetValue,
            peerSetAbsoluteDeltaValue,
            peerSetChangeValue,
            peerSetDataAvailable,

            thisWeekDataAvailable,
            goldProcessedAt
        FROM finalRanked
    ),
    ranked AS (
        SELECT
            m.*,
            row_number() OVER (
                PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName,comparisonType
                ORDER BY
                    abs(impactOnToplineValue) DESC NULLS LAST,
                    abs(absoluteDeltaValue) DESC NULLS LAST,
                    pairKey,rowBreakoutValue,columnBreakoutValue
            ) AS globalIntersectionImpactRank,
            count(*) OVER (
                PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName,comparisonType
            ) AS candidateIntersectionCount
        FROM crosstabsMatrix m
        WHERE displaySize='all'
          AND comparisonDataAvailable
          AND impactOnToplineValue IS NOT NULL
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

        pairKey,
        pairLabel,
        pairSortOrder,

        rowBreakoutType,
        rowBreakoutValue,
        rowDisplayRank,
        isRowOtherBucket,
        columnBreakoutType,
        columnBreakoutValue,
        columnDisplayRank,
        isColumnOtherBucket,
        concat(rowBreakoutValue,' × ',columnBreakoutValue) AS intersectionLabel,

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
        comparisonStartDate,
        comparisonEndDate,
        comparisonWeekCount,
        comparisonDataAvailable,
        comparisonWindowComplete,

        currentValue,
        comparisonValue,
        absoluteDeltaValue,
        changeValue,
        changeDirection,

        toplineCurrentValue,
        toplineComparisonValue,
        impactOnToplineValue,
        impactOnToplineUnit,

        cellImpactRankWithinPair,
        globalIntersectionImpactRank AS cellImpactRankAcrossPairs,

        coalesce(globalIntersectionImpactRank<=5,FALSE) AS isTop5,
        coalesce(globalIntersectionImpactRank<=10,FALSE) AS isTop10,
        coalesce(globalIntersectionImpactRank<=18,FALSE) AS isTop18,
        coalesce(globalIntersectionImpactRank<=100,FALSE) AS isTop100,

        candidateIntersectionCount,
        greatest(candidateIntersectionCount-100,0) AS allSuppressedIntersectionCount,
        100 AS allSelectionLimit,
        'All = Top 100 globally ranked intersections. Row/column (Other) buckets are already scoped within each pair; no cross-pair numeric Other is created.' AS allSelectionRule,

        peerSetValue,
        peerSetChangeValue,
        peerSetDataAvailable,

        goldProcessedAt
    FROM ranked
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
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsRankedPairs_long' AS targetObject,
            v_processedAt AS appProcessedAt;

    END IF;
END;

-- Development examples:
--
-- Preflight only:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appCrosstabsRankedPairs_long(
--     p_asOfDate       => DATE '2026-09-28',
--     p_weeksToRebuild => 1,
--     p_validateOnly   => TRUE
-- );
--
-- Load / rebuild:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appCrosstabsRankedPairs_long(
--     p_asOfDate       => DATE '2026-09-28',
--     p_weeksToRebuild => 1,
--     p_validateOnly   => FALSE
-- );
