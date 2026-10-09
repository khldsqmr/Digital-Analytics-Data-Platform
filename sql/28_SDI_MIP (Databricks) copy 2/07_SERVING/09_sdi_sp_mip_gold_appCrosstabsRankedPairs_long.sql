-- ============================================================================
-- RUNTIME-SAFE WRITE REVISION:
--   - Persisted table schema/API contract unchanged.
--   - Original comparison formulas retained, including existing 4-week behavior.
--   - Scoped INSERT ... REPLACE WHERE replaced with static MERGE.
--   - Metric Catalog is authoritative for metricLabel; NBV displays as Total NBV.
-- FILE  : 09_sdi_sp_mip_gold_appCrosstabsRankedPairs_long.sql
-- LAYER : GOLD / APP
-- TAB   : Crosstabs
-- SECTION: Every pair, ranked
--
-- PURPOSE:
--   Render-ready Top intersections across every active Crosstab pair.
--
-- UI:
--   Quarter
--   Week
--   Metric
--   Comparator = priorWeek | fourWeek | lastYear
--
-- SCREENSHOT CONTRACT:
--   - One selected comparator controls GLOBAL RANK and IMPACT ON TOPLINE.
--   - Visible columns simultaneously show:
--       This week
--       Vs prior week
--       Vs 4-wk trend
--       Vs same wk LY
--       Vs peer set
--       Impact on topline
--   - Top 18 rows are rendered by the UI.
--   - Clicking a row can load its pairKey/rowBreakoutType/columnBreakoutType Crosstab.
--
-- APP RULES:
--   - Metrics: isActive AND showOnBreakouts.
--   - Pairs: all active rows from Crosstab catalog.
--   - Each pair is bounded to Top100 row values + Other and Top100 column values + Other.
--   - No cross-pair numeric Other is created.
--   - Final App table exposes no numerator/denominator ingredients.
--
-- PEER-SET CONTRACT:
--   - Analytical Crosstab Gold now supplies a FOUR-WEEK peer counterfactual for
--     each real raw intersection.
--   - peerSetValue is that expected current cell value if the cell had moved at
--     the peer-set four-week rate.
--   - peerSetAbsoluteDiffValue = actual current cell - counterfactual cell.
--   - count peer gap = 100 * (current - counterfactual) / cell four-week baseline.
--   - ratio peer gap = 100 * (current ratio - counterfactual ratio).
--   - Peer set always uses the four-week basis, independent of comparisonType.
--   - When Top100 bucketing creates a synthetic row/column '(Other)' intersection,
--     peer fields are intentionally NULL because peer counterfactuals are
--     overlapping/non-additive and cannot be summed.
--
-- TEMPORARY DEMO FOUR-WEEK OVERRIDE:
--   - DEMO ONLY: keep the UI/API label as "4-wk trend", but allow the value to
--     use whatever historical baseline is currently available (1-4 weeks).
--   - Count metrics divide the available fourWeekTrendNumerator by the actual
--     available week count. Ratio metrics continue to use the available summed
--     numerator / denominator ingredients.
--   - Completeness metadata remains truthful: fourWeekWindowComplete is TRUE
--     only when all 4 baseline weeks exist.
--   - Peer-set values are NOT fabricated here. They remain sourced from
--     Analytical Gold and may stay NULL until Analytical Gold has a full 4-week
--     peer window.
--   - TEMPORARY FOR DEMO PURPOSES. After historical backfill is complete,
--     restore the original strict expressions marked "STRICT AFTER BACKFILL".
-- FOUR-WEEK COMPLETENESS METADATA:
--   - fourWeekDataAvailable retains its API meaning: at least one baseline week exists.
--   - fourWeekWindowComplete remains TRUE only when exactly four baseline weeks exist.
-- STRICT AFTER BACKFILL (original production contract; intentionally commented):
--   - A four-week value/comparison/impact/rank is emitted only when BOTH the
--     intersection and topline have complete four-week windows.
--
-- SCHEMA / API COMPATIBILITY:
--   This is a logic-only correction. Existing App/API columns remain unchanged.
-- ============================================================================
CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_gold_appCrosstabsRankedPairs_long(
    IN p_asOfDate DATE DEFAULT NULL,
    IN p_weeksToRebuild INT DEFAULT 1,
    IN p_validateOnly BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'MIP Gold App: Every pair ranked. Selected comparator drives global rank/impact; prior/4wk/LY/peer columns are simultaneously render-ready.'
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
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long g
        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_crosstabCatalog_static pc ON pc.pairKey=g.pairKey AND pc.isActive
        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc ON mc.metricName=g.metricName AND mc.isActive AND mc.showOnBreakouts
        WHERE g.targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Crosstab Gold has no eligible Ranked Pairs rows for the requested week range.';
    END IF;
    IF NOT EXISTS(
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long g
        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc ON mc.metricName=g.metricName AND mc.isActive AND mc.showOnBreakouts
        WHERE g.targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Overview Gold has no eligible Ranked Pairs metrics for the requested week range.';
    END IF;
    IF NOT EXISTS(SELECT 1 FROM prdrzranalytics.lab42.sdi_vw_mip_control_crosstabCatalog_static WHERE isActive LIMIT 1) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Crosstab Catalog has no active pairs.';
    END IF;
    IF NOT EXISTS(SELECT 1 FROM prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static WHERE isActive AND showOnBreakouts LIMIT 1) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Metric Catalog has no active Ranked Pairs metrics.';
    END IF;
    IF NOT EXISTS(
        SELECT 1 FROM prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static
        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Fiscal Calendar has no rows for the requested week range.';
    END IF;
    IF EXISTS(
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static
        WHERE isActive AND showOnBreakouts
          AND(metricKind NOT IN('count','ratio') OR displayFormat NOT IN('number','percent') OR changeUnit NOT IN('pct','pp'))
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Ranked Pairs metric metadata contains unsupported values.';
    END IF;
    IF p_validateOnly THEN
        SELECT
            'VALIDATION_ONLY' AS status,
            v_weekFrom AS rebuildWeekStartFrom,
            v_weekTo AS rebuildWeekStartTo,
            v_weekEndTo AS latestWeekEndDate,
            'priorWeek | fourWeek | lastYear' AS supportedComparisons,
            18 AS uiResultLimit,
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsRankedPairs_long' AS targetObject,
            'Validation passed. No Gold App table was created or modified.' AS message;
    ELSE
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsRankedPairs_long(
            targetWeekStartDate DATE,
            targetWeekEndDate DATE,
            fiscalYear INT,
            fiscalQuarterLabel STRING,
            fiscalWeekCode STRING,
            weekLabel STRING,
            weekEndingLabel STRING,
            filterLob STRING,
            filterPlatform STRING,
            pairKey STRING,
            pairLabel STRING,
            pairSortOrder INT,
            isPrebuiltPair BOOLEAN,
            rowBreakoutType STRING,
            rowBreakoutLabel STRING,
            rowBreakoutValue STRING,
            columnBreakoutType STRING,
            columnBreakoutLabel STRING,
            columnBreakoutValue STRING,
            intersectionLabel STRING,
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
            globalIntersectionImpactRank BIGINT,
            isTop5 BOOLEAN,
            isTop10 BOOLEAN,
            isTop18 BOOLEAN,
            isTop100 BOOLEAN,
            candidateIntersectionCount BIGINT,
            resultLimit INT,
            currentValue DOUBLE,
            currentValueDisplay STRING,
            priorWeekDataAvailable BOOLEAN,
            priorWeekAbsoluteDiffValue DOUBLE,
            priorWeekAbsoluteDiffDisplay STRING,
            priorWeekChangeValue DOUBLE,
            priorWeekChangeDisplay STRING,
            fourWeekDataAvailable BOOLEAN,
            fourWeekWindowComplete BOOLEAN,
            fourWeekAbsoluteDiffValue DOUBLE,
            fourWeekAbsoluteDiffDisplay STRING,
            fourWeekChangeValue DOUBLE,
            fourWeekChangeDisplay STRING,
            lastYearDataAvailable BOOLEAN,
            lastYearAbsoluteDiffValue DOUBLE,
            lastYearAbsoluteDiffDisplay STRING,
            lastYearChangeValue DOUBLE,
            lastYearChangeDisplay STRING,
            peerSetDataAvailable BOOLEAN,
            peerSetValue DOUBLE,
            peerSetValueDisplay STRING,
            peerSetAbsoluteDiffValue DOUBLE,
            peerSetAbsoluteDiffDisplay STRING,
            peerSetChangeValue DOUBLE,
            peerSetChangeDisplay STRING,
            impactOnToplineValue DOUBLE,
            impactOnToplineDisplay STRING,
            impactOnToplineUnit STRING,
            appProcessedAt TIMESTAMP
        )
        USING DELTA
        COMMENT 'MIP Gold App: Every pair ranked. Wide visible comparison columns with selected-comparator global ranking and impact.';
        WITH base AS(
            SELECT
                g.targetWeekStartDate,g.targetWeekEndDate,c.fiscalYear,g.fiscalQuarterLabel,g.fiscalWeekCode,g.weekLabel,c.weekEndingLabel,
                g.filterLob,g.filterPlatform,
                g.pairKey,pc.pairLabel,pc.sortOrder AS pairSortOrder,pc.isPrebuiltPair,
                g.rowBreakoutType,rb.breakoutLabel AS rowBreakoutLabel,g.rowBreakoutValue,
                g.columnBreakoutType,cb.breakoutLabel AS columnBreakoutLabel,g.columnBreakoutValue,
                g.metricName,mc.metricLabel AS metricLabel,
                mc.metricDescription,mc.metricKind,mc.displayFormat,mc.changeUnit,mc.sortOrder AS metricSortOrder,
                g.thisWeekNumerator,g.thisWeekDenominator,g.priorWeekNumerator,g.priorWeekDenominator,
                g.fourWeekTrendNumerator,g.fourWeekTrendDenominator,g.sameWeekLyNumerator,g.sameWeekLyDenominator,
                g.peerSetNumerator,g.peerSetDenominator,
                g.thisWeekDataAvailable,g.priorWeekDataAvailable,g.fourWeekTrendWeekCount,g.sameWeekLyDataAvailable
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long g
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_crosstabCatalog_static pc ON pc.pairKey=g.pairKey AND pc.isActive
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static rb ON rb.breakoutType=g.rowBreakoutType AND rb.isActive
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static cb ON cb.breakoutType=g.columnBreakoutType AND cb.isActive
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc ON mc.metricName=g.metricName AND mc.isActive AND mc.showOnBreakouts
            LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c ON c.weekStartDate=g.targetWeekStartDate
            WHERE g.targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        ),
        rowAgg AS(
            SELECT targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,rowBreakoutValue,metricKind,
                   sum(thisWeekNumerator) AS currentNumerator,sum(thisWeekDenominator) AS currentDenominator
            FROM base
            GROUP BY targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,rowBreakoutValue,metricKind
        ),
        rowValues AS(
            SELECT *,CASE WHEN metricKind='ratio' THEN try_divide(currentNumerator,currentDenominator) ELSE currentNumerator END AS rowCurrentValue
            FROM rowAgg
        ),
        rowRanks AS(
            SELECT *,row_number() OVER(
                PARTITION BY targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName
                ORDER BY rowCurrentValue DESC NULLS LAST,rowBreakoutValue
            ) AS rowRank
            FROM rowValues
        ),
        columnAgg AS(
            SELECT targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,columnBreakoutValue,metricKind,
                   sum(thisWeekNumerator) AS currentNumerator,sum(thisWeekDenominator) AS currentDenominator
            FROM base
            GROUP BY targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,columnBreakoutValue,metricKind
        ),
        columnValues AS(
            SELECT *,CASE WHEN metricKind='ratio' THEN try_divide(currentNumerator,currentDenominator) ELSE currentNumerator END AS columnCurrentValue
            FROM columnAgg
        ),
        columnRanks AS(
            SELECT *,row_number() OVER(
                PARTITION BY targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName
                ORDER BY columnCurrentValue DESC NULLS LAST,columnBreakoutValue
            ) AS columnRank
            FROM columnValues
        ),
        mapped AS(
            SELECT
                b.*,
                CASE WHEN r.rowRank<=100 THEN b.rowBreakoutValue ELSE '(Other)' END AS displayRowBreakoutValue,
                CASE WHEN k.columnRank<=100 THEN b.columnBreakoutValue ELSE '(Other)' END AS displayColumnBreakoutValue,
                r.rowRank>100 AS isSyntheticRowOtherMember,
                k.columnRank>100 AS isSyntheticColumnOtherMember
            FROM base b
            JOIN rowRanks r
              ON r.targetWeekStartDate=b.targetWeekStartDate AND r.filterLob=b.filterLob AND r.filterPlatform=b.filterPlatform
             AND r.pairKey=b.pairKey AND r.metricName=b.metricName AND r.rowBreakoutValue=b.rowBreakoutValue
            JOIN columnRanks k
              ON k.targetWeekStartDate=b.targetWeekStartDate AND k.filterLob=b.filterLob AND k.filterPlatform=b.filterPlatform
             AND k.pairKey=b.pairKey AND k.metricName=b.metricName AND k.columnBreakoutValue=b.columnBreakoutValue
        ),
        bucketAgg AS(
            SELECT
                targetWeekStartDate,max(targetWeekEndDate) AS targetWeekEndDate,max(fiscalYear) AS fiscalYear,
                max(fiscalQuarterLabel) AS fiscalQuarterLabel,max(fiscalWeekCode) AS fiscalWeekCode,
                max(weekLabel) AS weekLabel,max(weekEndingLabel) AS weekEndingLabel,
                filterLob,filterPlatform,pairKey,max(pairLabel) AS pairLabel,max(pairSortOrder) AS pairSortOrder,
                max(CASE WHEN isPrebuiltPair THEN 1 ELSE 0 END)=1 AS isPrebuiltPair,
                max(rowBreakoutType) AS rowBreakoutType,max(rowBreakoutLabel) AS rowBreakoutLabel,
                displayRowBreakoutValue AS rowBreakoutValue,
                max(columnBreakoutType) AS columnBreakoutType,max(columnBreakoutLabel) AS columnBreakoutLabel,
                displayColumnBreakoutValue AS columnBreakoutValue,
                metricName,max(metricLabel) AS metricLabel,max(metricDescription) AS metricDescription,
                max(metricKind) AS metricKind,max(displayFormat) AS displayFormat,max(changeUnit) AS changeUnit,max(metricSortOrder) AS metricSortOrder,
                sum(thisWeekNumerator) AS currentNumerator,sum(thisWeekDenominator) AS currentDenominator,
                sum(priorWeekNumerator) AS priorWeekNumerator,sum(priorWeekDenominator) AS priorWeekDenominator,
                sum(fourWeekTrendNumerator) AS fourWeekNumerator,sum(fourWeekTrendDenominator) AS fourWeekDenominator,
                max(fourWeekTrendWeekCount) AS fourWeekWeekCount,
                sum(sameWeekLyNumerator) AS lastYearNumerator,sum(sameWeekLyDenominator) AS lastYearDenominator,
                -- Peer counterfactuals are valid only for an unsynthesized raw
                -- intersection. Do not add them across row/column Other buckets.
                CASE
                    WHEN max(CASE WHEN isSyntheticRowOtherMember OR isSyntheticColumnOtherMember THEN 1 ELSE 0 END)=1
                        THEN cast(NULL AS DOUBLE)
                    ELSE max(peerSetNumerator)
                END AS peerSetNumerator,
                CASE
                    WHEN max(CASE WHEN isSyntheticRowOtherMember OR isSyntheticColumnOtherMember THEN 1 ELSE 0 END)=1
                        THEN cast(NULL AS DOUBLE)
                    ELSE max(peerSetDenominator)
                END AS peerSetDenominator,
                min(CASE WHEN thisWeekDataAvailable THEN 1 ELSE 0 END)=1 AS currentDataAvailable,
                min(CASE WHEN priorWeekDataAvailable THEN 1 ELSE 0 END)=1 AS priorWeekDataAvailable,
                min(CASE WHEN sameWeekLyDataAvailable THEN 1 ELSE 0 END)=1 AS lastYearDataAvailable
            FROM mapped
            GROUP BY targetWeekStartDate,filterLob,filterPlatform,pairKey,displayRowBreakoutValue,displayColumnBreakoutValue,metricName
        ),
        cellValues AS(
            SELECT
                *,
                CASE WHEN NOT currentDataAvailable THEN NULL WHEN metricKind='ratio' THEN try_divide(currentNumerator,currentDenominator) ELSE currentNumerator END AS currentValue,
                CASE WHEN NOT priorWeekDataAvailable THEN NULL WHEN metricKind='ratio' THEN try_divide(priorWeekNumerator,priorWeekDenominator) ELSE priorWeekNumerator END AS priorWeekValue,
                fourWeekWeekCount>0 AS fourWeekDataAvailable,
                fourWeekWeekCount=4 AS fourWeekWindowComplete,
                -- STRICT AFTER BACKFILL:
                -- CASE WHEN fourWeekWeekCount<>4 THEN NULL WHEN metricKind='ratio' THEN try_divide(fourWeekNumerator,fourWeekDenominator)
                --      ELSE try_divide(fourWeekNumerator,4D) END AS fourWeekValue,
                CASE WHEN fourWeekWeekCount<=0 THEN NULL
                     WHEN metricKind='ratio' THEN try_divide(fourWeekNumerator,fourWeekDenominator)
                     ELSE try_divide(fourWeekNumerator,cast(fourWeekWeekCount AS DOUBLE)) END AS fourWeekValue,
                CASE WHEN NOT lastYearDataAvailable THEN NULL WHEN metricKind='ratio' THEN try_divide(lastYearNumerator,lastYearDenominator) ELSE lastYearNumerator END AS lastYearValue,
                CASE WHEN metricKind='ratio' THEN try_divide(peerSetNumerator,peerSetDenominator) ELSE peerSetNumerator END AS peerSetValue,
                CASE WHEN metricKind='ratio' THEN peerSetNumerator IS NOT NULL AND nullif(peerSetDenominator,0D) IS NOT NULL
                     ELSE peerSetNumerator IS NOT NULL END AS peerSetDataAvailable
            FROM bucketAgg
        ),
        cellDeltas AS(
            SELECT
                *,
                currentValue-priorWeekValue AS priorWeekAbsoluteDiffValue,
                CASE WHEN currentValue IS NULL OR priorWeekValue IS NULL THEN NULL
                     WHEN changeUnit='pp' THEN 100D*(currentValue-priorWeekValue)
                     WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,priorWeekValue)-1D) END AS priorWeekChangeRaw,
                currentValue-fourWeekValue AS fourWeekAbsoluteDiffValue,
                CASE WHEN currentValue IS NULL OR fourWeekValue IS NULL THEN NULL
                     WHEN changeUnit='pp' THEN 100D*(currentValue-fourWeekValue)
                     WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,fourWeekValue)-1D) END AS fourWeekChangeRaw,
                currentValue-lastYearValue AS lastYearAbsoluteDiffValue,
                CASE WHEN currentValue IS NULL OR lastYearValue IS NULL THEN NULL
                     WHEN changeUnit='pp' THEN 100D*(currentValue-lastYearValue)
                     WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,lastYearValue)-1D) END AS lastYearChangeRaw,
                currentValue-peerSetValue AS peerSetAbsoluteDiffValue,
                CASE WHEN NOT peerSetDataAvailable OR currentValue IS NULL OR fourWeekValue IS NULL THEN NULL
                     WHEN changeUnit='pp' THEN 100D*(currentValue-peerSetValue)
                     WHEN changeUnit='pct' THEN 100D*try_divide(currentValue-peerSetValue,fourWeekValue) END AS peerSetChangeRaw
            FROM cellValues
        ),
        toplineBase AS(
            SELECT
                g.targetWeekStartDate,g.filterLob,g.filterPlatform,g.metricName,mc.metricKind,
                g.thisWeekNumerator,g.thisWeekDenominator,g.priorWeekNumerator,g.priorWeekDenominator,
                g.fourWeekTrendNumerator,g.fourWeekTrendDenominator,g.sameWeekLyNumerator,g.sameWeekLyDenominator,
                g.thisWeekDataAvailable,g.priorWeekDataAvailable,g.fourWeekTrendWeekCount,g.sameWeekLyDataAvailable
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long g
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
              ON mc.metricName=g.metricName AND mc.isActive AND mc.showOnBreakouts
            WHERE g.targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        ),
        toplineValues AS(
            SELECT
                *,
                CASE WHEN NOT thisWeekDataAvailable THEN NULL WHEN metricKind='ratio' THEN try_divide(thisWeekNumerator,thisWeekDenominator) ELSE thisWeekNumerator END AS toplineCurrentValue,
                CASE WHEN NOT priorWeekDataAvailable THEN NULL WHEN metricKind='ratio' THEN try_divide(priorWeekNumerator,priorWeekDenominator) ELSE priorWeekNumerator END AS toplinePriorWeekValue,
                -- STRICT AFTER BACKFILL:
                -- CASE WHEN fourWeekTrendWeekCount<>4 THEN NULL WHEN metricKind='ratio' THEN try_divide(fourWeekTrendNumerator,fourWeekTrendDenominator)
                --      ELSE try_divide(fourWeekTrendNumerator,4D) END AS toplineFourWeekValue,
                CASE WHEN fourWeekTrendWeekCount<=0 THEN NULL
                     WHEN metricKind='ratio' THEN try_divide(fourWeekTrendNumerator,fourWeekTrendDenominator)
                     ELSE try_divide(fourWeekTrendNumerator,cast(fourWeekTrendWeekCount AS DOUBLE)) END AS toplineFourWeekValue,
                CASE WHEN NOT sameWeekLyDataAvailable THEN NULL WHEN metricKind='ratio' THEN try_divide(sameWeekLyNumerator,sameWeekLyDenominator) ELSE sameWeekLyNumerator END AS toplineLastYearValue
            FROM toplineBase
        ),
        joined AS(
            SELECT
                c.*,
                t.toplineCurrentValue,t.thisWeekDenominator AS toplineCurrentDenominator,
                t.toplinePriorWeekValue,t.priorWeekDenominator AS toplinePriorWeekDenominator,t.priorWeekDataAvailable AS toplinePriorWeekDataAvailable,
                t.toplineFourWeekValue,t.fourWeekTrendDenominator AS toplineFourWeekDenominator,t.fourWeekTrendWeekCount AS toplineFourWeekWeekCount,
                t.toplineLastYearValue,t.sameWeekLyDenominator AS toplineLastYearDenominator,t.sameWeekLyDataAvailable AS toplineLastYearDataAvailable
            FROM cellDeltas c
            JOIN toplineValues t
              ON t.targetWeekStartDate=c.targetWeekStartDate AND t.filterLob=c.filterLob
             AND t.filterPlatform=c.filterPlatform AND t.metricName=c.metricName
        ),
        comparatorExpanded AS(
            SELECT
                j.*,'priorWeek' AS comparisonType,'Prior week' AS comparisonLabel,10 AS comparisonSortOrder,
                j.priorWeekDataAvailable AND j.toplinePriorWeekDataAvailable AS comparisonDataAvailable,
                j.priorWeekDataAvailable AND j.toplinePriorWeekDataAvailable AS comparisonWindowComplete,
                j.priorWeekAbsoluteDiffValue AS selectedAbsoluteDiffValue,
                j.priorWeekNumerator AS selectedComparisonNumerator,
                j.priorWeekDenominator AS selectedComparisonDenominator,
                j.toplinePriorWeekValue AS selectedToplineComparisonValue,
                j.toplinePriorWeekDenominator AS selectedToplineComparisonDenominator
            FROM joined j
            UNION ALL
            SELECT
                j.*,'fourWeek','4-wk trend',20,
                -- STRICT AFTER BACKFILL: j.fourWeekWindowComplete AND j.toplineFourWeekWeekCount=4,
                j.fourWeekDataAvailable AND j.toplineFourWeekWeekCount>0,
                -- Completeness remains strict/truthful.
                j.fourWeekWindowComplete AND j.toplineFourWeekWeekCount=4,
                j.fourWeekAbsoluteDiffValue,j.fourWeekNumerator,j.fourWeekDenominator,
                j.toplineFourWeekValue,j.toplineFourWeekDenominator
            FROM joined j
            UNION ALL
            SELECT
                j.*,'lastYear','Same wk LY',30,
                j.lastYearDataAvailable AND j.toplineLastYearDataAvailable,
                j.lastYearDataAvailable AND j.toplineLastYearDataAvailable,
                j.lastYearAbsoluteDiffValue,j.lastYearNumerator,j.lastYearDenominator,
                j.toplineLastYearValue,j.toplineLastYearDenominator
            FROM joined j
        ),
        impacts AS(
            SELECT
                *,
                CASE
                    WHEN NOT comparisonDataAvailable OR selectedAbsoluteDiffValue IS NULL THEN NULL
                    WHEN metricKind='count' THEN 100D*try_divide(selectedAbsoluteDiffValue,selectedToplineComparisonValue)
                    WHEN metricKind='ratio' THEN 100D*(
                        try_divide(currentNumerator,toplineCurrentDenominator)
                        -try_divide(selectedComparisonNumerator,selectedToplineComparisonDenominator)
                    )
                END AS impactOnToplineRaw,
                CASE WHEN metricKind='count' THEN 'pct' WHEN metricKind='ratio' THEN 'pp' END AS impactOnToplineUnit
            FROM comparatorExpanded
        ),
        ranked AS(
            SELECT
                *,
                row_number() OVER(
                    PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName,comparisonType
                    ORDER BY abs(impactOnToplineRaw) DESC NULLS LAST,abs(selectedAbsoluteDiffValue) DESC NULLS LAST,
                             pairKey,rowBreakoutValue,columnBreakoutValue
                ) AS globalIntersectionImpactRank,
                count(*) OVER(
                    PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName,comparisonType
                ) AS candidateIntersectionCount
            FROM impacts
            WHERE comparisonDataAvailable AND impactOnToplineRaw IS NOT NULL
        ),
        rounded AS(
            SELECT
                *,
                CASE WHEN priorWeekChangeRaw IS NULL THEN NULL WHEN abs(priorWeekChangeRaw)<0.05D THEN 0D ELSE round(priorWeekChangeRaw,1) END AS priorWeekChangeValue,
                CASE WHEN fourWeekChangeRaw IS NULL THEN NULL WHEN abs(fourWeekChangeRaw)<0.05D THEN 0D ELSE round(fourWeekChangeRaw,1) END AS fourWeekChangeValue,
                CASE WHEN lastYearChangeRaw IS NULL THEN NULL WHEN abs(lastYearChangeRaw)<0.05D THEN 0D ELSE round(lastYearChangeRaw,1) END AS lastYearChangeValue,
                CASE WHEN peerSetChangeRaw IS NULL THEN NULL WHEN abs(peerSetChangeRaw)<0.05D THEN 0D ELSE round(peerSetChangeRaw,1) END AS peerSetChangeValue,
                CASE WHEN impactOnToplineRaw IS NULL THEN NULL WHEN abs(impactOnToplineRaw)<0.05D THEN 0D ELSE round(impactOnToplineRaw,1) END AS impactOnToplineValue
            FROM ranked
            WHERE globalIntersectionImpactRank<=100
        ),
        formatted AS(
            SELECT
                *,
                CASE
                    WHEN currentValue IS NULL THEN NULL
                    WHEN displayFormat='percent' THEN concat(format_number(100D*currentValue,1),'%')
                    WHEN abs(currentValue)>=1000000000D THEN concat(regexp_replace(format_number(currentValue/1000000000D,1),'\\\\\\\\\\\\\\\\.0$',''),'B')
                    WHEN abs(currentValue)>=1000000D THEN concat(regexp_replace(format_number(currentValue/1000000D,1),'\\\\\\\\\\\\\\\\.0$',''),'M')
                    WHEN abs(currentValue)>=1000D THEN concat(regexp_replace(format_number(currentValue/1000D,1),'\\\\\\\\\\\\\\\\.0$',''),'K')
                    ELSE format_number(currentValue,0)
                END AS currentValueDisplay,
                CASE
                    WHEN priorWeekAbsoluteDiffValue IS NULL THEN NULL
                    WHEN metricKind='ratio' THEN concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(100D*priorWeekAbsoluteDiffValue,1),'pp')
                    WHEN abs(priorWeekAbsoluteDiffValue)>=1000000000D THEN concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(priorWeekAbsoluteDiffValue/1000000000D,1),'\\\\\\\\\\\\\\\\.0$',''),'B')
                    WHEN abs(priorWeekAbsoluteDiffValue)>=1000000D THEN concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(priorWeekAbsoluteDiffValue/1000000D,1),'\\\\\\\\\\\\\\\\.0$',''),'M')
                    WHEN abs(priorWeekAbsoluteDiffValue)>=1000D THEN concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(priorWeekAbsoluteDiffValue/1000D,1),'\\\\\\\\\\\\\\\\.0$',''),'K')
                    ELSE concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(priorWeekAbsoluteDiffValue,0))
                END AS priorWeekAbsoluteDiffDisplay,
                CASE WHEN priorWeekChangeValue IS NULL THEN NULL
                     WHEN changeUnit='pp' THEN concat(CASE WHEN priorWeekChangeValue>0D THEN '+' ELSE '' END,format_number(priorWeekChangeValue,1),'pp')
                     ELSE concat(CASE WHEN priorWeekChangeValue>0D THEN '+' ELSE '' END,format_number(priorWeekChangeValue,1),'%') END AS priorWeekChangeDisplay,
                CASE
                    WHEN fourWeekAbsoluteDiffValue IS NULL THEN NULL
                    WHEN metricKind='ratio' THEN concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(100D*fourWeekAbsoluteDiffValue,1),'pp')
                    WHEN abs(fourWeekAbsoluteDiffValue)>=1000000000D THEN concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(fourWeekAbsoluteDiffValue/1000000000D,1),'\\\\\\\\\\\\\\\\.0$',''),'B')
                    WHEN abs(fourWeekAbsoluteDiffValue)>=1000000D THEN concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(fourWeekAbsoluteDiffValue/1000000D,1),'\\\\\\\\\\\\\\\\.0$',''),'M')
                    WHEN abs(fourWeekAbsoluteDiffValue)>=1000D THEN concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(fourWeekAbsoluteDiffValue/1000D,1),'\\\\\\\\\\\\\\\\.0$',''),'K')
                    ELSE concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(fourWeekAbsoluteDiffValue,0))
                END AS fourWeekAbsoluteDiffDisplay,
                CASE WHEN fourWeekChangeValue IS NULL THEN NULL
                     WHEN changeUnit='pp' THEN concat(CASE WHEN fourWeekChangeValue>0D THEN '+' ELSE '' END,format_number(fourWeekChangeValue,1),'pp')
                     ELSE concat(CASE WHEN fourWeekChangeValue>0D THEN '+' ELSE '' END,format_number(fourWeekChangeValue,1),'%') END AS fourWeekChangeDisplay,
                CASE
                    WHEN lastYearAbsoluteDiffValue IS NULL THEN NULL
                    WHEN metricKind='ratio' THEN concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(100D*lastYearAbsoluteDiffValue,1),'pp')
                    WHEN abs(lastYearAbsoluteDiffValue)>=1000000000D THEN concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(lastYearAbsoluteDiffValue/1000000000D,1),'\\\\\\\\\\\\\\\\.0$',''),'B')
                    WHEN abs(lastYearAbsoluteDiffValue)>=1000000D THEN concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(lastYearAbsoluteDiffValue/1000000D,1),'\\\\\\\\\\\\\\\\.0$',''),'M')
                    WHEN abs(lastYearAbsoluteDiffValue)>=1000D THEN concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(lastYearAbsoluteDiffValue/1000D,1),'\\\\\\\\\\\\\\\\.0$',''),'K')
                    ELSE concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(lastYearAbsoluteDiffValue,0))
                END AS lastYearAbsoluteDiffDisplay,
                CASE WHEN lastYearChangeValue IS NULL THEN NULL
                     WHEN changeUnit='pp' THEN concat(CASE WHEN lastYearChangeValue>0D THEN '+' ELSE '' END,format_number(lastYearChangeValue,1),'pp')
                     ELSE concat(CASE WHEN lastYearChangeValue>0D THEN '+' ELSE '' END,format_number(lastYearChangeValue,1),'%') END AS lastYearChangeDisplay,
                CASE
                    WHEN peerSetValue IS NULL THEN NULL
                    WHEN displayFormat='percent' THEN concat(format_number(100D*peerSetValue,1),'%')
                    WHEN abs(peerSetValue)>=1000000000D THEN concat(regexp_replace(format_number(peerSetValue/1000000000D,1),'\\\\\\\\\\\\\\\\.0$',''),'B')
                    WHEN abs(peerSetValue)>=1000000D THEN concat(regexp_replace(format_number(peerSetValue/1000000D,1),'\\\\\\\\\\\\\\\\.0$',''),'M')
                    WHEN abs(peerSetValue)>=1000D THEN concat(regexp_replace(format_number(peerSetValue/1000D,1),'\\\\\\\\\\\\\\\\.0$',''),'K')
                    ELSE format_number(peerSetValue,0)
                END AS peerSetValueDisplay,
                CASE
                    WHEN peerSetAbsoluteDiffValue IS NULL THEN NULL
                    WHEN metricKind='ratio' THEN concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(100D*peerSetAbsoluteDiffValue,1),'pp')
                    WHEN abs(peerSetAbsoluteDiffValue)>=1000000000D THEN concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(peerSetAbsoluteDiffValue/1000000000D,1),'\\\\\\\\\\\\\\\\.0$',''),'B')
                    WHEN abs(peerSetAbsoluteDiffValue)>=1000000D THEN concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(peerSetAbsoluteDiffValue/1000000D,1),'\\\\\\\\\\\\\\\\.0$',''),'M')
                    WHEN abs(peerSetAbsoluteDiffValue)>=1000D THEN concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(peerSetAbsoluteDiffValue/1000D,1),'\\\\\\\\\\\\\\\\.0$',''),'K')
                    ELSE concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(peerSetAbsoluteDiffValue,0))
                END AS peerSetAbsoluteDiffDisplay,
                CASE WHEN peerSetChangeValue IS NULL THEN NULL
                     ELSE concat(CASE WHEN peerSetChangeValue>0D THEN '+' ELSE '' END,format_number(peerSetChangeValue,1),'pp') END AS peerSetChangeDisplay,
                CASE WHEN impactOnToplineValue IS NULL THEN NULL
                     WHEN impactOnToplineUnit='pp' THEN concat(CASE WHEN impactOnToplineValue>0D THEN '+' ELSE '' END,format_number(impactOnToplineValue,1),'pp')
                     ELSE concat(CASE WHEN impactOnToplineValue>0D THEN '+' ELSE '' END,format_number(impactOnToplineValue,1),'%') END AS impactOnToplineDisplay
            FROM rounded
        ),
        sourceRows AS (
SELECT
            targetWeekStartDate,targetWeekEndDate,fiscalYear,fiscalQuarterLabel,fiscalWeekCode,weekLabel,weekEndingLabel,
            filterLob,filterPlatform,
            pairKey,pairLabel,pairSortOrder,isPrebuiltPair,
            rowBreakoutType,rowBreakoutLabel,rowBreakoutValue,
            columnBreakoutType,columnBreakoutLabel,columnBreakoutValue,
            concat(rowBreakoutValue,' × ',columnBreakoutValue) AS intersectionLabel,
            metricName,metricLabel,metricDescription,metricKind,displayFormat,changeUnit,metricSortOrder,
            comparisonType,comparisonLabel,comparisonSortOrder,comparisonDataAvailable,comparisonWindowComplete,
            globalIntersectionImpactRank,
            globalIntersectionImpactRank<=5 AS isTop5,
            globalIntersectionImpactRank<=10 AS isTop10,
            globalIntersectionImpactRank<=18 AS isTop18,
            globalIntersectionImpactRank<=100 AS isTop100,
            candidateIntersectionCount,18 AS resultLimit,
            currentValue,currentValueDisplay,
            priorWeekDataAvailable,priorWeekAbsoluteDiffValue,priorWeekAbsoluteDiffDisplay,priorWeekChangeValue,priorWeekChangeDisplay,
            fourWeekDataAvailable,fourWeekWindowComplete,fourWeekAbsoluteDiffValue,fourWeekAbsoluteDiffDisplay,fourWeekChangeValue,fourWeekChangeDisplay,
            lastYearDataAvailable,lastYearAbsoluteDiffValue,lastYearAbsoluteDiffDisplay,lastYearChangeValue,lastYearChangeDisplay,
            peerSetDataAvailable,peerSetValue,peerSetValueDisplay,peerSetAbsoluteDiffValue,peerSetAbsoluteDiffDisplay,peerSetChangeValue,peerSetChangeDisplay,
            impactOnToplineValue,impactOnToplineDisplay,impactOnToplineUnit,
            v_processedAt AS appProcessedAt
        FROM formatted
        )
        MERGE INTO prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsRankedPairs_long AS t
        USING sourceRows AS s
          ON t.targetWeekStartDate = s.targetWeekStartDate
         AND t.filterLob <=> s.filterLob
         AND t.filterPlatform <=> s.filterPlatform
         AND t.metricName <=> s.metricName
         AND t.comparisonType <=> s.comparisonType
         AND t.pairKey <=> s.pairKey
         AND t.rowBreakoutValue <=> s.rowBreakoutValue
         AND t.columnBreakoutValue <=> s.columnBreakoutValue
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        WHEN NOT MATCHED BY SOURCE
         AND t.targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        THEN DELETE;
        SELECT
            'SUCCESS' AS status,
            v_weekFrom AS rebuiltWeekStartFrom,
            v_weekTo AS rebuiltWeekStartTo,
            v_weekEndTo AS latestWeekEndDate,
            'priorWeek | fourWeek | lastYear' AS supportedComparisons,
            18 AS uiResultLimit,
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsRankedPairs_long' AS targetObject,
            v_processedAt AS appProcessedAt;
    END IF;
END;
-- No schema migration is required for this peer-set correction.
-- Rebuild analytical Crosstab Gold first, then rebuild this App Gold table.
-- Rebuild analytical Crosstab Gold:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_crosstabMetricIngredientsByWeek_long(
--     p_asOfDate=>DATE '2026-09-28',
--     p_weeksToRebuild=>12,
--     p_validateOnly=>FALSE
-- );
-- Rebuild App Gold:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appCrosstabsRankedPairs_long(
--     p_asOfDate=>DATE '2026-09-28',
--     p_weeksToRebuild=>12,
--     p_validateOnly=>FALSE
-- );
-- Screenshot query:
-- SELECT *
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsRankedPairs_long
-- WHERE targetWeekStartDate=DATE '2026-08-09'
--   AND filterLob='All'
--   AND filterPlatform='All'
--   AND metricName='nbv'
--   AND comparisonType='fourWeek'
--   AND isTop18
-- ORDER BY globalIntersectionImpactRank;
-- Peer validation:
-- Synthetic Top100 '(Other)' intersections must have peerSetDataAvailable=FALSE.
-- SELECT *
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsRankedPairs_long
-- WHERE (rowBreakoutValue='(Other)' OR columnBreakoutValue='(Other)')
--   AND peerSetDataAvailable;
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
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appCrosstabsRankedPairs_long(
--   p_asOfDate       => DATE '{as_of_date.isoformat()}',
--   p_weeksToRebuild => {int(weeks_to_rebuild)},
--   p_validateOnly   => {validate_literal}
-- )
-- """
-- result = spark.sql(call_sql).collect()
-- display(result)
