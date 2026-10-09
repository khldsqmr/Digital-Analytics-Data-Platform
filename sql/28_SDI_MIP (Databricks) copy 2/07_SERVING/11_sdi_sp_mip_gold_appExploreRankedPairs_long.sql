-- ============================================================================
-- RUNTIME-SAFE WRITE REVISION:
--   - Persisted table schema/API contract unchanged.
--   - Original comparison formulas retained, including existing 4-week behavior.
--   - Scoped INSERT ... REPLACE WHERE replaced with static MERGE.
--   - Metric Catalog is authoritative for metricLabel; NBV displays as Total NBV.
-- FILE  : 11_sdi_sp_mip_gold_appExploreRankedPairs_long.sql
-- LAYER : GOLD / APP
-- TAB   : Explore
-- SECTION: Every pair, ranked
-- PURPOSE:
--   Render-ready default/unfiltered Explore ranked intersections.
--
-- SCREEN CONTRACT:
--   One physical row per week × filter context × metric × canonical pair × intersection.
--   The row carries Prior week, 4-wk trend and Same wk LY simultaneously.
--   The selected comparator is NOT persisted as another row; the API only chooses
--   which precomputed rank/impact column to order/display.
--
-- IMPORTANT:
--   - All active Crosstab pairs whose dimensions are active Explore dimensions are eligible.
--   - Metrics require isActive=TRUE AND showOnBreakouts=TRUE.
--   - No displaySize copies and no swapped-orientation copies are stored.
--   - No numerator/denominator helpers are exposed in the App contract.
--   - Per comparator, only the strongest 25 intersections per pair enter the ranking pool.
--     This preserves the exact Top18 after excluding one currently-open pair while bounding volume.
--   - The union of the Top100 global candidates for Prior/4-wk/LY is physically retained.
--   - Arbitrary Explore filters/Quick Filters are NOT precomputed here; they must use
--     sdi_tbl_mip_gold_appExploreBase_wide and apply filters before aggregation/ranking.
--
-- PEER-SET CONTRACT:
--   - This fast-path table consumes the approved peer counterfactual already
--     calculated in analytical Crosstab Gold for each raw intersection.
--   - peerSetValue is the expected current intersection value if it had moved at
--     its peer set's FOUR-WEEK rate.
--   - peerSetAbsoluteDiffValue = actual current - counterfactual.
--   - count peer gap = 100 * (current - counterfactual) / intersection four-week baseline.
--   - ratio peer gap = 100 * (current ratio - counterfactual ratio).
--   - Peer set is always four-week based regardless of which comparator the UI
--     chooses for ranking / impact on topline.
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
--   - four-week values, impact and ranks are NULL unless both the intersection
--     and topline have complete four-week windows.
--
-- SCHEMA / API COMPATIBILITY:
--   Existing App/API columns are unchanged.
-- ============================================================================
CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_gold_appExploreRankedPairs_long(
    IN p_asOfDate DATE DEFAULT NULL,
    IN p_weeksToRebuild INT DEFAULT 1,
    IN p_validateOnly BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'MIP Gold App: Explore Every pair ranked. One row per intersection with all three comparator results and ranks.'
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
        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_crosstabCatalog_static pc
          ON pc.pairKey=g.pairKey AND pc.isActive
        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static rb
          ON rb.breakoutType=g.rowBreakoutType AND rb.isActive AND rb.isExploreDimension
        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static cb
          ON cb.breakoutType=g.columnBreakoutType AND cb.isActive AND cb.isExploreDimension
        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
          ON mc.metricName=g.metricName AND mc.isActive AND mc.showOnBreakouts
        WHERE g.targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Crosstab Gold has no eligible Explore Ranked Pairs rows for the requested week range.';
    END IF;
    IF NOT EXISTS(
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long g
        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
          ON mc.metricName=g.metricName AND mc.isActive AND mc.showOnBreakouts
        WHERE g.targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Overview Gold has no eligible Explore Ranked Pairs metrics for the requested week range.';
    END IF;
    IF NOT EXISTS(
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_crosstabCatalog_static
        WHERE isActive
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Crosstab Catalog has no active pairs.';
    END IF;
    IF NOT EXISTS(
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static
        WHERE isActive AND showOnBreakouts
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Metric Catalog has no active Explore Ranked Pairs metrics.';
    END IF;
    IF NOT EXISTS(
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static
        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Fiscal Calendar has no rows for the requested week range.';
    END IF;
    IF EXISTS(
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long g
        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_crosstabCatalog_static pc
          ON pc.pairKey=g.pairKey AND pc.isActive
        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static rb
          ON rb.breakoutType=g.rowBreakoutType AND rb.isActive AND rb.isExploreDimension
        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static cb
          ON cb.breakoutType=g.columnBreakoutType AND cb.isActive AND cb.isExploreDimension
        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
          ON mc.metricName=g.metricName AND mc.isActive AND mc.showOnBreakouts
        WHERE g.targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        GROUP BY g.targetWeekStartDate,g.filterLob,g.filterPlatform,g.pairKey,g.rowBreakoutValue,g.columnBreakoutValue,g.metricName
        HAVING count(*)>1
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Duplicate eligible Crosstab Gold analytical keys detected.';
    END IF;
    IF EXISTS(
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long g
        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
          ON mc.metricName=g.metricName AND mc.isActive AND mc.showOnBreakouts
        WHERE g.targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        GROUP BY g.targetWeekStartDate,g.filterLob,g.filterPlatform,g.metricName
        HAVING count(*)>1
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Duplicate eligible Overview Gold analytical keys detected.';
    END IF;
    IF EXISTS(
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static
        WHERE isActive AND showOnBreakouts
          AND(metricKind NOT IN('count','ratio') OR displayFormat NOT IN('number','percent') OR changeUnit NOT IN('pct','pp'))
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Explore Ranked Pairs metric metadata contains unsupported values.';
    END IF;
    IF p_validateOnly THEN
        SELECT
            'VALIDATION_ONLY' AS status,
            v_weekFrom AS rebuildWeekStartFrom,
            v_weekTo AS rebuildWeekStartTo,
            v_weekEndTo AS latestWeekEndDate,
            CASE WHEN v_asOfDate<v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,
            'priorWeek | fourWeek | lastYear' AS supportedComparisons,
            25 AS candidatePerPairLimit,
            100 AS retainedGlobalLimit,
            18 AS uiResultLimit,
            'defaultAll' AS exploreFilterMode,
            FALSE AS supportsArbitraryExploreFilters,
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreBase_wide' AS dynamicFilterSourceObject,
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreRankedPairs_long' AS targetObject,
            'Validation passed. No Gold App table was created or modified.' AS message;
    ELSE
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreRankedPairs_long(
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
            currentValue DOUBLE,
            currentValueDisplay STRING,
            priorWeekDataAvailable BOOLEAN,
            priorWeekValue DOUBLE,
            priorWeekAbsoluteDiffValue DOUBLE,
            priorWeekAbsoluteDiffDisplay STRING,
            priorWeekChangeValue DOUBLE,
            priorWeekChangeDisplay STRING,
            priorWeekDirection STRING,
            priorWeekImpactOnToplineValue DOUBLE,
            priorWeekImpactOnToplineDisplay STRING,
            priorWeekGlobalRank BIGINT,
            priorWeekCandidateIntersectionCount BIGINT,
            fourWeekDataAvailable BOOLEAN,
            fourWeekWindowComplete BOOLEAN,
            fourWeekValue DOUBLE,
            fourWeekAbsoluteDiffValue DOUBLE,
            fourWeekAbsoluteDiffDisplay STRING,
            fourWeekChangeValue DOUBLE,
            fourWeekChangeDisplay STRING,
            fourWeekDirection STRING,
            fourWeekImpactOnToplineValue DOUBLE,
            fourWeekImpactOnToplineDisplay STRING,
            fourWeekGlobalRank BIGINT,
            fourWeekCandidateIntersectionCount BIGINT,
            lastYearDataAvailable BOOLEAN,
            lastYearValue DOUBLE,
            lastYearAbsoluteDiffValue DOUBLE,
            lastYearAbsoluteDiffDisplay STRING,
            lastYearChangeValue DOUBLE,
            lastYearChangeDisplay STRING,
            lastYearDirection STRING,
            lastYearImpactOnToplineValue DOUBLE,
            lastYearImpactOnToplineDisplay STRING,
            lastYearGlobalRank BIGINT,
            lastYearCandidateIntersectionCount BIGINT,
            peerSetDataAvailable BOOLEAN,
            peerSetValue DOUBLE,
            peerSetValueDisplay STRING,
            peerSetAbsoluteDiffValue DOUBLE,
            peerSetAbsoluteDiffDisplay STRING,
            peerSetChangeValue DOUBLE,
            peerSetChangeDisplay STRING,
            impactOnToplineUnit STRING,
            candidatePerPairLimit INT,
            retainedGlobalLimit INT,
            uiResultLimit INT,
            exploreFilterMode STRING,
            supportsArbitraryExploreFilters BOOLEAN,
            dynamicFilterSourceObject STRING,
            appProcessedAt TIMESTAMP
        )
        USING DELTA
        COMMENT 'MIP Gold App: Explore Every pair ranked. Wide Prior/4-wk/LY comparator contract; default/unfiltered fast path only.';
        WITH base AS(
            SELECT
                g.targetWeekStartDate,g.targetWeekEndDate,c.fiscalYear,g.fiscalQuarterLabel,g.fiscalWeekCode,g.weekLabel,c.weekEndingLabel,
                g.filterLob,g.filterPlatform,
                g.pairKey,pc.pairLabel,pc.sortOrder AS pairSortOrder,pc.isPrebuiltPair,
                g.rowBreakoutType,rb.breakoutLabel AS rowBreakoutLabel,coalesce(g.rowBreakoutValue,'(not set)') AS rowBreakoutValue,
                g.columnBreakoutType,cb.breakoutLabel AS columnBreakoutLabel,coalesce(g.columnBreakoutValue,'(not set)') AS columnBreakoutValue,
                g.metricName,mc.metricLabel AS metricLabel,
                mc.metricDescription,mc.metricKind,mc.displayFormat,mc.changeUnit,mc.sortOrder AS metricSortOrder,
                g.thisWeekNumerator,g.thisWeekDenominator,
                g.priorWeekNumerator,g.priorWeekDenominator,
                g.fourWeekTrendNumerator,g.fourWeekTrendDenominator,g.fourWeekTrendWeekCount,
                g.sameWeekLyNumerator,g.sameWeekLyDenominator,
                g.peerSetNumerator,g.peerSetDenominator,
                g.thisWeekDataAvailable,g.priorWeekDataAvailable,g.sameWeekLyDataAvailable,
                g.goldProcessedAt
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long g
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_crosstabCatalog_static pc
              ON pc.pairKey=g.pairKey AND pc.isActive
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static rb
              ON rb.breakoutType=g.rowBreakoutType AND rb.isActive AND rb.isExploreDimension
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static cb
              ON cb.breakoutType=g.columnBreakoutType AND cb.isActive AND cb.isExploreDimension
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
              ON mc.metricName=g.metricName AND mc.isActive AND mc.showOnBreakouts
            LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c
              ON c.weekStartDate=g.targetWeekStartDate
            WHERE g.targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        ),
        topline AS(
            SELECT
                g.targetWeekStartDate,g.filterLob,g.filterPlatform,g.metricName,mc.metricKind,
                g.thisWeekNumerator,g.thisWeekDenominator,
                g.priorWeekNumerator,g.priorWeekDenominator,
                g.fourWeekTrendNumerator,g.fourWeekTrendDenominator,g.fourWeekTrendWeekCount,
                g.sameWeekLyNumerator,g.sameWeekLyDenominator,
                g.thisWeekDataAvailable,g.priorWeekDataAvailable,g.sameWeekLyDataAvailable
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long g
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
              ON mc.metricName=g.metricName AND mc.isActive AND mc.showOnBreakouts
            WHERE g.targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        ),
        valuesCalculated AS(
            SELECT
                b.*,
                CASE WHEN NOT b.thisWeekDataAvailable THEN NULL
                     WHEN b.metricKind='ratio' THEN try_divide(b.thisWeekNumerator,b.thisWeekDenominator)
                     ELSE b.thisWeekNumerator END AS currentValue,
                CASE WHEN NOT b.priorWeekDataAvailable THEN NULL
                     WHEN b.metricKind='ratio' THEN try_divide(b.priorWeekNumerator,b.priorWeekDenominator)
                     ELSE b.priorWeekNumerator END AS priorWeekValue,
                b.fourWeekTrendWeekCount>0 AS fourWeekDataAvailable,
                b.fourWeekTrendWeekCount=4 AS fourWeekWindowComplete,
                -- STRICT AFTER BACKFILL:
                -- CASE WHEN b.fourWeekTrendWeekCount<>4 THEN NULL
                --      WHEN b.metricKind='ratio' THEN try_divide(b.fourWeekTrendNumerator,b.fourWeekTrendDenominator)
                --      ELSE try_divide(b.fourWeekTrendNumerator,4D) END AS fourWeekValue,
                CASE WHEN b.fourWeekTrendWeekCount<=0 THEN NULL
                     WHEN b.metricKind='ratio' THEN try_divide(b.fourWeekTrendNumerator,b.fourWeekTrendDenominator)
                     ELSE try_divide(b.fourWeekTrendNumerator,cast(b.fourWeekTrendWeekCount AS DOUBLE)) END AS fourWeekValue,
                CASE WHEN NOT b.sameWeekLyDataAvailable THEN NULL
                     WHEN b.metricKind='ratio' THEN try_divide(b.sameWeekLyNumerator,b.sameWeekLyDenominator)
                     ELSE b.sameWeekLyNumerator END AS lastYearValue,
                CASE WHEN b.metricKind='ratio' THEN try_divide(b.peerSetNumerator,b.peerSetDenominator)
                     ELSE b.peerSetNumerator END AS peerSetValue,
                CASE WHEN b.metricKind='ratio' THEN b.peerSetNumerator IS NOT NULL AND nullif(b.peerSetDenominator,0D) IS NOT NULL
                     ELSE b.peerSetNumerator IS NOT NULL END AS peerSetDataAvailable,
                CASE WHEN NOT t.thisWeekDataAvailable THEN NULL
                     WHEN t.metricKind='ratio' THEN try_divide(t.thisWeekNumerator,t.thisWeekDenominator)
                     ELSE t.thisWeekNumerator END AS toplineCurrentValue,
                CASE WHEN NOT t.priorWeekDataAvailable THEN NULL
                     WHEN t.metricKind='ratio' THEN try_divide(t.priorWeekNumerator,t.priorWeekDenominator)
                     ELSE t.priorWeekNumerator END AS toplinePriorWeekValue,
                -- STRICT AFTER BACKFILL:
                -- CASE WHEN t.fourWeekTrendWeekCount<>4 THEN NULL
                --      WHEN t.metricKind='ratio' THEN try_divide(t.fourWeekTrendNumerator,t.fourWeekTrendDenominator)
                --      ELSE try_divide(t.fourWeekTrendNumerator,4D) END AS toplineFourWeekValue,
                CASE WHEN t.fourWeekTrendWeekCount<=0 THEN NULL
                     WHEN t.metricKind='ratio' THEN try_divide(t.fourWeekTrendNumerator,t.fourWeekTrendDenominator)
                     ELSE try_divide(t.fourWeekTrendNumerator,cast(t.fourWeekTrendWeekCount AS DOUBLE)) END AS toplineFourWeekValue,
                CASE WHEN NOT t.sameWeekLyDataAvailable THEN NULL
                     WHEN t.metricKind='ratio' THEN try_divide(t.sameWeekLyNumerator,t.sameWeekLyDenominator)
                     ELSE t.sameWeekLyNumerator END AS toplineLastYearValue,
                t.thisWeekDenominator AS toplineCurrentDenominator,
                t.priorWeekDenominator AS toplinePriorWeekDenominator,
                t.fourWeekTrendDenominator AS toplineFourWeekDenominator,
                t.sameWeekLyDenominator AS toplineLastYearDenominator,
                t.priorWeekDataAvailable AS toplinePriorWeekDataAvailable,
                t.fourWeekTrendWeekCount AS toplineFourWeekWeekCount,
                t.sameWeekLyDataAvailable AS toplineLastYearDataAvailable
            FROM base b
            JOIN topline t
              ON t.targetWeekStartDate=b.targetWeekStartDate
             AND t.filterLob<=>b.filterLob
             AND t.filterPlatform<=>b.filterPlatform
             AND t.metricName=b.metricName
        ),
        deltas AS(
            SELECT
                *,
                currentValue-priorWeekValue AS priorWeekAbsoluteDiffValue,
                CASE WHEN currentValue IS NULL OR priorWeekValue IS NULL THEN NULL
                     WHEN changeUnit='pp' THEN 100D*(currentValue-priorWeekValue)
                     WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,priorWeekValue)-1D) END AS priorWeekChangeRaw,
                CASE WHEN NOT priorWeekDataAvailable OR NOT toplinePriorWeekDataAvailable OR currentValue IS NULL OR priorWeekValue IS NULL THEN NULL
                     WHEN metricKind='count' THEN 100D*try_divide(currentValue-priorWeekValue,toplinePriorWeekValue)
                     WHEN metricKind='ratio' THEN 100D*(
                         try_divide(thisWeekNumerator,toplineCurrentDenominator)
                         -try_divide(priorWeekNumerator,toplinePriorWeekDenominator)
                     ) END AS priorWeekImpactRaw,
                currentValue-fourWeekValue AS fourWeekAbsoluteDiffValue,
                CASE WHEN currentValue IS NULL OR fourWeekValue IS NULL THEN NULL
                     WHEN changeUnit='pp' THEN 100D*(currentValue-fourWeekValue)
                     WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,fourWeekValue)-1D) END AS fourWeekChangeRaw,
                -- STRICT AFTER BACKFILL:
                -- CASE WHEN NOT fourWeekWindowComplete OR toplineFourWeekWeekCount<>4 OR currentValue IS NULL OR fourWeekValue IS NULL THEN NULL
                CASE WHEN NOT fourWeekDataAvailable OR toplineFourWeekWeekCount<=0 OR currentValue IS NULL OR fourWeekValue IS NULL THEN NULL
                     WHEN metricKind='count' THEN 100D*try_divide(currentValue-fourWeekValue,toplineFourWeekValue)
                     WHEN metricKind='ratio' THEN 100D*(
                         try_divide(thisWeekNumerator,toplineCurrentDenominator)
                         -try_divide(fourWeekTrendNumerator,toplineFourWeekDenominator)
                     ) END AS fourWeekImpactRaw,
                currentValue-lastYearValue AS lastYearAbsoluteDiffValue,
                CASE WHEN currentValue IS NULL OR lastYearValue IS NULL THEN NULL
                     WHEN changeUnit='pp' THEN 100D*(currentValue-lastYearValue)
                     WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,lastYearValue)-1D) END AS lastYearChangeRaw,
                CASE WHEN NOT sameWeekLyDataAvailable OR NOT toplineLastYearDataAvailable OR currentValue IS NULL OR lastYearValue IS NULL THEN NULL
                     WHEN metricKind='count' THEN 100D*try_divide(currentValue-lastYearValue,toplineLastYearValue)
                     WHEN metricKind='ratio' THEN 100D*(
                         try_divide(thisWeekNumerator,toplineCurrentDenominator)
                         -try_divide(sameWeekLyNumerator,toplineLastYearDenominator)
                     ) END AS lastYearImpactRaw,
                currentValue-peerSetValue AS peerSetAbsoluteDiffValue,
                CASE WHEN NOT peerSetDataAvailable OR currentValue IS NULL OR fourWeekValue IS NULL THEN NULL
                     WHEN changeUnit='pp' THEN 100D*(currentValue-peerSetValue)
                     WHEN changeUnit='pct' THEN 100D*try_divide(currentValue-peerSetValue,fourWeekValue) END AS peerSetChangeRaw
            FROM valuesCalculated
        ),
        pairRanks AS(
            SELECT
                *,
                CASE WHEN priorWeekImpactRaw IS NOT NULL THEN row_number() OVER(
                    PARTITION BY targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName
                    ORDER BY abs(priorWeekImpactRaw) DESC NULLS LAST,abs(priorWeekAbsoluteDiffValue) DESC NULLS LAST,rowBreakoutValue,columnBreakoutValue
                ) END AS priorWeekRankWithinPair,
                CASE WHEN fourWeekImpactRaw IS NOT NULL THEN row_number() OVER(
                    PARTITION BY targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName
                    ORDER BY abs(fourWeekImpactRaw) DESC NULLS LAST,abs(fourWeekAbsoluteDiffValue) DESC NULLS LAST,rowBreakoutValue,columnBreakoutValue
                ) END AS fourWeekRankWithinPair,
                CASE WHEN lastYearImpactRaw IS NOT NULL THEN row_number() OVER(
                    PARTITION BY targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName
                    ORDER BY abs(lastYearImpactRaw) DESC NULLS LAST,abs(lastYearAbsoluteDiffValue) DESC NULLS LAST,rowBreakoutValue,columnBreakoutValue
                ) END AS lastYearRankWithinPair
            FROM deltas
        ),
        candidatePool AS(
            SELECT *
            FROM pairRanks
            WHERE coalesce(priorWeekRankWithinPair<=25,FALSE)
               OR coalesce(fourWeekRankWithinPair<=25,FALSE)
               OR coalesce(lastYearRankWithinPair<=25,FALSE)
        ),
        globalRanks AS(
            SELECT
                *,
                CASE WHEN priorWeekImpactRaw IS NOT NULL THEN row_number() OVER(
                    PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName
                    ORDER BY abs(priorWeekImpactRaw) DESC NULLS LAST,abs(priorWeekAbsoluteDiffValue) DESC NULLS LAST,pairKey,rowBreakoutValue,columnBreakoutValue
                ) END AS priorWeekGlobalRank,
                sum(CASE WHEN priorWeekImpactRaw IS NOT NULL THEN 1 ELSE 0 END) OVER(
                    PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName
                ) AS priorWeekCandidateIntersectionCount,
                CASE WHEN fourWeekImpactRaw IS NOT NULL THEN row_number() OVER(
                    PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName
                    ORDER BY abs(fourWeekImpactRaw) DESC NULLS LAST,abs(fourWeekAbsoluteDiffValue) DESC NULLS LAST,pairKey,rowBreakoutValue,columnBreakoutValue
                ) END AS fourWeekGlobalRank,
                sum(CASE WHEN fourWeekImpactRaw IS NOT NULL THEN 1 ELSE 0 END) OVER(
                    PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName
                ) AS fourWeekCandidateIntersectionCount,
                CASE WHEN lastYearImpactRaw IS NOT NULL THEN row_number() OVER(
                    PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName
                    ORDER BY abs(lastYearImpactRaw) DESC NULLS LAST,abs(lastYearAbsoluteDiffValue) DESC NULLS LAST,pairKey,rowBreakoutValue,columnBreakoutValue
                ) END AS lastYearGlobalRank,
                sum(CASE WHEN lastYearImpactRaw IS NOT NULL THEN 1 ELSE 0 END) OVER(
                    PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName
                ) AS lastYearCandidateIntersectionCount
            FROM candidatePool
        ),
        retained AS(
            SELECT *
            FROM globalRanks
            WHERE coalesce(priorWeekGlobalRank<=100,FALSE)
               OR coalesce(fourWeekGlobalRank<=100,FALSE)
               OR coalesce(lastYearGlobalRank<=100,FALSE)
        ),
        rounded AS(
            SELECT
                *,
                CASE WHEN priorWeekChangeRaw IS NULL THEN NULL WHEN abs(priorWeekChangeRaw)<0.05D THEN 0D ELSE round(priorWeekChangeRaw,1) END AS priorWeekChangeValue,
                CASE WHEN priorWeekImpactRaw IS NULL THEN NULL WHEN abs(priorWeekImpactRaw)<0.05D THEN 0D ELSE round(priorWeekImpactRaw,1) END AS priorWeekImpactOnToplineValue,
                CASE WHEN fourWeekChangeRaw IS NULL THEN NULL WHEN abs(fourWeekChangeRaw)<0.05D THEN 0D ELSE round(fourWeekChangeRaw,1) END AS fourWeekChangeValue,
                CASE WHEN fourWeekImpactRaw IS NULL THEN NULL WHEN abs(fourWeekImpactRaw)<0.05D THEN 0D ELSE round(fourWeekImpactRaw,1) END AS fourWeekImpactOnToplineValue,
                CASE WHEN lastYearChangeRaw IS NULL THEN NULL WHEN abs(lastYearChangeRaw)<0.05D THEN 0D ELSE round(lastYearChangeRaw,1) END AS lastYearChangeValue,
                CASE WHEN lastYearImpactRaw IS NULL THEN NULL WHEN abs(lastYearImpactRaw)<0.05D THEN 0D ELSE round(lastYearImpactRaw,1) END AS lastYearImpactOnToplineValue,
                CASE WHEN peerSetChangeRaw IS NULL THEN NULL WHEN abs(peerSetChangeRaw)<0.05D THEN 0D ELSE round(peerSetChangeRaw,1) END AS peerSetChangeValue
            FROM retained
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
                CASE WHEN priorWeekAbsoluteDiffValue IS NULL THEN NULL
                     WHEN metricKind='ratio' THEN concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(100D*priorWeekAbsoluteDiffValue,1),'pp')
                     WHEN abs(priorWeekAbsoluteDiffValue)>=1000000000D THEN concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(priorWeekAbsoluteDiffValue/1000000000D,1),'\\\\\\\\\\\\\\\\.0$',''),'B')
                     WHEN abs(priorWeekAbsoluteDiffValue)>=1000000D THEN concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(priorWeekAbsoluteDiffValue/1000000D,1),'\\\\\\\\\\\\\\\\.0$',''),'M')
                     WHEN abs(priorWeekAbsoluteDiffValue)>=1000D THEN concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(priorWeekAbsoluteDiffValue/1000D,1),'\\\\\\\\\\\\\\\\.0$',''),'K')
                     ELSE concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(priorWeekAbsoluteDiffValue,0)) END AS priorWeekAbsoluteDiffDisplay,
                CASE WHEN priorWeekChangeValue IS NULL THEN NULL WHEN changeUnit='pp'
                     THEN concat(CASE WHEN priorWeekChangeValue>0D THEN '+' ELSE '' END,format_number(priorWeekChangeValue,1),'pp')
                     ELSE concat(CASE WHEN priorWeekChangeValue>0D THEN '+' ELSE '' END,format_number(priorWeekChangeValue,1),'%') END AS priorWeekChangeDisplay,
                CASE WHEN priorWeekImpactOnToplineValue IS NULL THEN NULL WHEN metricKind='ratio'
                     THEN concat(CASE WHEN priorWeekImpactOnToplineValue>0D THEN '+' ELSE '' END,format_number(priorWeekImpactOnToplineValue,1),'pp')
                     ELSE concat(CASE WHEN priorWeekImpactOnToplineValue>0D THEN '+' ELSE '' END,format_number(priorWeekImpactOnToplineValue,1),'%') END AS priorWeekImpactOnToplineDisplay,
                CASE WHEN fourWeekAbsoluteDiffValue IS NULL THEN NULL
                     WHEN metricKind='ratio' THEN concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(100D*fourWeekAbsoluteDiffValue,1),'pp')
                     WHEN abs(fourWeekAbsoluteDiffValue)>=1000000000D THEN concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(fourWeekAbsoluteDiffValue/1000000000D,1),'\\\\\\\\\\\\\\\\.0$',''),'B')
                     WHEN abs(fourWeekAbsoluteDiffValue)>=1000000D THEN concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(fourWeekAbsoluteDiffValue/1000000D,1),'\\\\\\\\\\\\\\\\.0$',''),'M')
                     WHEN abs(fourWeekAbsoluteDiffValue)>=1000D THEN concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(fourWeekAbsoluteDiffValue/1000D,1),'\\\\\\\\\\\\\\\\.0$',''),'K')
                     ELSE concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(fourWeekAbsoluteDiffValue,0)) END AS fourWeekAbsoluteDiffDisplay,
                CASE WHEN fourWeekChangeValue IS NULL THEN NULL WHEN changeUnit='pp'
                     THEN concat(CASE WHEN fourWeekChangeValue>0D THEN '+' ELSE '' END,format_number(fourWeekChangeValue,1),'pp')
                     ELSE concat(CASE WHEN fourWeekChangeValue>0D THEN '+' ELSE '' END,format_number(fourWeekChangeValue,1),'%') END AS fourWeekChangeDisplay,
                CASE WHEN fourWeekImpactOnToplineValue IS NULL THEN NULL WHEN metricKind='ratio'
                     THEN concat(CASE WHEN fourWeekImpactOnToplineValue>0D THEN '+' ELSE '' END,format_number(fourWeekImpactOnToplineValue,1),'pp')
                     ELSE concat(CASE WHEN fourWeekImpactOnToplineValue>0D THEN '+' ELSE '' END,format_number(fourWeekImpactOnToplineValue,1),'%') END AS fourWeekImpactOnToplineDisplay,
                CASE WHEN lastYearAbsoluteDiffValue IS NULL THEN NULL
                     WHEN metricKind='ratio' THEN concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(100D*lastYearAbsoluteDiffValue,1),'pp')
                     WHEN abs(lastYearAbsoluteDiffValue)>=1000000000D THEN concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(lastYearAbsoluteDiffValue/1000000000D,1),'\\\\\\\\\\\\\\\\.0$',''),'B')
                     WHEN abs(lastYearAbsoluteDiffValue)>=1000000D THEN concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(lastYearAbsoluteDiffValue/1000000D,1),'\\\\\\\\\\\\\\\\.0$',''),'M')
                     WHEN abs(lastYearAbsoluteDiffValue)>=1000D THEN concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(lastYearAbsoluteDiffValue/1000D,1),'\\\\\\\\\\\\\\\\.0$',''),'K')
                     ELSE concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(lastYearAbsoluteDiffValue,0)) END AS lastYearAbsoluteDiffDisplay,
                CASE WHEN lastYearChangeValue IS NULL THEN NULL WHEN changeUnit='pp'
                     THEN concat(CASE WHEN lastYearChangeValue>0D THEN '+' ELSE '' END,format_number(lastYearChangeValue,1),'pp')
                     ELSE concat(CASE WHEN lastYearChangeValue>0D THEN '+' ELSE '' END,format_number(lastYearChangeValue,1),'%') END AS lastYearChangeDisplay,
                CASE WHEN lastYearImpactOnToplineValue IS NULL THEN NULL WHEN metricKind='ratio'
                     THEN concat(CASE WHEN lastYearImpactOnToplineValue>0D THEN '+' ELSE '' END,format_number(lastYearImpactOnToplineValue,1),'pp')
                     ELSE concat(CASE WHEN lastYearImpactOnToplineValue>0D THEN '+' ELSE '' END,format_number(lastYearImpactOnToplineValue,1),'%') END AS lastYearImpactOnToplineDisplay,
                CASE WHEN peerSetValue IS NULL THEN NULL
                     WHEN displayFormat='percent' THEN concat(format_number(100D*peerSetValue,1),'%')
                     WHEN abs(peerSetValue)>=1000000000D THEN concat(regexp_replace(format_number(peerSetValue/1000000000D,1),'\\\\\\\\\\\\\\\\.0$',''),'B')
                     WHEN abs(peerSetValue)>=1000000D THEN concat(regexp_replace(format_number(peerSetValue/1000000D,1),'\\\\\\\\\\\\\\\\.0$',''),'M')
                     WHEN abs(peerSetValue)>=1000D THEN concat(regexp_replace(format_number(peerSetValue/1000D,1),'\\\\\\\\\\\\\\\\.0$',''),'K')
                     ELSE format_number(peerSetValue,0) END AS peerSetValueDisplay,
                CASE WHEN peerSetAbsoluteDiffValue IS NULL THEN NULL
                     WHEN metricKind='ratio' THEN concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(100D*peerSetAbsoluteDiffValue,1),'pp')
                     WHEN abs(peerSetAbsoluteDiffValue)>=1000000000D THEN concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(peerSetAbsoluteDiffValue/1000000000D,1),'\\\\\\\\\\\\\\\\.0$',''),'B')
                     WHEN abs(peerSetAbsoluteDiffValue)>=1000000D THEN concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(peerSetAbsoluteDiffValue/1000000D,1),'\\\\\\\\\\\\\\\\.0$',''),'M')
                     WHEN abs(peerSetAbsoluteDiffValue)>=1000D THEN concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(peerSetAbsoluteDiffValue/1000D,1),'\\\\\\\\\\\\\\\\.0$',''),'K')
                     ELSE concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(peerSetAbsoluteDiffValue,0)) END AS peerSetAbsoluteDiffDisplay,
                CASE WHEN peerSetChangeValue IS NULL THEN NULL
                     ELSE concat(CASE WHEN peerSetChangeValue>0D THEN '+' ELSE '' END,format_number(peerSetChangeValue,1),'pp') END AS peerSetChangeDisplay
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
            currentValue,currentValueDisplay,
            priorWeekDataAvailable,priorWeekValue,priorWeekAbsoluteDiffValue,priorWeekAbsoluteDiffDisplay,
            priorWeekChangeValue,priorWeekChangeDisplay,
            CASE WHEN priorWeekValue IS NULL OR currentValue IS NULL THEN 'unavailable'
                 WHEN currentValue>priorWeekValue THEN 'up' WHEN currentValue<priorWeekValue THEN 'down' ELSE 'flat' END AS priorWeekDirection,
            priorWeekImpactOnToplineValue,priorWeekImpactOnToplineDisplay,priorWeekGlobalRank,priorWeekCandidateIntersectionCount,
            fourWeekDataAvailable,fourWeekWindowComplete,fourWeekValue,fourWeekAbsoluteDiffValue,fourWeekAbsoluteDiffDisplay,
            fourWeekChangeValue,fourWeekChangeDisplay,
            CASE WHEN fourWeekValue IS NULL OR currentValue IS NULL THEN 'unavailable'
                 WHEN currentValue>fourWeekValue THEN 'up' WHEN currentValue<fourWeekValue THEN 'down' ELSE 'flat' END AS fourWeekDirection,
            fourWeekImpactOnToplineValue,fourWeekImpactOnToplineDisplay,fourWeekGlobalRank,fourWeekCandidateIntersectionCount,
            sameWeekLyDataAvailable AS lastYearDataAvailable,lastYearValue,lastYearAbsoluteDiffValue,lastYearAbsoluteDiffDisplay,
            lastYearChangeValue,lastYearChangeDisplay,
            CASE WHEN lastYearValue IS NULL OR currentValue IS NULL THEN 'unavailable'
                 WHEN currentValue>lastYearValue THEN 'up' WHEN currentValue<lastYearValue THEN 'down' ELSE 'flat' END AS lastYearDirection,
            lastYearImpactOnToplineValue,lastYearImpactOnToplineDisplay,lastYearGlobalRank,lastYearCandidateIntersectionCount,
            peerSetDataAvailable,peerSetValue,peerSetValueDisplay,peerSetAbsoluteDiffValue,peerSetAbsoluteDiffDisplay,peerSetChangeValue,peerSetChangeDisplay,
            CASE WHEN metricKind='count' THEN 'pct' WHEN metricKind='ratio' THEN 'pp' END AS impactOnToplineUnit,
            25 AS candidatePerPairLimit,
            100 AS retainedGlobalLimit,
            18 AS uiResultLimit,
            'defaultAll' AS exploreFilterMode,
            FALSE AS supportsArbitraryExploreFilters,
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreBase_wide' AS dynamicFilterSourceObject,
            v_processedAt AS appProcessedAt
        FROM formatted
        )
        MERGE INTO prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreRankedPairs_long AS t
        USING sourceRows AS s
          ON t.targetWeekStartDate = s.targetWeekStartDate
         AND t.filterLob <=> s.filterLob
         AND t.filterPlatform <=> s.filterPlatform
         AND t.metricName <=> s.metricName
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
            CASE WHEN v_asOfDate<v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,
            'priorWeek | fourWeek | lastYear' AS supportedComparisons,
            25 AS candidatePerPairLimit,
            100 AS retainedGlobalLimit,
            18 AS uiResultLimit,
            'defaultAll' AS exploreFilterMode,
            FALSE AS supportsArbitraryExploreFilters,
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreBase_wide' AS dynamicFilterSourceObject,
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreRankedPairs_long' AS targetObject,
            v_processedAt AS appProcessedAt;
    END IF;
END;
-- ============================================================================
-- DEVELOPMENT / DEPLOYMENT EXAMPLES
-- ============================================================================
-- ONE-TIME MIGRATION:
-- The physical contract changed from comparator-long to comparator-wide.
-- Run separately before the first execution:
-- DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreRankedPairs_long;
-- Preflight only:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appExploreRankedPairs_long(
--     p_asOfDate=>DATE '2026-09-28',
--     p_weeksToRebuild=>1,
--     p_validateOnly=>TRUE
-- );
-- Rebuild latest week:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appExploreRankedPairs_long(
--     p_asOfDate=>DATE '2026-09-28',
--     p_weeksToRebuild=>1,
--     p_validateOnly=>FALSE
-- );
-- Backfill / rebuild multiple weeks:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appExploreRankedPairs_long(
--     p_asOfDate=>DATE '2026-09-28',
--     p_weeksToRebuild=>12,
--     p_validateOnly=>FALSE
-- );
-- PRIOR WEEK selected:
-- SELECT *
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreRankedPairs_long
-- WHERE targetWeekStartDate=?
--   AND metricName=?
--   AND pairKey<>?                 -- currently-open matrix canonical pair
--   AND priorWeekGlobalRank IS NOT NULL
-- ORDER BY priorWeekGlobalRank
-- LIMIT 18;
-- 4-WK selected:
-- SELECT *
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreRankedPairs_long
-- WHERE targetWeekStartDate=?
--   AND metricName=?
--   AND pairKey<>?
--   AND fourWeekGlobalRank IS NOT NULL
-- ORDER BY fourWeekGlobalRank
-- LIMIT 18;
-- LAST YEAR selected:
-- SELECT *
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreRankedPairs_long
-- WHERE targetWeekStartDate=?
--   AND metricName=?
--   AND pairKey<>?
--   AND lastYearGlobalRank IS NOT NULL
-- ORDER BY lastYearGlobalRank
-- LIMIT 18;
-- The UI always renders all three visible comparator columns from the SAME row:
--   priorWeekAbsoluteDiffDisplay + priorWeekChangeDisplay
--   fourWeekAbsoluteDiffDisplay + fourWeekChangeDisplay
--   lastYearAbsoluteDiffDisplay + lastYearChangeDisplay
-- The selected comparator only chooses ORDER BY rank and the pink Impact-on-topline field.
-- Arbitrary Explore Add-filter / Quick-filter states:
-- DO NOT query this persisted fast-path table.
-- Query prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreBase_wide.
-- Apply filters FIRST, then aggregate/rank. This is required for mathematically-correct
-- Prospects, Base users, Mobile web, Paid Search, Shop & browse, Freedom Explorer,
-- In cart and custom user filters.
-- Validate duplicate physical grain; expected result = 0 rows:
-- SELECT
--     targetWeekStartDate,filterLob,filterPlatform,metricName,
--     pairKey,rowBreakoutValue,columnBreakoutValue,count(*) AS rowCount
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreRankedPairs_long
-- GROUP BY
--     targetWeekStartDate,filterLob,filterPlatform,metricName,
--     pairKey,rowBreakoutValue,columnBreakoutValue
-- HAVING count(*)>1
-- ORDER BY rowCount DESC;
-- Validate row volume:
-- SELECT targetWeekStartDate,count(*) AS rows,count(DISTINCT pairKey) AS pairs,count(DISTINCT metricName) AS metrics
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreRankedPairs_long
-- GROUP BY targetWeekStartDate
-- ORDER BY targetWeekStartDate;
-- Peer set:
-- Suppress the UI peer-set column when peerSetDataAvailable=FALSE.
-- peerSetValue is the approved four-week counterfactual supplied by analytical
-- Crosstab Gold; peerSetChangeValue is the points gap versus that counterfactual.
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
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appExploreRankedPairs_long(
--   p_asOfDate       => DATE '{as_of_date.isoformat()}',
--   p_weeksToRebuild => {int(weeks_to_rebuild)},
--   p_validateOnly   => {validate_literal}
-- )
-- """
-- result = spark.sql(call_sql).collect()
-- display(result)
