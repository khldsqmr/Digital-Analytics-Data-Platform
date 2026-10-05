-- ###########################################################################
-- BEGIN 03_sdi_sp_mip_gold_appOverviewToplineMovers_long.sql
-- ###########################################################################
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
--
-- PEER-SET CONTRACT:
--   - Analytical Gold peerSetNumerator / peerSetDenominator represent the
--     FOUR-WEEK PEER COUNTERFACTUAL for the selected raw slice:
--       "what would this slice be now if it had moved at the peer-set rate?"
--   - peerSetAbsoluteDiffValue = actual current - peer counterfactual.
--   - For count metrics, peerSetChangeValue is the percentage-POINT gap between
--     the slice's four-week change and the peer-set four-week change:
--         100 * (currentValue - peerSetValue) / fourWeekValue
--   - For ratio metrics, peerSetChangeValue is the percentage-point difference:
--         100 * (currentValue - peerSetValue)
--   - Peer set ALWAYS uses the four-week baseline, regardless of comparisonType.
--   - Synthetic Top100 remainder '(Other)' buckets do NOT receive a peer value;
--     peer counterfactuals are overlapping/non-additive and must never be summed.
--
-- IMPACT-ON-TOPLINE CONTRACT:
--   comparisonType controls impact:
--     count -> 100 * (slice current - slice comparison) / topline comparison
--     ratio -> percentage-point contribution using topline denominators.
--
-- SCHEMA / API COMPATIBILITY:
--   This revision changes calculation logic only. The persisted App Gold schema
--   and API-facing columns are unchanged; no table drop is required solely for
--   this peer-set correction.
-- ============================================================================
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
                -- Peer counterfactuals are valid only for a real raw member.
                -- A synthetic '(Other)' bucket can contain multiple overlapping
                -- populations, so its peer counterfactual is intentionally NULL.
                CASE
                    WHEN max(CASE WHEN isSyntheticOtherMember THEN 1 ELSE 0 END)=1
                        THEN cast(NULL AS DOUBLE)
                    ELSE max(peerSetNumerator)
                END AS peerSetNumerator,
                CASE
                    WHEN max(CASE WHEN isSyntheticOtherMember THEN 1 ELSE 0 END)=1
                        THEN cast(NULL AS DOUBLE)
                    ELSE max(peerSetDenominator)
                END AS peerSetDenominator,
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
                    WHEN currentValue IS NULL OR peerSetValue IS NULL OR fourWeekValue IS NULL THEN NULL
                    -- Ratio metrics are already proportions; the gap is in percentage points.
                    WHEN changeUnit='pp' THEN 100D*(currentValue-peerSetValue)
                    -- Count metrics: compare the actual-vs-counterfactual excess to
                    -- the selected slice's OWN four-week baseline. This equals:
                    -- slice 4-week change % - peer-set 4-week change %.
                    WHEN changeUnit='pct' THEN 100D*try_divide(currentValue-peerSetValue,fourWeekValue)
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
                        THEN concat(regexp_replace(format_number(currentValue/1000000000D,1),'\\\\.0$',''),'B')
                    WHEN abs(currentValue)>=1000000D
                        THEN concat(regexp_replace(format_number(currentValue/1000000D,1),'\\\\.0$',''),'M')
                    WHEN abs(currentValue)>=1000D
                        THEN concat(regexp_replace(format_number(currentValue/1000D,1),'\\\\.0$',''),'K')
                    ELSE format_number(currentValue,0)
                END AS currentValueDisplay,
                CASE
                    WHEN priorWeekAbsoluteDiffValue IS NULL THEN NULL
                    WHEN metricKind='ratio'
                        THEN concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                    format_number(100D*priorWeekAbsoluteDiffValue,1),'pp')
                    WHEN abs(priorWeekAbsoluteDiffValue)>=1000000000D
                        THEN concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                    regexp_replace(format_number(priorWeekAbsoluteDiffValue/1000000000D,1),'\\\\.0$',''),'B')
                    WHEN abs(priorWeekAbsoluteDiffValue)>=1000000D
                        THEN concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                    regexp_replace(format_number(priorWeekAbsoluteDiffValue/1000000D,1),'\\\\.0$',''),'M')
                    WHEN abs(priorWeekAbsoluteDiffValue)>=1000D
                        THEN concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                    regexp_replace(format_number(priorWeekAbsoluteDiffValue/1000D,1),'\\\\.0$',''),'K')
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
                                    regexp_replace(format_number(fourWeekAbsoluteDiffValue/1000000000D,1),'\\\\.0$',''),'B')
                    WHEN abs(fourWeekAbsoluteDiffValue)>=1000000D
                        THEN concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                    regexp_replace(format_number(fourWeekAbsoluteDiffValue/1000000D,1),'\\\\.0$',''),'M')
                    WHEN abs(fourWeekAbsoluteDiffValue)>=1000D
                        THEN concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                    regexp_replace(format_number(fourWeekAbsoluteDiffValue/1000D,1),'\\\\.0$',''),'K')
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
                                    regexp_replace(format_number(lastYearAbsoluteDiffValue/1000000000D,1),'\\\\.0$',''),'B')
                    WHEN abs(lastYearAbsoluteDiffValue)>=1000000D
                        THEN concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                    regexp_replace(format_number(lastYearAbsoluteDiffValue/1000000D,1),'\\\\.0$',''),'M')
                    WHEN abs(lastYearAbsoluteDiffValue)>=1000D
                        THEN concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                    regexp_replace(format_number(lastYearAbsoluteDiffValue/1000D,1),'\\\\.0$',''),'K')
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
                        THEN concat(regexp_replace(format_number(peerSetValue/1000000000D,1),'\\\\.0$',''),'B')
                    WHEN abs(peerSetValue)>=1000000D
                        THEN concat(regexp_replace(format_number(peerSetValue/1000000D,1),'\\\\.0$',''),'M')
                    WHEN abs(peerSetValue)>=1000D
                        THEN concat(regexp_replace(format_number(peerSetValue/1000D,1),'\\\\.0$',''),'K')
                    ELSE format_number(peerSetValue,0)
                END AS peerSetValueDisplay,
                CASE
                    WHEN peerSetAbsoluteDiffValue IS NULL THEN NULL
                    WHEN metricKind='ratio'
                        THEN concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                    format_number(100D*peerSetAbsoluteDiffValue,1),'pp')
                    WHEN abs(peerSetAbsoluteDiffValue)>=1000000000D
                        THEN concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                    regexp_replace(format_number(peerSetAbsoluteDiffValue/1000000000D,1),'\\\\.0$',''),'B')
                    WHEN abs(peerSetAbsoluteDiffValue)>=1000000D
                        THEN concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                    regexp_replace(format_number(peerSetAbsoluteDiffValue/1000000D,1),'\\\\.0$',''),'M')
                    WHEN abs(peerSetAbsoluteDiffValue)>=1000D
                        THEN concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                    regexp_replace(format_number(peerSetAbsoluteDiffValue/1000D,1),'\\\\.0$',''),'K')
                    ELSE concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                                format_number(peerSetAbsoluteDiffValue,0))
                END AS peerSetAbsoluteDiffDisplay,
                CASE
                    WHEN peerSetChangeValue IS NULL THEN NULL
                    -- Peer-set change is always a gap between two change rates,
                    -- therefore its display unit is percentage points.
                    ELSE concat(CASE WHEN peerSetChangeValue>0D THEN '+' ELSE '' END,format_number(peerSetChangeValue,1),'pp')
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
-- No schema migration is required for this peer-set correction.
-- Re-run the procedure for the desired historical week range after rebuilding
-- corrected analytical Breakout Gold + Overview Gold.
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

-- ============================================================================
-- PEER-SET VALIDATION EXAMPLE
-- Expected: synthetic Other rows have NULL peer outputs; raw rows with an
-- analytical peer counterfactual have a four-week baseline.
-- ============================================================================
-- SELECT
--     targetWeekStartDate,metricName,breakoutType,breakoutValue,isOtherBucket,
--     currentValue,peerSetValue,peerSetAbsoluteDiffValue,peerSetChangeValue
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewToplineMovers_long
-- WHERE targetWeekStartDate=DATE '2026-08-09'
--   AND metricName='nbv'
--   AND comparisonType='fourWeek'
-- ORDER BY impactRankAcrossBreakouts;
-- ###########################################################################
-- END 03_sdi_sp_mip_gold_appOverviewToplineMovers_long.sql
-- ###########################################################################

-- ###########################################################################
-- BEGIN 05_sdi_sp_mip_gold_appBreakoutsComparisonTable_long.sql
-- ###########################################################################
-- ============================================================================
-- FILE  : 05_sdi_sp_mip_gold_appBreakoutsComparisonTable_long.sql
-- LAYER : GOLD / APP
-- TAB   : Breakouts
-- SECTION: Comparison table
--
-- PURPOSE:
--   Application-ready breakout comparison table.
--
-- API FILTERS:
--   fiscalYear
--   fiscalQuarterLabel
--   targetWeekStartDate
--   metricName
--   breakoutType
--   comparisonType = priorWeek | fourWeek | lastYear
--   filterLob
--   filterPlatform
--
-- APP-GOLD ELIGIBILITY:
--   metric     -> isActive AND showOnBreakouts
--   breakout   -> isActive AND isPrebuiltBreakout
--
-- CONTRACT:
--   Each slice row already contains:
--     This week
--     Vs prior week
--     Vs 4-wk trend
--     Vs same wk LY
--     Vs peer set
--     Impact on topline
--
--   comparisonType controls impact/ranking/Top100+(Other), but does NOT remove
--   the other visible comparison columns.
--
--   A final Topline row is appended for every breakout.
--
--   Numerators/denominators are internal calculation ingredients only.
--
-- PEER-SET CONTRACT:
--   - peerSetNumerator / peerSetDenominator from analytical Breakout Gold are the
--     FOUR-WEEK PEER COUNTERFACTUAL for the raw slice, not raw peer-population
--     totals and not the selected comparator.
--   - peerSetAbsoluteDiffValue = actual current - peer counterfactual.
--   - count peer gap = 100 * (current - peer counterfactual) / slice four-week baseline.
--   - ratio peer gap = 100 * (current ratio - peer counterfactual ratio).
--   - The peer basis is always four-week even when comparisonType is priorWeek
--     or lastYear.
--   - Synthetic '(Other)' rows do not expose peer values because overlapping
--     peer counterfactuals are non-additive.
--
-- IMPACT-ON-TOPLINE CONTRACT:
--   comparisonType continues to control impact/rank/Top100:
--     count -> (slice movement) / topline comparison baseline
--     ratio -> percentage-point contribution using topline denominators.
--
-- SCHEMA / API COMPATIBILITY:
--   No persisted App Gold columns are added/removed/renamed in this revision.
-- ============================================================================
CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_gold_appBreakoutsComparisonTable_long(
    IN p_asOfDate DATE DEFAULT NULL,
    IN p_weeksToRebuild INT DEFAULT 1,
    IN p_validateOnly BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'MIP Gold App: Breakouts comparison table with section-controlled metrics, wide visible comparisons, comparator-aware impact/ranking and final Topline row.'
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
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='p_weeksToRebuild must be >= 1.';
    END IF;
    SET v_weekTo=date_add(v_asOfDate,1-dayofweek(v_asOfDate));
    SET v_weekFrom=date_add(v_weekTo,-7*(p_weeksToRebuild-1));
    SET v_weekEndTo=date_add(v_weekTo,6);
    -- =========================================================================
    -- 1. Eligible source preflight
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
            SET MESSAGE_TEXT='Breakout Gold has no eligible Breakouts App rows for the requested target-week range.';
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
        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='Fiscal Calendar has no rows for the requested target-week range.';
    END IF;
    -- =========================================================================
    -- 2. Grain / metadata validation only for eligible Breakouts content
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
            SET MESSAGE_TEXT='Duplicate eligible Breakout Gold analytical keys detected.';
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
    IF p_validateOnly THEN
        SELECT
            'VALIDATION_ONLY' AS status,
            v_weekFrom AS rebuildWeekStartFrom,
            v_weekTo AS rebuildWeekStartTo,
            v_weekEndTo AS latestWeekEndDate,
            CASE WHEN v_asOfDate<v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,
            'priorWeek | fourWeek | lastYear' AS supportedComparisons,
            'isActive=true AND showOnBreakouts=true' AS metricEligibility,
            'isActive=true AND isPrebuiltBreakout=true' AS breakoutEligibility,
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsComparisonTable_long' AS targetObject,
            'Validation passed. No Gold App table was created or modified.' AS message;
    ELSE
        -- =====================================================================
        -- 3. Target App contract
        -- =====================================================================
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsComparisonTable_long(
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
            comparisonType STRING,
            comparisonLabel STRING,
            comparisonSortOrder INT,
            comparisonDataAvailable BOOLEAN,
            comparisonWindowComplete BOOLEAN,
            priorWeekDataAvailable BOOLEAN,
            fourWeekDataAvailable BOOLEAN,
            fourWeekWindowComplete BOOLEAN,
            lastYearDataAvailable BOOLEAN,
            peerSetDataAvailable BOOLEAN,
            breakoutType STRING,
            breakoutLabel STRING,
            breakoutValue STRING,
            breakoutSortOrder INT,
            rowType STRING,
            isTopline BOOLEAN,
            isOtherBucket BOOLEAN,
            displayRankWithinBreakout BIGINT,
            rowSortOrder BIGINT,
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
            appProcessedAt TIMESTAMP
        )
        USING DELTA
        CLUSTER BY(targetWeekStartDate,metricName,breakoutType,comparisonType)
        COMMENT 'MIP Gold App: Breakouts comparison table. Only approved Breakouts metrics and active prebuilt breakouts; all visible comparisons persisted; selected comparator controls impact/ranking; Topline row included.';
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsComparisonTable_long
        REPLACE WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        WITH scopeBreakouts AS(
            SELECT
                g.targetWeekStartDate,g.targetWeekEndDate,g.fiscalQuarterLabel,g.fiscalWeekCode,g.weekLabel,
                g.filterLob,g.filterPlatform,g.breakoutType,g.breakoutValue,g.metricName,
                g.thisWeekNumerator,g.thisWeekDenominator,
                g.priorWeekNumerator,g.priorWeekDenominator,
                g.fourWeekTrendNumerator,g.fourWeekTrendDenominator,
                g.sameWeekLyNumerator,g.sameWeekLyDenominator,
                g.peerSetNumerator,g.peerSetDenominator,
                g.thisWeekDataAvailable,g.priorWeekDataAvailable,g.fourWeekTrendWeekCount,g.sameWeekLyDataAvailable
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
        ),
        scopeOverview AS(
            SELECT
                g.targetWeekStartDate,g.targetWeekEndDate,g.fiscalQuarterLabel,g.fiscalWeekCode,g.weekLabel,
                g.filterLob,g.filterPlatform,g.metricName,g.metricKind,
                g.thisWeekNumerator,g.thisWeekDenominator,
                g.priorWeekNumerator,g.priorWeekDenominator,
                g.fourWeekTrendNumerator,g.fourWeekTrendDenominator,
                g.sameWeekLyNumerator,g.sameWeekLyDenominator,
                g.thisWeekDataAvailable,g.priorWeekDataAvailable,g.fourWeekTrendWeekCount,g.sameWeekLyDataAvailable
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long g
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
              ON mc.metricName=g.metricName
             AND mc.isActive
             AND mc.showOnBreakouts
            WHERE g.targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        ),
        base AS(
            SELECT
                g.targetWeekStartDate,g.targetWeekEndDate,
                c.fiscalYear,g.fiscalQuarterLabel,g.fiscalWeekCode,g.weekLabel,c.weekEndingLabel,
                g.filterLob,g.filterPlatform,
                g.metricName,
                CASE WHEN g.metricName='nbv' THEN 'Total UPV' ELSE mc.metricLabel END AS metricLabel,
                mc.metricDescription,mc.metricKind,mc.displayFormat,mc.changeUnit,mc.sortOrder AS metricSortOrder,
                g.breakoutType,bc.breakoutLabel,g.breakoutValue,bc.sortOrder AS breakoutSortOrder,
                g.thisWeekNumerator,g.thisWeekDenominator,
                g.priorWeekNumerator,g.priorWeekDenominator,
                g.fourWeekTrendNumerator,g.fourWeekTrendDenominator,
                g.sameWeekLyNumerator,g.sameWeekLyDenominator,
                g.peerSetNumerator,g.peerSetDenominator,
                g.thisWeekDataAvailable,g.priorWeekDataAvailable,g.fourWeekTrendWeekCount,g.sameWeekLyDataAvailable
            FROM scopeBreakouts g
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static bc
              ON bc.breakoutType=g.breakoutType
             AND bc.isActive
             AND bc.isPrebuiltBreakout
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
              ON mc.metricName=g.metricName
             AND mc.isActive
             AND mc.showOnBreakouts
            LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c
              ON c.weekStartDate=g.targetWeekStartDate
        ),
        toplineValues AS(
            SELECT
                g.targetWeekStartDate,g.targetWeekEndDate,
                c.fiscalYear,g.fiscalQuarterLabel,g.fiscalWeekCode,g.weekLabel,c.weekEndingLabel,
                g.filterLob,g.filterPlatform,
                g.metricName,
                CASE WHEN g.metricName='nbv' THEN 'Total UPV' ELSE mc.metricLabel END AS metricLabel,
                mc.metricDescription,mc.metricKind,mc.displayFormat,mc.changeUnit,mc.sortOrder AS metricSortOrder,
                g.thisWeekNumerator AS currentNumerator,
                g.thisWeekDenominator AS currentDenominator,
                g.priorWeekNumerator,g.priorWeekDenominator,
                g.fourWeekTrendNumerator,g.fourWeekTrendDenominator,g.fourWeekTrendWeekCount,
                g.sameWeekLyNumerator,g.sameWeekLyDenominator,
                g.thisWeekDataAvailable,
                g.priorWeekDataAvailable,
                g.fourWeekTrendWeekCount>0 AS fourWeekDataAvailable,
                g.fourWeekTrendWeekCount=4 AS fourWeekWindowComplete,
                g.sameWeekLyDataAvailable AS lastYearDataAvailable,
                CASE
                    WHEN NOT g.thisWeekDataAvailable THEN NULL
                    WHEN mc.metricKind='ratio' THEN try_divide(g.thisWeekNumerator,g.thisWeekDenominator)
                    ELSE g.thisWeekNumerator
                END AS currentValue,
                CASE
                    WHEN NOT g.priorWeekDataAvailable THEN NULL
                    WHEN mc.metricKind='ratio' THEN try_divide(g.priorWeekNumerator,g.priorWeekDenominator)
                    ELSE g.priorWeekNumerator
                END AS priorWeekValue,
                CASE
                    WHEN g.fourWeekTrendWeekCount<=0 THEN NULL
                    WHEN mc.metricKind='ratio' THEN try_divide(g.fourWeekTrendNumerator,g.fourWeekTrendDenominator)
                    ELSE try_divide(g.fourWeekTrendNumerator,cast(g.fourWeekTrendWeekCount AS DOUBLE))
                END AS fourWeekValue,
                CASE
                    WHEN NOT g.sameWeekLyDataAvailable THEN NULL
                    WHEN mc.metricKind='ratio' THEN try_divide(g.sameWeekLyNumerator,g.sameWeekLyDenominator)
                    ELSE g.sameWeekLyNumerator
                END AS lastYearValue
            FROM scopeOverview g
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
              ON mc.metricName=g.metricName
             AND mc.isActive
             AND mc.showOnBreakouts
            LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c
              ON c.weekStartDate=g.targetWeekStartDate
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
        rawSelected AS(
            SELECT
                r.*,
                'priorWeek' AS comparisonType,'Prior week' AS comparisonLabel,10 AS comparisonSortOrder,
                priorWeekDataAvailable AS comparisonDataAvailable,
                priorWeekDataAvailable AS comparisonWindowComplete,
                priorWeekValue AS selectedComparisonValue,
                priorWeekNumerator AS selectedComparisonNumerator,
                priorWeekDenominator AS selectedComparisonDenominator
            FROM rawValues r
            UNION ALL
            SELECT
                r.*,
                'fourWeek','4-wk trend',20,
                fourWeekTrendWeekCount>0,
                fourWeekTrendWeekCount=4,
                fourWeekValue,
                CASE
                    WHEN metricKind='count' AND fourWeekTrendWeekCount>0
                        THEN try_divide(fourWeekTrendNumerator,cast(fourWeekTrendWeekCount AS DOUBLE))
                    ELSE fourWeekTrendNumerator
                END,
                CASE WHEN metricKind='count' THEN NULL ELSE fourWeekTrendDenominator END
            FROM rawValues r
            UNION ALL
            SELECT
                r.*,
                'lastYear','Same wk LY',30,
                sameWeekLyDataAvailable,
                sameWeekLyDataAvailable,
                lastYearValue,
                sameWeekLyNumerator,
                sameWeekLyDenominator
            FROM rawValues r
        ),
        rawWithTopline AS(
            SELECT
                r.*,
                t.currentValue AS toplineCurrentValue,
                t.currentDenominator AS toplineCurrentDenominator,
                CASE r.comparisonType
                    WHEN 'priorWeek' THEN t.priorWeekValue
                    WHEN 'fourWeek' THEN t.fourWeekValue
                    WHEN 'lastYear' THEN t.lastYearValue
                END AS selectedToplineComparisonValue,
                CASE r.comparisonType
                    WHEN 'priorWeek' THEN t.priorWeekDenominator
                    WHEN 'fourWeek' THEN CASE WHEN r.metricKind='count' THEN NULL ELSE t.fourWeekTrendDenominator END
                    WHEN 'lastYear' THEN t.sameWeekLyDenominator
                END AS selectedToplineComparisonDenominator
            FROM rawSelected r
            JOIN toplineValues t
              ON t.targetWeekStartDate=r.targetWeekStartDate
             AND t.filterLob=r.filterLob
             AND t.filterPlatform=r.filterPlatform
             AND t.metricName=r.metricName
        ),
        rawImpact AS(
            SELECT
                *,
                currentValue-selectedComparisonValue AS selectedAbsoluteDiffValue,
                CASE
                    WHEN NOT comparisonDataAvailable OR currentValue IS NULL OR selectedComparisonValue IS NULL THEN NULL
                    WHEN metricKind='count'
                        THEN 100D*try_divide(currentValue-selectedComparisonValue,selectedToplineComparisonValue)
                    WHEN metricKind='ratio'
                        THEN 100D*(
                            try_divide(thisWeekNumerator,toplineCurrentDenominator)
                            -try_divide(selectedComparisonNumerator,selectedToplineComparisonDenominator)
                        )
                END AS impactOnToplineRaw
            FROM rawWithTopline
        ),
        rankedRaw AS(
            SELECT
                *,
                CASE
                    WHEN comparisonDataAvailable AND impactOnToplineRaw IS NOT NULL
                    THEN row_number() OVER(
                        PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName,breakoutType,comparisonType
                        ORDER BY
                            abs(impactOnToplineRaw) DESC NULLS LAST,
                            abs(selectedAbsoluteDiffValue) DESC NULLS LAST,
                            breakoutValue
                    )
                END AS rawImpactRankWithinBreakout
            FROM rawImpact
        ),
        bucketMembers AS(
            SELECT
                *,
                CASE
                    WHEN rawImpactRankWithinBreakout IS NULL
                        THEN concat('VALUE::',coalesce(breakoutValue,'(null)'))
                    WHEN rawImpactRankWithinBreakout<=100
                        THEN concat('VALUE::',coalesce(breakoutValue,'(null)'))
                    ELSE 'OTHER::REMAINDER'
                END AS displayBucketKey,
                CASE
                    WHEN rawImpactRankWithinBreakout IS NULL THEN breakoutValue
                    WHEN rawImpactRankWithinBreakout<=100 THEN breakoutValue
                    ELSE '(Other)'
                END AS displayBreakoutValue,
                rawImpactRankWithinBreakout>100 AS isSyntheticOtherMember
            FROM rankedRaw
        ),
        bucketAgg AS(
            SELECT
                targetWeekStartDate,targetWeekEndDate,fiscalYear,fiscalQuarterLabel,fiscalWeekCode,weekLabel,weekEndingLabel,
                filterLob,filterPlatform,
                metricName,metricLabel,metricDescription,metricKind,displayFormat,changeUnit,metricSortOrder,
                comparisonType,comparisonLabel,comparisonSortOrder,
                min(CASE WHEN comparisonDataAvailable THEN 1 ELSE 0 END)=1 AS comparisonDataAvailable,
                min(CASE WHEN comparisonWindowComplete THEN 1 ELSE 0 END)=1 AS comparisonWindowComplete,
                min(CASE WHEN priorWeekDataAvailable THEN 1 ELSE 0 END)=1 AS priorWeekDataAvailable,
                max(fourWeekTrendWeekCount)>0 AS fourWeekDataAvailable,
                max(fourWeekTrendWeekCount)=4 AS fourWeekWindowComplete,
                min(CASE WHEN sameWeekLyDataAvailable THEN 1 ELSE 0 END)=1 AS lastYearDataAvailable,
                breakoutType,breakoutLabel,displayBreakoutValue AS breakoutValue,breakoutSortOrder,
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
                -- Peer counterfactuals are non-additive. Preserve them only
                -- for a real raw member; suppress the synthetic '(Other)' bucket.
                CASE
                    WHEN max(CASE WHEN isSyntheticOtherMember THEN 1 ELSE 0 END)=1
                        THEN cast(NULL AS DOUBLE)
                    ELSE max(peerSetNumerator)
                END AS peerSetNumerator,
                CASE
                    WHEN max(CASE WHEN isSyntheticOtherMember THEN 1 ELSE 0 END)=1
                        THEN cast(NULL AS DOUBLE)
                    ELSE max(peerSetDenominator)
                END AS peerSetDenominator
            FROM bucketMembers
            GROUP BY
                targetWeekStartDate,targetWeekEndDate,fiscalYear,fiscalQuarterLabel,fiscalWeekCode,weekLabel,weekEndingLabel,
                filterLob,filterPlatform,
                metricName,metricLabel,metricDescription,metricKind,displayFormat,changeUnit,metricSortOrder,
                comparisonType,comparisonLabel,comparisonSortOrder,
                breakoutType,breakoutLabel,breakoutSortOrder,displayBucketKey,displayBreakoutValue
        ),
        bucketValues AS(
            SELECT
                *,
                CASE WHEN metricKind='ratio'
                     THEN try_divide(currentNumerator,currentDenominator)
                     ELSE currentNumerator END AS currentValue,
                CASE WHEN metricKind='ratio'
                     THEN try_divide(priorWeekNumerator,priorWeekDenominator)
                     ELSE priorWeekNumerator END AS priorWeekValue,
                CASE
                    WHEN metricKind='ratio' THEN try_divide(fourWeekTrendNumerator,fourWeekTrendDenominator)
                    WHEN fourWeekTrendWeekCount>0
                        THEN try_divide(fourWeekTrendNumerator,cast(fourWeekTrendWeekCount AS DOUBLE))
                    ELSE NULL
                END AS fourWeekValue,
                CASE WHEN metricKind='ratio'
                     THEN try_divide(sameWeekLyNumerator,sameWeekLyDenominator)
                     ELSE sameWeekLyNumerator END AS lastYearValue,
                CASE WHEN metricKind='ratio'
                     THEN try_divide(peerSetNumerator,peerSetDenominator)
                     ELSE peerSetNumerator END AS peerSetValue,
                CASE
                    WHEN metricKind='ratio'
                        THEN peerSetNumerator IS NOT NULL AND nullif(peerSetDenominator,0D) IS NOT NULL
                    ELSE peerSetNumerator IS NOT NULL
                END AS peerSetDataAvailable
            FROM bucketAgg
        ),
        bucketSelected AS(
            SELECT
                *,
                CASE comparisonType
                    WHEN 'priorWeek' THEN priorWeekValue
                    WHEN 'fourWeek' THEN fourWeekValue
                    WHEN 'lastYear' THEN lastYearValue
                END AS selectedComparisonValue,
                CASE comparisonType
                    WHEN 'priorWeek' THEN priorWeekNumerator
                    WHEN 'fourWeek' THEN
                        CASE
                            WHEN metricKind='count' AND fourWeekTrendWeekCount>0
                                THEN try_divide(fourWeekTrendNumerator,cast(fourWeekTrendWeekCount AS DOUBLE))
                            ELSE fourWeekTrendNumerator
                        END
                    WHEN 'lastYear' THEN sameWeekLyNumerator
                END AS selectedComparisonNumerator,
                CASE comparisonType
                    WHEN 'priorWeek' THEN priorWeekDenominator
                    WHEN 'fourWeek' THEN CASE WHEN metricKind='count' THEN NULL ELSE fourWeekTrendDenominator END
                    WHEN 'lastYear' THEN sameWeekLyDenominator
                END AS selectedComparisonDenominator
            FROM bucketValues
        ),
        bucketWithTopline AS(
            SELECT
                b.*,
                t.currentValue AS toplineCurrentValue,
                t.currentDenominator AS toplineCurrentDenominator,
                CASE b.comparisonType
                    WHEN 'priorWeek' THEN t.priorWeekValue
                    WHEN 'fourWeek' THEN t.fourWeekValue
                    WHEN 'lastYear' THEN t.lastYearValue
                END AS selectedToplineComparisonValue,
                CASE b.comparisonType
                    WHEN 'priorWeek' THEN t.priorWeekDenominator
                    WHEN 'fourWeek' THEN CASE WHEN b.metricKind='count' THEN NULL ELSE t.fourWeekTrendDenominator END
                    WHEN 'lastYear' THEN t.sameWeekLyDenominator
                END AS selectedToplineComparisonDenominator
            FROM bucketSelected b
            JOIN toplineValues t
              ON t.targetWeekStartDate=b.targetWeekStartDate
             AND t.filterLob=b.filterLob
             AND t.filterPlatform=b.filterPlatform
             AND t.metricName=b.metricName
        ),
        bucketCalculated AS(
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
                    WHEN currentValue IS NULL OR peerSetValue IS NULL OR fourWeekValue IS NULL THEN NULL
                    WHEN changeUnit='pp' THEN 100D*(currentValue-peerSetValue)
                    WHEN changeUnit='pct' THEN 100D*try_divide(currentValue-peerSetValue,fourWeekValue)
                END AS peerSetChangeRaw,
                currentValue-selectedComparisonValue AS selectedAbsoluteDiffValue,
                CASE
                    WHEN NOT comparisonDataAvailable OR currentValue IS NULL OR selectedComparisonValue IS NULL THEN NULL
                    WHEN metricKind='count'
                        THEN 100D*try_divide(currentValue-selectedComparisonValue,selectedToplineComparisonValue)
                    WHEN metricKind='ratio'
                        THEN 100D*(
                            try_divide(currentNumerator,toplineCurrentDenominator)
                            -try_divide(selectedComparisonNumerator,selectedToplineComparisonDenominator)
                        )
                END AS impactOnToplineRaw
            FROM bucketWithTopline
        ),
        sliceRanked AS(
            SELECT
                *,
                row_number() OVER(
                    PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName,breakoutType,comparisonType
                    ORDER BY
                        abs(impactOnToplineRaw) DESC NULLS LAST,
                        abs(selectedAbsoluteDiffValue) DESC NULLS LAST,
                        isOtherBucket,
                        breakoutValue
                ) AS displayRankWithinBreakout
            FROM bucketCalculated
        ),
        sliceRows AS(
            SELECT
                targetWeekStartDate,targetWeekEndDate,fiscalYear,fiscalQuarterLabel,fiscalWeekCode,weekLabel,weekEndingLabel,
                filterLob,filterPlatform,
                metricName,metricLabel,metricDescription,metricKind,displayFormat,changeUnit,metricSortOrder,
                comparisonType,comparisonLabel,comparisonSortOrder,comparisonDataAvailable,comparisonWindowComplete,
                priorWeekDataAvailable,fourWeekDataAvailable,fourWeekWindowComplete,lastYearDataAvailable,peerSetDataAvailable,
                breakoutType,breakoutLabel,breakoutValue,breakoutSortOrder,
                'slice' AS rowType,FALSE AS isTopline,isOtherBucket,
                displayRankWithinBreakout,
                displayRankWithinBreakout AS rowSortOrder,
                currentValue,
                priorWeekAbsoluteDiffValue,priorWeekChangeRaw,
                fourWeekAbsoluteDiffValue,fourWeekChangeRaw,
                lastYearAbsoluteDiffValue,lastYearChangeRaw,
                peerSetValue,peerSetAbsoluteDiffValue,peerSetChangeRaw,
                impactOnToplineRaw,
                CASE WHEN metricKind='count' THEN 'pct'
                     WHEN metricKind='ratio' THEN 'pp' END AS impactOnToplineUnit
            FROM sliceRanked
        ),
        availableBreakouts AS(
            SELECT DISTINCT
                targetWeekStartDate,filterLob,filterPlatform,metricName,
                breakoutType,breakoutLabel,breakoutSortOrder
            FROM base
        ),
        toplineExpanded AS(
            SELECT
                t.*,a.breakoutType,a.breakoutLabel,a.breakoutSortOrder,
                'priorWeek' AS comparisonType,'Prior week' AS comparisonLabel,10 AS comparisonSortOrder,
                t.priorWeekDataAvailable AS comparisonDataAvailable,
                t.priorWeekDataAvailable AS comparisonWindowComplete
            FROM toplineValues t
            JOIN availableBreakouts a
              ON a.targetWeekStartDate=t.targetWeekStartDate
             AND a.filterLob=t.filterLob
             AND a.filterPlatform=t.filterPlatform
             AND a.metricName=t.metricName
            UNION ALL
            SELECT
                t.*,a.breakoutType,a.breakoutLabel,a.breakoutSortOrder,
                'fourWeek','4-wk trend',20,
                t.fourWeekDataAvailable,
                t.fourWeekWindowComplete
            FROM toplineValues t
            JOIN availableBreakouts a
              ON a.targetWeekStartDate=t.targetWeekStartDate
             AND a.filterLob=t.filterLob
             AND a.filterPlatform=t.filterPlatform
             AND a.metricName=t.metricName
            UNION ALL
            SELECT
                t.*,a.breakoutType,a.breakoutLabel,a.breakoutSortOrder,
                'lastYear','Same wk LY',30,
                t.lastYearDataAvailable,
                t.lastYearDataAvailable
            FROM toplineValues t
            JOIN availableBreakouts a
              ON a.targetWeekStartDate=t.targetWeekStartDate
             AND a.filterLob=t.filterLob
             AND a.filterPlatform=t.filterPlatform
             AND a.metricName=t.metricName
        ),
        toplineDeltas AS(
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
                END AS lastYearChangeRaw
            FROM toplineExpanded
        ),
        toplineRows AS(
            SELECT
                targetWeekStartDate,targetWeekEndDate,fiscalYear,fiscalQuarterLabel,fiscalWeekCode,weekLabel,weekEndingLabel,
                filterLob,filterPlatform,
                metricName,metricLabel,metricDescription,metricKind,displayFormat,changeUnit,metricSortOrder,
                comparisonType,comparisonLabel,comparisonSortOrder,comparisonDataAvailable,comparisonWindowComplete,
                priorWeekDataAvailable,fourWeekDataAvailable,fourWeekWindowComplete,lastYearDataAvailable,
                FALSE AS peerSetDataAvailable,
                breakoutType,breakoutLabel,'Topline' AS breakoutValue,breakoutSortOrder,
                'topline' AS rowType,TRUE AS isTopline,FALSE AS isOtherBucket,
                cast(NULL AS BIGINT) AS displayRankWithinBreakout,
                cast(999999 AS BIGINT) AS rowSortOrder,
                currentValue,
                priorWeekAbsoluteDiffValue,priorWeekChangeRaw,
                fourWeekAbsoluteDiffValue,fourWeekChangeRaw,
                lastYearAbsoluteDiffValue,lastYearChangeRaw,
                cast(NULL AS DOUBLE) AS peerSetValue,
                cast(NULL AS DOUBLE) AS peerSetAbsoluteDiffValue,
                cast(NULL AS DOUBLE) AS peerSetChangeRaw,
                CASE comparisonType
                    WHEN 'priorWeek' THEN priorWeekChangeRaw
                    WHEN 'fourWeek' THEN fourWeekChangeRaw
                    WHEN 'lastYear' THEN lastYearChangeRaw
                END AS impactOnToplineRaw,
                changeUnit AS impactOnToplineUnit
            FROM toplineDeltas
        ),
        combined AS(
            SELECT * FROM sliceRows
            UNION ALL
            SELECT * FROM toplineRows
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
            FROM combined
        ),
        formatted AS(
            SELECT
                *,
                CASE
                    WHEN currentValue IS NULL THEN NULL
                    WHEN displayFormat='percent' THEN concat(format_number(100D*currentValue,1),'%')
                    WHEN abs(currentValue)>=1000000000D THEN concat(regexp_replace(format_number(currentValue/1000000000D,1),'\\\\.0$',''),'B')
                    WHEN abs(currentValue)>=1000000D THEN concat(regexp_replace(format_number(currentValue/1000000D,1),'\\\\.0$',''),'M')
                    WHEN abs(currentValue)>=1000D THEN concat(regexp_replace(format_number(currentValue/1000D,1),'\\\\.0$',''),'K')
                    ELSE format_number(currentValue,0)
                END AS currentValueDisplay,
                CASE
                    WHEN priorWeekAbsoluteDiffValue IS NULL THEN NULL
                    WHEN metricKind='ratio'
                        THEN concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(100D*priorWeekAbsoluteDiffValue,1),'pp')
                    WHEN abs(priorWeekAbsoluteDiffValue)>=1000000000D
                        THEN concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(priorWeekAbsoluteDiffValue/1000000000D,1),'\\\\.0$',''),'B')
                    WHEN abs(priorWeekAbsoluteDiffValue)>=1000000D
                        THEN concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(priorWeekAbsoluteDiffValue/1000000D,1),'\\\\.0$',''),'M')
                    WHEN abs(priorWeekAbsoluteDiffValue)>=1000D
                        THEN concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(priorWeekAbsoluteDiffValue/1000D,1),'\\\\.0$',''),'K')
                    ELSE concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(priorWeekAbsoluteDiffValue,0))
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
                        THEN concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(100D*fourWeekAbsoluteDiffValue,1),'pp')
                    WHEN abs(fourWeekAbsoluteDiffValue)>=1000000000D
                        THEN concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(fourWeekAbsoluteDiffValue/1000000000D,1),'\\\\.0$',''),'B')
                    WHEN abs(fourWeekAbsoluteDiffValue)>=1000000D
                        THEN concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(fourWeekAbsoluteDiffValue/1000000D,1),'\\\\.0$',''),'M')
                    WHEN abs(fourWeekAbsoluteDiffValue)>=1000D
                        THEN concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(fourWeekAbsoluteDiffValue/1000D,1),'\\\\.0$',''),'K')
                    ELSE concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(fourWeekAbsoluteDiffValue,0))
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
                        THEN concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(100D*lastYearAbsoluteDiffValue,1),'pp')
                    WHEN abs(lastYearAbsoluteDiffValue)>=1000000000D
                        THEN concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(lastYearAbsoluteDiffValue/1000000000D,1),'\\\\.0$',''),'B')
                    WHEN abs(lastYearAbsoluteDiffValue)>=1000000D
                        THEN concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(lastYearAbsoluteDiffValue/1000000D,1),'\\\\.0$',''),'M')
                    WHEN abs(lastYearAbsoluteDiffValue)>=1000D
                        THEN concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(lastYearAbsoluteDiffValue/1000D,1),'\\\\.0$',''),'K')
                    ELSE concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(lastYearAbsoluteDiffValue,0))
                END AS lastYearAbsoluteDiffDisplay,
                CASE
                    WHEN lastYearChangeValue IS NULL THEN NULL
                    WHEN changeUnit='pp'
                        THEN concat(CASE WHEN lastYearChangeValue>0D THEN '+' ELSE '' END,format_number(lastYearChangeValue,1),'pp')
                    ELSE concat(CASE WHEN lastYearChangeValue>0D THEN '+' ELSE '' END,format_number(lastYearChangeValue,1),'%')
                END AS lastYearChangeDisplay,
                CASE
                    WHEN peerSetValue IS NULL THEN NULL
                    WHEN displayFormat='percent' THEN concat(format_number(100D*peerSetValue,1),'%')
                    WHEN abs(peerSetValue)>=1000000000D THEN concat(regexp_replace(format_number(peerSetValue/1000000000D,1),'\\\\.0$',''),'B')
                    WHEN abs(peerSetValue)>=1000000D THEN concat(regexp_replace(format_number(peerSetValue/1000000D,1),'\\\\.0$',''),'M')
                    WHEN abs(peerSetValue)>=1000D THEN concat(regexp_replace(format_number(peerSetValue/1000D,1),'\\\\.0$',''),'K')
                    ELSE format_number(peerSetValue,0)
                END AS peerSetValueDisplay,
                CASE
                    WHEN peerSetAbsoluteDiffValue IS NULL THEN NULL
                    WHEN metricKind='ratio'
                        THEN concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(100D*peerSetAbsoluteDiffValue,1),'pp')
                    WHEN abs(peerSetAbsoluteDiffValue)>=1000000000D
                        THEN concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(peerSetAbsoluteDiffValue/1000000000D,1),'\\\\.0$',''),'B')
                    WHEN abs(peerSetAbsoluteDiffValue)>=1000000D
                        THEN concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(peerSetAbsoluteDiffValue/1000000D,1),'\\\\.0$',''),'M')
                    WHEN abs(peerSetAbsoluteDiffValue)>=1000D
                        THEN concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(peerSetAbsoluteDiffValue/1000D,1),'\\\\.0$',''),'K')
                    ELSE concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(peerSetAbsoluteDiffValue,0))
                END AS peerSetAbsoluteDiffDisplay,
                CASE
                    WHEN peerSetChangeValue IS NULL THEN NULL
                    -- The peer-set comparison is a gap between change rates.
                    ELSE concat(CASE WHEN peerSetChangeValue>0D THEN '+' ELSE '' END,format_number(peerSetChangeValue,1),'pp')
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
            targetWeekStartDate,targetWeekEndDate,fiscalYear,fiscalQuarterLabel,fiscalWeekCode,weekLabel,weekEndingLabel,
            filterLob,filterPlatform,
            metricName,metricLabel,metricDescription,metricKind,displayFormat,changeUnit,metricSortOrder,
            comparisonType,comparisonLabel,comparisonSortOrder,comparisonDataAvailable,comparisonWindowComplete,
            priorWeekDataAvailable,fourWeekDataAvailable,fourWeekWindowComplete,lastYearDataAvailable,peerSetDataAvailable,
            breakoutType,breakoutLabel,breakoutValue,breakoutSortOrder,
            rowType,isTopline,isOtherBucket,displayRankWithinBreakout,rowSortOrder,
            currentValue,currentValueDisplay,
            priorWeekAbsoluteDiffValue,priorWeekAbsoluteDiffDisplay,priorWeekChangeValue,priorWeekChangeDisplay,
            fourWeekAbsoluteDiffValue,fourWeekAbsoluteDiffDisplay,fourWeekChangeValue,fourWeekChangeDisplay,
            lastYearAbsoluteDiffValue,lastYearAbsoluteDiffDisplay,lastYearChangeValue,lastYearChangeDisplay,
            peerSetValue,peerSetValueDisplay,peerSetAbsoluteDiffValue,peerSetAbsoluteDiffDisplay,peerSetChangeValue,peerSetChangeDisplay,
            impactOnToplineValue,impactOnToplineDisplay,impactOnToplineUnit,
            v_processedAt AS appProcessedAt
        FROM formatted;
        SELECT
            'SUCCESS' AS status,
            v_weekFrom AS rebuiltWeekStartFrom,
            v_weekTo AS rebuiltWeekStartTo,
            v_weekEndTo AS latestWeekEndDate,
            CASE WHEN v_asOfDate<v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,
            'priorWeek | fourWeek | lastYear' AS supportedComparisons,
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsComparisonTable_long' AS targetObject,
            v_processedAt AS appProcessedAt;
    END IF;
END;
-- ============================================================================
-- DEVELOPMENT
-- ============================================================================
-- Preflight:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appBreakoutsComparisonTable_long(
--     p_asOfDate=>DATE '2026-09-28',
--     p_weeksToRebuild=>1,
--     p_validateOnly=>TRUE
-- );
-- Rebuild:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appBreakoutsComparisonTable_long(
--     p_asOfDate=>DATE '2026-09-28',
--     p_weeksToRebuild=>12,
--     p_validateOnly=>FALSE
-- );
-- ============================================================================
-- API: metric dropdown
-- Reads ONLY metrics that physically exist in Breakouts App Gold.
-- No section/business logic required in FastAPI.
-- ============================================================================
-- SELECT DISTINCT
--     metricName,
--     metricLabel,
--     metricDescription,
--     metricKind,
--     displayFormat,
--     changeUnit,
--     metricSortOrder
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsComparisonTable_long
-- WHERE NOT isTopline OR isTopline
-- ORDER BY metricSortOrder;
-- ============================================================================
-- API: breakout picker
-- Again, API does not determine eligibility.
-- ============================================================================
-- SELECT DISTINCT
--     breakoutType,
--     breakoutLabel,
--     breakoutSortOrder
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsComparisonTable_long
-- ORDER BY breakoutSortOrder;
-- ============================================================================
-- API: comparison table
-- ============================================================================
-- SELECT *
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsComparisonTable_long
-- WHERE fiscalYear=2026
--   AND fiscalQuarterLabel='Q3'
--   AND targetWeekStartDate=DATE '2026-08-09'
--   AND filterLob='All'
--   AND filterPlatform='All'
--   AND metricName='nbv'
--   AND breakoutType='channel'
--   AND comparisonType='fourWeek'
-- ORDER BY rowSortOrder;
-- Expected:
--   SMS
--   Paid Search
--   ...
--   (Other), if required
--   Topline
-- ============================================================================
-- APP GOLD ELIGIBILITY CHECK
-- Expected zero rows.
-- ============================================================================
-- SELECT DISTINCT metricName,metricLabel
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsComparisonTable_long a
-- WHERE NOT EXISTS(
--     SELECT 1
--     FROM prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
--     WHERE mc.metricName=a.metricName
--       AND mc.isActive
--       AND mc.showOnBreakouts
-- );
-- Expected zero rows.
-- SELECT DISTINCT breakoutType,breakoutLabel
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsComparisonTable_long a
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
--     breakoutValue,
--     rowType,
--     count(*) AS rowCount
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsComparisonTable_long
-- GROUP BY
--     targetWeekStartDate,
--     filterLob,
--     filterPlatform,
--     metricName,
--     breakoutType,
--     comparisonType,
--     breakoutValue,
--     rowType
-- HAVING count(*)>1;
-- ============================================================================
-- TOPLINE CHECK
-- Exactly one per contract grain.
-- ============================================================================
-- SELECT
--     targetWeekStartDate,
--     filterLob,
--     filterPlatform,
--     metricName,
--     breakoutType,
--     comparisonType,
--     count(*) AS toplineRows
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsComparisonTable_long
-- WHERE isTopline
-- GROUP BY
--     targetWeekStartDate,
--     filterLob,
--     filterPlatform,
--     metricName,
--     breakoutType,
--     comparisonType
-- HAVING count(*)<>1;

-- ============================================================================
-- PEER-SET VALIDATION
-- Expected:
--   1) isOtherBucket=TRUE -> peerSetDataAvailable=FALSE and peerSetValue IS NULL.
--   2) raw slice peerSetChangeValue is independent of comparisonType because
--      peer set always uses the four-week baseline.
-- ============================================================================
-- SELECT
--     targetWeekStartDate,metricName,breakoutType,breakoutValue,comparisonType,
--     isOtherBucket,peerSetDataAvailable,peerSetValue,peerSetChangeValue
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsComparisonTable_long
-- WHERE targetWeekStartDate=DATE '2026-08-09'
--   AND metricName='nbv'
-- ORDER BY breakoutType,breakoutValue,comparisonType;
-- ###########################################################################
-- END 05_sdi_sp_mip_gold_appBreakoutsComparisonTable_long.sql
-- ###########################################################################

-- ###########################################################################
-- BEGIN 09_sdi_sp_mip_gold_appCrosstabsRankedPairs_long.sql
-- ###########################################################################
-- ============================================================================
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
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsRankedPairs_long
        REPLACE WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        WITH base AS(
            SELECT
                g.targetWeekStartDate,g.targetWeekEndDate,c.fiscalYear,g.fiscalQuarterLabel,g.fiscalWeekCode,g.weekLabel,c.weekEndingLabel,
                g.filterLob,g.filterPlatform,
                g.pairKey,pc.pairLabel,pc.sortOrder AS pairSortOrder,pc.isPrebuiltPair,
                g.rowBreakoutType,rb.breakoutLabel AS rowBreakoutLabel,g.rowBreakoutValue,
                g.columnBreakoutType,cb.breakoutLabel AS columnBreakoutLabel,g.columnBreakoutValue,
                g.metricName,CASE WHEN g.metricName='nbv' THEN 'Total UPV' ELSE mc.metricLabel END AS metricLabel,
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
                CASE WHEN fourWeekWeekCount<=0 THEN NULL WHEN metricKind='ratio' THEN try_divide(fourWeekNumerator,fourWeekDenominator)
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
                CASE WHEN fourWeekTrendWeekCount<=0 THEN NULL WHEN metricKind='ratio' THEN try_divide(fourWeekTrendNumerator,fourWeekTrendDenominator)
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
                j.fourWeekDataAvailable AND j.toplineFourWeekWeekCount>0,
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
                    WHEN abs(currentValue)>=1000000000D THEN concat(regexp_replace(format_number(currentValue/1000000000D,1),'\\\\.0$',''),'B')
                    WHEN abs(currentValue)>=1000000D THEN concat(regexp_replace(format_number(currentValue/1000000D,1),'\\\\.0$',''),'M')
                    WHEN abs(currentValue)>=1000D THEN concat(regexp_replace(format_number(currentValue/1000D,1),'\\\\.0$',''),'K')
                    ELSE format_number(currentValue,0)
                END AS currentValueDisplay,
                CASE
                    WHEN priorWeekAbsoluteDiffValue IS NULL THEN NULL
                    WHEN metricKind='ratio' THEN concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(100D*priorWeekAbsoluteDiffValue,1),'pp')
                    WHEN abs(priorWeekAbsoluteDiffValue)>=1000000000D THEN concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(priorWeekAbsoluteDiffValue/1000000000D,1),'\\\\.0$',''),'B')
                    WHEN abs(priorWeekAbsoluteDiffValue)>=1000000D THEN concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(priorWeekAbsoluteDiffValue/1000000D,1),'\\\\.0$',''),'M')
                    WHEN abs(priorWeekAbsoluteDiffValue)>=1000D THEN concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(priorWeekAbsoluteDiffValue/1000D,1),'\\\\.0$',''),'K')
                    ELSE concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(priorWeekAbsoluteDiffValue,0))
                END AS priorWeekAbsoluteDiffDisplay,
                CASE WHEN priorWeekChangeValue IS NULL THEN NULL
                     WHEN changeUnit='pp' THEN concat(CASE WHEN priorWeekChangeValue>0D THEN '+' ELSE '' END,format_number(priorWeekChangeValue,1),'pp')
                     ELSE concat(CASE WHEN priorWeekChangeValue>0D THEN '+' ELSE '' END,format_number(priorWeekChangeValue,1),'%') END AS priorWeekChangeDisplay,
                CASE
                    WHEN fourWeekAbsoluteDiffValue IS NULL THEN NULL
                    WHEN metricKind='ratio' THEN concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(100D*fourWeekAbsoluteDiffValue,1),'pp')
                    WHEN abs(fourWeekAbsoluteDiffValue)>=1000000000D THEN concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(fourWeekAbsoluteDiffValue/1000000000D,1),'\\\\.0$',''),'B')
                    WHEN abs(fourWeekAbsoluteDiffValue)>=1000000D THEN concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(fourWeekAbsoluteDiffValue/1000000D,1),'\\\\.0$',''),'M')
                    WHEN abs(fourWeekAbsoluteDiffValue)>=1000D THEN concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(fourWeekAbsoluteDiffValue/1000D,1),'\\\\.0$',''),'K')
                    ELSE concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(fourWeekAbsoluteDiffValue,0))
                END AS fourWeekAbsoluteDiffDisplay,
                CASE WHEN fourWeekChangeValue IS NULL THEN NULL
                     WHEN changeUnit='pp' THEN concat(CASE WHEN fourWeekChangeValue>0D THEN '+' ELSE '' END,format_number(fourWeekChangeValue,1),'pp')
                     ELSE concat(CASE WHEN fourWeekChangeValue>0D THEN '+' ELSE '' END,format_number(fourWeekChangeValue,1),'%') END AS fourWeekChangeDisplay,
                CASE
                    WHEN lastYearAbsoluteDiffValue IS NULL THEN NULL
                    WHEN metricKind='ratio' THEN concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(100D*lastYearAbsoluteDiffValue,1),'pp')
                    WHEN abs(lastYearAbsoluteDiffValue)>=1000000000D THEN concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(lastYearAbsoluteDiffValue/1000000000D,1),'\\\\.0$',''),'B')
                    WHEN abs(lastYearAbsoluteDiffValue)>=1000000D THEN concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(lastYearAbsoluteDiffValue/1000000D,1),'\\\\.0$',''),'M')
                    WHEN abs(lastYearAbsoluteDiffValue)>=1000D THEN concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(lastYearAbsoluteDiffValue/1000D,1),'\\\\.0$',''),'K')
                    ELSE concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(lastYearAbsoluteDiffValue,0))
                END AS lastYearAbsoluteDiffDisplay,
                CASE WHEN lastYearChangeValue IS NULL THEN NULL
                     WHEN changeUnit='pp' THEN concat(CASE WHEN lastYearChangeValue>0D THEN '+' ELSE '' END,format_number(lastYearChangeValue,1),'pp')
                     ELSE concat(CASE WHEN lastYearChangeValue>0D THEN '+' ELSE '' END,format_number(lastYearChangeValue,1),'%') END AS lastYearChangeDisplay,
                CASE
                    WHEN peerSetValue IS NULL THEN NULL
                    WHEN displayFormat='percent' THEN concat(format_number(100D*peerSetValue,1),'%')
                    WHEN abs(peerSetValue)>=1000000000D THEN concat(regexp_replace(format_number(peerSetValue/1000000000D,1),'\\\\.0$',''),'B')
                    WHEN abs(peerSetValue)>=1000000D THEN concat(regexp_replace(format_number(peerSetValue/1000000D,1),'\\\\.0$',''),'M')
                    WHEN abs(peerSetValue)>=1000D THEN concat(regexp_replace(format_number(peerSetValue/1000D,1),'\\\\.0$',''),'K')
                    ELSE format_number(peerSetValue,0)
                END AS peerSetValueDisplay,
                CASE
                    WHEN peerSetAbsoluteDiffValue IS NULL THEN NULL
                    WHEN metricKind='ratio' THEN concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(100D*peerSetAbsoluteDiffValue,1),'pp')
                    WHEN abs(peerSetAbsoluteDiffValue)>=1000000000D THEN concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(peerSetAbsoluteDiffValue/1000000000D,1),'\\\\.0$',''),'B')
                    WHEN abs(peerSetAbsoluteDiffValue)>=1000000D THEN concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(peerSetAbsoluteDiffValue/1000000D,1),'\\\\.0$',''),'M')
                    WHEN abs(peerSetAbsoluteDiffValue)>=1000D THEN concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(peerSetAbsoluteDiffValue/1000D,1),'\\\\.0$',''),'K')
                    ELSE concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(peerSetAbsoluteDiffValue,0))
                END AS peerSetAbsoluteDiffDisplay,
                CASE WHEN peerSetChangeValue IS NULL THEN NULL
                     ELSE concat(CASE WHEN peerSetChangeValue>0D THEN '+' ELSE '' END,format_number(peerSetChangeValue,1),'pp') END AS peerSetChangeDisplay,
                CASE WHEN impactOnToplineValue IS NULL THEN NULL
                     WHEN impactOnToplineUnit='pp' THEN concat(CASE WHEN impactOnToplineValue>0D THEN '+' ELSE '' END,format_number(impactOnToplineValue,1),'pp')
                     ELSE concat(CASE WHEN impactOnToplineValue>0D THEN '+' ELSE '' END,format_number(impactOnToplineValue,1),'%') END AS impactOnToplineDisplay
            FROM rounded
        )
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
        FROM formatted;
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
-- ###########################################################################
-- END 09_sdi_sp_mip_gold_appCrosstabsRankedPairs_long.sql
-- ###########################################################################

-- ###########################################################################
-- BEGIN 11_sdi_sp_mip_gold_appExploreRankedPairs_long.sql
-- ###########################################################################
-- ============================================================================
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
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreRankedPairs_long
        REPLACE WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        WITH base AS(
            SELECT
                g.targetWeekStartDate,g.targetWeekEndDate,c.fiscalYear,g.fiscalQuarterLabel,g.fiscalWeekCode,g.weekLabel,c.weekEndingLabel,
                g.filterLob,g.filterPlatform,
                g.pairKey,pc.pairLabel,pc.sortOrder AS pairSortOrder,pc.isPrebuiltPair,
                g.rowBreakoutType,rb.breakoutLabel AS rowBreakoutLabel,coalesce(g.rowBreakoutValue,'(not set)') AS rowBreakoutValue,
                g.columnBreakoutType,cb.breakoutLabel AS columnBreakoutLabel,coalesce(g.columnBreakoutValue,'(not set)') AS columnBreakoutValue,
                g.metricName,CASE WHEN g.metricName='nbv' THEN 'Total UPV' ELSE mc.metricLabel END AS metricLabel,
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
                    WHEN abs(currentValue)>=1000000000D THEN concat(regexp_replace(format_number(currentValue/1000000000D,1),'\\\\.0$',''),'B')
                    WHEN abs(currentValue)>=1000000D THEN concat(regexp_replace(format_number(currentValue/1000000D,1),'\\\\.0$',''),'M')
                    WHEN abs(currentValue)>=1000D THEN concat(regexp_replace(format_number(currentValue/1000D,1),'\\\\.0$',''),'K')
                    ELSE format_number(currentValue,0)
                END AS currentValueDisplay,
                CASE WHEN priorWeekAbsoluteDiffValue IS NULL THEN NULL
                     WHEN metricKind='ratio' THEN concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(100D*priorWeekAbsoluteDiffValue,1),'pp')
                     WHEN abs(priorWeekAbsoluteDiffValue)>=1000000000D THEN concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(priorWeekAbsoluteDiffValue/1000000000D,1),'\\\\.0$',''),'B')
                     WHEN abs(priorWeekAbsoluteDiffValue)>=1000000D THEN concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(priorWeekAbsoluteDiffValue/1000000D,1),'\\\\.0$',''),'M')
                     WHEN abs(priorWeekAbsoluteDiffValue)>=1000D THEN concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(priorWeekAbsoluteDiffValue/1000D,1),'\\\\.0$',''),'K')
                     ELSE concat(CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(priorWeekAbsoluteDiffValue,0)) END AS priorWeekAbsoluteDiffDisplay,
                CASE WHEN priorWeekChangeValue IS NULL THEN NULL WHEN changeUnit='pp'
                     THEN concat(CASE WHEN priorWeekChangeValue>0D THEN '+' ELSE '' END,format_number(priorWeekChangeValue,1),'pp')
                     ELSE concat(CASE WHEN priorWeekChangeValue>0D THEN '+' ELSE '' END,format_number(priorWeekChangeValue,1),'%') END AS priorWeekChangeDisplay,
                CASE WHEN priorWeekImpactOnToplineValue IS NULL THEN NULL WHEN metricKind='ratio'
                     THEN concat(CASE WHEN priorWeekImpactOnToplineValue>0D THEN '+' ELSE '' END,format_number(priorWeekImpactOnToplineValue,1),'pp')
                     ELSE concat(CASE WHEN priorWeekImpactOnToplineValue>0D THEN '+' ELSE '' END,format_number(priorWeekImpactOnToplineValue,1),'%') END AS priorWeekImpactOnToplineDisplay,
                CASE WHEN fourWeekAbsoluteDiffValue IS NULL THEN NULL
                     WHEN metricKind='ratio' THEN concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(100D*fourWeekAbsoluteDiffValue,1),'pp')
                     WHEN abs(fourWeekAbsoluteDiffValue)>=1000000000D THEN concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(fourWeekAbsoluteDiffValue/1000000000D,1),'\\\\.0$',''),'B')
                     WHEN abs(fourWeekAbsoluteDiffValue)>=1000000D THEN concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(fourWeekAbsoluteDiffValue/1000000D,1),'\\\\.0$',''),'M')
                     WHEN abs(fourWeekAbsoluteDiffValue)>=1000D THEN concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(fourWeekAbsoluteDiffValue/1000D,1),'\\\\.0$',''),'K')
                     ELSE concat(CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(fourWeekAbsoluteDiffValue,0)) END AS fourWeekAbsoluteDiffDisplay,
                CASE WHEN fourWeekChangeValue IS NULL THEN NULL WHEN changeUnit='pp'
                     THEN concat(CASE WHEN fourWeekChangeValue>0D THEN '+' ELSE '' END,format_number(fourWeekChangeValue,1),'pp')
                     ELSE concat(CASE WHEN fourWeekChangeValue>0D THEN '+' ELSE '' END,format_number(fourWeekChangeValue,1),'%') END AS fourWeekChangeDisplay,
                CASE WHEN fourWeekImpactOnToplineValue IS NULL THEN NULL WHEN metricKind='ratio'
                     THEN concat(CASE WHEN fourWeekImpactOnToplineValue>0D THEN '+' ELSE '' END,format_number(fourWeekImpactOnToplineValue,1),'pp')
                     ELSE concat(CASE WHEN fourWeekImpactOnToplineValue>0D THEN '+' ELSE '' END,format_number(fourWeekImpactOnToplineValue,1),'%') END AS fourWeekImpactOnToplineDisplay,
                CASE WHEN lastYearAbsoluteDiffValue IS NULL THEN NULL
                     WHEN metricKind='ratio' THEN concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(100D*lastYearAbsoluteDiffValue,1),'pp')
                     WHEN abs(lastYearAbsoluteDiffValue)>=1000000000D THEN concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(lastYearAbsoluteDiffValue/1000000000D,1),'\\\\.0$',''),'B')
                     WHEN abs(lastYearAbsoluteDiffValue)>=1000000D THEN concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(lastYearAbsoluteDiffValue/1000000D,1),'\\\\.0$',''),'M')
                     WHEN abs(lastYearAbsoluteDiffValue)>=1000D THEN concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(lastYearAbsoluteDiffValue/1000D,1),'\\\\.0$',''),'K')
                     ELSE concat(CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(lastYearAbsoluteDiffValue,0)) END AS lastYearAbsoluteDiffDisplay,
                CASE WHEN lastYearChangeValue IS NULL THEN NULL WHEN changeUnit='pp'
                     THEN concat(CASE WHEN lastYearChangeValue>0D THEN '+' ELSE '' END,format_number(lastYearChangeValue,1),'pp')
                     ELSE concat(CASE WHEN lastYearChangeValue>0D THEN '+' ELSE '' END,format_number(lastYearChangeValue,1),'%') END AS lastYearChangeDisplay,
                CASE WHEN lastYearImpactOnToplineValue IS NULL THEN NULL WHEN metricKind='ratio'
                     THEN concat(CASE WHEN lastYearImpactOnToplineValue>0D THEN '+' ELSE '' END,format_number(lastYearImpactOnToplineValue,1),'pp')
                     ELSE concat(CASE WHEN lastYearImpactOnToplineValue>0D THEN '+' ELSE '' END,format_number(lastYearImpactOnToplineValue,1),'%') END AS lastYearImpactOnToplineDisplay,
                CASE WHEN peerSetValue IS NULL THEN NULL
                     WHEN displayFormat='percent' THEN concat(format_number(100D*peerSetValue,1),'%')
                     WHEN abs(peerSetValue)>=1000000000D THEN concat(regexp_replace(format_number(peerSetValue/1000000000D,1),'\\\\.0$',''),'B')
                     WHEN abs(peerSetValue)>=1000000D THEN concat(regexp_replace(format_number(peerSetValue/1000000D,1),'\\\\.0$',''),'M')
                     WHEN abs(peerSetValue)>=1000D THEN concat(regexp_replace(format_number(peerSetValue/1000D,1),'\\\\.0$',''),'K')
                     ELSE format_number(peerSetValue,0) END AS peerSetValueDisplay,
                CASE WHEN peerSetAbsoluteDiffValue IS NULL THEN NULL
                     WHEN metricKind='ratio' THEN concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(100D*peerSetAbsoluteDiffValue,1),'pp')
                     WHEN abs(peerSetAbsoluteDiffValue)>=1000000000D THEN concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(peerSetAbsoluteDiffValue/1000000000D,1),'\\\\.0$',''),'B')
                     WHEN abs(peerSetAbsoluteDiffValue)>=1000000D THEN concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(peerSetAbsoluteDiffValue/1000000D,1),'\\\\.0$',''),'M')
                     WHEN abs(peerSetAbsoluteDiffValue)>=1000D THEN concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,regexp_replace(format_number(peerSetAbsoluteDiffValue/1000D,1),'\\\\.0$',''),'K')
                     ELSE concat(CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,format_number(peerSetAbsoluteDiffValue,0)) END AS peerSetAbsoluteDiffDisplay,
                CASE WHEN peerSetChangeValue IS NULL THEN NULL
                     ELSE concat(CASE WHEN peerSetChangeValue>0D THEN '+' ELSE '' END,format_number(peerSetChangeValue,1),'pp') END AS peerSetChangeDisplay
            FROM rounded
        )
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
        FROM formatted;
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
-- ###########################################################################
-- END 11_sdi_sp_mip_gold_appExploreRankedPairs_long.sql
-- ###########################################################################
