-- ============================================================================
-- FILE  : 05_sdi_sp_mip_gold_appBreakoutsComparisonTable_long.sql
-- LAYER : GOLD / APP
-- TAB   : Breakouts
-- SECTION: Comparison table
--
-- PURPOSE:
--   Application-ready Breakouts comparison table.
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
-- CONTRACT:
--   One row per:
--     week x filter context x metric x breakout type x displayed slice
--     x selected comparison type
--
--   Every slice row contains simultaneously:
--     This week
--     Vs prior week
--     Vs 4-wk trend
--     Vs same wk LY
--     Vs peer set
--     Impact on topline
--
--   comparisonType controls:
--     Impact on topline
--     ranking/order
--     Top100 + (Other) bucketing
--
--   A final Topline row is appended for every selected breakout.
--
--   Numerators/denominators are used internally only and are NOT persisted
--   in the Gold App table.
--
--   Breakout types are NOT hardcoded. They come from:
--     sdi_vw_mip_control_breakoutCatalog_static
--   where isActive = TRUE and isPrebuiltBreakout = TRUE.
-- ============================================================================

-- ONE-TIME MIGRATION ONLY:
-- DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsComparisonTable_long;

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_gold_appBreakoutsComparisonTable_long(
    IN p_asOfDate DATE DEFAULT NULL,
    IN p_weeksToRebuild INT DEFAULT 1,
    IN p_validateOnly BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'MIP Gold App: Breakouts comparison table. Wide visible comparisons with comparator-aware impact/ranking and final Topline row.'
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
        GROUP BY targetWeekStartDate,filterLob,filterPlatform,metricName,breakoutType,breakoutValue
        HAVING count(*)>1
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='Duplicate Breakout Gold analytical keys detected.';
    END IF;

    IF EXISTS(
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
        WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        GROUP BY targetWeekStartDate,filterLob,filterPlatform,metricName
        HAVING count(*)>1
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='Duplicate Overview Gold analytical keys detected.';
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
            SET MESSAGE_TEXT='Metric Catalog contains unsupported metric metadata.';
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
            'priorWeek | fourWeek | lastYear' AS supportedComparisons,
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsComparisonTable_long' AS targetObject,
            'Validation passed. No Gold App table was created or modified.' AS message;
    ELSE

        -- =====================================================================
        -- 4. Gold App table
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
        COMMENT 'MIP Gold App: Breakouts comparison table. All visible comparisons are persisted per slice; comparisonType controls impact/ranking; Topline row included.';

        -- =====================================================================
        -- 5. Rebuild target weeks
        -- =====================================================================
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsComparisonTable_long
        REPLACE WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo

        WITH scopeBreakouts AS(
            SELECT
                targetWeekStartDate,targetWeekEndDate,fiscalQuarterLabel,fiscalWeekCode,weekLabel,
                filterLob,filterPlatform,
                breakoutType,breakoutValue,
                metricName,
                thisWeekNumerator,thisWeekDenominator,
                priorWeekNumerator,priorWeekDenominator,
                fourWeekTrendNumerator,fourWeekTrendDenominator,
                sameWeekLyNumerator,sameWeekLyDenominator,
                peerSetNumerator,peerSetDenominator,
                thisWeekDataAvailable,priorWeekDataAvailable,fourWeekTrendWeekCount,sameWeekLyDataAvailable
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
            WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        ),

        scopeOverview AS(
            SELECT
                targetWeekStartDate,targetWeekEndDate,fiscalQuarterLabel,fiscalWeekCode,weekLabel,
                filterLob,filterPlatform,
                metricName,metricKind,
                thisWeekNumerator,thisWeekDenominator,
                priorWeekNumerator,priorWeekDenominator,
                fourWeekTrendNumerator,fourWeekTrendDenominator,
                sameWeekLyNumerator,sameWeekLyDenominator,
                thisWeekDataAvailable,priorWeekDataAvailable,fourWeekTrendWeekCount,sameWeekLyDataAvailable
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
            WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        ),

        base AS(
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

        toplineValues AS(
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

                g.thisWeekNumerator AS currentNumerator,
                g.thisWeekDenominator AS currentDenominator,

                g.priorWeekNumerator,
                g.priorWeekDenominator,

                g.fourWeekTrendNumerator,
                g.fourWeekTrendDenominator,
                g.fourWeekTrendWeekCount,

                g.sameWeekLyNumerator,
                g.sameWeekLyDenominator,

                g.thisWeekDataAvailable,
                g.priorWeekDataAvailable,
                g.fourWeekTrendWeekCount>0 AS fourWeekDataAvailable,
                g.fourWeekTrendWeekCount=4 AS fourWeekWindowComplete,
                g.sameWeekLyDataAvailable AS lastYearDataAvailable,

                CASE
                    WHEN NOT g.thisWeekDataAvailable THEN NULL
                    WHEN mc.metricKind='ratio'
                        THEN try_divide(g.thisWeekNumerator,g.thisWeekDenominator)
                    ELSE g.thisWeekNumerator
                END AS currentValue,

                CASE
                    WHEN NOT g.priorWeekDataAvailable THEN NULL
                    WHEN mc.metricKind='ratio'
                        THEN try_divide(g.priorWeekNumerator,g.priorWeekDenominator)
                    ELSE g.priorWeekNumerator
                END AS priorWeekValue,

                CASE
                    WHEN g.fourWeekTrendWeekCount<=0 THEN NULL
                    WHEN mc.metricKind='ratio'
                        THEN try_divide(g.fourWeekTrendNumerator,g.fourWeekTrendDenominator)
                    ELSE try_divide(g.fourWeekTrendNumerator,cast(g.fourWeekTrendWeekCount AS DOUBLE))
                END AS fourWeekValue,

                CASE
                    WHEN NOT g.sameWeekLyDataAvailable THEN NULL
                    WHEN mc.metricKind='ratio'
                        THEN try_divide(g.sameWeekLyNumerator,g.sameWeekLyDenominator)
                    ELSE g.sameWeekLyNumerator
                END AS lastYearValue
            FROM scopeOverview g
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
              ON mc.metricName=g.metricName
             AND mc.isActive
            LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c
              ON c.weekStartDate=g.targetWeekStartDate
        ),

        rawValues AS(
            SELECT
                b.*,

                CASE
                    WHEN NOT thisWeekDataAvailable THEN NULL
                    WHEN metricKind='ratio'
                        THEN try_divide(thisWeekNumerator,thisWeekDenominator)
                    ELSE thisWeekNumerator
                END AS currentValue,

                CASE
                    WHEN NOT priorWeekDataAvailable THEN NULL
                    WHEN metricKind='ratio'
                        THEN try_divide(priorWeekNumerator,priorWeekDenominator)
                    ELSE priorWeekNumerator
                END AS priorWeekValue,

                CASE
                    WHEN fourWeekTrendWeekCount<=0 THEN NULL
                    WHEN metricKind='ratio'
                        THEN try_divide(fourWeekTrendNumerator,fourWeekTrendDenominator)
                    ELSE try_divide(fourWeekTrendNumerator,cast(fourWeekTrendWeekCount AS DOUBLE))
                END AS fourWeekValue,

                CASE
                    WHEN NOT sameWeekLyDataAvailable THEN NULL
                    WHEN metricKind='ratio'
                        THEN try_divide(sameWeekLyNumerator,sameWeekLyDenominator)
                    ELSE sameWeekLyNumerator
                END AS lastYearValue,

                CASE
                    WHEN metricKind='ratio'
                        THEN try_divide(peerSetNumerator,peerSetDenominator)
                    ELSE peerSetNumerator
                END AS peerSetValue
            FROM base b
        ),

        rawSelected AS(
            SELECT
                r.*,
                'priorWeek' AS comparisonType,
                'Prior week' AS comparisonLabel,
                10 AS comparisonSortOrder,
                priorWeekDataAvailable AS comparisonDataAvailable,
                priorWeekDataAvailable AS comparisonWindowComplete,
                priorWeekValue AS selectedComparisonValue,
                priorWeekNumerator AS selectedComparisonNumerator,
                priorWeekDenominator AS selectedComparisonDenominator
            FROM rawValues r

            UNION ALL

            SELECT
                r.*,
                'fourWeek' AS comparisonType,
                '4-wk trend' AS comparisonLabel,
                20 AS comparisonSortOrder,
                fourWeekTrendWeekCount>0 AS comparisonDataAvailable,
                fourWeekTrendWeekCount=4 AS comparisonWindowComplete,
                fourWeekValue AS selectedComparisonValue,
                CASE
                    WHEN metricKind='count' AND fourWeekTrendWeekCount>0
                        THEN try_divide(fourWeekTrendNumerator,cast(fourWeekTrendWeekCount AS DOUBLE))
                    ELSE fourWeekTrendNumerator
                END AS selectedComparisonNumerator,
                CASE
                    WHEN metricKind='count' THEN NULL
                    ELSE fourWeekTrendDenominator
                END AS selectedComparisonDenominator
            FROM rawValues r

            UNION ALL

            SELECT
                r.*,
                'lastYear' AS comparisonType,
                'Same wk LY' AS comparisonLabel,
                30 AS comparisonSortOrder,
                sameWeekLyDataAvailable AS comparisonDataAvailable,
                sameWeekLyDataAvailable AS comparisonWindowComplete,
                lastYearValue AS selectedComparisonValue,
                sameWeekLyNumerator AS selectedComparisonNumerator,
                sameWeekLyDenominator AS selectedComparisonDenominator
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
                    WHEN 'fourWeek' THEN
                        CASE
                            WHEN r.metricKind='count' THEN NULL
                            ELSE t.fourWeekTrendDenominator
                        END
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
                    WHEN NOT comparisonDataAvailable
                      OR currentValue IS NULL
                      OR selectedComparisonValue IS NULL THEN NULL

                    WHEN metricKind='count' THEN
                        100D*try_divide(
                            currentValue-selectedComparisonValue,
                            selectedToplineComparisonValue
                        )

                    WHEN metricKind='ratio' THEN
                        100D*(
                            try_divide(thisWeekNumerator,toplineCurrentDenominator)
                            -try_divide(
                                selectedComparisonNumerator,
                                selectedToplineComparisonDenominator
                            )
                        )
                END AS impactOnToplineRaw
            FROM rawWithTopline
        ),

        rankedRaw AS(
            SELECT
                *,
                CASE
                    WHEN comparisonDataAvailable
                     AND impactOnToplineRaw IS NOT NULL
                    THEN row_number() OVER(
                        PARTITION BY
                            targetWeekStartDate,
                            filterLob,
                            filterPlatform,
                            metricName,
                            breakoutType,
                            comparisonType
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

                CASE
                    WHEN rawImpactRankWithinBreakout>100 THEN TRUE
                    ELSE FALSE
                END AS isSyntheticOtherMember
            FROM rankedRaw
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

                comparisonType,
                comparisonLabel,
                comparisonSortOrder,

                min(CASE WHEN comparisonDataAvailable THEN 1 ELSE 0 END)=1
                    AS comparisonDataAvailable,

                min(CASE WHEN comparisonWindowComplete THEN 1 ELSE 0 END)=1
                    AS comparisonWindowComplete,

                min(CASE WHEN priorWeekDataAvailable THEN 1 ELSE 0 END)=1
                    AS priorWeekDataAvailable,

                max(fourWeekTrendWeekCount)>0
                    AS fourWeekDataAvailable,

                max(fourWeekTrendWeekCount)=4
                    AS fourWeekWindowComplete,

                min(CASE WHEN sameWeekLyDataAvailable THEN 1 ELSE 0 END)=1
                    AS lastYearDataAvailable,

                breakoutType,
                breakoutLabel,
                displayBreakoutValue AS breakoutValue,
                breakoutSortOrder,

                max(CASE WHEN isSyntheticOtherMember THEN 1 ELSE 0 END)=1
                    AS isOtherBucket,

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
                sum(peerSetDenominator) AS peerSetDenominator
            FROM bucketMembers
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
                    WHEN metricKind='ratio'
                        THEN try_divide(currentNumerator,currentDenominator)
                    ELSE currentNumerator
                END AS currentValue,

                CASE
                    WHEN metricKind='ratio'
                        THEN try_divide(priorWeekNumerator,priorWeekDenominator)
                    ELSE priorWeekNumerator
                END AS priorWeekValue,

                CASE
                    WHEN metricKind='ratio'
                        THEN try_divide(fourWeekTrendNumerator,fourWeekTrendDenominator)
                    WHEN fourWeekTrendWeekCount>0
                        THEN try_divide(
                            fourWeekTrendNumerator,
                            cast(fourWeekTrendWeekCount AS DOUBLE)
                        )
                    ELSE NULL
                END AS fourWeekValue,

                CASE
                    WHEN metricKind='ratio'
                        THEN try_divide(sameWeekLyNumerator,sameWeekLyDenominator)
                    ELSE sameWeekLyNumerator
                END AS lastYearValue,

                CASE
                    WHEN metricKind='ratio'
                        THEN try_divide(peerSetNumerator,peerSetDenominator)
                    ELSE peerSetNumerator
                END AS peerSetValue,

                CASE
                    WHEN metricKind='ratio'
                        THEN peerSetNumerator IS NOT NULL
                         AND nullif(peerSetDenominator,0D) IS NOT NULL
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
                                THEN try_divide(
                                    fourWeekTrendNumerator,
                                    cast(fourWeekTrendWeekCount AS DOUBLE)
                                )
                            ELSE fourWeekTrendNumerator
                        END

                    WHEN 'lastYear' THEN sameWeekLyNumerator
                END AS selectedComparisonNumerator,

                CASE comparisonType
                    WHEN 'priorWeek' THEN priorWeekDenominator
                    WHEN 'fourWeek' THEN
                        CASE
                            WHEN metricKind='count' THEN NULL
                            ELSE fourWeekTrendDenominator
                        END
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
                    WHEN 'fourWeek' THEN
                        CASE
                            WHEN b.metricKind='count' THEN NULL
                            ELSE t.fourWeekTrendDenominator
                        END
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

                currentValue-priorWeekValue
                    AS priorWeekAbsoluteDiffValue,

                CASE
                    WHEN currentValue IS NULL OR priorWeekValue IS NULL THEN NULL
                    WHEN changeUnit='pp'
                        THEN 100D*(currentValue-priorWeekValue)
                    WHEN changeUnit='pct'
                        THEN 100D*(try_divide(currentValue,priorWeekValue)-1D)
                END AS priorWeekChangeRaw,

                currentValue-fourWeekValue
                    AS fourWeekAbsoluteDiffValue,

                CASE
                    WHEN currentValue IS NULL OR fourWeekValue IS NULL THEN NULL
                    WHEN changeUnit='pp'
                        THEN 100D*(currentValue-fourWeekValue)
                    WHEN changeUnit='pct'
                        THEN 100D*(try_divide(currentValue,fourWeekValue)-1D)
                END AS fourWeekChangeRaw,

                currentValue-lastYearValue
                    AS lastYearAbsoluteDiffValue,

                CASE
                    WHEN currentValue IS NULL OR lastYearValue IS NULL THEN NULL
                    WHEN changeUnit='pp'
                        THEN 100D*(currentValue-lastYearValue)
                    WHEN changeUnit='pct'
                        THEN 100D*(try_divide(currentValue,lastYearValue)-1D)
                END AS lastYearChangeRaw,

                currentValue-peerSetValue
                    AS peerSetAbsoluteDiffValue,

                CASE
                    WHEN currentValue IS NULL OR peerSetValue IS NULL THEN NULL
                    WHEN changeUnit='pp'
                        THEN 100D*(currentValue-peerSetValue)
                    WHEN changeUnit='pct'
                        THEN 100D*(try_divide(currentValue,peerSetValue)-1D)
                END AS peerSetChangeRaw,

                currentValue-selectedComparisonValue
                    AS selectedAbsoluteDiffValue,

                CASE
                    WHEN NOT comparisonDataAvailable
                      OR currentValue IS NULL
                      OR selectedComparisonValue IS NULL THEN NULL

                    WHEN metricKind='count' THEN
                        100D*try_divide(
                            currentValue-selectedComparisonValue,
                            selectedToplineComparisonValue
                        )

                    WHEN metricKind='ratio' THEN
                        100D*(
                            try_divide(currentNumerator,toplineCurrentDenominator)
                            -try_divide(
                                selectedComparisonNumerator,
                                selectedToplineComparisonDenominator
                            )
                        )
                END AS impactOnToplineRaw
            FROM bucketWithTopline
        ),

        sliceRanked AS(
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
                        abs(selectedAbsoluteDiffValue) DESC NULLS LAST,
                        isOtherBucket,
                        breakoutValue
                ) AS displayRankWithinBreakout
            FROM bucketCalculated
        ),

        sliceRows AS(
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

                comparisonType,
                comparisonLabel,
                comparisonSortOrder,
                comparisonDataAvailable,
                comparisonWindowComplete,

                priorWeekDataAvailable,
                fourWeekDataAvailable,
                fourWeekWindowComplete,
                lastYearDataAvailable,
                peerSetDataAvailable,

                breakoutType,
                breakoutLabel,
                breakoutValue,
                breakoutSortOrder,

                'slice' AS rowType,
                FALSE AS isTopline,
                isOtherBucket,
                displayRankWithinBreakout,
                displayRankWithinBreakout AS rowSortOrder,

                currentValue,

                priorWeekAbsoluteDiffValue,
                priorWeekChangeRaw,

                fourWeekAbsoluteDiffValue,
                fourWeekChangeRaw,

                lastYearAbsoluteDiffValue,
                lastYearChangeRaw,

                peerSetValue,
                peerSetAbsoluteDiffValue,
                peerSetChangeRaw,

                impactOnToplineRaw,
                changeUnit AS impactOnToplineUnit
            FROM sliceRanked
        ),

        availableBreakouts AS(
            SELECT DISTINCT
                targetWeekStartDate,
                filterLob,
                filterPlatform,
                metricName,
                breakoutType,
                breakoutLabel,
                breakoutSortOrder
            FROM base
        ),

        toplineExpanded AS(
            SELECT
                t.*,
                a.breakoutType,
                a.breakoutLabel,
                a.breakoutSortOrder,

                'priorWeek' AS comparisonType,
                'Prior week' AS comparisonLabel,
                10 AS comparisonSortOrder,
                t.priorWeekDataAvailable AS comparisonDataAvailable,
                t.priorWeekDataAvailable AS comparisonWindowComplete,
                t.priorWeekValue AS selectedComparisonValue
            FROM toplineValues t
            JOIN availableBreakouts a
              ON a.targetWeekStartDate=t.targetWeekStartDate
             AND a.filterLob=t.filterLob
             AND a.filterPlatform=t.filterPlatform
             AND a.metricName=t.metricName

            UNION ALL

            SELECT
                t.*,
                a.breakoutType,
                a.breakoutLabel,
                a.breakoutSortOrder,

                'fourWeek' AS comparisonType,
                '4-wk trend' AS comparisonLabel,
                20 AS comparisonSortOrder,
                t.fourWeekDataAvailable AS comparisonDataAvailable,
                t.fourWeekWindowComplete AS comparisonWindowComplete,
                t.fourWeekValue AS selectedComparisonValue
            FROM toplineValues t
            JOIN availableBreakouts a
              ON a.targetWeekStartDate=t.targetWeekStartDate
             AND a.filterLob=t.filterLob
             AND a.filterPlatform=t.filterPlatform
             AND a.metricName=t.metricName

            UNION ALL

            SELECT
                t.*,
                a.breakoutType,
                a.breakoutLabel,
                a.breakoutSortOrder,

                'lastYear' AS comparisonType,
                'Same wk LY' AS comparisonLabel,
                30 AS comparisonSortOrder,
                t.lastYearDataAvailable AS comparisonDataAvailable,
                t.lastYearDataAvailable AS comparisonWindowComplete,
                t.lastYearValue AS selectedComparisonValue
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

                currentValue-priorWeekValue
                    AS priorWeekAbsoluteDiffValue,

                CASE
                    WHEN currentValue IS NULL OR priorWeekValue IS NULL THEN NULL
                    WHEN changeUnit='pp'
                        THEN 100D*(currentValue-priorWeekValue)
                    WHEN changeUnit='pct'
                        THEN 100D*(try_divide(currentValue,priorWeekValue)-1D)
                END AS priorWeekChangeRaw,

                currentValue-fourWeekValue
                    AS fourWeekAbsoluteDiffValue,

                CASE
                    WHEN currentValue IS NULL OR fourWeekValue IS NULL THEN NULL
                    WHEN changeUnit='pp'
                        THEN 100D*(currentValue-fourWeekValue)
                    WHEN changeUnit='pct'
                        THEN 100D*(try_divide(currentValue,fourWeekValue)-1D)
                END AS fourWeekChangeRaw,

                currentValue-lastYearValue
                    AS lastYearAbsoluteDiffValue,

                CASE
                    WHEN currentValue IS NULL OR lastYearValue IS NULL THEN NULL
                    WHEN changeUnit='pp'
                        THEN 100D*(currentValue-lastYearValue)
                    WHEN changeUnit='pct'
                        THEN 100D*(try_divide(currentValue,lastYearValue)-1D)
                END AS lastYearChangeRaw
            FROM toplineExpanded
        ),

        toplineRows AS(
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

                comparisonType,
                comparisonLabel,
                comparisonSortOrder,
                comparisonDataAvailable,
                comparisonWindowComplete,

                priorWeekDataAvailable,
                fourWeekDataAvailable,
                fourWeekWindowComplete,
                lastYearDataAvailable,
                FALSE AS peerSetDataAvailable,

                breakoutType,
                breakoutLabel,
                'Topline' AS breakoutValue,
                breakoutSortOrder,

                'topline' AS rowType,
                TRUE AS isTopline,
                FALSE AS isOtherBucket,
                cast(NULL AS BIGINT) AS displayRankWithinBreakout,
                cast(999999 AS BIGINT) AS rowSortOrder,

                currentValue,

                priorWeekAbsoluteDiffValue,
                priorWeekChangeRaw,

                fourWeekAbsoluteDiffValue,
                fourWeekChangeRaw,

                lastYearAbsoluteDiffValue,
                lastYearChangeRaw,

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
                        THEN concat(
                            CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            format_number(100D*priorWeekAbsoluteDiffValue,1),
                            'pp'
                        )
                    WHEN abs(priorWeekAbsoluteDiffValue)>=1000000000D
                        THEN concat(
                            CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(priorWeekAbsoluteDiffValue/1000000000D,1),'\\.0$',''),
                            'B'
                        )
                    WHEN abs(priorWeekAbsoluteDiffValue)>=1000000D
                        THEN concat(
                            CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(priorWeekAbsoluteDiffValue/1000000D,1),'\\.0$',''),
                            'M'
                        )
                    WHEN abs(priorWeekAbsoluteDiffValue)>=1000D
                        THEN concat(
                            CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(priorWeekAbsoluteDiffValue/1000D,1),'\\.0$',''),
                            'K'
                        )
                    ELSE concat(
                        CASE WHEN priorWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                        format_number(priorWeekAbsoluteDiffValue,0)
                    )
                END AS priorWeekAbsoluteDiffDisplay,

                CASE
                    WHEN priorWeekChangeValue IS NULL THEN NULL
                    WHEN changeUnit='pp'
                        THEN concat(
                            CASE WHEN priorWeekChangeValue>0D THEN '+' ELSE '' END,
                            format_number(priorWeekChangeValue,1),
                            'pp'
                        )
                    ELSE concat(
                        CASE WHEN priorWeekChangeValue>0D THEN '+' ELSE '' END,
                        format_number(priorWeekChangeValue,1),
                        '%'
                    )
                END AS priorWeekChangeDisplay,

                CASE
                    WHEN fourWeekAbsoluteDiffValue IS NULL THEN NULL
                    WHEN metricKind='ratio'
                        THEN concat(
                            CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            format_number(100D*fourWeekAbsoluteDiffValue,1),
                            'pp'
                        )
                    WHEN abs(fourWeekAbsoluteDiffValue)>=1000000000D
                        THEN concat(
                            CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(fourWeekAbsoluteDiffValue/1000000000D,1),'\\.0$',''),
                            'B'
                        )
                    WHEN abs(fourWeekAbsoluteDiffValue)>=1000000D
                        THEN concat(
                            CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(fourWeekAbsoluteDiffValue/1000000D,1),'\\.0$',''),
                            'M'
                        )
                    WHEN abs(fourWeekAbsoluteDiffValue)>=1000D
                        THEN concat(
                            CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(fourWeekAbsoluteDiffValue/1000D,1),'\\.0$',''),
                            'K'
                        )
                    ELSE concat(
                        CASE WHEN fourWeekAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                        format_number(fourWeekAbsoluteDiffValue,0)
                    )
                END AS fourWeekAbsoluteDiffDisplay,

                CASE
                    WHEN fourWeekChangeValue IS NULL THEN NULL
                    WHEN changeUnit='pp'
                        THEN concat(
                            CASE WHEN fourWeekChangeValue>0D THEN '+' ELSE '' END,
                            format_number(fourWeekChangeValue,1),
                            'pp'
                        )
                    ELSE concat(
                        CASE WHEN fourWeekChangeValue>0D THEN '+' ELSE '' END,
                        format_number(fourWeekChangeValue,1),
                        '%'
                    )
                END AS fourWeekChangeDisplay,

                CASE
                    WHEN lastYearAbsoluteDiffValue IS NULL THEN NULL
                    WHEN metricKind='ratio'
                        THEN concat(
                            CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            format_number(100D*lastYearAbsoluteDiffValue,1),
                            'pp'
                        )
                    WHEN abs(lastYearAbsoluteDiffValue)>=1000000000D
                        THEN concat(
                            CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(lastYearAbsoluteDiffValue/1000000000D,1),'\\.0$',''),
                            'B'
                        )
                    WHEN abs(lastYearAbsoluteDiffValue)>=1000000D
                        THEN concat(
                            CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(lastYearAbsoluteDiffValue/1000000D,1),'\\.0$',''),
                            'M'
                        )
                    WHEN abs(lastYearAbsoluteDiffValue)>=1000D
                        THEN concat(
                            CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(lastYearAbsoluteDiffValue/1000D,1),'\\.0$',''),
                            'K'
                        )
                    ELSE concat(
                        CASE WHEN lastYearAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                        format_number(lastYearAbsoluteDiffValue,0)
                    )
                END AS lastYearAbsoluteDiffDisplay,

                CASE
                    WHEN lastYearChangeValue IS NULL THEN NULL
                    WHEN changeUnit='pp'
                        THEN concat(
                            CASE WHEN lastYearChangeValue>0D THEN '+' ELSE '' END,
                            format_number(lastYearChangeValue,1),
                            'pp'
                        )
                    ELSE concat(
                        CASE WHEN lastYearChangeValue>0D THEN '+' ELSE '' END,
                        format_number(lastYearChangeValue,1),
                        '%'
                    )
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
                        THEN concat(
                            CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            format_number(100D*peerSetAbsoluteDiffValue,1),
                            'pp'
                        )
                    WHEN abs(peerSetAbsoluteDiffValue)>=1000000000D
                        THEN concat(
                            CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(peerSetAbsoluteDiffValue/1000000000D,1),'\\.0$',''),
                            'B'
                        )
                    WHEN abs(peerSetAbsoluteDiffValue)>=1000000D
                        THEN concat(
                            CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(peerSetAbsoluteDiffValue/1000000D,1),'\\.0$',''),
                            'M'
                        )
                    WHEN abs(peerSetAbsoluteDiffValue)>=1000D
                        THEN concat(
                            CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                            regexp_replace(format_number(peerSetAbsoluteDiffValue/1000D,1),'\\.0$',''),
                            'K'
                        )
                    ELSE concat(
                        CASE WHEN peerSetAbsoluteDiffValue>0D THEN '+' ELSE '' END,
                        format_number(peerSetAbsoluteDiffValue,0)
                    )
                END AS peerSetAbsoluteDiffDisplay,

                CASE
                    WHEN peerSetChangeValue IS NULL THEN NULL
                    WHEN changeUnit='pp'
                        THEN concat(
                            CASE WHEN peerSetChangeValue>0D THEN '+' ELSE '' END,
                            format_number(peerSetChangeValue,1),
                            'pp'
                        )
                    ELSE concat(
                        CASE WHEN peerSetChangeValue>0D THEN '+' ELSE '' END,
                        format_number(peerSetChangeValue,1),
                        '%'
                    )
                END AS peerSetChangeDisplay,

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
                END AS impactOnToplineDisplay
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

            comparisonType,
            comparisonLabel,
            comparisonSortOrder,
            comparisonDataAvailable,
            comparisonWindowComplete,

            priorWeekDataAvailable,
            fourWeekDataAvailable,
            fourWeekWindowComplete,
            lastYearDataAvailable,
            peerSetDataAvailable,

            breakoutType,
            breakoutLabel,
            breakoutValue,
            breakoutSortOrder,

            rowType,
            isTopline,
            isOtherBucket,
            displayRankWithinBreakout,
            rowSortOrder,

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
            'priorWeek | fourWeek | lastYear' AS supportedComparisons,
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsComparisonTable_long' AS targetObject,
            v_processedAt AS appProcessedAt;
    END IF;
END;

-- ============================================================================
-- DEVELOPMENT
-- ============================================================================

-- ONE TIME before first execution of the redesigned schema:
-- DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsComparisonTable_long;

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
-- API METADATA: BREAKOUT PICKER
--
-- Do not hardcode Channel / Region / Entry page etc. in React.
-- This query supplies the picker.
-- ============================================================================

-- SELECT
--     breakoutType,
--     breakoutLabel,
--     sortOrder
-- FROM prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static
-- WHERE isActive
--   AND isPrebuiltBreakout
-- ORDER BY sortOrder;

-- ============================================================================
-- API EXAMPLE
--
-- Q3 2026
-- W7 / 9-15 Aug
-- Total UPV
-- Channel
-- selected impact comparator = 4-wk
-- ============================================================================

-- SELECT
--     *
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

-- This returns:
--
-- SMS
-- Paid Search
-- Direct
-- Organic Search
-- Email
-- ...
-- (Other), if required
-- Topline
--
-- Every returned row already contains:
--
-- currentValueDisplay
--
-- priorWeekAbsoluteDiffDisplay
-- priorWeekChangeDisplay
--
-- fourWeekAbsoluteDiffDisplay
-- fourWeekChangeDisplay
--
-- lastYearAbsoluteDiffDisplay
-- lastYearChangeDisplay
--
-- peerSetChangeDisplay
-- peerSetAbsoluteDiffDisplay
--
-- impactOnToplineDisplay
--
-- So the backend does not recalculate any business metrics.

-- ============================================================================
-- DUPLICATE CHECK
-- Expected zero rows
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
-- Exactly one Topline row per selected contract grain
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