-- ============================================================================
-- FILE  : 02_sdi_sp_mip_gold_appOverviewTrend_long.sql
-- LAYER : GOLD / APP
-- TAB   : Overview
-- SECTION: Overview Trend
-- PURPOSE:
--   Application-ready Overview trend.
--   One row per reporting week x filter context x metric x comparison type.
--   comparisonType supports: priorWeek | fourWeek | lastYear.
--   Analytical numerator/denominator ingredients remain upstream in Gold.
-- ============================================================================

-- ONE-TIME MIGRATION ONLY:
-- DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long;

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewTrend_long(
    IN p_asOfDate DATE DEFAULT NULL,
    IN p_weeksToRebuild INT DEFAULT 1,
    IN p_validateOnly BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'MIP Gold App: application-ready comparator-aware Overview Trend.'
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
    -- 2. Source validation
    -- =========================================================================
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
        WHERE isActive AND showOnOverview
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='Metric Catalog has no active Overview metrics.';
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
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
        WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        GROUP BY targetWeekStartDate,filterLob,filterPlatform,metricName
        HAVING count(*)>1
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT='Duplicate Overview Gold analytical keys detected for the requested target-week range.';
    END IF;

    IF EXISTS(
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static
        WHERE isActive
          AND showOnOverview
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
    -- 3. Validation-only
    -- =========================================================================
    IF p_validateOnly THEN
        SELECT
            'VALIDATION_ONLY' AS status,
            v_weekFrom AS rebuildWeekStartFrom,
            v_weekTo AS rebuildWeekStartTo,
            v_weekEndTo AS latestWeekEndDate,
            CASE WHEN v_asOfDate<v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long' AS targetObject,
            'Validation passed. No Gold App table was created or modified.' AS message;
    ELSE
        -- =====================================================================
        -- 4. App table
        -- =====================================================================
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long(
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
            currentValue DOUBLE,
            currentValueDisplay STRING,
            comparisonValue DOUBLE,
            comparisonValueDisplay STRING,
            absoluteDiffValue DOUBLE,
            absoluteDiffDisplay STRING,
            changeValue DOUBLE,
            changeDisplay STRING,
            appProcessedAt TIMESTAMP
        )
        USING DELTA
        CLUSTER BY(metricName,targetWeekStartDate,comparisonType)
        COMMENT 'MIP Gold App: comparator-aware Overview Trend with reporting metadata, metric metadata, compact values, absolute differences and comparison changes.';

        -- =====================================================================
        -- 5. Rebuild requested reporting weeks
        -- =====================================================================
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long
        REPLACE WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        WITH scopeOverview AS(
            SELECT
                targetWeekStartDate,
                targetWeekEndDate,
                fiscalQuarterLabel,
                fiscalWeekCode,
                weekLabel,
                filterLob,
                filterPlatform,
                metricName,
                thisWeekNumerator,
                thisWeekDenominator,
                priorWeekNumerator,
                priorWeekDenominator,
                fourWeekTrendNumerator,
                fourWeekTrendDenominator,
                sameWeekLyNumerator,
                sameWeekLyDenominator,
                thisWeekDataAvailable,
                priorWeekDataAvailable,
                fourWeekTrendWeekCount,
                sameWeekLyDataAvailable
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
                CASE WHEN g.metricName='nbv' THEN 'Total UPV' ELSE m.metricLabel END AS metricLabel,
                m.metricDescription,
                m.metricKind,
                m.displayFormat,
                m.changeUnit,
                m.sortOrder AS metricSortOrder,
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
            FROM scopeOverview g
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static m
              ON m.metricName=g.metricName
             AND m.isActive
             AND m.showOnOverview
            LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c
              ON c.weekStartDate=g.targetWeekStartDate
        ),
        comparisonLong AS(
            SELECT
                base.*,
                'priorWeek' AS comparisonType,
                'Prior week' AS comparisonLabel,
                10 AS comparisonSortOrder,
                priorWeekDataAvailable AS comparisonDataAvailable,
                priorWeekNumerator AS comparisonNumerator,
                priorWeekDenominator AS comparisonDenominator
            FROM base
            UNION ALL
            SELECT
                base.*,
                'fourWeek' AS comparisonType,
                '4-wk trend' AS comparisonLabel,
                20 AS comparisonSortOrder,
                fourWeekTrendWeekCount>0 AS comparisonDataAvailable,
                CASE
                    WHEN metricKind='count' AND fourWeekTrendWeekCount>0
                        THEN try_divide(fourWeekTrendNumerator,cast(fourWeekTrendWeekCount AS DOUBLE))
                    ELSE fourWeekTrendNumerator
                END AS comparisonNumerator,
                CASE
                    WHEN metricKind='count' THEN NULL
                    ELSE fourWeekTrendDenominator
                END AS comparisonDenominator
            FROM base
            UNION ALL
            SELECT
                base.*,
                'lastYear' AS comparisonType,
                'Same wk LY' AS comparisonLabel,
                30 AS comparisonSortOrder,
                sameWeekLyDataAvailable AS comparisonDataAvailable,
                sameWeekLyNumerator AS comparisonNumerator,
                sameWeekLyDenominator AS comparisonDenominator
            FROM base
        ),
        valuesCalculated AS(
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
        changesCalculated AS(
            SELECT
                *,
                CASE
                    WHEN currentValue IS NULL OR comparisonValue IS NULL THEN NULL
                    ELSE currentValue-comparisonValue
                END AS absoluteDiffValue,
                CASE
                    WHEN currentValue IS NULL OR comparisonValue IS NULL THEN NULL
                    WHEN changeUnit='pp' THEN 100D*(currentValue-comparisonValue)
                    WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,comparisonValue)-1D)
                    ELSE NULL
                END AS changeRaw
            FROM valuesCalculated
        ),
        roundedValues AS(
            SELECT
                *,
                CASE
                    WHEN changeRaw IS NULL THEN NULL
                    WHEN abs(changeRaw)<0.05D THEN 0D
                    ELSE round(changeRaw,1)
                END AS changeValue
            FROM changesCalculated
        ),
        displayValues AS(
            SELECT
                *,
                CASE
                    WHEN currentValue IS NULL THEN NULL
                    WHEN displayFormat='percent' THEN concat(format_number(100D*currentValue,1),'%')
                    WHEN displayFormat='number' AND abs(currentValue)>=1000000000D THEN concat(format_number(currentValue/1000000000D,1),'B')
                    WHEN displayFormat='number' AND abs(currentValue)>=1000000D THEN concat(format_number(currentValue/1000000D,1),'M')
                    WHEN displayFormat='number' AND abs(currentValue)>=1000D THEN concat(format_number(currentValue/1000D,0),'K')
                    WHEN displayFormat='number' THEN format_number(currentValue,0)
                    ELSE cast(round(currentValue,2) AS STRING)
                END AS currentValueDisplay,
                CASE
                    WHEN comparisonValue IS NULL THEN NULL
                    WHEN displayFormat='percent' THEN concat(format_number(100D*comparisonValue,1),'%')
                    WHEN displayFormat='number' AND abs(comparisonValue)>=1000000000D THEN concat(format_number(comparisonValue/1000000000D,1),'B')
                    WHEN displayFormat='number' AND abs(comparisonValue)>=1000000D THEN concat(format_number(comparisonValue/1000000D,1),'M')
                    WHEN displayFormat='number' AND abs(comparisonValue)>=1000D THEN concat(format_number(comparisonValue/1000D,0),'K')
                    WHEN displayFormat='number' THEN format_number(comparisonValue,0)
                    ELSE cast(round(comparisonValue,2) AS STRING)
                END AS comparisonValueDisplay,
                CASE
                    WHEN absoluteDiffValue IS NULL THEN NULL
                    WHEN metricKind='ratio' THEN concat(
                        CASE WHEN 100D*absoluteDiffValue>0D THEN '+' ELSE '' END,
                        format_number(CASE WHEN abs(100D*absoluteDiffValue)<0.05D THEN 0D ELSE round(100D*absoluteDiffValue,1) END,1),
                        ' pp'
                    )
                    WHEN abs(absoluteDiffValue)>=1000000000D THEN concat(
                        CASE WHEN absoluteDiffValue>0D THEN '+' ELSE '' END,
                        format_number(absoluteDiffValue/1000000000D,1),
                        'B'
                    )
                    WHEN abs(absoluteDiffValue)>=1000000D THEN concat(
                        CASE WHEN absoluteDiffValue>0D THEN '+' ELSE '' END,
                        format_number(absoluteDiffValue/1000000D,1),
                        'M'
                    )
                    WHEN abs(absoluteDiffValue)>=1000D THEN concat(
                        CASE WHEN absoluteDiffValue>0D THEN '+' ELSE '' END,
                        format_number(absoluteDiffValue/1000D,0),
                        'K'
                    )
                    ELSE concat(
                        CASE WHEN absoluteDiffValue>0D THEN '+' ELSE '' END,
                        format_number(absoluteDiffValue,0)
                    )
                END AS absoluteDiffDisplay,
                CASE
                    WHEN changeValue IS NULL THEN NULL
                    WHEN changeUnit='pp' THEN concat(
                        CASE WHEN changeValue>0D THEN '+' ELSE '' END,
                        format_number(changeValue,1),
                        ' pp'
                    )
                    WHEN changeUnit='pct' THEN concat(
                        CASE WHEN changeValue>0D THEN '+' ELSE '' END,
                        format_number(changeValue,1),
                        '%'
                    )
                    ELSE cast(changeValue AS STRING)
                END AS changeDisplay
            FROM roundedValues
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
            currentValue,
            currentValueDisplay,
            comparisonValue,
            comparisonValueDisplay,
            absoluteDiffValue,
            absoluteDiffDisplay,
            changeValue,
            changeDisplay,
            v_processedAt AS appProcessedAt
        FROM displayValues;

        -- =====================================================================
        -- 6. Success
        -- =====================================================================
        SELECT
            'SUCCESS' AS status,
            v_weekFrom AS rebuiltWeekStartFrom,
            v_weekTo AS rebuiltWeekStartTo,
            v_weekEndTo AS latestWeekEndDate,
            CASE WHEN v_asOfDate<v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long' AS targetObject,
            v_processedAt AS appProcessedAt;
    END IF;
END;

-- ============================================================================
-- DEVELOPMENT EXAMPLES
-- ============================================================================

-- Validation only:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewTrend_long(
--     p_asOfDate=>DATE '2026-09-28',
--     p_weeksToRebuild=>1,
--     p_validateOnly=>TRUE
-- );

-- Rebuild 12 weeks so the graph has historical points:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewTrend_long(
--     p_asOfDate=>DATE '2026-09-28',
--     p_weeksToRebuild=>12,
--     p_validateOnly=>FALSE
-- );

-- ============================================================================
-- VALIDATION
-- ============================================================================

-- SELECT *
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long
-- ORDER BY metricSortOrder,comparisonSortOrder,targetWeekStartDate;

-- Expected zero duplicate rows:
-- SELECT targetWeekStartDate,filterLob,filterPlatform,metricName,comparisonType,count(*) AS rowCount
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long
-- GROUP BY targetWeekStartDate,filterLob,filterPlatform,metricName,comparisonType
-- HAVING count(*)>1;

-- Example: Total UPV + 4-wk trend:
-- SELECT
--     targetWeekStartDate,
--     targetWeekEndDate,
--     fiscalQuarterLabel,
--     fiscalWeekCode,
--     weekLabel,
--     weekEndingLabel,
--     metricName,
--     metricLabel,
--     currentValue,
--     currentValueDisplay,
--     comparisonType,
--     comparisonLabel,
--     comparisonValue,
--     comparisonValueDisplay,
--     absoluteDiffValue,
--     absoluteDiffDisplay,
--     changeValue,
--     changeDisplay
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long
-- WHERE filterLob='All'
--   AND filterPlatform='All'
--   AND metricName='nbv'
--   AND comparisonType='fourWeek'
-- ORDER BY targetWeekStartDate;