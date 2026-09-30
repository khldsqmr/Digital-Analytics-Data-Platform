-- ============================================================================
-- FILE  : 02_sdi_sp_mip_gold_appOverviewTrend_long.sql
-- LAYER : GOLD / APP
-- TAB   : Overview
-- PURPOSE:
--   Application-ready comparator-aware Overview trend for Prior week, 4-wk trend and Same wk LY.
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

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewTrend_long(
    IN p_asOfDate       DATE    DEFAULT NULL,
    IN p_weeksToRebuild INT     DEFAULT 1,
    IN p_validateOnly   BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'MIP Gold app: Overview trend. Comparator-aware long contract with forecast overlay fields.'
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
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long' AS targetObject,
            'No Gold app table was created or modified.' AS message;

    ELSE

        -- --------------------------------------------------------------------
        -- 4. Bootstrap target schema only if the table does not exist.
        --    The zero-row CTAS keeps the target schema exactly aligned to the
        --    application contract without materialized-view/serverless compute.
        -- --------------------------------------------------------------------
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long
        USING DELTA
        CLUSTER BY (metricName, targetWeekStartDate, comparisonType)
        COMMENT 'MIP Gold app: Overview trend. Comparator-aware long contract with forecast overlay fields.'
        AS
        SELECT *
        FROM (
            SELECT
                appResult.*,
                v_processedAt AS appProcessedAt
            FROM (
                WITH
                scopeOverview AS (
                    SELECT *
                    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
                    WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
                ),
                scopeForecast AS (
                    SELECT *
                    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricForecastByWeek_long
                    WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
                ),
                forecastLatest AS (
                    SELECT *
                    FROM scopeForecast
                    QUALIFY row_number() OVER (
                        PARTITION BY weekStartDate, filterLob, filterPlatform, metricName
                        ORDER BY forecastCreatedAt DESC NULLS LAST, forecastRunId DESC NULLS LAST
                    ) = 1
                ),
                base AS (
                    SELECT
                        g.targetWeekStartDate,
                        g.targetWeekEndDate,
                        g.fiscalQuarterLabel,
                        g.fiscalWeekCode,
                        g.weekLabel,
                        c.weekEndingLabel,
                        c.priorWeekStartDate,
                        c.fourWeekAvgStartDate,
                        c.fourWeekAvgEndDate,
                        c.sameWeekLastYearStartDate,
                        g.filterLob,
                        g.filterPlatform,
                        g.metricName,
                        g.metricLabel,
                        CASE WHEN g.metricName='nbv' THEN 'Total UPV' ELSE g.metricLabel END AS uiMetricLabel,
                        m.metricDescription,
                        g.metricKind,
                        g.displayFormat,
                        g.changeUnit,
                        m.definitionStatus,
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
                        g.sameWeekLyDataAvailable,
                        g.goldProcessedAt,
                        f.forecastValue,
                        f.forecastLow,
                        f.forecastHigh,
                        f.modelName AS forecastModelName,
                        f.modelVersion AS forecastModelVersion,
                        f.forecastRunId,
                        f.forecastCreatedAt
                    FROM scopeOverview g
                    JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static m
                      ON m.metricName=g.metricName AND m.isActive AND m.showOnOverview
                    LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c
                      ON c.weekStartDate=g.targetWeekStartDate
                    LEFT JOIN forecastLatest f
                      ON f.weekStartDate=g.targetWeekStartDate
                     AND f.filterLob=g.filterLob
                     AND f.filterPlatform=g.filterPlatform
                     AND f.metricName=g.metricName
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
                    uiMetricLabel,
                    metricDescription,
                    metricKind,
                    displayFormat,
                    changeUnit,
                    definitionStatus,
                    metricSortOrder,
                    comparisonType,
                    comparisonLabel,
                    comparisonSortOrder,
                    comparisonStartDate,
                    comparisonEndDate,
                    comparisonWeekCount,
                    comparisonDataAvailable,
                    comparisonWindowComplete,
                    thisWeekNumerator AS currentNumerator,
                    thisWeekDenominator AS currentDenominator,
                    comparisonNumerator,
                    comparisonDenominator,
                    currentValue AS actualValue,
                    comparisonValue,
                    absoluteDeltaValue,
                    changeValue,
                    changeDirection,
                    forecastValue,
                    forecastLow,
                    forecastHigh,
                    forecastModelName,
                    forecastModelVersion,
                    forecastRunId,
                    forecastCreatedAt,
                    thisWeekDataAvailable,
                    goldProcessedAt
                FROM deltasCalculated
            ) appResult
        ) schemaBootstrap
        WHERE 1 = 0;

        -- --------------------------------------------------------------------
        -- 5. Rebuild requested whole target-week range.
        --    Whole-week replacement is intentional because comparator ranks,
        --    Top-N membership and (Other) buckets can all change together.
        -- --------------------------------------------------------------------
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long
        REPLACE WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
SELECT
    appResult.*,
    v_processedAt AS appProcessedAt
FROM (
    WITH
    scopeOverview AS (
        SELECT *
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
        WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
    ),
    scopeForecast AS (
        SELECT *
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricForecastByWeek_long
        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
    ),
    forecastLatest AS (
        SELECT *
        FROM scopeForecast
        QUALIFY row_number() OVER (
            PARTITION BY weekStartDate, filterLob, filterPlatform, metricName
            ORDER BY forecastCreatedAt DESC NULLS LAST, forecastRunId DESC NULLS LAST
        ) = 1
    ),
    base AS (
        SELECT
            g.targetWeekStartDate,
            g.targetWeekEndDate,
            g.fiscalQuarterLabel,
            g.fiscalWeekCode,
            g.weekLabel,
            c.weekEndingLabel,
            c.priorWeekStartDate,
            c.fourWeekAvgStartDate,
            c.fourWeekAvgEndDate,
            c.sameWeekLastYearStartDate,
            g.filterLob,
            g.filterPlatform,
            g.metricName,
            g.metricLabel,
            CASE WHEN g.metricName='nbv' THEN 'Total UPV' ELSE g.metricLabel END AS uiMetricLabel,
            m.metricDescription,
            g.metricKind,
            g.displayFormat,
            g.changeUnit,
            m.definitionStatus,
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
            g.sameWeekLyDataAvailable,
            g.goldProcessedAt,
            f.forecastValue,
            f.forecastLow,
            f.forecastHigh,
            f.modelName AS forecastModelName,
            f.modelVersion AS forecastModelVersion,
            f.forecastRunId,
            f.forecastCreatedAt
        FROM scopeOverview g
        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static m
          ON m.metricName=g.metricName AND m.isActive AND m.showOnOverview
        LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c
          ON c.weekStartDate=g.targetWeekStartDate
        LEFT JOIN forecastLatest f
          ON f.weekStartDate=g.targetWeekStartDate
         AND f.filterLob=g.filterLob
         AND f.filterPlatform=g.filterPlatform
         AND f.metricName=g.metricName
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
        uiMetricLabel,
        metricDescription,
        metricKind,
        displayFormat,
        changeUnit,
        definitionStatus,
        metricSortOrder,
        comparisonType,
        comparisonLabel,
        comparisonSortOrder,
        comparisonStartDate,
        comparisonEndDate,
        comparisonWeekCount,
        comparisonDataAvailable,
        comparisonWindowComplete,
        thisWeekNumerator AS currentNumerator,
        thisWeekDenominator AS currentDenominator,
        comparisonNumerator,
        comparisonDenominator,
        currentValue AS actualValue,
        comparisonValue,
        absoluteDeltaValue,
        changeValue,
        changeDirection,
        forecastValue,
        forecastLow,
        forecastHigh,
        forecastModelName,
        forecastModelVersion,
        forecastRunId,
        forecastCreatedAt,
        thisWeekDataAvailable,
        goldProcessedAt
    FROM deltasCalculated
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
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long' AS targetObject,
            v_processedAt AS appProcessedAt;

    END IF;
END;

-- Development examples:
--
-- Preflight only:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewTrend_long(
--     p_asOfDate       => DATE '2026-09-28',
--     p_weeksToRebuild => 1,
--     p_validateOnly   => TRUE
-- );
--
-- Load / rebuild:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewTrend_long(
--     p_asOfDate       => DATE '2026-09-28',
--     p_weeksToRebuild => 1,
--     p_validateOnly   => FALSE
-- );
