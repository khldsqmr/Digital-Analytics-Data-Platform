-- ============================================================================
-- FILE  : 01_sdi_sp_mip_gold_appOverviewCards_wide.sql
-- LAYER : GOLD / APP
-- TAB   : Overview
-- PURPOSE:
--   Application-ready Overview cards; one row per reporting week × report filter context × overview metric.
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

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewCards_wide(
    IN p_asOfDate       DATE    DEFAULT NULL,
    IN p_weeksToRebuild INT     DEFAULT 1,
    IN p_validateOnly   BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'MIP Gold app: Overview cards. Wide comparison contract for simultaneous Prior week, 4-wk trend, Same wk LY and Forecast.'
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
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide' AS targetObject,
            'No Gold app table was created or modified.' AS message;

    ELSE

        -- --------------------------------------------------------------------
        -- 4. Bootstrap target schema only if the table does not exist.
        --    The zero-row CTAS keeps the target schema exactly aligned to the
        --    application contract without materialized-view/serverless compute.
        -- --------------------------------------------------------------------
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide
        USING DELTA
        CLUSTER BY (targetWeekStartDate, metricName)
        COMMENT 'MIP Gold app: Overview cards. Wide comparison contract for simultaneous Prior week, 4-wk trend, Same wk LY and Forecast.'
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
                    SELECT
                        weekStartDate,
                        weekEndDate,
                        filterLob,
                        filterPlatform,
                        metricName,
                        forecastValue,
                        forecastLow,
                        forecastHigh,
                        modelName,
                        modelVersion,
                        forecastRunId,
                        forecastCreatedAt
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
                        CASE WHEN g.metricName = 'nbv' THEN 'Total UPV' ELSE g.metricLabel END AS uiMetricLabel,
                        m.metricDescription,
                        g.metricKind,
                        g.displayFormat,
                        g.changeUnit,
                        m.hasForecast,
                        m.definitionStatus,
                        m.sortOrder,
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
                        CASE WHEN g.metricKind = 'ratio' THEN try_divide(g.thisWeekNumerator, g.thisWeekDenominator)
                             ELSE g.thisWeekNumerator END AS currentValue,
                        CASE WHEN g.metricKind = 'ratio' THEN try_divide(g.priorWeekNumerator, g.priorWeekDenominator)
                             ELSE g.priorWeekNumerator END AS priorWeekValue,
                        CASE WHEN g.fourWeekTrendWeekCount <= 0 THEN NULL
                             WHEN g.metricKind = 'ratio' THEN try_divide(g.fourWeekTrendNumerator, g.fourWeekTrendDenominator)
                             ELSE try_divide(g.fourWeekTrendNumerator, cast(g.fourWeekTrendWeekCount AS DOUBLE)) END AS fourWeekValue,
                        CASE WHEN g.metricKind = 'ratio' THEN try_divide(g.sameWeekLyNumerator, g.sameWeekLyDenominator)
                             ELSE g.sameWeekLyNumerator END AS lastYearValue,
                        f.forecastValue,
                        f.forecastLow,
                        f.forecastHigh,
                        f.modelName AS forecastModelName,
                        f.modelVersion AS forecastModelVersion,
                        f.forecastRunId,
                        f.forecastCreatedAt,
                        g.goldProcessedAt
                    FROM scopeOverview g
                    JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static m
                      ON m.metricName = g.metricName AND m.isActive AND m.showOnOverview
                    LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c
                      ON c.weekStartDate = g.targetWeekStartDate
                    LEFT JOIN forecastLatest f
                      ON f.weekStartDate = g.targetWeekStartDate
                     AND f.filterLob = g.filterLob
                     AND f.filterPlatform = g.filterPlatform
                     AND f.metricName = g.metricName
                )
                SELECT
                    *,
                    currentValue - priorWeekValue AS priorWeekAbsoluteDeltaValue,
                    CASE WHEN changeUnit='pp' THEN 100D*(currentValue-priorWeekValue)
                         WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,priorWeekValue)-1D) END AS priorWeekChangeValue,
                    currentValue - fourWeekValue AS fourWeekAbsoluteDeltaValue,
                    CASE WHEN changeUnit='pp' THEN 100D*(currentValue-fourWeekValue)
                         WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,fourWeekValue)-1D) END AS fourWeekChangeValue,
                    currentValue - lastYearValue AS lastYearAbsoluteDeltaValue,
                    CASE WHEN changeUnit='pp' THEN 100D*(currentValue-lastYearValue)
                         WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,lastYearValue)-1D) END AS lastYearChangeValue,
                    currentValue - forecastValue AS forecastAbsoluteDeltaValue,
                    CASE WHEN forecastValue IS NULL THEN NULL
                         WHEN changeUnit='pp' THEN 100D*(currentValue-forecastValue)
                         WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,forecastValue)-1D) END AS forecastChangeValue,
                    forecastValue IS NOT NULL AS forecastDataAvailable,
                    fourWeekTrendWeekCount = 4 AS fourWeekWindowComplete
                FROM base
            ) appResult
        ) schemaBootstrap
        WHERE 1 = 0;

        -- --------------------------------------------------------------------
        -- 5. Rebuild requested whole target-week range.
        --    Whole-week replacement is intentional because comparator ranks,
        --    Top-N membership and (Other) buckets can all change together.
        -- --------------------------------------------------------------------
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide
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
        SELECT
            weekStartDate,
            weekEndDate,
            filterLob,
            filterPlatform,
            metricName,
            forecastValue,
            forecastLow,
            forecastHigh,
            modelName,
            modelVersion,
            forecastRunId,
            forecastCreatedAt
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
            CASE WHEN g.metricName = 'nbv' THEN 'Total UPV' ELSE g.metricLabel END AS uiMetricLabel,
            m.metricDescription,
            g.metricKind,
            g.displayFormat,
            g.changeUnit,
            m.hasForecast,
            m.definitionStatus,
            m.sortOrder,
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
            CASE WHEN g.metricKind = 'ratio' THEN try_divide(g.thisWeekNumerator, g.thisWeekDenominator)
                 ELSE g.thisWeekNumerator END AS currentValue,
            CASE WHEN g.metricKind = 'ratio' THEN try_divide(g.priorWeekNumerator, g.priorWeekDenominator)
                 ELSE g.priorWeekNumerator END AS priorWeekValue,
            CASE WHEN g.fourWeekTrendWeekCount <= 0 THEN NULL
                 WHEN g.metricKind = 'ratio' THEN try_divide(g.fourWeekTrendNumerator, g.fourWeekTrendDenominator)
                 ELSE try_divide(g.fourWeekTrendNumerator, cast(g.fourWeekTrendWeekCount AS DOUBLE)) END AS fourWeekValue,
            CASE WHEN g.metricKind = 'ratio' THEN try_divide(g.sameWeekLyNumerator, g.sameWeekLyDenominator)
                 ELSE g.sameWeekLyNumerator END AS lastYearValue,
            f.forecastValue,
            f.forecastLow,
            f.forecastHigh,
            f.modelName AS forecastModelName,
            f.modelVersion AS forecastModelVersion,
            f.forecastRunId,
            f.forecastCreatedAt,
            g.goldProcessedAt
        FROM scopeOverview g
        JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static m
          ON m.metricName = g.metricName AND m.isActive AND m.showOnOverview
        LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c
          ON c.weekStartDate = g.targetWeekStartDate
        LEFT JOIN forecastLatest f
          ON f.weekStartDate = g.targetWeekStartDate
         AND f.filterLob = g.filterLob
         AND f.filterPlatform = g.filterPlatform
         AND f.metricName = g.metricName
    )
    SELECT
        *,
        currentValue - priorWeekValue AS priorWeekAbsoluteDeltaValue,
        CASE WHEN changeUnit='pp' THEN 100D*(currentValue-priorWeekValue)
             WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,priorWeekValue)-1D) END AS priorWeekChangeValue,
        currentValue - fourWeekValue AS fourWeekAbsoluteDeltaValue,
        CASE WHEN changeUnit='pp' THEN 100D*(currentValue-fourWeekValue)
             WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,fourWeekValue)-1D) END AS fourWeekChangeValue,
        currentValue - lastYearValue AS lastYearAbsoluteDeltaValue,
        CASE WHEN changeUnit='pp' THEN 100D*(currentValue-lastYearValue)
             WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,lastYearValue)-1D) END AS lastYearChangeValue,
        currentValue - forecastValue AS forecastAbsoluteDeltaValue,
        CASE WHEN forecastValue IS NULL THEN NULL
             WHEN changeUnit='pp' THEN 100D*(currentValue-forecastValue)
             WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,forecastValue)-1D) END AS forecastChangeValue,
        forecastValue IS NOT NULL AS forecastDataAvailable,
        fourWeekTrendWeekCount = 4 AS fourWeekWindowComplete
    FROM base
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
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide' AS targetObject,
            v_processedAt AS appProcessedAt;

    END IF;
END;

-- Development examples:
--
-- Preflight only:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewCards_wide(
--     p_asOfDate       => DATE '2026-09-28',
--     p_weeksToRebuild => 1,
--     p_validateOnly   => TRUE
-- );
--
-- Load / rebuild:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewCards_wide(
--     p_asOfDate       => DATE '2026-09-28',
--     p_weeksToRebuild => 1,
--     p_validateOnly   => FALSE
-- );
