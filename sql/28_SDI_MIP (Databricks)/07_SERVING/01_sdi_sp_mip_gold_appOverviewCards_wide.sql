-- ============================================================================
-- FILE  : 01_sdi_sp_mip_gold_appOverviewCards_wide.sql
-- LAYER : GOLD / APP
-- TAB   : Overview
-- SECTION: Overview Cards
-- PURPOSE:
--   Application-ready Overview cards.
--   One row per reporting week x filter context x active Overview metric.
--   Analytical numerator/denominator ingredients stay upstream in Gold.
--   App table contains reporting metadata, metric metadata, finished values,
--   comparison changes and display-ready values only.
-- ============================================================================

-- ONE-TIME MIGRATION ONLY:
-- Existing table has the previous schema. Run this manually ONCE before first
-- execution of the redesigned procedure:
-- DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide;

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewCards_wide(
    IN p_asOfDate DATE DEFAULT NULL,
    IN p_weeksToRebuild INT DEFAULT 1,
    IN p_validateOnly BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'MIP Gold App: application-ready Overview Cards.'
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
    IF p_weeksToRebuild IS NULL OR p_weeksToRebuild < 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'p_weeksToRebuild must be >= 1.';
    END IF;

    SET v_weekTo = date_add(v_asOfDate,1-dayofweek(v_asOfDate));
    SET v_weekFrom = date_add(v_weekTo,-7*(p_weeksToRebuild-1));
    SET v_weekEndTo = date_add(v_weekTo,6);

    -- =========================================================================
    -- 2. Source validation
    -- =========================================================================
    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
        WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Overview Gold analytical ingredients has no rows for the requested target-week range.';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static
        WHERE isActive AND showOnOverview
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Metric Catalog has no active Overview metrics.';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static
        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Fiscal Calendar has no rows for the requested target-week range.';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
        WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        GROUP BY targetWeekStartDate,filterLob,filterPlatform,metricName
        HAVING count(*) > 1
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Duplicate Overview Gold analytical keys detected for the requested target-week range.';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static
        WHERE isActive
          AND showOnOverview
          AND (
              metricKind NOT IN ('count','ratio')
              OR displayFormat NOT IN ('number','percent')
              OR changeUnit NOT IN ('pct','pp')
          )
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Metric Catalog contains unsupported metricKind, displayFormat or changeUnit values.';
    END IF;

    -- =========================================================================
    -- 3. Validation-only mode
    -- =========================================================================
    IF p_validateOnly THEN
        SELECT
            'VALIDATION_ONLY' AS status,
            v_weekFrom AS rebuildWeekStartFrom,
            v_weekTo AS rebuildWeekStartTo,
            v_weekEndTo AS latestWeekEndDate,
            CASE WHEN v_asOfDate < v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide' AS targetObject,
            'Validation passed. No Gold App table was created or modified.' AS message;
    ELSE
        -- =====================================================================
        -- 4. App table
        -- =====================================================================
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide(
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
            sortOrder INT,
            currentValue DOUBLE,
            currentValueDisplay STRING,
            priorWeekChangeValue DOUBLE,
            priorWeekChangeDisplay STRING,
            fourWeekChangeValue DOUBLE,
            fourWeekChangeDisplay STRING,
            lastYearChangeValue DOUBLE,
            lastYearChangeDisplay STRING,
            forecastValue DOUBLE,
            forecastValueDisplay STRING,
            appProcessedAt TIMESTAMP
        )
        USING DELTA
        CLUSTER BY (targetWeekStartDate,metricName)
        COMMENT 'MIP Gold App: Overview Cards with reporting metadata, metric metadata, finished comparison values and display-ready values.';

        -- =====================================================================
        -- 5. Rebuild requested reporting weeks
        -- =====================================================================
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide
        REPLACE WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        WITH scopeOverview AS (
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
        forecastLatest AS (
            SELECT
                weekStartDate,
                filterLob,
                filterPlatform,
                metricName,
                forecastValue
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricForecastByWeek_long
            WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
            QUALIFY row_number() OVER (
                PARTITION BY weekStartDate,filterLob,filterPlatform,metricName
                ORDER BY forecastCreatedAt DESC NULLS LAST,forecastRunId DESC NULLS LAST
            ) = 1
        ),
        metricValues AS (
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
                m.sortOrder,
                CASE
                    WHEN NOT g.thisWeekDataAvailable THEN NULL
                    WHEN m.metricKind='ratio' THEN try_divide(g.thisWeekNumerator,g.thisWeekDenominator)
                    ELSE g.thisWeekNumerator
                END AS currentValue,
                CASE
                    WHEN NOT g.priorWeekDataAvailable THEN NULL
                    WHEN m.metricKind='ratio' THEN try_divide(g.priorWeekNumerator,g.priorWeekDenominator)
                    ELSE g.priorWeekNumerator
                END AS priorWeekValue,
                CASE
                    WHEN g.fourWeekTrendWeekCount IS NULL OR g.fourWeekTrendWeekCount<=0 THEN NULL
                    WHEN m.metricKind='ratio' THEN try_divide(g.fourWeekTrendNumerator,g.fourWeekTrendDenominator)
                    ELSE try_divide(g.fourWeekTrendNumerator,cast(g.fourWeekTrendWeekCount AS DOUBLE))
                END AS fourWeekValue,
                CASE
                    WHEN NOT g.sameWeekLyDataAvailable THEN NULL
                    WHEN m.metricKind='ratio' THEN try_divide(g.sameWeekLyNumerator,g.sameWeekLyDenominator)
                    ELSE g.sameWeekLyNumerator
                END AS lastYearValue,
                f.forecastValue
            FROM scopeOverview g
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static m
              ON m.metricName=g.metricName
             AND m.isActive
             AND m.showOnOverview
            LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c
              ON c.weekStartDate=g.targetWeekStartDate
            LEFT JOIN forecastLatest f
              ON f.weekStartDate=g.targetWeekStartDate
             AND f.filterLob=g.filterLob
             AND f.filterPlatform=g.filterPlatform
             AND f.metricName=g.metricName
        ),
        comparisonValues AS (
            SELECT
                *,
                CASE
                    WHEN currentValue IS NULL OR priorWeekValue IS NULL THEN NULL
                    WHEN changeUnit='pp' THEN 100D*(currentValue-priorWeekValue)
                    WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,priorWeekValue)-1D)
                    ELSE NULL
                END AS priorWeekChangeRaw,
                CASE
                    WHEN currentValue IS NULL OR fourWeekValue IS NULL THEN NULL
                    WHEN changeUnit='pp' THEN 100D*(currentValue-fourWeekValue)
                    WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,fourWeekValue)-1D)
                    ELSE NULL
                END AS fourWeekChangeRaw,
                CASE
                    WHEN currentValue IS NULL OR lastYearValue IS NULL THEN NULL
                    WHEN changeUnit='pp' THEN 100D*(currentValue-lastYearValue)
                    WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,lastYearValue)-1D)
                    ELSE NULL
                END AS lastYearChangeRaw
            FROM metricValues
        ),
        roundedValues AS (
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
                END AS lastYearChangeValue
            FROM comparisonValues
        ),
        displayValues AS (
            SELECT
                *,
                CASE
                    WHEN currentValue IS NULL THEN NULL
                    WHEN displayFormat='percent' THEN concat(format_number(100D*currentValue,1),'%')
                    WHEN displayFormat='number' THEN format_number(currentValue,0)
                    ELSE cast(round(currentValue,2) AS STRING)
                END AS currentValueDisplay,
                CASE
                    WHEN priorWeekChangeValue IS NULL THEN NULL
                    WHEN changeUnit='pp' THEN concat(CASE WHEN priorWeekChangeValue>0D THEN '+' ELSE '' END,format_number(priorWeekChangeValue,1),' pp')
                    WHEN changeUnit='pct' THEN concat(CASE WHEN priorWeekChangeValue>0D THEN '+' ELSE '' END,format_number(priorWeekChangeValue,1),'%')
                    ELSE cast(priorWeekChangeValue AS STRING)
                END AS priorWeekChangeDisplay,
                CASE
                    WHEN fourWeekChangeValue IS NULL THEN NULL
                    WHEN changeUnit='pp' THEN concat(CASE WHEN fourWeekChangeValue>0D THEN '+' ELSE '' END,format_number(fourWeekChangeValue,1),' pp')
                    WHEN changeUnit='pct' THEN concat(CASE WHEN fourWeekChangeValue>0D THEN '+' ELSE '' END,format_number(fourWeekChangeValue,1),'%')
                    ELSE cast(fourWeekChangeValue AS STRING)
                END AS fourWeekChangeDisplay,
                CASE
                    WHEN lastYearChangeValue IS NULL THEN NULL
                    WHEN changeUnit='pp' THEN concat(CASE WHEN lastYearChangeValue>0D THEN '+' ELSE '' END,format_number(lastYearChangeValue,1),' pp')
                    WHEN changeUnit='pct' THEN concat(CASE WHEN lastYearChangeValue>0D THEN '+' ELSE '' END,format_number(lastYearChangeValue,1),'%')
                    ELSE cast(lastYearChangeValue AS STRING)
                END AS lastYearChangeDisplay,
                CASE
                    WHEN forecastValue IS NULL THEN NULL
                    WHEN displayFormat='percent' THEN concat(format_number(100D*forecastValue,1),'%')
                    WHEN displayFormat='number' THEN format_number(forecastValue,0)
                    ELSE cast(round(forecastValue,2) AS STRING)
                END AS forecastValueDisplay
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
            sortOrder,
            currentValue,
            currentValueDisplay,
            priorWeekChangeValue,
            priorWeekChangeDisplay,
            fourWeekChangeValue,
            fourWeekChangeDisplay,
            lastYearChangeValue,
            lastYearChangeDisplay,
            forecastValue,
            forecastValueDisplay,
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
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide' AS targetObject,
            v_processedAt AS appProcessedAt;
    END IF;
END;

-- ============================================================================
-- DEVELOPMENT EXAMPLES
-- ============================================================================

-- Validation only:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewCards_wide(
--     p_asOfDate => DATE '2026-09-28',
--     p_weeksToRebuild => 1,
--     p_validateOnly => TRUE
-- );

-- Rebuild:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewCards_wide(
--     p_asOfDate => DATE '2026-09-28',
--     p_weeksToRebuild => 1,
--     p_validateOnly => FALSE
-- );

-- ============================================================================
-- VALIDATION QUERIES
-- ============================================================================

-- SELECT *
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide
-- ORDER BY targetWeekStartDate DESC,filterLob,filterPlatform,sortOrder;

-- Duplicate check; expected zero rows:
-- SELECT targetWeekStartDate,filterLob,filterPlatform,metricName,count(*) AS rowCount
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide
-- GROUP BY targetWeekStartDate,filterLob,filterPlatform,metricName
-- HAVING count(*)>1;

-- API-style read:
-- SELECT
--     targetWeekStartDate,
--     targetWeekEndDate,
--     fiscalQuarterLabel,
--     fiscalWeekCode,
--     weekLabel,
--     weekEndingLabel,
--     filterLob,
--     filterPlatform,
--     metricName,
--     metricLabel,
--     metricDescription,
--     metricKind,
--     displayFormat,
--     changeUnit,
--     currentValue,
--     currentValueDisplay,
--     priorWeekChangeValue,
--     priorWeekChangeDisplay,
--     fourWeekChangeValue,
--     fourWeekChangeDisplay,
--     lastYearChangeValue,
--     lastYearChangeDisplay,
--     forecastValue,
--     forecastValueDisplay
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide
-- WHERE targetWeekStartDate=DATE '2026-09-27'
--   AND filterLob='All'
--   AND filterPlatform='All'
-- ORDER BY sortOrder;