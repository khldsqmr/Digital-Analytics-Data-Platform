-- ============================================================================
-- FILE  : 01_sdi_sp_mip_gold_appOverviewCards_wide.sql
-- LAYER : GOLD / APP
-- TAB   : Overview
--
-- PURPOSE:
--   Application-ready Overview cards.
--
--   One row per:
--     target reporting week
--     x report filter context
--     x overview metric
--
-- APP CONTRACT:
--   - App/API receives finished metric values and comparison changes.
--   - Numerator/denominator ingredients remain in Analytical Gold only.
--   - App table does not expose unnecessary fiscal/calendar/model metadata.
--   - Numeric values are retained for sorting / UI logic.
--   - Display-ready values are persisted for direct browser/API use.
--
-- DESIGN:
--   - Reads Analytical Gold + Metric Catalog + Forecast Gold.
--   - No app-table-to-app-table dependency.
--   - Incremental/idempotent by target reporting week.
--   - Browser/API does not recompute metric or comparison math.
--   - Previous complete Pacific calendar day is the default as-of date.
-- ============================================================================

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewCards_wide(
    IN p_asOfDate       DATE    DEFAULT NULL,
    IN p_weeksToRebuild INT     DEFAULT 1,
    IN p_validateOnly   BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'MIP Gold app: simplified application-ready Overview cards.'
AS
BEGIN

    -- ------------------------------------------------------------------------
    -- 1. Runtime variables
    -- ------------------------------------------------------------------------

    DECLARE v_asOfDate DATE DEFAULT coalesce(
        p_asOfDate,
        date_add(
            to_date(
                from_utc_timestamp(
                    current_timestamp(),
                    'America/Los_Angeles'
                )
            ),
            -1
        )
    );

    DECLARE v_weekTo DATE;
    DECLARE v_weekFrom DATE;
    DECLARE v_weekEndTo DATE;
    DECLARE v_processedAt TIMESTAMP DEFAULT current_timestamp();


    -- ------------------------------------------------------------------------
    -- 2. Parameter validation
    -- ------------------------------------------------------------------------

    IF p_weeksToRebuild IS NULL OR p_weeksToRebuild < 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'p_weeksToRebuild must be >= 1.';
    END IF;

    SET v_weekTo =
        date_add(
            v_asOfDate,
            1 - dayofweek(v_asOfDate)
        );

    SET v_weekFrom =
        date_add(
            v_weekTo,
            -7 * (p_weeksToRebuild - 1)
        );

    SET v_weekEndTo =
        date_add(
            v_weekTo,
            6
        );


    -- ------------------------------------------------------------------------
    -- 3. Source validation
    -- ------------------------------------------------------------------------

    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
        WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT =
                'Overview Gold analytical ingredients has no rows for the requested app target-week range.';
    END IF;


    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static
        WHERE isActive
          AND showOnOverview
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT =
                'Metric catalog has no active Overview metrics.';
    END IF;


    -- ------------------------------------------------------------------------
    -- 4. Validation-only mode
    -- ------------------------------------------------------------------------

    IF p_validateOnly THEN

        SELECT
            'VALIDATION_ONLY' AS status,
            v_weekFrom AS rebuildWeekStartFrom,
            v_weekTo AS rebuildWeekStartTo,
            v_weekEndTo AS latestWeekEndDate,
            CASE
                WHEN v_asOfDate < v_weekEndTo
                    THEN TRUE
                ELSE FALSE
            END AS latestWeekIsPartial,
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide'
                AS targetObject,
            'No Gold app table was created or modified.'
                AS message;


    ELSE

        -- --------------------------------------------------------------------
        -- 5. Create simplified App table if it does not already exist
        -- --------------------------------------------------------------------

        CREATE TABLE IF NOT EXISTS
            prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide
        (
            targetWeekStartDate       DATE,

            filterLob                 STRING,
            filterPlatform            STRING,

            metricName                STRING,
            metricLabel               STRING,
            metricDescription         STRING,
            metricKind                STRING,
            displayFormat             STRING,
            changeUnit                STRING,
            sortOrder                 INT,

            currentValue              DOUBLE,
            currentValueDisplay       STRING,

            priorWeekChangeValue      DOUBLE,
            priorWeekChangeDisplay    STRING,

            fourWeekChangeValue       DOUBLE,
            fourWeekChangeDisplay     STRING,

            lastYearChangeValue       DOUBLE,
            lastYearChangeDisplay     STRING,

            forecastValue             DOUBLE,
            forecastValueDisplay      STRING,

            appProcessedAt            TIMESTAMP
        )
        USING DELTA
        CLUSTER BY (
            targetWeekStartDate,
            metricName
        )
        COMMENT
            'MIP Gold app: simplified Overview card contract with finished metric and comparison values.';


        -- --------------------------------------------------------------------
        -- 6. Rebuild requested target-week range
        -- --------------------------------------------------------------------

        INSERT INTO TABLE
            prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide

        REPLACE WHERE
            targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo

        (
            targetWeekStartDate,

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

            appProcessedAt
        )

        WITH

        -- --------------------------------------------------------------------
        -- Analytical Gold rows required for requested App weeks
        -- --------------------------------------------------------------------

        scopeOverview AS (

            SELECT
                targetWeekStartDate,
                filterLob,
                filterPlatform,

                metricName,
                metricLabel,
                metricKind,
                displayFormat,
                changeUnit,

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

            FROM
                prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long

            WHERE
                targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo

        ),


        -- --------------------------------------------------------------------
        -- Forecast rows required for requested App weeks
        -- --------------------------------------------------------------------

        scopeForecast AS (

            SELECT
                weekStartDate,
                filterLob,
                filterPlatform,
                metricName,

                forecastValue,
                forecastRunId,
                forecastCreatedAt

            FROM
                prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricForecastByWeek_long

            WHERE
                weekStartDate BETWEEN v_weekFrom AND v_weekTo

        ),


        -- --------------------------------------------------------------------
        -- Keep only most recent forecast for each week/filter/metric
        -- --------------------------------------------------------------------

        forecastLatest AS (

            SELECT
                weekStartDate,
                filterLob,
                filterPlatform,
                metricName,
                forecastValue

            FROM
                scopeForecast

            QUALIFY
                row_number() OVER (
                    PARTITION BY
                        weekStartDate,
                        filterLob,
                        filterPlatform,
                        metricName

                    ORDER BY
                        forecastCreatedAt DESC NULLS LAST,
                        forecastRunId DESC NULLS LAST
                ) = 1

        ),


        -- --------------------------------------------------------------------
        -- Convert Analytical Gold ingredients into finished metric values
        -- --------------------------------------------------------------------

        metricValues AS (

            SELECT
                g.targetWeekStartDate,

                g.filterLob,
                g.filterPlatform,

                g.metricName,

                CASE
                    WHEN g.metricName = 'nbv'
                        THEN 'Total UPV'
                    ELSE g.metricLabel
                END AS metricLabel,

                m.metricDescription,

                g.metricKind,
                g.displayFormat,
                g.changeUnit,

                m.sortOrder,


                -- ------------------------------------------------------------
                -- Current value
                -- ------------------------------------------------------------

                CASE
                    WHEN NOT g.thisWeekDataAvailable
                        THEN NULL

                    WHEN g.metricKind = 'ratio'
                        THEN try_divide(
                            g.thisWeekNumerator,
                            g.thisWeekDenominator
                        )

                    ELSE g.thisWeekNumerator
                END AS currentValue,


                -- ------------------------------------------------------------
                -- Prior-week value
                -- ------------------------------------------------------------

                CASE
                    WHEN NOT g.priorWeekDataAvailable
                        THEN NULL

                    WHEN g.metricKind = 'ratio'
                        THEN try_divide(
                            g.priorWeekNumerator,
                            g.priorWeekDenominator
                        )

                    ELSE g.priorWeekNumerator
                END AS priorWeekValue,


                -- ------------------------------------------------------------
                -- Four-week trend value
                --
                -- Count:
                --   average weekly count over available 4-week window
                --
                -- Ratio:
                --   aggregated numerator / aggregated denominator
                -- ------------------------------------------------------------

                CASE
                    WHEN g.fourWeekTrendWeekCount IS NULL
                      OR g.fourWeekTrendWeekCount <= 0
                        THEN NULL

                    WHEN g.metricKind = 'ratio'
                        THEN try_divide(
                            g.fourWeekTrendNumerator,
                            g.fourWeekTrendDenominator
                        )

                    ELSE try_divide(
                        g.fourWeekTrendNumerator,
                        cast(
                            g.fourWeekTrendWeekCount
                            AS DOUBLE
                        )
                    )
                END AS fourWeekValue,


                -- ------------------------------------------------------------
                -- Same-week-last-year value
                -- ------------------------------------------------------------

                CASE
                    WHEN NOT g.sameWeekLyDataAvailable
                        THEN NULL

                    WHEN g.metricKind = 'ratio'
                        THEN try_divide(
                            g.sameWeekLyNumerator,
                            g.sameWeekLyDenominator
                        )

                    ELSE g.sameWeekLyNumerator
                END AS lastYearValue,


                -- ------------------------------------------------------------
                -- Latest forecast
                -- ------------------------------------------------------------

                f.forecastValue

            FROM
                scopeOverview g

            JOIN
                prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static m

                ON m.metricName = g.metricName
               AND m.isActive
               AND m.showOnOverview

            LEFT JOIN
                forecastLatest f

                ON f.weekStartDate = g.targetWeekStartDate
               AND f.filterLob = g.filterLob
               AND f.filterPlatform = g.filterPlatform
               AND f.metricName = g.metricName

        ),


        -- --------------------------------------------------------------------
        -- Calculate comparison change values
        --
        -- count metric:
        --     percent change
        --
        -- ratio metric:
        --     percentage-point change
        --
        -- Numeric values are already expressed in UI units:
        --
        --     -10.24 = -10.24%
        --       0.35 = +0.35 pp
        -- --------------------------------------------------------------------

        comparisonValues AS (

            SELECT
                *,

                CASE
                    WHEN currentValue IS NULL
                      OR priorWeekValue IS NULL
                        THEN NULL

                    WHEN changeUnit = 'pp'
                        THEN 100D
                             * (
                                 currentValue
                                 - priorWeekValue
                             )

                    WHEN changeUnit = 'pct'
                        THEN 100D
                             * (
                                 try_divide(
                                     currentValue,
                                     priorWeekValue
                                 )
                                 - 1D
                             )

                    ELSE NULL
                END AS priorWeekChangeValue,


                CASE
                    WHEN currentValue IS NULL
                      OR fourWeekValue IS NULL
                        THEN NULL

                    WHEN changeUnit = 'pp'
                        THEN 100D
                             * (
                                 currentValue
                                 - fourWeekValue
                             )

                    WHEN changeUnit = 'pct'
                        THEN 100D
                             * (
                                 try_divide(
                                     currentValue,
                                     fourWeekValue
                                 )
                                 - 1D
                             )

                    ELSE NULL
                END AS fourWeekChangeValue,


                CASE
                    WHEN currentValue IS NULL
                      OR lastYearValue IS NULL
                        THEN NULL

                    WHEN changeUnit = 'pp'
                        THEN 100D
                             * (
                                 currentValue
                                 - lastYearValue
                             )

                    WHEN changeUnit = 'pct'
                        THEN 100D
                             * (
                                 try_divide(
                                     currentValue,
                                     lastYearValue
                                 )
                                 - 1D
                             )

                    ELSE NULL
                END AS lastYearChangeValue

            FROM
                metricValues

        ),


        -- --------------------------------------------------------------------
        -- Add browser-ready display values
        -- --------------------------------------------------------------------

        displayValues AS (

            SELECT
                *,

                -- ------------------------------------------------------------
                -- Current value display
                -- ------------------------------------------------------------

                CASE
                    WHEN currentValue IS NULL
                        THEN NULL

                    WHEN displayFormat = 'percent'
                        THEN concat(
                            format_number(
                                100D * currentValue,
                                1
                            ),
                            '%'
                        )

                    WHEN displayFormat = 'number'
                        THEN format_number(
                            currentValue,
                            0
                        )

                    ELSE cast(
                        round(
                            currentValue,
                            2
                        )
                        AS STRING
                    )
                END AS currentValueDisplay,


                -- ------------------------------------------------------------
                -- Prior-week display
                -- ------------------------------------------------------------

                CASE
                    WHEN priorWeekChangeValue IS NULL
                        THEN NULL

                    WHEN changeUnit = 'pp'
                        THEN concat(
                            CASE
                                WHEN priorWeekChangeValue > 0
                                    THEN '+'
                                ELSE ''
                            END,
                            format_number(
                                priorWeekChangeValue,
                                1
                            ),
                            ' pp'
                        )

                    WHEN changeUnit = 'pct'
                        THEN concat(
                            CASE
                                WHEN priorWeekChangeValue > 0
                                    THEN '+'
                                ELSE ''
                            END,
                            format_number(
                                priorWeekChangeValue,
                                1
                            ),
                            '%'
                        )

                    ELSE cast(
                        round(
                            priorWeekChangeValue,
                            1
                        )
                        AS STRING
                    )
                END AS priorWeekChangeDisplay,


                -- ------------------------------------------------------------
                -- Four-week display
                -- ------------------------------------------------------------

                CASE
                    WHEN fourWeekChangeValue IS NULL
                        THEN NULL

                    WHEN changeUnit = 'pp'
                        THEN concat(
                            CASE
                                WHEN fourWeekChangeValue > 0
                                    THEN '+'
                                ELSE ''
                            END,
                            format_number(
                                fourWeekChangeValue,
                                1
                            ),
                            ' pp'
                        )

                    WHEN changeUnit = 'pct'
                        THEN concat(
                            CASE
                                WHEN fourWeekChangeValue > 0
                                    THEN '+'
                                ELSE ''
                            END,
                            format_number(
                                fourWeekChangeValue,
                                1
                            ),
                            '%'
                        )

                    ELSE cast(
                        round(
                            fourWeekChangeValue,
                            1
                        )
                        AS STRING
                    )
                END AS fourWeekChangeDisplay,


                -- ------------------------------------------------------------
                -- Same-week-last-year display
                -- ------------------------------------------------------------

                CASE
                    WHEN lastYearChangeValue IS NULL
                        THEN NULL

                    WHEN changeUnit = 'pp'
                        THEN concat(
                            CASE
                                WHEN lastYearChangeValue > 0
                                    THEN '+'
                                ELSE ''
                            END,
                            format_number(
                                lastYearChangeValue,
                                1
                            ),
                            ' pp'
                        )

                    WHEN changeUnit = 'pct'
                        THEN concat(
                            CASE
                                WHEN lastYearChangeValue > 0
                                    THEN '+'
                                ELSE ''
                            END,
                            format_number(
                                lastYearChangeValue,
                                1
                            ),
                            '%'
                        )

                    ELSE cast(
                        round(
                            lastYearChangeValue,
                            1
                        )
                        AS STRING
                    )
                END AS lastYearChangeDisplay,


                -- ------------------------------------------------------------
                -- Forecast display
                -- ------------------------------------------------------------

                CASE
                    WHEN forecastValue IS NULL
                        THEN NULL

                    WHEN displayFormat = 'percent'
                        THEN concat(
                            format_number(
                                100D * forecastValue,
                                1
                            ),
                            '%'
                        )

                    WHEN displayFormat = 'number'
                        THEN format_number(
                            forecastValue,
                            0
                        )

                    ELSE cast(
                        round(
                            forecastValue,
                            2
                        )
                        AS STRING
                    )
                END AS forecastValueDisplay

            FROM
                comparisonValues

        )


        -- --------------------------------------------------------------------
        -- Final application contract
        -- --------------------------------------------------------------------

        SELECT
            targetWeekStartDate,

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

            round(
                priorWeekChangeValue,
                1
            ) AS priorWeekChangeValue,

            priorWeekChangeDisplay,

            round(
                fourWeekChangeValue,
                1
            ) AS fourWeekChangeValue,

            fourWeekChangeDisplay,

            round(
                lastYearChangeValue,
                1
            ) AS lastYearChangeValue,

            lastYearChangeDisplay,

            forecastValue,
            forecastValueDisplay,

            v_processedAt AS appProcessedAt

        FROM
            displayValues
        ;


        -- --------------------------------------------------------------------
        -- 7. Success metadata
        -- --------------------------------------------------------------------

        SELECT
            'SUCCESS' AS status,

            v_weekFrom
                AS rebuiltWeekStartFrom,

            v_weekTo
                AS rebuiltWeekStartTo,

            v_weekEndTo
                AS latestWeekEndDate,

            CASE
                WHEN v_asOfDate < v_weekEndTo
                    THEN TRUE
                ELSE FALSE
            END AS latestWeekIsPartial,

            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide'
                AS targetObject,

            v_processedAt
                AS appProcessedAt;

    END IF;

END;


-- ============================================================================
-- DEVELOPMENT EXAMPLES
-- ============================================================================

-- Validation only:
--
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewCards_wide(
--     p_asOfDate       => DATE '2026-09-28',
--     p_weeksToRebuild => 1,
--     p_validateOnly   => TRUE
-- );


-- Load / rebuild:
--
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewCards_wide(
--     p_asOfDate       => DATE '2026-09-28',
--     p_weeksToRebuild => 1,
--     p_validateOnly   => FALSE
-- );


-- Example API-facing result:
--
-- SELECT
--     targetWeekStartDate,
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
--
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide
--
-- WHERE targetWeekStartDate = DATE '2026-09-27'
--   AND filterLob = 'All'
--   AND filterPlatform = 'All'
--
-- ORDER BY sortOrder;


-- DROP TABLE IF EXISTS
--     prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide;

-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewCards_wide(
--     p_asOfDate       => DATE '2026-09-28',
--     p_weeksToRebuild => 12,
--     p_validateOnly   => FALSE
-- );