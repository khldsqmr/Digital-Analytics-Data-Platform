-- ============================================================================
-- FILE  : 01_sdi_sp_mip_gold_appOverviewCards_wide.sql
-- LAYER : GOLD / APP
-- TAB   : Overview
-- SECTION: Overview Cards
--
-- PURPOSE:
--   Application-ready Overview cards.
--
--   One row per:
--     target reporting week
--     x report filter context
--     x active Overview metric
--
-- APP CONTRACT:
--   The App table contains only values required by the API/UI:
--
--     - reporting-week key
--     - global filter context
--     - metric identity / UI metadata
--     - current metric value
--     - prior-week change
--     - four-week change
--     - same-week-last-year change
--     - forecast value
--     - display-ready versions of those values
--
--   Numerators / denominators remain in Analytical Gold and are NOT exposed
--   in this App table.
--
-- DESIGN:
--   - Reads reusable Gold analytical ingredients.
--   - Reads Metric Catalog for UI/metric metadata.
--   - Reads latest available Gold forecast.
--   - No App-table-to-App-table dependency.
--   - Comparison calculations happen once here, not in browser/API.
--   - Incremental/idempotent by target reporting week.
--   - p_weeksToRebuild controls the reporting-week range rebuilt.
--   - p_validateOnly = TRUE performs validation only.
--   - Default as-of date = previous Pacific calendar day.
--
-- CHANGE RULES:
--   changeUnit = 'pct'
--       100 * (current / comparison - 1)
--
--   changeUnit = 'pp'
--       100 * (current - comparison)
--
-- DISPLAY RULES:
--   displayFormat = 'number'
--       125430 -> "125,430"
--
--   displayFormat = 'percent'
--       0.0413 -> "4.1%"
--
--   changeUnit = 'pct'
--       -10.24 -> "-10.2%"
--
--   changeUnit = 'pp'
--       0.34 -> "+0.3 pp"
-- ============================================================================


CREATE OR REPLACE PROCEDURE
    prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewCards_wide(
        IN p_asOfDate       DATE    DEFAULT NULL,
        IN p_weeksToRebuild INT     DEFAULT 1,
        IN p_validateOnly   BOOLEAN DEFAULT FALSE
    )

LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA

COMMENT
    'MIP Gold App: simplified application-ready Overview Cards contract.'

AS

BEGIN

    -- ========================================================================
    -- 1. Runtime variables
    -- ========================================================================

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

    DECLARE v_processedAt TIMESTAMP
        DEFAULT current_timestamp();


    -- ========================================================================
    -- 2. Parameter validation
    -- ========================================================================

    IF p_weeksToRebuild IS NULL
       OR p_weeksToRebuild < 1
    THEN

        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT =
                'p_weeksToRebuild must be >= 1.';

    END IF;


    -- Sunday start of reporting week

    SET v_weekTo =
        date_add(
            v_asOfDate,
            1 - dayofweek(v_asOfDate)
        );


    -- Earliest target reporting week to rebuild

    SET v_weekFrom =
        date_add(
            v_weekTo,
            -7 * (p_weeksToRebuild - 1)
        );


    -- Saturday end of latest requested reporting week

    SET v_weekEndTo =
        date_add(
            v_weekTo,
            6
        );


    -- ========================================================================
    -- 3. Source validation
    -- ========================================================================

    -- ------------------------------------------------------------------------
    -- Analytical Overview Gold must contain rows for requested target weeks.
    -- ------------------------------------------------------------------------

    IF NOT EXISTS (

        SELECT
            1

        FROM
            prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long

        WHERE
            targetWeekStartDate
                BETWEEN v_weekFrom AND v_weekTo

        LIMIT 1

    )
    THEN

        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT =
                'Overview Gold analytical ingredients has no rows for the requested target-week range.';

    END IF;


    -- ------------------------------------------------------------------------
    -- Metric Catalog must contain active Overview metrics.
    -- ------------------------------------------------------------------------

    IF NOT EXISTS (

        SELECT
            1

        FROM
            prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static

        WHERE
            isActive
            AND showOnOverview

        LIMIT 1

    )
    THEN

        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT =
                'Metric Catalog has no active Overview metrics.';

    END IF;


    -- ------------------------------------------------------------------------
    -- Prevent unexpected duplicate Gold grain.
    --
    -- Expected:
    -- one row per targetWeekStartDate x filterLob x filterPlatform x metricName
    -- ------------------------------------------------------------------------

    IF EXISTS (

        SELECT
            1

        FROM
            prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long

        WHERE
            targetWeekStartDate
                BETWEEN v_weekFrom AND v_weekTo

        GROUP BY
            targetWeekStartDate,
            filterLob,
            filterPlatform,
            metricName

        HAVING
            count(*) > 1

        LIMIT 1

    )
    THEN

        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT =
                'Duplicate Overview Gold analytical keys detected for the requested target-week range.';

    END IF;


    -- ------------------------------------------------------------------------
    -- Validate UI metadata used by this App procedure.
    -- ------------------------------------------------------------------------

    IF EXISTS (

        SELECT
            1

        FROM
            prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static

        WHERE
            isActive
            AND showOnOverview

            AND (
                metricKind NOT IN (
                    'count',
                    'ratio'
                )

                OR displayFormat NOT IN (
                    'number',
                    'percent'
                )

                OR changeUnit NOT IN (
                    'pct',
                    'pp'
                )
            )

        LIMIT 1

    )
    THEN

        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT =
                'Metric Catalog contains unsupported metricKind, displayFormat, or changeUnit values for Overview metrics.';

    END IF;


    -- ========================================================================
    -- 4. Validation-only mode
    -- ========================================================================

    IF p_validateOnly
    THEN

        SELECT
            'VALIDATION_ONLY'
                AS status,

            v_weekFrom
                AS rebuildWeekStartFrom,

            v_weekTo
                AS rebuildWeekStartTo,

            v_weekEndTo
                AS latestWeekEndDate,

            CASE
                WHEN v_asOfDate < v_weekEndTo
                    THEN TRUE

                ELSE FALSE
            END
                AS latestWeekIsPartial,

            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide'
                AS targetObject,

            'Validation passed. No Gold App table was created or modified.'
                AS message;


    ELSE


        -- ====================================================================
        -- 5. Create App table if it does not exist
        --
        -- IMPORTANT:
        -- Existing legacy table must be dropped ONCE before first deployment
        -- of this new schema.
        -- ====================================================================

        CREATE TABLE IF NOT EXISTS
            prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide
        (

            -- ----------------------------------------------------------------
            -- Reporting grain / global filters
            -- ----------------------------------------------------------------

            targetWeekStartDate       DATE,

            filterLob                 STRING,

            filterPlatform            STRING,


            -- ----------------------------------------------------------------
            -- Metric identity / UI metadata
            -- ----------------------------------------------------------------

            metricName                STRING,

            metricLabel               STRING,

            metricDescription         STRING,

            metricKind                STRING,

            displayFormat             STRING,

            changeUnit                STRING,

            sortOrder                 INT,


            -- ----------------------------------------------------------------
            -- Current value
            -- ----------------------------------------------------------------

            currentValue              DOUBLE,

            currentValueDisplay       STRING,


            -- ----------------------------------------------------------------
            -- Prior-week comparison
            -- ----------------------------------------------------------------

            priorWeekChangeValue      DOUBLE,

            priorWeekChangeDisplay    STRING,


            -- ----------------------------------------------------------------
            -- Four-week comparison
            -- ----------------------------------------------------------------

            fourWeekChangeValue       DOUBLE,

            fourWeekChangeDisplay     STRING,


            -- ----------------------------------------------------------------
            -- Same-week-last-year comparison
            -- ----------------------------------------------------------------

            lastYearChangeValue       DOUBLE,

            lastYearChangeDisplay     STRING,


            -- ----------------------------------------------------------------
            -- Forecast
            -- ----------------------------------------------------------------

            forecastValue             DOUBLE,

            forecastValueDisplay      STRING,


            -- ----------------------------------------------------------------
            -- Processing metadata
            -- ----------------------------------------------------------------

            appProcessedAt            TIMESTAMP

        )

        USING DELTA

        CLUSTER BY (
            targetWeekStartDate,
            metricName
        )

        COMMENT
            'MIP Gold App: simplified Overview Cards contract containing final metric values, comparison changes and UI display values.';


        -- ====================================================================
        -- 6. Rebuild requested reporting-week range
        --
        -- No explicit INSERT column list is used with REPLACE WHERE.
        -- The final SELECT intentionally matches target-table column order.
        -- ====================================================================

        INSERT INTO TABLE
            prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide

        REPLACE WHERE
            targetWeekStartDate
                BETWEEN v_weekFrom AND v_weekTo

        WITH


        -- ====================================================================
        -- A. Requested Analytical Gold scope
        --
        -- Only columns required for the App calculation are projected.
        -- ====================================================================

        scopeOverview AS (

            SELECT
                targetWeekStartDate,

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

            FROM
                prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long

            WHERE
                targetWeekStartDate
                    BETWEEN v_weekFrom AND v_weekTo

        ),


        -- ====================================================================
        -- B. Latest available forecast
        --
        -- Forecast is optional.
        -- Lack of forecast simply produces NULL forecastValue.
        -- ====================================================================

        forecastLatest AS (

            SELECT
                weekStartDate,

                filterLob,

                filterPlatform,

                metricName,

                forecastValue

            FROM
                prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricForecastByWeek_long

            WHERE
                weekStartDate
                    BETWEEN v_weekFrom AND v_weekTo

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


        -- ====================================================================
        -- C. Convert Analytical Gold ingredients into final metric values
        --
        -- COUNT metric:
        --   current = numerator
        --
        -- RATIO metric:
        --   current = numerator / denominator
        --
        -- FOUR-WEEK COUNT:
        --   average weekly value
        --
        -- FOUR-WEEK RATIO:
        --   aggregated numerator / aggregated denominator
        -- ====================================================================

        metricValues AS (

            SELECT

                g.targetWeekStartDate,

                g.filterLob,

                g.filterPlatform,


                -- ------------------------------------------------------------
                -- Stable application metric key
                -- ------------------------------------------------------------

                g.metricName,


                -- ------------------------------------------------------------
                -- UI-facing label
                -- ------------------------------------------------------------

                CASE
                    WHEN g.metricName = 'nbv'
                        THEN 'Total UPV'

                    ELSE m.metricLabel
                END
                    AS metricLabel,


                m.metricDescription,

                m.metricKind,

                m.displayFormat,

                m.changeUnit,

                m.sortOrder,


                -- ------------------------------------------------------------
                -- Current value
                -- ------------------------------------------------------------

                CASE

                    WHEN NOT g.thisWeekDataAvailable
                        THEN NULL


                    WHEN m.metricKind = 'ratio'
                        THEN try_divide(
                            g.thisWeekNumerator,
                            g.thisWeekDenominator
                        )


                    ELSE
                        g.thisWeekNumerator

                END
                    AS currentValue,


                -- ------------------------------------------------------------
                -- Prior-week value
                -- Intermediate only.
                -- Not written to final App table.
                -- ------------------------------------------------------------

                CASE

                    WHEN NOT g.priorWeekDataAvailable
                        THEN NULL


                    WHEN m.metricKind = 'ratio'
                        THEN try_divide(
                            g.priorWeekNumerator,
                            g.priorWeekDenominator
                        )


                    ELSE
                        g.priorWeekNumerator

                END
                    AS priorWeekValue,


                -- ------------------------------------------------------------
                -- Four-week reference value
                --
                -- Count:
                --   average of available weekly counts
                --
                -- Ratio:
                --   aggregate numerator / aggregate denominator
                -- ------------------------------------------------------------

                CASE

                    WHEN g.fourWeekTrendWeekCount IS NULL
                      OR g.fourWeekTrendWeekCount <= 0
                        THEN NULL


                    WHEN m.metricKind = 'ratio'
                        THEN try_divide(
                            g.fourWeekTrendNumerator,
                            g.fourWeekTrendDenominator
                        )


                    ELSE
                        try_divide(
                            g.fourWeekTrendNumerator,
                            cast(
                                g.fourWeekTrendWeekCount
                                AS DOUBLE
                            )
                        )

                END
                    AS fourWeekValue,


                -- ------------------------------------------------------------
                -- Same-week-last-year value
                -- Intermediate only.
                -- ------------------------------------------------------------

                CASE

                    WHEN NOT g.sameWeekLyDataAvailable
                        THEN NULL


                    WHEN m.metricKind = 'ratio'
                        THEN try_divide(
                            g.sameWeekLyNumerator,
                            g.sameWeekLyDenominator
                        )


                    ELSE
                        g.sameWeekLyNumerator

                END
                    AS lastYearValue,


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

                ON f.weekStartDate =
                    g.targetWeekStartDate

               AND f.filterLob =
                    g.filterLob

               AND f.filterPlatform =
                    g.filterPlatform

               AND f.metricName =
                    g.metricName

        ),


        -- ====================================================================
        -- D. Calculate comparison changes
        --
        -- Result values are already in UI units.
        --
        -- Example:
        --
        --   count:
        --     -10.2473 = -10.2473%
        --
        --   ratio:
        --      0.3421 = +0.3421 percentage points
        -- ====================================================================

        comparisonValues AS (

            SELECT
                *,


                -- ------------------------------------------------------------
                -- Prior week
                -- ------------------------------------------------------------

                CASE

                    WHEN currentValue IS NULL
                      OR priorWeekValue IS NULL
                        THEN NULL


                    WHEN changeUnit = 'pp'
                        THEN
                            100D
                            * (
                                currentValue
                                - priorWeekValue
                            )


                    WHEN changeUnit = 'pct'
                        THEN
                            100D
                            * (
                                try_divide(
                                    currentValue,
                                    priorWeekValue
                                )
                                - 1D
                            )


                    ELSE NULL

                END
                    AS priorWeekChangeRaw,


                -- ------------------------------------------------------------
                -- Four-week
                -- ------------------------------------------------------------

                CASE

                    WHEN currentValue IS NULL
                      OR fourWeekValue IS NULL
                        THEN NULL


                    WHEN changeUnit = 'pp'
                        THEN
                            100D
                            * (
                                currentValue
                                - fourWeekValue
                            )


                    WHEN changeUnit = 'pct'
                        THEN
                            100D
                            * (
                                try_divide(
                                    currentValue,
                                    fourWeekValue
                                )
                                - 1D
                            )


                    ELSE NULL

                END
                    AS fourWeekChangeRaw,


                -- ------------------------------------------------------------
                -- Same week last year
                -- ------------------------------------------------------------

                CASE

                    WHEN currentValue IS NULL
                      OR lastYearValue IS NULL
                        THEN NULL


                    WHEN changeUnit = 'pp'
                        THEN
                            100D
                            * (
                                currentValue
                                - lastYearValue
                            )


                    WHEN changeUnit = 'pct'
                        THEN
                            100D
                            * (
                                try_divide(
                                    currentValue,
                                    lastYearValue
                                )
                                - 1D
                            )


                    ELSE NULL

                END
                    AS lastYearChangeRaw


            FROM
                metricValues

        ),


        -- ====================================================================
        -- E. Round comparison values once
        --
        -- Also normalizes tiny values to 0.0 so the UI does not receive
        -- "-0.0%" or "-0.0 pp".
        -- ====================================================================

        roundedValues AS (

            SELECT
                *,


                CASE

                    WHEN priorWeekChangeRaw IS NULL
                        THEN NULL

                    WHEN abs(priorWeekChangeRaw) < 0.05D
                        THEN 0D

                    ELSE
                        round(
                            priorWeekChangeRaw,
                            1
                        )

                END
                    AS priorWeekChangeValue,


                CASE

                    WHEN fourWeekChangeRaw IS NULL
                        THEN NULL

                    WHEN abs(fourWeekChangeRaw) < 0.05D
                        THEN 0D

                    ELSE
                        round(
                            fourWeekChangeRaw,
                            1
                        )

                END
                    AS fourWeekChangeValue,


                CASE

                    WHEN lastYearChangeRaw IS NULL
                        THEN NULL

                    WHEN abs(lastYearChangeRaw) < 0.05D
                        THEN 0D

                    ELSE
                        round(
                            lastYearChangeRaw,
                            1
                        )

                END
                    AS lastYearChangeValue


            FROM
                comparisonValues

        ),


        -- ====================================================================
        -- F. Add UI-ready display values
        -- ====================================================================

        displayValues AS (

            SELECT
                *,


                -- ------------------------------------------------------------
                -- Current metric
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


                    ELSE
                        cast(
                            round(
                                currentValue,
                                2
                            )
                            AS STRING
                        )

                END
                    AS currentValueDisplay,


                -- ------------------------------------------------------------
                -- Prior week
                -- ------------------------------------------------------------

                CASE

                    WHEN priorWeekChangeValue IS NULL
                        THEN NULL


                    WHEN changeUnit = 'pp'
                        THEN concat(

                            CASE
                                WHEN priorWeekChangeValue > 0D
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
                                WHEN priorWeekChangeValue > 0D
                                    THEN '+'

                                ELSE ''
                            END,

                            format_number(
                                priorWeekChangeValue,
                                1
                            ),

                            '%'
                        )


                    ELSE
                        cast(
                            priorWeekChangeValue
                            AS STRING
                        )

                END
                    AS priorWeekChangeDisplay,


                -- ------------------------------------------------------------
                -- Four week
                -- ------------------------------------------------------------

                CASE

                    WHEN fourWeekChangeValue IS NULL
                        THEN NULL


                    WHEN changeUnit = 'pp'
                        THEN concat(

                            CASE
                                WHEN fourWeekChangeValue > 0D
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
                                WHEN fourWeekChangeValue > 0D
                                    THEN '+'

                                ELSE ''
                            END,

                            format_number(
                                fourWeekChangeValue,
                                1
                            ),

                            '%'
                        )


                    ELSE
                        cast(
                            fourWeekChangeValue
                            AS STRING
                        )

                END
                    AS fourWeekChangeDisplay,


                -- ------------------------------------------------------------
                -- Same week last year
                -- ------------------------------------------------------------

                CASE

                    WHEN lastYearChangeValue IS NULL
                        THEN NULL


                    WHEN changeUnit = 'pp'
                        THEN concat(

                            CASE
                                WHEN lastYearChangeValue > 0D
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
                                WHEN lastYearChangeValue > 0D
                                    THEN '+'

                                ELSE ''
                            END,

                            format_number(
                                lastYearChangeValue,
                                1
                            ),

                            '%'
                        )


                    ELSE
                        cast(
                            lastYearChangeValue
                            AS STRING
                        )

                END
                    AS lastYearChangeDisplay,


                -- ------------------------------------------------------------
                -- Forecast
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


                    ELSE
                        cast(
                            round(
                                forecastValue,
                                2
                            )
                            AS STRING
                        )

                END
                    AS forecastValueDisplay


            FROM
                roundedValues

        )


        -- ====================================================================
        -- G. Final App contract
        --
        -- IMPORTANT:
        -- Column order matches the physical App table definition above.
        -- ====================================================================

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


            priorWeekChangeValue,

            priorWeekChangeDisplay,


            fourWeekChangeValue,

            fourWeekChangeDisplay,


            lastYearChangeValue,

            lastYearChangeDisplay,


            forecastValue,

            forecastValueDisplay,


            v_processedAt
                AS appProcessedAt


        FROM
            displayValues

        ;


        -- ====================================================================
        -- 7. Success metadata
        -- ====================================================================

        SELECT

            'SUCCESS'
                AS status,


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

            END
                AS latestWeekIsPartial,


            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide'
                AS targetObject,


            v_processedAt
                AS appProcessedAt;


    END IF;

END;


-- ============================================================================
-- DEVELOPMENT / VALIDATION EXAMPLES
-- ============================================================================


-- ----------------------------------------------------------------------------
-- Preflight only
-- ----------------------------------------------------------------------------

-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewCards_wide(
--     p_asOfDate       => DATE '2026-09-28',
--     p_weeksToRebuild => 1,
--     p_validateOnly   => TRUE
-- );


-- ----------------------------------------------------------------------------
-- Build / rebuild one target week
-- ----------------------------------------------------------------------------

-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewCards_wide(
--     p_asOfDate       => DATE '2026-09-28',
--     p_weeksToRebuild => 1,
--     p_validateOnly   => FALSE
-- );


-- ----------------------------------------------------------------------------
-- Example multi-week rebuild
-- ----------------------------------------------------------------------------

-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewCards_wide(
--     p_asOfDate       => DATE '2026-09-28',
--     p_weeksToRebuild => 12,
--     p_validateOnly   => FALSE
-- );


-- ============================================================================
-- APP TABLE VALIDATION
-- ============================================================================


-- ----------------------------------------------------------------------------
-- 1. Inspect latest rows
-- ----------------------------------------------------------------------------

-- SELECT
--     *
--
-- FROM
--     prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide
--
-- ORDER BY
--     targetWeekStartDate DESC,
--     filterLob,
--     filterPlatform,
--     sortOrder;


-- ----------------------------------------------------------------------------
-- 2. Inspect one reporting week / filter context
-- ----------------------------------------------------------------------------

-- SELECT
--     targetWeekStartDate,
--     filterLob,
--     filterPlatform,
--
--     metricName,
--     metricLabel,
--     metricDescription,
--     metricKind,
--     displayFormat,
--     changeUnit,
--
--     currentValue,
--     currentValueDisplay,
--
--     priorWeekChangeValue,
--     priorWeekChangeDisplay,
--
--     fourWeekChangeValue,
--     fourWeekChangeDisplay,
--
--     lastYearChangeValue,
--     lastYearChangeDisplay,
--
--     forecastValue,
--     forecastValueDisplay
--
-- FROM
--     prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide
--
-- WHERE
--     targetWeekStartDate = DATE '2026-09-27'
--
--     AND filterLob = 'All'
--
--     AND filterPlatform = 'All'
--
-- ORDER BY
--     sortOrder;


-- ----------------------------------------------------------------------------
-- 3. Duplicate-grain check
-- Expected result: zero rows
-- ----------------------------------------------------------------------------

-- SELECT
--     targetWeekStartDate,
--     filterLob,
--     filterPlatform,
--     metricName,
--     count(*) AS rowCount
--
-- FROM
--     prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide
--
-- GROUP BY
--     targetWeekStartDate,
--     filterLob,
--     filterPlatform,
--     metricName
--
-- HAVING
--     count(*) > 1;


-- ----------------------------------------------------------------------------
-- 4. Simple App/API-facing query
-- ----------------------------------------------------------------------------

-- SELECT
--     metricName,
--     metricLabel,
--     metricDescription,
--
--     currentValueDisplay,
--     priorWeekChangeDisplay,
--     fourWeekChangeDisplay,
--     lastYearChangeDisplay,
--     forecastValueDisplay
--
-- FROM
--     prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide
--
-- WHERE
--     targetWeekStartDate = DATE '2026-09-27'
--
--     AND filterLob = 'All'
--
--     AND filterPlatform = 'All'
--
-- ORDER BY
--     sortOrder;