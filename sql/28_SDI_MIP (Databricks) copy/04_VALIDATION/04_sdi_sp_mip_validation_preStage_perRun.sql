-- ============================================================================
-- FILE  : 01_sdi_sp_mip_validation_preStage_perRun.sql
-- LAYER : VALIDATION
-- PURPOSE:
--   Lightweight PRE-stage readiness gate.
--
-- POLICY:
--   - Missing/late business data => WARNING, gateAction=PROCEED.
--   - Missing mandatory control/configuration => FAILED, gateAction=STOP.
--   - Warnings are persisted with nextSteps but never stop orchestration.
--   - STOP rows SIGNAL and therefore stop the parent orchestration/notebook.
-- ============================================================================

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_validation_preStage_perRun(
    IN p_stageName       STRING,
    IN p_runId           STRING DEFAULT NULL,
    IN p_asOfDate        DATE DEFAULT NULL,
    IN p_eventWindowDays INT DEFAULT 1,
    IN p_weeksToCheck    INT DEFAULT 1
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'MIP PRE-stage validation gate. Only blocking configuration failures stop orchestration; routine data availability is warning-only.'
AS
BEGIN
    DECLARE v_stageName STRING DEFAULT upper(trim(p_stageName));
    DECLARE v_runId STRING DEFAULT coalesce(
        nullif(trim(p_runId), ''),
        concat(
            'MAN_',
            date_format(from_utc_timestamp(current_timestamp(), 'America/Los_Angeles'), 'yyyyMMdd_HHmmss'),
            '_',
            upper(substr(sha2(concat(cast(current_timestamp() AS STRING), cast(rand() AS STRING)), 256), 1, 8))
        )
    );
    DECLARE v_checkedAt TIMESTAMP DEFAULT current_timestamp();
    DECLARE v_asOfDate DATE DEFAULT coalesce(
        p_asOfDate,
        date_add(
            to_date(from_utc_timestamp(current_timestamp(), 'America/Los_Angeles')),
            -1
        )
    );
    DECLARE v_windowEnd DATE;
    DECLARE v_windowStart DATE;
    DECLARE v_weekTo DATE;
    DECLARE v_weekFrom DATE;
    DECLARE v_stopCount BIGINT DEFAULT 0;

    IF v_stageName NOT IN (
        'BRONZE',
        'SILVER_HIT',
        'SILVER_SESSION',
        'SILVER_WEEK',
        'GOLD_OVERVIEW',
        'GOLD_BREAKOUT',
        'GOLD_CROSSTAB',
        'GOLD_EXPLORE',
        'GOLD_APP'
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Invalid p_stageName for MIP PRE validation.';
    END IF;

    IF p_eventWindowDays IS NULL OR p_eventWindowDays < 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'p_eventWindowDays must be >= 1.';
    END IF;

    IF p_weeksToCheck IS NULL OR p_weeksToCheck < 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'p_weeksToCheck must be >= 1.';
    END IF;

    SET v_windowEnd = v_asOfDate;
    SET v_windowStart = date_add(v_asOfDate, -(p_eventWindowDays - 1));
    SET v_weekTo = date_add(v_asOfDate, 1 - dayofweek(v_asOfDate));
    SET v_weekFrom = date_add(v_weekTo, -7 * (p_weeksToCheck - 1));

    DELETE FROM prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun
    WHERE runId = v_runId
      AND validationPhase = 'PRE'
      AND stageName = v_stageName;

    -- ------------------------------------------------------------------------
    -- Mandatory controls.
    -- These are structural dependencies. Empty controls are blocking.
    -- ------------------------------------------------------------------------
    INSERT INTO prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun
    WITH controls AS (
        SELECT
            'prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static' AS objectName,
            count(*) AS rowCount
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static
        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo

        UNION ALL

        SELECT
            'prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static',
            count(*)
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static
        WHERE isActive

        UNION ALL

        SELECT
            'prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static',
            count(*)
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static
        WHERE isActive

        UNION ALL

        SELECT
            'prdrzranalytics.lab42.sdi_vw_mip_control_crosstabCatalog_static',
            count(*)
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_crosstabCatalog_static
        WHERE isActive
    )
    SELECT
        concat('VAL_', upper(substr(sha2(concat_ws('|', v_runId, 'PRE', v_stageName, objectName, 'controlAvailability'), 256), 1, 24))),
        v_runId,
        v_checkedAt,
        v_asOfDate,
        'PRE',
        v_stageName,
        'CONTROL',
        objectName,
        'configuration',
        v_weekFrom,
        v_weekTo,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        'controlAvailability',
        'AVAILABILITY',
        'CONFIGURATION',
        1D,
        cast(rowCount AS DOUBLE),
        cast(rowCount - 1 AS DOUBLE),
        NULL,
        CASE WHEN rowCount > 0 THEN 'HEALTHY' ELSE 'FAILED' END,
        CASE WHEN rowCount > 0 THEN 'INFO' ELSE 'CRITICAL' END,
        CASE WHEN rowCount > 0 THEN 'PROCEED' ELSE 'STOP' END,
        CASE WHEN rowCount > 0 THEN FALSE ELSE TRUE END,
        NULL,
        NULL,
        CASE
            WHEN rowCount > 0 THEN 'Required control/configuration is available.'
            ELSE 'Required control/configuration is missing or empty.'
        END,
        CASE WHEN rowCount = 0 THEN 'Control/configuration was not seeded, refreshed, or does not cover the requested week.' END,
        CASE WHEN rowCount = 0 THEN 'Refresh the required control object before running this stage.' END,
        'MIP Data Engineering',
        NULL,
        NULL,
        NULL,
        NULL
    FROM controls;

    -- ------------------------------------------------------------------------
    -- Stage-specific upstream readiness.
    -- Missing data is a WARNING and does not stop the pipeline by itself.
    -- Child procedures retain their own hard source preflight as a final guard.
    -- ------------------------------------------------------------------------
    INSERT INTO prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun
    WITH readiness AS (
        SELECT
            'BRONZE' AS stageName,
            'BRONZE_SOURCE',
            'prd_dbi_analytics.silver_digital_interactions.unified_digital_interactions' AS objectName,
            'eventDate' AS scopeType,
            v_windowStart AS scopeStart,
            v_windowEnd AS scopeEnd,
            count(*) AS rowCount
        FROM prd_dbi_analytics.silver_digital_interactions.unified_digital_interactions
        WHERE event_date BETWEEN v_windowStart AND v_windowEnd
        HAVING v_stageName = 'BRONZE'

        UNION ALL

        SELECT
            'SILVER_HIT',
            'BRONZE',
            'prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHits_daily',
            'eventDate',
            v_windowStart,
            v_windowEnd,
            count(*)
        FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHits_daily
        WHERE event_date BETWEEN v_windowStart AND v_windowEnd
        HAVING v_stageName = 'SILVER_HIT'

        UNION ALL

        SELECT
            'SILVER_SESSION',
            'SILVER',
            'prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily',
            'eventDate',
            v_windowStart,
            v_windowEnd,
            count(*)
        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily
        WHERE eventDate BETWEEN v_windowStart AND v_windowEnd
        HAVING v_stageName = 'SILVER_SESSION'

        UNION ALL

        SELECT
            'SILVER_WEEK',
            'SILVER',
            'prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily',
            'sessionStartDatePst',
            v_windowStart,
            v_windowEnd,
            count(*)
        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily
        WHERE sessionStartDatePst BETWEEN v_windowStart AND v_windowEnd
        HAVING v_stageName = 'SILVER_WEEK'

        UNION ALL

        SELECT
            'GOLD_OVERVIEW',
            'SILVER',
            'prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly',
            'weekStartDate',
            v_weekFrom,
            v_weekTo,
            count(*)
        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly
        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
        HAVING v_stageName = 'GOLD_OVERVIEW'

        UNION ALL

        SELECT
            'GOLD_BREAKOUT',
            'GOLD',
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long',
            'weekStartDate',
            v_weekFrom,
            v_weekTo,
            count(*)
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
        WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        HAVING v_stageName = 'GOLD_BREAKOUT'

        UNION ALL

        SELECT
            'GOLD_CROSSTAB',
            'GOLD',
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long',
            'weekStartDate',
            v_weekFrom,
            v_weekTo,
            count(*)
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
        WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        HAVING v_stageName = 'GOLD_CROSSTAB'

        UNION ALL

        SELECT
            'GOLD_EXPLORE',
            'SILVER',
            'prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily',
            'sessionStartDatePst',
            v_windowStart,
            v_windowEnd,
            count(*)
        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily
        WHERE sessionStartDatePst BETWEEN v_windowStart AND v_windowEnd
        HAVING v_stageName = 'GOLD_EXPLORE'

        UNION ALL

        SELECT
            'GOLD_APP',
            'GOLD',
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long',
            'weekStartDate',
            v_weekFrom,
            v_weekTo,
            count(*)
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
        WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        HAVING v_stageName = 'GOLD_APP'

        UNION ALL

        SELECT
            'GOLD_APP',
            'GOLD',
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long',
            'weekStartDate',
            v_weekFrom,
            v_weekTo,
            count(*)
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
        WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        HAVING v_stageName = 'GOLD_APP'

        UNION ALL

        SELECT
            'GOLD_APP',
            'GOLD',
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long',
            'weekStartDate',
            v_weekFrom,
            v_weekTo,
            count(*)
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long
        WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        HAVING v_stageName = 'GOLD_APP'

        UNION ALL

        SELECT
            'GOLD_APP',
            'GOLD',
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_exploreSessionPageCategoryByWeek_wide',
            'weekStartDate',
            v_weekFrom,
            v_weekTo,
            count(*)
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_exploreSessionPageCategoryByWeek_wide
        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
        HAVING v_stageName = 'GOLD_APP'
    )
    SELECT
        concat('VAL_', upper(substr(sha2(concat_ws('|', v_runId, 'PRE', stageName, objectName, 'upstreamAvailability'), 256), 1, 24))),
        v_runId,
        v_checkedAt,
        v_asOfDate,
        'PRE',
        stageName,
        CASE WHEN stageName = 'BRONZE' THEN 'BRONZE' ELSE regexp_extract(objectName, 'sdi_tbl_mip_([^_]+)', 1) END,
        objectName,
        scopeType,
        scopeStart,
        scopeEnd,
        CASE WHEN scopeType = 'weekStartDate' THEN scopeEnd ELSE NULL END,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        'upstreamAvailability',
        'AVAILABILITY',
        'DATA_AVAILABILITY',
        1D,
        cast(rowCount AS DOUBLE),
        cast(rowCount - 1 AS DOUBLE),
        NULL,
        CASE WHEN rowCount > 0 THEN 'HEALTHY' ELSE 'WARNING' END,
        CASE WHEN rowCount > 0 THEN 'INFO' ELSE 'MEDIUM' END,
        'PROCEED',
        FALSE,
        NULL,
        NULL,
        CASE
            WHEN rowCount > 0 THEN 'Required upstream object contains data in the requested scope.'
            ELSE 'Required upstream object has no rows in the requested scope.'
        END,
        CASE WHEN rowCount = 0 THEN 'The upstream source may be late, incomplete, or the requested date/week has not been loaded yet.' END,
        CASE WHEN rowCount = 0 THEN 'Check upstream availability. The child procedure still performs its own hard source preflight before writing.' END,
        'MIP Data Engineering',
        NULL,
        NULL,
        NULL,
        NULL
    FROM readiness;

    -- Comparator readiness is informational/warning-only for App Gold.
    IF v_stageName = 'GOLD_APP' THEN
        INSERT INTO prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun
        WITH comparatorAvailability AS (
            SELECT
                'OVERVIEW' AS sourceType,
                sum(CASE WHEN priorWeekDataAvailable THEN 1 ELSE 0 END) AS priorRows,
                sum(CASE WHEN fourWeekTrendWeekCount > 0 THEN 1 ELSE 0 END) AS fourWeekRows,
                max(coalesce(fourWeekTrendWeekCount, 0)) AS maxFourWeekCount,
                sum(CASE WHEN sameWeekLyDataAvailable THEN 1 ELSE 0 END) AS lastYearRows
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
            WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo

            UNION ALL

            SELECT
                'BREAKOUT',
                sum(CASE WHEN priorWeekDataAvailable THEN 1 ELSE 0 END),
                sum(CASE WHEN fourWeekTrendWeekCount > 0 THEN 1 ELSE 0 END),
                max(coalesce(fourWeekTrendWeekCount, 0)),
                sum(CASE WHEN sameWeekLyDataAvailable THEN 1 ELSE 0 END)
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
            WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo

            UNION ALL

            SELECT
                'CROSSTAB',
                sum(CASE WHEN priorWeekDataAvailable THEN 1 ELSE 0 END),
                sum(CASE WHEN fourWeekTrendWeekCount > 0 THEN 1 ELSE 0 END),
                max(coalesce(fourWeekTrendWeekCount, 0)),
                sum(CASE WHEN sameWeekLyDataAvailable THEN 1 ELSE 0 END)
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long
            WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        ),
        longForm AS (
            SELECT sourceType, 'priorWeek' AS comparisonType, priorRows AS availableRows, cast(NULL AS BIGINT) AS weekCount FROM comparatorAvailability
            UNION ALL
            SELECT sourceType, 'fourWeek', fourWeekRows, maxFourWeekCount FROM comparatorAvailability
            UNION ALL
            SELECT sourceType, 'lastYear', lastYearRows, cast(NULL AS BIGINT) FROM comparatorAvailability
        )
        SELECT
            concat('VAL_', upper(substr(sha2(concat_ws('|', v_runId, 'PRE', 'GOLD_APP', sourceType, comparisonType, 'comparatorAvailability'), 256), 1, 24))),
            v_runId,
            v_checkedAt,
            v_asOfDate,
            'PRE',
            'GOLD_APP',
            'GOLD',
            concat(sourceType, ' analytical Gold'),
            'weekStartDate',
            v_weekFrom,
            v_weekTo,
            v_weekTo,
            NULL,
            comparisonType,
            NULL,
            NULL,
            NULL,
            NULL,
            'comparatorAvailability',
            'COVERAGE',
            'COMPARATOR_AVAILABILITY',
            CASE WHEN comparisonType = 'fourWeek' THEN 4D ELSE 1D END,
            CASE WHEN comparisonType = 'fourWeek' THEN cast(coalesce(weekCount, 0) AS DOUBLE)
                 ELSE CASE WHEN coalesce(availableRows, 0) > 0 THEN 1D ELSE 0D END END,
            NULL,
            NULL,
            CASE
                WHEN comparisonType = 'fourWeek' AND coalesce(weekCount, 0) >= 4 THEN 'HEALTHY'
                WHEN comparisonType <> 'fourWeek' AND coalesce(availableRows, 0) > 0 THEN 'HEALTHY'
                ELSE 'WARNING'
            END,
            CASE
                WHEN comparisonType = 'fourWeek' AND coalesce(weekCount, 0) >= 4 THEN 'INFO'
                WHEN comparisonType <> 'fourWeek' AND coalesce(availableRows, 0) > 0 THEN 'INFO'
                ELSE 'LOW'
            END,
            'PROCEED',
            FALSE,
            NULL,
            NULL,
            CASE
                WHEN comparisonType = 'fourWeek' AND coalesce(weekCount, 0) >= 4 THEN concat(sourceType, ' has complete four-week comparison history.')
                WHEN comparisonType <> 'fourWeek' AND coalesce(availableRows, 0) > 0 THEN concat(sourceType, ' has ', comparisonType, ' comparison history.')
                ELSE concat(sourceType, ' is missing or incomplete for ', comparisonType, ' comparison history.')
            END,
            CASE WHEN (
                (comparisonType = 'fourWeek' AND coalesce(weekCount, 0) < 4)
                OR (comparisonType <> 'fourWeek' AND coalesce(availableRows, 0) = 0)
            ) THEN 'Historical comparison data has not been loaded/backfilled yet.' END,
            CASE WHEN (
                (comparisonType = 'fourWeek' AND coalesce(weekCount, 0) < 4)
                OR (comparisonType <> 'fourWeek' AND coalesce(availableRows, 0) = 0)
            ) THEN 'Backfill the required historical week(s) when needed. Current-week App sections can still proceed.' END,
            'MIP Data Engineering',
            NULL,
            NULL,
            NULL,
            NULL
        FROM longForm;
    END IF;

    SET v_stopCount = (
        SELECT count(*)
        FROM prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun
        WHERE runId = v_runId
          AND validationPhase = 'PRE'
          AND stageName = v_stageName
          AND gateAction = 'STOP'
    );

    SELECT
        v_runId AS runId,
        v_stageName AS stageName,
        'PRE' AS validationPhase,
        count(*) AS checkCount,
        sum(CASE WHEN checkStatus = 'WARNING' THEN 1 ELSE 0 END) AS warningCount,
        sum(CASE WHEN checkStatus IN ('FAILED', 'ERROR') THEN 1 ELSE 0 END) AS failedOrErrorCount,
        sum(CASE WHEN gateAction = 'STOP' THEN 1 ELSE 0 END) AS stopCount,
        CASE WHEN v_stopCount > 0 THEN 'STOP' ELSE 'PROCEED' END AS gateAction
    FROM prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun
    WHERE runId = v_runId
      AND validationPhase = 'PRE'
      AND stageName = v_stageName;

    IF v_stopCount > 0 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'MIP_PRE_VALIDATION_STOP: blocking PRE-stage validation failed. Review sdi_tbl_mip_validation_checkHistory_perRun.';
    END IF;
END;
