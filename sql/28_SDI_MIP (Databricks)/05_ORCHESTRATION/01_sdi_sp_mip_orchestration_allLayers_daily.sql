-- ============================================================================
-- FILE  : 01_sdi_sp_mip_orchestration_allLayers_daily.sql
-- LAYER : ORCHESTRATION
-- PURPOSE:
--   Thin database orchestration for the full MIP pipeline.
--
-- DESIGN:
--   - Main procedure can be called with no arguments.
--   - Default as-of date = previous Pacific calendar day.
--   - One runId is generated automatically when not supplied.
--   - PRE and POST validation wrap logical stages, not every child procedure.
--   - Validation WARNING/INFO rows do not stop execution.
--   - Validation FAILED/ERROR rows with gateAction=STOP SIGNAL and propagate.
--   - Child stored-procedure errors also propagate naturally to the notebook/job.
--   - Job/run/task IDs remain nullable until Databricks Jobs parameters are wired.
-- ============================================================================

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_orchestration_allLayers_daily(
    IN p_runId           STRING  DEFAULT NULL,
    IN p_asOfDate        DATE    DEFAULT NULL,
    IN p_eventWindowDays INT     DEFAULT 1,
    IN p_weeksToRebuild  INT     DEFAULT 1,
    IN p_refreshControl  BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'MIP end-to-end orchestration. CALL with no args for normal previous-Pacific-day processing; PRE/POST validation failures and child SQL errors propagate to the calling notebook/job.'
AS
BEGIN
    DECLARE v_runStartedAt TIMESTAMP DEFAULT current_timestamp();
    DECLARE v_runId STRING DEFAULT coalesce(
        nullif(trim(p_runId), ''),
        concat(
            'MAN_',
            date_format(from_utc_timestamp(current_timestamp(), 'America/Los_Angeles'), 'yyyyMMdd_HHmmss'),
            '_',
            upper(substr(sha2(concat(cast(current_timestamp() AS STRING), cast(rand() AS STRING)), 256), 1, 8))
        )
    );
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
    DECLARE v_warningCount BIGINT DEFAULT 0;
    DECLARE v_failureCount BIGINT DEFAULT 0;

    IF p_eventWindowDays IS NULL OR p_eventWindowDays < 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'p_eventWindowDays must be >= 1.';
    END IF;

    IF p_weeksToRebuild IS NULL OR p_weeksToRebuild < 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'p_weeksToRebuild must be >= 1.';
    END IF;

    SET v_windowEnd = v_asOfDate;
    SET v_windowStart = date_add(v_asOfDate, -(p_eventWindowDays - 1));
    SET v_weekTo = date_add(v_asOfDate, 1 - dayofweek(v_asOfDate));
    SET v_weekFrom = date_add(v_weekTo, -7 * (p_weeksToRebuild - 1));

    -- ------------------------------------------------------------------------
    -- Run header.
    -- Databricks Job/Run/Task metadata remains NULL for now and can be populated
    -- by the notebook/job wrapper later without changing the validation model.
    -- ------------------------------------------------------------------------
    DELETE FROM prdrzranalytics.lab42.sdi_tbl_mip_validation_runDetails_perRun
    WHERE runId = v_runId;

    INSERT INTO prdrzranalytics.lab42.sdi_tbl_mip_validation_runDetails_perRun
    SELECT
        v_runId,
        CASE WHEN p_runId IS NULL THEN 'MAN' ELSE 'MAN' END,
        'MANUAL',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        'prdrzranalytics.lab42.sdi_sp_mip_orchestration_allLayers_daily',
        v_asOfDate,
        v_windowStart,
        v_windowEnd,
        v_weekFrom,
        v_weekTo,
        v_runStartedAt,
        NULL,
        'RUNNING',
        0,
        0,
        NULL,
        NULL,
        NULL,
        NULL,
        v_runStartedAt,
        v_runStartedAt;

    -- ------------------------------------------------------------------------
    -- CONTROL
    -- Normally deployed once. Refresh only when definitions intentionally change.
    -- ------------------------------------------------------------------------
    IF p_refreshControl THEN
        CALL prdrzranalytics.lab42.sdi_sp_mip_control_fiscalCalendar_static();
        CALL prdrzranalytics.lab42.sdi_sp_mip_control_metricCatalog_static();
        CALL prdrzranalytics.lab42.sdi_sp_mip_control_breakoutCatalog_static();
        CALL prdrzranalytics.lab42.sdi_sp_mip_control_crosstabCatalog_static();
        CALL prdrzranalytics.lab42.sdi_sp_mip_control_validationRules_static();
    END IF;

    -- =========================================================================
    -- BRONZE
    -- =========================================================================
    CALL prdrzranalytics.lab42.sdi_sp_mip_validation_preStage_perRun(
        p_stageName       => 'BRONZE',
        p_runId           => v_runId,
        p_asOfDate        => v_asOfDate,
        p_eventWindowDays => p_eventWindowDays,
        p_weeksToCheck    => p_weeksToRebuild
    );

    CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlMarketingCodes_snapshot(
        p_runId => v_runId
    );

    CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessions_daily(
        p_runId           => v_runId,
        p_asOfDate        => v_asOfDate,
        p_eventWindowDays => p_eventWindowDays
    );

    CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlHitSessionLinks_daily(
        p_runId           => v_runId,
        p_asOfDate        => v_asOfDate,
        p_eventWindowDays => p_eventWindowDays
    );

    CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlHits_daily(
        p_runId           => v_runId,
        p_asOfDate        => v_asOfDate,
        p_eventWindowDays => p_eventWindowDays
    );

    CALL prdrzranalytics.lab42.sdi_sp_mip_validation_postStage_perRun(
        p_stageName       => 'BRONZE',
        p_runId           => v_runId,
        p_asOfDate        => v_asOfDate,
        p_eventWindowDays => p_eventWindowDays,
        p_weeksToCheck    => p_weeksToRebuild
    );

    -- =========================================================================
    -- SILVER — HIT
    -- =========================================================================
    CALL prdrzranalytics.lab42.sdi_sp_mip_validation_preStage_perRun(
        p_stageName       => 'SILVER_HIT',
        p_runId           => v_runId,
        p_asOfDate        => v_asOfDate,
        p_eventWindowDays => p_eventWindowDays,
        p_weeksToCheck    => p_weeksToRebuild
    );

    CALL prdrzranalytics.lab42.sdi_sp_mip_silver_detailsPerHit_daily(
        p_runId           => v_runId,
        p_asOfDate        => v_asOfDate,
        p_eventWindowDays => p_eventWindowDays
    );

    CALL prdrzranalytics.lab42.sdi_sp_mip_validation_postStage_perRun(
        p_stageName       => 'SILVER_HIT',
        p_runId           => v_runId,
        p_asOfDate        => v_asOfDate,
        p_eventWindowDays => p_eventWindowDays,
        p_weeksToCheck    => p_weeksToRebuild
    );

    -- =========================================================================
    -- SILVER — SESSION
    -- =========================================================================
    CALL prdrzranalytics.lab42.sdi_sp_mip_validation_preStage_perRun(
        p_stageName       => 'SILVER_SESSION',
        p_runId           => v_runId,
        p_asOfDate        => v_asOfDate,
        p_eventWindowDays => p_eventWindowDays,
        p_weeksToCheck    => p_weeksToRebuild
    );

    CALL prdrzranalytics.lab42.sdi_sp_mip_silver_attributesPerSession_daily(
        p_runId           => v_runId,
        p_asOfDate        => v_asOfDate,
        p_eventWindowDays => p_eventWindowDays
    );

    CALL prdrzranalytics.lab42.sdi_sp_mip_silver_actionsPerSessionPageCategory_daily(
        p_runId           => v_runId,
        p_asOfDate        => v_asOfDate,
        p_eventWindowDays => p_eventWindowDays
    );

    CALL prdrzranalytics.lab42.sdi_sp_mip_validation_postStage_perRun(
        p_stageName       => 'SILVER_SESSION',
        p_runId           => v_runId,
        p_asOfDate        => v_asOfDate,
        p_eventWindowDays => p_eventWindowDays,
        p_weeksToCheck    => p_weeksToRebuild
    );

    -- =========================================================================
    -- SILVER — VISITOR WEEK
    -- =========================================================================
    CALL prdrzranalytics.lab42.sdi_sp_mip_validation_preStage_perRun(
        p_stageName       => 'SILVER_WEEK',
        p_runId           => v_runId,
        p_asOfDate        => v_asOfDate,
        p_eventWindowDays => p_eventWindowDays,
        p_weeksToCheck    => p_weeksToRebuild
    );

    CALL prdrzranalytics.lab42.sdi_sp_mip_silver_attributesPerVisitorWeek_weekly(
        p_runId          => v_runId,
        p_asOfDate       => v_asOfDate,
        p_weeksToRebuild => p_weeksToRebuild
    );

    CALL prdrzranalytics.lab42.sdi_sp_mip_silver_actionsPerVisitorWeek_weekly(
        p_runId          => v_runId,
        p_asOfDate       => v_asOfDate,
        p_weeksToRebuild => p_weeksToRebuild
    );

    CALL prdrzranalytics.lab42.sdi_sp_mip_validation_postStage_perRun(
        p_stageName       => 'SILVER_WEEK',
        p_runId           => v_runId,
        p_asOfDate        => v_asOfDate,
        p_eventWindowDays => p_eventWindowDays,
        p_weeksToCheck    => p_weeksToRebuild
    );

    -- =========================================================================
    -- GOLD — OVERVIEW
    -- =========================================================================
    CALL prdrzranalytics.lab42.sdi_sp_mip_validation_preStage_perRun(
        p_stageName       => 'GOLD_OVERVIEW',
        p_runId           => v_runId,
        p_asOfDate        => v_asOfDate,
        p_eventWindowDays => p_eventWindowDays,
        p_weeksToCheck    => p_weeksToRebuild
    );

    CALL prdrzranalytics.lab42.sdi_sp_mip_gold_overviewMetricIngredientsByWeek_long(
        p_runId          => v_runId,
        p_asOfDate       => v_asOfDate,
        p_weeksToRebuild => p_weeksToRebuild
    );

    CALL prdrzranalytics.lab42.sdi_sp_mip_gold_overviewMetricForecastByWeek_long(
        p_asOfDate       => v_asOfDate,
        p_weeksToRebuild => p_weeksToRebuild
    );

    CALL prdrzranalytics.lab42.sdi_sp_mip_validation_postStage_perRun(
        p_stageName       => 'GOLD_OVERVIEW',
        p_runId           => v_runId,
        p_asOfDate        => v_asOfDate,
        p_eventWindowDays => p_eventWindowDays,
        p_weeksToCheck    => p_weeksToRebuild
    );

    -- =========================================================================
    -- GOLD — BREAKOUT
    -- =========================================================================
    CALL prdrzranalytics.lab42.sdi_sp_mip_validation_preStage_perRun(
        p_stageName       => 'GOLD_BREAKOUT',
        p_runId           => v_runId,
        p_asOfDate        => v_asOfDate,
        p_eventWindowDays => p_eventWindowDays,
        p_weeksToCheck    => p_weeksToRebuild
    );

    CALL prdrzranalytics.lab42.sdi_sp_mip_gold_breakoutMetricIngredientsByWeek_long(
        p_runId          => v_runId,
        p_asOfDate       => v_asOfDate,
        p_weeksToRebuild => p_weeksToRebuild
    );

    CALL prdrzranalytics.lab42.sdi_sp_mip_validation_postStage_perRun(
        p_stageName       => 'GOLD_BREAKOUT',
        p_runId           => v_runId,
        p_asOfDate        => v_asOfDate,
        p_eventWindowDays => p_eventWindowDays,
        p_weeksToCheck    => p_weeksToRebuild
    );

    -- =========================================================================
    -- GOLD — CROSSTAB
    -- =========================================================================
    CALL prdrzranalytics.lab42.sdi_sp_mip_validation_preStage_perRun(
        p_stageName       => 'GOLD_CROSSTAB',
        p_runId           => v_runId,
        p_asOfDate        => v_asOfDate,
        p_eventWindowDays => p_eventWindowDays,
        p_weeksToCheck    => p_weeksToRebuild
    );

    CALL prdrzranalytics.lab42.sdi_sp_mip_gold_crosstabMetricIngredientsByWeek_long(
        p_runId          => v_runId,
        p_asOfDate       => v_asOfDate,
        p_weeksToRebuild => p_weeksToRebuild
    );

    CALL prdrzranalytics.lab42.sdi_sp_mip_validation_postStage_perRun(
        p_stageName       => 'GOLD_CROSSTAB',
        p_runId           => v_runId,
        p_asOfDate        => v_asOfDate,
        p_eventWindowDays => p_eventWindowDays,
        p_weeksToCheck    => p_weeksToRebuild
    );

    -- =========================================================================
    -- GOLD — EXPLORE
    -- =========================================================================
    CALL prdrzranalytics.lab42.sdi_sp_mip_validation_preStage_perRun(
        p_stageName       => 'GOLD_EXPLORE',
        p_runId           => v_runId,
        p_asOfDate        => v_asOfDate,
        p_eventWindowDays => p_eventWindowDays,
        p_weeksToCheck    => p_weeksToRebuild
    );

    CALL prdrzranalytics.lab42.sdi_sp_mip_gold_exploreSessionPageCategoryByWeek_wide(
        p_runId          => v_runId,
        p_asOfDate       => v_asOfDate,
        p_weeksToRebuild => p_weeksToRebuild
    );

    CALL prdrzranalytics.lab42.sdi_sp_mip_validation_postStage_perRun(
        p_stageName       => 'GOLD_EXPLORE',
        p_runId           => v_runId,
        p_asOfDate        => v_asOfDate,
        p_eventWindowDays => p_eventWindowDays,
        p_weeksToCheck    => p_weeksToRebuild
    );

    -- =========================================================================
    -- GOLD APP — ALL 11 APP CONTRACTS
    -- =========================================================================
    CALL prdrzranalytics.lab42.sdi_sp_mip_validation_preStage_perRun(
        p_stageName       => 'GOLD_APP',
        p_runId           => v_runId,
        p_asOfDate        => v_asOfDate,
        p_eventWindowDays => p_eventWindowDays,
        p_weeksToCheck    => p_weeksToRebuild
    );

    CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewCards_wide(
        p_asOfDate => v_asOfDate, p_weeksToRebuild => p_weeksToRebuild, p_validateOnly => FALSE
    );
    CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewTrend_long(
        p_asOfDate => v_asOfDate, p_weeksToRebuild => p_weeksToRebuild, p_validateOnly => FALSE
    );
    CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewToplineMovers_long(
        p_asOfDate => v_asOfDate, p_weeksToRebuild => p_weeksToRebuild, p_validateOnly => FALSE
    );
    CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewConversionFunnel_long(
        p_asOfDate => v_asOfDate, p_weeksToRebuild => p_weeksToRebuild, p_validateOnly => FALSE
    );
    CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appBreakoutsComparisonTable_long(
        p_asOfDate => v_asOfDate, p_weeksToRebuild => p_weeksToRebuild, p_validateOnly => FALSE
    );
    CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appBreakoutsWaterfall_long(
        p_asOfDate => v_asOfDate, p_weeksToRebuild => p_weeksToRebuild, p_validateOnly => FALSE
    );
    CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appBreakoutsAbsoluteTrend_long(
        p_asOfDate => v_asOfDate, p_weeksToRebuild => p_weeksToRebuild, p_validateOnly => FALSE
    );
    CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appCrosstabsMatrix_long(
        p_asOfDate => v_asOfDate, p_weeksToRebuild => p_weeksToRebuild, p_validateOnly => FALSE
    );
    CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appCrosstabsRankedPairs_long(
        p_asOfDate => v_asOfDate, p_weeksToRebuild => p_weeksToRebuild, p_validateOnly => FALSE
    );
    CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appExploreBase_wide(
        p_asOfDate => v_asOfDate, p_weeksToRebuild => p_weeksToRebuild, p_validateOnly => FALSE
    );
    CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appExploreRankedPairs_long(
        p_asOfDate => v_asOfDate, p_weeksToRebuild => p_weeksToRebuild, p_validateOnly => FALSE
    );

    CALL prdrzranalytics.lab42.sdi_sp_mip_validation_postStage_perRun(
        p_stageName       => 'GOLD_APP',
        p_runId           => v_runId,
        p_asOfDate        => v_asOfDate,
        p_eventWindowDays => p_eventWindowDays,
        p_weeksToCheck    => p_weeksToRebuild
    );

    -- ------------------------------------------------------------------------
    -- Successful completion.
    -- Warning count is preserved in the run header but does not fail the run.
    -- ------------------------------------------------------------------------
    SET v_warningCount = (
        SELECT count(*)
        FROM prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun
        WHERE runId=v_runId
          AND checkStatus='WARNING'
    );

    SET v_failureCount = (
        SELECT count(*)
        FROM prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun
        WHERE runId=v_runId
          AND checkStatus IN ('FAILED','ERROR')
    );

    UPDATE prdrzranalytics.lab42.sdi_tbl_mip_validation_runDetails_perRun
    SET
        runFinishedAt = current_timestamp(),
        runStatus = CASE
            WHEN v_failureCount>0 THEN 'FAILED'
            WHEN v_warningCount>0 THEN 'WARNING'
            ELSE 'HEALTHY'
        END,
        warningCount = v_warningCount,
        failureCount = v_failureCount,
        updatedAt = current_timestamp()
    WHERE runId=v_runId;

    SELECT
        v_runId AS runId,
        v_asOfDate AS asOfDate,
        v_windowStart AS eventWindowStart,
        v_windowEnd AS eventWindowEnd,
        v_weekFrom AS weekWindowStart,
        v_weekTo AS weekWindowEnd,
        CASE
            WHEN v_failureCount>0 THEN 'FAILED'
            WHEN v_warningCount>0 THEN 'WARNING'
            ELSE 'HEALTHY'
        END AS runStatus,
        v_warningCount AS warningCount,
        v_failureCount AS failureCount,
        'MIP orchestration completed. Any validation STOP or child procedure error would have propagated before this point.' AS message;
END;

-- Normal manual/dev run: no arguments required.
-- CALL prdrzranalytics.lab42.sdi_sp_mip_orchestration_allLayers_daily();

-- Optional explicit date/backfill scope:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_orchestration_allLayers_daily(
--     p_asOfDate        => DATE '2026-09-28',
--     p_eventWindowDays => 1,
--     p_weeksToRebuild  => 1
-- );
