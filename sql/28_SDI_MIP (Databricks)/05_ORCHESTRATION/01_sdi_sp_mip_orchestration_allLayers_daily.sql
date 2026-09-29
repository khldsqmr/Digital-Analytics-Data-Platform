
-- ###########################################################################
-- BEGIN orchestration/01_sdi_sp_mip_orchestration_allLayers_daily.sql
-- ###########################################################################

-- ============================================================================
-- FILE  : 01_sdi_sp_mip_orchestration_allLayers_daily.sql
-- LAYER : ORCHESTRATION
-- PURPOSE:
--   Thin database orchestration: calls child procedures and validation gates in dependency order.
-- ============================================================================

USE CATALOG prdrzranalytics;
USE SCHEMA lab42;
CREATE OR REPLACE PROCEDURE sdi_sp_mip_orchestration_allLayers_daily(
  p_runId           STRING,
  p_asOfDate        DATE    DEFAULT NULL,
  p_eventWindowDays INT     DEFAULT 1,
  p_weeksToRebuild  INT     DEFAULT 1,
  p_refreshControl  BOOLEAN DEFAULT false,
  p_failOnWarning   BOOLEAN DEFAULT false
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Thin MIP database orchestration. Child SQL/validation failures propagate to the calling Databricks notebook.'
AS
BEGIN

  -- Shared date used by every child procedure.
  DECLARE v_asOfDate DATE DEFAULT coalesce(
    p_asOfDate,
    to_date(from_utc_timestamp(current_timestamp(), 'America/Los_Angeles'))
  );

  -- --------------------------------------------------------------------------
  -- CONTROL
  -- Static Control data should normally be seeded only during deployment.
  -- Set p_refreshControl=true only when definitions intentionally changed.
  -- --------------------------------------------------------------------------
  IF p_refreshControl THEN
    CALL sdi_sp_mip_control_fiscalCalendar_static();
    CALL sdi_sp_mip_control_metricCatalog_static();
    CALL sdi_sp_mip_control_breakoutCatalog_static();
    CALL sdi_sp_mip_control_crosstabCatalog_static();
    CALL sdi_sp_mip_control_validationRules_static();
  END IF;

  -- ==========================================================================
  -- BRONZE
  --
  -- Small/reference sources first; the very large UDI hit load is deliberately
  -- last. The notebook performs cheap source preflight checks before this SP.
  -- ==========================================================================

  CALL sdi_sp_mip_bronze_edlMarketingCodes_snapshot(
    p_runId => p_runId
  );

  CALL sdi_sp_mip_bronze_edlSessions_daily(
    p_runId           => p_runId,
    p_asOfDate        => v_asOfDate,
    p_eventWindowDays => p_eventWindowDays
  );

  CALL sdi_sp_mip_bronze_edlHitSessionLinks_daily(
    p_runId           => p_runId,
    p_asOfDate        => v_asOfDate,
    p_eventWindowDays => p_eventWindowDays
  );

  CALL sdi_sp_mip_bronze_edlHits_daily(
    p_runId           => p_runId,
    p_asOfDate        => v_asOfDate,
    p_eventWindowDays => p_eventWindowDays
  );

  -- Stop before Silver if Bronze is unhealthy.
  CALL sdi_sp_mip_validation_runChecks_perRun(
    p_runId           => p_runId,
    p_stageName       => 'bronze',
    p_asOfDate        => v_asOfDate,
    p_eventWindowDays => p_eventWindowDays,
    p_weeksToCheck    => p_weeksToRebuild,
    p_failOnWarning   => p_failOnWarning
  );

  -- ==========================================================================
  -- SILVER — HIT
  -- ==========================================================================

  CALL sdi_sp_mip_silver_detailsPerHit_daily(
    p_runId           => p_runId,
    p_asOfDate        => v_asOfDate,
    p_eventWindowDays => p_eventWindowDays
  );

  -- Stop before the session aggregation if the canonical hit layer is wrong.
  CALL sdi_sp_mip_validation_runChecks_perRun(
    p_runId           => p_runId,
    p_stageName       => 'silverHit',
    p_asOfDate        => v_asOfDate,
    p_eventWindowDays => p_eventWindowDays,
    p_weeksToCheck    => p_weeksToRebuild,
    p_failOnWarning   => p_failOnWarning
  );

  -- ==========================================================================
  -- SILVER — SESSION
  -- ==========================================================================

  CALL sdi_sp_mip_silver_attributesPerSession_daily(
    p_runId           => p_runId,
    p_asOfDate        => v_asOfDate,
    p_eventWindowDays => p_eventWindowDays
  );

  CALL sdi_sp_mip_silver_actionsPerSessionPageCategory_daily(
    p_runId           => p_runId,
    p_asOfDate        => v_asOfDate,
    p_eventWindowDays => p_eventWindowDays
  );

  -- Stop before visitor-week aggregation if the manager-aligned session
  -- Silvers do not have the expected grain / lifecycle rules.
  CALL sdi_sp_mip_validation_runChecks_perRun(
    p_runId           => p_runId,
    p_stageName       => 'silverSession',
    p_asOfDate        => v_asOfDate,
    p_eventWindowDays => p_eventWindowDays,
    p_weeksToCheck    => p_weeksToRebuild,
    p_failOnWarning   => p_failOnWarning
  );

  -- ==========================================================================
  -- SILVER — VISITOR WEEK
  -- ==========================================================================

  CALL sdi_sp_mip_silver_attributesPerVisitorWeek_weekly(
    p_runId          => p_runId,
    p_asOfDate       => v_asOfDate,
    p_weeksToRebuild => p_weeksToRebuild
  );

  CALL sdi_sp_mip_silver_actionsPerVisitorWeek_weekly(
    p_runId          => p_runId,
    p_asOfDate       => v_asOfDate,
    p_weeksToRebuild => p_weeksToRebuild
  );

  -- This is the last validation gate before Gold starts.
  CALL sdi_sp_mip_validation_runChecks_perRun(
    p_runId           => p_runId,
    p_stageName       => 'silverWeek',
    p_asOfDate        => v_asOfDate,
    p_eventWindowDays => p_eventWindowDays,
    p_weeksToCheck    => p_weeksToRebuild,
    p_failOnWarning   => p_failOnWarning
  );

  -- ==========================================================================
  -- GOLD — OVERVIEW
  -- Build and validate the cheapest/base Gold first.
  -- ==========================================================================

  CALL sdi_sp_mip_gold_overviewMetricIngredientsByWeek_long(
    p_runId          => p_runId,
    p_asOfDate       => v_asOfDate,
    p_weeksToRebuild => p_weeksToRebuild
  );

  CALL sdi_sp_mip_validation_runChecks_perRun(
    p_runId           => p_runId,
    p_stageName       => 'goldOverview',
    p_asOfDate        => v_asOfDate,
    p_eventWindowDays => p_eventWindowDays,
    p_weeksToCheck    => p_weeksToRebuild,
    p_failOnWarning   => p_failOnWarning
  );

  -- ==========================================================================
  -- GOLD — BREAKOUTS
  -- Do not build Crosstabs/Explore if this layer fails.
  -- ==========================================================================

  CALL sdi_sp_mip_gold_breakoutMetricIngredientsByWeek_long(
    p_runId          => p_runId,
    p_asOfDate       => v_asOfDate,
    p_weeksToRebuild => p_weeksToRebuild
  );

  CALL sdi_sp_mip_validation_runChecks_perRun(
    p_runId           => p_runId,
    p_stageName       => 'goldBreakout',
    p_asOfDate        => v_asOfDate,
    p_eventWindowDays => p_eventWindowDays,
    p_weeksToCheck    => p_weeksToRebuild,
    p_failOnWarning   => p_failOnWarning
  );

  -- ==========================================================================
  -- GOLD — CROSSTABS
  -- ==========================================================================

  CALL sdi_sp_mip_gold_crosstabMetricIngredientsByWeek_long(
    p_runId          => p_runId,
    p_asOfDate       => v_asOfDate,
    p_weeksToRebuild => p_weeksToRebuild
  );

  CALL sdi_sp_mip_validation_runChecks_perRun(
    p_runId           => p_runId,
    p_stageName       => 'goldCrosstab',
    p_asOfDate        => v_asOfDate,
    p_eventWindowDays => p_eventWindowDays,
    p_weeksToCheck    => p_weeksToRebuild,
    p_failOnWarning   => p_failOnWarning
  );

  -- ==========================================================================
  -- GOLD — EXPLORE
  -- Most flexible serving base; intentionally last.
  -- ==========================================================================

  CALL sdi_sp_mip_gold_exploreSessionPageCategory_weekly(
    p_runId          => p_runId,
    p_asOfDate       => v_asOfDate,
    p_weeksToRebuild => p_weeksToRebuild
  );

  CALL sdi_sp_mip_validation_runChecks_perRun(
    p_runId           => p_runId,
    p_stageName       => 'goldExplore',
    p_asOfDate        => v_asOfDate,
    p_eventWindowDays => p_eventWindowDays,
    p_weeksToCheck    => p_weeksToRebuild,
    p_failOnWarning   => p_failOnWarning
  );

END;


-- ###########################################################################
-- END orchestration/01_sdi_sp_mip_orchestration_allLayers_daily.sql
-- ###########################################################################
