

-- ###########################################################################
-- BEGIN validation/01_sdi_sp_mip_validation_runChecks_perRun.sql
-- ###########################################################################

-- ============================================================================
-- FILE  : 01_sdi_sp_mip_validation_runChecks_perRun.sql
-- LAYER : VALIDATION
-- PURPOSE:
--   Run/job lineage tables, machine-readable checks, actionable issues, and stage-gated validation.
-- ============================================================================

USE CATALOG prdrzranalytics;
USE SCHEMA lab42;
-- ----------------------------------------------------------------------------
-- One row per end-to-end pipeline run
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sdi_tbl_mip_validation_runDetails_perRun (
  runId                    STRING COMMENT 'e.g. MAN_20260929_051500_A1B2C3',
  executionType            STRING COMMENT 'MAN | JOB | BCK | RPR | RTY | TST',
  triggerType              STRING COMMENT 'MANUAL | SCHEDULED | API | UPSTREAM | RETRY | BACKFILL',

  databricksJobId          STRING,
  databricksJobRunId       STRING,
  databricksTaskRunId      STRING,
  databricksJobName        STRING,
  notebookPath             STRING,

  orchestrationProcedure   STRING,

  asOfDate                 DATE,
  eventWindowStart         DATE,
  eventWindowEnd           DATE,
  weekWindowStart          DATE,
  weekWindowEnd            DATE,

  runStartedAt             TIMESTAMP,
  runFinishedAt            TIMESTAMP,
  runStatus                STRING COMMENT 'RUNNING | HEALTHY | WARNING | FAILED | ERROR',

  warningCount             BIGINT,
  failureCount             BIGINT,

  errorSqlState            STRING,
  errorCondition           STRING,
  errorLine                BIGINT,
  errorMessage             STRING,

  createdAt                TIMESTAMP,
  updatedAt                TIMESTAMP
)
USING DELTA
CLUSTER BY (runStartedAt)
COMMENT 'Validation: one row per MIP execution, with actual Databricks lineage when available.';

-- ----------------------------------------------------------------------------
-- One row per procedure/object execution inside a run
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sdi_tbl_mip_validation_objectRuns_perRun (
  objectRunId              STRING,
  runId                    STRING,

  layerName                STRING,
  procedureName            STRING,
  targetObject             STRING,

  databricksTaskRunId      STRING COMMENT 'Optional task-level lineage if each layer/task is split later',

  scopeType                STRING COMMENT 'eventDate | sessionStartDatePst | weekStartDate | snapshot',
  scopeStart               STRING,
  scopeEnd                 STRING,

  objectRunStartedAt       TIMESTAMP,
  objectRunFinishedAt      TIMESTAMP,
  objectRunStatus          STRING COMMENT 'RUNNING | SUCCEEDED | FAILED | ERROR',

  rowsInScope              BIGINT,
  sourceWatermark          STRING,

  errorSqlState            STRING,
  errorCondition           STRING,
  errorLine                BIGINT,
  errorMessage             STRING,

  notes                    STRING
)
USING DELTA
CLUSTER BY (objectRunStartedAt)
COMMENT 'Validation: which MIP procedure wrote which object, for which scope, in each pipeline run.';

-- ----------------------------------------------------------------------------
-- Evaluated validation checks
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sdi_tbl_mip_validation_checkResults_perRun (
  runId                    STRING,
  stageName                STRING,
  checkedAt                TIMESTAMP,

  layerAffected            STRING,
  objectAffected           STRING,
  scopePeriod              STRING,

  checkName                STRING,
  checkType                STRING COMMENT 'FRESHNESS | UNIQUENESS | COVERAGE | RECONCILIATION | DATA_QUALITY | VOLUME',

  expectedValue            DOUBLE,
  actualValue              DOUBLE,
  varianceValue            DOUBLE,
  variancePct              DOUBLE,

  checkStatus              STRING COMMENT 'HEALTHY | WARNING | FAILED | INFO',
  severity                 STRING COMMENT 'INFO | LOW | MEDIUM | HIGH | CRITICAL',
  isCritical               BOOLEAN,

  issueShortDescription    STRING,
  nextSteps                STRING,
  ownerTeam                STRING
)
USING DELTA
CLUSTER BY (checkedAt)
COMMENT 'Validation: machine-readable results of MIP validation checks.';

-- ----------------------------------------------------------------------------
-- Human/actionable issues
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sdi_tbl_mip_validation_issueDetails_perRun (
  issueId                  STRING,
  runId                    STRING,
  stageName                STRING,
  detectedAt               TIMESTAMP,

  issueStatus              STRING COMMENT 'OPEN | RESOLVED',
  severity                 STRING,
  issueType                STRING,

  layerAffected            STRING,
  objectAffected           STRING,
  scopePeriod              STRING,
  checkName                STRING,

  issueShortDescription    STRING,
  issueDetailedDescription STRING,

  actualValue              DOUBLE,
  expectedValue            DOUBLE,
  varianceValue            DOUBLE,
  variancePct              DOUBLE,

  errorSqlState            STRING,
  errorCondition           STRING,
  errorLine                BIGINT,
  errorMessage             STRING,

  likelyCause              STRING,
  nextSteps                STRING,
  ownerTeam                STRING,

  firstSeenAt              TIMESTAMP,
  lastSeenAt               TIMESTAMP,
  occurrenceCount          BIGINT,

  resolvedAt               TIMESTAMP,
  resolutionNotes          STRING
)
USING DELTA
CLUSTER BY (detectedAt)
COMMENT 'Validation: actionable MIP issues with affected layer/object, description, next steps and owner.';

-- ----------------------------------------------------------------------------
-- Stage validation procedure
-- Critical FAILED checks always SIGNAL and stop the caller.
-- WARNING checks stop only when p_failOnWarning = true.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE sdi_sp_mip_validation_runChecks_perRun(
  p_runId             STRING,
  p_stageName         STRING COMMENT 'bronze | silverHit | silverSession | silverWeek | goldOverview | goldBreakout | goldCrosstab | goldExplore | gold',
  p_asOfDate          DATE DEFAULT NULL,
  p_eventWindowDays   INT DEFAULT 1,
  p_weeksToCheck      INT DEFAULT 1,
  p_failOnWarning     BOOLEAN DEFAULT false
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  DECLARE v_checkedAt    TIMESTAMP DEFAULT current_timestamp();
  DECLARE v_asOfDate     DATE DEFAULT coalesce(
    p_asOfDate,
    to_date(from_utc_timestamp(current_timestamp(), 'America/Los_Angeles'))
  );
  DECLARE v_windowEnd    DATE DEFAULT v_asOfDate;
  DECLARE v_windowStart  DATE DEFAULT date_add(v_asOfDate, -(p_eventWindowDays - 1));
  DECLARE v_weekTo       DATE DEFAULT date_add(v_asOfDate, 1 - dayofweek(v_asOfDate));
  DECLARE v_weekFrom     DATE DEFAULT date_add(v_weekTo, -7 * (p_weeksToCheck - 1));
  DECLARE v_stopCount    BIGINT DEFAULT 0;
  DECLARE v_warningCount BIGINT DEFAULT 0;

  -- Make the procedure idempotent for one run/stage.
  DELETE FROM sdi_tbl_mip_validation_issueDetails_perRun
  WHERE runId = p_runId
    AND stageName = p_stageName
    AND checkName IS NOT NULL;

  DELETE FROM sdi_tbl_mip_validation_checkResults_perRun
  WHERE runId = p_runId
    AND stageName = p_stageName;

  -- --------------------------------------------------------------------------
  -- BRONZE
  -- --------------------------------------------------------------------------
  IF lower(p_stageName) = 'bronze' THEN

    -- UDI source/target must contain the requested event-date window.
    INSERT INTO sdi_tbl_mip_validation_checkResults_perRun
    SELECT
      p_runId,
      p_stageName,
      v_checkedAt,
      'BRONZE',
      'sdi_tbl_mip_bronze_edlHits_daily',
      concat(cast(v_windowStart AS STRING), ' to ', cast(v_windowEnd AS STRING)),
      'bronzeHitsNonEmpty',
      'VOLUME',
      1D,
      cast(count(*) AS DOUBLE),
      cast(count(*) - 1 AS DOUBLE),
      NULL,
      CASE WHEN count(*) > 0 THEN 'HEALTHY' ELSE 'FAILED' END,
      CASE WHEN count(*) > 0 THEN 'INFO' ELSE 'CRITICAL' END,
      true,
      CASE WHEN count(*) > 0 THEN 'Bronze UDI window contains rows' ELSE 'Bronze UDI window is empty' END,
      CASE WHEN count(*) > 0 THEN NULL ELSE 'Check UDI availability and requested event-date window.' END,
      'MIP Data Engineering'
    FROM sdi_tbl_mip_bronze_edlHits_daily
    WHERE event_date BETWEEN v_windowStart AND v_windowEnd;

    -- Duplicate hit-session keys are visible even though detailsPerHit defensively deduplicates them.
    INSERT INTO sdi_tbl_mip_validation_checkResults_perRun
    WITH d AS (
      SELECT coalesce(sum(cnt - 1), 0) AS duplicateRows
      FROM (
        SELECT
          row_identity_hash,
          event_date,
          source_table,
          count(*) AS cnt
        FROM sdi_tbl_mip_bronze_edlHitSessionLinks_daily
        WHERE event_date BETWEEN v_windowStart AND v_windowEnd
        GROUP BY row_identity_hash, event_date, source_table
        HAVING count(*) > 1
      )
    )
    SELECT
      p_runId,
      p_stageName,
      v_checkedAt,
      'BRONZE',
      'sdi_tbl_mip_bronze_edlHitSessionLinks_daily',
      concat(cast(v_windowStart AS STRING), ' to ', cast(v_windowEnd AS STRING)),
      'duplicateHitSessionLinkKeys',
      'UNIQUENESS',
      0D,
      cast(duplicateRows AS DOUBLE),
      cast(duplicateRows AS DOUBLE),
      NULL,
      CASE WHEN duplicateRows = 0 THEN 'HEALTHY' ELSE 'WARNING' END,
      CASE WHEN duplicateRows = 0 THEN 'INFO' ELSE 'MEDIUM' END,
      false,
      CASE WHEN duplicateRows = 0
           THEN 'SEF hit-session keys are unique'
           ELSE 'Duplicate SEF hit-session join keys detected' END,
      CASE WHEN duplicateRows = 0
           THEN NULL
           ELSE 'Review duplicated row_identity_hash + event_date + source_table assignments upstream.' END,
      'EDL Engineering'
    FROM d;
  END IF;

  -- --------------------------------------------------------------------------
  -- SILVER HIT
  -- --------------------------------------------------------------------------
  IF lower(p_stageName) = 'silverhit' THEN
    INSERT INTO sdi_tbl_mip_validation_checkResults_perRun
    WITH counts AS (
      SELECT
        (SELECT count(*)
         FROM sdi_tbl_mip_bronze_edlHits_daily
         WHERE event_date BETWEEN v_windowStart AND v_windowEnd) AS bronzeRows,

        (SELECT count(*)
         FROM sdi_tbl_mip_silver_detailsPerHit_daily
         WHERE eventDate BETWEEN v_windowStart AND v_windowEnd) AS detailRows,

        (SELECT count(*)
         FROM sdi_tbl_mip_silver_detailsPerHit_daily
         WHERE eventDate BETWEEN v_windowStart AND v_windowEnd
           AND isSessionized = 1) AS sessionizedRows,

        (SELECT count(*)
         FROM sdi_tbl_mip_silver_detailsPerHit_daily
         WHERE eventDate BETWEEN v_windowStart AND v_windowEnd
           AND isSessionized = 0
           AND isOrder = 1) AS unmatchedPurchases
    ),
    rawChecks AS (
      SELECT
        'detailsVsBronzeRowCountPctDiff' AS checkName,
        'RECONCILIATION' AS checkType,
        0D AS expectedValue,
        CASE WHEN bronzeRows = 0 THEN 1D
             ELSE abs(detailRows - bronzeRows) / cast(bronzeRows AS DOUBLE) END AS actualValue,
        'detailsPerHit row count differs from Bronze hits' AS description
      FROM counts

      UNION ALL

      SELECT
        'sessionizationCoveragePct',
        'COVERAGE',
        1D,
        CASE WHEN detailRows = 0 THEN 0D
             ELSE sessionizedRows / cast(detailRows AS DOUBLE) END,
        'EDL sessionization coverage is outside expected range'
      FROM counts

      UNION ALL

      SELECT
        'unmatchedPurchaseEvents',
        'DATA_QUALITY',
        0D,
        cast(unmatchedPurchases AS DOUBLE),
        'Purchase events exist without an EDL session assignment'
      FROM counts
    )
    SELECT
      p_runId,
      p_stageName,
      v_checkedAt,
      'SILVER',
      'sdi_tbl_mip_silver_detailsPerHit_daily',
      concat(cast(v_windowStart AS STRING), ' to ', cast(v_windowEnd AS STRING)),
      r.checkName,
      r.checkType,
      r.expectedValue,
      r.actualValue,
      r.actualValue - r.expectedValue,
      CASE WHEN r.expectedValue <> 0D
           THEN (r.actualValue - r.expectedValue) / abs(r.expectedValue)
           ELSE NULL END,
      CASE
        WHEN cfg.thresholdDirection = 'MIN_REQUIRED' AND r.actualValue < cfg.failureThreshold THEN 'FAILED'
        WHEN cfg.thresholdDirection = 'MIN_REQUIRED' AND r.actualValue < cfg.warningThreshold THEN 'WARNING'
        WHEN cfg.thresholdDirection = 'MAX_ALLOWED' AND r.actualValue >= cfg.failureThreshold THEN 'FAILED'
        WHEN cfg.thresholdDirection = 'MAX_ALLOWED' AND r.actualValue >= cfg.warningThreshold THEN 'WARNING'
        ELSE 'HEALTHY'
      END,
      CASE
        WHEN cfg.thresholdDirection = 'MIN_REQUIRED' AND r.actualValue < cfg.failureThreshold THEN 'HIGH'
        WHEN cfg.thresholdDirection = 'MAX_ALLOWED' AND r.actualValue >= cfg.failureThreshold THEN 'HIGH'
        WHEN cfg.thresholdDirection = 'MIN_REQUIRED' AND r.actualValue < cfg.warningThreshold THEN 'MEDIUM'
        WHEN cfg.thresholdDirection = 'MAX_ALLOWED' AND r.actualValue >= cfg.warningThreshold THEN 'MEDIUM'
        ELSE 'INFO'
      END,
      cfg.isCritical,
      r.description,
      CASE
        WHEN (
          (cfg.thresholdDirection = 'MIN_REQUIRED' AND r.actualValue < cfg.failureThreshold)
          OR
          (cfg.thresholdDirection = 'MAX_ALLOWED' AND r.actualValue >= cfg.failureThreshold)
        ) THEN cfg.failureNextSteps
        ELSE cfg.warningNextSteps
      END,
      cfg.ownerTeam
    FROM rawChecks r
    JOIN sdi_tbl_mip_control_validationRules_static cfg
      ON cfg.checkName = r.checkName
     AND cfg.isActive;
  END IF;

  -- --------------------------------------------------------------------------
  -- SILVER SESSION
  -- --------------------------------------------------------------------------
  IF lower(p_stageName) = 'silversession' THEN

    INSERT INTO sdi_tbl_mip_validation_checkResults_perRun
    WITH stats AS (
      SELECT
        sum(CASE WHEN isNonBounced <> 1
                   OR pageViews <= 1
                   OR sessionStatus NOT IN ('OPEN', 'CLOSED')
                 THEN 1 ELSE 0 END) AS invalidRows
      FROM sdi_tbl_mip_silver_attributesPerSession_daily
      WHERE sessionStartDatePst BETWEEN v_windowStart AND v_windowEnd
    ),
    cfg AS (
      SELECT *
      FROM sdi_tbl_mip_control_validationRules_static
      WHERE checkName = 'invalidSessionRows' AND isActive
    )
    SELECT
      p_runId, p_stageName, v_checkedAt,
      'SILVER',
      'sdi_tbl_mip_silver_attributesPerSession_daily',
      concat(cast(v_windowStart AS STRING), ' to ', cast(v_windowEnd AS STRING)),
      cfg.checkName,
      'DATA_QUALITY',
      0D,
      cast(coalesce(s.invalidRows, 0) AS DOUBLE),
      cast(coalesce(s.invalidRows, 0) AS DOUBLE),
      NULL,
      CASE WHEN coalesce(s.invalidRows, 0) >= cfg.failureThreshold THEN 'FAILED'
           WHEN coalesce(s.invalidRows, 0) >= cfg.warningThreshold THEN 'WARNING'
           ELSE 'HEALTHY' END,
      CASE WHEN coalesce(s.invalidRows, 0) >= cfg.failureThreshold THEN 'CRITICAL'
           WHEN coalesce(s.invalidRows, 0) >= cfg.warningThreshold THEN 'HIGH'
           ELSE 'INFO' END,
      cfg.isCritical,
      'Session Silver contains invalid non-bounce/status rows',
      CASE WHEN coalesce(s.invalidRows, 0) >= cfg.failureThreshold
           THEN cfg.failureNextSteps ELSE cfg.warningNextSteps END,
      cfg.ownerTeam
    FROM stats s
    CROSS JOIN cfg;

    INSERT INTO sdi_tbl_mip_validation_checkResults_perRun
    WITH d AS (
      SELECT coalesce(sum(cnt - 1), 0) AS duplicateRows
      FROM (
        SELECT sessionId, count(*) AS cnt
        FROM sdi_tbl_mip_silver_attributesPerSession_daily
        WHERE sessionStartDatePst BETWEEN v_windowStart AND v_windowEnd
        GROUP BY sessionId
        HAVING count(*) > 1
      )
    ),
    cfg AS (
      SELECT *
      FROM sdi_tbl_mip_control_validationRules_static
      WHERE checkName = 'duplicateSessionKeys' AND isActive
    )
    SELECT
      p_runId, p_stageName, v_checkedAt,
      'SILVER',
      'sdi_tbl_mip_silver_attributesPerSession_daily',
      concat(cast(v_windowStart AS STRING), ' to ', cast(v_windowEnd AS STRING)),
      cfg.checkName,
      'UNIQUENESS',
      0D,
      cast(d.duplicateRows AS DOUBLE),
      cast(d.duplicateRows AS DOUBLE),
      NULL,
      CASE WHEN d.duplicateRows >= cfg.failureThreshold THEN 'FAILED'
           WHEN d.duplicateRows >= cfg.warningThreshold THEN 'WARNING'
           ELSE 'HEALTHY' END,
      CASE WHEN d.duplicateRows >= cfg.failureThreshold THEN 'CRITICAL'
           WHEN d.duplicateRows >= cfg.warningThreshold THEN 'HIGH'
           ELSE 'INFO' END,
      cfg.isCritical,
      'Duplicate session IDs detected in session attributes',
      CASE WHEN d.duplicateRows >= cfg.failureThreshold
           THEN cfg.failureNextSteps ELSE cfg.warningNextSteps END,
      cfg.ownerTeam
    FROM d
    CROSS JOIN cfg;

    INSERT INTO sdi_tbl_mip_validation_checkResults_perRun
    WITH d AS (
      SELECT coalesce(sum(cnt - 1), 0) AS duplicateRows
      FROM (
        SELECT sessionId, pageCategory, count(*) AS cnt
        FROM sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily
        WHERE sessionStartDatePst BETWEEN v_windowStart AND v_windowEnd
        GROUP BY sessionId, pageCategory
        HAVING count(*) > 1
      )
    ),
    cfg AS (
      SELECT *
      FROM sdi_tbl_mip_control_validationRules_static
      WHERE checkName = 'duplicateSessionPageCategoryKeys' AND isActive
    )
    SELECT
      p_runId, p_stageName, v_checkedAt,
      'SILVER',
      'sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily',
      concat(cast(v_windowStart AS STRING), ' to ', cast(v_windowEnd AS STRING)),
      cfg.checkName,
      'UNIQUENESS',
      0D,
      cast(d.duplicateRows AS DOUBLE),
      cast(d.duplicateRows AS DOUBLE),
      NULL,
      CASE WHEN d.duplicateRows >= cfg.failureThreshold THEN 'FAILED'
           WHEN d.duplicateRows >= cfg.warningThreshold THEN 'WARNING'
           ELSE 'HEALTHY' END,
      CASE WHEN d.duplicateRows >= cfg.failureThreshold THEN 'CRITICAL'
           WHEN d.duplicateRows >= cfg.warningThreshold THEN 'HIGH'
           ELSE 'INFO' END,
      cfg.isCritical,
      'Duplicate session × page-category rows detected',
      CASE WHEN d.duplicateRows >= cfg.failureThreshold
           THEN cfg.failureNextSteps ELSE cfg.warningNextSteps END,
      cfg.ownerTeam
    FROM d
    CROSS JOIN cfg;
  END IF;

  -- --------------------------------------------------------------------------
  -- SILVER WEEK
  -- --------------------------------------------------------------------------
  IF lower(p_stageName) = 'silverweek' THEN

    INSERT INTO sdi_tbl_mip_validation_checkResults_perRun
    WITH k AS (
      SELECT
        (
          SELECT count(*)
          FROM (
            SELECT weekStartDate, visitorId
            FROM sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly
            WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
            EXCEPT
            SELECT weekStartDate, visitorId
            FROM sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly
            WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
          )
        )
        +
        (
          SELECT count(*)
          FROM (
            SELECT weekStartDate, visitorId
            FROM sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly
            WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
            EXCEPT
            SELECT weekStartDate, visitorId
            FROM sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly
            WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
          )
        ) AS mismatchedKeys
    ),
    cfg AS (
      SELECT *
      FROM sdi_tbl_mip_control_validationRules_static
      WHERE checkName = 'visitorWeekPairMismatch' AND isActive
    )
    SELECT
      p_runId, p_stageName, v_checkedAt,
      'SILVER',
      'attributesPerVisitorWeek + actionsPerVisitorWeek',
      concat(cast(v_weekFrom AS STRING), ' to ', cast(v_weekTo AS STRING)),
      cfg.checkName,
      'RECONCILIATION',
      0D,
      cast(k.mismatchedKeys AS DOUBLE),
      cast(k.mismatchedKeys AS DOUBLE),
      NULL,
      CASE WHEN k.mismatchedKeys >= cfg.failureThreshold THEN 'FAILED'
           WHEN k.mismatchedKeys >= cfg.warningThreshold THEN 'WARNING'
           ELSE 'HEALTHY' END,
      CASE WHEN k.mismatchedKeys >= cfg.failureThreshold THEN 'CRITICAL'
           WHEN k.mismatchedKeys >= cfg.warningThreshold THEN 'HIGH'
           ELSE 'INFO' END,
      cfg.isCritical,
      'Visitor-week attribute/action keys are not 1:1',
      CASE WHEN k.mismatchedKeys >= cfg.failureThreshold
           THEN cfg.failureNextSteps ELSE cfg.warningNextSteps END,
      cfg.ownerTeam
    FROM k
    CROSS JOIN cfg;

    INSERT INTO sdi_tbl_mip_validation_checkResults_perRun
    WITH s AS (
      SELECT
        sum(CASE
              WHEN orders <> ordersAcquisition + ordersBase
                OR orders <> ordersUnassisted + ordersAssisted
              THEN 1 ELSE 0
            END) AS violations
      FROM sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly
      WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
    ),
    cfg AS (
      SELECT *
      FROM sdi_tbl_mip_control_validationRules_static
      WHERE checkName = 'visitorWeekOrderSplitViolations' AND isActive
    )
    SELECT
      p_runId, p_stageName, v_checkedAt,
      'SILVER',
      'sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly',
      concat(cast(v_weekFrom AS STRING), ' to ', cast(v_weekTo AS STRING)),
      cfg.checkName,
      'RECONCILIATION',
      0D,
      cast(coalesce(s.violations, 0) AS DOUBLE),
      cast(coalesce(s.violations, 0) AS DOUBLE),
      NULL,
      CASE WHEN coalesce(s.violations, 0) >= cfg.failureThreshold THEN 'FAILED'
           WHEN coalesce(s.violations, 0) >= cfg.warningThreshold THEN 'WARNING'
           ELSE 'HEALTHY' END,
      CASE WHEN coalesce(s.violations, 0) >= cfg.failureThreshold THEN 'CRITICAL'
           WHEN coalesce(s.violations, 0) >= cfg.warningThreshold THEN 'HIGH'
           ELSE 'INFO' END,
      cfg.isCritical,
      'Visitor-week order splits do not reconcile',
      CASE WHEN coalesce(s.violations, 0) >= cfg.failureThreshold
           THEN cfg.failureNextSteps ELSE cfg.warningNextSteps END,
      cfg.ownerTeam
    FROM s
    CROSS JOIN cfg;
  END IF;

  -- --------------------------------------------------------------------------
  -- GOLD OVERVIEW
  -- Validate immediately after Overview Gold so we do not spend money building
  -- Breakout / Crosstab / Explore Gold when the base weekly ingredients are wrong.
  -- --------------------------------------------------------------------------
  IF lower(p_stageName) IN ('goldoverview', 'gold') THEN

    INSERT INTO sdi_tbl_mip_validation_checkResults_perRun
    WITH s AS (
      SELECT count(*) AS rowCount
      FROM sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
      WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        AND filterLob = 'All'
        AND filterPlatform = 'All'
    )
    SELECT
      p_runId, p_stageName, v_checkedAt,
      'GOLD',
      'sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long',
      concat(cast(v_weekFrom AS STRING), ' to ', cast(v_weekTo AS STRING)),
      'goldOverviewRowsNonEmpty',
      'VOLUME',
      1D,
      cast(rowCount AS DOUBLE),
      cast(rowCount - 1 AS DOUBLE),
      NULL,
      CASE WHEN rowCount > 0 THEN 'HEALTHY' ELSE 'FAILED' END,
      CASE WHEN rowCount > 0 THEN 'INFO' ELSE 'CRITICAL' END,
      true,
      CASE WHEN rowCount > 0
           THEN 'Overview Gold contains rows'
           ELSE 'Overview Gold is empty for the requested week window' END,
      CASE WHEN rowCount > 0
           THEN NULL
           ELSE 'Stop downstream Gold processing and inspect weekly Silver inputs / Overview Gold procedure.' END,
      'MIP Data Engineering'
    FROM s;

    -- Overview count ingredients vs weekly Silver
    INSERT INTO sdi_tbl_mip_validation_checkResults_perRun
    WITH silverWide AS (
      SELECT
        weekStartDate,
        sum(nbv) AS nbv,
        sum(sessionCount) AS sessionCount,
        sum(pageViews) AS pageViews,
        sum(nbvBuyFlow) AS nbvBuyFlow,
        sum(nbvConfigure) AS nbvConfigure,
        sum(nbvCheckoutStart) AS nbvCheckoutStart,
        sum(orders) AS orders,
        sum(ordersAcquisition) AS ordersAcquisition,
        sum(ordersBase) AS ordersBase,
        sum(ordersUnassisted) AS ordersUnassisted,
        sum(ordersAssisted) AS ordersAssisted,
        sum(vrCalls) AS vrCalls,
        sum(vrChats) AS vrChats,
        sum(storeLocator) AS storeLocator,
        sum(orderCount) AS orderCount
      FROM sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly
      WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
      GROUP BY weekStartDate
    ),
    silverLong AS (
      SELECT weekStartDate, metricName, metricValue
      FROM silverWide
      LATERAL VIEW stack(
        15,
        'nbv',               cast(nbv AS DOUBLE),
        'sessionCount',      cast(sessionCount AS DOUBLE),
        'pageViews',         cast(pageViews AS DOUBLE),
        'nbvBuyFlow',        cast(nbvBuyFlow AS DOUBLE),
        'nbvConfigure',      cast(nbvConfigure AS DOUBLE),
        'nbvCheckoutStart',  cast(nbvCheckoutStart AS DOUBLE),
        'orders',            cast(orders AS DOUBLE),
        'ordersAcquisition', cast(ordersAcquisition AS DOUBLE),
        'ordersBase',        cast(ordersBase AS DOUBLE),
        'ordersUnassisted',  cast(ordersUnassisted AS DOUBLE),
        'ordersAssisted',    cast(ordersAssisted AS DOUBLE),
        'vrCalls',           cast(vrCalls AS DOUBLE),
        'vrChats',           cast(vrChats AS DOUBLE),
        'storeLocator',      cast(storeLocator AS DOUBLE),
        'orderCount',        cast(orderCount AS DOUBLE)
      ) s AS metricName, metricValue
    ),
    recon AS (
      SELECT
        s.weekStartDate,
        s.metricName,
        s.metricValue AS expectedValue,
        g.thisWeekNumerator AS actualValue,
        CASE
          WHEN g.metricName IS NULL THEN 1D
          WHEN s.metricValue = 0D THEN abs(coalesce(g.thisWeekNumerator, 0D))
          ELSE abs(coalesce(g.thisWeekNumerator, 0D) - s.metricValue) / abs(s.metricValue)
        END AS pctDiff
      FROM silverLong s
      LEFT JOIN sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long g
        ON  g.targetWeekStartDate = s.weekStartDate
        AND g.filterLob = 'All'
        AND g.filterPlatform = 'All'
        AND g.metricName = s.metricName
        AND g.metricKind = 'count'
    ),
    cfg AS (
      SELECT *
      FROM sdi_tbl_mip_control_validationRules_static
      WHERE checkName = 'goldOverviewReconciliationPctDiff'
        AND isActive
    )
    SELECT
      p_runId, p_stageName, v_checkedAt,
      'GOLD',
      'sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long',
      concat(cast(r.weekStartDate AS STRING), ' | ', r.metricName),
      cfg.checkName,
      'RECONCILIATION',
      r.expectedValue,
      r.actualValue,
      coalesce(r.actualValue, 0D) - coalesce(r.expectedValue, 0D),
      r.pctDiff,
      CASE WHEN r.pctDiff >= cfg.failureThreshold THEN 'FAILED'
           WHEN r.pctDiff >= cfg.warningThreshold THEN 'WARNING'
           ELSE 'HEALTHY' END,
      CASE WHEN r.pctDiff >= cfg.failureThreshold THEN 'CRITICAL'
           WHEN r.pctDiff >= cfg.warningThreshold THEN 'HIGH'
           ELSE 'INFO' END,
      cfg.isCritical,
      concat('Overview Gold does not reconcile to weekly Silver for ', r.metricName),
      CASE WHEN r.pctDiff >= cfg.failureThreshold
           THEN cfg.failureNextSteps ELSE cfg.warningNextSteps END,
      cfg.ownerTeam
    FROM recon r
    CROSS JOIN cfg;

    -- Overview key uniqueness
    INSERT INTO sdi_tbl_mip_validation_checkResults_perRun
    WITH d AS (
      SELECT coalesce(sum(cnt - 1), 0) AS duplicateRows
      FROM (
        SELECT
          targetWeekStartDate, filterLob, filterPlatform, metricName,
          count(*) AS cnt
        FROM sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
        WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        GROUP BY targetWeekStartDate, filterLob, filterPlatform, metricName
        HAVING count(*) > 1
      )
    ),
    cfg AS (
      SELECT *
      FROM sdi_tbl_mip_control_validationRules_static
      WHERE checkName = 'duplicateGoldKeys'
        AND isActive
    )
    SELECT
      p_runId, p_stageName, v_checkedAt,
      'GOLD',
      'sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long',
      concat(cast(v_weekFrom AS STRING), ' to ', cast(v_weekTo AS STRING)),
      cfg.checkName,
      'UNIQUENESS',
      0D,
      cast(d.duplicateRows AS DOUBLE),
      cast(d.duplicateRows AS DOUBLE),
      NULL,
      CASE WHEN d.duplicateRows >= cfg.failureThreshold THEN 'FAILED'
           WHEN d.duplicateRows >= cfg.warningThreshold THEN 'WARNING'
           ELSE 'HEALTHY' END,
      CASE WHEN d.duplicateRows >= cfg.failureThreshold THEN 'CRITICAL'
           WHEN d.duplicateRows >= cfg.warningThreshold THEN 'HIGH'
           ELSE 'INFO' END,
      cfg.isCritical,
      'Duplicate Overview Gold keys detected',
      CASE WHEN d.duplicateRows >= cfg.failureThreshold
           THEN cfg.failureNextSteps ELSE cfg.warningNextSteps END,
      cfg.ownerTeam
    FROM d
    CROSS JOIN cfg;
  END IF;

  -- --------------------------------------------------------------------------
  -- GOLD BREAKOUT
  -- --------------------------------------------------------------------------
  IF lower(p_stageName) IN ('goldbreakout', 'gold') THEN

    INSERT INTO sdi_tbl_mip_validation_checkResults_perRun
    WITH s AS (
      SELECT count(*) AS rowCount
      FROM sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
      WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        AND filterLob = 'All'
        AND filterPlatform = 'All'
    )
    SELECT
      p_runId, p_stageName, v_checkedAt,
      'GOLD',
      'sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long',
      concat(cast(v_weekFrom AS STRING), ' to ', cast(v_weekTo AS STRING)),
      'goldBreakoutRowsNonEmpty',
      'VOLUME',
      1D,
      cast(rowCount AS DOUBLE),
      cast(rowCount - 1 AS DOUBLE),
      NULL,
      CASE WHEN rowCount > 0 THEN 'HEALTHY' ELSE 'FAILED' END,
      CASE WHEN rowCount > 0 THEN 'INFO' ELSE 'CRITICAL' END,
      true,
      CASE WHEN rowCount > 0
           THEN 'Breakout Gold contains rows'
           ELSE 'Breakout Gold is empty for the requested week window' END,
      CASE WHEN rowCount > 0
           THEN NULL
           ELSE 'Stop before Crosstab/Explore Gold and inspect breakout attribution / Gold procedure.' END,
      'MIP Data Engineering'
    FROM s;

    -- Every attributed breakout should reconcile to topline for NBV.
    INSERT INTO sdi_tbl_mip_validation_checkResults_perRun
    WITH expectedBreakouts AS (
      SELECT breakoutType
      FROM sdi_tbl_mip_control_breakoutCatalog_static
      WHERE isActive
        AND isPrebuiltBreakout
    ),
    targetWeeks AS (
      SELECT targetWeekStartDate, thisWeekNumerator AS toplineNbv
      FROM sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
      WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        AND filterLob = 'All'
        AND filterPlatform = 'All'
        AND metricName = 'nbv'
    ),
    b AS (
      SELECT
        targetWeekStartDate,
        breakoutType,
        sum(coalesce(thisWeekNumerator, 0D)) AS breakoutNbv
      FROM sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
      WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        AND filterLob = 'All'
        AND filterPlatform = 'All'
        AND metricName = 'nbv'
      GROUP BY targetWeekStartDate, breakoutType
    ),
    r AS (
      SELECT
        t.targetWeekStartDate,
        e.breakoutType,
        t.toplineNbv,
        b.breakoutNbv,
        CASE
          WHEN b.breakoutType IS NULL THEN 1D
          WHEN coalesce(t.toplineNbv, 0D) = 0D THEN abs(coalesce(b.breakoutNbv, 0D))
          ELSE abs(b.breakoutNbv - t.toplineNbv) / abs(t.toplineNbv)
        END AS pctDiff
      FROM targetWeeks t
      CROSS JOIN expectedBreakouts e
      LEFT JOIN b
        ON  b.targetWeekStartDate = t.targetWeekStartDate
        AND b.breakoutType = e.breakoutType
    ),
    cfg AS (
      SELECT *
      FROM sdi_tbl_mip_control_validationRules_static
      WHERE checkName = 'goldBreakoutToplinePctDiff'
        AND isActive
    )
    SELECT
      p_runId, p_stageName, v_checkedAt,
      'GOLD',
      'sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long',
      concat(cast(r.targetWeekStartDate AS STRING), ' | ', r.breakoutType),
      cfg.checkName,
      'RECONCILIATION',
      r.toplineNbv,
      r.breakoutNbv,
      coalesce(r.breakoutNbv, 0D) - coalesce(r.toplineNbv, 0D),
      r.pctDiff,
      CASE WHEN r.pctDiff >= cfg.failureThreshold THEN 'FAILED'
           WHEN r.pctDiff >= cfg.warningThreshold THEN 'WARNING'
           ELSE 'HEALTHY' END,
      CASE WHEN r.pctDiff >= cfg.failureThreshold THEN 'CRITICAL'
           WHEN r.pctDiff >= cfg.warningThreshold THEN 'HIGH'
           ELSE 'INFO' END,
      cfg.isCritical,
      concat('Breakout NBV slices do not reconcile to topline for ', r.breakoutType),
      CASE WHEN r.pctDiff >= cfg.failureThreshold
           THEN cfg.failureNextSteps ELSE cfg.warningNextSteps END,
      cfg.ownerTeam
    FROM r
    CROSS JOIN cfg;

    INSERT INTO sdi_tbl_mip_validation_checkResults_perRun
    WITH d AS (
      SELECT coalesce(sum(cnt - 1), 0) AS duplicateRows
      FROM (
        SELECT
          targetWeekStartDate, filterLob, filterPlatform,
          breakoutType, breakoutValue, metricName,
          count(*) AS cnt
        FROM sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
        WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        GROUP BY
          targetWeekStartDate, filterLob, filterPlatform,
          breakoutType, breakoutValue, metricName
        HAVING count(*) > 1
      )
    ),
    cfg AS (
      SELECT *
      FROM sdi_tbl_mip_control_validationRules_static
      WHERE checkName = 'duplicateGoldKeys'
        AND isActive
    )
    SELECT
      p_runId, p_stageName, v_checkedAt,
      'GOLD',
      'sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long',
      concat(cast(v_weekFrom AS STRING), ' to ', cast(v_weekTo AS STRING)),
      cfg.checkName,
      'UNIQUENESS',
      0D,
      cast(d.duplicateRows AS DOUBLE),
      cast(d.duplicateRows AS DOUBLE),
      NULL,
      CASE WHEN d.duplicateRows >= cfg.failureThreshold THEN 'FAILED'
           WHEN d.duplicateRows >= cfg.warningThreshold THEN 'WARNING'
           ELSE 'HEALTHY' END,
      CASE WHEN d.duplicateRows >= cfg.failureThreshold THEN 'CRITICAL'
           WHEN d.duplicateRows >= cfg.warningThreshold THEN 'HIGH'
           ELSE 'INFO' END,
      cfg.isCritical,
      'Duplicate Breakout Gold keys detected',
      CASE WHEN d.duplicateRows >= cfg.failureThreshold
           THEN cfg.failureNextSteps ELSE cfg.warningNextSteps END,
      cfg.ownerTeam
    FROM d
    CROSS JOIN cfg;
  END IF;

  -- --------------------------------------------------------------------------
  -- GOLD CROSSTAB
  -- --------------------------------------------------------------------------
  IF lower(p_stageName) IN ('goldcrosstab', 'gold') THEN

    INSERT INTO sdi_tbl_mip_validation_checkResults_perRun
    WITH s AS (
      SELECT count(*) AS rowCount
      FROM sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long
      WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        AND filterLob = 'All'
        AND filterPlatform = 'All'
    )
    SELECT
      p_runId, p_stageName, v_checkedAt,
      'GOLD',
      'sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long',
      concat(cast(v_weekFrom AS STRING), ' to ', cast(v_weekTo AS STRING)),
      'goldCrosstabRowsNonEmpty',
      'VOLUME',
      1D,
      cast(rowCount AS DOUBLE),
      cast(rowCount - 1 AS DOUBLE),
      NULL,
      CASE WHEN rowCount > 0 THEN 'HEALTHY' ELSE 'FAILED' END,
      CASE WHEN rowCount > 0 THEN 'INFO' ELSE 'CRITICAL' END,
      true,
      CASE WHEN rowCount > 0
           THEN 'Crosstab Gold contains rows'
           ELSE 'Crosstab Gold is empty for the requested week window' END,
      CASE WHEN rowCount > 0
           THEN NULL
           ELSE 'Stop before Explore Gold and inspect crosstab-pair construction.' END,
      'MIP Data Engineering'
    FROM s;

    INSERT INTO sdi_tbl_mip_validation_checkResults_perRun
    WITH expectedPairs AS (
      SELECT pairKey
      FROM sdi_tbl_mip_control_crosstabCatalog_static
      WHERE isActive
    ),
    targetWeeks AS (
      SELECT targetWeekStartDate, thisWeekNumerator AS toplineNbv
      FROM sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
      WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        AND filterLob = 'All'
        AND filterPlatform = 'All'
        AND metricName = 'nbv'
    ),
    c AS (
      SELECT
        targetWeekStartDate,
        pairKey,
        sum(coalesce(thisWeekNumerator, 0D)) AS crosstabNbv
      FROM sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long
      WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        AND filterLob = 'All'
        AND filterPlatform = 'All'
        AND metricName = 'nbv'
      GROUP BY targetWeekStartDate, pairKey
    ),
    r AS (
      SELECT
        t.targetWeekStartDate,
        e.pairKey,
        t.toplineNbv,
        c.crosstabNbv,
        CASE
          WHEN c.pairKey IS NULL THEN 1D
          WHEN coalesce(t.toplineNbv, 0D) = 0D THEN abs(coalesce(c.crosstabNbv, 0D))
          ELSE abs(c.crosstabNbv - t.toplineNbv) / abs(t.toplineNbv)
        END AS pctDiff
      FROM targetWeeks t
      CROSS JOIN expectedPairs e
      LEFT JOIN c
        ON  c.targetWeekStartDate = t.targetWeekStartDate
        AND c.pairKey = e.pairKey
    ),
    cfg AS (
      SELECT *
      FROM sdi_tbl_mip_control_validationRules_static
      WHERE checkName = 'goldCrosstabToplinePctDiff'
        AND isActive
    )
    SELECT
      p_runId, p_stageName, v_checkedAt,
      'GOLD',
      'sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long',
      concat(cast(r.targetWeekStartDate AS STRING), ' | ', r.pairKey),
      cfg.checkName,
      'RECONCILIATION',
      r.toplineNbv,
      r.crosstabNbv,
      coalesce(r.crosstabNbv, 0D) - coalesce(r.toplineNbv, 0D),
      r.pctDiff,
      CASE WHEN r.pctDiff >= cfg.failureThreshold THEN 'FAILED'
           WHEN r.pctDiff >= cfg.warningThreshold THEN 'WARNING'
           ELSE 'HEALTHY' END,
      CASE WHEN r.pctDiff >= cfg.failureThreshold THEN 'CRITICAL'
           WHEN r.pctDiff >= cfg.warningThreshold THEN 'HIGH'
           ELSE 'INFO' END,
      cfg.isCritical,
      concat('Crosstab pair NBV cells do not reconcile to topline for ', r.pairKey),
      CASE WHEN r.pctDiff >= cfg.failureThreshold
           THEN cfg.failureNextSteps ELSE cfg.warningNextSteps END,
      cfg.ownerTeam
    FROM r
    CROSS JOIN cfg;

    INSERT INTO sdi_tbl_mip_validation_checkResults_perRun
    WITH d AS (
      SELECT coalesce(sum(cnt - 1), 0) AS duplicateRows
      FROM (
        SELECT
          targetWeekStartDate, filterLob, filterPlatform, pairKey,
          rowBreakoutValue, columnBreakoutValue, metricName,
          count(*) AS cnt
        FROM sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long
        WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        GROUP BY
          targetWeekStartDate, filterLob, filterPlatform, pairKey,
          rowBreakoutValue, columnBreakoutValue, metricName
        HAVING count(*) > 1
      )
    ),
    cfg AS (
      SELECT *
      FROM sdi_tbl_mip_control_validationRules_static
      WHERE checkName = 'duplicateGoldKeys'
        AND isActive
    )
    SELECT
      p_runId, p_stageName, v_checkedAt,
      'GOLD',
      'sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long',
      concat(cast(v_weekFrom AS STRING), ' to ', cast(v_weekTo AS STRING)),
      cfg.checkName,
      'UNIQUENESS',
      0D,
      cast(d.duplicateRows AS DOUBLE),
      cast(d.duplicateRows AS DOUBLE),
      NULL,
      CASE WHEN d.duplicateRows >= cfg.failureThreshold THEN 'FAILED'
           WHEN d.duplicateRows >= cfg.warningThreshold THEN 'WARNING'
           ELSE 'HEALTHY' END,
      CASE WHEN d.duplicateRows >= cfg.failureThreshold THEN 'CRITICAL'
           WHEN d.duplicateRows >= cfg.warningThreshold THEN 'HIGH'
           ELSE 'INFO' END,
      cfg.isCritical,
      'Duplicate Crosstab Gold keys detected',
      CASE WHEN d.duplicateRows >= cfg.failureThreshold
           THEN cfg.failureNextSteps ELSE cfg.warningNextSteps END,
      cfg.ownerTeam
    FROM d
    CROSS JOIN cfg;
  END IF;

  -- --------------------------------------------------------------------------
  -- GOLD EXPLORE
  -- --------------------------------------------------------------------------
  IF lower(p_stageName) IN ('goldexplore', 'gold') THEN

    INSERT INTO sdi_tbl_mip_validation_checkResults_perRun
    WITH expected AS (
      SELECT count(*) AS rowCount
      FROM sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily a
      JOIN sdi_tbl_mip_silver_attributesPerSession_daily s
        ON s.sessionId = a.sessionId
      WHERE s.weekStartDate BETWEEN v_weekFrom AND v_weekTo
        AND s.visitorId IS NOT NULL
    ),
    actual AS (
      SELECT count(*) AS rowCount
      FROM sdi_tbl_mip_gold_exploreSessionPageCategory_weekly
      WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
    )
    SELECT
      p_runId, p_stageName, v_checkedAt,
      'GOLD',
      'sdi_tbl_mip_gold_exploreSessionPageCategory_weekly',
      concat(cast(v_weekFrom AS STRING), ' to ', cast(v_weekTo AS STRING)),
      'goldExploreRowCountMatch',
      'RECONCILIATION',
      cast(e.rowCount AS DOUBLE),
      cast(a.rowCount AS DOUBLE),
      cast(a.rowCount - e.rowCount AS DOUBLE),
      CASE WHEN e.rowCount = 0
           THEN abs(a.rowCount - e.rowCount)
           ELSE abs(a.rowCount - e.rowCount) / cast(e.rowCount AS DOUBLE) END,
      CASE WHEN a.rowCount = e.rowCount THEN 'HEALTHY' ELSE 'FAILED' END,
      CASE WHEN a.rowCount = e.rowCount THEN 'INFO' ELSE 'CRITICAL' END,
      true,
      CASE WHEN a.rowCount = e.rowCount
           THEN 'Explore Gold row count matches session-page-category Silver'
           ELSE 'Explore Gold row count does not match its Silver source' END,
      CASE WHEN a.rowCount = e.rowCount
           THEN NULL
           ELSE 'Inspect the Explore Gold write/join before exposing Explore to the application.' END,
      'MIP Data Engineering'
    FROM expected e
    CROSS JOIN actual a;

    INSERT INTO sdi_tbl_mip_validation_checkResults_perRun
    WITH d AS (
      SELECT coalesce(sum(cnt - 1), 0) AS duplicateRows
      FROM (
        SELECT weekStartDate, sessionId, pageCategory, count(*) AS cnt
        FROM sdi_tbl_mip_gold_exploreSessionPageCategory_weekly
        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
        GROUP BY weekStartDate, sessionId, pageCategory
        HAVING count(*) > 1
      )
    ),
    cfg AS (
      SELECT *
      FROM sdi_tbl_mip_control_validationRules_static
      WHERE checkName = 'duplicateGoldKeys'
        AND isActive
    )
    SELECT
      p_runId, p_stageName, v_checkedAt,
      'GOLD',
      'sdi_tbl_mip_gold_exploreSessionPageCategory_weekly',
      concat(cast(v_weekFrom AS STRING), ' to ', cast(v_weekTo AS STRING)),
      cfg.checkName,
      'UNIQUENESS',
      0D,
      cast(d.duplicateRows AS DOUBLE),
      cast(d.duplicateRows AS DOUBLE),
      NULL,
      CASE WHEN d.duplicateRows >= cfg.failureThreshold THEN 'FAILED'
           WHEN d.duplicateRows >= cfg.warningThreshold THEN 'WARNING'
           ELSE 'HEALTHY' END,
      CASE WHEN d.duplicateRows >= cfg.failureThreshold THEN 'CRITICAL'
           WHEN d.duplicateRows >= cfg.warningThreshold THEN 'HIGH'
           ELSE 'INFO' END,
      cfg.isCritical,
      'Duplicate Explore Gold keys detected',
      CASE WHEN d.duplicateRows >= cfg.failureThreshold
           THEN cfg.failureNextSteps ELSE cfg.warningNextSteps END,
      cfg.ownerTeam
    FROM d
    CROSS JOIN cfg;
  END IF;

  -- --------------------------------------------------------------------------
  -- Convert WARNING/FAILED checks into actionable issues.
  -- --------------------------------------------------------------------------
  INSERT INTO sdi_tbl_mip_validation_issueDetails_perRun
  SELECT
    concat(
      'ISS_', p_runId, '_',
      upper(substr(sha2(concat_ws('|', p_stageName, checkName, scopePeriod, objectAffected), 256), 1, 16))
    ) AS issueId,
    p_runId,
    p_stageName,
    v_checkedAt,

    'OPEN',
    severity,
    checkType,

    layerAffected,
    objectAffected,
    scopePeriod,
    checkName,

    issueShortDescription,
    concat(
      issueShortDescription,
      '. actual=', coalesce(cast(actualValue AS STRING), 'NULL'),
      '; expected=', coalesce(cast(expectedValue AS STRING), 'NULL'),
      CASE WHEN variancePct IS NOT NULL
           THEN concat('; variancePct=', cast(variancePct AS STRING))
           ELSE '' END
    ),

    actualValue,
    expectedValue,
    varianceValue,
    variancePct,

    NULL,
    NULL,
    NULL,
    NULL,

    NULL,
    nextSteps,
    ownerTeam,

    v_checkedAt,
    v_checkedAt,
    1,

    NULL,
    NULL
  FROM sdi_tbl_mip_validation_checkResults_perRun
  WHERE runId = p_runId
    AND stageName = p_stageName
    AND checkedAt = v_checkedAt
    AND checkStatus IN ('WARNING', 'FAILED');

  -- --------------------------------------------------------------------------
  -- Fail-fast behaviour
  -- --------------------------------------------------------------------------
  SET VAR v_stopCount = (
    SELECT count(*)
    FROM sdi_tbl_mip_validation_checkResults_perRun
    WHERE runId = p_runId
      AND stageName = p_stageName
      AND checkedAt = v_checkedAt
      AND checkStatus = 'FAILED'
      AND isCritical
  );

  SET VAR v_warningCount = (
    SELECT count(*)
    FROM sdi_tbl_mip_validation_checkResults_perRun
    WHERE runId = p_runId
      AND stageName = p_stageName
      AND checkedAt = v_checkedAt
      AND checkStatus = 'WARNING'
  );

  IF v_stopCount > 0 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'MIP_VALIDATION_FAILED: critical validation failure. See validation_checkResults_perRun and validation_issueDetails_perRun.';
  END IF;

  IF p_failOnWarning AND v_warningCount > 0 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'MIP_VALIDATION_WARNING_STRICT: warning found and strict mode is enabled.';
  END IF;
END;


-- ###########################################################################
-- END validation/01_sdi_sp_mip_validation_runChecks_perRun.sql
-- ###########################################################################

