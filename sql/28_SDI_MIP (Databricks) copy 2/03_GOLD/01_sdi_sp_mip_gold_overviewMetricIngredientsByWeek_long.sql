-- ============================================================================
-- FILE  : 01_sdi_sp_mip_gold_overviewMetricIngredientsByWeek_long.sql
-- LAYER : GOLD
-- PURPOSE:
--   Gold Overview comparison ingredients by target week and metric; final percentages/pp are calculated downstream.
--
-- DESIGN:
--   - One top-level CREATE OR REPLACE PROCEDURE per file.
--   - No run/job ID dependency during development.
--   - Uses control VIEWS, not persisted control tables.
--   - Validates required sources/control metadata before target creation/write.
--   - p_validateOnly = TRUE performs preflight only.
--   - Default as-of date is the previous Pacific calendar day.
--   - Comparison percentages are NOT persisted; Gold stores ingredients.
--   - PERFORMANCE: only target/prior/4-week/LY weeks are scanned from weekly Silver.
--   - PEER SET: Overview topline has no "all other values" peer population, so the
--     existing peerSetNumerator/peerSetDenominator columns remain NULL here.
--     Breakout and Crosstab Gold populate their peer counterfactual ingredients.
--   - IMPACT: Overview supplies the topline comparison denominator used downstream
--     for impact-on-topline: (slice current - slice comparator) / topline comparator.
-- ============================================================================
CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_gold_overviewMetricIngredientsByWeek_long(
    IN p_asOfDate       DATE    DEFAULT NULL,
    IN p_weeksToRebuild INT     DEFAULT 1,
    IN p_validateOnly   BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Gold Overview comparison ingredients by target week and metric; final percentages/pp are calculated downstream.'
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
        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly
        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Silver actionsPerVisitorWeek has no rows for the requested Gold target-week range.';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static
        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Fiscal calendar control view has no rows for the requested Gold target-week range.';
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
            'No Gold table was created or modified.' AS message;
    ELSE
        -- --------------------------------------------------------------------
        -- 4. Create target only after preflight succeeds
        -- --------------------------------------------------------------------
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long (
targetWeekStartDate         DATE,
  targetWeekEndDate           DATE,
  fiscalQuarterLabel          STRING,
  fiscalWeekCode              STRING,
  weekLabel                   STRING,
  filterLob                   STRING COMMENT 'All for now; schema reserved for whole-report LOB filter contexts',
  filterPlatform              STRING COMMENT 'All for now; schema reserved for whole-report Platform filter contexts',
  metricName                  STRING,
  metricLabel                 STRING,
  metricKind                  STRING,
  displayFormat               STRING,
  changeUnit                  STRING,
  thisWeekNumerator           DOUBLE,
  thisWeekDenominator         DOUBLE,
  priorWeekNumerator          DOUBLE,
  priorWeekDenominator        DOUBLE,
  fourWeekTrendNumerator      DOUBLE,
  fourWeekTrendDenominator    DOUBLE,
  sameWeekLyNumerator         DOUBLE,
  sameWeekLyDenominator       DOUBLE,
  thisWeekDataAvailable       BOOLEAN,
  priorWeekDataAvailable      BOOLEAN,
  fourWeekTrendWeekCount      INT,
  sameWeekLyDataAvailable     BOOLEAN,
  peerSetNumerator            DOUBLE COMMENT 'Intentionally NULL at topline grain; peer sets are defined for breakout values/crosstab cells, not the topline itself',
  peerSetDenominator          DOUBLE COMMENT 'Intentionally NULL at topline grain; retained for downstream schema compatibility',
  goldProcessedAt             TIMESTAMP
        )
        USING DELTA
        CLUSTER BY (targetWeekStartDate, metricName)
        COMMENT 'Gold Overview: comparison ingredients by target week and metric. Final percentages/pp are calculated later.';
        -- --------------------------------------------------------------------
        -- 5. Rebuild requested target-week range
        -- --------------------------------------------------------------------
        WITH targetCalendar AS (
            SELECT *
            FROM prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static
            WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
        ),
        requiredWeeks AS (
            SELECT weekStartDate FROM targetCalendar
            UNION
            SELECT priorWeekStartDate FROM targetCalendar
            UNION
            SELECT sameWeekLastYearStartDate FROM targetCalendar
            UNION
            SELECT explode(sequence(fourWeekAvgStartDate,fourWeekAvgEndDate,INTERVAL 7 DAYS)) AS weekStartDate
            FROM targetCalendar
        ),
        weeklyWide AS (
            SELECT
              weekStartDate,
              max(weekEndDate) AS weekEndDate,
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
            FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly a
            WHERE a.weekStartDate IN (SELECT weekStartDate FROM requiredWeeks)
            GROUP BY a.weekStartDate
          ),
          weeklyCounts AS (
            SELECT
              weekStartDate,
              weekEndDate,
              metricName,
              metricValue
            FROM weeklyWide
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
          weeklyIngredients AS (
            -- Counts: denominator stays NULL; four-week count trends use fourWeekTrendWeekCount.
            SELECT
              c.weekStartDate,
              c.weekEndDate,
              c.metricName,
              c.metricValue AS numeratorValue,
              cast(NULL AS DOUBLE) AS denominatorValue
            FROM weeklyCounts c
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static m
              ON  m.metricName = c.metricName
              AND m.metricKind = 'count'
              AND m.isActive
            UNION ALL
            -- Ratios: keep the underlying count numerator/denominator.
            SELECT
              n.weekStartDate,
              max(n.weekEndDate) AS weekEndDate,
              r.metricName,
              max(CASE WHEN n.metricName = r.numeratorMetric THEN n.metricValue END) AS numeratorValue,
              max(CASE WHEN n.metricName = r.denominatorMetric THEN n.metricValue END) AS denominatorValue
            FROM weeklyCounts n
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static r
              ON  r.metricKind = 'ratio'
              AND r.isActive
              AND n.metricName IN (r.numeratorMetric, r.denominatorMetric)
            GROUP BY n.weekStartDate, r.metricName
          ),
          availableWeeks AS (
            SELECT weekStartDate
            FROM weeklyWide
          ),
          fourWeekAvailability AS (
            SELECT
              t.weekStartDate AS targetWeekStartDate,
              count(a.weekStartDate) AS fourWeekTrendWeekCount
            FROM targetCalendar t
            LEFT JOIN availableWeeks a
              ON a.weekStartDate BETWEEN t.fourWeekAvgStartDate AND t.fourWeekAvgEndDate
            GROUP BY t.weekStartDate
          ),
          targetWeeks AS (
            SELECT
              t.*,
              (cur.weekStartDate IS NOT NULL) AS thisWeekDataAvailable,
              (pw.weekStartDate IS NOT NULL) AS priorWeekDataAvailable,
              cast(coalesce(fwa.fourWeekTrendWeekCount, 0) AS INT) AS fourWeekTrendWeekCount,
              (ly.weekStartDate IS NOT NULL) AS sameWeekLyDataAvailable
            FROM targetCalendar t
            LEFT JOIN availableWeeks cur
              ON cur.weekStartDate = t.weekStartDate
            LEFT JOIN availableWeeks pw
              ON pw.weekStartDate = t.priorWeekStartDate
            LEFT JOIN fourWeekAvailability fwa
              ON fwa.targetWeekStartDate = t.weekStartDate
            LEFT JOIN availableWeeks ly
              ON ly.weekStartDate = t.sameWeekLastYearStartDate
          )
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
        REPLACE WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        SELECT
            t.weekStartDate AS targetWeekStartDate,
            t.weekEndDate AS targetWeekEndDate,
            t.fiscalQuarterLabel,
            t.fiscalWeekCode,
            t.weekLabel,
            'All' AS filterLob,
            'All' AS filterPlatform,
            m.metricName,
            m.metricLabel,
            m.metricKind,
            m.displayFormat,
            m.changeUnit,
            CASE
              WHEN t.thisWeekDataAvailable THEN coalesce(cur.numeratorValue, 0D)
              ELSE NULL
            END AS thisWeekNumerator,
            CASE
              WHEN m.metricKind = 'ratio' AND t.thisWeekDataAvailable
                THEN coalesce(cur.denominatorValue, 0D)
              ELSE NULL
            END AS thisWeekDenominator,
            CASE
              WHEN t.priorWeekDataAvailable THEN coalesce(pw.numeratorValue, 0D)
              ELSE NULL
            END AS priorWeekNumerator,
            CASE
              WHEN m.metricKind = 'ratio' AND t.priorWeekDataAvailable
                THEN coalesce(pw.denominatorValue, 0D)
              ELSE NULL
            END AS priorWeekDenominator,
            CASE
              WHEN t.fourWeekTrendWeekCount > 0 THEN coalesce(sum(fw.numeratorValue), 0D)
              ELSE NULL
            END AS fourWeekTrendNumerator,
            CASE
              WHEN m.metricKind = 'ratio' AND t.fourWeekTrendWeekCount > 0
                THEN coalesce(sum(fw.denominatorValue), 0D)
              ELSE NULL
            END AS fourWeekTrendDenominator,
            CASE
              WHEN t.sameWeekLyDataAvailable THEN coalesce(ly.numeratorValue, 0D)
              ELSE NULL
            END AS sameWeekLyNumerator,
            CASE
              WHEN m.metricKind = 'ratio' AND t.sameWeekLyDataAvailable
                THEN coalesce(ly.denominatorValue, 0D)
              ELSE NULL
            END AS sameWeekLyDenominator,
            t.thisWeekDataAvailable,
            t.priorWeekDataAvailable,
            t.fourWeekTrendWeekCount,
            t.sameWeekLyDataAvailable,
            cast(NULL AS DOUBLE) AS peerSetNumerator,
            cast(NULL AS DOUBLE) AS peerSetDenominator,
            v_processedAt AS goldProcessedAt
          FROM targetWeeks t
          JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static m
            ON m.isActive
          LEFT JOIN weeklyIngredients cur
            ON  cur.weekStartDate = t.weekStartDate
            AND cur.metricName = m.metricName
          LEFT JOIN weeklyIngredients pw
            ON  pw.weekStartDate = t.priorWeekStartDate
            AND pw.metricName = m.metricName
          LEFT JOIN weeklyIngredients fw
            ON  fw.weekStartDate BETWEEN t.fourWeekAvgStartDate AND t.fourWeekAvgEndDate
            AND fw.metricName = m.metricName
          LEFT JOIN weeklyIngredients ly
            ON  ly.weekStartDate = t.sameWeekLastYearStartDate
            AND ly.metricName = m.metricName
          GROUP BY
            t.weekStartDate,
            t.weekEndDate,
            t.fiscalQuarterLabel,
            t.fiscalWeekCode,
            t.weekLabel,
            m.metricName,
            m.metricLabel,
            m.metricKind,
            m.displayFormat,
            m.changeUnit,
            t.thisWeekDataAvailable,
            t.priorWeekDataAvailable,
            t.fourWeekTrendWeekCount,
            t.sameWeekLyDataAvailable,
            cur.numeratorValue,
            cur.denominatorValue,
            pw.numeratorValue,
            pw.denominatorValue,
            ly.numeratorValue,
            ly.denominatorValue;
        SELECT
            'SUCCESS' AS status,
            v_weekFrom AS rebuiltWeekStartFrom,
            v_weekTo AS rebuiltWeekStartTo,
            v_weekEndTo AS latestWeekEndDate,
            CASE WHEN v_asOfDate < v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long' AS targetObject;
    END IF;
END;
-- ============================================================================
-- DEVELOPMENT / TEST EXAMPLES
-- Run these statements separately after deploying the procedure.
-- ============================================================================
-- A. PREFLIGHT ONLY
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_overviewMetricIngredientsByWeek_long(
--     p_asOfDate       => DATE '2026-09-28',
--     p_weeksToRebuild => 1,
--     p_validateOnly   => TRUE
-- );
-- B. EXECUTE / REBUILD
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_overviewMetricIngredientsByWeek_long(
--     p_asOfDate       => DATE '2026-09-28',
--     p_weeksToRebuild => 1,
--     p_validateOnly   => FALSE
-- );
-- C. VALIDATION 1: TARGET GRAIN UNIQUENESS
-- Expected: no rows.
-- SELECT
--     targetWeekStartDate,filterLob,filterPlatform,metricName,COUNT(*) AS rowCount
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
-- WHERE targetWeekStartDate = DATE '2026-09-27'
-- GROUP BY targetWeekStartDate,filterLob,filterPlatform,metricName
-- HAVING COUNT(*) > 1
-- ORDER BY rowCount DESC;
-- D. VALIDATION 2: NBV RECONCILIATION TO WEEKLY SILVER
-- Expected: nbvDiff = 0.
-- WITH silver AS (
--     SELECT SUM(nbv) AS nbv
--     FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly
--     WHERE weekStartDate = DATE '2026-09-27'
-- ),
-- gold AS (
--     SELECT thisWeekNumerator AS nbv
--     FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
--     WHERE targetWeekStartDate = DATE '2026-09-27'
--       AND filterLob = 'All'
--       AND filterPlatform = 'All'
--       AND metricName = 'nbv'
-- )
-- SELECT gold.nbv-silver.nbv AS nbvDiff FROM gold CROSS JOIN silver;
-- E. VALIDATION 3: ACTIVE METRIC / RATIO SANITY
-- Expected: invalidRatioRows = 0.
-- SELECT
--     COUNT_IF(metricKind='ratio' AND thisWeekDataAvailable
--              AND thisWeekDenominator IS NULL) AS invalidRatioRows,
--     COUNT_IF(metricName='nbv' AND metricLabel<>'Total NBV') AS invalidNbvLabelRows
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
-- WHERE targetWeekStartDate = DATE '2026-09-27';
