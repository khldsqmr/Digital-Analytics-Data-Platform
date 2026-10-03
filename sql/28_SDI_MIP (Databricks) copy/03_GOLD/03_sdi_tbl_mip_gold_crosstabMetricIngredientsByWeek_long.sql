-- ============================================================================
-- FILE  : 03_sdi_sp_mip_gold_crosstabMetricIngredientsByWeek_long.sql
-- LAYER : GOLD
-- PURPOSE:
--   Gold crosstab comparison ingredients by target week, supported pair cell, and metric.
--
-- DESIGN:
--   - One top-level CREATE OR REPLACE PROCEDURE per file.
--   - No run/job ID dependency during development.
--   - Uses control VIEWS, not persisted control tables.
--   - Validates required sources/control metadata before target creation/write.
--   - p_validateOnly = TRUE performs preflight only.
--   - Default as-of date is the previous Pacific calendar day.
--   - Comparison percentages are NOT persisted; Gold stores ingredients.
--   - PERFORMANCE: visitor-week Silver is restricted to target/prior/4-week/LY weeks before pair expansion.
--   - PERFORMANCE: only dimensions required by the crosstab dimension map are projected.
--   - Every isActive row in sdi_vw_mip_control_crosstabCatalog_static is generated here.
--   - isPrebuiltPair is App/UI metadata only and is intentionally not persisted in analytical Gold.
-- ============================================================================
CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_gold_crosstabMetricIngredientsByWeek_long(
    IN p_asOfDate       DATE    DEFAULT NULL,
    IN p_weeksToRebuild INT     DEFAULT 1,
    IN p_validateOnly   BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Gold crosstab comparison ingredients by target week, supported pair cell, and metric.'
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
        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly a
        JOIN prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly x
          ON x.weekStartDate = a.weekStartDate
         AND x.visitorId = a.visitorId
        WHERE a.weekStartDate BETWEEN v_weekFrom AND v_weekTo
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Weekly Silver attribute/action visitor keys have no joined rows for the requested Gold target-week range.';
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
    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_crosstabCatalog_static
        WHERE isActive
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Crosstab catalog control view has no active pairs.';
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
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long (
targetWeekStartDate         DATE,
  targetWeekEndDate           DATE,
  fiscalQuarterLabel          STRING,
  fiscalWeekCode              STRING,
  weekLabel                   STRING,
  filterLob                   STRING COMMENT 'All for now; reserved for precomputed whole-report filter contexts',
  filterPlatform              STRING COMMENT 'All for now; reserved for precomputed whole-report filter contexts',
  pairKey                     STRING,
  pairLabel                   STRING,
  rowBreakoutType             STRING,
  rowBreakoutValue            STRING,
  columnBreakoutType          STRING,
  columnBreakoutValue         STRING,
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
  peerSetNumerator            DOUBLE,
  peerSetDenominator          DOUBLE,
  goldProcessedAt             TIMESTAMP
        )
        USING DELTA
        CLUSTER BY (targetWeekStartDate, pairKey, metricName)
        COMMENT 'Gold Crosstabs: supported attributed breakout-pair cells with safe comparison ingredients.';
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
        visitorWeek AS (
            SELECT
              a.weekStartDate,
              a.lob,
              a.platform,
              a.prospectVsBase,
              a.authState,
              a.channel,
              a.campaign,
              a.entryPage,
              a.pageCategory,
              a.device,
              a.utmSource,
              a.utmMedium,
              a.utmCampaign,
              a.buyFlowStep,
              x.nbv,
              x.sessionCount,
              x.pageViews,
              x.nbvBuyFlow,
              x.nbvConfigure,
              x.nbvCheckoutStart,
              x.orders,
              x.ordersAcquisition,
              x.ordersBase,
              x.ordersUnassisted,
              x.ordersAssisted,
              x.vrCalls,
              x.vrChats,
              x.storeLocator,
              x.orderCount,
              map(
                'lob',            coalesce(a.lob, 'Other'),
                'platform',       coalesce(a.platform, '(not set)'),
                'prospectVsBase', coalesce(a.prospectVsBase, 'Unknown'),
                'authState',      coalesce(a.authState, '(not set)'),
                'channel',        coalesce(a.channel, '(not set)'),
                'campaign',       coalesce(a.campaign, '(not set)'),
                'entryPage',      coalesce(a.entryPage, '(not set)'),
                'pageCategory',   coalesce(a.pageCategory, '(not set)'),
                'device',         coalesce(a.device, 'Unknown'),
                'utmSource',      coalesce(a.utmSource, '(not set)'),
                'utmMedium',      coalesce(a.utmMedium, '(not set)'),
                'utmCampaign',    coalesce(a.utmCampaign, '(not set)'),
                'buyFlowStep',    coalesce(a.buyFlowStep, 'Did not enter buy flow')
              ) AS dimensionMap
            FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly a
            JOIN prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly x
              ON  x.weekStartDate = a.weekStartDate
              AND x.visitorId = a.visitorId
            WHERE a.weekStartDate IN (SELECT weekStartDate FROM requiredWeeks)
          ),
          pairRows AS (
            SELECT
              v.weekStartDate,
              p.pairKey,
              p.pairLabel,
              p.rowBreakoutType,
              element_at(v.dimensionMap, p.rowBreakoutType) AS rowBreakoutValue,
              p.columnBreakoutType,
              element_at(v.dimensionMap, p.columnBreakoutType) AS columnBreakoutValue,
              v.nbv,
              v.sessionCount,
              v.pageViews,
              v.nbvBuyFlow,
              v.nbvConfigure,
              v.nbvCheckoutStart,
              v.orders,
              v.ordersAcquisition,
              v.ordersBase,
              v.ordersUnassisted,
              v.ordersAssisted,
              v.vrCalls,
              v.vrChats,
              v.storeLocator,
              v.orderCount
            FROM visitorWeek v
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_crosstabCatalog_static p
              ON p.isActive
          ),
          weeklyWide AS (
            SELECT
              weekStartDate,
              pairKey,
              max(pairLabel) AS pairLabel,
              max(rowBreakoutType) AS rowBreakoutType,
              rowBreakoutValue,
              max(columnBreakoutType) AS columnBreakoutType,
              columnBreakoutValue,
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
            FROM pairRows
            GROUP BY weekStartDate, pairKey, rowBreakoutValue, columnBreakoutValue
          ),
          weeklyCounts AS (
            SELECT
              weekStartDate,
              pairKey,
              pairLabel,
              rowBreakoutType,
              rowBreakoutValue,
              columnBreakoutType,
              columnBreakoutValue,
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
            SELECT
              c.weekStartDate,
              c.pairKey,
              c.pairLabel,
              c.rowBreakoutType,
              c.rowBreakoutValue,
              c.columnBreakoutType,
              c.columnBreakoutValue,
              c.metricName,
              c.metricValue AS numeratorValue,
              cast(NULL AS DOUBLE) AS denominatorValue
            FROM weeklyCounts c
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static m
              ON  m.metricName = c.metricName
              AND m.metricKind = 'count'
              AND m.isActive
            UNION ALL
            SELECT
              n.weekStartDate,
              n.pairKey,
              max(n.pairLabel) AS pairLabel,
              max(n.rowBreakoutType) AS rowBreakoutType,
              n.rowBreakoutValue,
              max(n.columnBreakoutType) AS columnBreakoutType,
              n.columnBreakoutValue,
              r.metricName,
              max(CASE WHEN n.metricName = r.numeratorMetric THEN n.metricValue END) AS numeratorValue,
              max(CASE WHEN n.metricName = r.denominatorMetric THEN n.metricValue END) AS denominatorValue
            FROM weeklyCounts n
            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static r
              ON  r.metricKind = 'ratio'
              AND r.isActive
              AND n.metricName IN (r.numeratorMetric, r.denominatorMetric)
            GROUP BY
              n.weekStartDate, n.pairKey, n.rowBreakoutValue, n.columnBreakoutValue, r.metricName
          ),
          availableWeeks AS (
            SELECT DISTINCT weekStartDate
            FROM visitorWeek
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
          ),
          candidatePairs AS (
            SELECT DISTINCT
              t.weekStartDate AS targetWeekStartDate,
              h.pairKey,
              h.pairLabel,
              h.rowBreakoutType,
              h.rowBreakoutValue,
              h.columnBreakoutType,
              h.columnBreakoutValue
            FROM targetWeeks t
            JOIN weeklyWide h
              ON  h.weekStartDate = t.weekStartDate
               OR h.weekStartDate = t.priorWeekStartDate
               OR h.weekStartDate BETWEEN t.fourWeekAvgStartDate AND t.fourWeekAvgEndDate
               OR h.weekStartDate = t.sameWeekLastYearStartDate
          )
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long
        REPLACE WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        SELECT
            t.weekStartDate AS targetWeekStartDate,
            t.weekEndDate AS targetWeekEndDate,
            t.fiscalQuarterLabel,
            t.fiscalWeekCode,
            t.weekLabel,
            'All' AS filterLob,
            'All' AS filterPlatform,
            v.pairKey,
            v.pairLabel,
            v.rowBreakoutType,
            v.rowBreakoutValue,
            v.columnBreakoutType,
            v.columnBreakoutValue,
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
          JOIN candidatePairs v
            ON v.targetWeekStartDate = t.weekStartDate
          JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static m
            ON m.isActive
          LEFT JOIN weeklyIngredients cur
            ON  cur.weekStartDate = t.weekStartDate
            AND cur.pairKey = v.pairKey
            AND cur.rowBreakoutValue = v.rowBreakoutValue
            AND cur.columnBreakoutValue = v.columnBreakoutValue
            AND cur.metricName = m.metricName
          LEFT JOIN weeklyIngredients pw
            ON  pw.weekStartDate = t.priorWeekStartDate
            AND pw.pairKey = v.pairKey
            AND pw.rowBreakoutValue = v.rowBreakoutValue
            AND pw.columnBreakoutValue = v.columnBreakoutValue
            AND pw.metricName = m.metricName
          LEFT JOIN weeklyIngredients fw
            ON  fw.weekStartDate BETWEEN t.fourWeekAvgStartDate AND t.fourWeekAvgEndDate
            AND fw.pairKey = v.pairKey
            AND fw.rowBreakoutValue = v.rowBreakoutValue
            AND fw.columnBreakoutValue = v.columnBreakoutValue
            AND fw.metricName = m.metricName
          LEFT JOIN weeklyIngredients ly
            ON  ly.weekStartDate = t.sameWeekLastYearStartDate
            AND ly.pairKey = v.pairKey
            AND ly.rowBreakoutValue = v.rowBreakoutValue
            AND ly.columnBreakoutValue = v.columnBreakoutValue
            AND ly.metricName = m.metricName
          GROUP BY
            t.weekStartDate,
            t.weekEndDate,
            t.fiscalQuarterLabel,
            t.fiscalWeekCode,
            t.weekLabel,
            v.pairKey,
            v.pairLabel,
            v.rowBreakoutType,
            v.rowBreakoutValue,
            v.columnBreakoutType,
            v.columnBreakoutValue,
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
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long' AS targetObject;
    END IF;
END;

-- ============================================================================
-- DEVELOPMENT / TEST EXAMPLES
-- ============================================================================

-- A. PREFLIGHT ONLY
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_crosstabMetricIngredientsByWeek_long(
--     p_asOfDate       => DATE '2026-09-28',
--     p_weeksToRebuild => 1,
--     p_validateOnly   => TRUE
-- );

-- B. EXECUTE / REBUILD
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_crosstabMetricIngredientsByWeek_long(
--     p_asOfDate       => DATE '2026-09-28',
--     p_weeksToRebuild => 1,
--     p_validateOnly   => FALSE
-- );

-- C. VALIDATION 1: CELL GRAIN UNIQUENESS
-- Expected: no rows.
-- SELECT
--     targetWeekStartDate,pairKey,rowBreakoutValue,columnBreakoutValue,metricName,
--     COUNT(*) AS rowCount
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long
-- WHERE targetWeekStartDate = DATE '2026-09-27'
-- GROUP BY targetWeekStartDate,pairKey,rowBreakoutValue,columnBreakoutValue,metricName
-- HAVING COUNT(*) > 1
-- ORDER BY rowCount DESC
-- LIMIT 100;

-- D. VALIDATION 2: NBV ADDITIVITY BY PAIR
-- Each visitor maps to one cell for each active pair.
-- Expected: nbvDiff = 0 for every pairKey.
-- WITH total AS (
--     SELECT SUM(nbv) AS nbv
--     FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly
--     WHERE weekStartDate = DATE '2026-09-27'
-- ),
-- byPair AS (
--     SELECT pairKey,SUM(thisWeekNumerator) AS nbv
--     FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long
--     WHERE targetWeekStartDate = DATE '2026-09-27'
--       AND metricName = 'nbv'
--     GROUP BY pairKey
-- )
-- SELECT pairKey,p.nbv-t.nbv AS nbvDiff
-- FROM byPair p CROSS JOIN total t
-- ORDER BY pairKey;

-- E. VALIDATION 3: ACTIVE PAIR CATALOG
-- Expected: invalidRows = 0.
-- SELECT COUNT(*) AS invalidRows
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long g
-- LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_crosstabCatalog_static p
--   ON p.pairKey=g.pairKey
-- WHERE g.targetWeekStartDate = DATE '2026-09-27'
--   AND (p.pairKey IS NULL OR NOT p.isActive);

