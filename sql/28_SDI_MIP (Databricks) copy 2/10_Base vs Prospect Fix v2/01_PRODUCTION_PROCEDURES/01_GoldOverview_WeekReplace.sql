-- PERMANENT PRODUCTION PROCEDURE. Deploy only after validating staging and scheduling cutover.
-- This file DOES NOT execute the procedure; executing its CREATE OR REPLACE updates procedure metadata.
-- ============================================================================

-- FILE  : 01_sdi_sp_mip_gold_overviewMetricIngredientsByWeek_long.sql

-- LAYER : GOLD
--
-- RUNTIME WRITE NOTE:
--   Uses atomic whole-week Delta scoped REPLACE WHERE; no row-level MERGE.
--   Runtime scope dates are rendered into EXECUTE IMMEDIATE using make_date().
--   Procedure-local week DATE variables are used directly in normal source filters
--   and in the scoped REPLACE WHERE predicate.
--
-- CALL COMPATIBILITY:
--   - Manual SQL: CALL with DATE / INT / BOOLEAN literals.
--   - Notebook: render only already-validated Python date/int values as SQL literals
--     in CALL because this runtime requires foldable stored-procedure arguments.
--

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
--   - GLOBAL FILTER: precomputes filterProspectVsBase='All' plus mapped weekly states (Prospect/Base/Unknown).
--     Customer and Care map to Base at global-filter grain; upstream Silver remains unchanged.

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
    DECLARE v_writeSql STRING;
    DECLARE v_scopeStartSql STRING;
    DECLARE v_scopeEndSql STRING;

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
        INNER JOIN prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly x
          ON x.weekStartDate=a.weekStartDate
         AND x.visitorId=a.visitorId
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

  filterProspectVsBase        STRING COMMENT 'Global filter context: All plus normalized weekly prospectVsBase (Prospect/Base/Unknown)',

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

        SET v_scopeStartSql = concat('make_date(',year(v_weekFrom),',',month(v_weekFrom),',',day(v_weekFrom),')');
        SET v_scopeEndSql = concat('make_date(',year(v_weekTo),',',month(v_weekTo),',',day(v_weekTo),')');
        SET v_writeSql = concat(
            'WITH targetCalendar AS (
            SELECT *
            FROM prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static
            WHERE weekStartDate BETWEEN ',
            v_scopeStartSql,
            ' AND ',
            v_scopeEndSql,
            '
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
            -- Action-driven so filterProspectVsBase=\'All\' preserves the exact existing
            -- Overview topline even if an attribute row is unexpectedly absent. Any
            -- unmatched attribute row is visible under Unknown rather than dropping data.
            SELECT
                x.weekStartDate,
                x.weekEndDate,
                x.visitorId,
                CASE
                    WHEN trim(a.prospectVsBase)=\'Prospect\' THEN \'Prospect\'
                    WHEN trim(a.prospectVsBase) IN (\'Customer\',\'Care\') THEN \'Base\'
                    ELSE \'Unknown\'
                END AS prospectVsBase,
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
                x.orderCount
            FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly x
            LEFT JOIN prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly a
              ON a.weekStartDate=x.weekStartDate
             AND a.visitorId=x.visitorId
            WHERE x.weekStartDate IN (SELECT weekStartDate FROM requiredWeeks)
        ),
        visitorWeekContexts AS (
            SELECT
                v.*,
                fview.filterProspectVsBase
            FROM visitorWeek v
            LATERAL VIEW explode(
                array_distinct(array(\'All\',v.prospectVsBase))
            ) fview AS filterProspectVsBase
        ),
        filterContexts AS (
            SELECT DISTINCT filterProspectVsBase
            FROM visitorWeekContexts
        ),
        weeklyWide AS (
            SELECT
              weekStartDate,
              max(weekEndDate) AS weekEndDate,
              filterProspectVsBase,
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
            FROM visitorWeekContexts
            GROUP BY weekStartDate,filterProspectVsBase
        ),
        weeklyCounts AS (
            SELECT
              weekStartDate,
              weekEndDate,
              filterProspectVsBase,
              metricName,
              metricValue
            FROM weeklyWide
            LATERAL VIEW stack(
              15,
              \'nbv\',               cast(nbv AS DOUBLE),
              \'sessionCount\',      cast(sessionCount AS DOUBLE),
              \'pageViews\',         cast(pageViews AS DOUBLE),
              \'nbvBuyFlow\',        cast(nbvBuyFlow AS DOUBLE),
              \'nbvConfigure\',      cast(nbvConfigure AS DOUBLE),
              \'nbvCheckoutStart\',  cast(nbvCheckoutStart AS DOUBLE),
              \'orders\',            cast(orders AS DOUBLE),
              \'ordersAcquisition\', cast(ordersAcquisition AS DOUBLE),
              \'ordersBase\',        cast(ordersBase AS DOUBLE),
              \'ordersUnassisted\',  cast(ordersUnassisted AS DOUBLE),
              \'ordersAssisted\',    cast(ordersAssisted AS DOUBLE),
              \'vrCalls\',           cast(vrCalls AS DOUBLE),
              \'vrChats\',           cast(vrChats AS DOUBLE),
              \'storeLocator\',      cast(storeLocator AS DOUBLE),
              \'orderCount\',        cast(orderCount AS DOUBLE)
            ) s AS metricName,metricValue
        ),
        weeklyIngredients AS (
            SELECT
              c.weekStartDate,
              c.weekEndDate,
              c.filterProspectVsBase,
              c.metricName,
              c.metricValue AS numeratorValue,
              cast(NULL AS DOUBLE) AS denominatorValue
            FROM weeklyCounts c
            INNER JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static m
              ON m.metricName=c.metricName
             AND m.metricKind=\'count\'
             AND m.isActive
            UNION ALL
            SELECT
              n.weekStartDate,
              max(n.weekEndDate) AS weekEndDate,
              n.filterProspectVsBase,
              r.metricName,
              max(CASE WHEN n.metricName=r.numeratorMetric THEN n.metricValue END) AS numeratorValue,
              max(CASE WHEN n.metricName=r.denominatorMetric THEN n.metricValue END) AS denominatorValue
            FROM weeklyCounts n
            INNER JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static r
              ON r.metricKind=\'ratio\'
             AND r.isActive
             AND n.metricName IN (r.numeratorMetric,r.denominatorMetric)
            GROUP BY n.weekStartDate,n.filterProspectVsBase,r.metricName
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
              cast(coalesce(fwa.fourWeekTrendWeekCount,0) AS INT) AS fourWeekTrendWeekCount,
              (ly.weekStartDate IS NOT NULL) AS sameWeekLyDataAvailable
            FROM targetCalendar t
            LEFT JOIN availableWeeks cur ON cur.weekStartDate=t.weekStartDate
            LEFT JOIN availableWeeks pw ON pw.weekStartDate=t.priorWeekStartDate
            LEFT JOIN fourWeekAvailability fwa ON fwa.targetWeekStartDate=t.weekStartDate
            LEFT JOIN availableWeeks ly ON ly.weekStartDate=t.sameWeekLastYearStartDate
        ),
        sourceRows AS (
            SELECT
              t.weekStartDate AS targetWeekStartDate,
              t.weekEndDate AS targetWeekEndDate,
              t.fiscalQuarterLabel,
              t.fiscalWeekCode,
              t.weekLabel,
              \'All\' AS filterLob,
              \'All\' AS filterPlatform,
              f.filterProspectVsBase,
              m.metricName,
              m.metricLabel,
              m.metricKind,
              m.displayFormat,
              m.changeUnit,
              CASE WHEN t.thisWeekDataAvailable THEN coalesce(cur.numeratorValue,0D) END AS thisWeekNumerator,
              CASE WHEN m.metricKind=\'ratio\' AND t.thisWeekDataAvailable THEN coalesce(cur.denominatorValue,0D) END AS thisWeekDenominator,
              CASE WHEN t.priorWeekDataAvailable THEN coalesce(pw.numeratorValue,0D) END AS priorWeekNumerator,
              CASE WHEN m.metricKind=\'ratio\' AND t.priorWeekDataAvailable THEN coalesce(pw.denominatorValue,0D) END AS priorWeekDenominator,
              CASE WHEN t.fourWeekTrendWeekCount>0 THEN coalesce(sum(fw.numeratorValue),0D) END AS fourWeekTrendNumerator,
              CASE WHEN m.metricKind=\'ratio\' AND t.fourWeekTrendWeekCount>0 THEN coalesce(sum(fw.denominatorValue),0D) END AS fourWeekTrendDenominator,
              CASE WHEN t.sameWeekLyDataAvailable THEN coalesce(ly.numeratorValue,0D) END AS sameWeekLyNumerator,
              CASE WHEN m.metricKind=\'ratio\' AND t.sameWeekLyDataAvailable THEN coalesce(ly.denominatorValue,0D) END AS sameWeekLyDenominator,
              t.thisWeekDataAvailable,
              t.priorWeekDataAvailable,
              t.fourWeekTrendWeekCount,
              t.sameWeekLyDataAvailable,
              cast(NULL AS DOUBLE) AS peerSetNumerator,
              cast(NULL AS DOUBLE) AS peerSetDenominator,
              current_timestamp() AS goldProcessedAt
            FROM targetWeeks t
            CROSS JOIN filterContexts f
            INNER JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static m
              ON m.isActive
            LEFT JOIN weeklyIngredients cur
              ON cur.weekStartDate=t.weekStartDate
             AND cur.filterProspectVsBase=f.filterProspectVsBase
             AND cur.metricName=m.metricName
            LEFT JOIN weeklyIngredients pw
              ON pw.weekStartDate=t.priorWeekStartDate
             AND pw.filterProspectVsBase=f.filterProspectVsBase
             AND pw.metricName=m.metricName
            LEFT JOIN weeklyIngredients fw
              ON fw.weekStartDate BETWEEN t.fourWeekAvgStartDate AND t.fourWeekAvgEndDate
             AND fw.filterProspectVsBase=f.filterProspectVsBase
             AND fw.metricName=m.metricName
            LEFT JOIN weeklyIngredients ly
              ON ly.weekStartDate=t.sameWeekLastYearStartDate
             AND ly.filterProspectVsBase=f.filterProspectVsBase
             AND ly.metricName=m.metricName
            GROUP BY
              t.weekStartDate,t.weekEndDate,t.fiscalQuarterLabel,t.fiscalWeekCode,t.weekLabel,
              f.filterProspectVsBase,
              m.metricName,m.metricLabel,m.metricKind,m.displayFormat,m.changeUnit,
              t.thisWeekDataAvailable,t.priorWeekDataAvailable,t.fourWeekTrendWeekCount,t.sameWeekLyDataAvailable,
              cur.numeratorValue,cur.denominatorValue,pw.numeratorValue,pw.denominatorValue,ly.numeratorValue,ly.denominatorValue
        )
INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long BY NAME
REPLACE WHERE targetWeekStartDate BETWEEN ',
            v_scopeStartSql,
            ' AND ',
            v_scopeEndSql,
            '
SELECT * FROM sourceRows'
        );
        EXECUTE IMMEDIATE v_writeSql;

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

--     targetWeekStartDate,filterLob,filterPlatform,filterProspectVsBase,metricName,COUNT(*) AS rowCount

-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long

-- WHERE targetWeekStartDate = DATE '2026-09-27'

-- GROUP BY targetWeekStartDate,filterLob,filterPlatform,filterProspectVsBase,metricName

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

--       AND filterProspectVsBase = 'All'

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

-- ============================================================================
-- NOTEBOOK CALL EXAMPLE
-- ============================================================================
-- as_of_date: Python datetime.date
-- weeks_to_rebuild: validated Python int >= 1
--
-- call_sql = f"""
--     CALL prdrzranalytics.lab42.sdi_sp_mip_gold_overviewMetricIngredientsByWeek_long(
--         p_asOfDate       => DATE '{as_of_date.isoformat()}',
--         p_weeksToRebuild => {int(weeks_to_rebuild)},
--         p_validateOnly   => FALSE
--     )
-- """
-- result = spark.sql(call_sql).collect()
-- ============================================================================

