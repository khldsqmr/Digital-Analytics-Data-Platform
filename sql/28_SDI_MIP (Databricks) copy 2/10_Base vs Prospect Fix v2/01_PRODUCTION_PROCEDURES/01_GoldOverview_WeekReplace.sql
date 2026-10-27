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



output:
targetWeekStartDate	targetWeekEndDate	fiscalQuarterLabel	fiscalWeekCode	weekLabel	filterLob	filterPlatform	filterProspectVsBase	metricName	metricLabel	metricKind	displayFormat	changeUnit	thisWeekNumerator	thisWeekDenominator	priorWeekNumerator	priorWeekDenominator	fourWeekTrendNumerator	fourWeekTrendDenominator	sameWeekLyNumerator	sameWeekLyDenominator	thisWeekDataAvailable	priorWeekDataAvailable	fourWeekTrendWeekCount	sameWeekLyDataAvailable	peerSetNumerator	peerSetDenominator	goldProcessedAt
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Unknown	buyFlowToConfigureRate	Buy flow → Configure	ratio	percent	pp	519	22604	567	28961	2295	190245	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Unknown	nbvCheckoutStart	NBV Checkout Start	count	number	pct	243	null	283	null	1067	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Base	storeLocator	Store Locator	count	number	pct	336	null	349	null	4145	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Prospect	nbvBuyFlowPerNbv	NBV Buy Flow / NBV	ratio	percent	pp	1582302	4136638	1516690	3946109	6926855	16829774	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Base	nbvConfigure	NBV Configure	count	number	pct	670556	null	564299	null	2757947	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Base	orderCount	Order Count	count	number	pct	360506	null	303452	null	1475668	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Prospect	configureToCheckoutRate	Configure → Checkout start	ratio	percent	pp	164025	402199	165127	377974	750879	1623464	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Prospect	sessionCount	Sessions	count	number	pct	5162783	null	4823387	null	20554697	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Unknown	checkoutToOrderRate	Checkout start → Order	ratio	percent	pp	82	243	87	283	257	1067	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Base	ordersAcquisition	Orders Acquisition	count	number	pct	11996	null	13302	null	64730	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Base	configureToCheckoutRate	Configure → Checkout start	ratio	percent	pp	397579	670556	334810	564299	1612125	2757947	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Prospect	ordersUnassisted	Unassisted Orders	count	number	pct	40643	null	42119	null	179985	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Prospect	vrChats	VR Chat	count	number	pct	62869	null	63502	null	280292	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Base	nbvBuyFlowPerNbv	NBV Buy Flow / NBV	ratio	percent	pp	3037739	15295766	2732408	15545456	13653847	64247096	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Unknown	vrCalls	VR Call	count	number	pct	0	null	0	null	0	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Prospect	ordersPerNbvBuyFlow	Orders / NBV Buy Flow	ratio	percent	pp	64589	1582302	61518	1516690	249587	6926855	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Prospect	buyFlowToConfigureRate	Buy flow → Configure	ratio	percent	pp	402199	1582302	377974	1516690	1623464	6926855	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Prospect	pageViews	Page Views	count	number	pct	33584591	null	33236326	null	143912293	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Prospect	ordersAssisted	Assisted Orders	count	number	pct	23946	null	19399	null	69602	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	All	buyFlowToConfigureRate	Buy flow → Configure	ratio	percent	pp	1073274	4642645	942840	4278059	4383706	20770947	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Base	checkoutToOrderRate	Checkout start → Order	ratio	percent	pp	316243	397579	267372	334810	1273810	1612125	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Base	ordersPerNbvBuyFlow	Orders / NBV Buy Flow	ratio	percent	pp	316243	3037739	267372	2732408	1273810	13653847	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Unknown	pageViews	Page Views	count	number	pct	11409945	null	12780184	null	44778325	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Base	ordersAssisted	Assisted Orders	count	number	pct	126668	null	122620	null	533043	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Prospect	nbvConfigure	NBV Configure	count	number	pct	402199	null	377974	null	1623464	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Prospect	orderCount	Order Count	count	number	pct	67840	null	64873	null	264351	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	All	configureToCheckoutRate	Configure → Checkout start	ratio	percent	pp	561847	1073274	500220	942840	2364071	4383706	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	All	checkoutToOrderRate	Checkout start → Order	ratio	percent	pp	380914	561847	328977	500220	1523654	2364071	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Prospect	nbvCheckoutStart	NBV Checkout Start	count	number	pct	164025	null	165127	null	750879	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Unknown	nbvBuyFlowPerNbv	NBV Buy Flow / NBV	ratio	percent	pp	22604	843097	28961	977142	190245	3486502	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Unknown	nbvConfigure	NBV Configure	count	number	pct	519	null	567	null	2295	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Prospect	nbvBuyFlow	NBV Buy Flow	count	number	pct	1582302	null	1516690	null	6926855	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Unknown	sessionCount	Sessions	count	number	pct	1059808	null	1190294	null	4244360	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Unknown	ordersAcquisition	Orders Acquisition	count	number	pct	0	null	0	null	0	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Prospect	ordersPerNbv	Orders / NBV	ratio	percent	pp	64589	4136638	61518	3946109	249587	16829774	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Unknown	orders	Orders	count	number	pct	82	null	87	null	257	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Unknown	ordersPerNbvBuyFlow	Orders / NBV Buy Flow	ratio	percent	pp	82	22604	87	28961	257	190245	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Prospect	orders	Orders	count	number	pct	64589	null	61518	null	249587	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Unknown	vrChats	VR Chat	count	number	pct	5849	null	5454	null	14306	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Base	orders	Orders	count	number	pct	316243	null	267372	null	1273810	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Prospect	nbv	Total NBV	count	number	pct	4136638	null	3946109	null	16829774	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Unknown	ordersUnassisted	Unassisted Orders	count	number	pct	10	null	7	null	22	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Base	ordersBase	Orders Base	count	number	pct	304247	null	254070	null	1209080	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Prospect	ordersBase	Orders Base	count	number	pct	757	null	895	null	2797	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Base	vrChats	VR Chat	count	number	pct	182687	null	177860	null	812764	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Base	buyFlowToConfigureRate	Buy flow → Configure	ratio	percent	pp	670556	3037739	564299	2732408	2757947	13653847	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Base	ordersUnassisted	Unassisted Orders	count	number	pct	189575	null	144752	null	740767	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Base	nbv	Total NBV	count	number	pct	15295766	null	15545456	null	64247096	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Unknown	nbvBuyFlow	NBV Buy Flow	count	number	pct	22604	null	28961	null	190245	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Unknown	configureToCheckoutRate	Configure → Checkout start	ratio	percent	pp	243	519	283	567	1067	2295	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Prospect	checkoutToOrderRate	Checkout start → Order	ratio	percent	pp	64589	164025	61518	165127	249587	750879	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Unknown	nbv	Total NBV	count	number	pct	843097	null	977142	null	3486502	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Unknown	ordersAssisted	Assisted Orders	count	number	pct	72	null	80	null	235	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Unknown	ordersPerNbv	Orders / NBV	ratio	percent	pp	82	843097	87	977142	257	3486502	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Base	pageViews	Page Views	count	number	pct	447945269	null	444953529	null	1789115946	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Unknown	storeLocator	Store Locator	count	number	pct	0	null	0	null	0	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Unknown	ordersBase	Orders Base	count	number	pct	82	null	87	null	257	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Prospect	storeLocator	Store Locator	count	number	pct	2960	null	2621	null	50528	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Prospect	ordersAcquisition	Orders Acquisition	count	number	pct	63832	null	60623	null	246790	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Prospect	vrCalls	VR Call	count	number	pct	70136	null	76353	null	356337	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Base	nbvBuyFlow	NBV Buy Flow	count	number	pct	3037739	null	2732408	null	13653847	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Base	nbvCheckoutStart	NBV Checkout Start	count	number	pct	397579	null	334810	null	1612125	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Unknown	orderCount	Order Count	count	number	pct	84	null	92	null	267	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Base	sessionCount	Sessions	count	number	pct	31698708	null	30868575	null	131444354	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Base	vrCalls	VR Call	count	number	pct	43738	null	45193	null	207713	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	Base	ordersPerNbv	Orders / NBV	ratio	percent	pp	316243	15295766	267372	15545456	1273810	64247096	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	All	nbvBuyFlow	NBV Buy Flow	count	number	pct	4642645	null	4278059	null	20770947	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	All	vrChats	VR Chat	count	number	pct	251405	null	246816	null	1107362	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	All	ordersPerNbv	Orders / NBV	ratio	percent	pp	380914	20275501	328977	20468707	1523654	84563372	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	All	nbv	Total NBV	count	number	pct	20275501	null	20468707	null	84563372	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	All	vrCalls	VR Call	count	number	pct	113874	null	121546	null	564050	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	All	ordersPerNbvBuyFlow	Orders / NBV Buy Flow	ratio	percent	pp	380914	4642645	328977	4278059	1523654	20770947	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	All	sessionCount	Sessions	count	number	pct	37921299	null	36882256	null	156243411	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	All	ordersAcquisition	Orders Acquisition	count	number	pct	75828	null	73925	null	311520	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	All	orderCount	Order Count	count	number	pct	428430	null	368417	null	1740286	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	All	storeLocator	Store Locator	count	number	pct	3296	null	2970	null	54673	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	All	nbvBuyFlowPerNbv	NBV Buy Flow / NBV	ratio	percent	pp	4642645	20275501	4278059	20468707	20770947	84563372	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	All	ordersUnassisted	Unassisted Orders	count	number	pct	230228	null	186878	null	920774	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	All	ordersBase	Orders Base	count	number	pct	305086	null	255052	null	1212134	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	All	nbvConfigure	NBV Configure	count	number	pct	1073274	null	942840	null	4383706	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	All	nbvCheckoutStart	NBV Checkout Start	count	number	pct	561847	null	500220	null	2364071	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	All	pageViews	Page Views	count	number	pct	492939805	null	490970039	null	1977806564	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	All	orders	Orders	count	number	pct	380914	null	328977	null	1523654	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-11	2026-10-17	2026 Q4	2026Q4W41	W41 · 11 to 17 Oct	All	All	All	ordersAssisted	Assisted Orders	count	number	pct	150686	null	142099	null	602880	null	null	null	true	true	4	false	null	null	2026-10-27T16:31:10.296+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Unknown	vrChats	VR Chat	count	number	pct	5454	null	3993	null	11407	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Base	ordersAcquisition	Orders Acquisition	count	number	pct	13302	null	15090	null	69456	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Unknown	pageViews	Page Views	count	number	pct	12780184	null	10956851	null	43109528	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Base	orders	Orders	count	number	pct	267372	null	307432	null	1330916	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Unknown	vrCalls	VR Call	count	number	pct	0	null	0	null	0	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Base	orderCount	Order Count	count	number	pct	303452	null	353944	null	1548887	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Unknown	nbvBuyFlowPerNbv	NBV Buy Flow / NBV	ratio	percent	pp	28961	977142	46435	827244	216373	3366454	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Base	checkoutToOrderRate	Checkout start → Order	ratio	percent	pp	267372	334810	307432	385441	1330916	1683835	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Unknown	storeLocator	Store Locator	count	number	pct	0	null	0	null	0	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Unknown	ordersAssisted	Assisted Orders	count	number	pct	80	null	65	null	203	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Base	nbvConfigure	NBV Configure	count	number	pct	564299	null	636006	null	2893135	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Unknown	ordersPerNbv	Orders / NBV	ratio	percent	pp	87	977142	74	827244	223	3366454	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Base	nbv	Total NBV	count	number	pct	15545456	null	16389787	null	64678356	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Unknown	ordersUnassisted	Unassisted Orders	count	number	pct	7	null	9	null	20	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Base	ordersBase	Orders Base	count	number	pct	254070	null	292342	null	1261460	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Unknown	sessionCount	Sessions	count	number	pct	1190294	null	1011758	null	4110069	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Unknown	nbvBuyFlow	NBV Buy Flow	count	number	pct	28961	null	46435	null	216373	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Base	nbvCheckoutStart	NBV Checkout Start	count	number	pct	334810	null	385441	null	1683835	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Unknown	buyFlowToConfigureRate	Buy flow → Configure	ratio	percent	pp	567	28961	613	46435	2223	216373	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Base	vrChats	VR Chat	count	number	pct	177860	null	215805	null	859716	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Unknown	ordersAcquisition	Orders Acquisition	count	number	pct	0	null	0	null	0	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Base	pageViews	Page Views	count	number	pct	444953529	null	456035408	null	1802530243	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Unknown	orders	Orders	count	number	pct	87	null	74	null	223	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Base	vrCalls	VR Call	count	number	pct	45193	null	52142	null	214383	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Unknown	orderCount	Order Count	count	number	pct	92	null	78	null	228	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Base	nbvBuyFlowPerNbv	NBV Buy Flow / NBV	ratio	percent	pp	2732408	15545456	3174184	16389787	14580921	64678356	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Unknown	checkoutToOrderRate	Checkout start → Order	ratio	percent	pp	87	283	74	290	223	1031	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Base	storeLocator	Store Locator	count	number	pct	349	null	780	null	6281	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Unknown	ordersPerNbvBuyFlow	Orders / NBV Buy Flow	ratio	percent	pp	87	28961	74	46435	223	216373	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Base	ordersAssisted	Assisted Orders	count	number	pct	122620	null	137529	null	521977	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Unknown	nbvConfigure	NBV Configure	count	number	pct	567	null	613	null	2223	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Base	ordersPerNbv	Orders / NBV	ratio	percent	pp	267372	15545456	307432	16389787	1330916	64678356	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Unknown	nbv	Total NBV	count	number	pct	977142	null	827244	null	3366454	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Base	ordersUnassisted	Unassisted Orders	count	number	pct	144752	null	169903	null	808939	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Unknown	ordersBase	Orders Base	count	number	pct	87	null	74	null	223	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Base	sessionCount	Sessions	count	number	pct	30868575	null	33678124	null	134104745	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Unknown	configureToCheckoutRate	Configure → Checkout start	ratio	percent	pp	283	567	290	613	1031	2223	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Base	nbvBuyFlow	NBV Buy Flow	count	number	pct	2732408	null	3174184	null	14580921	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Unknown	nbvCheckoutStart	NBV Checkout Start	count	number	pct	283	null	290	null	1031	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Prospect	ordersAssisted	Assisted Orders	count	number	pct	19399	null	17597	null	64588	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	All	nbvConfigure	NBV Configure	count	number	pct	942840	null	1017523	null	4491307	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Prospect	ordersPerNbv	Orders / NBV	ratio	percent	pp	61518	3946109	63527	4167983	246518	17040399	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	All	nbv	Total NBV	count	number	pct	20468707	null	21385014	null	85085209	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Prospect	ordersUnassisted	Unassisted Orders	count	number	pct	42119	null	45930	null	181930	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	All	ordersBase	Orders Base	count	number	pct	255052	null	293344	null	1264087	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Prospect	sessionCount	Sessions	count	number	pct	4823387	null	5078314	null	20821689	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	All	configureToCheckoutRate	Configure → Checkout start	ratio	percent	pp	500220	942840	575914	1017523	2448835	4491307	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Prospect	nbvBuyFlow	NBV Buy Flow	count	number	pct	1516690	null	1697294	null	7151993	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	All	nbvCheckoutStart	NBV Checkout Start	count	number	pct	500220	null	575914	null	2448835	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Prospect	buyFlowToConfigureRate	Buy flow → Configure	ratio	percent	pp	377974	1516690	380904	1697294	1595949	7151993	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	All	vrChats	VR Chat	count	number	pct	246816	null	289438	null	1159730	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Prospect	ordersAcquisition	Orders Acquisition	count	number	pct	60623	null	62599	null	244114	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	All	pageViews	Page Views	count	number	pct	490970039	null	502943976	null	1992934256	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Prospect	orders	Orders	count	number	pct	61518	null	63527	null	246518	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	All	vrCalls	VR Call	count	number	pct	121546	null	138438	null	587345	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Prospect	orderCount	Order Count	count	number	pct	64873	null	67039	null	262020	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Prospect	checkoutToOrderRate	Checkout start → Order	ratio	percent	pp	61518	165127	63527	190183	246518	763969	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	All	storeLocator	Store Locator	count	number	pct	2970	null	9030	null	74396	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Prospect	ordersPerNbvBuyFlow	Orders / NBV Buy Flow	ratio	percent	pp	61518	1516690	63527	1697294	246518	7151993	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	All	ordersAssisted	Assisted Orders	count	number	pct	142099	null	155191	null	586768	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Prospect	nbvConfigure	NBV Configure	count	number	pct	377974	null	380904	null	1595949	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	All	ordersPerNbv	Orders / NBV	ratio	percent	pp	328977	20468707	371033	21385014	1577657	85085209	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Prospect	nbv	Total NBV	count	number	pct	3946109	null	4167983	null	17040399	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	All	ordersUnassisted	Unassisted Orders	count	number	pct	186878	null	215842	null	990889	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Prospect	ordersBase	Orders Base	count	number	pct	895	null	928	null	2404	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	All	sessionCount	Sessions	count	number	pct	36882256	null	39768196	null	159036503	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Prospect	configureToCheckoutRate	Configure → Checkout start	ratio	percent	pp	165127	377974	190183	380904	763969	1595949	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	All	nbvBuyFlow	NBV Buy Flow	count	number	pct	4278059	null	4917913	null	21949287	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Prospect	nbvCheckoutStart	NBV Checkout Start	count	number	pct	165127	null	190183	null	763969	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	All	buyFlowToConfigureRate	Buy flow → Configure	ratio	percent	pp	942840	4278059	1017523	4917913	4491307	21949287	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Prospect	vrChats	VR Chat	count	number	pct	63502	null	69640	null	288607	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	All	ordersAcquisition	Orders Acquisition	count	number	pct	73925	null	77689	null	313570	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Prospect	pageViews	Page Views	count	number	pct	33236326	null	35951717	null	147294485	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	All	orders	Orders	count	number	pct	328977	null	371033	null	1577657	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Prospect	vrCalls	VR Call	count	number	pct	76353	null	86296	null	372962	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	All	orderCount	Order Count	count	number	pct	368417	null	421061	null	1811135	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Prospect	nbvBuyFlowPerNbv	NBV Buy Flow / NBV	ratio	percent	pp	1516690	3946109	1697294	4167983	7151993	17040399	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	All	checkoutToOrderRate	Checkout start → Order	ratio	percent	pp	328977	500220	371033	575914	1577657	2448835	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Prospect	storeLocator	Store Locator	count	number	pct	2621	null	8250	null	68115	null	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Base	ordersPerNbvBuyFlow	Orders / NBV Buy Flow	ratio	percent	pp	267372	2732408	307432	3174184	1330916	14580921	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Base	configureToCheckoutRate	Configure → Checkout start	ratio	percent	pp	334810	564299	385441	636006	1683835	2893135	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	Base	buyFlowToConfigureRate	Buy flow → Configure	ratio	percent	pp	564299	2732408	636006	3174184	2893135	14580921	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	All	nbvBuyFlowPerNbv	NBV Buy Flow / NBV	ratio	percent	pp	4278059	20468707	4917913	21385014	21949287	85085209	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-10-04	2026-10-10	2026 Q4	2026Q4W40	W40 · 4 to 10 Oct	All	All	All	ordersPerNbvBuyFlow	Orders / NBV Buy Flow	ratio	percent	pp	328977	4278059	371033	4917913	1577657	21949287	null	null	true	true	4	false	null	null	2026-10-27T16:32:28.080+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Unknown	ordersUnassisted	Unassisted Orders	count	number	pct	9	null	4	null	11	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Base	ordersAcquisition	Orders Acquisition	count	number	pct	15090	null	17279	null	54366	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Unknown	buyFlowToConfigureRate	Buy flow → Configure	ratio	percent	pp	613	46435	557	58304	1610	169938	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Base	orders	Orders	count	number	pct	307432	null	337840	null	1023484	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Unknown	vrChats	VR Chat	count	number	pct	3993	null	2458	null	7414	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Base	ordersBase	Orders Base	count	number	pct	292342	null	320561	null	969118	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Unknown	ordersPerNbvBuyFlow	Orders / NBV Buy Flow	ratio	percent	pp	74	46435	42	58304	149	169938	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Base	pageViews	Page Views	count	number	pct	456035408	null	446071564	null	1346494835	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Unknown	nbvCheckoutStart	NBV Checkout Start	count	number	pct	290	null	241	null	741	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Base	ordersPerNbv	Orders / NBV	ratio	percent	pp	307432	16389787	337840	16071462	1023484	48288569	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Unknown	nbv	Total NBV	count	number	pct	827244	null	845048	null	2539210	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Base	storeLocator	Store Locator	count	number	pct	780	null	1542	null	5501	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Unknown	nbvBuyFlowPerNbv	NBV Buy Flow / NBV	ratio	percent	pp	46435	827244	58304	845048	169938	2539210	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Base	nbvBuyFlow	NBV Buy Flow	count	number	pct	3174184	null	3630659	null	11406737	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Unknown	ordersAssisted	Assisted Orders	count	number	pct	65	null	38	null	138	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Base	nbvConfigure	NBV Configure	count	number	pct	636006	null	724867	null	2257129	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Unknown	sessionCount	Sessions	count	number	pct	1011758	null	1027111	null	3098311	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Base	configureToCheckoutRate	Configure → Checkout start	ratio	percent	pp	385441	636006	427182	724867	1298394	2257129	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Unknown	vrCalls	VR Call	count	number	pct	0	null	0	null	0	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Base	orderCount	Order Count	count	number	pct	353944	null	395480	null	1194943	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Unknown	checkoutToOrderRate	Checkout start → Order	ratio	percent	pp	74	290	42	241	149	741	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Base	ordersUnassisted	Unassisted Orders	count	number	pct	169903	null	197281	null	639036	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Unknown	ordersAcquisition	Orders Acquisition	count	number	pct	0	null	0	null	0	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Base	buyFlowToConfigureRate	Buy flow → Configure	ratio	percent	pp	636006	3174184	724867	3630659	2257129	11406737	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Unknown	orders	Orders	count	number	pct	74	null	42	null	149	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Base	vrChats	VR Chat	count	number	pct	215805	null	219284	null	643911	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Unknown	ordersBase	Orders Base	count	number	pct	74	null	42	null	149	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Base	ordersPerNbvBuyFlow	Orders / NBV Buy Flow	ratio	percent	pp	307432	3174184	337840	3630659	1023484	11406737	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Unknown	pageViews	Page Views	count	number	pct	10956851	null	10463903	null	32152677	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Base	nbvCheckoutStart	NBV Checkout Start	count	number	pct	385441	null	427182	null	1298394	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Unknown	ordersPerNbv	Orders / NBV	ratio	percent	pp	74	827244	42	845048	149	2539210	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Base	nbv	Total NBV	count	number	pct	16389787	null	16071462	null	48288569	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Unknown	storeLocator	Store Locator	count	number	pct	0	null	0	null	0	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Base	nbvBuyFlowPerNbv	NBV Buy Flow / NBV	ratio	percent	pp	3174184	16389787	3630659	16071462	11406737	48288569	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Unknown	nbvBuyFlow	NBV Buy Flow	count	number	pct	46435	null	58304	null	169938	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Base	ordersAssisted	Assisted Orders	count	number	pct	137529	null	140559	null	384448	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Unknown	nbvConfigure	NBV Configure	count	number	pct	613	null	557	null	1610	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Base	sessionCount	Sessions	count	number	pct	33678124	null	33368664	null	100426621	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Unknown	configureToCheckoutRate	Configure → Checkout start	ratio	percent	pp	290	613	241	557	741	1610	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Base	vrCalls	VR Call	count	number	pct	52142	null	55800	null	162241	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Unknown	orderCount	Order Count	count	number	pct	78	null	42	null	150	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Base	checkoutToOrderRate	Checkout start → Order	ratio	percent	pp	307432	385441	337840	427182	1023484	1298394	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	All	storeLocator	Store Locator	count	number	pct	9030	null	22769	null	65366	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	All	nbvBuyFlow	NBV Buy Flow	count	number	pct	4917913	null	5492171	null	17031374	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	All	nbvConfigure	NBV Configure	count	number	pct	1017523	null	1159298	null	3473784	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	All	configureToCheckoutRate	Configure → Checkout start	ratio	percent	pp	575914	1017523	628179	1159298	1872921	3473784	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	All	orderCount	Order Count	count	number	pct	421061	null	462907	null	1390074	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Prospect	checkoutToOrderRate	Checkout start → Order	ratio	percent	pp	63527	190183	63378	200756	182991	573786	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	All	ordersUnassisted	Unassisted Orders	count	number	pct	215842	null	244160	null	775047	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	All	buyFlowToConfigureRate	Buy flow → Configure	ratio	percent	pp	1017523	4917913	1159298	5492171	3473784	17031374	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	All	vrChats	VR Chat	count	number	pct	289438	null	295863	null	870292	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	All	nbvCheckoutStart	NBV Checkout Start	count	number	pct	575914	null	628179	null	1872921	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Prospect	ordersPerNbv	Orders / NBV	ratio	percent	pp	63527	4167983	63378	4371886	182991	12872416	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	All	nbv	Total NBV	count	number	pct	21385014	null	21288396	null	63700195	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	All	ordersAssisted	Assisted Orders	count	number	pct	155191	null	157100	null	431577	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	All	sessionCount	Sessions	count	number	pct	39768196	null	39767731	null	119268307	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Prospect	configureToCheckoutRate	Configure → Checkout start	ratio	percent	pp	190183	380904	200756	433874	573786	1215045	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	All	vrCalls	VR Call	count	number	pct	138438	null	153651	null	448907	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	All	ordersAcquisition	Orders Acquisition	count	number	pct	77689	null	80205	null	235881	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Prospect	buyFlowToConfigureRate	Buy flow → Configure	ratio	percent	pp	380904	1697294	433874	1803208	1215045	5454699	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	All	orders	Orders	count	number	pct	371033	null	401260	null	1206624	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	All	ordersBase	Orders Base	count	number	pct	293344	null	321055	null	970743	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Prospect	ordersPerNbvBuyFlow	Orders / NBV Buy Flow	ratio	percent	pp	63527	1697294	63378	1803208	182991	5454699	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	All	pageViews	Page Views	count	number	pct	502943976	null	493820509	null	1489990280	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	All	ordersPerNbv	Orders / NBV	ratio	percent	pp	371033	21385014	401260	21288396	1206624	63700195	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Prospect	nbv	Total NBV	count	number	pct	4167983	null	4371886	null	12872416	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Prospect	nbvBuyFlowPerNbv	NBV Buy Flow / NBV	ratio	percent	pp	1697294	4167983	1803208	4371886	5454699	12872416	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Prospect	ordersAssisted	Assisted Orders	count	number	pct	17597	null	16503	null	46991	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Prospect	sessionCount	Sessions	count	number	pct	5078314	null	5371956	null	15743375	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Prospect	vrCalls	VR Call	count	number	pct	86296	null	97851	null	286666	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Prospect	ordersAcquisition	Orders Acquisition	count	number	pct	62599	null	62926	null	181515	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Prospect	orders	Orders	count	number	pct	63527	null	63378	null	182991	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Prospect	ordersBase	Orders Base	count	number	pct	928	null	452	null	1476	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	All	ordersPerNbvBuyFlow	Orders / NBV Buy Flow	ratio	percent	pp	371033	4917913	401260	5492171	1206624	17031374	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Prospect	pageViews	Page Views	count	number	pct	35951717	null	37285042	null	111342768	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Prospect	storeLocator	Store Locator	count	number	pct	8250	null	21227	null	59865	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	All	nbvBuyFlowPerNbv	NBV Buy Flow / NBV	ratio	percent	pp	4917913	21385014	5492171	21288396	17031374	63700195	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Prospect	nbvBuyFlow	NBV Buy Flow	count	number	pct	1697294	null	1803208	null	5454699	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Prospect	nbvConfigure	NBV Configure	count	number	pct	380904	null	433874	null	1215045	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Prospect	orderCount	Order Count	count	number	pct	67039	null	67385	null	194981	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	All	checkoutToOrderRate	Checkout start → Order	ratio	percent	pp	371033	575914	401260	628179	1206624	1872921	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Prospect	ordersUnassisted	Unassisted Orders	count	number	pct	45930	null	46875	null	136000	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Prospect	vrChats	VR Chat	count	number	pct	69640	null	74121	null	218967	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-27	2026-10-03	2026 Q3	2026Q3W39	W39 · 27 Sep to 3 Oct	All	All	Prospect	nbvCheckoutStart	NBV Checkout Start	count	number	pct	190183	null	200756	null	573786	null	null	null	true	true	3	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	All	storeLocator	Store Locator	count	number	pct	22769	null	19904	null	42597	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	All	nbvBuyFlow	NBV Buy Flow	count	number	pct	5492171	null	6082804	null	11539203	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	All	nbvConfigure	NBV Configure	count	number	pct	1159298	null	1264045	null	2314486	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	All	orderCount	Order Count	count	number	pct	462907	null	487901	null	927167	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Prospect	checkoutToOrderRate	Checkout start → Order	ratio	percent	pp	63378	200756	61164	194813	119613	373030	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	All	ordersUnassisted	Unassisted Orders	count	number	pct	244160	null	273894	null	530887	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	All	buyFlowToConfigureRate	Buy flow → Configure	ratio	percent	pp	1159298	5492171	1264045	6082804	2314486	11539203	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	All	vrChats	VR Chat	count	number	pct	295863	null	275245	null	574429	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	All	nbvCheckoutStart	NBV Checkout Start	count	number	pct	628179	null	659758	null	1244742	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	All	nbv	Total NBV	count	number	pct	21288396	null	21421255	null	42411799	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	All	ordersAssisted	Assisted Orders	count	number	pct	157100	null	148490	null	274477	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	All	sessionCount	Sessions	count	number	pct	39767731	null	39825228	null	79500576	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Prospect	configureToCheckoutRate	Configure → Checkout start	ratio	percent	pp	200756	433874	194813	430712	373030	781171	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	All	vrCalls	VR Call	count	number	pct	153651	null	150415	null	295256	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	All	ordersAcquisition	Orders Acquisition	count	number	pct	80205	null	79701	null	155676	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	All	orders	Orders	count	number	pct	401260	null	422384	null	805364	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	All	ordersBase	Orders Base	count	number	pct	321055	null	342683	null	649688	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Prospect	ordersPerNbvBuyFlow	Orders / NBV Buy Flow	ratio	percent	pp	63378	1803208	61164	1909663	119613	3651491	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	All	pageViews	Page Views	count	number	pct	493820509	null	490072040	null	996169771	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	All	ordersPerNbv	Orders / NBV	ratio	percent	pp	401260	21288396	422384	21421255	805364	42411799	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Unknown	ordersUnassisted	Unassisted Orders	count	number	pct	4	null	2	null	7	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Base	ordersAcquisition	Orders Acquisition	count	number	pct	17279	null	19059	null	37087	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Unknown	buyFlowToConfigureRate	Buy flow → Configure	ratio	percent	pp	557	58304	558	56545	1053	111634	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Base	orders	Orders	count	number	pct	337840	null	361166	null	685644	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Unknown	vrChats	VR Chat	count	number	pct	2458	null	2401	null	4956	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Base	ordersBase	Orders Base	count	number	pct	320561	null	342107	null	648557	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Unknown	ordersPerNbvBuyFlow	Orders / NBV Buy Flow	ratio	percent	pp	42	58304	54	56545	107	111634	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Base	pageViews	Page Views	count	number	pct	446071564	null	442055445	null	900423271	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Unknown	nbvCheckoutStart	NBV Checkout Start	count	number	pct	241	null	253	null	500	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Base	ordersPerNbv	Orders / NBV	ratio	percent	pp	337840	16071462	361166	16240391	685644	32217107	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Unknown	nbv	Total NBV	count	number	pct	845048	null	837068	null	1694162	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Base	storeLocator	Store Locator	count	number	pct	1542	null	1474	null	3959	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Unknown	nbvBuyFlowPerNbv	NBV Buy Flow / NBV	ratio	percent	pp	58304	845048	56545	837068	111634	1694162	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Base	nbvBuyFlow	NBV Buy Flow	count	number	pct	3630659	null	4116596	null	7776078	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Unknown	ordersAssisted	Assisted Orders	count	number	pct	38	null	52	null	100	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Base	nbvConfigure	NBV Configure	count	number	pct	724867	null	832775	null	1532262	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Unknown	sessionCount	Sessions	count	number	pct	1027111	null	1015197	null	2071200	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Base	configureToCheckoutRate	Configure → Checkout start	ratio	percent	pp	427182	724867	464692	832775	871212	1532262	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Unknown	vrCalls	VR Call	count	number	pct	0	null	0	null	0	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Base	orderCount	Order Count	count	number	pct	395480	null	422792	null	799463	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Base	ordersUnassisted	Unassisted Orders	count	number	pct	197281	null	228831	null	441755	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Unknown	ordersAcquisition	Orders Acquisition	count	number	pct	0	null	0	null	0	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Base	buyFlowToConfigureRate	Buy flow → Configure	ratio	percent	pp	724867	3630659	832775	4116596	1532262	7776078	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Unknown	orders	Orders	count	number	pct	42	null	54	null	107	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Base	vrChats	VR Chat	count	number	pct	219284	null	199815	null	424627	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Unknown	ordersBase	Orders Base	count	number	pct	42	null	54	null	107	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Unknown	pageViews	Page Views	count	number	pct	10463903	null	10577387	null	21688774	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Base	nbvCheckoutStart	NBV Checkout Start	count	number	pct	427182	null	464692	null	871212	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Unknown	ordersPerNbv	Orders / NBV	ratio	percent	pp	42	845048	54	837068	107	1694162	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Base	nbv	Total NBV	count	number	pct	16071462	null	16240391	null	32217107	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Unknown	storeLocator	Store Locator	count	number	pct	0	null	0	null	0	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Base	nbvBuyFlowPerNbv	NBV Buy Flow / NBV	ratio	percent	pp	3630659	16071462	4116596	16240391	7776078	32217107	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Unknown	nbvBuyFlow	NBV Buy Flow	count	number	pct	58304	null	56545	null	111634	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Base	ordersAssisted	Assisted Orders	count	number	pct	140559	null	132335	null	243889	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Unknown	nbvConfigure	NBV Configure	count	number	pct	557	null	558	null	1053	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Base	sessionCount	Sessions	count	number	pct	33368664	null	33528991	null	67057957	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Base	vrCalls	VR Call	count	number	pct	55800	null	54578	null	106441	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Unknown	orderCount	Order Count	count	number	pct	42	null	55	null	108	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Base	checkoutToOrderRate	Checkout start → Order	ratio	percent	pp	337840	427182	361166	464692	685644	871212	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Prospect	nbv	Total NBV	count	number	pct	4371886	null	4343796	null	8500530	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Prospect	nbvBuyFlowPerNbv	NBV Buy Flow / NBV	ratio	percent	pp	1803208	4371886	1909663	4343796	3651491	8500530	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Prospect	ordersAssisted	Assisted Orders	count	number	pct	16503	null	16103	null	30488	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Prospect	sessionCount	Sessions	count	number	pct	5371956	null	5281040	null	10371419	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Prospect	vrCalls	VR Call	count	number	pct	97851	null	95837	null	188815	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Prospect	ordersAcquisition	Orders Acquisition	count	number	pct	62926	null	60642	null	118589	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Prospect	orders	Orders	count	number	pct	63378	null	61164	null	119613	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Prospect	ordersBase	Orders Base	count	number	pct	452	null	522	null	1024	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	All	ordersPerNbvBuyFlow	Orders / NBV Buy Flow	ratio	percent	pp	401260	5492171	422384	6082804	805364	11539203	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Prospect	pageViews	Page Views	count	number	pct	37285042	null	37439208	null	74057726	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Prospect	ordersPerNbv	Orders / NBV	ratio	percent	pp	63378	4371886	61164	4343796	119613	8500530	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Prospect	storeLocator	Store Locator	count	number	pct	21227	null	18430	null	38638	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	All	nbvBuyFlowPerNbv	NBV Buy Flow / NBV	ratio	percent	pp	5492171	21288396	6082804	21421255	11539203	42411799	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Prospect	nbvBuyFlow	NBV Buy Flow	count	number	pct	1803208	null	1909663	null	3651491	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Prospect	nbvConfigure	NBV Configure	count	number	pct	433874	null	430712	null	781171	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Prospect	orderCount	Order Count	count	number	pct	67385	null	65054	null	127596	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	All	checkoutToOrderRate	Checkout start → Order	ratio	percent	pp	401260	628179	422384	659758	805364	1244742	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Prospect	ordersUnassisted	Unassisted Orders	count	number	pct	46875	null	45061	null	89125	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Prospect	buyFlowToConfigureRate	Buy flow → Configure	ratio	percent	pp	433874	1803208	430712	1909663	781171	3651491	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Prospect	vrChats	VR Chat	count	number	pct	74121	null	73029	null	144846	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Prospect	nbvCheckoutStart	NBV Checkout Start	count	number	pct	200756	null	194813	null	373030	null	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Unknown	checkoutToOrderRate	Checkout start → Order	ratio	percent	pp	42	241	54	253	107	500	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Unknown	configureToCheckoutRate	Configure → Checkout start	ratio	percent	pp	241	557	253	558	500	1053	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	All	configureToCheckoutRate	Configure → Checkout start	ratio	percent	pp	628179	1159298	659758	1264045	1244742	2314486	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-20	2026-09-26	2026 Q3	2026Q3W38	W38 · 20 to 26 Sep	All	All	Base	ordersPerNbvBuyFlow	Orders / NBV Buy Flow	ratio	percent	pp	337840	3630659	361166	4116596	685644	7776078	null	null	true	true	2	false	null	null	2026-10-27T16:40:12.060+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Base	ordersBase	Orders Base	count	number	pct	342107	null	306450	null	306450	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	All	storeLocator	Store Locator	count	number	pct	19904	null	22693	null	22693	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Unknown	vrCalls	VR Call	count	number	pct	0	null	0	null	0	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	All	checkoutToOrderRate	Checkout start → Order	ratio	percent	pp	422384	659758	382980	584984	382980	584984	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Base	storeLocator	Store Locator	count	number	pct	1474	null	2485	null	2485	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	All	nbvBuyFlowPerNbv	NBV Buy Flow / NBV	ratio	percent	pp	6082804	21421255	5456399	20990544	5456399	20990544	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	All	sessionCount	Sessions	count	number	pct	39825228	null	39675348	null	39675348	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Prospect	configureToCheckoutRate	Configure → Checkout start	ratio	percent	pp	194813	430712	178217	350459	178217	350459	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Prospect	vrChats	VR Chat	count	number	pct	73029	null	71817	null	71817	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Base	nbvCheckoutStart	NBV Checkout Start	count	number	pct	464692	null	406520	null	406520	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Unknown	checkoutToOrderRate	Checkout start → Order	ratio	percent	pp	54	253	53	247	53	247	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Unknown	ordersBase	Orders Base	count	number	pct	54	null	53	null	53	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Prospect	nbv	Total NBV	count	number	pct	4343796	null	4156734	null	4156734	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Base	configureToCheckoutRate	Configure → Checkout start	ratio	percent	pp	464692	832775	406520	699487	406520	699487	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Base	orderCount	Order Count	count	number	pct	422792	null	376671	null	376671	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Unknown	nbvBuyFlow	NBV Buy Flow	count	number	pct	56545	null	55089	null	55089	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Base	sessionCount	Sessions	count	number	pct	33528991	null	33528966	null	33528966	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Prospect	checkoutToOrderRate	Checkout start → Order	ratio	percent	pp	61164	194813	58449	178217	58449	178217	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Unknown	sessionCount	Sessions	count	number	pct	1015197	null	1056003	null	1056003	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Unknown	ordersUnassisted	Unassisted Orders	count	number	pct	2	null	5	null	5	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Base	vrChats	VR Chat	count	number	pct	199815	null	224812	null	224812	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Prospect	pageViews	Page Views	count	number	pct	37439208	null	36618518	null	36618518	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Base	nbvBuyFlow	NBV Buy Flow	count	number	pct	4116596	null	3659482	null	3659482	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	All	ordersAssisted	Assisted Orders	count	number	pct	148490	null	125987	null	125987	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	All	ordersAcquisition	Orders Acquisition	count	number	pct	79701	null	75975	null	75975	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	All	ordersUnassisted	Unassisted Orders	count	number	pct	273894	null	256993	null	256993	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	All	ordersPerNbv	Orders / NBV	ratio	percent	pp	422384	21421255	382980	20990544	382980	20990544	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	All	configureToCheckoutRate	Configure → Checkout start	ratio	percent	pp	659758	1264045	584984	1050441	584984	1050441	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Prospect	nbvCheckoutStart	NBV Checkout Start	count	number	pct	194813	null	178217	null	178217	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Prospect	ordersPerNbvBuyFlow	Orders / NBV Buy Flow	ratio	percent	pp	61164	1909663	58449	1741828	58449	1741828	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Prospect	storeLocator	Store Locator	count	number	pct	18430	null	20208	null	20208	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	All	nbvConfigure	NBV Configure	count	number	pct	1264045	null	1050441	null	1050441	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Base	checkoutToOrderRate	Checkout start → Order	ratio	percent	pp	361166	464692	324478	406520	324478	406520	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Base	nbv	Total NBV	count	number	pct	16240391	null	15976716	null	15976716	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Base	orders	Orders	count	number	pct	361166	null	324478	null	324478	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Prospect	orders	Orders	count	number	pct	61164	null	58449	null	58449	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	All	buyFlowToConfigureRate	Buy flow → Configure	ratio	percent	pp	1264045	6082804	1050441	5456399	1050441	5456399	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Prospect	nbvConfigure	NBV Configure	count	number	pct	430712	null	350459	null	350459	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Prospect	vrCalls	VR Call	count	number	pct	95837	null	92978	null	92978	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	All	nbvCheckoutStart	NBV Checkout Start	count	number	pct	659758	null	584984	null	584984	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Base	pageViews	Page Views	count	number	pct	442055445	null	458367826	null	458367826	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Unknown	orderCount	Order Count	count	number	pct	55	null	53	null	53	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Unknown	ordersPerNbv	Orders / NBV	ratio	percent	pp	54	837068	53	857094	53	857094	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Base	ordersPerNbv	Orders / NBV	ratio	percent	pp	361166	16240391	324478	15976716	324478	15976716	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Unknown	buyFlowToConfigureRate	Buy flow → Configure	ratio	percent	pp	558	56545	495	55089	495	55089	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Prospect	nbvBuyFlow	NBV Buy Flow	count	number	pct	1909663	null	1741828	null	1741828	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	All	nbvBuyFlow	NBV Buy Flow	count	number	pct	6082804	null	5456399	null	5456399	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Unknown	orders	Orders	count	number	pct	54	null	53	null	53	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	All	pageViews	Page Views	count	number	pct	490072040	null	506097731	null	506097731	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Prospect	ordersPerNbv	Orders / NBV	ratio	percent	pp	61164	4343796	58449	4156734	58449	4156734	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Unknown	ordersPerNbvBuyFlow	Orders / NBV Buy Flow	ratio	percent	pp	54	56545	53	55089	53	55089	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Unknown	nbvBuyFlowPerNbv	NBV Buy Flow / NBV	ratio	percent	pp	56545	837068	55089	857094	55089	857094	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Prospect	ordersBase	Orders Base	count	number	pct	522	null	502	null	502	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	All	ordersBase	Orders Base	count	number	pct	342683	null	307005	null	307005	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Prospect	orderCount	Order Count	count	number	pct	65054	null	62542	null	62542	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Unknown	configureToCheckoutRate	Configure → Checkout start	ratio	percent	pp	253	558	247	495	247	495	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Base	nbvBuyFlowPerNbv	NBV Buy Flow / NBV	ratio	percent	pp	4116596	16240391	3659482	15976716	3659482	15976716	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Prospect	ordersAssisted	Assisted Orders	count	number	pct	16103	null	14385	null	14385	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	All	orders	Orders	count	number	pct	422384	null	382980	null	382980	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Base	ordersAcquisition	Orders Acquisition	count	number	pct	19059	null	18028	null	18028	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Unknown	ordersAcquisition	Orders Acquisition	count	number	pct	0	null	0	null	0	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Prospect	ordersUnassisted	Unassisted Orders	count	number	pct	45061	null	44064	null	44064	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Base	ordersAssisted	Assisted Orders	count	number	pct	132335	null	111554	null	111554	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Base	nbvConfigure	NBV Configure	count	number	pct	832775	null	699487	null	699487	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Base	buyFlowToConfigureRate	Buy flow → Configure	ratio	percent	pp	832775	4116596	699487	3659482	699487	3659482	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Prospect	nbvBuyFlowPerNbv	NBV Buy Flow / NBV	ratio	percent	pp	1909663	4343796	1741828	4156734	1741828	4156734	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	All	ordersPerNbvBuyFlow	Orders / NBV Buy Flow	ratio	percent	pp	422384	6082804	382980	5456399	382980	5456399	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Base	ordersUnassisted	Unassisted Orders	count	number	pct	228831	null	212924	null	212924	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Unknown	vrChats	VR Chat	count	number	pct	2401	null	2555	null	2555	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Unknown	nbvConfigure	NBV Configure	count	number	pct	558	null	495	null	495	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	All	vrCalls	VR Call	count	number	pct	150415	null	144841	null	144841	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Prospect	sessionCount	Sessions	count	number	pct	5281040	null	5090379	null	5090379	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	All	vrChats	VR Chat	count	number	pct	275245	null	299184	null	299184	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Base	vrCalls	VR Call	count	number	pct	54578	null	51863	null	51863	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Base	ordersPerNbvBuyFlow	Orders / NBV Buy Flow	ratio	percent	pp	361166	4116596	324478	3659482	324478	3659482	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Unknown	storeLocator	Store Locator	count	number	pct	0	null	0	null	0	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Unknown	nbv	Total NBV	count	number	pct	837068	null	857094	null	857094	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Unknown	ordersAssisted	Assisted Orders	count	number	pct	52	null	48	null	48	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	All	orderCount	Order Count	count	number	pct	487901	null	439266	null	439266	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	All	nbv	Total NBV	count	number	pct	21421255	null	20990544	null	20990544	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Unknown	pageViews	Page Views	count	number	pct	10577387	null	11111387	null	11111387	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Prospect	ordersAcquisition	Orders Acquisition	count	number	pct	60642	null	57947	null	57947	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Unknown	nbvCheckoutStart	NBV Checkout Start	count	number	pct	253	null	247	null	247	null	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-13	2026-09-19	2026 Q3	2026Q3W37	W37 · 13 to 19 Sep	All	All	Prospect	buyFlowToConfigureRate	Buy flow → Configure	ratio	percent	pp	430712	1909663	350459	1741828	350459	1741828	null	null	true	true	1	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	All	orders	Orders	count	number	pct	382980	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Base	nbvCheckoutStart	NBV Checkout Start	count	number	pct	406520	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	All	nbvBuyFlowPerNbv	NBV Buy Flow / NBV	ratio	percent	pp	5456399	20990544	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	All	ordersUnassisted	Unassisted Orders	count	number	pct	256993	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Prospect	sessionCount	Sessions	count	number	pct	5090379	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	All	vrChats	VR Chat	count	number	pct	299184	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Unknown	vrCalls	VR Call	count	number	pct	0	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Base	ordersBase	Orders Base	count	number	pct	306450	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Unknown	ordersUnassisted	Unassisted Orders	count	number	pct	5	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Unknown	nbv	Total NBV	count	number	pct	857094	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Prospect	nbv	Total NBV	count	number	pct	4156734	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Base	pageViews	Page Views	count	number	pct	458367826	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Unknown	sessionCount	Sessions	count	number	pct	1056003	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Prospect	vrChats	VR Chat	count	number	pct	71817	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Base	orders	Orders	count	number	pct	324478	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	All	nbvConfigure	NBV Configure	count	number	pct	1050441	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Unknown	orders	Orders	count	number	pct	53	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Prospect	buyFlowToConfigureRate	Buy flow → Configure	ratio	percent	pp	350459	1741828	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Base	nbvBuyFlow	NBV Buy Flow	count	number	pct	3659482	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	All	pageViews	Page Views	count	number	pct	506097731	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Prospect	nbvConfigure	NBV Configure	count	number	pct	350459	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Unknown	vrChats	VR Chat	count	number	pct	2555	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Base	ordersPerNbvBuyFlow	Orders / NBV Buy Flow	ratio	percent	pp	324478	3659482	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Unknown	ordersPerNbv	Orders / NBV	ratio	percent	pp	53	857094	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Unknown	pageViews	Page Views	count	number	pct	11111387	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Prospect	nbvBuyFlowPerNbv	NBV Buy Flow / NBV	ratio	percent	pp	1741828	4156734	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Base	ordersAcquisition	Orders Acquisition	count	number	pct	18028	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Base	ordersPerNbv	Orders / NBV	ratio	percent	pp	324478	15976716	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Prospect	pageViews	Page Views	count	number	pct	36618518	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	All	buyFlowToConfigureRate	Buy flow → Configure	ratio	percent	pp	1050441	5456399	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	All	storeLocator	Store Locator	count	number	pct	22693	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	All	ordersPerNbv	Orders / NBV	ratio	percent	pp	382980	20990544	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Base	ordersAssisted	Assisted Orders	count	number	pct	111554	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	All	vrCalls	VR Call	count	number	pct	144841	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Unknown	storeLocator	Store Locator	count	number	pct	0	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Base	vrCalls	VR Call	count	number	pct	51863	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Base	sessionCount	Sessions	count	number	pct	33528966	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Base	orderCount	Order Count	count	number	pct	376671	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Base	checkoutToOrderRate	Checkout start → Order	ratio	percent	pp	324478	406520	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Base	configureToCheckoutRate	Configure → Checkout start	ratio	percent	pp	406520	699487	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Base	nbv	Total NBV	count	number	pct	15976716	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Unknown	nbvBuyFlowPerNbv	NBV Buy Flow / NBV	ratio	percent	pp	55089	857094	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Unknown	nbvBuyFlow	NBV Buy Flow	count	number	pct	55089	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	All	nbvBuyFlow	NBV Buy Flow	count	number	pct	5456399	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Prospect	ordersUnassisted	Unassisted Orders	count	number	pct	44064	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	All	nbv	Total NBV	count	number	pct	20990544	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Unknown	checkoutToOrderRate	Checkout start → Order	ratio	percent	pp	53	247	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Prospect	ordersPerNbv	Orders / NBV	ratio	percent	pp	58449	4156734	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Base	nbvConfigure	NBV Configure	count	number	pct	699487	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Prospect	checkoutToOrderRate	Checkout start → Order	ratio	percent	pp	58449	178217	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	All	ordersPerNbvBuyFlow	Orders / NBV Buy Flow	ratio	percent	pp	382980	5456399	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Unknown	ordersBase	Orders Base	count	number	pct	53	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Unknown	buyFlowToConfigureRate	Buy flow → Configure	ratio	percent	pp	495	55089	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Base	vrChats	VR Chat	count	number	pct	224812	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Base	ordersUnassisted	Unassisted Orders	count	number	pct	212924	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Prospect	nbvCheckoutStart	NBV Checkout Start	count	number	pct	178217	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Unknown	ordersAssisted	Assisted Orders	count	number	pct	48	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Base	storeLocator	Store Locator	count	number	pct	2485	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	All	nbvCheckoutStart	NBV Checkout Start	count	number	pct	584984	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Base	nbvBuyFlowPerNbv	NBV Buy Flow / NBV	ratio	percent	pp	3659482	15976716	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	All	sessionCount	Sessions	count	number	pct	39675348	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Prospect	orderCount	Order Count	count	number	pct	62542	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	All	ordersBase	Orders Base	count	number	pct	307005	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Unknown	ordersAcquisition	Orders Acquisition	count	number	pct	0	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	All	ordersAssisted	Assisted Orders	count	number	pct	125987	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Unknown	ordersPerNbvBuyFlow	Orders / NBV Buy Flow	ratio	percent	pp	53	55089	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	All	configureToCheckoutRate	Configure → Checkout start	ratio	percent	pp	584984	1050441	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Prospect	nbvBuyFlow	NBV Buy Flow	count	number	pct	1741828	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Unknown	orderCount	Order Count	count	number	pct	53	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Prospect	storeLocator	Store Locator	count	number	pct	20208	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Base	buyFlowToConfigureRate	Buy flow → Configure	ratio	percent	pp	699487	3659482	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	All	ordersAcquisition	Orders Acquisition	count	number	pct	75975	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Prospect	ordersPerNbvBuyFlow	Orders / NBV Buy Flow	ratio	percent	pp	58449	1741828	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Prospect	ordersAssisted	Assisted Orders	count	number	pct	14385	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Unknown	configureToCheckoutRate	Configure → Checkout start	ratio	percent	pp	247	495	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Unknown	nbvConfigure	NBV Configure	count	number	pct	495	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Prospect	orders	Orders	count	number	pct	58449	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	All	orderCount	Order Count	count	number	pct	439266	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Prospect	configureToCheckoutRate	Configure → Checkout start	ratio	percent	pp	178217	350459	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Unknown	nbvCheckoutStart	NBV Checkout Start	count	number	pct	247	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Prospect	ordersBase	Orders Base	count	number	pct	502	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Prospect	ordersAcquisition	Orders Acquisition	count	number	pct	57947	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	All	checkoutToOrderRate	Checkout start → Order	ratio	percent	pp	382980	584984	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00
2026-09-06	2026-09-12	2026 Q3	2026Q3W36	W36 · 6 to 12 Sep	All	All	Prospect	vrCalls	VR Call	count	number	pct	92978	null	null	null	null	null	null	null	true	false	0	false	null	null	2026-10-27T16:44:45.909+00:00