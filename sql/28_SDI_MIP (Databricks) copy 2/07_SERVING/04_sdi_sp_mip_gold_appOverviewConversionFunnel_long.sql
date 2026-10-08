-- ============================================================================
-- RUNTIME-SAFE WRITE REVISION:
--   - Persisted table schema/API contract unchanged.
--   - Original comparison formulas retained, including existing 4-week behavior.
--   - Scoped INSERT ... REPLACE WHERE replaced with static MERGE.
--   - Metric Catalog is authoritative for metricLabel; NBV displays as Total NBV.

-- FILE  : 04_sdi_sp_mip_gold_appOverviewConversionFunnel_long.sql

-- LAYER : GOLD / APP

-- TAB   : Overview

-- SECTION: Conversion funnel over time

--

-- PURPOSE:

--   Application-ready conversion funnel over time.

--

-- GLOBAL CONTROLS:

--   Quarter    -> applicable

--   Week       -> applicable

--   Metric     -> NOT applicable to this section

--   Comparator -> priorWeek | fourWeek | lastYear

--   LOB        -> applicable

--   Platform   -> applicable

--

-- UI CARDS:

--   1. Total NBV -> buy flow

--   2. Buy flow -> configure

--   3. Configure -> checkout start

--   4. Checkout start -> order placed

--

-- Each row contains:

--   current conversion rate

--   selected comparator value

--   selected comparator change

--   order-change decomposition for the selected comparator

--

-- Historical weeks in this same table support the sparkline.

--

-- IMPORTANT:

--   Forecast is intentionally not included until funnel-level forecast metrics

--   are available/approved.

--

--   Traffic/conversion decomposition preserves the existing symmetric two-factor

--   implementation and remains a proposed business definition.

-- ============================================================================



-- ONE-TIME MIGRATION ONLY:

-- DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewConversionFunnel_long;



CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewConversionFunnel_long(

    IN p_asOfDate DATE DEFAULT NULL,

    IN p_weeksToRebuild INT DEFAULT 1,

    IN p_validateOnly BOOLEAN DEFAULT FALSE

)

LANGUAGE SQL

SQL SECURITY INVOKER

MODIFIES SQL DATA

COMMENT 'MIP Gold App: Conversion funnel over time. Comparator-aware conversion cards and proposed order-change decomposition.'

AS

BEGIN

    DECLARE v_asOfDate DATE DEFAULT coalesce(

        p_asOfDate,

        date_add(to_date(from_utc_timestamp(current_timestamp(),'America/Los_Angeles')),-1)

    );

    DECLARE v_weekTo DATE;

    DECLARE v_weekFrom DATE;

    DECLARE v_weekEndTo DATE;

    DECLARE v_processedAt TIMESTAMP DEFAULT current_timestamp();



    -- =========================================================================

    -- 1. Parameters

    -- =========================================================================

    IF p_weeksToRebuild IS NULL OR p_weeksToRebuild<1 THEN

        SIGNAL SQLSTATE '45000'

            SET MESSAGE_TEXT='p_weeksToRebuild must be >= 1.';

    END IF;



    SET v_weekTo=date_add(v_asOfDate,1-dayofweek(v_asOfDate));

    SET v_weekFrom=date_add(v_weekTo,-7*(p_weeksToRebuild-1));

    SET v_weekEndTo=date_add(v_weekTo,6);



    -- =========================================================================

    -- 2. Preflight

    -- =========================================================================

    IF NOT EXISTS(

        SELECT 1

        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long

        WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo

        LIMIT 1

    ) THEN

        SIGNAL SQLSTATE '45000'

            SET MESSAGE_TEXT='Overview Gold analytical ingredients has no rows for the requested target-week range.';

    END IF;



    IF NOT EXISTS(

        SELECT 1

        FROM prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static

        WHERE isActive

        LIMIT 1

    ) THEN

        SIGNAL SQLSTATE '45000'

            SET MESSAGE_TEXT='Metric Catalog has no active metrics.';

    END IF;



    IF NOT EXISTS(

        SELECT 1

        FROM prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static

        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo

        LIMIT 1

    ) THEN

        SIGNAL SQLSTATE '45000'

            SET MESSAGE_TEXT='Fiscal Calendar has no rows for the requested target-week range.';

    END IF;



    IF EXISTS(

        SELECT 1

        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long

        WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo

          AND metricName IN(

              'nbv',

              'orders',

              'nbvBuyFlowPerNbv',

              'buyFlowToConfigureRate',

              'configureToCheckoutRate',

              'checkoutToOrderRate'

          )

        GROUP BY targetWeekStartDate,filterLob,filterPlatform,metricName

        HAVING count(*)>1

        LIMIT 1

    ) THEN

        SIGNAL SQLSTATE '45000'

            SET MESSAGE_TEXT='Duplicate Overview Gold analytical keys detected for funnel metrics.';

    END IF;



    IF EXISTS(

        SELECT 1

        FROM prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static

        WHERE isActive

          AND metricName IN(

              'nbvBuyFlowPerNbv',

              'buyFlowToConfigureRate',

              'configureToCheckoutRate',

              'checkoutToOrderRate'

          )

          AND (

              metricKind NOT IN('count','ratio')

              OR displayFormat NOT IN('number','percent')

              OR changeUnit NOT IN('pct','pp')

          )

        LIMIT 1

    ) THEN

        SIGNAL SQLSTATE '45000'

            SET MESSAGE_TEXT='Metric Catalog contains unsupported funnel metric metadata.';

    END IF;



    -- =========================================================================

    -- 3. Validation only

    -- =========================================================================

    IF p_validateOnly THEN

        SELECT

            'VALIDATION_ONLY' AS status,

            v_weekFrom AS rebuildWeekStartFrom,

            v_weekTo AS rebuildWeekStartTo,

            v_weekEndTo AS latestWeekEndDate,

            CASE WHEN v_asOfDate<v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,

            'priorWeek | fourWeek | lastYear' AS supportedComparisons,

            FALSE AS metricDropdownApplies,

            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewConversionFunnel_long' AS targetObject,

            'Validation passed. No Gold App table was created or modified.' AS message;

    ELSE



        -- =====================================================================

        -- 4. App table

        -- =====================================================================

        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewConversionFunnel_long(

            targetWeekStartDate DATE,

            targetWeekEndDate DATE,

            fiscalQuarterLabel STRING,

            fiscalWeekCode STRING,

            weekLabel STRING,

            weekEndingLabel STRING,



            filterLob STRING,

            filterPlatform STRING,



            comparisonType STRING,

            comparisonLabel STRING,

            comparisonSortOrder INT,

            comparisonDataAvailable BOOLEAN,

            comparisonWindowComplete BOOLEAN,



            metricName STRING,

            metricLabel STRING,

            metricDescription STRING,

            metricKind STRING,

            displayFormat STRING,

            changeUnit STRING,

            funnelStepOrder INT,



            currentValue DOUBLE,

            currentValueDisplay STRING,



            comparisonValue DOUBLE,

            comparisonValueDisplay STRING,



            changeValue DOUBLE,

            changeDisplay STRING,



            orderChangeValue DOUBLE,

            orderChangeDisplay STRING,



            trafficEffectValue DOUBLE,

            trafficEffectDisplay STRING,



            conversionEffectValue DOUBLE,

            conversionEffectDisplay STRING,



            appProcessedAt TIMESTAMP

        )

        USING DELTA

        CLUSTER BY(targetWeekStartDate,comparisonType,funnelStepOrder)

        COMMENT 'MIP Gold App: Conversion funnel over time. Four conversion cards with comparator-aware values and order-change decomposition.';



        -- =====================================================================

        -- 5. Rebuild requested reporting weeks

        -- =====================================================================

        WITH funnelMap AS(

            SELECT * FROM VALUES

                ('nbvBuyFlowPerNbv',        'Total NBV → buy flow',          10),

                ('buyFlowToConfigureRate',  'Buy flow → configure',          20),

                ('configureToCheckoutRate', 'Configure → checkout start',    30),

                ('checkoutToOrderRate',     'Checkout start → order placed', 40)

            AS f(metricName,metricLabel,funnelStepOrder)

        ),



        scopeOverview AS(

            SELECT

                targetWeekStartDate,

                targetWeekEndDate,

                fiscalQuarterLabel,

                fiscalWeekCode,

                weekLabel,

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

            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long

            WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo

              AND metricName IN(

                  'nbv',

                  'orders',

                  'nbvBuyFlowPerNbv',

                  'buyFlowToConfigureRate',

                  'configureToCheckoutRate',

                  'checkoutToOrderRate'

              )

        ),



        base AS(

            SELECT

                g.targetWeekStartDate,

                g.targetWeekEndDate,

                g.fiscalQuarterLabel,

                g.fiscalWeekCode,

                g.weekLabel,

                c.weekEndingLabel,



                g.filterLob,

                g.filterPlatform,



                g.metricName,

                mc.metricLabel AS sourceMetricLabel,

                mc.metricDescription,

                mc.metricKind,

                mc.displayFormat,

                mc.changeUnit,



                g.thisWeekNumerator,

                g.thisWeekDenominator,

                g.priorWeekNumerator,

                g.priorWeekDenominator,

                g.fourWeekTrendNumerator,

                g.fourWeekTrendDenominator,

                g.sameWeekLyNumerator,

                g.sameWeekLyDenominator,



                g.thisWeekDataAvailable,

                g.priorWeekDataAvailable,

                g.fourWeekTrendWeekCount,

                g.sameWeekLyDataAvailable

            FROM scopeOverview g

            JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc

              ON mc.metricName=g.metricName

             AND mc.isActive

            LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c

              ON c.weekStartDate=g.targetWeekStartDate

        ),



        comparisonLong AS(

            SELECT

                b.*,

                'priorWeek' AS comparisonType,

                'Prior week' AS comparisonLabel,

                10 AS comparisonSortOrder,

                priorWeekDataAvailable AS comparisonDataAvailable,

                priorWeekDataAvailable AS comparisonWindowComplete,

                priorWeekNumerator AS comparisonNumerator,

                priorWeekDenominator AS comparisonDenominator

            FROM base b



            UNION ALL



            SELECT

                b.*,

                'fourWeek' AS comparisonType,

                '4-wk trend' AS comparisonLabel,

                20 AS comparisonSortOrder,

                fourWeekTrendWeekCount>0 AS comparisonDataAvailable,

                fourWeekTrendWeekCount=4 AS comparisonWindowComplete,

                CASE

                    WHEN metricKind='count' AND fourWeekTrendWeekCount>0

                        THEN try_divide(fourWeekTrendNumerator,cast(fourWeekTrendWeekCount AS DOUBLE))

                    ELSE fourWeekTrendNumerator

                END AS comparisonNumerator,

                CASE

                    WHEN metricKind='count' THEN NULL

                    ELSE fourWeekTrendDenominator

                END AS comparisonDenominator

            FROM base b



            UNION ALL



            SELECT

                b.*,

                'lastYear' AS comparisonType,

                'Same wk LY' AS comparisonLabel,

                30 AS comparisonSortOrder,

                sameWeekLyDataAvailable AS comparisonDataAvailable,

                sameWeekLyDataAvailable AS comparisonWindowComplete,

                sameWeekLyNumerator AS comparisonNumerator,

                sameWeekLyDenominator AS comparisonDenominator

            FROM base b

        ),



        valuesCalculated AS(

            SELECT

                *,

                CASE

                    WHEN NOT thisWeekDataAvailable THEN NULL

                    WHEN metricKind='ratio' THEN try_divide(thisWeekNumerator,thisWeekDenominator)

                    ELSE thisWeekNumerator

                END AS currentValue,



                CASE

                    WHEN NOT comparisonDataAvailable THEN NULL

                    WHEN metricKind='ratio' THEN try_divide(comparisonNumerator,comparisonDenominator)

                    ELSE comparisonNumerator

                END AS comparisonValue

            FROM comparisonLong

        ),



        changesCalculated AS(

            SELECT

                *,

                CASE

                    WHEN currentValue IS NULL OR comparisonValue IS NULL THEN NULL

                    WHEN changeUnit='pp' THEN 100D*(currentValue-comparisonValue)

                    WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,comparisonValue)-1D)

                END AS changeRaw

            FROM valuesCalculated

        ),



        decompInputs AS(

            SELECT

                targetWeekStartDate,

                filterLob,

                filterPlatform,

                comparisonType,



                max(CASE WHEN metricName='nbv' THEN currentValue END) AS currentTraffic,

                max(CASE WHEN metricName='nbv' THEN comparisonValue END) AS comparisonTraffic,



                max(CASE WHEN metricName='orders' THEN currentValue END) AS currentOrders,

                max(CASE WHEN metricName='orders' THEN comparisonValue END) AS comparisonOrders

            FROM valuesCalculated

            GROUP BY

                targetWeekStartDate,

                filterLob,

                filterPlatform,

                comparisonType

        ),



        decompConversion AS(

            SELECT

                *,

                try_divide(currentOrders,currentTraffic) AS currentOrderConversion,

                try_divide(comparisonOrders,comparisonTraffic) AS comparisonOrderConversion

            FROM decompInputs

        ),



        decomposition AS(

            SELECT

                *,

                currentOrders-comparisonOrders AS orderChangeValue,



                (currentTraffic-comparisonTraffic)

                    *((currentOrderConversion+comparisonOrderConversion)/2D)

                    AS trafficEffectValue,



                (currentOrderConversion-comparisonOrderConversion)

                    *((currentTraffic+comparisonTraffic)/2D)

                    AS conversionEffectValue

            FROM decompConversion

        ),



        cardRows AS(

            SELECT

                d.targetWeekStartDate,

                d.targetWeekEndDate,

                d.fiscalQuarterLabel,

                d.fiscalWeekCode,

                d.weekLabel,

                d.weekEndingLabel,



                d.filterLob,

                d.filterPlatform,



                d.comparisonType,

                d.comparisonLabel,

                d.comparisonSortOrder,

                d.comparisonDataAvailable,

                d.comparisonWindowComplete,



                d.metricName,

                f.metricLabel,

                d.metricDescription,

                d.metricKind,

                d.displayFormat,

                d.changeUnit,

                f.funnelStepOrder,



                d.currentValue,

                d.comparisonValue,

                d.changeRaw,



                x.orderChangeValue,

                x.trafficEffectValue,

                x.conversionEffectValue

            FROM changesCalculated d

            JOIN funnelMap f

              ON f.metricName=d.metricName

            LEFT JOIN decomposition x

              ON x.targetWeekStartDate=d.targetWeekStartDate

             AND x.filterLob=d.filterLob

             AND x.filterPlatform=d.filterPlatform

             AND x.comparisonType=d.comparisonType

        ),



        rounded AS(

            SELECT

                *,

                CASE

                    WHEN changeRaw IS NULL THEN NULL

                    WHEN abs(changeRaw)<0.05D THEN 0D

                    ELSE round(changeRaw,1)

                END AS changeValue

            FROM cardRows

        ),



        formatted AS(

            SELECT

                *,



                CASE

                    WHEN currentValue IS NULL THEN NULL

                    WHEN displayFormat='percent'

                        THEN concat(format_number(100D*currentValue,1),'%')

                    WHEN abs(currentValue)>=1000000000D

                        THEN concat(regexp_replace(format_number(currentValue/1000000000D,1),'\\\\.0$',''),'B')

                    WHEN abs(currentValue)>=1000000D

                        THEN concat(regexp_replace(format_number(currentValue/1000000D,1),'\\\\.0$',''),'M')

                    WHEN abs(currentValue)>=1000D

                        THEN concat(regexp_replace(format_number(currentValue/1000D,1),'\\\\.0$',''),'K')

                    ELSE format_number(currentValue,0)

                END AS currentValueDisplay,



                CASE

                    WHEN comparisonValue IS NULL THEN NULL

                    WHEN displayFormat='percent'

                        THEN concat(format_number(100D*comparisonValue,1),'%')

                    WHEN abs(comparisonValue)>=1000000000D

                        THEN concat(regexp_replace(format_number(comparisonValue/1000000000D,1),'\\\\.0$',''),'B')

                    WHEN abs(comparisonValue)>=1000000D

                        THEN concat(regexp_replace(format_number(comparisonValue/1000000D,1),'\\\\.0$',''),'M')

                    WHEN abs(comparisonValue)>=1000D

                        THEN concat(regexp_replace(format_number(comparisonValue/1000D,1),'\\\\.0$',''),'K')

                    ELSE format_number(comparisonValue,0)

                END AS comparisonValueDisplay,



                CASE

                    WHEN changeValue IS NULL THEN NULL

                    WHEN changeUnit='pp'

                        THEN concat(

                            CASE WHEN changeValue>0D THEN '+' ELSE '' END,

                            format_number(changeValue,1),

                            'pp'

                        )

                    WHEN changeUnit='pct'

                        THEN concat(

                            CASE WHEN changeValue>0D THEN '+' ELSE '' END,

                            format_number(changeValue,1),

                            '%'

                        )

                    ELSE cast(changeValue AS STRING)

                END AS changeDisplay,



                CASE

                    WHEN orderChangeValue IS NULL THEN NULL

                    ELSE concat(

                        CASE WHEN orderChangeValue>0D THEN '+' ELSE '' END,

                        format_number(

                            CASE WHEN abs(orderChangeValue)<0.5D THEN 0D ELSE orderChangeValue END,

                            0

                        )

                    )

                END AS orderChangeDisplay,



                CASE

                    WHEN trafficEffectValue IS NULL THEN NULL

                    ELSE concat(

                        CASE WHEN trafficEffectValue>0D THEN '+' ELSE '' END,

                        format_number(

                            CASE WHEN abs(trafficEffectValue)<0.5D THEN 0D ELSE trafficEffectValue END,

                            0

                        )

                    )

                END AS trafficEffectDisplay,



                CASE

                    WHEN conversionEffectValue IS NULL THEN NULL

                    ELSE concat(

                        CASE WHEN conversionEffectValue>0D THEN '+' ELSE '' END,

                        format_number(

                            CASE WHEN abs(conversionEffectValue)<0.5D THEN 0D ELSE conversionEffectValue END,

                            0

                        )

                    )

                END AS conversionEffectDisplay

            FROM rounded

        ),
        sourceRows AS (
SELECT

            targetWeekStartDate,

            targetWeekEndDate,

            fiscalQuarterLabel,

            fiscalWeekCode,

            weekLabel,

            weekEndingLabel,



            filterLob,

            filterPlatform,



            comparisonType,

            comparisonLabel,

            comparisonSortOrder,

            comparisonDataAvailable,

            comparisonWindowComplete,



            metricName,

            metricLabel,

            metricDescription,

            metricKind,

            displayFormat,

            changeUnit,

            funnelStepOrder,



            currentValue,

            currentValueDisplay,



            comparisonValue,

            comparisonValueDisplay,



            changeValue,

            changeDisplay,



            orderChangeValue,

            orderChangeDisplay,



            trafficEffectValue,

            trafficEffectDisplay,



            conversionEffectValue,

            conversionEffectDisplay,



            v_processedAt AS appProcessedAt

        FROM formatted
        )
        MERGE INTO prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewConversionFunnel_long AS t
        USING sourceRows AS s
          ON t.targetWeekStartDate = s.targetWeekStartDate
         AND t.filterLob <=> s.filterLob
         AND t.filterPlatform <=> s.filterPlatform
         AND t.comparisonType <=> s.comparisonType
         AND t.metricName <=> s.metricName
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        WHEN NOT MATCHED BY SOURCE
         AND t.targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        THEN DELETE;



        -- =====================================================================

        -- 6. Success

        -- =====================================================================

        SELECT

            'SUCCESS' AS status,

            v_weekFrom AS rebuiltWeekStartFrom,

            v_weekTo AS rebuiltWeekStartTo,

            v_weekEndTo AS latestWeekEndDate,

            CASE WHEN v_asOfDate<v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,

            'priorWeek | fourWeek | lastYear' AS supportedComparisons,

            FALSE AS metricDropdownApplies,

            'prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewConversionFunnel_long' AS targetObject,

            v_processedAt AS appProcessedAt;

    END IF;

END;



-- ============================================================================

-- DEVELOPMENT / VALIDATION

-- ============================================================================



-- ONE TIME ONLY before first deployment of this redesigned contract:

-- DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewConversionFunnel_long;



-- Preflight:

-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewConversionFunnel_long(

--     p_asOfDate=>DATE '2026-09-28',

--     p_weeksToRebuild=>12,

--     p_validateOnly=>TRUE

-- );



-- Build enough history for the sparklines:

-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewConversionFunnel_long(

--     p_asOfDate=>DATE '2026-09-28',

--     p_weeksToRebuild=>12,

--     p_validateOnly=>FALSE

-- );



-- Check schema:

-- DESCRIBE TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewConversionFunnel_long;



-- Expected zero duplicate rows:

-- SELECT

--     targetWeekStartDate,

--     filterLob,

--     filterPlatform,

--     comparisonType,

--     metricName,

--     count(*) AS rowCount

-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewConversionFunnel_long

-- GROUP BY

--     targetWeekStartDate,

--     filterLob,

--     filterPlatform,

--     comparisonType,

--     metricName

-- HAVING count(*)>1;



-- ============================================================================

-- UI EXAMPLE

-- Selected controls:

--   Q3 2026

--   W7 / 9-15 Aug

--   4-wk

--

-- No metricName filter.

-- ============================================================================



-- Selected-week cards:

-- SELECT

--     metricName,

--     metricLabel,

--     funnelStepOrder,

--     currentValue,

--     currentValueDisplay,

--     comparisonValue,

--     comparisonValueDisplay,

--     changeValue,

--     changeDisplay

-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewConversionFunnel_long

-- WHERE targetWeekStartDate=DATE '2026-08-09'

--   AND filterLob='All'

--   AND filterPlatform='All'

--   AND comparisonType='fourWeek'

-- ORDER BY funnelStepOrder;



-- Bottom decomposition:

-- SELECT DISTINCT

--     comparisonType,

--     comparisonLabel,

--     orderChangeValue,

--     orderChangeDisplay,

--     trafficEffectValue,

--     trafficEffectDisplay,

--     conversionEffectValue,

--     conversionEffectDisplay

-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewConversionFunnel_long

-- WHERE targetWeekStartDate=DATE '2026-08-09'

--   AND filterLob='All'

--   AND filterPlatform='All'

--   AND comparisonType='fourWeek';



-- Sparkline history ending at the selected week:

-- NOTE:

-- Do NOT restrict this query to fiscalQuarterLabel='Q3'.

-- The screenshot intentionally allows the sparkline to reach back into Q2.

--

-- SELECT

--     targetWeekStartDate,

--     fiscalQuarterLabel,

--     fiscalWeekCode,

--     metricName,

--     metricLabel,

--     funnelStepOrder,

--     currentValue,

--     currentValueDisplay

-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewConversionFunnel_long

-- WHERE targetWeekStartDate BETWEEN date_add(DATE '2026-08-09',-49) AND DATE '2026-08-09'

--   AND filterLob='All'

--   AND filterPlatform='All'

--   AND comparisonType='fourWeek'

-- ORDER BY funnelStepOrder,targetWeekStartDate;

-- ============================================================================
-- NOTEBOOK CALL PATTERN (foldable SQL literals; do not use args= for CALL)
-- ============================================================================
-- Python:
-- from datetime import date
-- as_of_date = date(2026,10,3)
-- weeks_to_rebuild = 1
-- validate_only = False
-- validate_literal = 'TRUE' if validate_only else 'FALSE'
-- call_sql = f"""
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewConversionFunnel_long(
--   p_asOfDate       => DATE '{as_of_date.isoformat()}',
--   p_weeksToRebuild => {int(weeks_to_rebuild)},
--   p_validateOnly   => {validate_literal}
-- )
-- """
-- result = spark.sql(call_sql).collect()
-- display(result)
