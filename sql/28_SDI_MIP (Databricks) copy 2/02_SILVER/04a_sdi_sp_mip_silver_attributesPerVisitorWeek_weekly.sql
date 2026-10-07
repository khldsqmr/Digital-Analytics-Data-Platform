-- ============================================================================

-- FILE  : 04a_sdi_sp_mip_silver_attributesPerVisitorWeek_weekly.sql

-- LAYER : SILVER

-- RUNTIME WRITE NOTE:
--   S04 uses static SQL with a scoped MERGE.
--   No EXECUTE IMMEDIATE, REPLACE WHERE, REPLACE USING, or dynamic DATE
--   rendering is used for the write.
--   Procedure-local DATE variables are used directly in normal source filters
--   and in the bounded MERGE delete condition.
--   This is the same static MERGE execution pattern proven by Silver S01.
--
-- CALL COMPATIBILITY:
--   - Manual SQL: CALL with DATE / INT literals.
--   - Notebook: render only already-validated Python date/int values as SQL
--     literals in CALL because this runtime requires foldable CALL arguments.
--
-- PURPOSE:

--   One attributed attribute row per NBV visitor/week for additive serving.

--

-- PERFORMANCE:

--   - Operates on session-grain Silver, not hit-grain data.

--   - Weekly platform/device attribution uses session pageViews as weights.

--   - Only LOB uses its session map because LOB is genuinely multi-valued.

--

-- CHANNEL CONTRACT:

--   Weekly channel attribution keeps the exact session channel_name category;

--   Paid Search: Brand / PLAs / Non-Brand remain separate values. No roll-up.

--

-- PREFLIGHT / VALIDATION CONTRACT:

--   p_validateOnly=TRUE verifies that attributesPerSession contains every

--   requested Sunday weekStartDate. It also reports whether the latest requested

--   week is partial relative to p_asOfDate.

--

-- PEER / IMPACT CONTRACT:

--

--   The existing scalar channel remains the ONE attributed weekly channel used by

--   normal additive dashboard serving.

--

--   channelList is an overlapping helper containing every distinct resolved

--   session channel touched by the visitor in the week, ordered by that channel's

--   first sessionStartTsUtc. Exact channel_name values are preserved; there is no

--   Paid Search/category roll-up. Null resolved channels are represented as

--   '(not set)'; existing values such as 'Session Refresh' are preserved rather

--   than silently excluded in Silver.

--

--   channelList is intended for channel-membership / NBV peer-set logic and

--   diagnostics. Metric-qualified Channel membership is stored in Silver 05

--   channelMetricMemberships.

--

--   IMPORTANT: channelList is NOT crossed with platform/device/etc. to manufacture

--   crosstab pairs. Independent weekly arrays can create combinations that never

--   occurred together. Crosstab peer membership is therefore rebuilt downstream

--   from session/page-category Silver, which preserves the real pair context.

--

--   Impact-on-topline remains a Gold/App-Gold comparison calculation.

--

-- SCHEMA CHANGE NOTE:

--   channelList is a new column. If the existing target table was created from the

--   previous schema, the procedure checks for the helper column and evolves the target;

--   the scoped rebuild then populates that helper for the requested week(s).

-- ============================================================================

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_silver_attributesPerVisitorWeek_weekly(

    IN p_asOfDate DATE DEFAULT NULL,

    IN p_weeksToRebuild INT DEFAULT 1,

    IN p_validateOnly BOOLEAN DEFAULT FALSE

)

LANGUAGE SQL

SQL SECURITY INVOKER

MODIFIES SQL DATA

COMMENT 'Silver weekly NBV visitor attributes: one attributed row per visitor per Sunday-Saturday reporting week.'

AS

BEGIN

    DECLARE v_asOfDate DATE DEFAULT coalesce(

        p_asOfDate,

        date_add(to_date(from_utc_timestamp(current_timestamp(),'America/Los_Angeles')),-1)

    );

    DECLARE v_weekStartTo DATE;

    DECLARE v_weekStartFrom DATE;

    DECLARE v_weekEndTo DATE;

    DECLARE v_processedAt TIMESTAMP DEFAULT current_timestamp();

    DECLARE v_sourceWeekCount BIGINT DEFAULT 0;

    DECLARE v_writeSql STRING;

    DECLARE v_scopeStartSql STRING;

    DECLARE v_scopeEndSql STRING;

    IF p_weeksToRebuild IS NULL OR p_weeksToRebuild<1 THEN

        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='p_weeksToRebuild must be >= 1.';

    END IF;

    SET v_weekStartTo=date_add(v_asOfDate,1-dayofweek(v_asOfDate));

    SET v_weekStartFrom=date_add(v_weekStartTo,-7*(p_weeksToRebuild-1));

    SET v_weekEndTo=date_add(v_weekStartTo,6);

    SET v_sourceWeekCount=(

        SELECT COUNT(DISTINCT weekStartDate)

        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily

        WHERE weekStartDate BETWEEN v_weekStartFrom AND v_weekStartTo

          AND visitorId IS NOT NULL

    );

    IF v_sourceWeekCount<>p_weeksToRebuild THEN

        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Silver attributesPerSession does not contain every requested reporting week.';

    END IF;

    IF p_validateOnly THEN

        SELECT

            'VALIDATION_ONLY' AS status,

            v_weekStartFrom AS rebuildWeekStartFrom,

            v_weekStartTo AS rebuildWeekStartTo,

            v_weekEndTo AS latestWeekEndDate,

            v_sourceWeekCount AS sourceWeekCount,

            CASE WHEN v_asOfDate<v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,

            'No Silver weekly table was created or modified.' AS message;

    ELSE

        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly (

            weekStartDate DATE,

            weekEndDate DATE,

            visitorId STRING,

            identitySource STRING,

            lob STRING COMMENT 'Attributed weekly LOB: most NBV-session page views',

            lobList ARRAY<STRING> COMMENT 'Natural LOB memberships touched during the week',

            platform STRING COMMENT 'Attributed weekly platform: most NBV-session page views',

            platformList ARRAY<STRING> COMMENT 'Platforms touched during the week',

            prospectVsBase STRING COMMENT 'Strongest weekly state: Customer > Care > Prospect > Unknown',

            authState STRING COMMENT 'Strongest weekly auth state',

            channel STRING COMMENT 'Attributed exact UDI channel_name value; no Paid Search/category roll-up',

            channelList ARRAY<STRING> COMMENT 'All distinct resolved session channels touched by the visitor/week, ordered by first session touch; exact values preserved',

            campaign STRING,

            campaignCode STRING,

            entryPage STRING,

            utmSource STRING,

            utmMedium STRING,

            utmCampaign STRING,

            pageCategory STRING COMMENT 'Most-viewed weekly page category',

            device STRING COMMENT 'Attributed weekly device: most NBV-session page views',

            buyFlowStep STRING COMMENT 'Deepest weekly buy-flow step',

            region STRING COMMENT 'Attributed weekly temporary geography placeholder: Web ZIP/App country, weighted by NBV-session page views',

            geoContext STRING COMMENT 'Source-aware geography context from the strongest session within the attributed weekly placeholder value',

            isTmoNetwork INT,

            silverProcessedAt TIMESTAMP

        )

        USING DELTA

        CLUSTER BY (weekStartDate)

        COMMENT 'Silver: one attributed attribute row per NBV visitor/week; 1:1 with actionsPerVisitorWeek.';

        -- Evolve the known weekly helper column if this target predates the current schema.

        IF NOT EXISTS (

            SELECT 1

            FROM prdrzranalytics.information_schema.columns

            WHERE lower(table_catalog) = 'prdrzranalytics'

              AND lower(table_schema) = 'lab42'

              AND lower(table_name) = 'sdi_tbl_mip_silver_attributespervisitorweek_weekly'

              AND lower(column_name) = 'channellist'

        ) THEN

            ALTER TABLE prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly

            ADD COLUMNS (

                channelList ARRAY<STRING>

                COMMENT 'All distinct resolved session channels touched by the visitor/week, ordered by first session touch; exact values preserved'

            );

        END IF;

        -- --------------------------------------------------------------------

        -- Atomic selective overwrite for the requested Silver scope.

        -- --------------------------------------------------------------------

        -- --------------------------------------------------------------------
        -- Static scoped rebuild using the same MERGE pattern proven in S01.
        --
        -- Grain / MERGE key:
        --   weekStartDate + visitorId
        --
        -- The bounded NOT MATCHED BY SOURCE clause removes stale rows only
        -- inside the requested rebuild weeks.
        -- --------------------------------------------------------------------
        WITH weekSessions AS (

            SELECT

                sessionId,

                visitorId,

                identitySource,

                weekStartDate,

                pageViews,

                lobList,

                lobPageViews,

                platform,

                device,

                region,

                geoContext,

                prospectVsBase,

                prospectVsBaseRank,

                authState,

                authStateRank,

                channel,

                campaignCode,

                campaignName,

                entryPage,

                utmSource,

                utmMedium,

                utmCampaign,

                deepestBuyFlowStep,

                deepestBuyFlowStepOrder,

                isTmoNetworkSession,

                sessionStartTsUtc

            FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily

            WHERE weekStartDate BETWEEN v_weekStartFrom AND v_weekStartTo

              AND visitorId IS NOT NULL

        ),

        lobPv AS (

            SELECT

                s.visitorId,

                s.weekStartDate,

                x.lobKey AS lob,

                SUM(x.lobValue) AS pageViews

            FROM weekSessions s

            LATERAL VIEW explode(s.lobPageViews) x AS lobKey,lobValue

            GROUP BY s.visitorId,s.weekStartDate,x.lobKey

        ),

        lobResolved AS (

            SELECT

                visitorId,

                weekStartDate,

                max_by(lob,struct(pageViews,lob)) AS lob

            FROM lobPv

            GROUP BY visitorId,weekStartDate

        ),

        platformResolved AS (

            SELECT

                visitorId,

                weekStartDate,

                max_by(platform,struct(pageViews,platform)) AS platform

            FROM (

                SELECT

                    visitorId,

                    weekStartDate,

                    coalesce(platform,'(not set)') AS platform,

                    SUM(pageViews) AS pageViews

                FROM weekSessions

                GROUP BY visitorId,weekStartDate,coalesce(platform,'(not set)')

            )

            GROUP BY visitorId,weekStartDate

        ),

        deviceResolved AS (

            SELECT

                visitorId,

                weekStartDate,

                max_by(device,struct(pageViews,device)) AS device

            FROM (

                SELECT

                    visitorId,

                    weekStartDate,

                    coalesce(device,'Unknown') AS device,

                    SUM(pageViews) AS pageViews

                FROM weekSessions

                GROUP BY visitorId,weekStartDate,coalesce(device,'Unknown')

            )

            GROUP BY visitorId,weekStartDate

        ),

        regionResolved AS (

            SELECT

                visitorId,

                weekStartDate,

                max_by(region,struct(pageViews,region)) AS region

            FROM (

                SELECT

                    visitorId,

                    weekStartDate,

                    coalesce(region,'(not available)') AS region,

                    SUM(pageViews) AS pageViews

                FROM weekSessions

                GROUP BY visitorId,weekStartDate,coalesce(region,'(not available)')

            )

            GROUP BY visitorId,weekStartDate

        ),

        geoContextResolved AS (

            SELECT

                s.visitorId,

                s.weekStartDate,

                max_by(

                    s.geoContext,

                    struct(s.pageViews,s.sessionStartTsUtc,s.sessionId)

                ) FILTER (WHERE s.geoContext IS NOT NULL) AS geoContext

            FROM weekSessions s

            INNER JOIN regionResolved r

              ON r.visitorId=s.visitorId

             AND r.weekStartDate=s.weekStartDate

             AND r.region=coalesce(s.region,'(not available)')

            GROUP BY s.visitorId,s.weekStartDate

        ),

        categoryResolved AS (

            SELECT

                visitorId,

                weekStartDate,

                max_by(pageCategory,struct(pageViews,pageCategory)) AS pageCategory

            FROM (

                SELECT

                    visitorId,

                    weekStartDate,

                    pageCategory,

                    SUM(pageViews) AS pageViews

                FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily

                WHERE weekStartDate BETWEEN v_weekStartFrom AND v_weekStartTo

                  AND visitorId IS NOT NULL

                GROUP BY visitorId,weekStartDate,pageCategory

            )

            GROUP BY visitorId,weekStartDate

        ),

        channelFirstTouch AS (

            SELECT

                visitorId,

                weekStartDate,

                coalesce(channel,'(not set)') AS channel,

                MIN(sessionStartTsUtc) AS firstTouchTs

            FROM weekSessions

            GROUP BY visitorId,weekStartDate,coalesce(channel,'(not set)')

        ),

        channelListResolved AS (

            SELECT

                visitorId,

                weekStartDate,

                transform(

                    array_sort(

                        collect_list(

                            named_struct(

                                'firstTouchTs',firstTouchTs,

                                'channel',channel

                            )

                        )

                    ),

                    x -> x.channel

                ) AS channelList

            FROM channelFirstTouch

            GROUP BY visitorId,weekStartDate

        ),

        agg AS (

            SELECT

                visitorId,

                weekStartDate,

                min_by(identitySource,sessionStartTsUtc)

                    FILTER (WHERE identitySource IS NOT NULL) AS identitySource,

                array_sort(

                    array_distinct(

                        flatten(

                            collect_list(

                                coalesce(lobList,cast(array() AS ARRAY<STRING>))

                            )

                        )

                    )

                ) AS lobList,

                array_sort(collect_set(coalesce(platform,'(not set)'))) AS platformList,

                max_by(

                    prospectVsBase,

                    struct(prospectVsBaseRank,sessionStartTsUtc,sessionId)

                ) AS prospectVsBase,

                max_by(

                    authState,

                    struct(authStateRank,sessionStartTsUtc,sessionId)

                ) AS authState,

                max_by(

                    named_struct(

                        'channel',channel,

                        'campaignCode',campaignCode,

                        'campaignName',campaignName,

                        'entryPage',entryPage,

                        'utmSource',utmSource,

                        'utmMedium',utmMedium,

                        'utmCampaign',utmCampaign

                    ),

                    struct(

                        CASE

                            WHEN channel IS NOT NULL

                             AND channel NOT IN ('(not set)','Session Refresh') THEN 1

                            ELSE 0

                        END,

                        sessionStartTsUtc,

                        sessionId

                    )

                ) AS attributedTouch,

                max_by(

                    deepestBuyFlowStep,

                    struct(

                        coalesce(deepestBuyFlowStepOrder,-1),

                        sessionStartTsUtc,

                        coalesce(deepestBuyFlowStep,'')

                    )

                ) FILTER (WHERE deepestBuyFlowStep IS NOT NULL) AS deepestBuyFlowStep,

                max(isTmoNetworkSession) AS isTmoNetwork

            FROM weekSessions

            GROUP BY visitorId,weekStartDate

        ),
        sourceRows AS (
            SELECT

            a.weekStartDate,

            date_add(a.weekStartDate,6) AS weekEndDate,

            a.visitorId,

            a.identitySource,

            coalesce(l.lob,'Other') AS lob,

            a.lobList,

            coalesce(p.platform,'(not set)') AS platform,

            a.platformList,

            coalesce(a.prospectVsBase,'Unknown') AS prospectVsBase,

            coalesce(a.authState,'(not set)') AS authState,

            coalesce(a.attributedTouch.channel,'(not set)') AS channel,

            coalesce(cl.channelList,array(coalesce(a.attributedTouch.channel,'(not set)'))) AS channelList,

            CASE

                WHEN a.attributedTouch.campaignCode IS NULL THEN '(not set)'

                WHEN a.attributedTouch.campaignName IS NOT NULL

                    THEN concat(a.attributedTouch.campaignCode,' · ',a.attributedTouch.campaignName)

                ELSE a.attributedTouch.campaignCode

            END AS campaign,

            a.attributedTouch.campaignCode AS campaignCode,

            coalesce(a.attributedTouch.entryPage,'(not set)') AS entryPage,

            coalesce(a.attributedTouch.utmSource,'(not set)') AS utmSource,

            coalesce(a.attributedTouch.utmMedium,'(not set)') AS utmMedium,

            coalesce(a.attributedTouch.utmCampaign,'(not set)') AS utmCampaign,

            coalesce(c.pageCategory,'(not set)') AS pageCategory,

            coalesce(d.device,'Unknown') AS device,

            coalesce(a.deepestBuyFlowStep,'Did not enter buy flow') AS buyFlowStep,

            coalesce(r.region,'(not available)') AS region,

            g.geoContext AS geoContext,

            coalesce(a.isTmoNetwork,0) AS isTmoNetwork,

            current_timestamp() AS silverProcessedAt

        FROM agg a

        LEFT JOIN channelListResolved cl

          ON cl.visitorId=a.visitorId

         AND cl.weekStartDate=a.weekStartDate

        LEFT JOIN lobResolved l

          ON l.visitorId=a.visitorId

         AND l.weekStartDate=a.weekStartDate

        LEFT JOIN platformResolved p

          ON p.visitorId=a.visitorId

         AND p.weekStartDate=a.weekStartDate

        LEFT JOIN deviceResolved d

          ON d.visitorId=a.visitorId

         AND d.weekStartDate=a.weekStartDate

        LEFT JOIN regionResolved r

          ON r.visitorId=a.visitorId

         AND r.weekStartDate=a.weekStartDate

        LEFT JOIN geoContextResolved g

          ON g.visitorId=a.visitorId

         AND g.weekStartDate=a.weekStartDate

        LEFT JOIN categoryResolved c

          ON c.visitorId=a.visitorId

         AND c.weekStartDate=a.weekStartDate
        )
        MERGE INTO prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly AS t
        USING sourceRows AS s
          ON t.weekStartDate = s.weekStartDate
         AND t.visitorId = s.visitorId
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        WHEN NOT MATCHED BY SOURCE
         AND t.weekStartDate BETWEEN v_weekStartFrom AND v_weekStartTo
        THEN DELETE;

        SELECT

            'SUCCESS' AS status,

            v_weekStartFrom AS rebuiltWeekStartFrom,

            v_weekStartTo AS rebuiltWeekStartTo,

            v_weekEndTo AS latestWeekEndDate,

            CASE WHEN v_asOfDate<v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,

            'prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly' AS targetObject;

    END IF;

END;

-- ============================================================================

-- DEVELOPMENT / TEST EXAMPLES

-- Run these statements separately after deploying the procedure.

-- ============================================================================

-- --------------------------------------------------------------------------

-- A. PREFLIGHT ONLY

-- 2026-09-28 belongs to the Sunday-starting week 2026-09-27.

-- --------------------------------------------------------------------------

-- CALL prdrzranalytics.lab42.sdi_sp_mip_silver_attributesPerVisitorWeek_weekly(

--     p_asOfDate       => DATE '2026-09-28',

--     p_weeksToRebuild => 1,

--     p_validateOnly   => TRUE

-- );

-- --------------------------------------------------------------------------

-- B. EXECUTE / REBUILD ONE REPORTING WEEK

-- --------------------------------------------------------------------------

-- CALL prdrzranalytics.lab42.sdi_sp_mip_silver_attributesPerVisitorWeek_weekly(

--     p_asOfDate       => DATE '2026-09-28',

--     p_weeksToRebuild => 1,

--     p_validateOnly   => FALSE

-- );

-- --------------------------------------------------------------------------

-- C. VALIDATION 1: VISITOR/WEEK GRAIN UNIQUENESS

-- Expected: no rows.

-- --------------------------------------------------------------------------

-- SELECT

--     weekStartDate,

--     visitorId,

--     COUNT(*) AS rowCount

-- FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly

-- WHERE weekStartDate = DATE '2026-09-27'

-- GROUP BY weekStartDate,visitorId

-- HAVING COUNT(*) > 1

-- ORDER BY rowCount DESC

-- LIMIT 100;

-- --------------------------------------------------------------------------

-- D. VALIDATION 2: ROW COUNT RECONCILIATION TO DISTINCT NBV VISITORS

-- Expected: rowDiff = 0.

-- --------------------------------------------------------------------------

-- WITH expected AS (

--     SELECT DISTINCT visitorId,weekStartDate

--     FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily

--     WHERE weekStartDate = DATE '2026-09-27'

--       AND visitorId IS NOT NULL

-- ),

-- actual AS (

--     SELECT visitorId,weekStartDate

--     FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly

--     WHERE weekStartDate = DATE '2026-09-27'

-- )

-- SELECT

--     (SELECT COUNT(*) FROM expected) AS expectedVisitors,

--     (SELECT COUNT(*) FROM actual) AS actualVisitors,

--     (SELECT COUNT(*) FROM actual) - (SELECT COUNT(*) FROM expected) AS rowDiff;

-- --------------------------------------------------------------------------

-- E. VALIDATION 3: ATTRIBUTION SANITY

-- Expected:

--   attributed LOB belongs to lobList.

--   weekly platform belongs to platformList.

-- --------------------------------------------------------------------------

-- SELECT

--     COUNT(*) AS rowsChecked,

--     COUNT_IF(NOT array_contains(lobList,lob)) AS invalidLobAttribution,

--     COUNT_IF(NOT array_contains(platformList,platform)) AS invalidPlatformAttribution,

--     COUNT_IF(channelList IS NULL OR size(channelList)=0) AS emptyChannelLists,

--     COUNT_IF(size(channelList)<>size(array_distinct(channelList))) AS duplicateChannelEntries,

--     COUNT_IF(NOT array_contains(channelList,channel)) AS attributedChannelMissingFromList,

--     COUNT_IF(region IS NULL OR region='(not available)') AS missingRegionAttribution,

--     COUNT_IF(weekEndDate <> date_add(weekStartDate,6)) AS invalidWeekEnd

-- FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly

-- WHERE weekStartDate = DATE '2026-09-27';

-- --------------------------------------------------------------------------

-- F. VALIDATION 4: CHANNEL LIST RECONCILIATION

-- Rebuild the expected list directly from session Silver.

-- Expected: mismatchedChannelLists = 0.

-- --------------------------------------------------------------------------

-- WITH firstTouch AS (

--     SELECT

--         visitorId,

--         weekStartDate,

--         coalesce(channel,'(not set)') AS channel,

--         MIN(sessionStartTsUtc) AS firstTouchTs

--     FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily

--     WHERE weekStartDate = DATE '2026-09-27'

--       AND visitorId IS NOT NULL

--     GROUP BY visitorId,weekStartDate,coalesce(channel,'(not set)')

-- ),

-- expected AS (

--     SELECT

--         visitorId,

--         weekStartDate,

--         transform(

--             array_sort(

--                 collect_list(named_struct('firstTouchTs',firstTouchTs,'channel',channel))

--             ),

--             x -> x.channel

--         ) AS expectedChannelList

--     FROM firstTouch

--     GROUP BY visitorId,weekStartDate

-- )

-- SELECT

--     COUNT_IF(a.channelList<>e.expectedChannelList) AS mismatchedChannelLists

-- FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly a

-- INNER JOIN expected e

--   ON e.visitorId=a.visitorId

--  AND e.weekStartDate=a.weekStartDate

-- WHERE a.weekStartDate = DATE '2026-09-27';

-- ============================================================================
-- NOTEBOOK CALL EXAMPLE
-- ============================================================================
-- as_of_date: Python datetime.date
-- weeks_to_rebuild: validated Python int >= 1
--
-- call_sql = f"""
--     CALL prdrzranalytics.lab42.sdi_sp_mip_silver_attributesPerVisitorWeek_weekly(
--         p_asOfDate       => DATE '{as_of_date.isoformat()}',
--         p_weeksToRebuild => {int(weeks_to_rebuild)},
--         p_validateOnly   => FALSE
--     )
-- """
-- result = spark.sql(call_sql).collect()
-- ============================================================================

