-- ============================================================================
-- FILE  : 04_sdi_sp_mip_silver_attributesPerVisitorWeek_weekly.sql
-- LAYER : SILVER
-- PURPOSE:
--   One attributed attribute row per visitor/week for additive serving breakouts.
--
-- NOTE:
--   p_asOfDate determines the Sunday-starting week containing that date.
--   During development this can intentionally be a partial week.
-- ============================================================================

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_silver_attributesPerVisitorWeek_weekly(
    IN p_asOfDate       DATE    DEFAULT NULL,
    IN p_weeksToRebuild INT     DEFAULT 1,
    IN p_validateOnly   BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Silver weekly visitor attributes: one attributed row per visitor per Sunday-Saturday reporting week.'
AS
BEGIN
    DECLARE v_asOfDate DATE DEFAULT coalesce(
        p_asOfDate,
        date_add(
            to_date(from_utc_timestamp(current_timestamp(), 'America/Los_Angeles')),
            -1
        )
    );
    DECLARE v_weekStartTo DATE;
    DECLARE v_weekStartFrom DATE;
    DECLARE v_weekEndTo DATE;
    DECLARE v_processedAt TIMESTAMP DEFAULT current_timestamp();

    IF p_weeksToRebuild IS NULL OR p_weeksToRebuild < 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'p_weeksToRebuild must be >= 1.';
    END IF;

    SET v_weekStartTo = date_add(v_asOfDate, 1 - dayofweek(v_asOfDate));
    SET v_weekStartFrom = date_add(v_weekStartTo, -7 * (p_weeksToRebuild - 1));
    SET v_weekEndTo = date_add(v_weekStartTo, 6);

    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily
        WHERE weekStartDate BETWEEN v_weekStartFrom AND v_weekStartTo
          AND visitorId IS NOT NULL
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Silver attributesPerSession returned no visitor/week rows for the requested weekly rebuild.';
    END IF;

    IF p_validateOnly THEN
        SELECT
            'VALIDATION_ONLY' AS status,
            v_weekStartFrom AS rebuildWeekStartFrom,
            v_weekStartTo AS rebuildWeekStartTo,
            v_weekEndTo AS latestWeekEndDate,
            CASE WHEN v_asOfDate < v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,
            'No Silver weekly table was created or modified.' AS message;
    ELSE
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly (
            weekStartDate       DATE,
            weekEndDate         DATE,
            visitorId           STRING,
            identitySource      STRING,
            lob                 STRING COMMENT 'Attributed weekly breakout LOB: most page views',
            lobList             ARRAY<STRING> COMMENT 'Natural LOB memberships touched during the week',
            platform            STRING COMMENT 'Attributed weekly platform: most page views',
            platformList        ARRAY<STRING> COMMENT 'Natural platforms touched during the week',
            prospectVsBase      STRING COMMENT 'Strongest weekly state: Customer > Care > Prospect > Unknown',
            authState           STRING COMMENT 'Strongest weekly auth state',
            channel             STRING COMMENT 'Last qualifying session channel; dashboard attribution only',
            campaign            STRING,
            campaignCode        STRING,
            entryPage           STRING,
            utmSource           STRING,
            utmMedium           STRING,
            utmCampaign         STRING,
            pageCategory        STRING COMMENT 'Most-viewed weekly page category',
            device              STRING COMMENT 'Most-viewed proposed device grouping',
            buyFlowStep         STRING COMMENT 'Deepest weekly step; Did not enter buy flow if none',
            region              STRING COMMENT 'Placeholder until geo solution is supplied',
            isTmoNetwork        INT,
            silverProcessedAt   TIMESTAMP
        )
        USING DELTA
        CLUSTER BY (weekStartDate)
        COMMENT 'Silver: one attributed attribute row per visitor per week; 1:1 with actionsPerVisitorWeek.';

        WITH weekSessions AS (
            SELECT *
            FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily
            WHERE weekStartDate BETWEEN v_weekStartFrom AND v_weekStartTo
              AND visitorId IS NOT NULL
        ),
        keys AS (
            SELECT DISTINCT visitorId, weekStartDate
            FROM weekSessions
        ),
        lobPv AS (
            SELECT
                s.visitorId,
                s.weekStartDate,
                x.lobKey AS lob,
                sum(x.lobValue) AS pageViews
            FROM weekSessions s,
                 LATERAL explode(s.lobPageViews) AS x(lobKey, lobValue)
            GROUP BY s.visitorId, s.weekStartDate, x.lobKey
        ),
        lobResolved AS (
            SELECT
                visitorId,
                weekStartDate,
                max_by(lob, struct(pageViews, lob)) AS lob
            FROM lobPv
            GROUP BY visitorId, weekStartDate
        ),
        platformPv AS (
            SELECT
                s.visitorId,
                s.weekStartDate,
                x.platformKey AS platform,
                sum(x.platformValue) AS pageViews
            FROM weekSessions s,
                 LATERAL explode(s.platformPageViews) AS x(platformKey, platformValue)
            GROUP BY s.visitorId, s.weekStartDate, x.platformKey
        ),
        platformResolved AS (
            SELECT
                visitorId,
                weekStartDate,
                max_by(platform, struct(pageViews, platform)) AS platform
            FROM platformPv
            GROUP BY visitorId, weekStartDate
        ),
        devicePv AS (
            SELECT
                s.visitorId,
                s.weekStartDate,
                x.deviceKey AS device,
                sum(x.deviceValue) AS pageViews
            FROM weekSessions s,
                 LATERAL explode(s.devicePageViews) AS x(deviceKey, deviceValue)
            GROUP BY s.visitorId, s.weekStartDate, x.deviceKey
        ),
        deviceResolved AS (
            SELECT
                visitorId,
                weekStartDate,
                max_by(device, struct(pageViews, device)) AS device
            FROM devicePv
            GROUP BY visitorId, weekStartDate
        ),
        categoryPv AS (
            SELECT
                visitorId,
                weekStartDate,
                pageCategory,
                sum(pageViews) AS pageViews
            FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily
            WHERE weekStartDate BETWEEN v_weekStartFrom AND v_weekStartTo
              AND visitorId IS NOT NULL
            GROUP BY visitorId, weekStartDate, pageCategory
        ),
        categoryResolved AS (
            SELECT
                visitorId,
                weekStartDate,
                max_by(pageCategory, struct(pageViews, pageCategory)) AS pageCategory
            FROM categoryPv
            GROUP BY visitorId, weekStartDate
        ),
        agg AS (
            SELECT
                visitorId,
                weekStartDate,
                min_by(identitySource, sessionStartTsUtc)
                    FILTER (WHERE identitySource IS NOT NULL) AS identitySource,
                array_sort(
                    array_distinct(
                        flatten(
                            collect_list(
                                coalesce(lobList, cast(array() AS ARRAY<STRING>))
                            )
                        )
                    )
                ) AS lobList,
                array_sort(
                    collect_set(coalesce(platform, '(not set)'))
                ) AS platformList,
                max_by(
                    prospectVsBase,
                    struct(prospectVsBaseRank, sessionStartTsUtc)
                ) AS prospectVsBase,
                max_by(
                    authState,
                    struct(authStateRank, sessionStartTsUtc)
                ) AS authState,
                max_by(
                    named_struct(
                        'channel', channel,
                        'campaignCode', campaignCode,
                        'campaignName', campaignName,
                        'entryPage', entryPage,
                        'utmSource', utmSource,
                        'utmMedium', utmMedium,
                        'utmCampaign', utmCampaign
                    ),
                    struct(
                        CASE
                            WHEN channel IS NOT NULL
                             AND channel NOT IN ('(not set)', 'Session Refresh') THEN 1
                            ELSE 0
                        END,
                        sessionStartTsUtc,
                        sessionId
                    )
                ) AS attributedTouch,
                max_by(
                    deepestBuyFlowStep,
                    struct(
                        coalesce(deepestBuyFlowStepOrder, -1),
                        sessionStartTsUtc,
                        coalesce(deepestBuyFlowStep, '')
                    )
                ) FILTER (WHERE deepestBuyFlowStep IS NOT NULL) AS deepestBuyFlowStep,
                max(isTmoNetworkSession) AS isTmoNetwork
            FROM weekSessions
            GROUP BY visitorId, weekStartDate
        )
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly
        REPLACE WHERE weekStartDate BETWEEN v_weekStartFrom AND v_weekStartTo
        SELECT
            k.weekStartDate,
            date_add(k.weekStartDate, 6) AS weekEndDate,
            k.visitorId,
            a.identitySource,
            coalesce(l.lob, 'Other') AS lob,
            a.lobList,
            coalesce(p.platform, '(not set)') AS platform,
            a.platformList,
            coalesce(a.prospectVsBase, 'Unknown') AS prospectVsBase,
            coalesce(a.authState, '(not set)') AS authState,
            coalesce(a.attributedTouch.channel, '(not set)') AS channel,
            CASE
                WHEN a.attributedTouch.campaignCode IS NULL THEN '(not set)'
                WHEN a.attributedTouch.campaignName IS NOT NULL
                    THEN concat(a.attributedTouch.campaignCode, ' · ', a.attributedTouch.campaignName)
                ELSE a.attributedTouch.campaignCode
            END AS campaign,
            a.attributedTouch.campaignCode AS campaignCode,
            coalesce(a.attributedTouch.entryPage, '(not set)') AS entryPage,
            coalesce(a.attributedTouch.utmSource, '(not set)') AS utmSource,
            coalesce(a.attributedTouch.utmMedium, '(not set)') AS utmMedium,
            coalesce(a.attributedTouch.utmCampaign, '(not set)') AS utmCampaign,
            coalesce(c.pageCategory, '(not set)') AS pageCategory,
            coalesce(d.device, 'Unknown') AS device,
            coalesce(a.deepestBuyFlowStep, 'Did not enter buy flow') AS buyFlowStep,
            '(not available)' AS region,
            coalesce(a.isTmoNetwork, 0) AS isTmoNetwork,
            v_processedAt AS silverProcessedAt
        FROM keys k
        JOIN agg a
          ON a.visitorId = k.visitorId
         AND a.weekStartDate = k.weekStartDate
        LEFT JOIN lobResolved l
          ON l.visitorId = k.visitorId
         AND l.weekStartDate = k.weekStartDate
        LEFT JOIN platformResolved p
          ON p.visitorId = k.visitorId
         AND p.weekStartDate = k.weekStartDate
        LEFT JOIN deviceResolved d
          ON d.visitorId = k.visitorId
         AND d.weekStartDate = k.weekStartDate
        LEFT JOIN categoryResolved c
          ON c.visitorId = k.visitorId
         AND c.weekStartDate = k.weekStartDate;

        SELECT
            'SUCCESS' AS status,
            v_weekStartFrom AS rebuiltWeekStartFrom,
            v_weekStartTo AS rebuiltWeekStartTo,
            v_weekEndTo AS latestWeekEndDate,
            CASE WHEN v_asOfDate < v_weekEndTo THEN TRUE ELSE FALSE END AS latestWeekIsPartial,
            'prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly' AS targetObject;
    END IF;
END;

-- Test:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_silver_attributesPerVisitorWeek_weekly(
--   p_asOfDate => DATE '2026-09-28', p_weeksToRebuild => 1, p_validateOnly => TRUE);
-- CALL prdrzranalytics.lab42.sdi_sp_mip_silver_attributesPerVisitorWeek_weekly(
--   p_asOfDate => DATE '2026-09-28', p_weeksToRebuild => 1, p_validateOnly => FALSE);
