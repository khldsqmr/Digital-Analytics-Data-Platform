-- ============================================================================

-- FILE  : 02a_sdi_sp_mip_silver_attributesPerSession_daily.sql

-- LAYER : SILVER

-- RUNTIME WRITE NOTE:
--   The scoped overwrite is executed through dynamic SQL using make_date(...)
--   expressions generated from validated local DATE values. This is the same
--   runtime-safe pattern proven in Bronze and avoids local-variable resolution
--   and DATE-binding issues inside REPLACE WHERE.
--   The transformation query remains unchanged and inserts BY NAME.
--
-- PURPOSE:

--   One row per NBV (non-bounced) OPEN/CLOSED session with attributes/actions.

--

-- NBV RULE:

--   SUM(isPageView) > 1 across the complete loaded session hit set.

--

-- PERFORMANCE:

--   - Reads only detailsPerHit; no repeated Bronze/marketing joins.

--   - Filters directly on persisted sessionStartDatePst.

--   - Uses one full session aggregation plus one narrow LOB/page-view aggregation.

--

-- CHANNEL CONTRACT:

--   channel is resolved from the exact UDI channel_name values. No marketing

--   channel categories are rolled together. Because channel_name is currently

--   non-sticky upstream, this table keeps one interim session value until the

--   upstream persistence fix lands.

--

-- TEMPORARY GEO CONTRACT:

--   Session region remains the temporary Web postal-code / App country field.

--   No METRO/RETAIL correction or ZIP-to-region mapping is applied here.

--

-- PREFLIGHT / VALIDATION CONTRACT:

--   p_validateOnly=TRUE verifies that detailsPerHit contains every requested

--   session-start date before writing. p_eventWindowDays therefore describes the

--   sessionStartDatePst rebuild window in this session-grain procedure.

--

-- PEER / IMPACT CONTRACT:

--   No peer-set or impact-on-topline calculation is persisted at session grain.

-- ============================================================================

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_silver_attributesPerSession_daily(

    IN p_asOfDate DATE DEFAULT NULL,

    IN p_eventWindowDays INT DEFAULT 1,

    IN p_validateOnly BOOLEAN DEFAULT FALSE

)

LANGUAGE SQL

SQL SECURITY INVOKER

MODIFIES SQL DATA

COMMENT 'Silver NBV session attribute layer. One row per OPEN/CLOSED session with 2+ real page views.'

AS

BEGIN

    DECLARE v_asOfDate DATE DEFAULT coalesce(

        p_asOfDate,

        date_add(to_date(from_utc_timestamp(current_timestamp(),'America/Los_Angeles')),-1)

    );

    DECLARE v_windowEnd DATE DEFAULT v_asOfDate;

    DECLARE v_windowStart DATE DEFAULT date_add(v_asOfDate,-(p_eventWindowDays-1));

    DECLARE v_processedAt TIMESTAMP DEFAULT current_timestamp();

    DECLARE v_sourceSessionDateCount BIGINT DEFAULT 0;
    DECLARE v_writeSql STRING;
    DECLARE v_scopeStartSql STRING;
    DECLARE v_scopeEndSql STRING;


    IF p_eventWindowDays IS NULL OR p_eventWindowDays<1 THEN

        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='p_eventWindowDays must be >= 1.';

    END IF;

    SET v_sourceSessionDateCount=(

        SELECT COUNT(DISTINCT sessionStartDatePst)

        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily

        WHERE sessionStartDatePst BETWEEN v_windowStart AND v_windowEnd

    );

    IF v_sourceSessionDateCount<>p_eventWindowDays THEN

        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Silver detailsPerHit does not contain every requested session-start date.';

    END IF;

    IF p_validateOnly THEN

        SELECT

            'VALIDATION_ONLY' AS status,

            v_windowStart AS requestedSessionStartDate,

            v_windowEnd AS requestedSessionEndDate,

            v_sourceSessionDateCount AS sourceSessionStartDateCount,

            'prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily' AS sourceObject,

            'NBV will be resolved as SUM(isPageView)>1 at session grain. No table was modified.' AS message;

    ELSE

        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily (

            sessionId STRING,

            canonicalUserId STRING,

            resolvedIdentityId STRING,

            visitorId STRING,

            identitySource STRING,

            identityStatus STRING,

            sessionStatus STRING,

            sessionStartTsUtc TIMESTAMP,

            sessionEndTsUtc TIMESTAMP,

            sessionStartTsPst TIMESTAMP,

            sessionStartDatePst DATE,

            weekStartDate DATE,

            weekEndDate DATE,

            pageViews BIGINT,

            isNonBounced INT COMMENT 'Always 1 in this table',

            lobList ARRAY<STRING>,

            lobPageViews MAP<STRING,BIGINT>,

            platform STRING,

            device STRING,

            region STRING COMMENT 'Temporary session geography placeholder: Web=geo_postal_code, App=attribute_country; not a normalized geographic Region',

            geoContext STRING COMMENT 'Source-aware Web-postal/App-country context; first session hit preferred, otherwise earliest non-null hit',

            prospectVsBase STRING,

            prospectVsBaseRank INT,

            authState STRING,

            authStateRank INT,

            channel STRING COMMENT 'Resolved exact UDI channel_name value; no channel-category roll-up',

            campaignCode STRING,

            campaignName STRING,

            campaignCategory STRING,

            campaignIsActive BOOLEAN,

            entryPage STRING,

            utmSource STRING,

            utmMedium STRING,

            utmCampaign STRING,

            deepestBuyFlowStep STRING,

            deepestBuyFlowStepOrder INT,

            hasBuyFlow INT,

            hasConfigure INT,

            hasCheckoutStart INT,

            hasOrder INT,

            hasAcquisitionOrder INT,

            hasAssistedOrder INT,

            hasVrCall INT,

            hasVrChat INT,

            hasStoreLocator INT,

            orderCount BIGINT,

            isTmoNetworkSession INT,

            silverProcessedAt TIMESTAMP

        )

        USING DELTA

        CLUSTER BY (sessionStartDatePst)

        COMMENT 'Silver: one row per NBV OPEN/CLOSED session. NBV = >1 real page view.';

                -- --------------------------------------------------------------------
        -- Atomic selective overwrite for the requested Silver scope.
        -- --------------------------------------------------------------------
        SET v_scopeStartSql = concat(
            'make_date(',
            cast(year(v_windowStart) AS STRING), ',',
            cast(month(v_windowStart) AS STRING), ',',
            cast(day(v_windowStart) AS STRING),
            ')'
        );

        SET v_scopeEndSql = concat(
            'make_date(',
            cast(year(v_windowEnd) AS STRING), ',',
            cast(month(v_windowEnd) AS STRING), ',',
            cast(day(v_windowEnd) AS STRING),
            ')'
        );

        SET v_writeSql = concat(
            'WITH scopedHits AS (
            SELECT
                sessionId,
                canonicalUserId,
                resolvedIdentityId,
                visitorId,
                identitySource,
                identityStatus,
                sessionStatus,
                sessionStartTsUtc,
                sessionEndTsUtc,
                sessionStartTsPst,
                sessionStartDatePst,
                weekStartDate,
                weekEndDate,
                eventTimestampUtc,
                hitNumberInSession,
                lob,
                platform,
                device,
                geoRegion,
                geoContext,
                customerType,
                customerTypeRank,
                authState,
                authStateRank,
                channelName,
                campaignCode,
                campaignName,
                campaignCategory,
                campaignIsActive,
                sessionEntryPageUrlPath,
                utmSource,
                utmMedium,
                utmCampaign,
                buyFlowStep,
                buyFlowStepOrder,
                isPageView,
                isOrder,
                isVrCall,
                isVrChat,
                isStoreLocator,
                isConfigure,
                isCheckoutStart,
                isBuyFlow,
                isAssistedOrder,
                isTmoNetwork
            FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily
            WHERE sessionStartDatePst BETWEEN ',
            v_scopeStartSql,
            ' AND ',
            v_scopeEndSql,
            '
        ),
        lobPvRaw AS (
            SELECT
                sessionId,
                coalesce(lob,''Other'') AS lob,
                SUM(CAST(isPageView AS BIGINT)) AS pageViews
            FROM scopedHits
            GROUP BY sessionId,coalesce(lob,''Other'')
        ),
        lobPv AS (
            SELECT
                sessionId,
                map_from_entries(
                    collect_list(named_struct(''key'',lob,''value'',pageViews))
                ) AS lobPageViews
            FROM lobPvRaw
            GROUP BY sessionId
        ),
        agg AS (
            SELECT
                sessionId,
                max(canonicalUserId) AS canonicalUserId,
                min_by(resolvedIdentityId,eventTimestampUtc)
                    FILTER (WHERE resolvedIdentityId IS NOT NULL) AS resolvedIdentityId,
                min_by(identitySource,eventTimestampUtc)
                    FILTER (WHERE identitySource IS NOT NULL) AS fallbackIdentitySource,
                max(identityStatus) AS identityStatus,
                max(sessionStatus) AS sessionStatus,
                max(sessionStartTsUtc) AS sessionStartTsUtc,
                max(sessionEndTsUtc) AS sessionEndTsUtc,
                max(sessionStartTsPst) AS sessionStartTsPst,
                max(sessionStartDatePst) AS sessionStartDatePst,
                max(weekStartDate) AS weekStartDate,
                max(weekEndDate) AS weekEndDate,
                SUM(CAST(isPageView AS BIGINT)) AS pageViews,
                array_sort(collect_set(coalesce(lob,''Other''))) AS lobList,
                coalesce(max(platform),''(not set)'') AS platform,
                max_by(
                    coalesce(device,''Unknown''),
                    struct(CAST(isPageView AS INT),eventTimestampUtc,coalesce(device,''Unknown''))
                ) AS device,
                coalesce(
                    max(CASE WHEN hitNumberInSession=1 THEN nullif(trim(geoRegion),'''') END),
                    min_by(nullif(trim(geoRegion),''''),eventTimestampUtc)
                        FILTER (WHERE nullif(trim(geoRegion),'''') IS NOT NULL)
                ) AS region,
                coalesce(
                    max(CASE WHEN hitNumberInSession=1 THEN geoContext END),
                    min_by(geoContext,eventTimestampUtc)
                        FILTER (WHERE geoContext IS NOT NULL)
                ) AS geoContext,
                max_by(customerType,struct(customerTypeRank,eventTimestampUtc)) AS prospectVsBase,
                max(customerTypeRank) AS prospectVsBaseRank,
                max_by(authState,struct(authStateRank,eventTimestampUtc)) AS authState,
                max(authStateRank) AS authStateRank,
                max(channelName) AS channel,
                max_by(
                    named_struct(
                        ''campaignCode'',campaignCode,
                        ''campaignName'',campaignName,
                        ''campaignCategory'',campaignCategory,
                        ''campaignIsActive'',campaignIsActive
                    ),
                    coalesce(campaignCode,'''')
                ) FILTER (WHERE campaignCode IS NOT NULL) AS campaignTouch,
                max(sessionEntryPageUrlPath) AS entryPage,
                max(utmSource) AS utmSource,
                max(utmMedium) AS utmMedium,
                max(utmCampaign) AS utmCampaign,
                max_by(
                    buyFlowStep,
                    struct(coalesce(buyFlowStepOrder,-1),eventTimestampUtc,coalesce(buyFl',
            'owStep,''''))
                ) FILTER (WHERE buyFlowStep IS NOT NULL) AS deepestBuyFlowStep,
                max(buyFlowStepOrder) AS deepestBuyFlowStepOrder,
                max(isBuyFlow) AS hasBuyFlow,
                max(isConfigure) AS hasConfigure,
                max(isCheckoutStart) AS hasCheckoutStart,
                max(isOrder) AS hasOrder,
                max(CASE WHEN isOrder=1 AND customerType=''Prospect'' THEN 1 ELSE 0 END) AS hasAcquisitionOrder,
                max(isAssistedOrder) AS hasAssistedOrder,
                max(isVrCall) AS hasVrCall,
                max(isVrChat) AS hasVrChat,
                max(isStoreLocator) AS hasStoreLocator,
                SUM(CAST(isOrder AS BIGINT)) AS orderCount,
                max(isTmoNetwork) AS isTmoNetworkSession
            FROM scopedHits
            GROUP BY sessionId
            HAVING SUM(CAST(isPageView AS BIGINT))>1
        )
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily BY NAME
        REPLACE WHERE sessionStartDatePst BETWEEN ',
            v_scopeStartSql,
            ' AND ',
            v_scopeEndSql,
            '
        SELECT
            a.sessionId,
            a.canonicalUserId,
            a.resolvedIdentityId,
            coalesce(a.canonicalUserId,a.resolvedIdentityId) AS visitorId,
            CASE
                WHEN a.canonicalUserId IS NOT NULL THEN ''canonicalUserId''
                ELSE a.fallbackIdentitySource
            END AS identitySource,
            a.identityStatus,
            a.sessionStatus,
            a.sessionStartTsUtc,
            a.sessionEndTsUtc,
            a.sessionStartTsPst,
            a.sessionStartDatePst,
            a.weekStartDate,
            a.weekEndDate,
            a.pageViews,
            1 AS isNonBounced,
            a.lobList,
            lp.lobPageViews,
            a.platform,
            coalesce(a.device,''Unknown'') AS device,
            coalesce(a.region,''(not available)'') AS region,
            a.geoContext,
            coalesce(a.prospectVsBase,''Unknown'') AS prospectVsBase,
            coalesce(a.prospectVsBaseRank,0) AS prospectVsBaseRank,
            coalesce(a.authState,''(not set)'') AS authState,
            coalesce(a.authStateRank,-1) AS authStateRank,
            coalesce(a.channel,''(not set)'') AS channel,
            a.campaignTouch.campaignCode AS campaignCode,
            a.campaignTouch.campaignName AS campaignName,
            a.campaignTouch.campaignCategory AS campaignCategory,
            a.campaignTouch.campaignIsActive AS campaignIsActive,
            coalesce(nullif(trim(a.entryPage),''''),''(not set)'') AS entryPage,
            coalesce(nullif(trim(a.utmSource),''''),''(not set)'') AS utmSource,
            coalesce(nullif(trim(a.utmMedium),''''),''(not set)'') AS utmMedium,
            coalesce(nullif(trim(a.utmCampaign),''''),''(not set)'') AS utmCampaign,
            a.deepestBuyFlowStep,
            a.deepestBuyFlowStepOrder,
            a.hasBuyFlow,
            a.hasConfigure,
            a.hasCheckoutStart,
            a.hasOrder,
            a.hasAcquisitionOrder,
            a.hasAssistedOrder,
            a.hasVrCall,
            a.hasVrChat,
            a.hasStoreLocator,
            a.orderCount,
            a.isTmoNetworkSession,
            current_timestamp() AS silverProcessedAt
        FROM agg a
        LEFT JOIN lobPv lp
          ON lp.sessionId=a.sessionId'
        );

        EXECUTE IMMEDIATE v_writeSql;


        SELECT

            'SUCCESS' AS status,

            v_windowStart AS loadedSessionStartDate,

            v_windowEnd AS loadedSessionEndDate,

            'prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily' AS targetObject;

    END IF;

END;

-- ============================================================================

-- DEVELOPMENT / TEST EXAMPLES

-- Run these statements separately after deploying the procedure.

-- ============================================================================

-- --------------------------------------------------------------------------

-- A. PREFLIGHT ONLY

-- --------------------------------------------------------------------------

-- CALL prdrzranalytics.lab42.sdi_sp_mip_silver_attributesPerSession_daily(

--     p_asOfDate        => DATE '2026-09-28',

--     p_eventWindowDays => 1,

--     p_validateOnly    => TRUE

-- );

-- --------------------------------------------------------------------------

-- B. EXECUTE / REBUILD ONE SESSION-START DAY

-- --------------------------------------------------------------------------

-- CALL prdrzranalytics.lab42.sdi_sp_mip_silver_attributesPerSession_daily(

--     p_asOfDate        => DATE '2026-09-28',

--     p_eventWindowDays => 1,

--     p_validateOnly    => FALSE

-- );

-- --------------------------------------------------------------------------

-- C. VALIDATION 1: NBV SESSION CONTRACT

-- Expected:

--   minPageViews >= 2

--   invalidNbvSessions = 0

--   invalidSessionStatusRows = 0

-- --------------------------------------------------------------------------

-- SELECT

--     sessionStartDatePst,

--     COUNT(*) AS nbvSessions,

--     MIN(pageViews) AS minPageViews,

--     COUNT_IF(pageViews <= 1) AS invalidNbvSessions,

--     COUNT_IF(isNonBounced <> 1) AS invalidNonBounceFlags,

--     COUNT_IF(sessionStatus NOT IN ('OPEN','CLOSED')) AS invalidSessionStatusRows,

--     COUNT_IF(region IS NULL OR region='(not available)') AS missingRegionSessions

-- FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily

-- WHERE sessionStartDatePst = DATE '2026-09-28'

-- GROUP BY sessionStartDatePst;

-- --------------------------------------------------------------------------

-- D. VALIDATION 2: SESSION GRAIN UNIQUENESS

-- Expected: no rows.

-- --------------------------------------------------------------------------

-- SELECT

--     sessionId,

--     COUNT(*) AS rowCount

-- FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily

-- WHERE sessionStartDatePst = DATE '2026-09-28'

-- GROUP BY sessionId

-- HAVING COUNT(*) > 1

-- ORDER BY rowCount DESC

-- LIMIT 100;

-- --------------------------------------------------------------------------

-- E. VALIDATION 3: EXACT NBV RECONCILIATION TO detailsPerHit

-- Heavier validation; run when validating a new deployment/backfill.

-- Expected: rowDiff = 0.

-- --------------------------------------------------------------------------

-- WITH expected AS (

--     SELECT

--         sessionId

--     FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily

--     WHERE sessionStartDatePst = DATE '2026-09-28'

--     GROUP BY sessionId

--     HAVING SUM(isPageView) > 1

-- ),

-- actual AS (

--     SELECT sessionId

--     FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily

--     WHERE sessionStartDatePst = DATE '2026-09-28'

-- )

-- SELECT

--     (SELECT COUNT(*) FROM expected) AS expectedNbvSessions,

--     (SELECT COUNT(*) FROM actual) AS actualNbvSessions,

--     (SELECT COUNT(*) FROM actual) - (SELECT COUNT(*) FROM expected) AS rowDiff;
