-- ============================================================================
-- FILE  : 02_sdi_sp_mip_silver_attributesPerSession_daily.sql
-- LAYER : SILVER
-- PURPOSE:
--   One row per non-bounced OPEN/CLOSED session with attributes and action flags.
-- ============================================================================

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_silver_attributesPerSession_daily(
    IN p_asOfDate        DATE    DEFAULT NULL,
    IN p_eventWindowDays INT     DEFAULT 1,
    IN p_validateOnly    BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Silver session attribute layer. Keeps non-bounced OPEN/CLOSED sessions and derives dashboard/session attributes.'
AS
BEGIN
    DECLARE v_asOfDate DATE DEFAULT coalesce(
        p_asOfDate,
        date_add(
            to_date(
                from_utc_timestamp(
                    current_timestamp(),
                    'America/Los_Angeles'
                )
            ),
            -1
        )
    );

    DECLARE v_windowStart DATE;
    DECLARE v_windowEnd DATE;
    DECLARE v_processedAt TIMESTAMP DEFAULT current_timestamp();

    IF p_eventWindowDays IS NULL OR p_eventWindowDays < 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'p_eventWindowDays must be >= 1.';
    END IF;

    SET v_windowEnd = v_asOfDate;
    SET v_windowStart = date_add(
        v_asOfDate,
        -(p_eventWindowDays - 1)
    );

    -- Validate that eligible Bronze sessions exist.
    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessions_daily
        WHERE session_start_date
              BETWEEN date_add(v_windowStart, -1)
                  AND date_add(v_windowEnd, 1)
          AND session_status IN ('OPEN', 'CLOSED')
          AND to_date(
                from_utc_timestamp(
                    try_cast(session_start_time AS TIMESTAMP),
                    'America/Los_Angeles'
                )
              ) BETWEEN v_windowStart AND v_windowEnd
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Bronze session summary returned no eligible OPEN/CLOSED sessions for the requested Silver window.';
    END IF;

    -- Validate that Silver hit-level details exist.
    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily
        WHERE eventDate
              BETWEEN date_add(v_windowStart, -1)
                  AND date_add(v_windowEnd, 2)
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Silver detailsPerHit returned no rows for the session-hit lookup window.';
    END IF;

    -- Validate that the marketing-code snapshot contains data.
    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlMarketingCodes_snapshot
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Bronze marketing-code snapshot is empty.';
    END IF;

    IF p_validateOnly THEN
        SELECT
            'VALIDATION_ONLY' AS status,
            v_windowStart AS requestedSessionStartDate,
            v_windowEnd AS requestedSessionEndDate,
            date_add(v_windowStart, -1) AS widenedSessionSourceStart,
            date_add(v_windowEnd, 1) AS widenedSessionSourceEnd,
            date_add(v_windowStart, -1) AS hitLookupStart,
            date_add(v_windowEnd, 2) AS hitLookupEnd,
            'No Silver table was created or modified.' AS message;

    ELSE
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily (
            sessionId                 STRING,
            canonicalUserId           STRING,
            resolvedIdentityId        STRING,
            visitorId                 STRING,
            identitySource            STRING,
            identityStatus            STRING,
            sessionStatus             STRING,
            sessionStartTsUtc         TIMESTAMP,
            sessionEndTsUtc           TIMESTAMP,
            sessionStartTsPst         TIMESTAMP,
            sessionStartDatePst       DATE,
            weekStartDate             DATE,
            weekEndDate               DATE,
            pageViews                 BIGINT,
            isNonBounced              INT COMMENT 'Always 1 in this table',
            lobList                   ARRAY<STRING>,
            lobPageViews              MAP<STRING, BIGINT>,
            platform                  STRING,
            platformPageViews         MAP<STRING, BIGINT>,
            device                    STRING,
            devicePageViews           MAP<STRING, BIGINT>,
            prospectVsBase            STRING,
            prospectVsBaseRank        INT,
            authState                 STRING,
            authStateRank             INT,
            channel                   STRING COMMENT 'Interim single channel resolution using MAX(channelName) until upstream stickiness is fixed',
            campaignCode              STRING,
            campaignName              STRING,
            campaignCategory          STRING,
            campaignIsActive          BOOLEAN,
            entryPage                 STRING,
            utmSource                 STRING,
            utmMedium                 STRING,
            utmCampaign               STRING,
            deepestBuyFlowStep        STRING,
            deepestBuyFlowStepOrder   INT,
            hasBuyFlow                INT,
            hasConfigure              INT,
            hasCheckoutStart          INT,
            hasOrder                  INT,
            hasAcquisitionOrder       INT,
            hasAssistedOrder          INT,
            hasVrCall                 INT,
            hasVrChat                 INT,
            hasStoreLocator           INT,
            orderCount                BIGINT,
            isTmoNetworkSession       INT,
            silverProcessedAt         TIMESTAMP
        )
        USING DELTA
        CLUSTER BY (sessionStartDatePst)
        COMMENT 'Silver: one row per non-bounced OPEN/CLOSED session; manager-defined session attribute layer plus MIP action flags.';

        WITH candidateSessions AS (
            SELECT
                cast(session_id AS STRING) AS sessionId,
                nullif(
                    trim(cast(canonical_user_id AS STRING)),
                    ''
                ) AS canonicalUserId,
                cast(identity_status AS STRING) AS identityStatus,
                cast(session_status AS STRING) AS sessionStatus,
                try_cast(session_start_time AS TIMESTAMP) AS sessionStartTsUtc,
                try_cast(session_end_time AS TIMESTAMP) AS sessionEndTsUtc,
                from_utc_timestamp(
                    try_cast(session_start_time AS TIMESTAMP),
                    'America/Los_Angeles'
                ) AS sessionStartTsPst,
                cast(entry_page_url_path AS STRING) AS entryPage,
                cast(entry_page_url_full AS STRING) AS entryPageUrlFull
            FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessions_daily
            WHERE session_start_date
                  BETWEEN date_add(v_windowStart, -1)
                      AND date_add(v_windowEnd, 1)
              AND session_status IN ('OPEN', 'CLOSED')
              AND to_date(
                    from_utc_timestamp(
                        try_cast(session_start_time AS TIMESTAMP),
                        'America/Los_Angeles'
                    )
                  ) BETWEEN v_windowStart AND v_windowEnd
            QUALIFY row_number() OVER (
                PARTITION BY session_id
                ORDER BY
                    try_cast(session_end_time AS TIMESTAMP) DESC NULLS LAST,
                    _ingestedAt DESC
            ) = 1
        ),

        marketingCodeDedup AS (
            SELECT
                cast(MKT_CODE AS STRING) AS MKT_CODE,
                cast(MKT_CODE_NAME AS STRING) AS MKT_CODE_NAME,
                cast(Category AS STRING) AS Category,
                try_cast(is_active AS BOOLEAN) AS is_active
            FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlMarketingCodes_snapshot
            QUALIFY row_number() OVER (
                PARTITION BY cast(MKT_CODE AS STRING)
                ORDER BY
                    try_cast(is_active AS BOOLEAN) DESC NULLS LAST,
                    cast(MKT_CODE_NAME AS STRING) ASC NULLS LAST
            ) = 1
        ),

        hits AS (
            SELECT
                h.*
            FROM candidateSessions AS s
            INNER JOIN prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily AS h
                ON h.sessionId = s.sessionId
            WHERE h.eventDate
                  BETWEEN date_add(v_windowStart, -1)
                      AND date_add(v_windowEnd, 2)
        ),

        lobPvRaw AS (
            SELECT
                h.sessionId,
                coalesce(
                    nullif(trim(h.lob), ''),
                    'Other'
                ) AS lob,
                sum(coalesce(h.isPageView, 0)) AS pageViews
            FROM hits AS h
            GROUP BY
                h.sessionId,
                coalesce(
                    nullif(trim(h.lob), ''),
                    'Other'
                )
        ),

        lobPv AS (
            SELECT
                l.sessionId,
                array_sort(
                    collect_set(l.lob)
                ) AS lobList,
                map_from_entries(
                    collect_list(
                        struct(l.lob, l.pageViews)
                    )
                ) AS lobPageViews
            FROM lobPvRaw AS l
            GROUP BY l.sessionId
        ),

        platformPvRaw AS (
            SELECT
                h.sessionId,
                coalesce(
                    nullif(trim(h.platform), ''),
                    '(not set)'
                ) AS platform,
                sum(coalesce(h.isPageView, 0)) AS pageViews
            FROM hits AS h
            GROUP BY
                h.sessionId,
                coalesce(
                    nullif(trim(h.platform), ''),
                    '(not set)'
                )
        ),

        platformPv AS (
            SELECT
                p.sessionId,
                max_by(
                    p.platform,
                    struct(p.pageViews, p.platform)
                ) AS platform,
                map_from_entries(
                    collect_list(
                        struct(p.platform, p.pageViews)
                    )
                ) AS platformPageViews
            FROM platformPvRaw AS p
            GROUP BY p.sessionId
        ),

        devicePvRaw AS (
            SELECT
                h.sessionId,
                coalesce(
                    nullif(trim(h.device), ''),
                    'Unknown'
                ) AS device,
                sum(coalesce(h.isPageView, 0)) AS pageViews
            FROM hits AS h
            GROUP BY
                h.sessionId,
                coalesce(
                    nullif(trim(h.device), ''),
                    'Unknown'
                )
        ),

        devicePv AS (
            SELECT
                d.sessionId,
                max_by(
                    d.device,
                    struct(d.pageViews, d.device)
                ) AS device,
                map_from_entries(
                    collect_list(
                        struct(d.device, d.pageViews)
                    )
                ) AS devicePageViews
            FROM devicePvRaw AS d
            GROUP BY d.sessionId
        ),

        agg AS (
            SELECT
                h.sessionId,

                min_by(
                    h.resolvedIdentityId,
                    h.eventTimestampUtc
                ) FILTER (
                    WHERE h.resolvedIdentityId IS NOT NULL
                ) AS resolvedIdentityId,

                min_by(
                    h.identitySource,
                    h.eventTimestampUtc
                ) FILTER (
                    WHERE h.identitySource IS NOT NULL
                ) AS identitySource,

                count(*) AS hitCount,

                sum(
                    coalesce(h.isPageView, 0)
                ) AS pageViews,

                max_by(
                    h.customerType,
                    struct(
                        coalesce(h.customerTypeRank, 0),
                        h.eventTimestampUtc
                    )
                ) FILTER (
                    WHERE h.customerType IS NOT NULL
                ) AS prospectVsBase,

                max(
                    h.customerTypeRank
                ) AS prospectVsBaseRank,

                max_by(
                    h.authState,
                    struct(
                        coalesce(h.authStateRank, -1),
                        h.eventTimestampUtc
                    )
                ) FILTER (
                    WHERE h.authState IS NOT NULL
                ) AS authState,

                max(
                    h.authStateRank
                ) AS authStateRank,

                max(
                    h.channelName
                ) AS channel,

                max(
                    h.campaignCode
                ) AS campaignCode,

                max_by(
                    h.buyFlowStep,
                    struct(
                        coalesce(h.buyFlowStepOrder, -1),
                        h.eventTimestampUtc,
                        coalesce(h.buyFlowStep, '')
                    )
                ) FILTER (
                    WHERE h.buyFlowStep IS NOT NULL
                ) AS deepestBuyFlowStep,

                max(
                    h.buyFlowStepOrder
                ) AS deepestBuyFlowStepOrder,

                max(
                    coalesce(h.isBuyFlow, 0)
                ) AS hasBuyFlow,

                max(
                    coalesce(h.isConfigure, 0)
                ) AS hasConfigure,

                max(
                    coalesce(h.isCheckoutStart, 0)
                ) AS hasCheckoutStart,

                max(
                    coalesce(h.isOrder, 0)
                ) AS hasOrder,

                max(
                    CASE
                        WHEN coalesce(h.isOrder, 0) = 1
                         AND h.customerType = 'Prospect'
                            THEN 1
                        ELSE 0
                    END
                ) AS hasAcquisitionOrder,

                max(
                    coalesce(h.isAssistedOrder, 0)
                ) AS hasAssistedOrder,

                max(
                    coalesce(h.isVrCall, 0)
                ) AS hasVrCall,

                max(
                    coalesce(h.isVrChat, 0)
                ) AS hasVrChat,

                max(
                    coalesce(h.isStoreLocator, 0)
                ) AS hasStoreLocator,

                sum(
                    coalesce(h.isOrder, 0)
                ) AS orderCount,

                max(
                    coalesce(h.isTmoNetwork, 0)
                ) AS isTmoNetworkSession

            FROM hits AS h
            GROUP BY h.sessionId
        ),

        nonBounced AS (
            SELECT
                a.*
            FROM agg AS a
            WHERE a.pageViews > 1
        )

        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily
        REPLACE WHERE sessionStartDatePst
                      BETWEEN v_windowStart AND v_windowEnd
        SELECT
            s.sessionId,
            s.canonicalUserId,
            a.resolvedIdentityId,

            coalesce(
                s.canonicalUserId,
                a.resolvedIdentityId
            ) AS visitorId,

            CASE
                WHEN s.canonicalUserId IS NOT NULL
                    THEN 'canonicalUserId'
                ELSE a.identitySource
            END AS identitySource,

            s.identityStatus,
            s.sessionStatus,
            s.sessionStartTsUtc,
            s.sessionEndTsUtc,
            s.sessionStartTsPst,

            to_date(
                s.sessionStartTsPst
            ) AS sessionStartDatePst,

            date_add(
                to_date(s.sessionStartTsPst),
                1 - dayofweek(to_date(s.sessionStartTsPst))
            ) AS weekStartDate,

            date_add(
                to_date(s.sessionStartTsPst),
                7 - dayofweek(to_date(s.sessionStartTsPst))
            ) AS weekEndDate,

            cast(a.pageViews AS BIGINT) AS pageViews,
            1 AS isNonBounced,

            coalesce(
                lp.lobList,
                array('Other')
            ) AS lobList,

            coalesce(
                lp.lobPageViews,
                map('Other', cast(0 AS BIGINT))
            ) AS lobPageViews,

            coalesce(
                pp.platform,
                '(not set)'
            ) AS platform,

            coalesce(
                pp.platformPageViews,
                map('(not set)', cast(0 AS BIGINT))
            ) AS platformPageViews,

            coalesce(
                dp.device,
                'Unknown'
            ) AS device,

            coalesce(
                dp.devicePageViews,
                map('Unknown', cast(0 AS BIGINT))
            ) AS devicePageViews,

            coalesce(
                a.prospectVsBase,
                'Unknown'
            ) AS prospectVsBase,

            coalesce(
                a.prospectVsBaseRank,
                0
            ) AS prospectVsBaseRank,

            coalesce(
                a.authState,
                '(not set)'
            ) AS authState,

            coalesce(
                a.authStateRank,
                -1
            ) AS authStateRank,

            coalesce(
                a.channel,
                '(not set)'
            ) AS channel,

            a.campaignCode,
            m.MKT_CODE_NAME AS campaignName,
            m.Category AS campaignCategory,
            m.is_active AS campaignIsActive,

            coalesce(
                nullif(trim(s.entryPage), ''),
                '(not set)'
            ) AS entryPage,

            coalesce(
                nullif(
                    try_parse_url(
                        CASE
                            WHEN lower(
                                coalesce(s.entryPageUrlFull, '')
                            ) LIKE 'http%'
                                THEN s.entryPageUrlFull

                            WHEN nullif(
                                trim(s.entryPageUrlFull),
                                ''
                            ) IS NOT NULL
                                THEN concat(
                                    'https://',
                                    trim(s.entryPageUrlFull)
                                )

                            ELSE NULL
                        END,
                        'QUERY',
                        'utm_source'
                    ),
                    ''
                ),
                '(not set)'
            ) AS utmSource,

            coalesce(
                nullif(
                    try_parse_url(
                        CASE
                            WHEN lower(
                                coalesce(s.entryPageUrlFull, '')
                            ) LIKE 'http%'
                                THEN s.entryPageUrlFull

                            WHEN nullif(
                                trim(s.entryPageUrlFull),
                                ''
                            ) IS NOT NULL
                                THEN concat(
                                    'https://',
                                    trim(s.entryPageUrlFull)
                                )

                            ELSE NULL
                        END,
                        'QUERY',
                        'utm_medium'
                    ),
                    ''
                ),
                '(not set)'
            ) AS utmMedium,

            coalesce(
                nullif(
                    try_parse_url(
                        CASE
                            WHEN lower(
                                coalesce(s.entryPageUrlFull, '')
                            ) LIKE 'http%'
                                THEN s.entryPageUrlFull

                            WHEN nullif(
                                trim(s.entryPageUrlFull),
                                ''
                            ) IS NOT NULL
                                THEN concat(
                                    'https://',
                                    trim(s.entryPageUrlFull)
                                )

                            ELSE NULL
                        END,
                        'QUERY',
                        'utm_campaign'
                    ),
                    ''
                ),
                '(not set)'
            ) AS utmCampaign,

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
            cast(a.orderCount AS BIGINT) AS orderCount,
            a.isTmoNetworkSession,
            v_processedAt AS silverProcessedAt

        FROM candidateSessions AS s

        INNER JOIN nonBounced AS a
            ON a.sessionId = s.sessionId

        LEFT JOIN lobPv AS lp
            ON lp.sessionId = s.sessionId

        LEFT JOIN platformPv AS pp
            ON pp.sessionId = s.sessionId

        LEFT JOIN devicePv AS dp
            ON dp.sessionId = s.sessionId

        LEFT JOIN marketingCodeDedup AS m
            ON m.MKT_CODE = a.campaignCode;

        SELECT
            'SUCCESS' AS status,
            v_windowStart AS loadedSessionStartDate,
            v_windowEnd AS loadedSessionEndDate,
            'prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily' AS targetObject;
    END IF;
END;

-- ============================================================================
-- TEST: Validation only
-- ============================================================================

-- CALL prdrzranalytics.lab42.sdi_sp_mip_silver_attributesPerSession_daily(
--     p_asOfDate        => DATE '2026-09-28',
--     p_eventWindowDays => 1,
--     p_validateOnly    => TRUE
-- );

-- ============================================================================
-- TEST: Execute load
-- ============================================================================

-- CALL prdrzranalytics.lab42.sdi_sp_mip_silver_attributesPerSession_daily(
--     p_asOfDate        => DATE '2026-09-28',
--     p_eventWindowDays => 1,
--     p_validateOnly    => FALSE
-- );