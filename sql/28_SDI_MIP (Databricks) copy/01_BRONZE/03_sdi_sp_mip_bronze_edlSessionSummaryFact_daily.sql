-- ============================================================================
-- FILE  : 03_sdi_sp_mip_bronze_edlSessionSummaryFact_daily.sql
-- LAYER : BRONZE
-- SOURCE: prd_dbi_analytics.silver_digital_interactions.session_summary_fact
--
-- PURPOSE:
--   Persist only SSF sessions referenced by the requested Bronze SEF event
--   window.
--
-- DEPENDENCY AND SESSION CONTRACT:
--   Run Bronze SEF first.
--
--   This procedure takes the exact non-null session_id set from the requested
--   Bronze SEF event window and retrieves those sessions from the source
--   SESSION_SUMMARY_FACT table.
--
--   It intentionally does not use a fixed session_start_date range because a
--   session can span event-date boundaries.
--
-- UTM CONTRACT:
--   entry_page_url_path and entry_page_url_full remain the raw SSF fields.
--
--   utm_source, utm_medium and utm_campaign are deterministic parsed helpers
--   derived once at session grain from entry_page_url_full.
--
--   These values remain NULL in Bronze when the parameter is absent.
--   Silver maps NULL or blank values to '(not set)'.
--
--   No UTM normalization, channel classification or Top-N bucketing belongs
--   in this Bronze procedure.
--
-- PREFLIGHT AND VALIDATION CONTRACT:
--   p_validateOnly=TRUE writes nothing.
--
--   Validation verifies:
--     1. Bronze SEF contains every requested event date.
--     2. Bronze SEF contains at least one non-null session_id.
--     3. Every requested Bronze SEF session_id exists in source SSF.
--
-- PHYSICAL DESIGN AND COST CONTRACT:
--   This is an accumulating session snapshot.
--
--   Liquid clustering uses:
--     session_id
--     session_start_date
--     source_table
--
--   The clustering definition is established when the table is created.
--   The procedure does not repeatedly alter clustering metadata during
--   subsequent executions or backfills.
--
-- SCHEMA EVOLUTION CONTRACT:
--   The current table definition includes all required columns.
--
--   Schema migrations for older deployments must be handled as explicit,
--   one-time deployment operations outside this daily loading procedure.
--
--   This prevents repeated ADD COLUMN attempts during routine backfills.
--
-- PEER AND IMPACT CONTRACT:
--   No peer-set or impact-on-topline logic belongs in Bronze.
-- ============================================================================

CREATE OR REPLACE PROCEDURE
prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessionSummaryFact_daily
(
    IN p_asOfDate DATE DEFAULT NULL,
    IN p_eventWindowDays INT DEFAULT 1,
    IN p_validateOnly BOOLEAN DEFAULT FALSE
)

LANGUAGE SQL

SQL SECURITY INVOKER

MODIFIES SQL DATA

COMMENT 'Bronze MIP projection of EDL session_summary_fact, upserted for session_ids referenced by the requested Bronze SEF event window.'

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

    DECLARE v_windowEnd DATE DEFAULT v_asOfDate;

    DECLARE v_windowStart DATE DEFAULT date_add(
        v_asOfDate,
        -(p_eventWindowDays - 1)
    );

    DECLARE v_sefDateCount BIGINT DEFAULT 0;

    DECLARE v_expectedSessionCount BIGINT DEFAULT 0;

    DECLARE v_sourceMatchedSessionCount BIGINT DEFAULT 0;


    -- ------------------------------------------------------------------------
    -- 1. Parameter validation
    -- ------------------------------------------------------------------------

    IF p_eventWindowDays IS NULL OR p_eventWindowDays < 1 THEN

        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT =
            'p_eventWindowDays must be greater than or equal to 1.';

    END IF;


    -- ------------------------------------------------------------------------
    -- 2. Bronze SEF date-coverage validation
    -- ------------------------------------------------------------------------

    SET v_sefDateCount = (

        SELECT
            COUNT(DISTINCT event_date)

        FROM
            prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily

        WHERE
            event_date BETWEEN v_windowStart AND v_windowEnd

    );


    IF v_sefDateCount <> p_eventWindowDays THEN

        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT =
            'Bronze SESSION_EVENT_FACT does not contain every requested event date. Load or rebuild Bronze SEF before Bronze SSF.';

    END IF;


    -- ------------------------------------------------------------------------
    -- 3. Requested-session validation
    -- ------------------------------------------------------------------------

    SET v_expectedSessionCount = (

        SELECT
            COUNT(DISTINCT session_id)

        FROM
            prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily

        WHERE
            event_date BETWEEN v_windowStart AND v_windowEnd
            AND session_id IS NOT NULL

    );


    IF v_expectedSessionCount = 0 THEN

        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT =
            'Bronze SESSION_EVENT_FACT has no non-null session_id values for the requested event window.';

    END IF;


    -- ------------------------------------------------------------------------
    -- 4. Source SSF coverage validation
    --
    -- Every distinct non-null session_id found in the requested Bronze SEF
    -- window must exist in the authoritative source SSF table.
    -- ------------------------------------------------------------------------

    SET v_sourceMatchedSessionCount = (

        SELECT
            COUNT(*)

        FROM (

            SELECT
                e.session_id

            FROM (

                SELECT DISTINCT
                    session_id

                FROM
                    prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily

                WHERE
                    event_date BETWEEN v_windowStart AND v_windowEnd
                    AND session_id IS NOT NULL

            ) e

            INNER JOIN
                prd_dbi_analytics.silver_digital_interactions.session_summary_fact s

                ON s.session_id = e.session_id

            GROUP BY
                e.session_id

        ) matched

    );


    IF v_sourceMatchedSessionCount <> v_expectedSessionCount THEN

        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT =
            'SESSION_SUMMARY_FACT does not contain every session_id referenced by Bronze SEF for the requested event window. Bronze SSF was not refreshed.';

    END IF;


    -- ------------------------------------------------------------------------
    -- 5. Validation-only mode
    -- ------------------------------------------------------------------------

    IF p_validateOnly THEN

        SELECT
            'VALIDATION_ONLY' AS status,
            v_windowStart AS requestedEventWindowStart,
            v_windowEnd AS requestedEventWindowEnd,
            v_sefDateCount AS bronzeSefDateCount,
            v_expectedSessionCount AS expectedSefSessions,
            v_sourceMatchedSessionCount AS matchedSourceSsfSessions,
            'prd_dbi_analytics.silver_digital_interactions.session_summary_fact'
                AS sourceObject,
            'Exact SSF session_ids are driven by Bronze SESSION_EVENT_FACT. No Bronze table was created or modified.'
                AS message;


    ELSE


        -- --------------------------------------------------------------------
        -- 6. Create the Bronze SSF accumulating snapshot when it does not exist
        --
        -- The full schema is declared explicitly.
        --
        -- Because the UTM columns are already part of this definition, routine
        -- calls and future backfills do not attempt to add them again.
        -- --------------------------------------------------------------------

        CREATE TABLE IF NOT EXISTS
            prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily
        (
            session_id STRING
                COMMENT 'Stable authoritative server session identifier.',

            source_table STRING
                COMMENT 'Session source: t_web_interactions or t_app_interactions.',

            canonical_user_id STRING
                COMMENT 'Current canonical user associated with the session.',

            identity_status STRING
                COMMENT 'Current session identity result: RESOLVED, UNRESOLVED or CONFLICT.',

            session_status STRING
                COMMENT 'Session lifecycle status such as OPEN, CLOSED, MERGED or SUPERSEDED.',

            session_start_date DATE
                COMMENT 'UTC date of authoritative session start.',

            session_start_time TIMESTAMP
                COMMENT 'UTC timestamp of the first event currently assigned to the session.',

            session_end_time TIMESTAMP
                COMMENT 'UTC timestamp of the last event currently assigned to the session.',

            entry_page_url_path STRING
                COMMENT 'URL path from the first event in the session.',

            entry_page_url_full STRING
                COMMENT 'Complete URL from the first event in the session.',

            utm_source STRING
                COMMENT 'Parsed raw utm_source from SSF entry_page_url_full; NULL when absent.',

            utm_medium STRING
                COMMENT 'Parsed raw utm_medium from SSF entry_page_url_full; NULL when absent.',

            utm_campaign STRING
                COMMENT 'Parsed raw utm_campaign from SSF entry_page_url_full; NULL when absent.',

            _ingestedAt TIMESTAMP
                COMMENT 'Timestamp when the Bronze SSF row was inserted or refreshed.'
        )

        USING DELTA

        CLUSTER BY (
            session_id,
            session_start_date,
            source_table
        )

        COMMENT
            'Bronze: narrow MIP projection of EDL session_summary_fact. Accumulating snapshot keyed by session_id with session-grain parsed UTM helpers.';


        -- --------------------------------------------------------------------
        -- 7. Upsert the exact requested sessions
        --
        -- requestedSessions contains one record per non-null session_id from
        -- the requested Bronze SEF window.
        --
        -- sourceSessions retrieves those sessions from source SSF and creates
        -- one valid URL value for UTM parsing.
        --
        -- The source URL receives an https prefix only when the source value
        -- is present and does not already begin with http or https.
        -- --------------------------------------------------------------------

        MERGE INTO
            prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily t

        USING (

            WITH requestedSessions AS (

                SELECT
                    session_id

                FROM
                    prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily

                WHERE
                    event_date BETWEEN v_windowStart AND v_windowEnd
                    AND session_id IS NOT NULL

                GROUP BY
                    session_id

            ),

            sourceSessions AS (

                SELECT
                    s.session_id,
                    s.source_table,
                    s.canonical_user_id,
                    s.identity_status,
                    s.session_status,
                    s.session_start_date,
                    s.session_start_time,
                    s.session_end_time,
                    s.entry_page_url_path,
                    s.entry_page_url_full,

                    CASE

                        WHEN lower(
                            coalesce(
                                trim(
                                    cast(
                                        s.entry_page_url_full AS STRING
                                    )
                                ),
                                ''
                            )
                        ) LIKE 'http%'

                            THEN trim(
                                cast(
                                    s.entry_page_url_full AS STRING
                                )
                            )

                        WHEN nullif(
                            trim(
                                cast(
                                    s.entry_page_url_full AS STRING
                                )
                            ),
                            ''
                        ) IS NOT NULL

                            THEN concat(
                                'https://',
                                trim(
                                    cast(
                                        s.entry_page_url_full AS STRING
                                    )
                                )
                            )

                        ELSE NULL

                    END AS entryUrlForParse

                FROM
                    prd_dbi_analytics.silver_digital_interactions.session_summary_fact s

                INNER JOIN requestedSessions e

                    ON e.session_id = s.session_id

            ),

            parsedSessions AS (

                SELECT
                    session_id,
                    source_table,
                    canonical_user_id,
                    identity_status,
                    session_status,
                    session_start_date,
                    session_start_time,
                    session_end_time,
                    entry_page_url_path,
                    entry_page_url_full,

                    nullif(
                        try_parse_url(
                            entryUrlForParse,
                            'QUERY',
                            'utm_source'
                        ),
                        ''
                    ) AS utm_source,

                    nullif(
                        try_parse_url(
                            entryUrlForParse,
                            'QUERY',
                            'utm_medium'
                        ),
                        ''
                    ) AS utm_medium,

                    nullif(
                        try_parse_url(
                            entryUrlForParse,
                            'QUERY',
                            'utm_campaign'
                        ),
                        ''
                    ) AS utm_campaign,

                    current_timestamp() AS _ingestedAt

                FROM
                    sourceSessions

            )

            SELECT
                session_id,
                source_table,
                canonical_user_id,
                identity_status,
                session_status,
                session_start_date,
                session_start_time,
                session_end_time,
                entry_page_url_path,
                entry_page_url_full,
                utm_source,
                utm_medium,
                utm_campaign,
                _ingestedAt

            FROM
                parsedSessions

        ) s

        ON t.session_id = s.session_id


        WHEN MATCHED THEN

            UPDATE SET
                t.source_table = s.source_table,
                t.canonical_user_id = s.canonical_user_id,
                t.identity_status = s.identity_status,
                t.session_status = s.session_status,
                t.session_start_date = s.session_start_date,
                t.session_start_time = s.session_start_time,
                t.session_end_time = s.session_end_time,
                t.entry_page_url_path = s.entry_page_url_path,
                t.entry_page_url_full = s.entry_page_url_full,
                t.utm_source = s.utm_source,
                t.utm_medium = s.utm_medium,
                t.utm_campaign = s.utm_campaign,
                t._ingestedAt = s._ingestedAt


        WHEN NOT MATCHED THEN

            INSERT (
                session_id,
                source_table,
                canonical_user_id,
                identity_status,
                session_status,
                session_start_date,
                session_start_time,
                session_end_time,
                entry_page_url_path,
                entry_page_url_full,
                utm_source,
                utm_medium,
                utm_campaign,
                _ingestedAt
            )

            VALUES (
                s.session_id,
                s.source_table,
                s.canonical_user_id,
                s.identity_status,
                s.session_status,
                s.session_start_date,
                s.session_start_time,
                s.session_end_time,
                s.entry_page_url_path,
                s.entry_page_url_full,
                s.utm_source,
                s.utm_medium,
                s.utm_campaign,
                s._ingestedAt
            );


        -- --------------------------------------------------------------------
        -- 8. Success result
        -- --------------------------------------------------------------------

        SELECT
            'SUCCESS' AS status,
            v_windowStart AS requestedEventWindowStart,
            v_windowEnd AS requestedEventWindowEnd,
            v_sefDateCount AS bronzeSefDateCount,
            v_expectedSessionCount AS requestedSefSessions,
            v_sourceMatchedSessionCount AS matchedSourceSsfSessions,
            'prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily'
                AS targetObject,
            'Requested SSF sessions were successfully merged into the Bronze accumulating snapshot.'
                AS message;

    END IF;

END;


-- ============================================================================
-- DEVELOPMENT AND TEST EXAMPLES
-- Run each statement separately after deploying the procedure.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- A. PREFLIGHT ONLY
--
-- Requires Bronze SEF to contain all requested event dates.
-- Creates or modifies nothing.
-- ----------------------------------------------------------------------------

-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessionSummaryFact_daily(
--     p_asOfDate        => DATE '2026-10-03',
--     p_eventWindowDays => 10,
--     p_validateOnly    => TRUE
-- );


-- ----------------------------------------------------------------------------
-- B. EXECUTE TEN-DAY BACKFILL
--
-- This recreates the target table automatically when it does not exist.
-- Later executions reuse the existing table and only perform the MERGE.
-- ----------------------------------------------------------------------------

-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessionSummaryFact_daily(
--     p_asOfDate        => DATE '2026-10-03',
--     p_eventWindowDays => 10,
--     p_validateOnly    => FALSE
-- );


-- ----------------------------------------------------------------------------
-- C. TABLE SCHEMA VALIDATION
-- ----------------------------------------------------------------------------

-- DESCRIBE TABLE
-- prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily;


-- ----------------------------------------------------------------------------
-- D. SESSION-STATUS VALIDATION
-- ----------------------------------------------------------------------------

-- SELECT
--     session_status,
--     COUNT(*) AS sessions
-- FROM
--     prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily
-- GROUP BY
--     session_status
-- ORDER BY
--     sessions DESC;


-- ----------------------------------------------------------------------------
-- E. DUPLICATE SESSION VALIDATION
--
-- Expected result: no rows.
-- ----------------------------------------------------------------------------

-- SELECT
--     session_id,
--     COUNT(*) AS rowCount
-- FROM
--     prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily
-- GROUP BY
--     session_id
-- HAVING
--     COUNT(*) > 1
-- ORDER BY
--     rowCount DESC
-- LIMIT 100;


-- ----------------------------------------------------------------------------
-- F. UTM PARSE VALIDATION
--
-- Bronze keeps absent UTM values as NULL.
-- Silver maps NULL or blank values to '(not set)'.
-- ----------------------------------------------------------------------------

-- SELECT
--     source_table,
--     COUNT(*) AS sessions,
--     COUNT_IF(entry_page_url_full IS NOT NULL) AS sessionsWithEntryUrl,
--     COUNT_IF(utm_source IS NOT NULL) AS sessionsWithUtmSource,
--     COUNT_IF(utm_medium IS NOT NULL) AS sessionsWithUtmMedium,
--     COUNT_IF(utm_campaign IS NOT NULL) AS sessionsWithUtmCampaign
-- FROM
--     prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily
-- WHERE
--     session_start_date BETWEEN DATE '2026-09-24' AND DATE '2026-10-03'
-- GROUP BY
--     source_table
-- ORDER BY
--     sessions DESC;


-- ----------------------------------------------------------------------------
-- G. REQUESTED SESSION COVERAGE VALIDATION
--
-- Expected result:
--     missingBronzeSsfSessions = 0
-- ----------------------------------------------------------------------------

-- SELECT
--     COUNT(*) AS missingBronzeSsfSessions
-- FROM (
--
--     SELECT DISTINCT
--         e.session_id
--
--     FROM
--         prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily e
--
--     LEFT ANTI JOIN
--         prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily s
--
--         ON s.session_id = e.session_id
--
--     WHERE
--         e.event_date BETWEEN DATE '2026-09-24' AND DATE '2026-10-03'
--         AND e.session_id IS NOT NULL
--
-- ) missing;