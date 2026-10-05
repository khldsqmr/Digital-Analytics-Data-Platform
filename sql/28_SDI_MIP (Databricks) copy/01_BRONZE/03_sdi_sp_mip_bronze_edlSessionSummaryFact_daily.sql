-- ============================================================================

-- FILE  : 03_sdi_sp_mip_bronze_edlSessionSummaryFact_daily.sql

-- LAYER : BRONZE

-- SOURCE: prd_dbi_analytics.silver_digital_interactions.session_summary_fact

-- PURPOSE:

--   Persist only SSF sessions referenced by the requested Bronze SEF event window.

--

-- DEPENDENCY / SESSION CONTRACT:

--   Run Bronze SEF first. This procedure takes the exact non-null session_id set

--   from that SEF window and pulls those sessions from SESSION_SUMMARY_FACT.

--   It intentionally does NOT use a fixed +/- session_start_date window because

--   sessions can span event dates.

--

-- UTM CONTRACT:

--   entry_page_url_path and entry_page_url_full remain the raw SSF source fields.

--   utm_source / utm_medium / utm_campaign are deterministic parsed helpers

--   derived ONCE at session grain from entry_page_url_full. They remain nullable

--   in Bronze when the parameter is absent; Silver maps NULL/blank to '(not set)'.

--   No UTM normalization, channel classification or Top-N bucketing belongs here.

--

-- PREFLIGHT / VALIDATION CONTRACT:

--   p_validateOnly=TRUE writes nothing. It verifies that Bronze SEF covers every

--   requested event date and that every non-null SEF session_id can be found in

--   source SSF. This prevents a partial SEF->SSF backfill from silently flowing

--   into Silver.

--

-- PHYSICAL DESIGN / COST CONTRACT:

--   This is an accumulating session snapshot. Liquid clustering uses three keys:

--     session_id         -> MERGE key and Silver hit-enrichment join

--     session_start_date -> operational/date diagnostics

--     source_table       -> Web/App source pruning

--   Databricks supports at most four liquid-clustering columns; a fourth key is

--   intentionally not added because no additional SSF field is repeatedly used

--   strongly enough to justify it.

--

-- PEER / IMPACT CONTRACT:

--   No peer-set or impact-on-topline logic belongs in Bronze.

-- ============================================================================

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessionSummaryFact_daily(

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

        date_add(to_date(from_utc_timestamp(current_timestamp(),'America/Los_Angeles')),-1)

    );

    DECLARE v_windowEnd DATE DEFAULT v_asOfDate;

    DECLARE v_windowStart DATE DEFAULT date_add(v_asOfDate,-(p_eventWindowDays-1));

    DECLARE v_sefDateCount BIGINT DEFAULT 0;

    DECLARE v_expectedSessionCount BIGINT DEFAULT 0;

    DECLARE v_sourceMatchedSessionCount BIGINT DEFAULT 0;

    -- 1. Parameter validation.

    IF p_eventWindowDays IS NULL OR p_eventWindowDays<1 THEN

        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='p_eventWindowDays must be >= 1.';

    END IF;

    -- 2. Bronze SEF dependency preflight.

    SET v_sefDateCount=(

        SELECT COUNT(DISTINCT event_date)

        FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily

        WHERE event_date BETWEEN v_windowStart AND v_windowEnd

    );

    IF v_sefDateCount<>p_eventWindowDays THEN

        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Bronze SESSION_EVENT_FACT does not contain every requested event date. Load/rebuild SEF Bronze before SSF Bronze.';

    END IF;

    SET v_expectedSessionCount=(

        SELECT COUNT(DISTINCT session_id)

        FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily

        WHERE event_date BETWEEN v_windowStart AND v_windowEnd

          AND session_id IS NOT NULL

    );

    IF v_expectedSessionCount=0 THEN

        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Bronze SESSION_EVENT_FACT has no non-null session_id values for the requested event window.';

    END IF;

    -- 3. Source SSF coverage preflight. Every requested SEF session must resolve.

    SET v_sourceMatchedSessionCount=(

        SELECT COUNT(*)

        FROM (

            SELECT e.session_id

            FROM (

                SELECT DISTINCT session_id

                FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily

                WHERE event_date BETWEEN v_windowStart AND v_windowEnd

                  AND session_id IS NOT NULL

            ) e

            INNER JOIN prd_dbi_analytics.silver_digital_interactions.session_summary_fact s

              ON s.session_id=e.session_id

            GROUP BY e.session_id

        ) matched

    );

    IF v_sourceMatchedSessionCount<>v_expectedSessionCount THEN

        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='SESSION_SUMMARY_FACT does not contain every session_id referenced by Bronze SEF for the requested event window. SSF Bronze was not refreshed.';

    END IF;

    -- 4. Validation-only mode.

    IF p_validateOnly THEN

        SELECT

            'VALIDATION_ONLY' AS status,

            v_windowStart AS requestedEventWindowStart,

            v_windowEnd AS requestedEventWindowEnd,

            v_sefDateCount AS bronzeSefDateCount,

            v_expectedSessionCount AS expectedSefSessions,

            v_sourceMatchedSessionCount AS matchedSourceSsfSessions,

            'prd_dbi_analytics.silver_digital_interactions.session_summary_fact' AS sourceObject,

            'Exact SSF session_ids are driven by Bronze SESSION_EVENT_FACT. No Bronze table was created or modified.' AS message;

    ELSE

        -- 5. Create accumulating snapshot and upsert the exact requested sessions.

        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily

        USING DELTA

        CLUSTER BY (session_id,session_start_date,source_table)

        COMMENT 'Bronze: narrow MIP projection of EDL session_summary_fact. Accumulating snapshot keyed by session_id with session-grain parsed UTM helpers.'

        AS

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

            cast(NULL AS STRING) AS utm_source,

            cast(NULL AS STRING) AS utm_medium,

            cast(NULL AS STRING) AS utm_campaign,

            current_timestamp() AS _ingestedAt

        FROM prd_dbi_analytics.silver_digital_interactions.session_summary_fact

        WHERE 1=0;

        -- Existing deployments may have been created before the parsed UTM
        -- helpers were added. Evolve only missing columns explicitly.
        IF NOT EXISTS (
            SELECT 1
            FROM prdrzranalytics.information_schema.columns
            WHERE table_schema='lab42'
              AND table_name='sdi_tbl_mip_bronze_edlSessionSummaryFact_daily'
              AND column_name='utm_source'
        ) THEN
            ALTER TABLE prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily
            ADD COLUMNS (
                utm_source STRING COMMENT 'Parsed raw utm_source from SSF entry_page_url_full; NULL when absent'
            );
        END IF;

        IF NOT EXISTS (
            SELECT 1
            FROM prdrzranalytics.information_schema.columns
            WHERE table_schema='lab42'
              AND table_name='sdi_tbl_mip_bronze_edlSessionSummaryFact_daily'
              AND column_name='utm_medium'
        ) THEN
            ALTER TABLE prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily
            ADD COLUMNS (
                utm_medium STRING COMMENT 'Parsed raw utm_medium from SSF entry_page_url_full; NULL when absent'
            );
        END IF;

        IF NOT EXISTS (
            SELECT 1
            FROM prdrzranalytics.information_schema.columns
            WHERE table_schema='lab42'
              AND table_name='sdi_tbl_mip_bronze_edlSessionSummaryFact_daily'
              AND column_name='utm_campaign'
        ) THEN
            ALTER TABLE prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily
            ADD COLUMNS (
                utm_campaign STRING COMMENT 'Parsed raw utm_campaign from SSF entry_page_url_full; NULL when absent'
            );
        END IF;

        -- Enforce current liquid-clustering metadata for both new and old tables.
        -- Existing files are not force-rewritten here; no OPTIMIZE is triggered.
        ALTER TABLE prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily
        CLUSTER BY (session_id,session_start_date,source_table);

        MERGE INTO prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily t

        USING (

            WITH requestedSessions AS (

                SELECT session_id

                FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily

                WHERE event_date BETWEEN v_windowStart AND v_windowEnd

                  AND session_id IS NOT NULL

                GROUP BY session_id

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

                        WHEN lower(coalesce(s.entry_page_url_full,'')) LIKE 'http%'

                            THEN s.entry_page_url_full

                        WHEN nullif(trim(s.entry_page_url_full),'') IS NOT NULL

                            THEN concat('https://',s.entry_page_url_full)

                        ELSE NULL

                    END AS entryUrlForParse

                FROM prd_dbi_analytics.silver_digital_interactions.session_summary_fact s

                INNER JOIN requestedSessions e

                  ON e.session_id=s.session_id

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

                nullif(try_parse_url(entryUrlForParse,'QUERY','utm_source'),'') AS utm_source,

                nullif(try_parse_url(entryUrlForParse,'QUERY','utm_medium'),'') AS utm_medium,

                nullif(try_parse_url(entryUrlForParse,'QUERY','utm_campaign'),'') AS utm_campaign,

                current_timestamp() AS _ingestedAt

            FROM sourceSessions

        ) s

        ON t.session_id=s.session_id

        WHEN MATCHED THEN UPDATE SET

            t.source_table=s.source_table,

            t.canonical_user_id=s.canonical_user_id,

            t.identity_status=s.identity_status,

            t.session_status=s.session_status,

            t.session_start_date=s.session_start_date,

            t.session_start_time=s.session_start_time,

            t.session_end_time=s.session_end_time,

            t.entry_page_url_path=s.entry_page_url_path,

            t.entry_page_url_full=s.entry_page_url_full,

            t.utm_source=s.utm_source,

            t.utm_medium=s.utm_medium,

            t.utm_campaign=s.utm_campaign,

            t._ingestedAt=s._ingestedAt

        WHEN NOT MATCHED THEN INSERT (

            session_id,source_table,canonical_user_id,identity_status,session_status,

            session_start_date,session_start_time,session_end_time,

            entry_page_url_path,entry_page_url_full,

            utm_source,utm_medium,utm_campaign,_ingestedAt

        ) VALUES (

            s.session_id,s.source_table,s.canonical_user_id,s.identity_status,s.session_status,

            s.session_start_date,s.session_start_time,s.session_end_time,

            s.entry_page_url_path,s.entry_page_url_full,

            s.utm_source,s.utm_medium,s.utm_campaign,s._ingestedAt

        );

        SELECT

            'SUCCESS' AS status,

            v_windowStart AS requestedEventWindowStart,

            v_windowEnd AS requestedEventWindowEnd,

            v_expectedSessionCount AS requestedSefSessions,

            'prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily' AS targetObject;

    END IF;

END;

-- ============================================================================

-- DEVELOPMENT / TEST EXAMPLES

-- ============================================================================

-- A. PREFLIGHT ONLY - requires Bronze SEF to be loaded first.

-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessionSummaryFact_daily(

--   p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>TRUE);

--

-- B. EXECUTE AFTER SEF + PREFLIGHT SUCCEED.

-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessionSummaryFact_daily(

--   p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>FALSE);

--

-- C. QUICK VALIDATION - detailed checks live in 10_mip_bronze_sanity_checks.sql.

-- SELECT session_status,COUNT(*) AS sessions

-- FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily

-- GROUP BY session_status ORDER BY sessions DESC;

-- --------------------------------------------------------------------------
-- D. UTM PARSE VALIDATION
-- Bronze keeps absent values NULL; Silver maps them to '(not set)'.
-- --------------------------------------------------------------------------
-- SELECT
--     source_table,
--     COUNT(*) AS sessions,
--     COUNT_IF(entry_page_url_full IS NOT NULL) AS sessionsWithEntryUrl,
--     COUNT_IF(utm_source IS NOT NULL) AS sessionsWithUtmSource,
--     COUNT_IF(utm_medium IS NOT NULL) AS sessionsWithUtmMedium,
--     COUNT_IF(utm_campaign IS NOT NULL) AS sessionsWithUtmCampaign
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily
-- WHERE session_start_date BETWEEN DATE '2026-09-24' AND DATE '2026-10-03'
-- GROUP BY source_table
-- ORDER BY sessions DESC;
