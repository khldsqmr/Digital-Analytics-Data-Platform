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
--   entry_page_url_path and entry_page_url_full are retained here. Silver
--   detailsPerHit parses Web utm_source/utm_medium/utm_campaign from the SSF
--   entry URL and propagates the resulting session-entry attribution downstream.
--
-- PREFLIGHT / VALIDATION CONTRACT:
--   p_validateOnly=TRUE writes nothing. It verifies that Bronze SEF covers every
--   requested event date and that every non-null SEF session_id can be found in
--   source SSF. This prevents a partial SEF->SSF backfill from silently flowing
--   into Silver.
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
        CLUSTER BY (session_start_date,source_table)
        COMMENT 'Bronze: narrow MIP projection of EDL session_summary_fact. Accumulating snapshot keyed by session_id.'
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
            current_timestamp() AS _ingestedAt
        FROM prd_dbi_analytics.silver_digital_interactions.session_summary_fact
        WHERE 1=0;

        MERGE INTO prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily t
        USING (
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
                current_timestamp() AS _ingestedAt
            FROM prd_dbi_analytics.silver_digital_interactions.session_summary_fact s
            INNER JOIN (
                SELECT session_id
                FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily
                WHERE event_date BETWEEN v_windowStart AND v_windowEnd
                  AND session_id IS NOT NULL
                GROUP BY session_id
            ) e ON e.session_id=s.session_id
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
            t._ingestedAt=s._ingestedAt
        WHEN NOT MATCHED THEN INSERT (
            session_id,source_table,canonical_user_id,identity_status,session_status,
            session_start_date,session_start_time,session_end_time,
            entry_page_url_path,entry_page_url_full,_ingestedAt
        ) VALUES (
            s.session_id,s.source_table,s.canonical_user_id,s.identity_status,s.session_status,
            s.session_start_date,s.session_start_time,s.session_end_time,
            s.entry_page_url_path,s.entry_page_url_full,s._ingestedAt
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
