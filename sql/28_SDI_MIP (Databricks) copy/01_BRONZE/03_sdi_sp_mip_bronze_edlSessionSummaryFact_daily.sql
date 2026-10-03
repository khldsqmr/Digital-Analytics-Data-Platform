-- ============================================================================
-- FILE  : 03_sdi_sp_mip_bronze_edlSessionSummaryFact_daily.sql
-- LAYER : BRONZE
-- SOURCE: prd_dbi_analytics.silver_digital_interactions.session_summary_fact
-- PURPOSE:
--   Persist only SSF sessions referenced by the requested Bronze SEF event window.
--
-- NOTE:
--   This intentionally avoids a fixed +/- day session_start_date window. Sessions
--   can have activity well after their start date, so exact session_id coverage is
--   both safer and cheaper than arbitrary widening.
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
    IF p_eventWindowDays IS NULL OR p_eventWindowDays<1 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='p_eventWindowDays must be >= 1.';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily
        WHERE event_date BETWEEN v_windowStart AND v_windowEnd
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Bronze SESSION_EVENT_FACT has no rows for the requested event window. Load SEF Bronze before SSF Bronze.';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM prd_dbi_analytics.silver_digital_interactions.session_summary_fact s
        INNER JOIN (
            SELECT session_id
            FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily
            WHERE event_date BETWEEN v_windowStart AND v_windowEnd
            GROUP BY session_id
        ) e ON e.session_id=s.session_id
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='SESSION_SUMMARY_FACT returned no matching sessions for the requested SEF event window. Bronze was not created or refreshed.';
    END IF;
    IF p_validateOnly THEN
        SELECT
            'VALIDATION_ONLY' AS status,
            v_windowStart AS requestedEventWindowStart,
            v_windowEnd AS requestedEventWindowEnd,
            'prd_dbi_analytics.silver_digital_interactions.session_summary_fact' AS sourceObject,
            'Exact SSF session_ids are driven by Bronze SESSION_EVENT_FACT. No Bronze table was created or modified.' AS message;
    ELSE
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
            'prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily' AS targetObject;
    END IF;
END;
-- ============================================================================
-- ============================================================================
-- DEVELOPMENT / TEST EXAMPLES
-- ============================================================================
-- A. PREFLIGHT
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessionSummaryFact_daily(
--   p_asOfDate=>DATE '2026-09-28', p_eventWindowDays=>1, p_validateOnly=>TRUE);
-- B. EXECUTE
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessionSummaryFact_daily(
--   p_asOfDate=>DATE '2026-09-28', p_eventWindowDays=>1, p_validateOnly=>FALSE);
-- C. VALIDATION
-- SELECT session_status,COUNT(*) AS sessions
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily
-- GROUP BY session_status ORDER BY sessions DESC;
