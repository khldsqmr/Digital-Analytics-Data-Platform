-- ============================================================================
-- FILE  : 02_sdi_sp_mip_bronze_edlSessionEventFact_daily.sql
-- LAYER : BRONZE
-- SOURCE: prd_dbi_analytics.silver_digital_interactions.session_event_fact
-- PURPOSE:
--   Persist the narrow MIP-required event-to-session bridge.
-- ============================================================================
CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessionEventFact_daily(
    IN p_asOfDate DATE DEFAULT NULL,
    IN p_eventWindowDays INT DEFAULT 1,
    IN p_validateOnly BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Bronze MIP projection of EDL session_event_fact: composite UDI bridge, session_id, hit sequence and flow_name.'
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
        FROM prd_dbi_analytics.silver_digital_interactions.session_event_fact
        WHERE event_date BETWEEN v_windowStart AND v_windowEnd
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='SESSION_EVENT_FACT returned no rows for the requested Bronze window. Bronze was not created or refreshed.';
    END IF;
    IF p_validateOnly THEN
        SELECT
            'VALIDATION_ONLY' AS status,
            v_windowStart AS requestedWindowStart,
            v_windowEnd AS requestedWindowEnd,
            'prd_dbi_analytics.silver_digital_interactions.session_event_fact' AS sourceObject,
            'No Bronze table was created or modified.' AS message;
    ELSE
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily
        USING DELTA
        CLUSTER BY (event_date,source_table)
        COMMENT 'Bronze: narrow MIP projection of EDL session_event_fact. Event-to-session bridge at source grain.'
        AS
        SELECT
            row_identity_hash,
            event_date,
            source_table,
            event_timestamp_utc,
            session_id,
            hit_number_in_session,
            flow_name,
            current_timestamp() AS _ingestedAt
        FROM prd_dbi_analytics.silver_digital_interactions.session_event_fact
        WHERE 1=0;
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily
        REPLACE WHERE event_date BETWEEN v_windowStart AND v_windowEnd
        SELECT
            row_identity_hash,
            event_date,
            source_table,
            event_timestamp_utc,
            session_id,
            hit_number_in_session,
            flow_name,
            current_timestamp() AS _ingestedAt
        FROM prd_dbi_analytics.silver_digital_interactions.session_event_fact
        WHERE event_date BETWEEN v_windowStart AND v_windowEnd;
        SELECT
            'SUCCESS' AS status,
            v_windowStart AS loadedWindowStart,
            v_windowEnd AS loadedWindowEnd,
            'prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily' AS targetObject;
    END IF;
END;
-- ============================================================================
-- ============================================================================
-- DEVELOPMENT / TEST EXAMPLES
-- ============================================================================
-- A. PREFLIGHT
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessionEventFact_daily(
--   p_asOfDate=>DATE '2026-09-28', p_eventWindowDays=>1, p_validateOnly=>TRUE);
-- B. EXECUTE
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessionEventFact_daily(
--   p_asOfDate=>DATE '2026-09-28', p_eventWindowDays=>1, p_validateOnly=>FALSE);
-- C. VALIDATION
-- SELECT event_date,COUNT(*) AS rows,COUNT(DISTINCT session_id) AS sessions,
--        COUNT_IF(session_id IS NULL) AS missingSessionId
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily
-- WHERE event_date=DATE '2026-09-28' GROUP BY event_date;
