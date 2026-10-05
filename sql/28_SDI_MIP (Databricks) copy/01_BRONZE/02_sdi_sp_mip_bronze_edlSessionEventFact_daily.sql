-- ============================================================================
-- FILE  : 02_sdi_sp_mip_bronze_edlSessionEventFact_daily.sql
-- LAYER : BRONZE
-- SOURCE: prd_dbi_analytics.silver_digital_interactions.session_event_fact
-- PURPOSE:
--   Persist the narrow MIP-required event-to-session bridge.
--
-- GRAIN / JOIN CONTRACT:
--   Preserve source grain and the validated UDI bridge key:
--     row_identity_hash + event_date + source_table
--   session_id, hit_number_in_session and flow_name are retained for downstream
--   sessionization, sequencing and buy-flow logic.
--
-- DEPENDENCY:
--   UDI Bronze and SEF Bronze are independent loads. SEF MUST complete before
--   SSF Bronze because SSF Bronze uses the SEF session_id population from the
--   requested event window to pull the exact session snapshots.
--
-- PREFLIGHT / VALIDATION CONTRACT:
--   p_validateOnly=TRUE validates parameters and source event-date completeness
--   without creating or modifying the target. Every requested event_date must be
--   present before a multi-day REPLACE WHERE backfill is allowed to run.
--
-- PEER / IMPACT CONTRACT:
--   No peer-set or impact-on-topline logic belongs in Bronze.
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
    DECLARE v_sourceDateCount BIGINT DEFAULT 0;

    -- 1. Parameter validation.
    IF p_eventWindowDays IS NULL OR p_eventWindowDays<1 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='p_eventWindowDays must be >= 1.';
    END IF;

    -- 2. Source preflight. Nothing has been written to Bronze yet.
    SET v_sourceDateCount=(
        SELECT COUNT(DISTINCT event_date)
        FROM prd_dbi_analytics.silver_digital_interactions.session_event_fact
        WHERE event_date BETWEEN v_windowStart AND v_windowEnd
    );
    IF v_sourceDateCount<>p_eventWindowDays THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='SESSION_EVENT_FACT does not contain every requested event date. Bronze was not created or refreshed.';
    END IF;

    -- 3. Validation-only mode.
    IF p_validateOnly THEN
        SELECT
            'VALIDATION_ONLY' AS status,
            v_windowStart AS requestedWindowStart,
            v_windowEnd AS requestedWindowEnd,
            v_sourceDateCount AS sourceDateCount,
            'prd_dbi_analytics.silver_digital_interactions.session_event_fact' AS sourceObject,
            'No Bronze table was created or modified.' AS message;
    ELSE
        -- 4. Create target after preflight and replace only requested partitions.
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
-- DEVELOPMENT / TEST EXAMPLES
-- ============================================================================
-- A. PREFLIGHT ONLY - validates the requested window and writes nothing.
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessionEventFact_daily(
--   p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>TRUE);
--
-- B. EXECUTE AFTER PREFLIGHT SUCCEEDS.
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessionEventFact_daily(
--   p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>FALSE);
--
-- C. QUICK VALIDATION - detailed checks live in 10_mip_bronze_sanity_checks.sql.
-- SELECT event_date,source_table,COUNT(*) AS rows,COUNT(DISTINCT session_id) AS sessions,
--        COUNT_IF(session_id IS NULL) AS missingSessionId
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily
-- WHERE event_date BETWEEN DATE '2026-09-20' AND DATE '2026-09-29'
-- GROUP BY event_date,source_table ORDER BY event_date,source_table;
