-- ============================================================================
-- FILE  : 02_sdi_sp_mip_bronze_edlHitSessionLinks_daily.sql
-- LAYER : BRONZE
-- PURPOSE:
--   Persist a narrow raw SESSION_EVENT_FACT hit-to-session assignment snapshot.
--
-- DESIGN:
--   - One top-level SQL statement per file.
--   - No required run/job ID during development.
--   - Validates inputs/source before creating or writing the Bronze target.
--   - p_validateOnly = TRUE performs preflight only; no table is created/written.
--   - Default as-of date is the previous Pacific calendar day.
-- ============================================================================

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_bronze_edlHitSessionLinks_daily(
    IN p_asOfDate        DATE    DEFAULT NULL,
    IN p_eventWindowDays INT     DEFAULT 1,
    IN p_validateOnly    BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Bronze SESSION_EVENT_FACT hit-to-session links. Validates first, then creates the target if needed and replaces only the requested event-date window.'
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

    -- ------------------------------------------------------------------------
    -- 1. Parameter validation
    -- ------------------------------------------------------------------------
    IF p_eventWindowDays IS NULL OR p_eventWindowDays < 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'p_eventWindowDays must be >= 1.';
    END IF;

    -- ------------------------------------------------------------------------
    -- 2. Source preflight
    -- ------------------------------------------------------------------------
    IF NOT EXISTS (
        SELECT 1
        FROM prd_dbi_analytics.silver_digital_interactions.session_event_fact
        WHERE event_date BETWEEN v_windowStart AND v_windowEnd
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'SESSION_EVENT_FACT returned no rows for the requested Bronze window. Bronze was not created or refreshed.';
    END IF;

    -- ------------------------------------------------------------------------
    -- 3. Validation-only mode
    -- ------------------------------------------------------------------------
    IF p_validateOnly THEN

        SELECT
            'VALIDATION_ONLY' AS status,
            v_windowStart     AS requestedWindowStart,
            v_windowEnd       AS requestedWindowEnd,
            'prd_dbi_analytics.silver_digital_interactions.session_event_fact' AS sourceObject,
            'No Bronze table was created or modified.' AS message;

    ELSE

        -- --------------------------------------------------------------------
        -- 4. Create target only after preflight passes.
        -- --------------------------------------------------------------------
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHitSessionLinks_daily
        USING DELTA
        CLUSTER BY (event_date, source_table)
        COMMENT 'Bronze: narrow raw copy of session_event_fact. One row per EDL hit-to-session assignment.'
        AS
        SELECT
            row_identity_hash,
            event_date,
            source_table,
            event_timestamp_utc,
            session_id,
            canonical_user_id,
            identity_status,
            visitor_key_type,
            visitor_key_value,
            session_assignment_method,
            assignment_version,
            pipeline_batch_id,
            load_datetime_pst,
            current_timestamp() AS _ingestedAt

        FROM prd_dbi_analytics.silver_digital_interactions.session_event_fact
        WHERE 1 = 0;

        -- --------------------------------------------------------------------
        -- 5. Replace only the requested event-date window.
        -- --------------------------------------------------------------------
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHitSessionLinks_daily
        REPLACE WHERE event_date BETWEEN v_windowStart AND v_windowEnd
        SELECT
            row_identity_hash,
            event_date,
            source_table,
            event_timestamp_utc,
            session_id,
            canonical_user_id,
            identity_status,
            visitor_key_type,
            visitor_key_value,
            session_assignment_method,
            assignment_version,
            pipeline_batch_id,
            load_datetime_pst,
            current_timestamp() AS _ingestedAt

        FROM prd_dbi_analytics.silver_digital_interactions.session_event_fact
        WHERE event_date BETWEEN v_windowStart AND v_windowEnd;

        SELECT
            'SUCCESS'     AS status,
            v_windowStart AS loadedWindowStart,
            v_windowEnd   AS loadedWindowEnd,
            'prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHitSessionLinks_daily' AS targetObject;

    END IF;
END;

-- Development examples (run separately after deploying the procedure):
--
-- Preflight only:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlHitSessionLinks_daily(
--     p_asOfDate        => DATE '2026-09-28',
--     p_eventWindowDays => 1,
--     p_validateOnly    => TRUE
-- );
--
-- Load one explicit day:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlHitSessionLinks_daily(
--     p_asOfDate        => DATE '2026-09-28',
--     p_eventWindowDays => 1,
--     p_validateOnly    => FALSE
-- );
--
-- Default previous Pacific day:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlHitSessionLinks_daily();
