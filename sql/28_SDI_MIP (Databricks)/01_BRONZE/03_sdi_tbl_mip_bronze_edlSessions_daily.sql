-- ============================================================================
-- FILE  : 03_sdi_sp_mip_bronze_edlSessions_daily.sql
-- LAYER : BRONZE
-- PURPOSE:
--   Persist a narrow raw SESSION_SUMMARY_FACT snapshot with source-window
--   widening around the requested reporting window.
--
-- DESIGN:
--   - One top-level SQL statement per file.
--   - No required run/job ID during development.
--   - Validates inputs/source before creating or writing the Bronze target.
--   - p_validateOnly = TRUE performs preflight only; no table is created/written.
--   - Default as-of date is the previous Pacific calendar day.
--
-- NOTE:
--   Bronze keeps all session statuses. Silver determines valid lifecycle status.
-- ============================================================================

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessions_daily(
    IN p_asOfDate        DATE    DEFAULT NULL,
    IN p_eventWindowDays INT     DEFAULT 1,
    IN p_validateOnly    BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Bronze sessions. Source window is widened one day on each side of the requested reporting window.'
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

    DECLARE v_targetEnd DATE DEFAULT v_asOfDate;

    DECLARE v_targetStart DATE DEFAULT date_add(
        v_asOfDate,
        -(p_eventWindowDays - 1)
    );

    DECLARE v_sourceStart DATE DEFAULT date_add(v_targetStart, -1);
    DECLARE v_sourceEnd   DATE DEFAULT date_add(v_targetEnd, 1);

    -- ------------------------------------------------------------------------
    -- 1. Parameter validation
    -- ------------------------------------------------------------------------
    IF p_eventWindowDays IS NULL OR p_eventWindowDays < 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'p_eventWindowDays must be >= 1.';
    END IF;

    -- ------------------------------------------------------------------------
    -- 2. Source preflight
    --    The widened source range preserves the original session-boundary logic.
    -- ------------------------------------------------------------------------
    IF NOT EXISTS (
        SELECT 1
        FROM prd_dbi_analytics.silver_digital_interactions.session_summary_fact
        WHERE session_start_date BETWEEN v_sourceStart AND v_sourceEnd
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'SESSION_SUMMARY_FACT returned no rows for the widened Bronze source window. Bronze was not created or refreshed.';
    END IF;

    -- ------------------------------------------------------------------------
    -- 3. Validation-only mode
    -- ------------------------------------------------------------------------
    IF p_validateOnly THEN

        SELECT
            'VALIDATION_ONLY' AS status,
            v_targetStart     AS requestedWindowStart,
            v_targetEnd       AS requestedWindowEnd,
            v_sourceStart     AS widenedSourceStart,
            v_sourceEnd       AS widenedSourceEnd,
            'prd_dbi_analytics.silver_digital_interactions.session_summary_fact' AS sourceObject,
            'No Bronze table was created or modified.' AS message;

    ELSE

        -- --------------------------------------------------------------------
        -- 4. Create target only after preflight passes.
        -- --------------------------------------------------------------------
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessions_daily
        USING DELTA
        CLUSTER BY (session_start_date)
        COMMENT 'Bronze: narrow raw copy of session_summary_fact. One row per EDL session.'
        AS
        SELECT
            session_id,
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
        WHERE 1 = 0;

        -- --------------------------------------------------------------------
        -- 5. Refresh the widened source window.
        -- --------------------------------------------------------------------
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessions_daily
        REPLACE WHERE session_start_date BETWEEN v_sourceStart AND v_sourceEnd
        SELECT
            session_id,
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
        WHERE session_start_date BETWEEN v_sourceStart AND v_sourceEnd;

        SELECT
            'SUCCESS'      AS status,
            v_targetStart  AS requestedWindowStart,
            v_targetEnd    AS requestedWindowEnd,
            v_sourceStart  AS loadedSourceStart,
            v_sourceEnd    AS loadedSourceEnd,
            'prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessions_daily' AS targetObject;

    END IF;
END;

-- Development examples (run separately after deploying the procedure):
--
-- Preflight only:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessions_daily(
--     p_asOfDate        => DATE '2026-09-28',
--     p_eventWindowDays => 1,
--     p_validateOnly    => TRUE
-- );
--
-- Load one reporting day; the session source window is widened automatically:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessions_daily(
--     p_asOfDate        => DATE '2026-09-28',
--     p_eventWindowDays => 1,
--     p_validateOnly    => FALSE
-- );
--
-- Default previous Pacific day:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessions_daily();
