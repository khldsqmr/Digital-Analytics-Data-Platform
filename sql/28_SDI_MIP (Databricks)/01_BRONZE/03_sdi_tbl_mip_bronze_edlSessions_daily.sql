
-- ###########################################################################
-- BEGIN bronze/03_sdi_tbl_mip_bronze_edlSessions_daily.sql
-- ###########################################################################

-- ============================================================================
-- FILE  : 03_sdi_tbl_mip_bronze_edlSessions_daily.sql
-- LAYER : BRONZE
-- PURPOSE:
--   Narrow raw SESSION_SUMMARY_FACT snapshot with Pacific-boundary widening.
-- ============================================================================

USE CATALOG prdrzranalytics;
USE SCHEMA lab42;
-- ----------------------------------------------------------------------------
-- Session summary
-- Bronze keeps all statuses; Silver determines valid lifecycle statuses.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sdi_tbl_mip_bronze_edlSessions_daily
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
  cast(NULL AS STRING) AS _runId,
  current_timestamp() AS _ingestedAt
FROM prd_dbi_analytics.silver_digital_interactions.session_summary_fact
WHERE 1 = 0;

CREATE OR REPLACE PROCEDURE sdi_sp_mip_bronze_edlSessions_daily(
  p_runId           STRING,
  p_asOfDate        DATE DEFAULT NULL,
  p_eventWindowDays INT DEFAULT 1
)
LANGUAGE SQL
SQL SECURITY INVOKER
COMMENT 'Bronze sessions. UTC source partitions are widened one day on each side to cover Pacific session-start dates.'
AS
BEGIN
  DECLARE v_asOfDate       DATE DEFAULT coalesce(
    p_asOfDate,
    to_date(from_utc_timestamp(current_timestamp(), 'America/Los_Angeles'))
  );
  DECLARE v_targetEnd      DATE DEFAULT v_asOfDate;
  DECLARE v_targetStart    DATE DEFAULT date_add(v_asOfDate, -(p_eventWindowDays - 1));
  DECLARE v_sourceStart    DATE DEFAULT date_add(v_targetStart, -1);
  DECLARE v_sourceEnd      DATE DEFAULT date_add(v_targetEnd, 1);

  IF p_eventWindowDays < 1 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'p_eventWindowDays must be >= 1';
  END IF;

  INSERT INTO sdi_tbl_mip_bronze_edlSessions_daily
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
    p_runId AS _runId,
    current_timestamp() AS _ingestedAt
  FROM prd_dbi_analytics.silver_digital_interactions.session_summary_fact
  WHERE session_start_date BETWEEN v_sourceStart AND v_sourceEnd;
END;

-- ----------------------------------------------------------------------------


-- ###########################################################################
-- END bronze/03_sdi_tbl_mip_bronze_edlSessions_daily.sql
-- ###########################################################################

