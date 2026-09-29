
-- ###########################################################################
-- BEGIN bronze/02_sdi_tbl_mip_bronze_edlHitSessionLinks_daily.sql
-- ###########################################################################

-- ============================================================================
-- FILE  : 02_sdi_tbl_mip_bronze_edlHitSessionLinks_daily.sql
-- LAYER : BRONZE
-- PURPOSE:
--   Narrow raw SESSION_EVENT_FACT hit-to-session assignments.
-- ============================================================================

USE CATALOG prdrzranalytics;
USE SCHEMA lab42;
-- ----------------------------------------------------------------------------
-- Hit -> session link
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sdi_tbl_mip_bronze_edlHitSessionLinks_daily
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
  cast(NULL AS STRING) AS _runId,
  current_timestamp() AS _ingestedAt
FROM prd_dbi_analytics.silver_digital_interactions.session_event_fact
WHERE 1 = 0;

CREATE OR REPLACE PROCEDURE sdi_sp_mip_bronze_edlHitSessionLinks_daily(
  p_runId           STRING,
  p_asOfDate        DATE DEFAULT NULL,
  p_eventWindowDays INT DEFAULT 1
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  DECLARE v_asOfDate    DATE DEFAULT coalesce(
    p_asOfDate,
    to_date(from_utc_timestamp(current_timestamp(), 'America/Los_Angeles'))
  );
  DECLARE v_windowEnd   DATE DEFAULT v_asOfDate;
  DECLARE v_windowStart DATE DEFAULT date_add(v_asOfDate, -(p_eventWindowDays - 1));

  IF p_eventWindowDays < 1 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'p_eventWindowDays must be >= 1';
  END IF;

  INSERT INTO sdi_tbl_mip_bronze_edlHitSessionLinks_daily
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
    p_runId AS _runId,
    current_timestamp() AS _ingestedAt
  FROM prd_dbi_analytics.silver_digital_interactions.session_event_fact
  WHERE event_date BETWEEN v_windowStart AND v_windowEnd;
END;

-- ----------------------------------------------------------------------------


-- ###########################################################################
-- END bronze/02_sdi_tbl_mip_bronze_edlHitSessionLinks_daily.sql
-- ###########################################################################

