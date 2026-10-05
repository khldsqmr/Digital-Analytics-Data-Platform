-- ============================================================================
-- FILE  : 03_sdi_sp_mip_bronze_edlSessionSummaryFact_daily.sql
-- LAYER : BRONZE
-- SOURCE: prd_dbi_analytics.silver_digital_interactions.session_summary_fact
-- PURPOSE: Persist only SSF sessions referenced by the requested Bronze SEF event window.
-- DEPENDENCY / SESSION CONTRACT: Run Bronze SEF first. The exact non-null session_id population from the requested SEF window is used to retrieve SSF rows; no arbitrary session_start_date widening is used because sessions can span event-date boundaries.
-- SESSION HELPER CONTRACT: session_start_ts_pst, session_start_date_pst, week_start_date and week_end_date are deterministic session-grain calendar helpers derived once from authoritative session_start_time, avoiding repeated hit-grain timezone/calendar computation in Silver 01.
-- UTM CONTRACT: utm_source, utm_medium and utm_campaign are parsed once per session from entry_page_url_full. Raw entry URL fields are retained. Missing parameters remain NULL in Bronze; Silver maps NULL/blank to '(not set)'.
-- PREFLIGHT: Validates requested Bronze SEF date coverage, presence of non-null session_id values and complete source SSF coverage before any target write.
-- PHYSICAL DESIGN: One clustering key only: session_id, because both the accumulating MERGE and Silver 01 SSF join are session_id-driven. No OPTIMIZE is forced by this procedure.
-- FRESH-BUILD CONTRACT: The target is created with the complete final schema; no runtime schema-evolution logic is required.
-- ============================================================================
CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessionSummaryFact_daily(
    IN p_asOfDate DATE DEFAULT NULL,
    IN p_eventWindowDays INT DEFAULT 1,
    IN p_validateOnly BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Bronze MIP accumulating SSF snapshot with session-grain UTM and Pacific reporting-calendar helpers.'
AS
BEGIN
    DECLARE v_asOfDate DATE DEFAULT coalesce(p_asOfDate,date_add(to_date(from_utc_timestamp(current_timestamp(),'America/Los_Angeles')),-1));
    DECLARE v_windowEnd DATE DEFAULT v_asOfDate;
    DECLARE v_windowStart DATE DEFAULT date_add(v_asOfDate,-(p_eventWindowDays-1));
    DECLARE v_sefDateCount BIGINT DEFAULT 0;
    DECLARE v_missingSourceSsfSessionCount BIGINT DEFAULT 0;
    IF p_eventWindowDays IS NULL OR p_eventWindowDays<1 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='p_eventWindowDays must be >= 1.';
    END IF;
    SET v_sefDateCount=(
        SELECT COUNT(DISTINCT event_date)
        FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily
        WHERE event_date BETWEEN v_windowStart AND v_windowEnd
    );
    IF v_sefDateCount<>p_eventWindowDays THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Bronze SESSION_EVENT_FACT does not contain every requested event date. Load/rebuild Bronze SEF before Bronze SSF.';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily
        WHERE event_date BETWEEN v_windowStart AND v_windowEnd AND session_id IS NOT NULL
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Bronze SESSION_EVENT_FACT has no non-null session_id values for the requested event window.';
    END IF;
    SET v_missingSourceSsfSessionCount=(
        SELECT COUNT(*)
        FROM (
            SELECT e.session_id
            FROM (
                SELECT session_id
                FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily
                WHERE event_date BETWEEN v_windowStart AND v_windowEnd AND session_id IS NOT NULL
                GROUP BY session_id
            ) e
            LEFT ANTI JOIN prd_dbi_analytics.silver_digital_interactions.session_summary_fact s
                ON s.session_id=e.session_id
        ) missing
    );
    IF v_missingSourceSsfSessionCount>0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='SESSION_SUMMARY_FACT does not contain every session_id referenced by Bronze SEF for the requested event window. Bronze SSF was not refreshed.';
    END IF;
    IF p_validateOnly THEN
        SELECT 'VALIDATION_ONLY' AS status,v_windowStart AS requestedEventWindowStart,v_windowEnd AS requestedEventWindowEnd,v_sefDateCount AS bronzeSefDateCount,v_missingSourceSsfSessionCount AS missingSourceSsfSessions,'Bronze SEF + source SSF' AS sourceObjects,'No Bronze SSF table was created or modified.' AS message;
    ELSE
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily(
            session_id STRING COMMENT 'Stable authoritative server session identifier.',
            source_table STRING COMMENT 'Session source: t_web_interactions or t_app_interactions.',
            canonical_user_id STRING COMMENT 'Current canonical user associated with the session.',
            identity_status STRING COMMENT 'Current session identity result.',
            session_status STRING COMMENT 'Session lifecycle status.',
            session_start_date DATE COMMENT 'Raw authoritative UTC session-start date.',
            session_start_time TIMESTAMP COMMENT 'Raw authoritative UTC session-start timestamp.',
            session_end_time TIMESTAMP COMMENT 'Raw authoritative UTC session-end timestamp.',
            session_start_ts_pst TIMESTAMP COMMENT 'Pacific session-start timestamp derived once per session.',
            session_start_date_pst DATE COMMENT 'Pacific session-start date derived once per session.',
            week_start_date DATE COMMENT 'Sunday reporting-week start derived from session_start_date_pst.',
            week_end_date DATE COMMENT 'Saturday reporting-week end derived from session_start_date_pst.',
            entry_page_url_path STRING COMMENT 'Raw SSF entry URL path.',
            entry_page_url_full STRING COMMENT 'Raw SSF complete entry URL.',
            utm_source STRING COMMENT 'Parsed raw utm_source from entry_page_url_full; NULL when absent.',
            utm_medium STRING COMMENT 'Parsed raw utm_medium from entry_page_url_full; NULL when absent.',
            utm_campaign STRING COMMENT 'Parsed raw utm_campaign from entry_page_url_full; NULL when absent.',
            _ingestedAt TIMESTAMP COMMENT 'Timestamp when the Bronze SSF row was inserted/refreshed.'
        )
        USING DELTA
        CLUSTER BY (session_id)
        COMMENT 'Bronze: one accumulating SSF row per session_id with deterministic session-grain UTM and Pacific reporting-calendar helpers.';
        MERGE INTO prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily t
        USING (
            WITH requestedSessions AS (
                SELECT session_id
                FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily
                WHERE event_date BETWEEN v_windowStart AND v_windowEnd AND session_id IS NOT NULL
                GROUP BY session_id
            ),
            sourceSessions AS (
                SELECT
                    s.session_id,s.source_table,s.canonical_user_id,s.identity_status,s.session_status,s.session_start_date,s.session_start_time,s.session_end_time,s.entry_page_url_path,s.entry_page_url_full,
                    from_utc_timestamp(try_cast(s.session_start_time AS TIMESTAMP),'America/Los_Angeles') AS session_start_ts_pst,
                    CASE
                        WHEN lower(coalesce(trim(cast(s.entry_page_url_full AS STRING)),'')) LIKE 'http%' THEN trim(cast(s.entry_page_url_full AS STRING))
                        WHEN nullif(trim(cast(s.entry_page_url_full AS STRING)),'') IS NOT NULL THEN concat('https://',trim(cast(s.entry_page_url_full AS STRING)))
                        ELSE NULL
                    END AS entry_url_for_parse
                FROM prd_dbi_analytics.silver_digital_interactions.session_summary_fact s
                INNER JOIN requestedSessions e ON e.session_id=s.session_id
            ),
            parsedSessions AS (
                SELECT
                    session_id,source_table,canonical_user_id,identity_status,session_status,session_start_date,session_start_time,session_end_time,
                    session_start_ts_pst,
                    to_date(session_start_ts_pst) AS session_start_date_pst,
                    date_add(to_date(session_start_ts_pst),1-dayofweek(to_date(session_start_ts_pst))) AS week_start_date,
                    date_add(to_date(session_start_ts_pst),7-dayofweek(to_date(session_start_ts_pst))) AS week_end_date,
                    entry_page_url_path,entry_page_url_full,
                    nullif(try_parse_url(entry_url_for_parse,'QUERY','utm_source'),'') AS utm_source,
                    nullif(try_parse_url(entry_url_for_parse,'QUERY','utm_medium'),'') AS utm_medium,
                    nullif(try_parse_url(entry_url_for_parse,'QUERY','utm_campaign'),'') AS utm_campaign,
                    current_timestamp() AS _ingestedAt
                FROM sourceSessions
            )
            SELECT session_id,source_table,canonical_user_id,identity_status,session_status,session_start_date,session_start_time,session_end_time,session_start_ts_pst,session_start_date_pst,week_start_date,week_end_date,entry_page_url_path,entry_page_url_full,utm_source,utm_medium,utm_campaign,_ingestedAt
            FROM parsedSessions
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
            t.session_start_ts_pst=s.session_start_ts_pst,
            t.session_start_date_pst=s.session_start_date_pst,
            t.week_start_date=s.week_start_date,
            t.week_end_date=s.week_end_date,
            t.entry_page_url_path=s.entry_page_url_path,
            t.entry_page_url_full=s.entry_page_url_full,
            t.utm_source=s.utm_source,
            t.utm_medium=s.utm_medium,
            t.utm_campaign=s.utm_campaign,
            t._ingestedAt=s._ingestedAt
        WHEN NOT MATCHED THEN INSERT(
            session_id,source_table,canonical_user_id,identity_status,session_status,session_start_date,session_start_time,session_end_time,session_start_ts_pst,session_start_date_pst,week_start_date,week_end_date,entry_page_url_path,entry_page_url_full,utm_source,utm_medium,utm_campaign,_ingestedAt
        ) VALUES(
            s.session_id,s.source_table,s.canonical_user_id,s.identity_status,s.session_status,s.session_start_date,s.session_start_time,s.session_end_time,s.session_start_ts_pst,s.session_start_date_pst,s.week_start_date,s.week_end_date,s.entry_page_url_path,s.entry_page_url_full,s.utm_source,s.utm_medium,s.utm_campaign,s._ingestedAt
        );
        SELECT 'SUCCESS' AS status,v_windowStart AS requestedEventWindowStart,v_windowEnd AS requestedEventWindowEnd,v_sefDateCount AS bronzeSefDateCount,'prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily' AS targetObject;
    END IF;
END;
-- DEVELOPMENT / TEST EXAMPLES
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessionSummaryFact_daily(p_asOfDate=>DATE '2026-10-03',p_eventWindowDays=>10,p_validateOnly=>TRUE);
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessionSummaryFact_daily(p_asOfDate=>DATE '2026-10-03',p_eventWindowDays=>10,p_validateOnly=>FALSE);
-- SELECT session_id,COUNT(*) AS rowCount FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily GROUP BY session_id HAVING COUNT(*)>1 LIMIT 100;
-- SELECT source_table,COUNT(*) AS sessions,COUNT_IF(utm_source IS NOT NULL) AS sessionsWithUtmSource,COUNT_IF(session_start_date_pst IS NULL) AS missingSessionStartDatePst FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily GROUP BY source_table;
