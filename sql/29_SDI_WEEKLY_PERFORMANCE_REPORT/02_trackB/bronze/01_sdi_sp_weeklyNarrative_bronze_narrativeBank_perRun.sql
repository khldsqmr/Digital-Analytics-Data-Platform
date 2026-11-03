/* ============================================================================
 PROJECT      : Weekly Narrative | Databricks Unity Catalog | v7
 FILE         : 01_sdi_sp_weeklyNarrative_bronze_narrativeBank_perRun.sql
 LAYER        : TRACK B / BRONZE
 PURPOSE      : Create one Bronze store, immutable JSONL import or app version sync
 DEPENDENCIES : Track B sources control, optional Agent reportVersions
 EXECUTION    : CALL (..., HISTORICAL_IMPORT, path) or (..., APP_SYNC, NULL)
 OWNERSHIP    : Production writes only by governed job/service principal.
 GRAIN        : As defined by target object. All reporting weeks Sun-Sat.
 SAFETY       : Review/approval not inferred from LLM output. Retry-safe writer.
 ============================================================================ */

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_weeklyNarrative_bronze_narrativeBank_perRun(
 IN p_mode STRING, IN p_historical_path STRING DEFAULT NULL
)
LANGUAGE SQL SQL SECURITY INVOKER MODIFIES SQL DATA
COMMENT 'Owns single raw Bronze narrative table; imports historical JSONL and synchronizes immutable app versions.'
AS BEGIN
  DECLARE v_invalid BIGINT DEFAULT 0;
  IF p_mode NOT IN ('HISTORICAL_IMPORT','APP_SYNC','ENSURE_TABLE') OR p_mode IS NULL THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Allowed modes: HISTORICAL_IMPORT, APP_SYNC, ENSURE_TABLE';
  END IF;
  CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_bronze_narrativeBank_weekly (
    source_id              STRING NOT NULL COMMENT 'SRC_<sha256>: unique raw source version identifier; deterministic from source key and payload hash.',
    source_system          STRING NOT NULL COMMENT 'HISTORICAL_JSONL or WEEKLY_NARRATIVE_APP; must be enabled in Track B control view.',
    source_type            STRING NOT NULL COMMENT 'HISTORICAL_IMPORT or APP_VERSION; documents supported later.',
    ingestion_mode         STRING NOT NULL COMMENT 'ONE_TIME_BACKFILL or APP_SYNC; identifies how it entered Bronze.',
    source_key             STRING NOT NULL COMMENT 'hist:b-0001 or app:VER_<UUID>; stable upstream record key.',
    original_source_id     STRING COMMENT 'Historical id (b-0001) or source-native identifier for traceability.',
    report_id              STRING NOT NULL COMMENT 'WN_YYYYMMDD_ALL: one logical report for each reporting week and scope.',
    version_id             STRING COMMENT 'VER_<UUID> for saved app drafts/edits; NULL for historical JSONL.',
    week_end               DATE NOT NULL COMMENT 'Reporting week ending Saturday; Sunday-Saturday.',
    title                  STRING COMMENT 'Original report title, if supplied.',
    raw_json               STRING NOT NULL COMMENT 'Unchanged JSON payload with steps; MUST NOT be overwritten when review changes.',
    content_hash           STRING NOT NULL COMMENT 'SHA256(raw_json), used to detect upstream content changes.',
    source_status_at_ingest STRING NOT NULL COMMENT 'HISTORICAL_PUBLISHED or DRAFT; immutable initial state, not a live approval flag.',
    created_by_user_id     STRING COMMENT 'Authenticated user responsible for app save; NULL for historical imports.',
    created_by_name        STRING COMMENT 'Display name captured at save; NULL for historical imports.',
    source_uri             STRING NOT NULL COMMENT 'Source Volume URI for JSONL or app://<version_id> for app versions.',
    original_available_at  TIMESTAMP COMMENT 'Actual source publication time, when known; NULL for imported history with unknown publication time.',
    ingested_at            TIMESTAMP NOT NULL COMMENT 'When the row landed in Bronze (UTC). Historical ingestion is NOT original publication time.'
  )
  USING DELTA
  TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
  COMMENT 'Track B single immutable raw store: historical JSON and every saved app narrative version.';
  IF p_mode = 'ENSURE_TABLE' THEN
    -- Creates the Bronze table without requiring a Volume or reading Agent Gold.
    -- Used by the one-time interactive upload notebook.
    SET v_invalid = 0;
  ELSEIF p_mode = 'HISTORICAL_IMPORT' THEN
    IF p_historical_path IS NULL OR NOT startswith(p_historical_path,'/Volumes/')
       OR NOT endswith(lower(p_historical_path),'.jsonl') THEN
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Historical file must be a governed /Volumes/.../*.jsonl path';
    END IF;
    CREATE OR REPLACE TEMP VIEW wn_bronze_import AS
      WITH raw AS (
        SELECT value AS raw_json, _metadata.file_path AS file_path
        FROM read_files(p_historical_path, format => 'text') WHERE trim(value) <> ''
      ), parsed AS (
        SELECT raw_json,file_path,
          get_json_object(raw_json,'$.id') AS native_id,
          try_cast(get_json_object(raw_json,'$.week_end') AS DATE) AS report_week_end,
          lower(get_json_object(raw_json,'$.decision')) AS decision
        FROM raw
      ), keys AS (
        SELECT *,sha2(raw_json,256) AS content_hash,
          concat('hist:',native_id) AS source_key
        FROM parsed
      )
      SELECT concat('SRC_',sha2(concat('HISTORICAL_JSONL|',source_key,'|',content_hash),256)) source_id,
        'HISTORICAL_JSONL' source_system,'HISTORICAL_IMPORT' source_type,
        'ONE_TIME_BACKFILL' ingestion_mode,source_key,native_id original_source_id,
        concat('WN_',date_format(report_week_end,'yyyyMMdd'),'_ALL') report_id,
        CAST(NULL AS STRING) version_id,report_week_end week_end,
        get_json_object(raw_json,'$.title') title, raw_json,content_hash,
        'HISTORICAL_PUBLISHED' source_status_at_ingest,
        CAST(NULL AS STRING) created_by_user_id, CAST(NULL AS STRING) created_by_name,
        file_path source_uri,CAST(NULL AS TIMESTAMP) original_available_at,
        current_timestamp() ingested_at
      FROM keys;
    SET v_invalid = (SELECT count(*) FROM wn_bronze_import
       WHERE original_source_id IS NULL OR week_end IS NULL OR dayofweek(week_end)<>7
          OR get_json_object(raw_json,'$.steps') IS NULL
          OR lower(get_json_object(raw_json,'$.decision')) <> 'published');
    IF v_invalid > 0 THEN
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Invalid historical JSONL: id, Saturday, steps and published required';
    END IF;
    SET v_invalid=(SELECT count(*) FROM (
      SELECT source_key FROM wn_bronze_import GROUP BY source_key HAVING count(*)>1));
    IF v_invalid > 0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Duplicate historical IDs in batch'; END IF;
    SET v_invalid=(SELECT count(*) FROM wn_bronze_import s
      JOIN prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_bronze_narrativeBank_weekly t
       ON s.source_system=t.source_system AND s.source_key=t.source_key
      WHERE s.content_hash<>t.content_hash);
    IF v_invalid > 0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Historical ID already imported with changed content'; END IF;
    MERGE INTO prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_bronze_narrativeBank_weekly t
    USING wn_bronze_import s ON t.source_id=s.source_id
    WHEN NOT MATCHED THEN INSERT *;
  ELSE
    -- Source of truth: all immutable Gold report versions, regardless of future review outcome.
    -- Canonical serialization follows four sections as source steps/items.
    CREATE OR REPLACE TEMP VIEW wn_bronze_app AS
    WITH versions AS (
      SELECT v.*, from_json(v.sections_json,
       'STRUCT<sections:ARRAY<STRUCT<section_key:STRING,text:STRING,evidence_ids:ARRAY<STRING>>>>') doc
      FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_reportVersions_wide v
    ), payloads AS (
      SELECT *,to_json(named_struct(
        'id',version_id,'week_end',cast(week_end AS STRING),
        'title',concat('Weekly Narrative ',cast(week_end AS STRING)),
        'steps',transform(doc.sections, x -> named_struct(
           'title',CASE x.section_key
             WHEN 'WHAT_HAPPENED' THEN 'What happened'
             WHEN 'WHY_IT_HAPPENED' THEN 'Why it happened'
             WHEN 'IS_IT_MEANINGFUL' THEN 'Is it meaningful?'
             WHEN 'RECOMMENDATIONS_AND_RISKS' THEN 'Recommendations & risks to watch'
             ELSE x.section_key END,
           'items',array(x.text))),
        'origin',named_struct('version_id',version_id,'report_id',report_id,
          'author_type',author_type,'investigation_id',investigation_id)
       )) AS raw_payload
      FROM versions
    )
    SELECT concat('SRC_',sha2(concat('WEEKLY_NARRATIVE_APP|app:',version_id,'|',sha2(raw_payload,256)),256)) source_id,
      'WEEKLY_NARRATIVE_APP' source_system, 'APP_VERSION' source_type,'APP_SYNC' ingestion_mode,
      concat('app:',version_id) source_key,version_id original_source_id,
      report_id, version_id,week_end,
      concat('Weekly Narrative ',cast(week_end AS STRING)) title,
      raw_payload raw_json,sha2(raw_payload,256) content_hash,
      'DRAFT' source_status_at_ingest, created_by_user_id,
      created_by_display_name created_by_name,concat('app://',version_id) source_uri,
      created_at original_available_at,current_timestamp() ingested_at
    FROM payloads;
    SET v_invalid=(SELECT count(*) FROM wn_bronze_app WHERE week_end IS NULL
      OR dayofweek(week_end)<>7 OR raw_json IS NULL);
    IF v_invalid > 0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Invalid Agent Gold version'; END IF;
    SET v_invalid=(SELECT count(*) FROM wn_bronze_app s JOIN
       prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_bronze_narrativeBank_weekly t
       ON s.source_key=t.source_key AND s.source_system=t.source_system
       WHERE s.content_hash<>t.content_hash);
    IF v_invalid>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Immutable app version changed content'; END IF;
    MERGE INTO prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_bronze_narrativeBank_weekly t
    USING wn_bronze_app s ON t.source_id=s.source_id
    WHEN NOT MATCHED THEN INSERT *;
  END IF;
END;
