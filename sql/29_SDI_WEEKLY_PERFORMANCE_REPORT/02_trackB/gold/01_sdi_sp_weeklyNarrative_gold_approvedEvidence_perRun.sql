/* =====================================================================================
 PROJECT      : Weekly Narrative (Pulse TMS) | v8
 FILE         : 01_sdi_sp_weeklyNarrative_gold_approvedEvidence_perRun.sql
 LAYER        : TRACK B / GOLD
 OBJECT       : sdi_sp_weeklyNarrative_gold_approvedEvidence_perRun
 OUTPUT       : sdi_tbl_weeklyNarrative_gold_approvedEvidence_long (physical Delta)
 PURPOSE      : Materialize currently eligible Silver passages and optionally apply
                AI enrichments without modifying immutable Bronze or deterministic Silver.
 INPUT        : p_mode = REFRESH (Silver -> Gold eligibility reconciliation)
                        APPLY (validate and apply AI classification proposals)
                p_proposals_json = JSON ARRAY, passed in manageable notebook batches.
                p_week_end = optional Saturday scope for REFRESH.
 SAFETY       : AI tags are metadata, not verified evidence or causality; never promote
                drafts based on AI proposals; keep APPROVED_EVIDENCE governed by Silver.
 IDEMPOTENCY  : REFRESH MERGEs eligible rows and DELETEs no-longer-eligible rows;
                APPLY updates classification only for currently eligible Gold chunks.
 CONCURRENCY  : Serialize Gold writes using Databricks Workflow/task concurrency 1.
 DEPLOYMENT   : CALL ...('REFRESH', NULL, NULL) after the Silver SP creates its table.
 ===================================================================================== */
CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_weeklyNarrative_gold_approvedEvidence_perRun(
 IN p_mode STRING,
 IN p_proposals_json STRING DEFAULT NULL,
 IN p_week_end DATE DEFAULT NULL
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Gold knowledge persistence: REFRESH trusted eligibility; APPLY governed AI metadata.'
AS BEGIN
  DECLARE v_bad BIGINT DEFAULT 0;
  IF p_mode IS NULL OR p_mode NOT IN ('REFRESH','APPLY') THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Gold modes: REFRESH or APPLY';
  END IF;
  IF p_week_end IS NOT NULL AND dayofweek(p_week_end) <> 7 THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='p_week_end must be Saturday';
  END IF;
  CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_approvedEvidence_long (
    chunk_id STRING NOT NULL COMMENT 'Silver chunk stable identifier; one physical Gold row per eligible passage.',
    source_id STRING NOT NULL COMMENT 'Immutable Bronze source identifier.',
    report_id STRING NOT NULL COMMENT 'Weekly narrative logical report ID.',
    version_id STRING COMMENT 'Saved Agent Gold version, NULL for historical imported report.',
    report_week_end DATE NOT NULL COMMENT 'Sunday-Saturday reporting week ending Saturday.',
    section_key STRING NOT NULL COMMENT 'Governed report section key.',
    chunk_text STRING NOT NULL COMMENT 'Verbatim approved narrative item from Silver.',
    embedding_text STRING NOT NULL COMMENT 'Deterministic text to index or search semantically.',
    metric_keys ARRAY<STRING> COMMENT 'Candidate metric keys from Silver rule-based registry matching.',
    ai_metric_keys ARRAY<STRING> COMMENT 'Additional AI-proposed keys, constrained to active metric registry.',
    ai_category STRING COMMENT 'Controlled AI label: TRAFFIC, SPEND, ORDERS, CONVERSION, CAMPAIGN, DATA_QUALITY, OTHER.',
    ai_summary STRING COMMENT 'Optional AI-generated summary; not a source quote or verified statement.',
    model_id STRING COMMENT 'Endpoint/model that last enriched this passage.',
    source_type STRING NOT NULL COMMENT 'HISTORICAL_IMPORT or APP_VERSION.',
    source_uri STRING NOT NULL COMMENT 'Pointer to immutable original narrative version.',
    original_available_at TIMESTAMP COMMENT 'Known historical publication time; NULL if unknown.',
    ingested_at TIMESTAMP NOT NULL COMMENT 'Bronze ingestion time; NOT historical publication time.',
    last_enriched_at TIMESTAMP COMMENT 'Timestamp of last AI enrichment, NULL if not yet classified.',
    refreshed_at TIMESTAMP NOT NULL COMMENT 'Latest confirmed Silver eligibility refresh.'
  ) USING DELTA
  TBLPROPERTIES ('delta.enableChangeDataFeed'='true')
  COMMENT 'Physical curated Track B Gold evidence. Only published/eligible passages, AI annotations never grant approval.';

  IF p_mode='REFRESH' THEN
    CREATE OR REPLACE TEMP VIEW wn_gold_refresh_source AS
    SELECT s.chunk_id,s.source_id,s.report_id,s.version_id,s.report_week_end,s.section_key,
      s.chunk_text,s.search_text AS embedding_text,s.metric_keys,s.source_type,s.source_uri,
      b.original_available_at,b.ingested_at
    FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_silver_narrativeChunks_weekly s
    JOIN prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_bronze_narrativeBank_weekly b
      ON b.source_id=s.source_id
    WHERE s.is_evidence_eligible=TRUE AND s.knowledge_role='APPROVED_EVIDENCE'
      AND (p_week_end IS NULL OR s.report_week_end=p_week_end);
    SET v_bad=(SELECT count(*) FROM (SELECT chunk_id FROM wn_gold_refresh_source GROUP BY chunk_id HAVING count(*)>1));
    IF v_bad>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Duplicate source chunks for Gold'; END IF;
    MERGE INTO prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_approvedEvidence_long t
    USING wn_gold_refresh_source s ON t.chunk_id=s.chunk_id
    WHEN MATCHED THEN UPDATE SET
      t.source_id=s.source_id,t.report_id=s.report_id,t.version_id=s.version_id,
      t.report_week_end=s.report_week_end,t.section_key=s.section_key,
      t.chunk_text=s.chunk_text,t.embedding_text=s.embedding_text,
      t.metric_keys=s.metric_keys,t.source_type=s.source_type,t.source_uri=s.source_uri,
      t.original_available_at=s.original_available_at,t.ingested_at=s.ingested_at,
      t.refreshed_at=current_timestamp()
    WHEN NOT MATCHED THEN INSERT
     (chunk_id,source_id,report_id,version_id,report_week_end,section_key,chunk_text,
      embedding_text,metric_keys,ai_metric_keys,ai_category,ai_summary,model_id,source_type,
      source_uri,original_available_at,ingested_at,last_enriched_at,refreshed_at)
      VALUES(s.chunk_id,s.source_id,s.report_id,s.version_id,s.report_week_end,s.section_key,
      s.chunk_text,s.embedding_text,s.metric_keys,cast(array() AS ARRAY<STRING>),NULL,NULL,NULL,
      s.source_type,s.source_uri,s.original_available_at,s.ingested_at,NULL,current_timestamp());
    -- Revoke Gold evidence immediately after Silver marks content as rejected/superseded.
    -- For a weekly-scope refresh, do NOT remove other weeks' rows.
    DELETE FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_approvedEvidence_long g
      WHERE (p_week_end IS NULL OR g.report_week_end=p_week_end)
        AND NOT EXISTS (SELECT 1 FROM wn_gold_refresh_source s WHERE s.chunk_id=g.chunk_id);
  ELSE
    IF p_proposals_json IS NULL OR trim(p_proposals_json)='' THEN
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='APPLY requires JSON array of AI proposals';
    END IF;
    CREATE OR REPLACE TEMP VIEW wn_gold_ai_proposals AS
      SELECT p.chunk_id,p.category,p.summary,p.metric_keys,p.model_id
      FROM (SELECT explode(from_json(p_proposals_json,
       'ARRAY<STRUCT<chunk_id:STRING,category:STRING,summary:STRING,metric_keys:ARRAY<STRING>,model_id:STRING>>')) p);
    SET v_bad=(SELECT count(*) FROM wn_gold_ai_proposals WHERE chunk_id IS NULL OR category IS NULL
       OR category NOT IN ('TRAFFIC','SPEND','ORDERS','CONVERSION','CAMPAIGN','DATA_QUALITY','OTHER')
       OR model_id IS NULL OR trim(model_id)='');
    IF v_bad>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Invalid AI proposal fields'; END IF;
    SET v_bad=(SELECT count(*) FROM (SELECT chunk_id FROM wn_gold_ai_proposals GROUP BY chunk_id HAVING count(*)>1));
    IF v_bad>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Duplicate AI chunk proposals'; END IF;
    SET v_bad=(SELECT count(*) FROM wn_gold_ai_proposals p
      LEFT JOIN prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_approvedEvidence_long g
         ON g.chunk_id=p.chunk_id
      WHERE g.chunk_id IS NULL OR (p_week_end IS NOT NULL AND g.report_week_end<>p_week_end));
    IF v_bad>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='AI proposal references absent/ineligible Gold evidence'; END IF;
    SET v_bad=(SELECT count(*) FROM (
        SELECT explode(coalesce(metric_keys,cast(array() AS ARRAY<STRING>))) AS metric_key
        FROM wn_gold_ai_proposals
      ) keys LEFT JOIN prdrzranalytics.lab42.sdi_vw_weeklyNarrative_control_metricRegistry_static r
        ON keys.metric_key=r.metric_key AND r.is_active=TRUE AND r.metric_role='ACTUAL'
      WHERE r.metric_key IS NULL);
    IF v_bad>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='AI proposed unregistered metric key'; END IF;
    MERGE INTO prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_approvedEvidence_long g
    USING wn_gold_ai_proposals p ON g.chunk_id=p.chunk_id
    WHEN MATCHED THEN UPDATE SET
      g.ai_metric_keys=coalesce(p.metric_keys,cast(array() AS ARRAY<STRING>)),
      g.ai_category=p.category,g.ai_summary=p.summary,g.model_id=p.model_id,
      g.last_enriched_at=current_timestamp();
  END IF;
END;
