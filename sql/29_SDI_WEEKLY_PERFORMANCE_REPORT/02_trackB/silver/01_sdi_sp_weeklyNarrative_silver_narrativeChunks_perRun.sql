/* ============================================================================
 PROJECT      : Weekly Narrative | Databricks Unity Catalog | v7
 FILE         : 01_sdi_sp_weeklyNarrative_silver_narrativeChunks_perRun.sql
 LAYER        : TRACK B / SILVER
 PURPOSE      : Creates one chunk table; parses Bronze and refreshes app approval status
 DEPENDENCIES : Bronze table, Metric Registry; optional Agent review events
 EXECUTION    : CALL (week_end, refresh_reviews)
 OWNERSHIP    : Production writes only by governed job/service principal.
 GRAIN        : As defined by target object. All reporting weeks Sun-Sat.
 SAFETY       : Review/approval not inferred from LLM output. Retry-safe writer.
 ============================================================================ */

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_weeklyNarrative_silver_narrativeChunks_perRun(
 IN p_week_end DATE DEFAULT NULL, IN p_refresh_reviews BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL SQL SECURITY INVOKER MODIFIES SQL DATA
COMMENT 'Parses Bronze JSONL into deterministic item chunks; refreshes status from review sources when enabled.'
AS BEGIN
  DECLARE v_bad BIGINT DEFAULT 0;
  CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_silver_narrativeChunks_weekly (
   chunk_id               STRING NOT NULL COMMENT 'CHK_<sha256>: deterministic source_id, section ordinal, item ordinal; never reused.',
   source_id              STRING NOT NULL COMMENT 'FK-like reference to immutable Bronze row.',
   report_id              STRING NOT NULL COMMENT 'WN_YYYYMMDD_ALL from Bronze; logical reporting-week report.',
   version_id             STRING COMMENT 'App draft/revision ID; NULL for historical imports.',
   report_week_end        DATE NOT NULL COMMENT 'Saturday report period end.',
   source_type            STRING NOT NULL COMMENT 'HISTORICAL_IMPORT or APP_VERSION; copied from Bronze.',
   section_key            STRING NOT NULL COMMENT 'WHAT_HAPPENED / WHY_IT_HAPPENED / IS_IT_MEANINGFUL / RECOMMENDATIONS_AND_RISKS.',
   section_index          INT NOT NULL COMMENT 'Zero-based section ordinal in original source, used for stable chunk identity.',
   item_index             INT NOT NULL COMMENT 'Zero-based item ordinal within section.',
   chunk_text             STRING NOT NULL COMMENT 'Verbatim original item, never regenerated or rewritten by AI.',
   search_text            STRING NOT NULL COMMENT 'Search input: section label + original passage; derived deterministically.',
   metric_keys            ARRAY<STRING> COMMENT 'Candidate registry metric keys detected by controlled alias matching; never assumed verified.',
   tag_status             STRING NOT NULL COMMENT 'RULE_CANDIDATE or NOT_TAGGED; AI may later propose improved tags via reviewed job.',
   review_status          STRING NOT NULL COMMENT 'HISTORICAL_PUBLISHED / DRAFT / ACCEPTED / REJECTED / CHANGES_REQUESTED / PUBLISHED / SUPERSEDED, derived from events.',
   knowledge_role         STRING NOT NULL COMMENT 'APPROVED_EVIDENCE / NEGATIVE_EXAMPLE / PENDING_REVIEW / SUPERSEDED_EVIDENCE.',
   is_evidence_eligible   BOOLEAN NOT NULL COMMENT 'True ONLY for currently published source passages, not drafts or rejected content.',
   is_feedback_eligible   BOOLEAN NOT NULL COMMENT 'True for rejected/flagged section passages; may inform future feedback evaluation, not facts.',
   last_review_event_id   STRING COMMENT 'Most recent applicable reviewer event for this section, if any.',
   source_uri             STRING NOT NULL COMMENT 'Provenance pointer back to Bronze source origin.',
   created_at             TIMESTAMP NOT NULL COMMENT 'First insertion timestamp of this passage.',
   classified_at          TIMESTAMP NOT NULL COMMENT 'Most recent eligibility refresh timestamp.'
  )
  USING DELTA
  TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
  COMMENT 'Track B single normalized knowledge store: all historical and app narratives with controlled evidence/feedback eligibility.';
  -- Each original step/item is preserved verbatim. Avoid LLM rewrites of source content.
  CREATE OR REPLACE TEMP VIEW wn_silver_parsed AS
    WITH parsed AS (
      SELECT b.*,from_json(b.raw_json,
        'STRUCT<steps:ARRAY<STRUCT<title:STRING,items:ARRAY<STRING>>>>') doc
      FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_bronze_narrativeBank_weekly b
      WHERE p_week_end IS NULL OR b.week_end=p_week_end
    ), items AS (
      SELECT p.*,sec_index,sec_obj,item_index,item_text
      FROM parsed p
      LATERAL VIEW posexplode(p.doc.steps) sec AS sec_index,sec_obj
      LATERAL VIEW posexplode(sec_obj.items) itm AS item_index,item_text
    )
    SELECT concat('CHK_',sha2(concat(source_id,'|',cast(sec_index AS STRING),'|',cast(item_index AS STRING)),256)) chunk_id,
      source_id,report_id,version_id,week_end report_week_end,source_type,
      CASE lower(trim(sec_obj.title))
        WHEN 'what happened' THEN 'WHAT_HAPPENED'
        WHEN 'why it happened' THEN 'WHY_IT_HAPPENED'
        WHEN 'is it meaningful?' THEN 'IS_IT_MEANINGFUL'
        WHEN 'recommendations & risks to watch' THEN 'RECOMMENDATIONS_AND_RISKS'
        ELSE 'UNMAPPED_SECTION' END section_key,
      sec_index section_index,item_index, item_text chunk_text,
      concat(sec_obj.title,' | ',item_text) search_text,
      cast(array() AS ARRAY<STRING>) metric_keys,'NOT_TAGGED' tag_status,
      CASE WHEN source_type='HISTORICAL_IMPORT' THEN 'HISTORICAL_PUBLISHED' ELSE 'DRAFT' END review_status,
      CASE WHEN source_type='HISTORICAL_IMPORT' THEN 'APPROVED_EVIDENCE' ELSE 'PENDING_REVIEW' END knowledge_role,
      source_type='HISTORICAL_IMPORT' is_evidence_eligible,
      FALSE is_feedback_eligible,CAST(NULL AS STRING) last_review_event_id,
      source_uri,current_timestamp() created_at,current_timestamp() classified_at
    FROM items;
  SET v_bad=(SELECT count(*) FROM wn_silver_parsed WHERE section_key='UNMAPPED_SECTION'
       OR chunk_text IS NULL OR trim(chunk_text)='');
  IF v_bad>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Unsupported section or empty narrative item'; END IF;
  SET v_bad=(SELECT count(*) FROM (SELECT chunk_id FROM wn_silver_parsed GROUP BY chunk_id HAVING count(*)>1));
  IF v_bad>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Duplicate chunk IDs generated'; END IF;
  MERGE INTO prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_silver_narrativeChunks_weekly t
  USING wn_silver_parsed s ON t.chunk_id=s.chunk_id
  WHEN NOT MATCHED THEN INSERT *;
  -- Rule-based metric-key tagging matches registry aliases; this is never a verified fact claim.
  MERGE INTO prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_silver_narrativeChunks_weekly t
  USING (
    SELECT s.chunk_id,
      filter(collect_set(CASE WHEN array_exists(r.aliases,
           a -> length(a)>=3 AND instr(lower(s.search_text),lower(a))>0)
        THEN r.metric_key END),x -> x IS NOT NULL) AS detected_keys
    FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_silver_narrativeChunks_weekly s
    CROSS JOIN prdrzranalytics.lab42.sdi_vw_weeklyNarrative_control_metricRegistry_static r
    WHERE (p_week_end IS NULL OR s.report_week_end=p_week_end) AND r.metric_role='ACTUAL' AND r.is_active=TRUE
    GROUP BY s.chunk_id
  ) x ON t.chunk_id=x.chunk_id
  WHEN MATCHED THEN UPDATE SET
    t.metric_keys=x.detected_keys,
    t.tag_status=IF(size(x.detected_keys)>0,'RULE_CANDIDATE','NOT_TAGGED');
  IF p_refresh_reviews THEN
    -- Publication events are the authority; no AI proposal auto-approves.
    -- This section intentionally uses Gold review events and report status only after Agent deployment.
    CREATE OR REPLACE TEMP VIEW wn_silver_review_state AS
    WITH latest_published AS (
       SELECT version_id,report_id FROM (
         SELECT version_id,report_id,
           ROW_NUMBER() OVER (PARTITION BY report_id ORDER BY reviewed_at DESC,review_event_id DESC) rn
         FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_reviewEvents_long
         WHERE review_action='PUBLISH' AND section_key='REPORT'
       ) x WHERE rn=1
    ), last_review AS (
       SELECT version_id,section_key,review_event_id,review_action FROM (
         SELECT version_id,section_key,review_event_id,review_action,
           ROW_NUMBER() OVER (PARTITION BY version_id,section_key
             ORDER BY reviewed_at DESC,review_event_id DESC) rn
         FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_reviewEvents_long
         WHERE review_action IN ('ACCEPT','REJECT','REQUEST_CHANGES')
       ) x WHERE rn=1
    )
    SELECT c.chunk_id,
      CASE WHEN p.version_id IS NOT NULL THEN 'PUBLISHED'
           WHEN c.version_id IS NOT NULL AND EXISTS (
              SELECT 1 FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_reviewEvents_long e
              WHERE e.version_id=c.version_id AND e.review_action='PUBLISH') THEN 'SUPERSEDED'
           WHEN r.review_action='REJECT' THEN 'REJECTED'
           WHEN r.review_action='REQUEST_CHANGES' THEN 'CHANGES_REQUESTED'
           WHEN r.review_action='ACCEPT' THEN 'ACCEPTED'
           ELSE 'DRAFT' END review_status,
      r.review_event_id last_review_event_id
    FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_silver_narrativeChunks_weekly c
    LEFT JOIN latest_published p ON p.version_id=c.version_id
    LEFT JOIN last_review r ON r.version_id=c.version_id AND r.section_key=c.section_key
    WHERE c.source_type='APP_VERSION' AND (p_week_end IS NULL OR c.report_week_end=p_week_end);
    MERGE INTO prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_silver_narrativeChunks_weekly t
    USING wn_silver_review_state s ON t.chunk_id=s.chunk_id
    WHEN MATCHED THEN UPDATE SET
      t.review_status=s.review_status,
      t.knowledge_role=CASE WHEN s.review_status='PUBLISHED' THEN 'APPROVED_EVIDENCE'
                            WHEN s.review_status IN ('REJECTED','CHANGES_REQUESTED') THEN 'NEGATIVE_EXAMPLE'
                            WHEN s.review_status='SUPERSEDED' THEN 'SUPERSEDED_EVIDENCE'
                            ELSE 'PENDING_REVIEW' END,
      t.is_evidence_eligible=(s.review_status='PUBLISHED'),
      t.is_feedback_eligible=(s.review_status IN ('REJECTED','CHANGES_REQUESTED')),
      t.last_review_event_id=s.last_review_event_id,
      t.classified_at=current_timestamp();
  END IF;
END;
