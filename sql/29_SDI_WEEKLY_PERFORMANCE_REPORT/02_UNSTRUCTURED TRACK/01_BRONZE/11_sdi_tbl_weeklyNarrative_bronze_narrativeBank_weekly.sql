/* =============================================================================
BRONZE: One immutable raw narrative version per source record.

- Historical imported report: one source record for each original JSONL line.
- App: one source record for EACH saved, immutable version, irrespective of
  later review decisions. PUBLISH/REJECT/ACCEPT are EVENTS, not edits of Bronze.
- No other Bronze raw-narrative table is necessary.
- IDs are deterministic; the writer must serialize and de-duplicate because
  Databricks informational PK declarations do NOT enforce uniqueness.
- source_status_at_ingest is only a capture-time classification, not live status.
============================================================================= */
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
