/* PHASE 2 | ENTITY DICTIONARY. One canonical node can appear in hundreds of weekly reports.
 AI may PROPOSE an entity. Controlled resolution/review confirms it before graph retrieval. */
CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_knowledgeNodes_long (
  node_id STRING NOT NULL COMMENT 'Canonical stable entity key, e.g. campaign:freedomExplorer.',
  node_type STRING NOT NULL COMMENT 'METRIC, CAMPAIGN, CHANNEL, EVENT, LOB, or other governed entity category.',
  canonical_name STRING NOT NULL COMMENT 'Readable preferred display name of the entity.',
  metric_key STRING COMMENT 'Optional registered source metric key for METRIC nodes.',
  aliases ARRAY<STRING> COMMENT 'Alternative labels associated with the same canonical entity.',
  first_seen_date DATE COMMENT 'Earliest date linked to the entity in imported/published evidence.',
  last_seen_date DATE COMMENT 'Most recent date linked to the entity in imported/published evidence.',
  verification_status STRING NOT NULL COMMENT 'PENDING_REVIEW, APPROVED or REJECTED; only approved nodes enter trusted retrieval.',
  attributes_json STRING COMMENT 'Additional metadata that does not determine canonical identity.',
  created_at TIMESTAMP NOT NULL COMMENT 'First insertion timestamp.',
  updated_at TIMESTAMP NOT NULL COMMENT 'Last approved metadata refresh timestamp.'
) USING DELTA COMMENT 'Reusable canonical business entities; AI proposals separated from approval.';
