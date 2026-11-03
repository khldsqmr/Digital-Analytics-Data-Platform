/* ============================================================================
 PROJECT      : Weekly Narrative | Databricks Unity Catalog | v7
 FILE         : 03_sdi_tbl_weeklyNarrative_gold_reviewEvents_long.sql
 LAYER        : agent/gold
 PURPOSE      : See object-level comments and README for schema/processing contract
 DEPENDENCIES : Deploy in documented layer order
 EXECUTION    : Use Databricks SQL / UC runtime
 OWNERSHIP    : Production writes only by governed job/service principal.
 GRAIN        : As defined by target object. All reporting weeks Sun-Sat.
 SAFETY       : Review/approval not inferred from LLM output. Retry-safe writer.
 ============================================================================ */
/* ==================================================================================================
 FILE   : 32_sdi_tbl_weeklyNarrative_gold_reviewEvents_long.sql
 TRACK  : AGENT / GOLD / PHASE 3
 GRAIN  : ONE IMMUTABLE HUMAN ACTION on ONE exact version + section (or entire report).
 PURPOSE: Keep every person's decision, including simultaneous and conflicting decisions.
 IMPORTANT:
   - Reviewer identity is supplied by a verified API session. Browser-provided identity is ignored.
   - No reviewer role, email, or department columns. Authorization is enforced by the API.
   - Never update or delete an event. A changed opinion is another event from the same person.
   - Primary/unique keys in Databricks are informational. Use ONE serialized writer and
     client_request_id to prevent duplicates. Set the review worker Job max_concurrent_runs=1.
   - review_comment is mandatory for REJECT and REQUEST_CHANGES.
   - A conflict does not erase either reviewer's feedback; reportStatus derives REVIEW_CONFLICT.
   - APPROVED is not PUBLISHED. Only an explicit PUBLISH event changes the official report.
   - A newer edit creates a version, never updates or deletes any review event.
================================================================================================== */
CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_reviewEvents_long (
  review_event_id      STRING NOT NULL COMMENT 'Stable REV_<UUIDv5> of client_request_id. One row for each successful action.',
  client_request_id    STRING NOT NULL COMMENT 'Client-generated idempotency UUID for retries; unique within the controlled writer.',
  report_id            STRING NOT NULL COMMENT 'Logical report: WN_yyyyMMdd_SCOPE. All independent versions share report_id.',
  version_id           STRING NOT NULL COMMENT 'Immutable draft version reviewed: VER_<UUID>. Never infer from the latest report.',
  week_end             DATE NOT NULL COMMENT 'Saturday ending the Sunday-Saturday reporting week.',
  section_key          STRING NOT NULL COMMENT 'WHAT_HAPPENED, WHY_IT_HAPPENED, IS_IT_MEANINGFUL, RECOMMENDATIONS_AND_RISKS, or REPORT for PUBLISH.',
  review_action        STRING NOT NULL COMMENT 'ACCEPT, REJECT, REQUEST_CHANGES, PUBLISH. Human text changes create a new version instead.',
  related_metric_keys  ARRAY<STRING> COMMENT 'Optional verified registry keys relevant to the comment. Not a free-text LOB.',
  feedback_category    STRING COMMENT 'Optional category such as UNSUPPORTED_ATTRIBUTION or INCORRECT_NUMBER.',
  review_comment       STRING COMMENT 'Human feedback; mandatory for REJECT / REQUEST_CHANGES.',
  suggested_text       STRING COMMENT 'Optional replacement text offered by reviewer. Never silently publish this.',
  reviewer_id          STRING NOT NULL COMMENT 'Stable authenticated user ID (OIDC sub), not editable in browser.',
  reviewer_name        STRING NOT NULL COMMENT 'Display name captured at action time, for human-readable audit.',
  reviewed_at          TIMESTAMP NOT NULL COMMENT 'UTC write timestamp for this event.',
  seen_own_event_id    STRING COMMENT 'Previous event by THIS reviewer on this version+section which the UI had seen; allows safe corrections.'
) USING DELTA
COMMENT 'Weekly Narrative human feedback, all users and versions, append-only. REVIEW_CONFLICT is derived, not written here.';
