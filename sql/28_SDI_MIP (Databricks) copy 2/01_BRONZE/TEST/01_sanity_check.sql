-- ============================================================================
-- FILE  : 10_mip_bronze_sanity_checks.sql
-- PURPOSE:
--   Development/backfill execution and sanity checks for the current MIP Bronze.
--
-- DEFAULT EXAMPLE WINDOW:
--   2026-09-20 through 2026-09-29 (p_asOfDate=2026-09-29, 10 days).
--
-- EXECUTION ORDER:
--   1. UDI Bronze       (independent)
--   2. SEF Bronze       (independent of UDI, but required before SSF)
--   3. SSF Bronze       (depends on SEF)
--   4. Marketing snapshot (independent; must exist before Silver)
--
-- HOW TO USE:
--   Run section-by-section. The source tables are very large; do not highlight
--   and execute this entire file blindly.
--
-- FUTURE VALIDATION LAYER:
--   Checks explicitly marked CRITICAL are natural candidates for automated
--   validation gates. INFORMATIONAL checks are useful diagnostics but should not
--   automatically fail the future pipeline without an approved threshold.
-- ============================================================================

-- ############################################################################
-- A. PREFLIGHT ONLY - WRITES NOTHING
-- ############################################################################
-- CRITICAL: each call must succeed before the corresponding load.
CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlUdiHits_daily(
    p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>TRUE);
CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessionEventFact_daily(
    p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>TRUE);
-- SSF preflight requires SEF Bronze to have already been LOADED for the window.
-- Therefore run this after section B loads SEF when rebuilding from empty tables.
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessionSummaryFact_daily(
--     p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>TRUE);
CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlMarketingCodeDim_snapshot(
    p_validateOnly=>TRUE);

-- ############################################################################
-- B. LOAD - CORRECT DEPENDENCY ORDER
-- ############################################################################
CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlUdiHits_daily(
    p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>FALSE);
CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessionEventFact_daily(
    p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>FALSE);
-- Now the SSF preflight can verify the exact SEF session population.
CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessionSummaryFact_daily(
    p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>TRUE);
CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessionSummaryFact_daily(
    p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>FALSE);
CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlMarketingCodeDim_snapshot(
    p_validateOnly=>FALSE);

-- ############################################################################
-- C. UDI SOURCE VS BRONZE BY DAY / SOURCE
-- ############################################################################
-- CRITICAL: sourceRows = bronzeRows for every event_date + source_table.
WITH src AS (
    SELECT event_date,source_table,COUNT(*) AS sourceRows
    FROM prd_dbi_analytics.silver_digital_interactions.unified_digital_interactions
    WHERE event_date BETWEEN DATE '2026-09-20' AND DATE '2026-09-29'
    GROUP BY event_date,source_table
), br AS (
    SELECT event_date,source_table,COUNT(*) AS bronzeRows
    FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
    WHERE event_date BETWEEN DATE '2026-09-20' AND DATE '2026-09-29'
    GROUP BY event_date,source_table
)
SELECT
    coalesce(s.event_date,b.event_date) AS eventDate,
    coalesce(s.source_table,b.source_table) AS sourceTable,
    coalesce(s.sourceRows,0) AS sourceRows,
    coalesce(b.bronzeRows,0) AS bronzeRows,
    coalesce(b.bronzeRows,0)-coalesce(s.sourceRows,0) AS rowDiff
FROM src s FULL OUTER JOIN br b
  ON s.event_date=b.event_date AND s.source_table=b.source_table
ORDER BY eventDate,sourceTable;

-- CRITICAL: expected zero duplicate composite UDI keys.
SELECT row_identity_hash,event_date,source_table,COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
WHERE event_date BETWEEN DATE '2026-09-20' AND DATE '2026-09-29'
GROUP BY row_identity_hash,event_date,source_table
HAVING COUNT(*)>1
ORDER BY rowCount DESC
LIMIT 100;

-- INFORMATIONAL: raw channel / geo / action source coverage.
SELECT
    event_date,source_table,
    COUNT(*) AS rows,
    COUNT_IF(channel_name IS NOT NULL) AS rowsWithMarketingChannel,
    COUNT_IF(channel IS NOT NULL) AS rowsWithNavigationChannel,
    COUNT_IF(attribute_channel IS NOT NULL) AS rowsWithAppChannel,
    COUNT_IF(geo_postal_code IS NOT NULL) AS rowsWithGeoPostalCode,
    COUNT_IF(attribute_country IS NOT NULL) AS rowsWithAttributeCountry,
    COUNT_IF(external_campaign_code IS NOT NULL) AS rowsWithExternalCampaign,
    SUM(coalesce(event_page_view,0)) AS pageViews,
    SUM(coalesce(event_purchase,0)) AS purchases
FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
WHERE event_date BETWEEN DATE '2026-09-20' AND DATE '2026-09-29'
GROUP BY event_date,source_table
ORDER BY event_date,source_table;

-- INFORMATIONAL: confirm exact marketing-channel taxonomy is retained.
SELECT source_table,channel_name,COUNT(*) AS rows
FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
WHERE event_date=DATE '2026-09-24' AND channel_name IS NOT NULL
GROUP BY source_table,channel_name
ORDER BY source_table,rows DESC
LIMIT 200;

-- ############################################################################
-- D. SEF SOURCE VS BRONZE BY DAY / SOURCE
-- ############################################################################
-- CRITICAL: sourceRows = bronzeRows and sourceSessions = bronzeSessions.
WITH src AS (
    SELECT event_date,source_table,COUNT(*) AS sourceRows,
           COUNT(DISTINCT session_id) AS sourceSessions
    FROM prd_dbi_analytics.silver_digital_interactions.session_event_fact
    WHERE event_date BETWEEN DATE '2026-09-20' AND DATE '2026-09-29'
    GROUP BY event_date,source_table
), br AS (
    SELECT event_date,source_table,COUNT(*) AS bronzeRows,
           COUNT(DISTINCT session_id) AS bronzeSessions
    FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily
    WHERE event_date BETWEEN DATE '2026-09-20' AND DATE '2026-09-29'
    GROUP BY event_date,source_table
)
SELECT
    coalesce(s.event_date,b.event_date) AS eventDate,
    coalesce(s.source_table,b.source_table) AS sourceTable,
    coalesce(s.sourceRows,0) AS sourceRows,
    coalesce(b.bronzeRows,0) AS bronzeRows,
    coalesce(b.bronzeRows,0)-coalesce(s.sourceRows,0) AS rowDiff,
    coalesce(s.sourceSessions,0) AS sourceSessions,
    coalesce(b.bronzeSessions,0) AS bronzeSessions,
    coalesce(b.bronzeSessions,0)-coalesce(s.sourceSessions,0) AS sessionDiff
FROM src s FULL OUTER JOIN br b
  ON s.event_date=b.event_date AND s.source_table=b.source_table
ORDER BY eventDate,sourceTable;

-- CRITICAL: expected zero duplicate composite SEF hit keys.
SELECT row_identity_hash,event_date,source_table,COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily
WHERE event_date BETWEEN DATE '2026-09-20' AND DATE '2026-09-29'
GROUP BY row_identity_hash,event_date,source_table
HAVING COUNT(*)>1
ORDER BY rowCount DESC
LIMIT 100;

-- INFORMATIONAL: session assignment/null coverage by day/source.
SELECT event_date,source_table,COUNT(*) AS rows,
       COUNT(DISTINCT session_id) AS sessions,
       COUNT_IF(session_id IS NULL) AS nullSessionRows
FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily
WHERE event_date BETWEEN DATE '2026-09-20' AND DATE '2026-09-29'
GROUP BY event_date,source_table
ORDER BY event_date,source_table;

-- ############################################################################
-- E. SSF COVERAGE / GRAIN
-- ############################################################################
-- CRITICAL: every non-null SEF session in the requested event window is present.
WITH expected AS (
    SELECT DISTINCT session_id
    FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily
    WHERE event_date BETWEEN DATE '2026-09-20' AND DATE '2026-09-29'
      AND session_id IS NOT NULL
), actual AS (
    SELECT session_id
    FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily
)
SELECT
    (SELECT COUNT(*) FROM expected) AS expectedSefSessions,
    COUNT(a.session_id) AS matchedBronzeSsfSessions,
    (SELECT COUNT(*) FROM expected)-COUNT(a.session_id) AS missingSsfSessions
FROM expected e LEFT JOIN actual a ON a.session_id=e.session_id;

-- CRITICAL: expected zero duplicate session_id rows in Bronze SSF.
SELECT session_id,COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily
GROUP BY session_id
HAVING COUNT(*)>1
ORDER BY rowCount DESC
LIMIT 100;

-- INFORMATIONAL: status and entry-URL population for sessions touched by window.
WITH wanted AS (
    SELECT DISTINCT session_id
    FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily
    WHERE event_date BETWEEN DATE '2026-09-20' AND DATE '2026-09-29'
      AND session_id IS NOT NULL
)
SELECT s.source_table,s.session_status,COUNT(*) AS sessions,
       COUNT_IF(s.canonical_user_id IS NOT NULL) AS sessionsWithCanonicalUser,
       COUNT_IF(s.entry_page_url_full IS NOT NULL AND trim(s.entry_page_url_full)<>'') AS sessionsWithEntryUrl
FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily s
INNER JOIN wanted w ON w.session_id=s.session_id
GROUP BY s.source_table,s.session_status
ORDER BY s.source_table,s.session_status;

-- ############################################################################
-- F. MARKETING SNAPSHOT
-- ############################################################################
-- CRITICAL: source and snapshot row counts should match after INSERT OVERWRITE.
SELECT 'SOURCE' AS dataset,COUNT(*) AS rows,COUNT(DISTINCT MKT_CODE) AS distinctCodes
FROM prdrzranalytics.lab42.dim_marketing_code
UNION ALL
SELECT 'BRONZE' AS dataset,COUNT(*) AS rows,COUNT(DISTINCT MKT_CODE) AS distinctCodes
FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlMarketingCodeDim_snapshot;

-- INFORMATIONAL: duplicates are allowed to exist in the raw reference snapshot;
-- Silver detailsPerHit resolves a single enrichment row per MKT_CODE with max_by.
SELECT MKT_CODE,COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlMarketingCodeDim_snapshot
GROUP BY MKT_CODE
HAVING COUNT(*)>1
ORDER BY rowCount DESC
LIMIT 100;

-- ############################################################################
-- G. CROSS-BRONZE JOINABILITY
-- ############################################################################
-- INFORMATIONAL / future threshold candidate. This measures SEF bridge coverage.
WITH u AS (
    SELECT row_identity_hash,event_date,source_table
    FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
    WHERE event_date BETWEEN DATE '2026-09-20' AND DATE '2026-09-29'
), e AS (
    SELECT row_identity_hash,event_date,source_table,session_id
    FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily
    WHERE event_date BETWEEN DATE '2026-09-20' AND DATE '2026-09-29'
)
SELECT
    COUNT(*) AS udiRows,
    COUNT(e.row_identity_hash) AS matchedToSefRows,
    COUNT(*)-COUNT(e.row_identity_hash) AS unmatchedUdiRows,
    100D*try_divide(COUNT(e.row_identity_hash),COUNT(*)) AS matchedPct
FROM u LEFT JOIN e
  ON u.row_identity_hash=e.row_identity_hash
 AND u.event_date=e.event_date
 AND u.source_table=e.source_table;



-- ============================================================================
-- FILE  : 00_mip_drop_existing_bronze_silver_tables.sql
-- PURPOSE: One-time reset before a clean MIP Bronze/Silver rebuild.
-- WARNING: This deletes the current persisted Bronze/Silver data tables. Procedures are not dropped.
-- ORDER: Drop downstream Silver first, then Bronze dependencies.
-- ============================================================================
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlMarketingCodeDim_snapshot;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily;
