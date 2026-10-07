-- ============================================================================
-- MIP BRONZE SANITY CHECK
-- TEST DATE: 2026-09-28
--
-- FLOW:
--   1. Preflight only
--   2. Actual load
--   3. Check loaded data
--   4. Reconcile Source vs Bronze
--   5. Check duplicate grain
--
-- NOTE:
--   Run this section-by-section rather than executing the entire block blindly,
--   especially because the source tables are very large.
-- ============================================================================



-- ############################################################################
-- BRONZE 01: UDI HITS
-- prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHits_daily
-- ############################################################################


-- ============================================================================
-- 01A. PREFLIGHT
-- Creates/writes nothing.
-- ============================================================================

CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlHits_daily(
    p_asOfDate        => DATE '2026-09-28',
    p_eventWindowDays => 1,
    p_validateOnly    => TRUE
);


-- ============================================================================
-- 01B. LOAD
-- Loads/replaces only 2026-09-28.
-- ============================================================================

CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlHits_daily(
    p_asOfDate        => DATE '2026-09-28',
    p_eventWindowDays => 1,
    p_validateOnly    => FALSE
);


-- ============================================================================
-- 01C. CHECK LOADED DATA
-- ============================================================================

SELECT
    event_date,
    COUNT(*) AS rowCount,
    COUNT(DISTINCT row_identity_hash) AS distinctHits,
    MIN(_ingestedAt) AS minIngestedAt,
    MAX(_ingestedAt) AS maxIngestedAt
FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHits_daily
WHERE event_date = DATE '2026-09-28'
GROUP BY event_date;


-- ============================================================================
-- 01D. SOURCE VS BRONZE
-- Expected: SOURCE and BRONZE counts match.
-- ============================================================================

SELECT
    'SOURCE' AS dataset,
    COUNT(*) AS rowCount,
    COUNT(DISTINCT row_identity_hash) AS distinctHits
FROM prd_dbi_analytics.silver_digital_interactions.unified_digital_interactions
WHERE event_date = DATE '2026-09-28'

UNION ALL

SELECT
    'BRONZE' AS dataset,
    COUNT(*) AS rowCount,
    COUNT(DISTINCT row_identity_hash) AS distinctHits
FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHits_daily
WHERE event_date = DATE '2026-09-28';


-- ============================================================================
-- 01E. DUPLICATE HIT CHECK
-- Expected: no rows.
-- ============================================================================

SELECT
    row_identity_hash,
    COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHits_daily
WHERE event_date = DATE '2026-09-28'
GROUP BY row_identity_hash
HAVING COUNT(*) > 1
ORDER BY rowCount DESC
LIMIT 100;



-- ############################################################################
-- BRONZE 02: HIT -> SESSION LINKS
-- prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHitSessionLinks_daily
-- ############################################################################


-- ============================================================================
-- 02A. PREFLIGHT
-- ============================================================================

CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlHitSessionLinks_daily(
    p_asOfDate        => DATE '2026-09-28',
    p_eventWindowDays => 1,
    p_validateOnly    => TRUE
);


-- ============================================================================
-- 02B. LOAD
-- ============================================================================

CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlHitSessionLinks_daily(
    p_asOfDate        => DATE '2026-09-28',
    p_eventWindowDays => 1,
    p_validateOnly    => FALSE
);


-- ============================================================================
-- 02C. CHECK LOADED DATA
-- ============================================================================

SELECT
    event_date,
    COUNT(*) AS rowCount,
    COUNT(DISTINCT row_identity_hash) AS distinctHits,
    COUNT(DISTINCT session_id) AS distinctSessions,
    MIN(_ingestedAt) AS minIngestedAt,
    MAX(_ingestedAt) AS maxIngestedAt
FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHitSessionLinks_daily
WHERE event_date = DATE '2026-09-28'
GROUP BY event_date;


-- ============================================================================
-- 02D. SOURCE VS BRONZE
-- Expected: SOURCE and BRONZE counts match.
-- ============================================================================

SELECT
    'SOURCE' AS dataset,
    COUNT(*) AS rowCount,
    COUNT(DISTINCT row_identity_hash) AS distinctHits
FROM prd_dbi_analytics.silver_digital_interactions.session_event_fact
WHERE event_date = DATE '2026-09-28'

UNION ALL

SELECT
    'BRONZE' AS dataset,
    COUNT(*) AS rowCount,
    COUNT(DISTINCT row_identity_hash) AS distinctHits
FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHitSessionLinks_daily
WHERE event_date = DATE '2026-09-28';


-- ============================================================================
-- 02E. DUPLICATE HIT-SESSION KEY CHECK
-- Expected: ideally no rows.
-- ============================================================================

SELECT
    row_identity_hash,
    COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHitSessionLinks_daily
WHERE event_date = DATE '2026-09-28'
GROUP BY row_identity_hash
HAVING COUNT(*) > 1
ORDER BY rowCount DESC
LIMIT 100;


-- ============================================================================
-- 02F. NULL SESSION ASSIGNMENT CHECK
-- ============================================================================

SELECT
    COUNT(*) AS totalRows,

    SUM(
        CASE
            WHEN session_id IS NULL THEN 1
            ELSE 0
        END
    ) AS nullSessionRows,

    ROUND(
        100.0 *
        SUM(
            CASE
                WHEN session_id IS NULL THEN 1
                ELSE 0
            END
        )
        / COUNT(*),
        4
    ) AS nullSessionPct

FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHitSessionLinks_daily
WHERE event_date = DATE '2026-09-28';


-- ============================================================================
-- 02G. ASSIGNMENT / IDENTITY DISTRIBUTION
-- ============================================================================

SELECT
    identity_status,
    session_assignment_method,
    COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHitSessionLinks_daily
WHERE event_date = DATE '2026-09-28'
GROUP BY
    identity_status,
    session_assignment_method
ORDER BY rowCount DESC;



-- ############################################################################
-- BRONZE 03: SESSION SUMMARY
-- prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessions_daily
--
-- IMPORTANT:
-- Requested reporting date = 2026-09-28
-- Widened session source/load window = 2026-09-27 through 2026-09-29
-- ############################################################################


-- ============================================================================
-- 03A. PREFLIGHT
-- ============================================================================

CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessions_daily(
    p_asOfDate        => DATE '2026-09-28',
    p_eventWindowDays => 1,
    p_validateOnly    => TRUE
);


-- ============================================================================
-- 03B. LOAD
-- ============================================================================

CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessions_daily(
    p_asOfDate        => DATE '2026-09-28',
    p_eventWindowDays => 1,
    p_validateOnly    => FALSE
);


-- ============================================================================
-- 03C. CHECK LOADED DATA
-- Expected dates: 2026-09-27, 2026-09-28, 2026-09-29
-- ============================================================================

SELECT
    session_start_date,
    COUNT(*) AS rowCount,
    COUNT(DISTINCT session_id) AS distinctSessions,
    MIN(_ingestedAt) AS minIngestedAt,
    MAX(_ingestedAt) AS maxIngestedAt
FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessions_daily
WHERE session_start_date
    BETWEEN DATE '2026-09-27' AND DATE '2026-09-29'
GROUP BY session_start_date
ORDER BY session_start_date;


-- ============================================================================
-- 03D. SOURCE VS BRONZE
-- Expected: SOURCE and BRONZE counts match for widened window.
-- ============================================================================

SELECT
    'SOURCE' AS dataset,
    COUNT(*) AS rowCount,
    COUNT(DISTINCT session_id) AS distinctSessions
FROM prd_dbi_analytics.silver_digital_interactions.session_summary_fact
WHERE session_start_date
    BETWEEN DATE '2026-09-27' AND DATE '2026-09-29'

UNION ALL

SELECT
    'BRONZE' AS dataset,
    COUNT(*) AS rowCount,
    COUNT(DISTINCT session_id) AS distinctSessions
FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessions_daily
WHERE session_start_date
    BETWEEN DATE '2026-09-27' AND DATE '2026-09-29';


-- ============================================================================
-- 03E. DUPLICATE SESSION CHECK
-- Expected: ideally no rows if session_id is unique in source.
-- ============================================================================

SELECT
    session_id,
    COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessions_daily
WHERE session_start_date
    BETWEEN DATE '2026-09-27' AND DATE '2026-09-29'
GROUP BY session_id
HAVING COUNT(*) > 1
ORDER BY rowCount DESC
LIMIT 100;


-- ============================================================================
-- 03F. SESSION STATUS DISTRIBUTION
-- Bronze intentionally keeps all statuses.
-- ============================================================================

SELECT
    session_status,
    COUNT(*) AS rowCount,
    COUNT(DISTINCT session_id) AS distinctSessions
FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessions_daily
WHERE session_start_date
    BETWEEN DATE '2026-09-27' AND DATE '2026-09-29'
GROUP BY session_status
ORDER BY rowCount DESC;



-- ############################################################################
-- BRONZE 04: MARKETING CODE SNAPSHOT
-- prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlMarketingCodes_snapshot
-- ############################################################################


-- ============================================================================
-- 04A. PREFLIGHT
-- Checks that dim_marketing_code is not empty.
-- ============================================================================

CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlMarketingCodes_snapshot(
    p_validateOnly => TRUE
);


-- ============================================================================
-- 04B. LOAD
-- Full overwrite because this is a small reference table.
-- ============================================================================

CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlMarketingCodes_snapshot(
    p_validateOnly => FALSE
);


-- ============================================================================
-- 04C. CHECK LOADED DATA
-- ============================================================================

SELECT
    COUNT(*) AS rowCount,
    COUNT(DISTINCT MKT_CODE) AS distinctMarketingCodes,
    MIN(_ingestedAt) AS minIngestedAt,
    MAX(_ingestedAt) AS maxIngestedAt
FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlMarketingCodes_snapshot;


-- ============================================================================
-- 04D. SOURCE VS BRONZE
-- Expected: SOURCE and BRONZE row counts match.
-- ============================================================================

SELECT
    'SOURCE' AS dataset,
    COUNT(*) AS rowCount,
    COUNT(DISTINCT MKT_CODE) AS distinctMarketingCodes
FROM prdrzranalytics.lab42.dim_marketing_code

UNION ALL

SELECT
    'BRONZE' AS dataset,
    COUNT(*) AS rowCount,
    COUNT(DISTINCT MKT_CODE) AS distinctMarketingCodes
FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlMarketingCodes_snapshot;


-- ============================================================================
-- 04E. DUPLICATE MARKETING CODE CHECK
-- Review if rows appear; source may legitimately contain multiple rows/code
-- depending on the dimension design.
-- ============================================================================

SELECT
    MKT_CODE,
    COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlMarketingCodes_snapshot
GROUP BY MKT_CODE
HAVING COUNT(*) > 1
ORDER BY rowCount DESC
LIMIT 100;



-- ############################################################################
-- CROSS-BRONZE SANITY CHECKS
-- Run these only AFTER Bronze 01 and Bronze 02 are successfully loaded.
-- ############################################################################


-- ============================================================================
-- 05A. UDI HIT COUNT VS SESSION-LINK HIT COUNT
--
-- This does NOT require them to be equal.
-- It tells us how many records exist in each Bronze source.
-- ============================================================================

SELECT
    'UDI_HITS' AS dataset,
    COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHits_daily
WHERE event_date = DATE '2026-09-28'

UNION ALL

SELECT
    'SESSION_EVENT_FACT' AS dataset,
    COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHitSessionLinks_daily
WHERE event_date = DATE '2026-09-28';


-- ============================================================================
-- 05B. UDI -> SESSION LINK COVERAGE
--
-- NOTE:
-- This joins very large datasets.
-- Run once for initial validation; do not make this an ad-hoc routine query.
-- ============================================================================

SELECT
    COUNT(*) AS totalUdiHits,

    COUNT(s.row_identity_hash) AS matchedSessionizedHits,

    COUNT(*) - COUNT(s.row_identity_hash) AS unmatchedHits,

    ROUND(
        100.0 * COUNT(s.row_identity_hash) / COUNT(*),
        4
    ) AS sessionizationCoveragePct

FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHits_daily h

LEFT JOIN prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHitSessionLinks_daily s
    ON  h.row_identity_hash = s.row_identity_hash
    AND h.event_date        = s.event_date
    AND h.source_table      = s.source_table

WHERE h.event_date = DATE '2026-09-28';





























-- ============================================================================
-- FILE  : 99_mip_bronze_silver_rebuild_runbook.sql
-- PURPOSE:
--   Safe deployment/rebuild order after intentionally deleting MIP Bronze/Silver
--   tables. Procedures should be deployed first from files 01-05 in this package.
--
-- IMPORTANT:
--   - DROP statements below are COMMENTED OUT intentionally.
--   - UDI and SEF are independent; SSF depends on SEF.
--   - Marketing snapshot is independent but required before Silver 01.
--   - Silver 01 -> Silver 02 -> Silver 03 -> Silver 04/05.
--   - Peer-set and impact-on-topline are NOT added to these base tables.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 0. OPTIONAL CLEAN REBUILD - uncomment only when you intentionally want to
--    destroy the persisted base-layer history.
-- ----------------------------------------------------------------------------
-- DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly;
-- DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly;
-- DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily;
-- DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily;
-- DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily;
-- DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlMarketingCodeDim_snapshot;
-- DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily;
-- DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily;
-- DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily;

-- ----------------------------------------------------------------------------
-- 1. BRONZE EXAMPLE: 10 event days ending 2026-09-29.
--    Always run preflight before the write call.
-- ----------------------------------------------------------------------------
CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlUdiHits_daily(
    p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>TRUE);
CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlUdiHits_daily(
    p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>FALSE);

CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessionEventFact_daily(
    p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>TRUE);
CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessionEventFact_daily(
    p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>FALSE);

-- SSF preflight/load MUST follow the completed SEF load.
CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessionSummaryFact_daily(
    p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>TRUE);
CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlSessionSummaryFact_daily(
    p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>FALSE);

CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlMarketingCodeDim_snapshot(
    p_validateOnly=>TRUE);
CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlMarketingCodeDim_snapshot(
    p_validateOnly=>FALSE);

-- Run 10_mip_bronze_sanity_checks.sql before Silver.

-- ----------------------------------------------------------------------------
-- 2. SILVER DAILY/SESSION EXAMPLE for the same 10-day rebuild.
-- ----------------------------------------------------------------------------
CALL prdrzranalytics.lab42.sdi_sp_mip_silver_detailsPerHit_daily(
    p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>TRUE);
CALL prdrzranalytics.lab42.sdi_sp_mip_silver_detailsPerHit_daily(
    p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>FALSE);

CALL prdrzranalytics.lab42.sdi_sp_mip_silver_attributesPerSession_daily(
    p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>TRUE);
CALL prdrzranalytics.lab42.sdi_sp_mip_silver_attributesPerSession_daily(
    p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>FALSE);

CALL prdrzranalytics.lab42.sdi_sp_mip_silver_actionsPerSessionPageCategory_daily(
    p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>TRUE);
CALL prdrzranalytics.lab42.sdi_sp_mip_silver_actionsPerSessionPageCategory_daily(
    p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>FALSE);

-- ----------------------------------------------------------------------------
-- 3. WEEKLY SILVER.
--    With only Sep20-Sep29 daily data, week Sep20-Sep26 is complete; week
--    Sep27-Oct03 is partial. Build complete production weeks only.
-- ----------------------------------------------------------------------------
CALL prdrzranalytics.lab42.sdi_sp_mip_silver_attributesPerVisitorWeek_weekly(
    p_asOfDate=>DATE '2026-09-26',p_weeksToRebuild=>1,p_validateOnly=>TRUE);
CALL prdrzranalytics.lab42.sdi_sp_mip_silver_attributesPerVisitorWeek_weekly(
    p_asOfDate=>DATE '2026-09-26',p_weeksToRebuild=>1,p_validateOnly=>FALSE);
CALL prdrzranalytics.lab42.sdi_sp_mip_silver_actionsPerVisitorWeek_weekly(
    p_asOfDate=>DATE '2026-09-26',p_weeksToRebuild=>1,p_validateOnly=>TRUE);
CALL prdrzranalytics.lab42.sdi_sp_mip_silver_actionsPerVisitorWeek_weekly(
    p_asOfDate=>DATE '2026-09-26',p_weeksToRebuild=>1,p_validateOnly=>FALSE);

-- Run 20_mip_silver_sanity_checks.sql after the loads.

-- ----------------------------------------------------------------------------
-- 4. BACKFILL-HORIZON WARNING FOR GOLD / FUTURE PEER SET.
-- ----------------------------------------------------------------------------
-- A 10-day rebuild is enough only for a short development test. If Bronze/Silver
-- are deleted and you intend to rebuild production Gold comparisons from scratch:
--
--   * target + prior week requires the prior week history;
--   * target + four-week trend requires target history plus four prior weeks;
--   * future peer set also ALWAYS uses that four-week trend, so it needs the same
--     multi-week base history;
--   * same-week-last-year requires the corresponding prior-year base history.
--
-- Therefore backfill the full daily/session history required by the earliest Gold
-- target week and all of its comparison periods BEFORE rebuilding analytical Gold.
-- Do not delete historical Bronze/Silver and then expect a 10-day reload to
-- reproduce multi-week / LY Gold outputs.
