-- ============================================================================
-- MIP SILVER - DEVELOPMENT EXECUTION + SANITY CHECKS
-- TEST DATE: 2026-09-28
--
-- EXECUTION ORDER:
--
--   SILVER 01 - detailsPerHit_daily
--          ↓
--   SILVER 02 - attributesPerSession_daily
--          ↓
--   SILVER 03 - actionsPerSessionPageCategory_daily
--          ↓
--   SILVER 04 - attributesPerVisitorWeek_weekly
--          ↓
--   SILVER 05 - actionsPerVisitorWeek_weekly
--
-- IMPORTANT:
-- We currently loaded only 2026-09-28 in Bronze.
-- Therefore the weekly tables for week starting 2026-09-27 are PARTIAL-WEEK
-- development results, which is fine for logic/grain testing.
-- ============================================================================



-- ############################################################################
-- SILVER 01
-- DETAILS PER HIT
-- ############################################################################


-- ============================================================================
-- 01A. PREFLIGHT
-- No target creation/write.
-- ============================================================================

CALL prdrzranalytics.lab42.sdi_sp_mip_silver_detailsPerHit_daily(
    p_asOfDate        => DATE '2026-09-28',
    p_eventWindowDays => 1,
    p_validateOnly    => TRUE
);


-- ============================================================================
-- 01B. LOAD
-- ============================================================================

CALL prdrzranalytics.lab42.sdi_sp_mip_silver_detailsPerHit_daily(
    p_asOfDate        => DATE '2026-09-28',
    p_eventWindowDays => 1,
    p_validateOnly    => FALSE
);


-- ============================================================================
-- 01C. VALIDATION 1
-- Bronze hit count vs Silver details count.
--
-- EXPECTED:
-- BRONZE = SILVER
-- because Silver 01 preserves one row per Bronze hit.
-- ============================================================================

SELECT
    'BRONZE_HITS' AS dataset,
    COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHits_daily
WHERE event_date = DATE '2026-09-28'

UNION ALL

SELECT
    'SILVER_DETAILS' AS dataset,
    COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily
WHERE eventDate = DATE '2026-09-28';


-- ============================================================================
-- 01D. VALIDATION 2
-- Grain check: one Silver row per rowIdentityHash.
--
-- EXPECTED:
-- No rows.
-- ============================================================================

SELECT
    rowIdentityHash,
    COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily
WHERE eventDate = DATE '2026-09-28'
GROUP BY rowIdentityHash
HAVING COUNT(*) > 1
ORDER BY rowCount DESC
LIMIT 100;


-- ============================================================================
-- 01E. VALIDATION 3
-- Sessionization / identity sanity.
-- This is informational rather than pass/fail for now.
-- ============================================================================

SELECT
    COUNT(*) AS totalHits,

    SUM(isSessionized) AS sessionizedHits,

    COUNT(*) - SUM(isSessionized) AS nonSessionizedHits,

    ROUND(
        100.0 * SUM(isSessionized) / COUNT(*),
        4
    ) AS sessionizedPct,

    SUM(
        CASE
            WHEN visitorId IS NOT NULL THEN 1
            ELSE 0
        END
    ) AS hitsWithVisitorId,

    ROUND(
        100.0 *
        SUM(
            CASE
                WHEN visitorId IS NOT NULL THEN 1
                ELSE 0
            END
        ) / COUNT(*),
        4
    ) AS visitorIdCoveragePct

FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily
WHERE eventDate = DATE '2026-09-28';



-- ############################################################################
-- SILVER 02
-- ATTRIBUTES PER SESSION
-- ############################################################################


-- ============================================================================
-- 02A. PREFLIGHT
-- ============================================================================

CALL prdrzranalytics.lab42.sdi_sp_mip_silver_attributesPerSession_daily(
    p_asOfDate        => DATE '2026-09-28',
    p_eventWindowDays => 1,
    p_validateOnly    => TRUE
);


-- ============================================================================
-- 02B. LOAD
-- ============================================================================

CALL prdrzranalytics.lab42.sdi_sp_mip_silver_attributesPerSession_daily(
    p_asOfDate        => DATE '2026-09-28',
    p_eventWindowDays => 1,
    p_validateOnly    => FALSE
);


-- ============================================================================
-- 02C. VALIDATION 1
-- Basic session population.
--
-- EXPECTED:
-- rowCount = distinctSessions
-- ============================================================================

SELECT
    sessionStartDatePst,

    COUNT(*) AS rowCount,

    COUNT(DISTINCT sessionId) AS distinctSessions,

    COUNT(DISTINCT visitorId) AS distinctVisitors,

    SUM(pageViews) AS pageViews,

    SUM(orderCount) AS orderCount

FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily

WHERE sessionStartDatePst = DATE '2026-09-28'

GROUP BY sessionStartDatePst;


-- ============================================================================
-- 02D. VALIDATION 2
-- Duplicate session check.
--
-- EXPECTED:
-- No rows.
-- ============================================================================

SELECT
    sessionId,
    COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily
WHERE sessionStartDatePst = DATE '2026-09-28'
GROUP BY sessionId
HAVING COUNT(*) > 1
ORDER BY rowCount DESC
LIMIT 100;


-- ============================================================================
-- 02E. VALIDATION 3
-- Session-rule validation.
--
-- This table should contain only:
--   pageViews > 1
--   isNonBounced = 1
--   sessionStatus = OPEN or CLOSED
--
-- EXPECTED:
-- invalidRows = 0
-- ============================================================================

SELECT
    COUNT(*) AS invalidRows
FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily
WHERE sessionStartDatePst = DATE '2026-09-28'
  AND (
         pageViews <= 1
      OR isNonBounced <> 1
      OR sessionStatus NOT IN ('OPEN', 'CLOSED')
  );



-- ############################################################################
-- SILVER 03
-- ACTIONS PER SESSION × PAGE CATEGORY
-- ############################################################################


-- ============================================================================
-- 03A. PREFLIGHT
-- ============================================================================

CALL prdrzranalytics.lab42.sdi_sp_mip_silver_actionsPerSessionPageCategory_daily(
    p_asOfDate        => DATE '2026-09-28',
    p_eventWindowDays => 1,
    p_validateOnly    => TRUE
);


-- ============================================================================
-- 03B. LOAD
-- ============================================================================

CALL prdrzranalytics.lab42.sdi_sp_mip_silver_actionsPerSessionPageCategory_daily(
    p_asOfDate        => DATE '2026-09-28',
    p_eventWindowDays => 1,
    p_validateOnly    => FALSE
);


-- ============================================================================
-- 03C. VALIDATION 1
-- Basic population.
-- ============================================================================

SELECT
    sessionStartDatePst,

    COUNT(*) AS rowCount,

    COUNT(DISTINCT sessionId) AS distinctSessions,

    COUNT(DISTINCT visitorId) AS distinctVisitors,

    SUM(pageViews) AS pageViews,

    SUM(orderCount) AS orderCount,

    SUM(vrCallEvents) AS vrCallEvents,

    SUM(vrChatEvents) AS vrChatEvents,

    SUM(storeLocatorEvents) AS storeLocatorEvents

FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily

WHERE sessionStartDatePst = DATE '2026-09-28'

GROUP BY sessionStartDatePst;


-- ============================================================================
-- 03D. VALIDATION 2
-- Grain check:
-- one row per sessionId × pageCategory.
--
-- EXPECTED:
-- No rows.
-- ============================================================================

SELECT
    sessionId,
    pageCategory,
    COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily
WHERE sessionStartDatePst = DATE '2026-09-28'
GROUP BY
    sessionId,
    pageCategory
HAVING COUNT(*) > 1
ORDER BY rowCount DESC
LIMIT 100;


-- ============================================================================
-- 03E. VALIDATION 3
-- Every row should contain at least one relevant action.
--
-- This matches the HAVING condition used in the procedure.
--
-- EXPECTED:
-- invalidRows = 0
-- ============================================================================

SELECT
    COUNT(*) AS invalidRows
FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily
WHERE sessionStartDatePst = DATE '2026-09-28'
  AND coalesce(pageViews, 0) = 0
  AND coalesce(orderCount, 0) = 0
  AND coalesce(vrCallEvents, 0) = 0
  AND coalesce(vrChatEvents, 0) = 0
  AND coalesce(storeLocatorEvents, 0) = 0
  AND coalesce(assistedOrderEvents, 0) = 0
  AND coalesce(hasBuyFlow, 0) = 0
  AND coalesce(configureEvents, 0) = 0
  AND coalesce(checkoutStartEvents, 0) = 0;



-- ############################################################################
-- SILVER 04
-- ATTRIBUTES PER VISITOR / WEEK
--
-- 2026-09-28 belongs to:
--
-- weekStartDate = 2026-09-27
-- weekEndDate   = 2026-10-03
--
-- With only Sep 28 daily data currently loaded,
-- this is intentionally a PARTIAL WEEK.
-- ############################################################################


-- ============================================================================
-- 04A. PREFLIGHT
-- ============================================================================

CALL prdrzranalytics.lab42.sdi_sp_mip_silver_attributesPerVisitorWeek_weekly(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => TRUE
);


-- ============================================================================
-- 04B. LOAD
-- ============================================================================

CALL prdrzranalytics.lab42.sdi_sp_mip_silver_attributesPerVisitorWeek_weekly(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => FALSE
);


-- ============================================================================
-- 04C. VALIDATION 1
-- Basic weekly population.
--
-- EXPECTED:
-- rowCount = distinctVisitors
-- ============================================================================

SELECT
    weekStartDate,
    weekEndDate,

    COUNT(*) AS rowCount,

    COUNT(DISTINCT visitorId) AS distinctVisitors

FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly

WHERE weekStartDate = DATE '2026-09-27'

GROUP BY
    weekStartDate,
    weekEndDate;


-- ============================================================================
-- 04D. VALIDATION 2
-- Grain check:
-- one row per visitorId × weekStartDate.
--
-- EXPECTED:
-- No rows.
-- ============================================================================

SELECT
    visitorId,
    weekStartDate,
    COUNT(*) AS rowCount

FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly

WHERE weekStartDate = DATE '2026-09-27'

GROUP BY
    visitorId,
    weekStartDate

HAVING COUNT(*) > 1

ORDER BY rowCount DESC

LIMIT 100;


-- ============================================================================
-- 04E. VALIDATION 3
-- Required attributed values should not be NULL.
--
-- EXPECTED:
-- nullAttributeRows = 0
-- ============================================================================

SELECT
    COUNT(*) AS nullAttributeRows

FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly

WHERE weekStartDate = DATE '2026-09-27'
  AND (
         visitorId IS NULL
      OR lob IS NULL
      OR platform IS NULL
      OR prospectVsBase IS NULL
      OR authState IS NULL
      OR channel IS NULL
      OR entryPage IS NULL
      OR pageCategory IS NULL
      OR device IS NULL
      OR buyFlowStep IS NULL
      OR region IS NULL
  );



-- ############################################################################
-- SILVER 05
-- ACTIONS PER VISITOR / WEEK
-- ############################################################################


-- ============================================================================
-- 05A. PREFLIGHT
-- ============================================================================

CALL prdrzranalytics.lab42.sdi_sp_mip_silver_actionsPerVisitorWeek_weekly(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => TRUE
);


-- ============================================================================
-- 05B. LOAD
-- ============================================================================

CALL prdrzranalytics.lab42.sdi_sp_mip_silver_actionsPerVisitorWeek_weekly(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => FALSE
);


-- ============================================================================
-- 05C. VALIDATION 1
-- Weekly metric sanity.
--
-- EXPECTED:
-- rowCount = distinctVisitors
-- sum(nbv) = distinctVisitors
-- ============================================================================

SELECT
    weekStartDate,
    weekEndDate,

    COUNT(*) AS rowCount,

    COUNT(DISTINCT visitorId) AS distinctVisitors,

    SUM(nbv) AS nbv,

    SUM(sessionCount) AS sessionCount,

    SUM(pageViews) AS pageViews,

    SUM(nbvBuyFlow) AS nbvBuyFlow,

    SUM(nbvConfigure) AS nbvConfigure,

    SUM(nbvCheckoutStart) AS nbvCheckoutStart,

    SUM(orders) AS orderingVisitors,

    SUM(ordersAcquisition) AS acquisitionOrderingVisitors,

    SUM(ordersBase) AS baseOrderingVisitors,

    SUM(ordersUnassisted) AS unassistedOrderingVisitors,

    SUM(ordersAssisted) AS assistedOrderingVisitors,

    SUM(orderCount) AS orderCount

FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly

WHERE weekStartDate = DATE '2026-09-27'

GROUP BY
    weekStartDate,
    weekEndDate;


-- ============================================================================
-- 05D. VALIDATION 2
-- Grain check.
--
-- EXPECTED:
-- No rows.
-- ============================================================================

SELECT
    visitorId,
    weekStartDate,
    COUNT(*) AS rowCount

FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly

WHERE weekStartDate = DATE '2026-09-27'

GROUP BY
    visitorId,
    weekStartDate

HAVING COUNT(*) > 1

ORDER BY rowCount DESC

LIMIT 100;


-- ============================================================================
-- 05E. VALIDATION 3
-- Visitor metric invariants.
--
-- Expected relationships:
--
-- ordersAcquisition + ordersBase = orders
-- ordersAssisted + ordersUnassisted = orders
--
-- All unique-visitor flags should be 0 or 1.
--
-- EXPECTED:
-- invalidRows = 0
-- ============================================================================

SELECT
    COUNT(*) AS invalidRows

FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly

WHERE weekStartDate = DATE '2026-09-27'

  AND (
         ordersAcquisition + ordersBase <> orders

      OR ordersAssisted + ordersUnassisted <> orders

      OR nbv NOT IN (0, 1)

      OR nbvBuyFlow NOT IN (0, 1)

      OR nbvConfigure NOT IN (0, 1)

      OR nbvCheckoutStart NOT IN (0, 1)

      OR orders NOT IN (0, 1)

      OR ordersAcquisition NOT IN (0, 1)

      OR ordersBase NOT IN (0, 1)

      OR ordersUnassisted NOT IN (0, 1)

      OR ordersAssisted NOT IN (0, 1)

      OR vrCalls NOT IN (0, 1)

      OR vrChats NOT IN (0, 1)

      OR storeLocator NOT IN (0, 1)
  );



-- ############################################################################
-- FINAL CROSS-SILVER VALIDATION
-- SILVER 04 vs SILVER 05
--
-- Both weekly tables are intended to have the same visitor/week population.
-- ############################################################################


-- ============================================================================
-- 06A. KEY COVERAGE
--
-- EXPECTED:
-- mismatchRows = 0
-- ============================================================================

SELECT
    COUNT(*) AS mismatchRows

FROM (

    SELECT
        coalesce(a.visitorId, m.visitorId) AS visitorId,
        coalesce(a.weekStartDate, m.weekStartDate) AS weekStartDate

    FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly a

    FULL OUTER JOIN
        prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly m

        ON  a.visitorId = m.visitorId
        AND a.weekStartDate = m.weekStartDate

    WHERE coalesce(
        a.weekStartDate,
        m.weekStartDate
    ) = DATE '2026-09-27'

      AND (
             a.visitorId IS NULL
          OR m.visitorId IS NULL
      )

) mismatches;


-- ============================================================================
-- 06B. SIMPLE SIDE-BY-SIDE WEEKLY ROW COUNTS
--
-- EXPECTED:
-- ATTRIBUTES and ACTIONS row counts should match.
-- ============================================================================

SELECT
    'ATTRIBUTES' AS dataset,
    COUNT(*) AS rowCount,
    COUNT(DISTINCT visitorId) AS distinctVisitors

FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly

WHERE weekStartDate = DATE '2026-09-27'

UNION ALL

SELECT
    'ACTIONS' AS dataset,
    COUNT(*) AS rowCount,
    COUNT(DISTINCT visitorId) AS distinctVisitors

FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly

WHERE weekStartDate = DATE '2026-09-27';