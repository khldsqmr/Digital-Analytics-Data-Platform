-- ============================================================================
-- FILE  : 20_mip_silver_sanity_checks.sql
-- PURPOSE:
--   Development/backfill execution and sanity checks for MIP Silver 01-05.
--
-- EXAMPLE DAILY WINDOW:
--   2026-09-20 through 2026-09-29.
--
-- IMPORTANT GRAIN NOTE:
--   Silver 01 is NOT expected to equal all Bronze UDI rows. It contains only UDI
--   hits that join through Bronze SEF to an OPEN/CLOSED Bronze SSF session. The
--   correct reconciliation therefore uses that exact join as the expected set.
--
-- WEEKLY NOTE:
--   A 10-day daily backfill ending 2026-09-29 contains one complete reporting
--   week (2026-09-20..2026-09-26) plus a partial week beginning 2026-09-27.
--   Do not treat the partial week as production-complete.
--
-- FUTURE VALIDATION LAYER:
--   CRITICAL checks are strong candidates for automated gates. INFORMATIONAL
--   checks should receive approved thresholds before becoming fail-fast rules.
-- ============================================================================

-- ############################################################################
-- A. PREFLIGHT + LOAD ORDER
-- ############################################################################
-- SILVER 01 preflight/load uses event-date partitions.
CALL prdrzranalytics.lab42.sdi_sp_mip_silver_detailsPerHit_daily(
    p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>TRUE);
CALL prdrzranalytics.lab42.sdi_sp_mip_silver_detailsPerHit_daily(
    p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>FALSE);

-- SILVER 02/03 use sessionStartDatePst rebuild windows.
CALL prdrzranalytics.lab42.sdi_sp_mip_silver_attributesPerSession_daily(
    p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>TRUE);
CALL prdrzranalytics.lab42.sdi_sp_mip_silver_attributesPerSession_daily(
    p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>FALSE);
CALL prdrzranalytics.lab42.sdi_sp_mip_silver_actionsPerSessionPageCategory_daily(
    p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>TRUE);
CALL prdrzranalytics.lab42.sdi_sp_mip_silver_actionsPerSessionPageCategory_daily(
    p_asOfDate=>DATE '2026-09-29',p_eventWindowDays=>10,p_validateOnly=>FALSE);

-- WEEKLY EXAMPLE: build only the complete week ending 2026-09-26 from this
-- 10-day example window. For the partial week starting 2026-09-27, use a later
-- as-of date only after the full Sunday-Saturday source history is loaded.
CALL prdrzranalytics.lab42.sdi_sp_mip_silver_attributesPerVisitorWeek_weekly(
    p_asOfDate=>DATE '2026-09-26',p_weeksToRebuild=>1,p_validateOnly=>TRUE);
CALL prdrzranalytics.lab42.sdi_sp_mip_silver_attributesPerVisitorWeek_weekly(
    p_asOfDate=>DATE '2026-09-26',p_weeksToRebuild=>1,p_validateOnly=>FALSE);
CALL prdrzranalytics.lab42.sdi_sp_mip_silver_actionsPerVisitorWeek_weekly(
    p_asOfDate=>DATE '2026-09-26',p_weeksToRebuild=>1,p_validateOnly=>TRUE);
CALL prdrzranalytics.lab42.sdi_sp_mip_silver_actionsPerVisitorWeek_weekly(
    p_asOfDate=>DATE '2026-09-26',p_weeksToRebuild=>1,p_validateOnly=>FALSE);

-- ############################################################################
-- B. SILVER 01 - detailsPerHit
-- ############################################################################
-- CRITICAL: expected valid joined Bronze hit set = Silver 01 by event day/source.
WITH expected AS (
    SELECT h.event_date,h.source_table,COUNT(*) AS expectedRows
    FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily h
    INNER JOIN prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily e
      ON h.row_identity_hash=e.row_identity_hash
     AND h.event_date=e.event_date
     AND h.source_table=e.source_table
    INNER JOIN prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily s
      ON s.session_id=e.session_id
     AND s.session_status IN ('OPEN','CLOSED')
    WHERE h.event_date BETWEEN DATE '2026-09-20' AND DATE '2026-09-29'
    GROUP BY h.event_date,h.source_table
), actual AS (
    SELECT eventDate,sourceTable,COUNT(*) AS actualRows
    FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily
    WHERE eventDate BETWEEN DATE '2026-09-20' AND DATE '2026-09-29'
    GROUP BY eventDate,sourceTable
)
SELECT
    coalesce(e.event_date,a.eventDate) AS eventDate,
    coalesce(e.source_table,a.sourceTable) AS sourceTable,
    coalesce(e.expectedRows,0) AS expectedRows,
    coalesce(a.actualRows,0) AS actualRows,
    coalesce(a.actualRows,0)-coalesce(e.expectedRows,0) AS rowDiff
FROM expected e FULL OUTER JOIN actual a
  ON e.event_date=a.eventDate AND e.source_table=a.sourceTable
ORDER BY eventDate,sourceTable;

-- CRITICAL: expected zero duplicate composite hit keys.
SELECT rowIdentityHash,eventDate,sourceTable,COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily
WHERE eventDate BETWEEN DATE '2026-09-20' AND DATE '2026-09-29'
GROUP BY rowIdentityHash,eventDate,sourceTable
HAVING COUNT(*)>1
ORDER BY rowCount DESC
LIMIT 100;

-- INFORMATIONAL: identity/session coverage.
SELECT
    eventDate,sourceTable,COUNT(*) AS rows,
    COUNT_IF(sessionId IS NULL) AS missingSessionId,
    COUNT_IF(visitorId IS NULL) AS missingVisitorId,
    COUNT_IF(canonicalUserId IS NOT NULL) AS canonicalRows,
    COUNT_IF(identitySource='canonicalUserId') AS canonicalSourceRows
FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily
WHERE eventDate BETWEEN DATE '2026-09-20' AND DATE '2026-09-29'
GROUP BY eventDate,sourceTable
ORDER BY eventDate,sourceTable;

-- INFORMATIONAL: UTM/channel/campaign coverage. Web UTM is expected; App entry URL
-- and UTM are normally absent. channelName remains exact acquisition taxonomy.
SELECT
    sourceTable,COUNT(*) AS rows,
    COUNT_IF(sessionEntryPageUrlFull IS NOT NULL AND trim(sessionEntryPageUrlFull)<>'') AS rowsWithEntryUrl,
    COUNT_IF(utmSource<>'(not set)') AS rowsWithUtmSource,
    COUNT_IF(utmMedium<>'(not set)') AS rowsWithUtmMedium,
    COUNT_IF(utmCampaign<>'(not set)') AS rowsWithUtmCampaign,
    COUNT_IF(channelName IS NOT NULL) AS rowsWithChannel,
    COUNT_IF(externalCampaignCode IS NOT NULL) AS rowsWithExternalCampaign
FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily
WHERE eventDate BETWEEN DATE '2026-09-20' AND DATE '2026-09-29'
GROUP BY sourceTable ORDER BY sourceTable;

-- INFORMATIONAL: inspect native marketing-channel values; there should be no
-- MIP-created parent Paid Search bucket unless it exists natively upstream.
SELECT sourceTable,channelName,COUNT(*) AS rows
FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily
WHERE eventDate=DATE '2026-09-24' AND channelName IS NOT NULL
GROUP BY sourceTable,channelName
ORDER BY sourceTable,rows DESC
LIMIT 200;

-- INFORMATIONAL: current temporary geography, intentionally not normalized.
SELECT sourceTable,siteName,geoCountry,geoRegion,COUNT(*) AS rows
FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily
WHERE eventDate=DATE '2026-09-24'
GROUP BY sourceTable,siteName,geoCountry,geoRegion
ORDER BY rows DESC
LIMIT 200;

-- ############################################################################
-- C. SILVER 02 - attributesPerSession
-- ############################################################################
-- CRITICAL: one output row per qualifying NBV session (>1 real page view).
WITH expected AS (
    SELECT sessionId,sessionStartDatePst
    FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily
    WHERE sessionStartDatePst BETWEEN DATE '2026-09-20' AND DATE '2026-09-29'
    GROUP BY sessionId,sessionStartDatePst
    HAVING SUM(CAST(isPageView AS BIGINT))>1
), actual AS (
    SELECT sessionId,sessionStartDatePst
    FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily
    WHERE sessionStartDatePst BETWEEN DATE '2026-09-20' AND DATE '2026-09-29'
)
SELECT
    (SELECT COUNT(*) FROM expected) AS expectedNbvSessions,
    (SELECT COUNT(*) FROM actual) AS actualNbvSessions,
    (SELECT COUNT(*) FROM actual)-(SELECT COUNT(*) FROM expected) AS rowDiff;

-- CRITICAL: expected zero duplicates and zero invalid NBV rows.
SELECT sessionId,COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily
WHERE sessionStartDatePst BETWEEN DATE '2026-09-20' AND DATE '2026-09-29'
GROUP BY sessionId HAVING COUNT(*)>1
ORDER BY rowCount DESC LIMIT 100;

SELECT
    COUNT(*) AS rowsChecked,
    COUNT_IF(pageViews<=1) AS invalidNbvRows,
    COUNT_IF(isNonBounced<>1) AS invalidNonBouncedFlag,
    COUNT_IF(visitorId IS NULL) AS missingVisitorId
FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily
WHERE sessionStartDatePst BETWEEN DATE '2026-09-20' AND DATE '2026-09-29';

-- INFORMATIONAL: session-level exact channel taxonomy after interim resolution.
SELECT channel,COUNT(*) AS sessions
FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily
WHERE sessionStartDatePst=DATE '2026-09-24'
GROUP BY channel ORDER BY sessions DESC LIMIT 100;

-- ############################################################################
-- D. SILVER 03 - actionsPerSessionPageCategory
-- ############################################################################
-- CRITICAL: one row per session x pageCategory.
SELECT sessionId,pageCategory,COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily
WHERE sessionStartDatePst BETWEEN DATE '2026-09-20' AND DATE '2026-09-29'
GROUP BY sessionId,pageCategory
HAVING COUNT(*)>1
ORDER BY rowCount DESC LIMIT 100;

-- CRITICAL: zero-signal rows should not exist.
SELECT COUNT(*) AS zeroSignalRows
FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily
WHERE sessionStartDatePst BETWEEN DATE '2026-09-20' AND DATE '2026-09-29'
  AND pageViews=0 AND orderCount=0 AND vrCallEvents=0 AND vrChatEvents=0
  AND storeLocatorEvents=0 AND assistedOrderEvents=0 AND hasBuyFlow=0
  AND configureEvents=0 AND checkoutStartEvents=0;

-- CRITICAL: additive Page Views and orderCount reconcile back to session Silver.
WITH a AS (
    SELECT sessionStartDatePst,SUM(pageViews) AS actionPageViews,SUM(orderCount) AS actionOrderCount
    FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily
    WHERE sessionStartDatePst BETWEEN DATE '2026-09-20' AND DATE '2026-09-29'
    GROUP BY sessionStartDatePst
), s AS (
    SELECT sessionStartDatePst,SUM(pageViews) AS sessionPageViews,SUM(orderCount) AS sessionOrderCount
    FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily
    WHERE sessionStartDatePst BETWEEN DATE '2026-09-20' AND DATE '2026-09-29'
    GROUP BY sessionStartDatePst
)
SELECT coalesce(a.sessionStartDatePst,s.sessionStartDatePst) AS sessionStartDatePst,
       coalesce(a.actionPageViews,0) AS actionPageViews,coalesce(s.sessionPageViews,0) AS sessionPageViews,
       coalesce(a.actionPageViews,0)-coalesce(s.sessionPageViews,0) AS pageViewDiff,
       coalesce(a.actionOrderCount,0) AS actionOrderCount,coalesce(s.sessionOrderCount,0) AS sessionOrderCount,
       coalesce(a.actionOrderCount,0)-coalesce(s.sessionOrderCount,0) AS orderCountDiff
FROM a FULL OUTER JOIN s ON a.sessionStartDatePst=s.sessionStartDatePst
ORDER BY sessionStartDatePst;

-- ############################################################################
-- E. SILVER 04 - attributesPerVisitorWeek
-- ############################################################################
-- Example complete week from the 10-day daily window: 2026-09-20.
-- CRITICAL: one row per visitor/week.
SELECT weekStartDate,visitorId,COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly
WHERE weekStartDate=DATE '2026-09-20'
GROUP BY weekStartDate,visitorId HAVING COUNT(*)>1
ORDER BY rowCount DESC LIMIT 100;

-- CRITICAL: weekly visitor population equals distinct visitor/week in session Silver.
WITH expected AS (
    SELECT DISTINCT visitorId,weekStartDate
    FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily
    WHERE weekStartDate=DATE '2026-09-20' AND visitorId IS NOT NULL
), actual AS (
    SELECT visitorId,weekStartDate
    FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly
    WHERE weekStartDate=DATE '2026-09-20'
)
SELECT (SELECT COUNT(*) FROM expected) AS expectedVisitors,
       (SELECT COUNT(*) FROM actual) AS actualVisitors,
       (SELECT COUNT(*) FROM actual)-(SELECT COUNT(*) FROM expected) AS rowDiff;

-- INFORMATIONAL: exact channel categories remain separate after weekly attribution.
SELECT channel,COUNT(*) AS visitors
FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly
WHERE weekStartDate=DATE '2026-09-20'
GROUP BY channel ORDER BY visitors DESC LIMIT 100;

-- INFORMATIONAL: attribution sanity.
SELECT
    COUNT(*) AS rowsChecked,
    COUNT_IF(NOT array_contains(lobList,lob)) AS invalidLobAttribution,
    COUNT_IF(NOT array_contains(platformList,platform)) AS invalidPlatformAttribution,
    COUNT_IF(weekEndDate<>date_add(weekStartDate,6)) AS invalidWeekEnd
FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly
WHERE weekStartDate=DATE '2026-09-20';

-- ############################################################################
-- F. SILVER 05 - actionsPerVisitorWeek
-- ############################################################################
-- CRITICAL: one metric row per visitor/week.
SELECT weekStartDate,visitorId,COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly
WHERE weekStartDate=DATE '2026-09-20'
GROUP BY weekStartDate,visitorId HAVING COUNT(*)>1
ORDER BY rowCount DESC LIMIT 100;

-- CRITICAL: Silver 04 and Silver 05 must be 1:1 by visitor/week.
WITH a AS (
    SELECT visitorId,weekStartDate
    FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly
    WHERE weekStartDate=DATE '2026-09-20'
), m AS (
    SELECT visitorId,weekStartDate
    FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly
    WHERE weekStartDate=DATE '2026-09-20'
)
SELECT
    COUNT(*) AS unionKeys,
    COUNT_IF(a.visitorId IS NULL) AS missingAttributeRows,
    COUNT_IF(m.visitorId IS NULL) AS missingMetricRows
FROM a FULL OUTER JOIN m
  ON a.visitorId=m.visitorId AND a.weekStartDate=m.weekStartDate;

-- CRITICAL: additive weekly Page Views / orderCount reconcile to session Silver.
WITH expected AS (
    SELECT weekStartDate,SUM(pageViews) AS pageViews,SUM(orderCount) AS orderCount
    FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily
    WHERE weekStartDate=DATE '2026-09-20' AND visitorId IS NOT NULL
    GROUP BY weekStartDate
), actual AS (
    SELECT weekStartDate,SUM(pageViews) AS pageViews,SUM(orderCount) AS orderCount
    FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly
    WHERE weekStartDate=DATE '2026-09-20'
    GROUP BY weekStartDate
)
SELECT a.weekStartDate,
       a.pageViews AS actualPageViews,e.pageViews AS expectedPageViews,a.pageViews-e.pageViews AS pageViewDiff,
       a.orderCount AS actualOrderCount,e.orderCount AS expectedOrderCount,a.orderCount-e.orderCount AS orderCountDiff
FROM actual a JOIN expected e USING (weekStartDate);

-- CRITICAL: visitor metric flags are binary and derived splits do not exceed orders.
SELECT
    COUNT(*) AS rowsChecked,
    COUNT_IF(nbv<>1) AS invalidNbv,
    COUNT_IF(nbvBuyFlow NOT IN (0,1)) AS invalidNbvBuyFlow,
    COUNT_IF(nbvConfigure NOT IN (0,1)) AS invalidNbvConfigure,
    COUNT_IF(nbvCheckoutStart NOT IN (0,1)) AS invalidNbvCheckoutStart,
    COUNT_IF(orders NOT IN (0,1)) AS invalidOrders,
    COUNT_IF(ordersAcquisition NOT IN (0,1)) AS invalidOrdersAcquisition,
    COUNT_IF(ordersBase NOT IN (0,1)) AS invalidOrdersBase,
    COUNT_IF(ordersAssisted NOT IN (0,1)) AS invalidOrdersAssisted,
    COUNT_IF(ordersUnassisted NOT IN (0,1)) AS invalidOrdersUnassisted,
    COUNT_IF(vrCalls NOT IN (0,1)) AS invalidVrCalls,
    COUNT_IF(vrChats NOT IN (0,1)) AS invalidVrChats,
    COUNT_IF(storeLocator NOT IN (0,1)) AS invalidStoreLocator,
    COUNT_IF(ordersAcquisition>orders) AS acquisitionExceedsOrders,
    COUNT_IF(ordersAssisted>orders) AS assistedExceedsOrders
FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly
WHERE weekStartDate=DATE '2026-09-20';
