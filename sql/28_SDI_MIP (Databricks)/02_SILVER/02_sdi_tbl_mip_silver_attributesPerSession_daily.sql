
-- ###########################################################################
-- BEGIN silver/02_sdi_tbl_mip_silver_attributesPerSession_daily.sql
-- ###########################################################################

-- ============================================================================
-- FILE  : 02_sdi_tbl_mip_silver_attributesPerSession_daily.sql
-- LAYER : SILVER
-- PURPOSE:
--   One row per non-bounced OPEN/CLOSED session with attributes and action flags.
-- ============================================================================

USE CATALOG prdrzranalytics;
USE SCHEMA lab42;
-- ----------------------------------------------------------------------------
-- Manager-aligned session attributes: NON-BOUNCED ONLY.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sdi_tbl_mip_silver_attributesPerSession_daily (
  sessionId                 STRING,
  canonicalUserId           STRING,
  resolvedIdentityId        STRING,
  visitorId                 STRING,
  identitySource            STRING,
  identityStatus            STRING,
  sessionStatus             STRING,

  sessionStartTsUtc         TIMESTAMP,
  sessionEndTsUtc           TIMESTAMP,
  sessionStartTsPst         TIMESTAMP,
  sessionStartDatePst       DATE,
  weekStartDate             DATE,
  weekEndDate               DATE,

  pageViews                 BIGINT,
  isNonBounced              INT COMMENT 'Always 1 in this table',

  lobList                   ARRAY<STRING>,
  lobPageViews              MAP<STRING,BIGINT>,

  platform                  STRING,
  platformPageViews         MAP<STRING,BIGINT>,
  device                    STRING,
  devicePageViews           MAP<STRING,BIGINT>,

  prospectVsBase            STRING,
  prospectVsBaseRank        INT,
  authState                 STRING,
  authStateRank             INT,

  channel                   STRING COMMENT 'Interim single channel resolution using MAX(channel_name) until upstream stickiness is fixed',
  campaignCode              STRING,
  campaignName              STRING,
  campaignCategory          STRING,
  campaignIsActive          BOOLEAN,

  entryPage                 STRING,
  utmSource                 STRING,
  utmMedium                 STRING,
  utmCampaign               STRING,

  deepestBuyFlowStep        STRING,
  deepestBuyFlowStepOrder   INT,

  hasBuyFlow                INT,
  hasConfigure              INT,
  hasCheckoutStart          INT,
  hasOrder                  INT,
  hasAcquisitionOrder       INT,
  hasAssistedOrder          INT,
  hasVrCall                 INT,
  hasVrChat                 INT,
  hasStoreLocator           INT,
  orderCount                BIGINT,

  isTmoNetworkSession       INT,

  _runId                    STRING,
  silverProcessedAt         TIMESTAMP
)
USING DELTA
CLUSTER BY (sessionStartDatePst)
COMMENT 'Silver: one row per non-bounced OPEN/CLOSED session; manager-defined session attribute layer plus MIP action flags.';

CREATE OR REPLACE PROCEDURE sdi_sp_mip_silver_attributesPerSession_daily(
  p_runId           STRING,
  p_asOfDate        DATE DEFAULT NULL,
  p_eventWindowDays INT DEFAULT 1
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  DECLARE v_asOfDate    DATE DEFAULT coalesce(
    p_asOfDate,
    to_date(from_utc_timestamp(current_timestamp(), 'America/Los_Angeles'))
  );
  DECLARE v_windowEnd   DATE DEFAULT v_asOfDate;
  DECLARE v_windowStart DATE DEFAULT date_add(v_asOfDate, -(p_eventWindowDays - 1));
  DECLARE v_processedAt TIMESTAMP DEFAULT current_timestamp();

  INSERT INTO sdi_tbl_mip_silver_attributesPerSession_daily
  REPLACE WHERE sessionStartDatePst BETWEEN v_windowStart AND v_windowEnd
  WITH candidateSessions AS (
    SELECT
      cast(session_id AS STRING) AS sessionId,
      nullif(trim(cast(canonical_user_id AS STRING)), '') AS canonicalUserId,
      cast(identity_status AS STRING) AS identityStatus,
      cast(session_status AS STRING) AS sessionStatus,
      try_cast(session_start_time AS TIMESTAMP) AS sessionStartTsUtc,
      try_cast(session_end_time AS TIMESTAMP) AS sessionEndTsUtc,
      from_utc_timestamp(try_cast(session_start_time AS TIMESTAMP), 'America/Los_Angeles') AS sessionStartTsPst,
      cast(entry_page_url_path AS STRING) AS entryPage,
      cast(entry_page_url_full AS STRING) AS entryPageUrlFull
    FROM sdi_tbl_mip_bronze_edlSessions_daily
    WHERE session_start_date BETWEEN date_add(v_windowStart, -1) AND date_add(v_windowEnd, 1)
      AND session_status IN ('OPEN', 'CLOSED')
      AND to_date(from_utc_timestamp(try_cast(session_start_time AS TIMESTAMP), 'America/Los_Angeles'))
          BETWEEN v_windowStart AND v_windowEnd
    QUALIFY row_number() OVER (
      PARTITION BY session_id
      ORDER BY try_cast(session_end_time AS TIMESTAMP) DESC NULLS LAST, _ingestedAt DESC
    ) = 1
  ),
  marketingCodeDedup AS (
    SELECT
      cast(MKT_CODE AS STRING) AS MKT_CODE,
      cast(MKT_CODE_NAME AS STRING) AS MKT_CODE_NAME,
      cast(Category AS STRING) AS Category,
      try_cast(is_active AS BOOLEAN) AS is_active
    FROM sdi_tbl_mip_bronze_edlMarketingCodes_snapshot
    QUALIFY row_number() OVER (
      PARTITION BY cast(MKT_CODE AS STRING)
      ORDER BY try_cast(is_active AS BOOLEAN) DESC NULLS LAST,
               cast(MKT_CODE_NAME AS STRING) ASC NULLS LAST
    ) = 1
  ),
  hits AS (
    SELECT
      s.sessionId,
      h.*
    FROM candidateSessions s
    JOIN sdi_tbl_mip_silver_detailsPerHit_daily h
      ON h.sessionId = s.sessionId
    WHERE h.eventDate BETWEEN date_add(v_windowStart, -1) AND date_add(v_windowEnd, 2)
  ),
  lobPvRaw AS (
    SELECT sessionId, lob, sum(isPageView) AS pageViews
    FROM hits
    GROUP BY sessionId, lob
  ),
  lobPv AS (
    SELECT
      sessionId,
      array_sort(collect_set(lob)) AS lobList,
      map_from_entries(collect_list(struct(lob, pageViews))) AS lobPageViews
    FROM lobPvRaw
    GROUP BY sessionId
  ),
  platformPvRaw AS (
    SELECT
      sessionId,
      coalesce(platform, '(not set)') AS platform,
      sum(isPageView) AS pageViews
    FROM hits
    GROUP BY sessionId, coalesce(platform, '(not set)')
  ),
  platformPv AS (
    SELECT
      sessionId,
      max(platform) AS platform,
      map_from_entries(collect_list(struct(platform, pageViews))) AS platformPageViews
    FROM platformPvRaw
    GROUP BY sessionId
  ),
  devicePvRaw AS (
    SELECT
      sessionId,
      coalesce(device, 'Unknown') AS device,
      sum(isPageView) AS pageViews
    FROM hits
    GROUP BY sessionId, coalesce(device, 'Unknown')
  ),
  devicePv AS (
    SELECT
      sessionId,
      max_by(device, struct(pageViews, device)) AS device,
      map_from_entries(collect_list(struct(device, pageViews))) AS devicePageViews
    FROM devicePvRaw
    GROUP BY sessionId
  ),
  agg AS (
    SELECT
      sessionId,

      min_by(resolvedIdentityId, eventTimestampUtc)
        FILTER (WHERE resolvedIdentityId IS NOT NULL) AS resolvedIdentityId,
      min_by(identitySource, eventTimestampUtc)
        FILTER (WHERE identitySource IS NOT NULL) AS identitySource,

      count(*) AS hitCount,
      sum(isPageView) AS pageViews,

      max_by(customerType, struct(customerTypeRank, eventTimestampUtc)) AS prospectVsBase,
      max(customerTypeRank) AS prospectVsBaseRank,

      max_by(authState, struct(authStateRank, eventTimestampUtc)) AS authState,
      max(authStateRank) AS authStateRank,

      -- Deliberately simple interim resolution to match the manager draft.
      max(channelName) AS channel,

      max(campaignCode) AS campaignCode,

      max_by(buyFlowStep, struct(coalesce(buyFlowStepOrder, -1), eventTimestampUtc))
        FILTER (WHERE buyFlowStep IS NOT NULL) AS deepestBuyFlowStep,
      max(buyFlowStepOrder) AS deepestBuyFlowStepOrder,

      max(isBuyFlow) AS hasBuyFlow,
      max(isConfigure) AS hasConfigure,
      max(isCheckoutStart) AS hasCheckoutStart,
      max(isOrder) AS hasOrder,
      max(CASE WHEN isOrder = 1 AND customerType = 'Prospect' THEN 1 ELSE 0 END) AS hasAcquisitionOrder,
      max(isAssistedOrder) AS hasAssistedOrder,
      max(isVrCall) AS hasVrCall,
      max(isVrChat) AS hasVrChat,
      max(isStoreLocator) AS hasStoreLocator,
      sum(isOrder) AS orderCount,
      max(isTmoNetwork) AS isTmoNetworkSession
    FROM hits
    GROUP BY sessionId
  ),
  nonBounced AS (
    SELECT *
    FROM agg
    WHERE pageViews > 1
  )
  SELECT
    s.sessionId,
    s.canonicalUserId,
    a.resolvedIdentityId,
    coalesce(s.canonicalUserId, a.resolvedIdentityId) AS visitorId,
    CASE
      WHEN s.canonicalUserId IS NOT NULL THEN 'canonicalUserId'
      ELSE a.identitySource
    END AS identitySource,
    s.identityStatus,
    s.sessionStatus,

    s.sessionStartTsUtc,
    s.sessionEndTsUtc,
    s.sessionStartTsPst,
    to_date(s.sessionStartTsPst) AS sessionStartDatePst,
    date_add(to_date(s.sessionStartTsPst), 1 - dayofweek(to_date(s.sessionStartTsPst))) AS weekStartDate,
    date_add(to_date(s.sessionStartTsPst), 7 - dayofweek(to_date(s.sessionStartTsPst))) AS weekEndDate,

    a.pageViews,
    1 AS isNonBounced,

    coalesce(lp.lobList, array('Other')) AS lobList,
    lp.lobPageViews,

    coalesce(pp.platform, '(not set)') AS platform,
    pp.platformPageViews,
    coalesce(dp.device, 'Unknown') AS device,
    dp.devicePageViews,

    coalesce(a.prospectVsBase, 'Unknown') AS prospectVsBase,
    coalesce(a.prospectVsBaseRank, 0) AS prospectVsBaseRank,
    coalesce(a.authState, '(not set)') AS authState,
    coalesce(a.authStateRank, -1) AS authStateRank,

    coalesce(a.channel, '(not set)') AS channel,
    a.campaignCode,
    cast(m.MKT_CODE_NAME AS STRING) AS campaignName,
    cast(m.Category AS STRING) AS campaignCategory,
    try_cast(m.is_active AS BOOLEAN) AS campaignIsActive,

    coalesce(nullif(trim(s.entryPage), ''), '(not set)') AS entryPage,

    coalesce(
      nullif(
        try_parse_url(
          CASE
            WHEN lower(coalesce(s.entryPageUrlFull, '')) LIKE 'http%' THEN s.entryPageUrlFull
            WHEN nullif(trim(s.entryPageUrlFull), '') IS NOT NULL THEN concat('https://', s.entryPageUrlFull)
            ELSE NULL
          END,
          'QUERY', 'utm_source'
        ),
        ''
      ),
      '(not set)'
    ) AS utmSource,

    coalesce(
      nullif(
        try_parse_url(
          CASE
            WHEN lower(coalesce(s.entryPageUrlFull, '')) LIKE 'http%' THEN s.entryPageUrlFull
            WHEN nullif(trim(s.entryPageUrlFull), '') IS NOT NULL THEN concat('https://', s.entryPageUrlFull)
            ELSE NULL
          END,
          'QUERY', 'utm_medium'
        ),
        ''
      ),
      '(not set)'
    ) AS utmMedium,

    coalesce(
      nullif(
        try_parse_url(
          CASE
            WHEN lower(coalesce(s.entryPageUrlFull, '')) LIKE 'http%' THEN s.entryPageUrlFull
            WHEN nullif(trim(s.entryPageUrlFull), '') IS NOT NULL THEN concat('https://', s.entryPageUrlFull)
            ELSE NULL
          END,
          'QUERY', 'utm_campaign'
        ),
        ''
      ),
      '(not set)'
    ) AS utmCampaign,

    a.deepestBuyFlowStep,
    a.deepestBuyFlowStepOrder,

    a.hasBuyFlow,
    a.hasConfigure,
    a.hasCheckoutStart,
    a.hasOrder,
    a.hasAcquisitionOrder,
    a.hasAssistedOrder,
    a.hasVrCall,
    a.hasVrChat,
    a.hasStoreLocator,
    a.orderCount,

    a.isTmoNetworkSession,

    p_runId AS _runId,
    v_processedAt AS silverProcessedAt
  FROM candidateSessions s
  JOIN nonBounced a
    ON a.sessionId = s.sessionId
  LEFT JOIN lobPv lp
    ON lp.sessionId = s.sessionId
  LEFT JOIN platformPv pp
    ON pp.sessionId = s.sessionId
  LEFT JOIN devicePv dp
    ON dp.sessionId = s.sessionId
  LEFT JOIN marketingCodeDedup m
    ON m.MKT_CODE = a.campaignCode;
END;

-- ----------------------------------------------------------------------------


-- ###########################################################################
-- END silver/02_sdi_tbl_mip_silver_attributesPerSession_daily.sql
-- ###########################################################################

