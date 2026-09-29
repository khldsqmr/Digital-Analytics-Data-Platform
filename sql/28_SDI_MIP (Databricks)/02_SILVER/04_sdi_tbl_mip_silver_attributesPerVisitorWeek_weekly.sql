
-- ###########################################################################
-- BEGIN silver/04_sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly.sql
-- ###########################################################################

-- ============================================================================
-- FILE  : 04_sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly.sql
-- LAYER : SILVER
-- PURPOSE:
--   One attributed attribute row per visitor/week for additive serving breakouts.
-- ============================================================================

USE CATALOG prdrzranalytics;
USE SCHEMA lab42;
-- ----------------------------------------------------------------------------
-- One row per visitor × week: attributes
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly (
  weekStartDate       DATE,
  weekEndDate         DATE,
  visitorId           STRING,
  identitySource      STRING,

  lob                 STRING COMMENT 'Attributed weekly breakout LOB: most page views',
  lobList             ARRAY<STRING> COMMENT 'Natural LOB memberships touched during the week',
  platform            STRING COMMENT 'Attributed weekly platform: most page views',
  platformList        ARRAY<STRING> COMMENT 'Natural platforms touched during the week',

  prospectVsBase      STRING COMMENT 'Strongest weekly state: Customer > Care > Prospect > Unknown',
  authState           STRING COMMENT 'Strongest weekly auth state',

  channel             STRING COMMENT 'Last qualifying session channel; dashboard attribution only',
  campaign            STRING,
  campaignCode        STRING,
  entryPage           STRING,
  utmSource           STRING,
  utmMedium           STRING,
  utmCampaign         STRING,

  pageCategory        STRING COMMENT 'Most-viewed weekly page category',
  device              STRING COMMENT 'Most-viewed proposed device grouping',
  buyFlowStep         STRING COMMENT 'Deepest weekly step; Did not enter buy flow if none',
  region              STRING COMMENT 'Placeholder until geo solution is supplied',

  isTmoNetwork        INT,

  _runId              STRING,
  silverProcessedAt   TIMESTAMP
)
USING DELTA
CLUSTER BY (weekStartDate)
COMMENT 'Silver: one attributed attribute row per visitor per week; 1:1 with actionsPerVisitorWeek.';

CREATE OR REPLACE PROCEDURE sdi_sp_mip_silver_attributesPerVisitorWeek_weekly(
  p_runId          STRING,
  p_asOfDate       DATE DEFAULT NULL,
  p_weeksToRebuild INT DEFAULT 1
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  DECLARE v_asOfDate    DATE DEFAULT coalesce(
    p_asOfDate,
    to_date(from_utc_timestamp(current_timestamp(), 'America/Los_Angeles'))
  );
  DECLARE v_weekTo      DATE DEFAULT date_add(v_asOfDate, 1 - dayofweek(v_asOfDate));
  DECLARE v_weekFrom    DATE DEFAULT date_add(v_weekTo, -7 * (p_weeksToRebuild - 1));
  DECLARE v_processedAt TIMESTAMP DEFAULT current_timestamp();

  IF p_weeksToRebuild < 1 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'p_weeksToRebuild must be >= 1';
  END IF;

  INSERT INTO sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly
  REPLACE WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
  WITH weekSessions AS (
    SELECT *
    FROM sdi_tbl_mip_silver_attributesPerSession_daily
    WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
      AND visitorId IS NOT NULL
  ),
  keys AS (
    SELECT DISTINCT visitorId, weekStartDate
    FROM weekSessions
  ),

  lobPv AS (
    SELECT
      s.visitorId,
      s.weekStartDate,
      lobKey AS lob,
      sum(lobValue) AS pageViews
    FROM weekSessions s
    LATERAL VIEW explode(s.lobPageViews) x AS lobKey, lobValue
    GROUP BY s.visitorId, s.weekStartDate, lobKey
  ),
  lobResolved AS (
    SELECT
      visitorId,
      weekStartDate,
      max_by(lob, struct(pageViews, lob)) AS lob
    FROM lobPv
    GROUP BY visitorId, weekStartDate
  ),

  platformPv AS (
    SELECT
      s.visitorId,
      s.weekStartDate,
      platformKey AS platform,
      sum(platformValue) AS pageViews
    FROM weekSessions s
    LATERAL VIEW explode(s.platformPageViews) x AS platformKey, platformValue
    GROUP BY s.visitorId, s.weekStartDate, platformKey
  ),
  platformResolved AS (
    SELECT
      visitorId,
      weekStartDate,
      max_by(platform, struct(pageViews, platform)) AS platform
    FROM platformPv
    GROUP BY visitorId, weekStartDate
  ),

  devicePv AS (
    SELECT
      s.visitorId,
      s.weekStartDate,
      deviceKey AS device,
      sum(deviceValue) AS pageViews
    FROM weekSessions s
    LATERAL VIEW explode(s.devicePageViews) x AS deviceKey, deviceValue
    GROUP BY s.visitorId, s.weekStartDate, deviceKey
  ),
  deviceResolved AS (
    SELECT
      visitorId,
      weekStartDate,
      max_by(device, struct(pageViews, device)) AS device
    FROM devicePv
    GROUP BY visitorId, weekStartDate
  ),

  categoryPv AS (
    SELECT
      visitorId,
      weekStartDate,
      pageCategory,
      sum(pageViews) AS pageViews
    FROM sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily
    WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
      AND visitorId IS NOT NULL
    GROUP BY visitorId, weekStartDate, pageCategory
  ),
  categoryResolved AS (
    SELECT
      visitorId,
      weekStartDate,
      max_by(pageCategory, struct(pageViews, pageCategory)) AS pageCategory
    FROM categoryPv
    GROUP BY visitorId, weekStartDate
  ),

  agg AS (
    SELECT
      visitorId,
      weekStartDate,

      min_by(identitySource, sessionStartTsUtc)
        FILTER (WHERE identitySource IS NOT NULL) AS identitySource,

      array_sort(
        array_distinct(
          flatten(collect_list(coalesce(lobList, cast(array() AS ARRAY<STRING>))))
        )
      ) AS lobList,

      array_sort(collect_set(coalesce(platform, '(not set)'))) AS platformList,

      max_by(prospectVsBase, struct(prospectVsBaseRank, sessionStartTsUtc)) AS prospectVsBase,
      max(prospectVsBaseRank) AS prospectVsBaseRank,

      max_by(authState, struct(authStateRank, sessionStartTsUtc)) AS authState,
      max(authStateRank) AS authStateRank,

      -- One attributed acquisition context for the dashboard.
      -- Prefer a real channel over Session Refresh/(not set), then take latest.
      max_by(
        named_struct(
          'channel', channel,
          'campaignCode', campaignCode,
          'campaignName', campaignName,
          'entryPage', entryPage,
          'utmSource', utmSource,
          'utmMedium', utmMedium,
          'utmCampaign', utmCampaign
        ),
        struct(
          CASE
            WHEN channel IS NOT NULL
             AND channel NOT IN ('(not set)', 'Session Refresh') THEN 1
            ELSE 0
          END,
          sessionStartTsUtc,
          sessionId
        )
      ) AS attributedTouch,

      max_by(
        deepestBuyFlowStep,
        struct(coalesce(deepestBuyFlowStepOrder, -1), sessionStartTsUtc)
      ) FILTER (WHERE deepestBuyFlowStep IS NOT NULL) AS deepestBuyFlowStep,

      max(isTmoNetworkSession) AS isTmoNetwork
    FROM weekSessions
    GROUP BY visitorId, weekStartDate
  )

  SELECT
    k.weekStartDate,
    date_add(k.weekStartDate, 6) AS weekEndDate,
    k.visitorId,
    a.identitySource,

    coalesce(l.lob, 'Other') AS lob,
    a.lobList,
    coalesce(p.platform, '(not set)') AS platform,
    a.platformList,

    coalesce(a.prospectVsBase, 'Unknown') AS prospectVsBase,
    coalesce(a.authState, '(not set)') AS authState,

    coalesce(a.attributedTouch.channel, '(not set)') AS channel,
    CASE
      WHEN a.attributedTouch.campaignCode IS NULL THEN '(not set)'
      WHEN a.attributedTouch.campaignName IS NOT NULL
        THEN concat(a.attributedTouch.campaignCode, ' · ', a.attributedTouch.campaignName)
      ELSE a.attributedTouch.campaignCode
    END AS campaign,
    a.attributedTouch.campaignCode AS campaignCode,
    coalesce(a.attributedTouch.entryPage, '(not set)') AS entryPage,
    coalesce(a.attributedTouch.utmSource, '(not set)') AS utmSource,
    coalesce(a.attributedTouch.utmMedium, '(not set)') AS utmMedium,
    coalesce(a.attributedTouch.utmCampaign, '(not set)') AS utmCampaign,

    coalesce(c.pageCategory, '(not set)') AS pageCategory,
    coalesce(d.device, 'Unknown') AS device,
    coalesce(a.deepestBuyFlowStep, 'Did not enter buy flow') AS buyFlowStep,
    '(not available)' AS region,

    coalesce(a.isTmoNetwork, 0) AS isTmoNetwork,

    p_runId AS _runId,
    v_processedAt AS silverProcessedAt
  FROM keys k
  JOIN agg a
    ON  a.visitorId = k.visitorId
    AND a.weekStartDate = k.weekStartDate
  LEFT JOIN lobResolved l
    ON  l.visitorId = k.visitorId
    AND l.weekStartDate = k.weekStartDate
  LEFT JOIN platformResolved p
    ON  p.visitorId = k.visitorId
    AND p.weekStartDate = k.weekStartDate
  LEFT JOIN deviceResolved d
    ON  d.visitorId = k.visitorId
    AND d.weekStartDate = k.weekStartDate
  LEFT JOIN categoryResolved c
    ON  c.visitorId = k.visitorId
    AND c.weekStartDate = k.weekStartDate;
END;

-- ----------------------------------------------------------------------------


-- ###########################################################################
-- END silver/04_sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly.sql
-- ###########################################################################

