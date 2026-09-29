
-- ###########################################################################
-- BEGIN gold/02_sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long.sql
-- ###########################################################################

-- ============================================================================
-- FILE  : 02_sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long.sql
-- LAYER : GOLD
-- PURPOSE:
--   Single-breakout comparison ingredients for movers, tables, waterfall and trends.
-- ============================================================================

USE CATALOG prdrzranalytics;
USE SCHEMA lab42;
-- GOLD PRINCIPLE
-- Persist comparison INGREDIENTS, not precomputed percentages.
-- WoW / 4-week / LY comparisons join the appropriate weekly aggregates at the same
-- dimensional grain; do not cross-join raw visitors across weeks.
--
-- ----------------------------------------------------------------------------
-- BREAKOUTS
-- Used by:
--   * Overview "What moved the topline"
--   * Breakout table
--   * Waterfall
--   * Absolute trend cards
--
-- One weekly visitor has exactly one attributed value per breakout in the
-- visitor-week Silver, so within ONE breakout the slices reconcile to topline.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long (
  targetWeekStartDate         DATE,
  targetWeekEndDate           DATE,
  fiscalQuarterLabel          STRING,
  fiscalWeekCode              STRING,
  weekLabel                   STRING,

  filterLob                   STRING COMMENT 'All for now; reserved for precomputed whole-report filter contexts',
  filterPlatform              STRING COMMENT 'All for now; reserved for precomputed whole-report filter contexts',

  breakoutType                STRING,
  breakoutLabel               STRING,
  breakoutValue               STRING,

  valueRankByNbv              INT COMMENT 'Rank in the target week by NBV',
  isTopN                      BOOLEAN COMMENT 'Target-week Top-N flag; later view can bucket false rows into (Other)',

  metricName                  STRING,
  metricLabel                 STRING,
  metricKind                  STRING,
  displayFormat               STRING,
  changeUnit                  STRING,

  thisWeekNumerator           DOUBLE,
  thisWeekDenominator         DOUBLE,

  priorWeekNumerator          DOUBLE,
  priorWeekDenominator        DOUBLE,

  fourWeekTrendNumerator      DOUBLE,
  fourWeekTrendDenominator    DOUBLE,

  sameWeekLyNumerator         DOUBLE,
  sameWeekLyDenominator       DOUBLE,

  thisWeekDataAvailable       BOOLEAN,
  priorWeekDataAvailable      BOOLEAN,
  fourWeekTrendWeekCount      INT,
  sameWeekLyDataAvailable     BOOLEAN,

  peerSetNumerator            DOUBLE,
  peerSetDenominator          DOUBLE,

  _runId                      STRING,
  goldProcessedAt             TIMESTAMP
)
USING DELTA
CLUSTER BY (targetWeekStartDate, breakoutType, metricName)
COMMENT 'Gold Breakouts: raw attributed values plus safe comparison ingredients. Non-Top-N rows are bucketed into (Other) later.';

CREATE OR REPLACE PROCEDURE sdi_sp_mip_gold_breakoutMetricIngredientsByWeek_long(
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

  INSERT INTO sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
  REPLACE WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
  WITH visitorWeek AS (
    SELECT
      a.weekStartDate,
      a.visitorId,
      a.lob,
      a.platform,
      a.prospectVsBase,
      a.authState,
      a.channel,
      a.campaign,
      a.entryPage,
      a.utmSource,
      a.utmMedium,
      a.utmCampaign,
      a.pageCategory,
      a.device,
      a.buyFlowStep,
      x.nbv,
      x.sessionCount,
      x.pageViews,
      x.nbvBuyFlow,
      x.nbvConfigure,
      x.nbvCheckoutStart,
      x.orders,
      x.ordersAcquisition,
      x.ordersBase,
      x.ordersUnassisted,
      x.ordersAssisted,
      x.vrCalls,
      x.vrChats,
      x.storeLocator,
      x.orderCount
    FROM sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly a
    JOIN sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly x
      ON  x.weekStartDate = a.weekStartDate
      AND x.visitorId = a.visitorId
  ),
  exploded AS (
    SELECT
      weekStartDate,
      visitorId,
      d.breakoutType,
      d.breakoutValue,
      nbv,
      sessionCount,
      pageViews,
      nbvBuyFlow,
      nbvConfigure,
      nbvCheckoutStart,
      orders,
      ordersAcquisition,
      ordersBase,
      ordersUnassisted,
      ordersAssisted,
      vrCalls,
      vrChats,
      storeLocator,
      orderCount
    FROM visitorWeek
    LATERAL VIEW explode(array(
      named_struct('breakoutType','lob',            'breakoutValue',coalesce(lob,'Other')),
      named_struct('breakoutType','platform',       'breakoutValue',coalesce(platform,'(not set)')),
      named_struct('breakoutType','prospectVsBase', 'breakoutValue',coalesce(prospectVsBase,'Unknown')),
      named_struct('breakoutType','authState',      'breakoutValue',coalesce(authState,'(not set)')),
      named_struct('breakoutType','channel',        'breakoutValue',coalesce(channel,'(not set)')),
      named_struct('breakoutType','campaign',       'breakoutValue',coalesce(campaign,'(not set)')),
      named_struct('breakoutType','entryPage',      'breakoutValue',coalesce(entryPage,'(not set)')),
      named_struct('breakoutType','pageCategory',   'breakoutValue',coalesce(pageCategory,'(not set)')),
      named_struct('breakoutType','device',         'breakoutValue',coalesce(device,'Unknown')),
      named_struct('breakoutType','utmSource',      'breakoutValue',coalesce(utmSource,'(not set)')),
      named_struct('breakoutType','utmMedium',      'breakoutValue',coalesce(utmMedium,'(not set)')),
      named_struct('breakoutType','utmCampaign',    'breakoutValue',coalesce(utmCampaign,'(not set)')),
      named_struct('breakoutType','buyFlowStep',    'breakoutValue',coalesce(buyFlowStep,'Did not enter buy flow'))
    )) dview AS d
  ),
  weeklyWide AS (
    SELECT
      weekStartDate,
      breakoutType,
      breakoutValue,

      sum(nbv) AS nbv,
      sum(sessionCount) AS sessionCount,
      sum(pageViews) AS pageViews,
      sum(nbvBuyFlow) AS nbvBuyFlow,
      sum(nbvConfigure) AS nbvConfigure,
      sum(nbvCheckoutStart) AS nbvCheckoutStart,
      sum(orders) AS orders,
      sum(ordersAcquisition) AS ordersAcquisition,
      sum(ordersBase) AS ordersBase,
      sum(ordersUnassisted) AS ordersUnassisted,
      sum(ordersAssisted) AS ordersAssisted,
      sum(vrCalls) AS vrCalls,
      sum(vrChats) AS vrChats,
      sum(storeLocator) AS storeLocator,
      sum(orderCount) AS orderCount
    FROM exploded
    GROUP BY weekStartDate, breakoutType, breakoutValue
  ),
  weeklyCounts AS (
    SELECT
      weekStartDate,
      breakoutType,
      breakoutValue,
      metricName,
      metricValue
    FROM weeklyWide
    LATERAL VIEW stack(
      15,
      'nbv',               cast(nbv AS DOUBLE),
      'sessionCount',      cast(sessionCount AS DOUBLE),
      'pageViews',         cast(pageViews AS DOUBLE),
      'nbvBuyFlow',        cast(nbvBuyFlow AS DOUBLE),
      'nbvConfigure',      cast(nbvConfigure AS DOUBLE),
      'nbvCheckoutStart',  cast(nbvCheckoutStart AS DOUBLE),
      'orders',            cast(orders AS DOUBLE),
      'ordersAcquisition', cast(ordersAcquisition AS DOUBLE),
      'ordersBase',        cast(ordersBase AS DOUBLE),
      'ordersUnassisted',  cast(ordersUnassisted AS DOUBLE),
      'ordersAssisted',    cast(ordersAssisted AS DOUBLE),
      'vrCalls',           cast(vrCalls AS DOUBLE),
      'vrChats',           cast(vrChats AS DOUBLE),
      'storeLocator',      cast(storeLocator AS DOUBLE),
      'orderCount',        cast(orderCount AS DOUBLE)
    ) s AS metricName, metricValue
  ),
  weeklyIngredients AS (
    SELECT
      c.weekStartDate,
      c.breakoutType,
      c.breakoutValue,
      c.metricName,
      c.metricValue AS numeratorValue,
      cast(NULL AS DOUBLE) AS denominatorValue
    FROM weeklyCounts c
    JOIN sdi_tbl_mip_control_metricCatalog_static m
      ON  m.metricName = c.metricName
      AND m.metricKind = 'count'
      AND m.isActive

    UNION ALL

    SELECT
      n.weekStartDate,
      n.breakoutType,
      n.breakoutValue,
      r.metricName,
      max(CASE WHEN n.metricName = r.numeratorMetric THEN n.metricValue END) AS numeratorValue,
      max(CASE WHEN n.metricName = r.denominatorMetric THEN n.metricValue END) AS denominatorValue
    FROM weeklyCounts n
    JOIN sdi_tbl_mip_control_metricCatalog_static r
      ON  r.metricKind = 'ratio'
      AND r.isActive
      AND n.metricName IN (r.numeratorMetric, r.denominatorMetric)
    GROUP BY n.weekStartDate, n.breakoutType, n.breakoutValue, r.metricName
  ),
  availableWeeks AS (
    SELECT DISTINCT weekStartDate
    FROM sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly
  ),
  fourWeekAvailability AS (
    SELECT
      t.weekStartDate AS targetWeekStartDate,
      count(a.weekStartDate) AS fourWeekTrendWeekCount
    FROM sdi_tbl_mip_control_fiscalCalendar_static t
    LEFT JOIN availableWeeks a
      ON a.weekStartDate BETWEEN t.fourWeekAvgStartDate AND t.fourWeekAvgEndDate
    WHERE t.weekStartDate BETWEEN v_weekFrom AND v_weekTo
    GROUP BY t.weekStartDate
  ),
  targetWeeks AS (
    SELECT
      t.*,
      (cur.weekStartDate IS NOT NULL) AS thisWeekDataAvailable,
      (pw.weekStartDate IS NOT NULL) AS priorWeekDataAvailable,
      cast(coalesce(fwa.fourWeekTrendWeekCount, 0) AS INT) AS fourWeekTrendWeekCount,
      (ly.weekStartDate IS NOT NULL) AS sameWeekLyDataAvailable
    FROM sdi_tbl_mip_control_fiscalCalendar_static t
    LEFT JOIN availableWeeks cur
      ON cur.weekStartDate = t.weekStartDate
    LEFT JOIN availableWeeks pw
      ON pw.weekStartDate = t.priorWeekStartDate
    LEFT JOIN fourWeekAvailability fwa
      ON fwa.targetWeekStartDate = t.weekStartDate
    LEFT JOIN availableWeeks ly
      ON ly.weekStartDate = t.sameWeekLastYearStartDate
    WHERE t.weekStartDate BETWEEN v_weekFrom AND v_weekTo
  ),
  candidateValues AS (
    -- Include values appearing in ANY comparison period, not just current week,
    -- so historical-only long-tail values can later roll into (Other).
    SELECT DISTINCT
      t.weekStartDate AS targetWeekStartDate,
      h.breakoutType,
      h.breakoutValue
    FROM targetWeeks t
    JOIN weeklyWide h
      ON  h.weekStartDate = t.weekStartDate
       OR h.weekStartDate = t.priorWeekStartDate
       OR h.weekStartDate BETWEEN t.fourWeekAvgStartDate AND t.fourWeekAvgEndDate
       OR h.weekStartDate = t.sameWeekLastYearStartDate
  ),
  targetRanks AS (
    SELECT
      c.targetWeekStartDate,
      c.breakoutType,
      c.breakoutValue,
      cast(
        row_number() OVER (
          PARTITION BY c.targetWeekStartDate, c.breakoutType
          ORDER BY coalesce(w.nbv, 0) DESC, c.breakoutValue
        ) AS INT
      ) AS valueRankByNbv
    FROM candidateValues c
    LEFT JOIN weeklyWide w
      ON  w.weekStartDate = c.targetWeekStartDate
      AND w.breakoutType = c.breakoutType
      AND w.breakoutValue = c.breakoutValue
  )
  SELECT
    t.weekStartDate AS targetWeekStartDate,
    t.weekEndDate AS targetWeekEndDate,
    t.fiscalQuarterLabel,
    t.fiscalWeekCode,
    t.weekLabel,

    'All' AS filterLob,
    'All' AS filterPlatform,

    v.breakoutType,
    b.breakoutLabel,
    v.breakoutValue,

    r.valueRankByNbv,
    CASE
      WHEN b.topN IS NULL THEN true
      ELSE r.valueRankByNbv <= b.topN
    END AS isTopN,

    m.metricName,
    m.metricLabel,
    m.metricKind,
    m.displayFormat,
    m.changeUnit,

    CASE
      WHEN t.thisWeekDataAvailable THEN coalesce(cur.numeratorValue, 0D)
      ELSE NULL
    END AS thisWeekNumerator,
    CASE
      WHEN m.metricKind = 'ratio' AND t.thisWeekDataAvailable
        THEN coalesce(cur.denominatorValue, 0D)
      ELSE NULL
    END AS thisWeekDenominator,

    CASE
      WHEN t.priorWeekDataAvailable THEN coalesce(pw.numeratorValue, 0D)
      ELSE NULL
    END AS priorWeekNumerator,
    CASE
      WHEN m.metricKind = 'ratio' AND t.priorWeekDataAvailable
        THEN coalesce(pw.denominatorValue, 0D)
      ELSE NULL
    END AS priorWeekDenominator,

    CASE
      WHEN t.fourWeekTrendWeekCount > 0 THEN coalesce(sum(fw.numeratorValue), 0D)
      ELSE NULL
    END AS fourWeekTrendNumerator,
    CASE
      WHEN m.metricKind = 'ratio' AND t.fourWeekTrendWeekCount > 0
        THEN coalesce(sum(fw.denominatorValue), 0D)
      ELSE NULL
    END AS fourWeekTrendDenominator,

    CASE
      WHEN t.sameWeekLyDataAvailable THEN coalesce(ly.numeratorValue, 0D)
      ELSE NULL
    END AS sameWeekLyNumerator,
    CASE
      WHEN m.metricKind = 'ratio' AND t.sameWeekLyDataAvailable
        THEN coalesce(ly.denominatorValue, 0D)
      ELSE NULL
    END AS sameWeekLyDenominator,

    t.thisWeekDataAvailable,
    t.priorWeekDataAvailable,
    t.fourWeekTrendWeekCount,
    t.sameWeekLyDataAvailable,

    cast(NULL AS DOUBLE) AS peerSetNumerator,
    cast(NULL AS DOUBLE) AS peerSetDenominator,

    p_runId AS _runId,
    v_processedAt AS goldProcessedAt
  FROM targetWeeks t
  JOIN candidateValues v
    ON v.targetWeekStartDate = t.weekStartDate
  JOIN targetRanks r
    ON  r.targetWeekStartDate = v.targetWeekStartDate
    AND r.breakoutType = v.breakoutType
    AND r.breakoutValue = v.breakoutValue
  JOIN sdi_tbl_mip_control_breakoutCatalog_static b
    ON  b.breakoutType = v.breakoutType
    AND b.isActive
    AND b.isPrebuiltBreakout
  JOIN sdi_tbl_mip_control_metricCatalog_static m
    ON m.isActive
  LEFT JOIN weeklyIngredients cur
    ON  cur.weekStartDate = t.weekStartDate
    AND cur.breakoutType = v.breakoutType
    AND cur.breakoutValue = v.breakoutValue
    AND cur.metricName = m.metricName
  LEFT JOIN weeklyIngredients pw
    ON  pw.weekStartDate = t.priorWeekStartDate
    AND pw.breakoutType = v.breakoutType
    AND pw.breakoutValue = v.breakoutValue
    AND pw.metricName = m.metricName
  LEFT JOIN weeklyIngredients fw
    ON  fw.weekStartDate BETWEEN t.fourWeekAvgStartDate AND t.fourWeekAvgEndDate
    AND fw.breakoutType = v.breakoutType
    AND fw.breakoutValue = v.breakoutValue
    AND fw.metricName = m.metricName
  LEFT JOIN weeklyIngredients ly
    ON  ly.weekStartDate = t.sameWeekLastYearStartDate
    AND ly.breakoutType = v.breakoutType
    AND ly.breakoutValue = v.breakoutValue
    AND ly.metricName = m.metricName
  GROUP BY
    t.weekStartDate,
    t.weekEndDate,
    t.fiscalQuarterLabel,
    t.fiscalWeekCode,
    t.weekLabel,
    v.breakoutType,
    b.breakoutLabel,
    v.breakoutValue,
    r.valueRankByNbv,
    b.topN,
    m.metricName,
    m.metricLabel,
    m.metricKind,
    m.displayFormat,
    m.changeUnit,
    t.thisWeekDataAvailable,
    t.priorWeekDataAvailable,
    t.fourWeekTrendWeekCount,
    t.sameWeekLyDataAvailable,
    cur.numeratorValue,
    cur.denominatorValue,
    pw.numeratorValue,
    pw.denominatorValue,
    ly.numeratorValue,
    ly.denominatorValue;
END;

-- ----------------------------------------------------------------------------


-- ###########################################################################
-- END gold/02_sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long.sql
-- ###########################################################################

